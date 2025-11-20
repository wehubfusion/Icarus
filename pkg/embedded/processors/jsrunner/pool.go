package jsrunner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dop251/goja"
)

// VMPool manages a pool of reusable JavaScript VM instances
type VMPool struct {
	pool          chan *PooledVM
	utilRegistry  *UtilityRegistry
	config        *Config
	minSize       int
	maxSize       int
	maxReuseCount int
	currentSize   int32
	totalCreated  int64
	totalAcquired int64
	totalReleased int64
	mu            sync.Mutex
	closed        bool
}

// PooledVM represents a VM instance in the pool
type PooledVM struct {
	vm            *goja.Runtime
	createdAt     time.Time
	lastUsedAt    time.Time
	reuseCount    int
	maxReuseCount int
	mu            sync.RWMutex // Protects vm field for safe concurrent access
}

// PoolConfig defines the configuration for the VM pool
type PoolConfig struct {
	MinSize       int           // Minimum number of VMs to keep in pool
	MaxSize       int           // Maximum number of VMs in pool
	MaxReuseCount int           // Maximum times a VM can be reused before recreation
	IdleTimeout   time.Duration // How long a VM can be idle before being removed
}

// DefaultPoolConfig returns the default pool configuration
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinSize:       5,
		MaxSize:       50,
		MaxReuseCount: 1000,
		IdleTimeout:   5 * time.Minute,
	}
}

// NewVMPool creates a new VM pool
func NewVMPool(config *Config, poolConfig PoolConfig) (*VMPool, error) {
	if poolConfig.MinSize < 0 {
		poolConfig.MinSize = 0
	}
	if poolConfig.MaxSize <= 0 {
		poolConfig.MaxSize = DefaultPoolConfig().MaxSize
	}
	if poolConfig.MinSize > poolConfig.MaxSize {
		poolConfig.MinSize = poolConfig.MaxSize
	}
	if poolConfig.MaxReuseCount <= 0 {
		poolConfig.MaxReuseCount = DefaultPoolConfig().MaxReuseCount
	}

	pool := &VMPool{
		pool:          make(chan *PooledVM, poolConfig.MaxSize),
		utilRegistry:  NewUtilityRegistry(),
		config:        config,
		minSize:       poolConfig.MinSize,
		maxSize:       poolConfig.MaxSize,
		maxReuseCount: poolConfig.MaxReuseCount,
		currentSize:   0,
	}

	// Pre-create minimum number of VMs
	for i := 0; i < poolConfig.MinSize; i++ {
		vm, err := pool.createVM()
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create initial VM: %w", err)
		}
		pool.pool <- vm
	}

	return pool, nil
}

// Acquire gets a VM from the pool or creates a new one
func (p *VMPool) Acquire(ctx context.Context) (*PooledVM, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}
	p.mu.Unlock()

	atomic.AddInt64(&p.totalAcquired, 1)

	select {
	case vm, ok := <-p.pool:
		if !ok {
			// Pool was closed
			return nil, fmt.Errorf("pool is closed")
		}

		// If VM is nil or has been destroyed, create a new one
		if vm == nil || vm.vm == nil {
			newVM, err := p.createVM()
			if err != nil {
				return nil, fmt.Errorf("failed to create VM: %w", err)
			}
			return newVM, nil
		}

		// Quick health check: ensure VM is usable
		if !p.isVMHealthy(vm) {
			// Destroy the unhealthy VM and replace it
			p.destroyVM(vm)
			newVM, err := p.createVM()
			if err != nil {
				return nil, fmt.Errorf("failed to recreate VM: %w", err)
			}
			return newVM, nil
		}

		// Got a healthy VM from pool
		vm.lastUsedAt = time.Now()
		vm.reuseCount++

		// Check if VM needs to be recreated due to reuse count
		if vm.reuseCount >= vm.maxReuseCount {
			p.destroyVM(vm)
			newVM, err := p.createVM()
			if err != nil {
				return nil, fmt.Errorf("failed to recreate VM: %w", err)
			}
			return newVM, nil
		}

		return vm, nil

	case <-ctx.Done():
		return nil, ctx.Err()

	default:
		// Pool is empty, try to create a new VM
		currentSize := atomic.LoadInt32(&p.currentSize)
		if int(currentSize) < p.maxSize {
			vm, err := p.createVM()
			if err != nil {
				return nil, fmt.Errorf("failed to create VM: %w", err)
			}
			return vm, nil
		}

		// Pool is at max capacity, wait for a VM to become available
		select {
		case vm, ok := <-p.pool:
			if !ok {
				// Pool was closed
				return nil, fmt.Errorf("pool is closed")
			}

			if vm == nil || vm.vm == nil || !p.isVMHealthy(vm) {
				if vm != nil {
					p.destroyVM(vm)
				}
				newVM, err := p.createVM()
				if err != nil {
					return nil, fmt.Errorf("failed to recreate VM: %w", err)
				}
				return newVM, nil
			}

			vm.lastUsedAt = time.Now()
			vm.reuseCount++

			if vm.reuseCount >= vm.maxReuseCount {
				p.destroyVM(vm)
				newVM, err := p.createVM()
				if err != nil {
					return nil, fmt.Errorf("failed to recreate VM: %w", err)
				}
				return newVM, nil
			}

			return vm, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Release returns a VM to the pool
func (p *VMPool) Release(vm *PooledVM) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.destroyVM(vm)
		return nil
	}
	p.mu.Unlock()

	atomic.AddInt64(&p.totalReleased, 1)

	// Reset VM state before returning to pool
	if err := p.resetVM(vm); err != nil {
		// If reset fails, destroy the VM and attempt to replace it to preserve pool capacity
		p.destroyVM(vm)

		// Try to create a replacement VM and return it to the pool
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()

		if !closed {
			if replacement, createErr := p.createVM(); createErr == nil {
				// Try to return replacement to pool (best-effort)
				select {
				case p.pool <- replacement:
				default:
					// Pool full - destroy replacement
					p.destroyVM(replacement)
				}
			}
		}

		return fmt.Errorf("failed to reset VM: %w", err)
	}

	// Try to return to pool, or destroy if pool is full
	select {
	case p.pool <- vm:
		return nil
	default:
		// Pool is full, destroy this VM
		p.destroyVM(vm)
		return nil
	}
}

// createVM creates a new VM instance
func (p *VMPool) createVM() (*PooledVM, error) {
	vm := goja.New()

	// Apply sandbox restrictions
	if err := CreateSecureContext(vm, p.config); err != nil {
		return nil, fmt.Errorf("failed to create secure context: %w", err)
	}

	// Register utilities
	if err := p.utilRegistry.RegisterEnabled(vm, p.config); err != nil {
		return nil, fmt.Errorf("failed to register utilities: %w", err)
	}

	pooledVM := &PooledVM{
		vm:            vm,
		createdAt:     time.Now(),
		lastUsedAt:    time.Now(),
		reuseCount:    0,
		maxReuseCount: p.maxReuseCount,
	}

	atomic.AddInt32(&p.currentSize, 1)
	atomic.AddInt64(&p.totalCreated, 1)

	return pooledVM, nil
}

// resetVM clears the VM state for reuse
func (p *VMPool) resetVM(vm *PooledVM) error {
	// Clear global variables (except built-ins and utilities)
	// This is a simplified reset - in production you might want more thorough cleanup
	resetScript := `
		(function() {
			// Get all properties of the global object
			var globals = Object.getOwnPropertyNames(this);
			var builtins = [
				'Object', 'Array', 'Function', 'String', 'Number', 'Boolean',
				'Date', 'RegExp', 'Error', 'Math', 'JSON', 'console',
				'parseInt', 'parseFloat', 'isNaN', 'isFinite',
				'decodeURI', 'decodeURIComponent', 'encodeURI', 'encodeURIComponent',
				'undefined', 'NaN', 'Infinity', 'eval',
				'btoa', 'atob', 'setTimeout', 'clearTimeout', 'setInterval', 'clearInterval',
				'__security__'
			];
			
			for (var i = 0; i < globals.length; i++) {
				var prop = globals[i];
				if (builtins.indexOf(prop) === -1) {
					try {
						delete this[prop];
					} catch (e) {
						// Property might not be deletable
					}
				}
			}
		})()
	`

	// First cleanup utilities to stop timers and background tasks that may reference the VM
	vm.mu.RLock()
	if vm.vm != nil {
		if err := p.utilRegistry.CleanupEnabled(vm.vm, p.config); err != nil {
			vm.mu.RUnlock()
			return fmt.Errorf("failed to cleanup utilities: %w", err)
		}
	}
	vm.mu.RUnlock()

	// Then run a lightweight reset script to clear non-builtins
	vm.mu.RLock()
	if vm.vm != nil {
		_, err := vm.vm.RunString(resetScript)
		vm.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("failed to run reset script: %w", err)
		}
	} else {
		vm.mu.RUnlock()
	}

	return nil
}

// destroyVM destroys a VM instance
func (p *VMPool) destroyVM(vm *PooledVM) {
	if vm == nil {
		return
	}

	// Cleanup utilities with read lock (utilities need the VM)
	vm.mu.RLock()
	if vm.vm != nil {
		_ = p.utilRegistry.CleanupEnabled(vm.vm, p.config)
	}
	vm.mu.RUnlock()

	// Clear the VM reference with write lock
	vm.mu.Lock()
	vm.vm = nil
	vm.mu.Unlock()

	atomic.AddInt32(&p.currentSize, -1)
}

// isVMHealthy performs a lightweight health check on a VM by evaluating a trivial expression.
// It returns false if the VM is nil, destroyed or returns an error when executing the check script.
func (p *VMPool) isVMHealthy(vm *PooledVM) bool {
	if vm == nil {
		return false
	}
	vm.mu.RLock()
	if vm.vm == nil {
		vm.mu.RUnlock()
		return false
	}
	_, err := vm.vm.RunString("1+1")
	vm.mu.RUnlock()
	return err == nil
}

// Close closes the pool and destroys all VMs
func (p *VMPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	close(p.pool)

	// Drain and destroy all VMs in the pool
	for vm := range p.pool {
		p.destroyVM(vm)
	}

	return nil
}

// Stats returns pool statistics
func (p *VMPool) Stats() PoolStats {
	return PoolStats{
		CurrentSize:   int(atomic.LoadInt32(&p.currentSize)),
		MinSize:       p.minSize,
		MaxSize:       p.maxSize,
		TotalCreated:  atomic.LoadInt64(&p.totalCreated),
		TotalAcquired: atomic.LoadInt64(&p.totalAcquired),
		TotalReleased: atomic.LoadInt64(&p.totalReleased),
		Available:     len(p.pool),
	}
}

// PoolStats contains pool statistics
type PoolStats struct {
	CurrentSize   int   `json:"current_size"`
	MinSize       int   `json:"min_size"`
	MaxSize       int   `json:"max_size"`
	TotalCreated  int64 `json:"total_created"`
	TotalAcquired int64 `json:"total_acquired"`
	TotalReleased int64 `json:"total_released"`
	Available     int   `json:"available"`
}

// String returns a string representation of the stats
func (s PoolStats) String() string {
	return fmt.Sprintf(
		"Pool Stats: Current=%d, Min=%d, Max=%d, Created=%d, Acquired=%d, Released=%d, Available=%d",
		s.CurrentSize, s.MinSize, s.MaxSize, s.TotalCreated, s.TotalAcquired, s.TotalReleased, s.Available,
	)
}

// HealthCheck performs a health check on the pool
func (p *VMPool) HealthCheck() error {
	if p.closed {
		return fmt.Errorf("pool is closed")
	}

	stats := p.Stats()
	if stats.CurrentSize < p.minSize {
		return fmt.Errorf("pool size (%d) is below minimum (%d)", stats.CurrentSize, p.minSize)
	}

	return nil
}
