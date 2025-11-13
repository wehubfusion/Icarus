package jsrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements the NodeExecutor interface for JavaScript execution
type Executor struct {
	pool       *VMPool
	poolConfig *Config
	mu         sync.RWMutex
	poolMu     sync.Mutex
}

// NewExecutor creates a new JavaScript executor
func NewExecutor() *Executor {
	return &Executor{}
}

// NewExecutorWithPool creates a new JavaScript executor with a pre-configured pool
func NewExecutorWithPool(pool *VMPool) *Executor {
	return &Executor{
		pool: pool,
	}
}

// Execute implements the NodeExecutor interface
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var jsConfig Config
	if err := json.Unmarshal(config.Configuration, &jsConfig); err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Apply defaults
	jsConfig.ApplyDefaults()

	// Validate configuration
	if err := jsConfig.Validate(); err != nil {
		return nil, err
	}

	// Ensure pool is initialized with this config
	if err := e.ensurePool(&jsConfig); err != nil {
		return nil, fmt.Errorf("failed to initialize pool: %w", err)
	}

	// Parse input
	var input map[string]interface{}
	if len(config.Input) > 0 {
		if err := json.Unmarshal(config.Input, &input); err != nil {
			return nil, fmt.Errorf("failed to parse input: %w", err)
		}
	}

	// Parse schema if provided (injected by enrichment)
	var schema map[string]interface{}
	if len(jsConfig.SchemaDefinition) > 0 {
		if err := json.Unmarshal(jsConfig.SchemaDefinition, &schema); err != nil {
			return nil, fmt.Errorf("failed to parse schema: %w", err)
		}
	}

	// Acquire read lock to ensure pool doesn't change during execution
	e.mu.RLock()
	currentPool := e.pool
	e.mu.RUnlock()

	// Execute with timeout
	result, err := e.executeWithTimeoutOnPool(ctx, currentPool, &jsConfig, input, schema)
	if err != nil {
		// Handle JSError types
		jsErr, ok := err.(*JSError)
		if !ok {
			return nil, err
		}

		// Return error in structured format as raw bytes
		errorOutput := map[string]interface{}{
			"error": map[string]interface{}{
				"type":    jsErr.Type,
				"message": jsErr.Message,
				"stack":   jsErr.StackTrace,
			},
		}

		output, _ := json.Marshal(errorOutput)
		return output, fmt.Errorf("%v", jsErr)
	}

	// Convert result to raw bytes
	// If result is already bytes, return as-is
	if bytes, ok := result.([]byte); ok {
		return bytes, nil
	}

	// If result is a string, convert to bytes
	if str, ok := result.(string); ok {
		return []byte(str), nil
	}

	// Otherwise, marshal to JSON and return bytes
	output, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return output, nil
}

// executeWithTimeoutOnPool executes JavaScript code with timeout and interrupt handling on a specific pool
func (e *Executor) executeWithTimeoutOnPool(ctx context.Context, pool *VMPool, config *Config, input map[string]interface{}, schema map[string]interface{}) (result interface{}, err error) {
	// Recover from panics in script execution
	defer func() {
		if r := recover(); r != nil {
			err = NewInternalError(fmt.Sprintf("panic during execution: %v", r))
		}
	}()

	// Create context with timeout from config
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(config.Timeout)*time.Millisecond)
	defer cancel()

	// Acquire VM from pool
	vm, err := pool.Acquire(timeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire VM: %w", err)
	}
	defer pool.Release(vm)

	// Set up interrupt channel for timeout
	done := make(chan struct{})
	var interrupted bool
	var interruptMu sync.Mutex

	go func() {
		select {
		case <-timeoutCtx.Done():
			interruptMu.Lock()
			interrupted = true
			interruptMu.Unlock()
			// Safely access VM with read lock
			vm.mu.RLock()
			if vm.vm != nil {
				vm.vm.Interrupt("execution timeout")
			}
			vm.mu.RUnlock()
		case <-done:
			return
		}
	}()

	defer close(done)

	// Inject input data into the VM
	if err := vm.vm.Set("input", input); err != nil {
		return nil, fmt.Errorf("failed to set input: %w", err)
	}

	// Inject schema into the VM if provided
	if schema != nil {
		if err := vm.vm.Set("schema", schema); err != nil {
			return nil, fmt.Errorf("failed to set schema: %w", err)
		}
	}

	// Execute the script
	startTime := time.Now()
	value, err := vm.vm.RunString(config.Script)
	executionTime := time.Since(startTime)

	if err != nil {
		// Check if it was interrupted
		interruptMu.Lock()
		wasInterrupted := interrupted
		interruptMu.Unlock()

		if wasInterrupted {
			return nil, NewTimeoutError(config.Timeout)
		}

		// Parse goja exception
		if exc, ok := err.(*goja.Exception); ok {
			return nil, ParseGojaException(exc)
		}

		return nil, WrapError(err)
	}

	// Export the result
	result = value.Export()

	// Return result as-is (no metadata injection since we're returning raw bytes)
	// Metadata can be added by the caller if needed
	_ = executionTime // Keep for potential logging
	_ = vm.reuseCount // Keep for potential logging

	return result, nil
}

// ensurePool initializes the VM pool if not already created
func (e *Executor) ensurePool(config *Config) error {
	e.poolMu.Lock()
	defer e.poolMu.Unlock()

	if e.pool != nil {
		// Pool already exists, reuse it
		// Note: Pool will be created with first config and reused for subsequent calls
		// For different configs, create separate Executor instances
		e.poolConfig = config // Update for reference
		return nil
	}

	// Create new pool with default pool configuration
	pool, err := NewVMPool(config, DefaultPoolConfig())
	if err != nil {
		return fmt.Errorf("failed to create VM pool: %w", err)
	}

	e.pool = pool
	e.poolConfig = config
	return nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-jsrunner"
}

// Close closes the executor and releases resources
func (e *Executor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.pool != nil {
		return e.pool.Close()
	}

	return nil
}

// GetPoolStats returns statistics about the VM pool
func (e *Executor) GetPoolStats() *PoolStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.pool == nil {
		return nil
	}

	stats := e.pool.Stats()
	return &stats
}

// ExecuteScript is a convenience method for executing a script directly
func (e *Executor) ExecuteScript(ctx context.Context, script string, input map[string]interface{}) (interface{}, error) {
	config := Config{
		Script: script,
	}
	config.ApplyDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if err := e.ensurePool(&config); err != nil {
		return nil, fmt.Errorf("failed to initialize pool: %w", err)
	}

	// Acquire read lock and get current pool
	e.mu.RLock()
	currentPool := e.pool
	e.mu.RUnlock()

	return e.executeWithTimeoutOnPool(ctx, currentPool, &config, input, nil)
}

// CompileScript compiles a script without executing it (useful for validation)
func CompileScript(script string) error {
	vm := goja.New()
	// Use RunString to check syntax - goja will parse before executing
	_, err := vm.RunString("(function(){" + script + "})")
	if err != nil {
		if exc, ok := err.(*goja.Exception); ok {
			return ParseGojaException(exc)
		}
		return WrapError(err)
	}
	return nil
}

// ValidateConfig validates a configuration without executing
func ValidateConfig(configJSON []byte) error {
	var config Config
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	config.ApplyDefaults()
	if err := config.Validate(); err != nil {
		return err
	}

	// Try to compile the script
	return CompileScript(config.Script)
}
