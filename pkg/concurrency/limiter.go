package concurrency

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks concurrency limiter performance metrics
type Metrics struct {
	TotalAcquired   int64
	TotalReleased   int64
	PeakConcurrent  int64
	TotalWaitTimeNs int64
	mu              sync.RWMutex
}

// Limiter provides semaphore-based concurrency control with observability
type Limiter struct {
	sem            chan struct{}
	active         int64
	metrics        *Metrics
	circuitBreaker *CircuitBreaker
}

// NewLimiter creates a new concurrency limiter with the specified maximum concurrent operations
func NewLimiter(maxConcurrent int) *Limiter {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}

	return &Limiter{
		sem:            make(chan struct{}, maxConcurrent),
		active:         0,
		metrics:        &Metrics{},
		circuitBreaker: NewCircuitBreaker(100, 30*time.Second), // 100 failures in 30s opens circuit
	}
}

// NewLimiterWithCircuitBreaker creates a limiter with custom circuit breaker settings
func NewLimiterWithCircuitBreaker(maxConcurrent int, cb *CircuitBreaker) *Limiter {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}

	return &Limiter{
		sem:            make(chan struct{}, maxConcurrent),
		active:         0,
		metrics:        &Metrics{},
		circuitBreaker: cb,
	}
}

// Acquire attempts to acquire a slot in the limiter with context support
// Returns an error if context is cancelled or circuit breaker is open
func (l *Limiter) Acquire(ctx context.Context) error {
	// Check circuit breaker first
	if l.circuitBreaker.IsOpen() {
		return fmt.Errorf("circuit breaker is open")
	}

	start := time.Now()

	select {
	case l.sem <- struct{}{}:
		// Successfully acquired
		waitTime := time.Since(start)
		atomic.AddInt64(&l.metrics.TotalWaitTimeNs, waitTime.Nanoseconds())
		atomic.AddInt64(&l.metrics.TotalAcquired, 1)

		// Update active count and peak
		current := atomic.AddInt64(&l.active, 1)
		l.updatePeak(current)

		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release releases a slot back to the limiter
func (l *Limiter) Release() {
	select {
	case <-l.sem:
		atomic.AddInt64(&l.active, -1)
		atomic.AddInt64(&l.metrics.TotalReleased, 1)
	default:
		// Should not happen in correct usage
	}
}

// Go executes a function in a goroutine with concurrency limiting
// The function is executed only if a slot can be acquired
func (l *Limiter) Go(ctx context.Context, fn func() error) error {
	if err := l.Acquire(ctx); err != nil {
		return err
	}

	go func() {
		defer l.Release()

		if err := fn(); err != nil {
			l.circuitBreaker.RecordFailure()
		} else {
			l.circuitBreaker.RecordSuccess()
		}
	}()

	return nil
}

// GoSync executes a function synchronously with concurrency limiting
// Useful when you need to wait for the result
func (l *Limiter) GoSync(ctx context.Context, fn func() error) error {
	if err := l.Acquire(ctx); err != nil {
		return err
	}

	defer l.Release()

	if err := fn(); err != nil {
		l.circuitBreaker.RecordFailure()
		return err
	}

	l.circuitBreaker.RecordSuccess()
	return nil
}

// CurrentActive returns the current number of active goroutines
func (l *Limiter) CurrentActive() int64 {
	return atomic.LoadInt64(&l.active)
}

// GetMetrics returns a copy of the current metrics
func (l *Limiter) GetMetrics() Metrics {
	l.metrics.mu.RLock()
	defer l.metrics.mu.RUnlock()

	return Metrics{
		TotalAcquired:   atomic.LoadInt64(&l.metrics.TotalAcquired),
		TotalReleased:   atomic.LoadInt64(&l.metrics.TotalReleased),
		PeakConcurrent:  atomic.LoadInt64(&l.metrics.PeakConcurrent),
		TotalWaitTimeNs: atomic.LoadInt64(&l.metrics.TotalWaitTimeNs),
	}
}

// GetAverageWaitTime calculates the average wait time for acquiring a slot
func (l *Limiter) GetAverageWaitTime() time.Duration {
	metrics := l.GetMetrics()
	if metrics.TotalAcquired == 0 {
		return 0
	}

	avgNs := metrics.TotalWaitTimeNs / metrics.TotalAcquired
	return time.Duration(avgNs)
}

// Reset resets the metrics (useful for testing or periodic resets)
func (l *Limiter) Reset() {
	atomic.StoreInt64(&l.metrics.TotalAcquired, 0)
	atomic.StoreInt64(&l.metrics.TotalReleased, 0)
	atomic.StoreInt64(&l.metrics.PeakConcurrent, 0)
	atomic.StoreInt64(&l.metrics.TotalWaitTimeNs, 0)
}

// updatePeak updates the peak concurrent count if current is higher
func (l *Limiter) updatePeak(current int64) {
	for {
		peak := atomic.LoadInt64(&l.metrics.PeakConcurrent)
		if current <= peak {
			break
		}
		if atomic.CompareAndSwapInt64(&l.metrics.PeakConcurrent, peak, current) {
			break
		}
	}
}

// GetCircuitBreakerState returns the current state of the circuit breaker
func (l *Limiter) GetCircuitBreakerState() string {
	if l.circuitBreaker.IsOpen() {
		return "open"
	}
	return "closed"
}
