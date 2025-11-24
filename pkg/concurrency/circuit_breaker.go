package concurrency

import (
	"sync"
	"sync/atomic"
	"time"
)

// CircuitBreakerState represents the state of the circuit breaker
type CircuitBreakerState int32

const (
	// StateClosed indicates the circuit is closed and operations are allowed
	StateClosed CircuitBreakerState = 0

	// StateOpen indicates the circuit is open and operations are blocked
	StateOpen CircuitBreakerState = 1

	// StateHalfOpen indicates the circuit is testing if it should close
	StateHalfOpen CircuitBreakerState = 2
)

// CircuitBreaker prevents cascade failures during overload conditions
type CircuitBreaker struct {
	state                int32 // atomic: CircuitBreakerState
	consecutiveFailures  int64 // atomic
	consecutiveSuccesses int64 // atomic
	failureThreshold     int64
	resetTimeout         time.Duration
	lastFailureTime      int64 // atomic: Unix nano timestamp
	mu                   sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker with the specified threshold and timeout
func NewCircuitBreaker(failureThreshold int64, resetTimeout time.Duration) *CircuitBreaker {
	if failureThreshold <= 0 {
		failureThreshold = 10
	}
	if resetTimeout <= 0 {
		resetTimeout = 30 * time.Second
	}

	return &CircuitBreaker{
		state:                int32(StateClosed),
		consecutiveFailures:  0,
		consecutiveSuccesses: 0,
		failureThreshold:     failureThreshold,
		resetTimeout:         resetTimeout,
		lastFailureTime:      0,
	}
}

// IsOpen returns true if the circuit breaker is currently open (blocking operations)
func (cb *CircuitBreaker) IsOpen() bool {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	if state == StateOpen {
		// Check if we should transition to half-open
		lastFailure := atomic.LoadInt64(&cb.lastFailureTime)
		if lastFailure > 0 && time.Since(time.Unix(0, lastFailure)) > cb.resetTimeout {
			cb.transitionTo(StateHalfOpen)
			return false
		}
		return true
	}

	return false
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	// Reset failure count on success
	atomic.StoreInt64(&cb.consecutiveFailures, 0)

	if state == StateHalfOpen {
		// Increment success count in half-open state
		successes := atomic.AddInt64(&cb.consecutiveSuccesses, 1)

		// After 5 consecutive successes in half-open, close the circuit
		if successes >= 5 {
			cb.transitionTo(StateClosed)
			atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
		}
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	// Reset success count on failure
	atomic.StoreInt64(&cb.consecutiveSuccesses, 0)

	// Record failure time
	atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())

	// Increment failure count
	failures := atomic.AddInt64(&cb.consecutiveFailures, 1)

	// Check if we should open the circuit
	if state == StateClosed && failures >= cb.failureThreshold {
		cb.transitionTo(StateOpen)
	} else if state == StateHalfOpen {
		// Any failure in half-open state reopens the circuit
		cb.transitionTo(StateOpen)
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return CircuitBreakerState(atomic.LoadInt32(&cb.state))
}

// GetConsecutiveFailures returns the current number of consecutive failures
func (cb *CircuitBreaker) GetConsecutiveFailures() int64 {
	return atomic.LoadInt64(&cb.consecutiveFailures)
}

// GetConsecutiveSuccesses returns the current number of consecutive successes
func (cb *CircuitBreaker) GetConsecutiveSuccesses() int64 {
	return atomic.LoadInt64(&cb.consecutiveSuccesses)
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.transitionTo(StateClosed)
	atomic.StoreInt64(&cb.consecutiveFailures, 0)
	atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	atomic.StoreInt64(&cb.lastFailureTime, 0)
}

// transitionTo transitions the circuit breaker to a new state
func (cb *CircuitBreaker) transitionTo(newState CircuitBreakerState) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	oldState := CircuitBreakerState(atomic.LoadInt32(&cb.state))
	if oldState == newState {
		return
	}

	atomic.StoreInt32(&cb.state, int32(newState))

	// Reset counters on state transitions
	switch newState {
	case StateClosed:
		atomic.StoreInt64(&cb.consecutiveFailures, 0)
		atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	case StateHalfOpen:
		atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	}
}

// String returns the string representation of the circuit breaker state
func (s CircuitBreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	}
	return "unknown"
}
