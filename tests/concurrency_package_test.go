package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
)

func TestLoadConfigRespectsEnvironmentOverrides(t *testing.T) {
	t.Setenv("ICARUS_MAX_CONCURRENT", "42")
	t.Setenv("ICARUS_RUNNER_WORKERS", "7")
	t.Setenv("ICARUS_PROCESSOR_MODE", "SEQUENTIAL")
	t.Setenv("ICARUS_ITERATOR_MODE", "PARALLEL")

	cfg := concurrency.LoadConfig()

	if cfg.MaxConcurrent != 42 {
		t.Fatalf("expected MaxConcurrent 42, got %d", cfg.MaxConcurrent)
	}
	if cfg.RunnerWorkers != 7 {
		t.Fatalf("expected RunnerWorkers 7, got %d", cfg.RunnerWorkers)
	}
	if cfg.ProcessorMode != concurrency.ProcessorModeSequential {
		t.Fatalf("expected sequential processor mode, got %s", cfg.ProcessorMode)
	}
	if cfg.IteratorMode != concurrency.IteratorModeParallel {
		t.Fatalf("expected parallel iterator mode, got %s", cfg.IteratorMode)
	}
	if cfg.Source != concurrency.ConfigSourceEnvVar {
		t.Fatalf("expected env var source, got %s", cfg.Source)
	}
}

func TestLoadConfigFallsBackToDefaults(t *testing.T) {
	cfg := concurrency.LoadConfig()
	if cfg.MaxConcurrent < 1 {
		t.Fatalf("expected positive MaxConcurrent, got %d", cfg.MaxConcurrent)
	}
	if cfg.RunnerWorkers < 1 {
		t.Fatalf("expected positive RunnerWorkers, got %d", cfg.RunnerWorkers)
	}
	if cfg.Source == "" {
		t.Fatal("expected config source to be populated")
	}
}

func TestLimiterAcquireReleaseTracksMetrics(t *testing.T) {
	limiter := concurrency.NewLimiter(2)
	ctx := context.Background()

	if err := limiter.Acquire(ctx); err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if limiter.CurrentActive() != 1 {
		t.Fatalf("expected 1 active worker, got %d", limiter.CurrentActive())
	}
	limiter.Release()

	metrics := limiter.GetMetrics()
	if metrics.TotalAcquired != 1 {
		t.Fatalf("expected TotalAcquired 1, got %d", metrics.TotalAcquired)
	}
	if metrics.TotalReleased != 1 {
		t.Fatalf("expected TotalReleased 1, got %d", metrics.TotalReleased)
	}
}

func TestLimiterAcquireHonorsContextCancellation(t *testing.T) {
	limiter := concurrency.NewLimiter(1)
	// Fill the limiter so the next Acquire blocks on the semaphore
	require := func(err error) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	require(limiter.Acquire(context.Background()))
	defer limiter.Release()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := limiter.Acquire(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLimiterCircuitBreakerOpensAfterFailures(t *testing.T) {
	cb := concurrency.NewCircuitBreaker(1, time.Hour)
	limiter := concurrency.NewLimiterWithCircuitBreaker(1, cb)

	ctx := context.Background()
	_ = limiter.GoSync(ctx, func() error { return errors.New("boom") })

	if cb.GetState() != concurrency.StateOpen {
		t.Fatalf("expected circuit breaker to open, got %s", cb.GetState())
	}

	err := limiter.Acquire(ctx)
	if err == nil || !strings.Contains(err.Error(), "circuit breaker is open") {
		t.Fatalf("expected circuit breaker error, got %v", err)
	}
}
