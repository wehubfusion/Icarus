package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
	"github.com/wehubfusion/Icarus/pkg/iteration"
)

func TestIteratorProcessReturnsResults(t *testing.T) {
	items := []interface{}{1, 2, 3}
	iter := iteration.NewIterator(iteration.Config{MaxConcurrent: 2})

	results, err := iter.Process(context.Background(), items, func(ctx context.Context, item interface{}, idx int) (interface{}, error) {
		return item.(int) * 2, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != len(items) {
		t.Fatalf("expected %d results, got %d", len(items), len(results))
	}
	for idx, val := range results {
		if val.(int) != items[idx].(int)*2 {
			t.Fatalf("unexpected value at %d: %v", idx, val)
		}
	}
}

func TestIteratorProcessStopsOnError(t *testing.T) {
	items := []interface{}{1, 2, 3}
	iter := iteration.NewIterator(iteration.Config{MaxConcurrent: 2})

	_, err := iter.Process(context.Background(), items, func(ctx context.Context, item interface{}, idx int) (interface{}, error) {
		if idx == 1 {
			return nil, errors.New("boom")
		}
		return item, nil
	})
	if err == nil || err.Error() == "" {
		t.Fatalf("expected error, got %v", err)
	}
	if err.Error() != "failed processing item 1: boom" {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestIteratorHonorsLimiterPeakConcurrency(t *testing.T) {
	items := []interface{}{1, 2, 3, 4}
	limiter := concurrency.NewLimiter(1)
	iter := iteration.NewIteratorWithLimiter(iteration.Config{MaxConcurrent: 1}, limiter)

	_, err := iter.Process(context.Background(), items, func(ctx context.Context, item interface{}, idx int) (interface{}, error) {
		return item, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	metrics := limiter.GetMetrics()
	if metrics.PeakConcurrent != 1 {
		t.Fatalf("expected limiter peak concurrency 1, got %d", metrics.PeakConcurrent)
	}
}
