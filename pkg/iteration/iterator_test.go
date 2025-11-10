package iteration

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIterator_ProcessSequential_Success(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy: StrategySequential,
	})

	items := []interface{}{1, 2, 3}
	
	results, err := iterator.Process(context.Background(), items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		num := item.(int)
		return num * 2, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []interface{}{2, 4, 6}, results)
}

func TestIterator_ProcessSequential_FailFast(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy: StrategySequential,
	})

	items := []interface{}{1, 2, 3, 4, 5}
	processCount := 0
	
	results, err := iterator.Process(context.Background(), items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		processCount++
		if index == 2 {
			return nil, errors.New("item 2 failed")
		}
		return item, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed processing item 2")
	assert.Nil(t, results)
	assert.Equal(t, 3, processCount, "Should stop after item 2 fails")
}

func TestIterator_ProcessSequential_EmptyArray(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy: StrategySequential,
	})

	results, err := iterator.Process(context.Background(), []interface{}{}, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		t.Fatal("Should not be called for empty array")
		return nil, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []interface{}{}, results)
}

func TestIterator_ProcessParallel_Success(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy:      StrategyParallel,
		MaxConcurrent: 3,
	})

	items := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		items[i] = i
	}
	
	results, err := iterator.Process(context.Background(), items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		time.Sleep(10 * time.Millisecond) // Simulate work
		num := item.(int)
		return num * 2, nil
	})

	require.NoError(t, err)
	assert.Len(t, results, 10)
	
	// Verify all results are correct (order preserved)
	for i := 0; i < 10; i++ {
		assert.Equal(t, i*2, results[i])
	}
}

func TestIterator_ProcessParallel_FailFast(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy:      StrategyParallel,
		MaxConcurrent: 5,
	})

	items := make([]interface{}, 20)
	for i := 0; i < 20; i++ {
		items[i] = i
	}
	
	processedItems := &sync.Map{}
	
	results, err := iterator.Process(context.Background(), items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		processedItems.Store(index, true)
		
		// Item 5 fails
		if index == 5 {
			time.Sleep(20 * time.Millisecond) // Give time for others to start
			return nil, fmt.Errorf("item %d failed", index)
		}
		
		// Check if context was cancelled (fail-fast)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			time.Sleep(50 * time.Millisecond) // Simulate work
			return item, nil
		}
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed processing item 5")
	assert.Nil(t, results)
	
	// Count how many items were processed before cancellation
	count := 0
	processedItems.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	
	// Should be less than total (some workers stopped early due to fail-fast)
	assert.Less(t, count, 20, "Fail-fast should prevent processing all items")
}

func TestIterator_ProcessParallel_EmptyArray(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy:      StrategyParallel,
		MaxConcurrent: 4,
	})

	results, err := iterator.Process(context.Background(), []interface{}{}, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		t.Fatal("Should not be called for empty array")
		return nil, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []interface{}{}, results)
}

func TestIterator_NewIterator_DefaultMaxConcurrent(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy:      StrategyParallel,
		MaxConcurrent: 0, // Should default to runtime.NumCPU()
	})

	assert.Equal(t, runtime.NumCPU(), iterator.config.MaxConcurrent)
}

func TestIterator_ProcessSequential_PreservesOrder(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy: StrategySequential,
	})

	items := []interface{}{"a", "b", "c", "d"}
	
	results, err := iterator.Process(context.Background(), items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		return fmt.Sprintf("%s-%d", item.(string), index), nil
	})

	require.NoError(t, err)
	assert.Equal(t, []interface{}{"a-0", "b-1", "c-2", "d-3"}, results)
}

func TestIterator_ProcessParallel_PreservesOrder(t *testing.T) {
	iterator := NewIterator(Config{
		Strategy:      StrategyParallel,
		MaxConcurrent: 4,
	})

	items := []interface{}{5, 4, 3, 2, 1, 0}
	
	results, err := iterator.Process(context.Background(), items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		// Sleep different amounts to test order preservation
		time.Sleep(time.Duration(item.(int)) * time.Millisecond)
		return index * 10, nil
	})

	require.NoError(t, err)
	// Results should be in original order despite variable processing times
	assert.Equal(t, []interface{}{0, 10, 20, 30, 40, 50}, results)
}

