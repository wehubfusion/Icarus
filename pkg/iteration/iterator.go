package iteration

import (
	"context"
	"fmt"
	"runtime"
	"sync"
)

// Iterator handles array iteration with configurable execution strategy
type Iterator struct {
	config Config
}

// NewIterator creates a new iterator with given config
func NewIterator(config Config) *Iterator {
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = runtime.NumCPU()
	}
	return &Iterator{config: config}
}

// Process iterates over array items and processes each with the given function
// Returns array of results or error (fail-fast on first error)
func (it *Iterator) Process(ctx context.Context, items []interface{}, processFn ProcessFunc) ([]interface{}, error) {
	if len(items) == 0 {
		return []interface{}{}, nil
	}

	if it.config.Strategy == StrategySequential {
		return it.processSequential(ctx, items, processFn)
	}
	return it.processParallel(ctx, items, processFn)
}

// processSequential processes items one by one (fail-fast)
func (it *Iterator) processSequential(ctx context.Context, items []interface{}, processFn ProcessFunc) ([]interface{}, error) {
	results := make([]interface{}, len(items))

	for i, item := range items {
		output, err := processFn(ctx, item, i)
		if err != nil {
			return nil, fmt.Errorf("failed processing item %d: %w", i, err)
		}
		results[i] = output
	}

	return results, nil
}

// processParallel processes items concurrently with worker pool (fail-fast)
func (it *Iterator) processParallel(ctx context.Context, items []interface{}, processFn ProcessFunc) ([]interface{}, error) {
	numItems := len(items)
	results := make([]interface{}, numItems)

	// Create worker pool
	numWorkers := it.config.MaxConcurrent
	if numWorkers > numItems {
		numWorkers = numItems
	}

	// Use channels for work distribution and error signaling
	workCh := make(chan int, numItems)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstError error

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
					output, err := processFn(ctx, items[idx], idx)

					mu.Lock()
					if err != nil {
						if firstError == nil {
							firstError = fmt.Errorf("failed processing item %d: %w", idx, err)
							cancel() // Signal other workers to stop
						}
					} else {
						results[idx] = output
					}
					mu.Unlock()
				}
			}
		}()
	}

	// Send work to workers
sendLoop:
	for i := 0; i < numItems; i++ {
		select {
		case <-ctx.Done():
			break sendLoop
		case workCh <- i:
		}
	}
	close(workCh)

	wg.Wait()

	if firstError != nil {
		return nil, firstError
	}

	return results, nil
}
