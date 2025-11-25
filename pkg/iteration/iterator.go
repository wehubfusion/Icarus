package iteration

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
)

// Iterator handles array iteration with configurable execution strategy
type Iterator struct {
	config  Config
	limiter *concurrency.Limiter
}

// NewIterator creates a new iterator with given config
func NewIterator(config Config) *Iterator {
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = runtime.GOMAXPROCS(0) // Respects cgroup limits in containers
	}
	return &Iterator{
		config:  config,
		limiter: nil, // No limiter by default
	}
}

// NewIteratorWithLimiter creates a new iterator with concurrency limiter
func NewIteratorWithLimiter(config Config, limiter *concurrency.Limiter) *Iterator {
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = runtime.GOMAXPROCS(0) // Respects cgroup limits in containers
	}
	return &Iterator{
		config:  config,
		limiter: limiter,
	}
}

// Process iterates over array items and processes each with the given function concurrently
// Returns array of results or error (fail-fast on first error)
func (it *Iterator) Process(ctx context.Context, items []interface{}, processFn ProcessFunc) ([]interface{}, error) {
	if len(items) == 0 {
		return []interface{}{}, nil
	}

	return it.processParallel(ctx, items, processFn)
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

	// Start workers with limiter support
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)

		// Use limiter if available
		if it.limiter != nil {
			err := it.limiter.Go(ctx, func() error {
				defer wg.Done()
				for idx := range workCh {
					select {
					case <-ctx.Done():
						return ctx.Err()
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
				return nil
			})

			// If limiter fails to acquire
			if err != nil {
				wg.Done() // Balance the Add(1) above
				mu.Lock()
				if firstError == nil {
					firstError = fmt.Errorf("limiter error: %w", err)
					cancel()
				}
				mu.Unlock()
			}
		} else {
			// No limiter, spawn goroutine directly
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
