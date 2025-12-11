package runtime

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
)

// WorkerPool manages concurrent processing of array items.
// It integrates with the concurrency package's Limiter for rate limiting.
type WorkerPool struct {
	config     WorkerPoolConfig
	limiter    *concurrency.Limiter
	jobChan    chan workerJob
	resultChan chan BatchResult
	wg         sync.WaitGroup
	processor  ItemProcessor
	logger     Logger

	// Metrics
	processed atomic.Int64
	errors    atomic.Int64
}

// workerJob represents a job to be processed by a worker.
type workerJob struct {
	item BatchItem
	ctx  context.Context
}

// NewWorkerPool creates a new worker pool.
// If limiter is nil and config.UseLimiter is true, it will try to use a default limiter.
func NewWorkerPool(config WorkerPoolConfig, processor ItemProcessor, limiter *concurrency.Limiter, logger Logger) *WorkerPool {
	config.Validate()

	// Determine number of workers
	numWorkers := config.NumWorkers
	if numWorkers <= 0 {
		if limiter != nil {
			numWorkers = int(limiter.CurrentActive()) + 1 // Use limiter as guide
			if numWorkers < 1 {
				numWorkers = runtime.NumCPU()
			}
		} else {
			numWorkers = runtime.NumCPU()
		}
	}
	config.NumWorkers = numWorkers

	if logger == nil {
		logger = &NoOpLogger{}
	}

	return &WorkerPool{
		config:     config,
		limiter:    limiter,
		jobChan:    make(chan workerJob, config.BufferSize),
		resultChan: make(chan BatchResult, config.BufferSize),
		processor:  processor,
		logger:     logger,
	}
}

// Start starts the worker pool with the specified number of workers.
func (wp *WorkerPool) Start(ctx context.Context) {
	wp.logger.Debug("starting worker pool",
		Field{Key: "workers", Value: wp.config.NumWorkers},
		Field{Key: "buffer_size", Value: wp.config.BufferSize},
	)

	for i := 0; i < wp.config.NumWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

// worker is a single worker goroutine.
func (wp *WorkerPool) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	wp.logger.Debug("worker started", Field{Key: "worker_id", Value: id})

	for {
		select {
		case <-ctx.Done():
			wp.logger.Debug("worker stopping due to context cancellation",
				Field{Key: "worker_id", Value: id},
			)
			return

		case job, ok := <-wp.jobChan:
			if !ok {
				wp.logger.Debug("worker stopping, job channel closed",
					Field{Key: "worker_id", Value: id},
				)
				return
			}
			wp.processJob(job, id)
		}
	}
}

// processJob processes a single job.
func (wp *WorkerPool) processJob(job workerJob, workerId int) {
	// Acquire from limiter if configured
	if wp.config.UseLimiter && wp.limiter != nil {
		if err := wp.limiter.Acquire(job.ctx); err != nil {
			wp.errors.Add(1)
			wp.resultChan <- BatchResult{
				Index: job.item.Index,
				Error: err,
			}
			return
		}
		defer wp.limiter.Release()
	}

	// Process the item
	result := wp.processor.ProcessItem(job.ctx, job.item)

	// Track metrics
	if result.Error != nil {
		wp.errors.Add(1)
	} else {
		wp.processed.Add(1)
	}

	// Record to circuit breaker if available
	if wp.config.UseCircuitBreaker && wp.limiter != nil {
		// The limiter's Go/GoSync methods handle circuit breaker recording
		// For direct processing, we could add a method to record success/failure
	}

	wp.resultChan <- result
}

// Submit submits a single item for processing.
func (wp *WorkerPool) Submit(ctx context.Context, item BatchItem) {
	select {
	case <-ctx.Done():
		wp.resultChan <- BatchResult{
			Index: item.Index,
			Error: ctx.Err(),
		}
	case wp.jobChan <- workerJob{item: item, ctx: ctx}:
	}
}

// SubmitBatch submits multiple items for processing.
func (wp *WorkerPool) SubmitBatch(ctx context.Context, items []BatchItem) {
	for _, item := range items {
		select {
		case <-ctx.Done():
			// Submit remaining as cancelled
			for _, remaining := range items {
				if remaining.Index >= item.Index {
					wp.resultChan <- BatchResult{
						Index: remaining.Index,
						Error: ctx.Err(),
					}
				}
			}
			return
		default:
			wp.Submit(ctx, item)
		}
	}
}

// SubmitAll submits all items and then closes the job channel.
// This should be called in a goroutine.
func (wp *WorkerPool) SubmitAll(ctx context.Context, items []BatchItem) {
	wp.SubmitBatch(ctx, items)
	close(wp.jobChan)
}

// Results returns the results channel.
func (wp *WorkerPool) Results() <-chan BatchResult {
	return wp.resultChan
}

// Wait waits for all workers to finish and closes the result channel.
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
	close(wp.resultChan)
}

// Close closes the job channel and waits for workers to finish.
// The result channel will be closed after all workers complete.
func (wp *WorkerPool) Close() {
	close(wp.jobChan)
	wp.Wait()
}

// Stats returns the current processing statistics.
func (wp *WorkerPool) Stats() (processed, errors int64) {
	return wp.processed.Load(), wp.errors.Load()
}

// Config returns the worker pool configuration.
func (wp *WorkerPool) Config() WorkerPoolConfig {
	return wp.config
}

// CreateBatchItems converts raw map items to BatchItems with indices.
func CreateBatchItems(items []map[string]interface{}) []BatchItem {
	batchItems := make([]BatchItem, len(items))
	for i, item := range items {
		batchItems[i] = BatchItem{
			Index: i,
			Data:  item,
		}
	}
	return batchItems
}

// CreateBatches groups items into batches of the specified size.
func CreateBatches(items []map[string]interface{}, batchSize int) [][]BatchItem {
	if batchSize <= 0 {
		batchSize = 10
	}

	numBatches := (len(items) + batchSize - 1) / batchSize
	batches := make([][]BatchItem, 0, numBatches)

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := make([]BatchItem, 0, end-i)
		for j := i; j < end; j++ {
			batch = append(batch, BatchItem{
				Index: j,
				Data:  items[j],
			})
		}

		batches = append(batches, batch)
	}

	return batches
}

// CollectResults collects all results from the channel into an ordered slice.
// Results are ordered by their original index.
// CollectResults collects all results from the channel into an ordered slice.
// It returns per-item outputs and per-item iteration Items collected from BatchResult.Items.
func CollectResults(resultChan <-chan BatchResult, count int) ([]map[string]interface{}, [][]map[string]interface{}, error) {
	results := make([]map[string]interface{}, count)
	items := make([][]map[string]interface{}, count)
	var firstError error

	received := 0
	for result := range resultChan {
		if result.Error != nil && firstError == nil {
			firstError = result.Error
		}
		if result.Index >= 0 && result.Index < count {
			if result.Output != nil {
				results[result.Index] = result.Output
			}
			if len(result.Items) > 0 {
				items[result.Index] = result.Items
			}
		}
		received++
		if received >= count {
			break
		}
	}

	return results, items, firstError
}

// CollectResultsAll collects all results, returning all errors encountered.
func CollectResultsAll(resultChan <-chan BatchResult, count int) ([]map[string]interface{}, []error) {
	results := make([]map[string]interface{}, count)
	var errors []error

	received := 0
	for result := range resultChan {
		if result.Error != nil {
			errors = append(errors, result.Error)
		}
		if result.Index >= 0 && result.Index < count && result.Output != nil {
			results[result.Index] = result.Output
		}
		received++
		if received >= count {
			break
		}
	}

	return results, errors
}
