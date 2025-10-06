package runner

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// Processor defines the interface for processing messages.
// Implementations should handle the message processing logic and return an error
// if processing fails. The context can be used for cancellation and timeout control.
type Processor interface {
	// Process handles a single message and returns an error if processing fails.
	// The context should be respected for cancellation and timeout handling.
	Process(ctx context.Context, msg message.Message) error
}

// Runner manages the execution of message processing, supporting both sequential
// and concurrent processing modes. It provides a clean abstraction for plugin-based
// message processing systems with proper error handling and graceful shutdown.
//
// Example usage:
//
//	processor := &MyProcessor{}
//	runner := NewRunner(processor, 5) // 5 concurrent workers
//	err := runner.Run(ctx, msgsChan)
type Runner struct {
	processor    Processor
	numOfWorkers int
}

// NewRunner creates a new Runner instance with the specified processor and worker count.
// If numOfWorkers is 0, messages are processed sequentially.
// If numOfWorkers > 0, messages are processed concurrently using a worker pool.
//
// Parameters:
//   - processor: The message processor implementation
//   - numOfWorkers: Number of concurrent workers (0 for sequential processing)
//
// Returns:
//   - A new Runner instance ready for use
func NewRunner(processor Processor, numOfWorkers int) *Runner {
	return &Runner{
		processor:    processor,
		numOfWorkers: numOfWorkers,
	}
}

// Run starts processing messages from the provided channel.
// It blocks until all messages are processed or the context is cancelled.
// The method ensures proper cleanup of all workers and respects context cancellation.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - msgs: Channel of messages to process (message.Message type)
//
// Returns:
//   - error: Any error encountered during processing (nil if successful)
//
// Behavior:
//   - If numOfWorkers == 0: Processes messages sequentially
//   - If numOfWorkers > 0: Processes messages concurrently using a worker pool
//   - Logs errors but continues processing other messages
//   - Waits for all workers to complete before returning
//   - Returns immediately if context is cancelled
func (r *Runner) Run(ctx context.Context, msgs <-chan message.Message) error {
	if r.numOfWorkers == 0 {
		return r.runSequential(ctx, msgs)
	}
	return r.runConcurrent(ctx, msgs)
}

// runSequential processes messages one at a time in the calling goroutine.
// This is suitable for CPU-bound processing or when order preservation is critical.
func (r *Runner) runSequential(ctx context.Context, msgs <-chan message.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				// Channel closed, processing complete
				return nil
			}

			if err := r.processor.Process(ctx, msg); err != nil {
				log.Printf("Error processing message: %v", err)
				// Continue processing other messages
			}
		}
	}
}

// runConcurrent processes messages using a worker pool for parallel processing.
// This is suitable for I/O-bound processing or when throughput is prioritized over order.
func (r *Runner) runConcurrent(ctx context.Context, msgs <-chan message.Message) error {
	var wg sync.WaitGroup
	msgsChan := make(chan message.Message, r.numOfWorkers) // Buffered channel to prevent blocking

	// Start worker goroutines
	for i := 0; i < r.numOfWorkers; i++ {
		wg.Add(1)
		go r.worker(ctx, msgsChan, &wg)
	}

	// Distribute messages to workers
	defer func() {
		close(msgsChan) // Signal workers to stop
		wg.Wait()       // Wait for all workers to complete
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				// Input channel closed, stop distributing
				return nil
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case msgsChan <- msg:
				// Message sent to worker
			}
		}
	}
}

// worker is the goroutine function that processes messages from the worker channel.
// Each worker processes messages until the channel is closed or context is cancelled.
func (r *Runner) worker(ctx context.Context, msgs <-chan message.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				// Channel closed, worker can exit
				return
			}

			if err := r.processor.Process(ctx, msg); err != nil {
				log.Printf("Error processing message: %v", err)
				// Continue processing other messages
			}
		}
	}
}

// ProcessorFunc is a convenience type that allows using functions as processors.
// This enables functional programming patterns for simple processors.
//
// Example:
//
//	processor := runner.ProcessorFunc(func(ctx context.Context, msg message.Message) error {
//	    fmt.Printf("Processing: %v\n", msg)
//	    return nil
//	})
//	runner := runner.NewRunner(processor, 0)
type ProcessorFunc func(ctx context.Context, msg message.Message) error

// Process implements the Processor interface for ProcessorFunc.
func (pf ProcessorFunc) Process(ctx context.Context, msg message.Message) error {
	return pf(ctx, msg)
}

// Chain combines multiple processors into a single processor that executes them in sequence.
// If any processor returns an error, the chain stops and returns that error.
// This is useful for composing middleware-like behavior.
//
// Example:
//
//	processor := runner.Chain(
//	    runner.ProcessorFunc(loggingProcessor),
//	    runner.ProcessorFunc(validationProcessor),
//	    runner.ProcessorFunc(businessLogicProcessor),
//	)
//	runner := runner.NewRunner(processor, 5)
func Chain(processors ...Processor) Processor {
	return ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		for _, processor := range processors {
			if err := processor.Process(ctx, msg); err != nil {
				return err
			}
		}
		return nil
	})
}

// ValidateConfig validates the runner configuration.
// Returns an error if the configuration is invalid.
func (r *Runner) ValidateConfig() error {
	if r.processor == nil {
		return fmt.Errorf("processor cannot be nil")
	}
	if r.numOfWorkers < 0 {
		return fmt.Errorf("numOfWorkers cannot be negative, got %d", r.numOfWorkers)
	}
	return nil
}
