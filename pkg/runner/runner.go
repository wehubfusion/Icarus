// Package runner provides a concurrent message processing framework using NATS JetStream.
// It allows processing messages from a stream with configurable batch sizes and worker pools,
// with built-in success and error reporting capabilities.
package runner

import (
	"context"
	"fmt"
	"log"

	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// Processor defines the interface for message processing implementations.
// Implementations should handle the business logic for processing individual messages.
type Processor interface {
	Process(ctx context.Context, msg *message.Message) error
}

// Runner manages concurrent message processing from a NATS JetStream consumer.
// It pulls messages in batches and distributes them to worker goroutines for processing,
// with automatic success and error reporting to the "result" subject.
type Runner struct {
	client     *client.Client
	processor  Processor
	stream     string
	consumer   string
	batchSize  int
	numWorkers int
}

// NewRunner creates a new Runner instance with a connected client and stream/consumer configuration.
// The client must already be connected before creating the runner.
// batchSize specifies how many messages to pull at once from the stream.
// numWorkers specifies the number of worker goroutines for processing messages.
func NewRunner(client *client.Client, stream, consumer string, batchSize int, numWorkers int) *Runner {
	return &Runner{
		client:     client,
		stream:     stream,
		consumer:   consumer,
		batchSize:  batchSize,
		numWorkers: numWorkers,
	}
}

// RegisterProcessor sets the custom Processor implementation for handling messages.
// The processor must be registered before calling Run().
func (r *Runner) RegisterProcessor(p Processor) {
	r.processor = p
}

// Run starts the message processing pipeline.
// It spawns worker goroutines and begins pulling messages from the configured stream.
// The method blocks until the context is cancelled or an error occurs.
// Returns an error if no processor is registered.
func (r *Runner) Run(ctx context.Context) error {
	if r.processor == nil {
		return fmt.Errorf("no processor registered")
	}

	// Create message channel with buffer size equal to batch size
	messageChan := make(chan *message.Message, r.batchSize)

	// Start worker goroutines
	for i := 0; i < r.numWorkers; i++ {
		go r.worker(ctx, messageChan)
	}

	// Start message puller goroutine
	go func() {
		defer close(messageChan) // Close channel when done

		for {
			select {
			case <-ctx.Done():
				log.Println("Shutting down message processor...")
				return
			default:
				// Pull messages from the stream
				messages, err := r.client.Messages.PullMessages(ctx, r.stream, r.consumer, r.batchSize)
				if err != nil {
					log.Printf("Error pulling messages: %v", err)
					continue
				}

				if len(messages) == 0 {
					// No messages available, wait a bit before trying again
					continue
				}

				// Send messages to workers
				for _, msg := range messages {
					select {
					case messageChan <- msg:
						// Message sent successfully
					case <-ctx.Done():
						return // Context cancelled, stop sending messages
					}
				}
			}
		}
	}()

	return nil
}

// worker processes messages from the channel
func (r *Runner) worker(ctx context.Context, messageChan <-chan *message.Message) {
	for {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				// Channel closed, worker should exit
				return
			}
			r.processMessage(ctx, msg)
		case <-ctx.Done():
			return
		}
	}
}

// processMessage handles the actual message processing logic
func (r *Runner) processMessage(ctx context.Context, msg *message.Message) {
	// Extract workflow information for reporting
	var workflowID, runID string
	if msg.Workflow != nil {
		workflowID = msg.Workflow.WorkflowID
		runID = msg.Workflow.RunID
	}

	// Process the message
	if err := r.processor.Process(ctx, msg); err != nil {
		log.Printf("Error processing message: %v", err)
		// Report error if we have workflow information
		if workflowID != "" && runID != "" {
			if reportErr := r.client.Messages.ReportError(ctx, workflowID, runID, err.Error()); reportErr != nil {
				log.Printf("Error reporting failure: %v", reportErr)
			}
		}
	} else {
		log.Printf("Successfully processed message for workflow %s run %s", workflowID, runID)
		// Report success if we have workflow information
		if workflowID != "" && runID != "" {
			if reportErr := r.client.Messages.ReportSuccess(ctx, workflowID, runID, "Message processed successfully"); reportErr != nil {
				log.Printf("Error reporting success: %v", reportErr)
			}
		}
	}
}
