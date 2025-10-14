// Package runner provides a concurrent message processing framework using NATS JetStream.
// It allows processing messages from a stream with configurable batch sizes and worker pools,
// with built-in success and error reporting capabilities.
package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Processor defines the interface for message processing implementations.
// Implementations should handle the business logic for processing individual messages.
type Processor interface {
	Process(ctx context.Context, msg *message.Message) (message message.Message, err error)
}

// Runner manages concurrent message processing from a NATS JetStream consumer.
// It pulls messages in batches and distributes them to worker goroutines for processing,
// with automatic success and error reporting to the "result" subject.
type Runner struct {
	client          *client.Client
	processor       Processor
	stream          string
	consumer        string
	batchSize       int
	numWorkers      int
	logger          *zap.Logger
	processTimeout  time.Duration
	tracer          trace.Tracer
	tracingShutdown func(context.Context) error
}

// NewRunner creates a new Runner instance with a connected client and stream/consumer configuration.
// The client must already be connected before creating the runner.
// The processor must implement the Processor interface for message handling.
// batchSize specifies how many messages to pull at once from the stream.
// numWorkers specifies the number of worker goroutines for processing messages.
// processTimeout specifies the maximum time allowed for processing a single message.
// logger is the zap logger instance for structured logging.
// tracingConfig is optional - if nil, no tracing will be set up. If provided, tracing will be automatically configured and cleaned up.
// Returns an error if any of the parameters are invalid.
func NewRunner(client *client.Client, processor Processor, stream, consumer string, batchSize int, numWorkers int, processTimeout time.Duration, logger *zap.Logger, tracingConfig *tracing.TracingConfig) (*Runner, error) {
	if client == nil {
		return nil, errors.New("client cannot be nil")
	}
	if processor == nil {
		return nil, errors.New("processor cannot be nil")
	}
	if stream == "" {
		return nil, errors.New("stream name cannot be empty")
	}
	if consumer == "" {
		return nil, errors.New("consumer name cannot be empty")
	}
	if batchSize <= 0 {
		return nil, errors.New("batchSize must be greater than 0")
	}
	if numWorkers <= 0 {
		return nil, errors.New("numWorkers must be greater than 0")
	}
	if processTimeout <= 0 {
		return nil, errors.New("processTimeout must be greater than 0")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	// Ensure the stream exists, create it if necessary
	js := client.JetStream()
	if js == nil {
		return nil, errors.New("JetStream context is not available")
	}

	if err := ensureStream(js, stream, consumer, logger); err != nil {
		return nil, fmt.Errorf("failed to ensure stream '%s' exists: %w", stream, err)
	}

	runner := &Runner{
		client:         client,
		processor:      processor,
		stream:         stream,
		consumer:       consumer,
		batchSize:      batchSize,
		numWorkers:     numWorkers,
		processTimeout: processTimeout,
		logger:         logger,
		tracer:         otel.Tracer("icarus/runner"),
	}

	// Setup tracing if configuration is provided
	if tracingConfig != nil {
		ctx := context.Background()
		shutdown, err := tracing.SetupTracing(ctx, *tracingConfig, logger)
		if err != nil {
			logger.Warn("Failed to setup tracing, continuing without tracing", zap.Error(err))
		} else {
			runner.tracingShutdown = shutdown
			logger.Info("Tracing setup complete",
				zap.String("service", tracingConfig.ServiceName),
				zap.String("endpoint", tracingConfig.OTLPEndpoint))
		}
	}

	return runner, nil
}

// ensureStream creates the JetStream stream if it doesn't exist, or validates it exists
func ensureStream(js nats.JetStreamContext, streamName, consumerName string, logger *zap.Logger) error {
	// Try to get stream info first
	streamInfo, err := js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		if err == nats.ErrStreamNotFound {
			logger.Info("Creating JetStream stream", zap.String("stream", streamName))

			// Create stream configuration for HTTP requests
			// Using subject pattern that matches the consumer
			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.*", streamName)}, // Allow any subject under the stream name
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour, // Retain messages for 24 hours
				MaxMsgs:  100000,         // Max 100k messages (higher than Hermes for HTTP requests)
				Replicas: 1,
			}

			_, err = js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s': %w", streamName, err)
			}

			logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.Strings("subjects", streamConfig.Subjects),
				zap.Duration("max_age", streamConfig.MaxAge),
				zap.Int64("max_msgs", streamConfig.MaxMsgs))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	} else {
		// Stream exists, log its status
		logger.Info("JetStream stream already exists",
			zap.String("stream", streamName),
			zap.Uint64("messages", streamInfo.State.Msgs),
			zap.Int("consumers", streamInfo.State.Consumers))
	}

	return nil
}

// Close gracefully shuts down the runner and cleans up resources including tracing.
// This should be called when the runner is no longer needed.
func (r *Runner) Close() error {
	if r.tracingShutdown != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := r.tracingShutdown(ctx); err != nil {
			r.logger.Error("Error shutting down tracing", zap.Error(err))
			return err
		}
		r.logger.Info("Tracing shutdown complete")
	}
	return nil
}

// Run starts the message processing pipeline.
// It spawns worker goroutines and begins pulling messages from the configured stream.
// The method blocks until the context is cancelled and all workers have finished processing.
// Returns an error if there's a critical failure that prevents the runner from continuing.
func (r *Runner) Run(ctx context.Context) error {
	// Create message channel with buffer size equal to batch size
	messageChan := make(chan *message.Message, r.batchSize)

	// Use WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < r.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.worker(ctx, workerID, messageChan)
		}(i)
	}

	// Start message puller goroutine
	go func() {
		defer close(messageChan) // Close channel when done

		backoffDelay := 100 * time.Millisecond
		maxBackoff := 5 * time.Second

		for {
			select {
			case <-ctx.Done():
				r.logger.Info("Shutting down message processor...")
				return
			default:
				// Pull messages from the stream
				messages, err := r.client.Messages.PullMessages(ctx, r.stream, r.consumer, r.batchSize)
				if err != nil {
					// Check if this is due to context cancellation (graceful shutdown)
					if ctx.Err() != nil {
						r.logger.Debug("Message pulling stopped due to context cancellation")
						return // Context cancelled
					}
					// This is an actual error, not graceful shutdown
					r.logger.Error("Error pulling messages", zap.Error(err))
					// Exponential backoff for errors
					time.Sleep(backoffDelay)
					if backoffDelay < maxBackoff {
						backoffDelay *= 2
					}
					continue
				}

				if len(messages) == 0 {
					// No messages available, use shorter backoff to avoid busy waiting
					// but don't reset backoff completely as this is normal behavior
					select {
					case <-time.After(500 * time.Millisecond):
					case <-ctx.Done():
						return
					}
					continue
				}

				// Reset backoff on successful pull
				backoffDelay = 100 * time.Millisecond

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

	// Wait for all goroutines to finish or context cancellation
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	// Wait for completion or context cancellation
	select {
	case <-done:
		r.logger.Info("Runner completed successfully")
		return nil
	case <-ctx.Done():
		r.logger.Info("Runner stopped due to context cancellation")
		return ctx.Err()
	}
}

// worker processes messages from the channel
func (r *Runner) worker(ctx context.Context, workerID int, messageChan <-chan *message.Message) {
	r.logger.Info("Worker started", zap.Int("workerID", workerID))
	defer r.logger.Info("Worker stopped", zap.Int("workerID", workerID))

	for {
		select {
		case msg, ok := <-messageChan:
			if !ok {
				// Channel closed, worker should exit
				return
			}
			r.processMessage(ctx, workerID, msg)
		case <-ctx.Done():
			return
		}
	}
}

// processMessage handles the actual message processing logic
func (r *Runner) processMessage(ctx context.Context, workerID int, msg *message.Message) {
	// Extract workflow information for reporting
	var workflowID, runID string
	if msg.Workflow != nil {
		workflowID = msg.Workflow.WorkflowID
		runID = msg.Workflow.RunID
	}

	// Start tracing span for message processing
	ctx, span := r.tracer.Start(ctx, "runner.processMessage",
		trace.WithAttributes(
			attribute.Int("worker.id", workerID),
			attribute.String("workflow.id", workflowID),
			attribute.String("workflow.run_id", runID),
			attribute.String("stream", r.stream),
			attribute.String("consumer", r.consumer),
		))
	defer span.End()

	// Create a timeout context for message processing that respects both the main context and timeout
	processCtx, cancel := context.WithTimeout(ctx, r.processTimeout)
	defer cancel()

	// Check if the main context is already cancelled before starting processing
	select {
	case <-ctx.Done():
		r.logger.Info("Skipping message processing due to context cancellation",
			zap.Int("workerID", workerID),
			zap.String("workflowID", workflowID),
			zap.String("runID", runID))
		span.SetStatus(codes.Error, "Context cancelled before processing")
		return
	default:
		// Continue with processing
	}

	start := time.Now()
	r.logger.Info("Worker processing message",
		zap.Int("workerID", workerID),
		zap.String("workflowID", workflowID),
		zap.String("runID", runID))

	// Start nested span for processor.Process call
	processCtx, processSpan := r.tracer.Start(processCtx, "processor.Process")
	// Add message attributes if available
	if msg.Node != nil {
		processSpan.SetAttributes(attribute.String("message.node.id", msg.Node.NodeID))
	}
	if msg.Payload != nil {
		processSpan.SetAttributes(
			attribute.String("message.payload.source", msg.Payload.Source),
			attribute.String("message.payload.reference", msg.Payload.Reference),
		)
	}
	processSpan.SetAttributes(attribute.String("message.created_at", msg.CreatedAt))
	defer processSpan.End()

	// Process the message
	resultMessage, processErr := r.processor.Process(processCtx, msg)
	processingTime := time.Since(start)

	// Add processing time to spans
	span.SetAttributes(attribute.Int64("processing.duration_ms", processingTime.Milliseconds()))
	processSpan.SetAttributes(attribute.Int64("processing.duration_ms", processingTime.Milliseconds()))

	if processErr != nil {
		// Record error in spans
		span.RecordError(processErr)
		span.SetStatus(codes.Error, processErr.Error())
		processSpan.RecordError(processErr)
		processSpan.SetStatus(codes.Error, processErr.Error())

		r.logger.Error("Error processing message",
			zap.Int("workerID", workerID),
			zap.Duration("processingTime", processingTime),
			zap.String("workflowID", workflowID),
			zap.String("runID", runID),
			zap.Error(processErr))

		// Report error if we have workflow information
		// Use a background context with timeout to ensure reporting works even if parent context is cancelled
		if workflowID != "" && runID != "" {
			reportCtx, reportCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if reportErr := r.client.Messages.ReportError(reportCtx, workflowID, runID, processErr.Error(), msg.GetNATSMsg()); reportErr != nil {
				r.logger.Error("Error reporting failure",
					zap.Int("workerID", workerID),
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.Error(reportErr))
			}
			reportCancel()
		} else {
			// If we don't have workflow info, still nak the message since processing failed
			if nakErr := msg.Nak(); nakErr != nil {
				r.logger.Error("Error naking message after processing failure",
					zap.Int("workerID", workerID),
					zap.Error(nakErr))
			}
		}

		return
	}

	// Mark spans as successful
	span.SetStatus(codes.Ok, "Message processed successfully")
	processSpan.SetStatus(codes.Ok, "Message processed successfully")
	// Add result message attributes if available
	if resultMessage.Node != nil {
		span.SetAttributes(attribute.String("result.message.node.id", resultMessage.Node.NodeID))
	}
	if resultMessage.Output != nil {
		span.SetAttributes(attribute.String("result.message.output.destination_type", resultMessage.Output.DestinationType))
	}

	r.logger.Info("Successfully processed message",
		zap.Int("workerID", workerID),
		zap.String("workflowID", workflowID),
		zap.String("runID", runID),
		zap.Duration("processingTime", processingTime),
		zap.Any("resultMessage", resultMessage)) // Log the result message

	// Report success if we have workflow information
	if workflowID != "" && runID != "" {
		reportCtx, reportCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if reportErr := r.client.Messages.ReportSuccess(reportCtx, resultMessage, msg.GetNATSMsg()); reportErr != nil {
			r.logger.Error("Error reporting success",
				zap.Int("workerID", workerID),
				zap.String("workflowID", workflowID),
				zap.String("runID", runID),
				zap.Error(reportErr))
		}
		reportCancel()
	} else {
		// If we don't have workflow info, still ack the message since processing succeeded
		if ackErr := msg.Ack(); ackErr != nil {
			r.logger.Error("Error acking message after successful processing",
				zap.Int("workerID", workerID),
				zap.Error(ackErr))
		}
	}

}
