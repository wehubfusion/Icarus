// Package runner provides a concurrent message processing framework using NATS JetStream.
// It allows processing messages from a stream with configurable batch sizes and limiter-driven concurrency,
// with built-in success and error reporting capabilities.
package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	internaltracing "github.com/wehubfusion/Icarus/internal/tracing"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/concurrency"
	"github.com/wehubfusion/Icarus/pkg/message"
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
// It pulls messages in batches and dispatches them through the concurrency limiter for processing,
// with automatic success and error reporting to the "result" subject.
type Runner struct {
	client          *client.Client
	processor       Processor
	stream          string
	consumer        string
	batchSize       int
	logger          *zap.Logger
	processTimeout  time.Duration
	tracer          trace.Tracer
	tracingShutdown func(context.Context) error
	limiter         *concurrency.Limiter
}

// NewRunner creates a new Runner instance with a connected client and stream/consumer configuration.
// The client must already be connected before creating the runner.
// The processor must implement the Processor interface for message handling.
// batchSize specifies how many messages to pull at once from the stream.
// processTimeout specifies the maximum time allowed for processing a single message.
// logger is the zap logger instance for structured logging.
// tracingConfig is optional - if nil, no tracing will be set up. If provided, tracing will be automatically configured and cleaned up.
// limiter is optional - if nil, no concurrency limiting will be applied beyond the pull loop.
// Returns an error if any of the parameters are invalid.
func NewRunner(client *client.Client, processor Processor, stream, consumer string, batchSize int, processTimeout time.Duration, logger *zap.Logger, tracingConfig *TracingConfig, limiter *concurrency.Limiter) (*Runner, error) {
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
	if processTimeout <= 0 {
		return nil, errors.New("processTimeout must be greater than 0")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	// Ensure the stream and consumer exist, create them if necessary
	if err := client.Messages.EnsureStream(stream); err != nil {
		return nil, fmt.Errorf("failed to ensure stream '%s' exists: %w", stream, err)
	}

	if err := client.Messages.EnsureConsumer(stream, consumer); err != nil {
		return nil, fmt.Errorf("failed to ensure consumer '%s' exists: %w", consumer, err)
	}

	runner := &Runner{
		client:         client,
		processor:      processor,
		stream:         stream,
		consumer:       consumer,
		batchSize:      batchSize,
		processTimeout: processTimeout,
		logger:         logger,
		tracer:         otel.Tracer("icarus/runner"),
		limiter:        limiter,
	}

	// Setup tracing if configuration is provided
	if tracingConfig != nil {
		ctx := context.Background()
		internalCfg := tracingConfig.toInternalConfig()
		shutdown, err := internaltracing.SetupTracing(ctx, internalCfg, logger)
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
// It pulls messages from the configured stream and processes them through the limiter.
// The method blocks until the context is cancelled and all processing goroutines have finished.
// Returns an error if there's a critical failure that prevents the runner from continuing.
func (r *Runner) Run(ctx context.Context) error {
	var (
		backgroundWG sync.WaitGroup
		processWG    sync.WaitGroup
	)

	// Start concurrency stats logging if limiter is available
	if r.limiter != nil {
		backgroundWG.Add(1)
		go func() {
			defer backgroundWG.Done()
			r.logConcurrencyStats(ctx)
		}()
	}

	dispatchMessage := func(msg *message.Message) {
		if r.limiter == nil {
			if err := r.processMessage(ctx, msg); err != nil {
				r.logger.Error("Message processing failed without limiter",
					zap.Error(err))
			}
			return
		}

		currentMsg := msg
		processWG.Add(1)
		err := r.limiter.Go(ctx, func() error {
			defer processWG.Done()
			return r.processMessage(ctx, currentMsg)
		})
		if err != nil {
			processWG.Done()
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if errors.Is(err, concurrency.ErrCircuitOpen) {
				r.logger.Warn("Limiter circuit open; applying backpressure")
			} else {
				r.logger.Error("Limiter rejected message", zap.Error(err))
			}
			if nakErr := msg.Nak(); nakErr != nil {
				r.logger.Error("Failed to Nak message after limiter error", zap.Error(nakErr))
			}
		}
	}

	// Start message puller goroutine
	backgroundWG.Add(1)
	go func() {
		defer backgroundWG.Done()

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

				// Dispatch messages via limiter
				for _, msg := range messages {
					select {
					case <-ctx.Done():
						return // Context cancelled, stop sending messages
					default:
						dispatchMessage(msg)
					}
				}
			}
		}
	}()

	// Wait for all goroutines to finish or context cancellation
	done := make(chan struct{})
	go func() {
		defer close(done)
		backgroundWG.Wait()
		processWG.Wait()
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

// processMessage handles the actual message processing logic.
// It returns an error when message processing or result reporting fails.
func (r *Runner) processMessage(ctx context.Context, msg *message.Message) error {
	// Extract workflow information for reporting
	var workflowID, runID, correlationID string
	if msg.Workflow != nil {
		workflowID = msg.Workflow.WorkflowID
		runID = msg.Workflow.RunID
	}
	correlationID = msg.CorrelationID

	// Generate correlation ID if not present
	if correlationID == "" && workflowID != "" && runID != "" {
		correlationID = fmt.Sprintf("%s-%s", workflowID, runID)
		msg.CorrelationID = correlationID
	}

	// Start tracing span for message processing
	ctx, span := r.tracer.Start(ctx, "runner.processMessage",
		trace.WithAttributes(
			attribute.String("workflow.id", workflowID),
			attribute.String("workflow.run_id", runID),
			attribute.String("correlation.id", correlationID),
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
			zap.String("workflowID", workflowID),
			zap.String("runID", runID),
			zap.String("correlationID", correlationID))
		span.SetStatus(codes.Error, "Context cancelled before processing")
		return ctx.Err()
	default:
		// Continue with processing
	}

	start := time.Now()
	r.logger.Info("Processing message",
		zap.String("workflowID", workflowID),
		zap.String("runID", runID),
		zap.String("correlationID", correlationID))

	// Start nested span for processor.Process call
	processCtx, processSpan := r.tracer.Start(processCtx, "processor.Process")
	// Add message attributes if available
	if correlationID != "" {
		processSpan.SetAttributes(attribute.String("correlation.id", correlationID))
	}
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
			zap.Duration("processingTime", processingTime),
			zap.String("workflowID", workflowID),
			zap.String("runID", runID),
			zap.String("correlationID", correlationID),
			zap.Error(processErr))

		// Report error if we have workflow information
		// Use a background context with timeout to ensure reporting works even if parent context is cancelled
		if workflowID != "" && runID != "" {
			// Extract executionID from message metadata
			executionID := ""
			if msg.Metadata != nil {
				executionID = msg.Metadata["execution_id"]
			}

			reportCtx, reportCancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer reportCancel()

			if reportErr := r.client.Messages.ReportError(reportCtx, executionID, workflowID, runID, processErr, msg.GetNATSMsg()); reportErr != nil {
				// Critical: If we can't report the error, log it extensively but don't fail silently
				r.logger.Error("CRITICAL: Failed to report error to Temporal workflow - workflow may hang",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.String("executionID", executionID),
					zap.String("originalError", processErr.Error()),
					zap.Error(reportErr))

				// Try one more time with a fresh context after a brief delay
				time.Sleep(2 * time.Second)
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if retryErr := r.client.Messages.ReportError(retryCtx, executionID, workflowID, runID, processErr, msg.GetNATSMsg()); retryErr != nil {
					r.logger.Error("CRITICAL: Retry also failed to report error to Temporal",
						zap.String("workflowID", workflowID),
						zap.String("runID", runID),
						zap.String("executionID", executionID),
						zap.Error(retryErr))
				} else {
					r.logger.Info("Successfully reported error to Temporal on retry",
						zap.String("workflowID", workflowID),
						zap.String("executionID", executionID))
				}
				retryCancel()
			}
		} else {
			// If we don't have workflow info, still nak the message since processing failed
			if nakErr := msg.Nak(); nakErr != nil {
				r.logger.Error("Error naking message after processing failure",
					zap.Error(nakErr))
			}
		}

		return processErr
	}

	// Mark spans as successful
	span.SetStatus(codes.Ok, "Message processed successfully")
	processSpan.SetStatus(codes.Ok, "Message processed successfully")

	// Ensure result message has correlation ID
	if resultMessage.CorrelationID == "" && correlationID != "" {
		resultMessage.CorrelationID = correlationID
	}

	// Add result message attributes if available
	if resultMessage.CorrelationID != "" {
		span.SetAttributes(attribute.String("result.correlation.id", resultMessage.CorrelationID))
	}
	if resultMessage.Node != nil {
		span.SetAttributes(attribute.String("result.message.node.id", resultMessage.Node.NodeID))
	}
	if resultMessage.Output != nil {
		span.SetAttributes(attribute.String("result.message.output.destination_type", resultMessage.Output.DestinationType))
	}

	r.logger.Info("Successfully processed message",
		zap.String("workflowID", workflowID),
		zap.String("runID", runID),
		zap.String("correlationID", correlationID),
		zap.Duration("processingTime", processingTime),
		zap.Any("resultMessage", resultMessage)) // Log the result message

	// Report success if we have workflow information
	// Use a longer timeout for large blob uploads (10 minutes to handle very large files)
	if workflowID != "" && runID != "" {
		reportCtx, reportCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer reportCancel()
		if reportErr := r.client.Messages.ReportSuccess(reportCtx, resultMessage, msg.GetNATSMsg()); reportErr != nil {
			r.logger.Error("Error reporting success, will report as error to workflow",
				zap.String("workflowID", workflowID),
				zap.String("runID", runID),
				zap.Error(reportErr))

			// Extract executionID from resultMessage metadata
			executionID := ""
			if resultMessage.Metadata != nil {
				executionID = resultMessage.Metadata["execution_id"]
			}

			// Report the failure as an error to Temporal so the workflow knows about it
			// Use longer timeout (30s) to match ReportError's retry logic
			errorCtx, errorCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer errorCancel()

			if errorReportErr := r.client.Messages.ReportError(errorCtx, executionID, workflowID, runID, reportErr, msg.GetNATSMsg()); errorReportErr != nil {
				// Critical: If we can't report the error, log it extensively
				r.logger.Error("CRITICAL: Failed to report error to workflow after success report failed - workflow may hang",
					zap.String("workflowID", workflowID),
					zap.String("runID", runID),
					zap.String("executionID", executionID),
					zap.String("originalError", reportErr.Error()),
					zap.Error(errorReportErr))

				// Try one more time with a fresh context
				time.Sleep(2 * time.Second)
				retryCtx, retryCancel := context.WithTimeout(context.Background(), 30*time.Second)
				if retryErr := r.client.Messages.ReportError(retryCtx, executionID, workflowID, runID, reportErr, msg.GetNATSMsg()); retryErr != nil {
					r.logger.Error("CRITICAL: Retry also failed to report error to Temporal",
						zap.String("workflowID", workflowID),
						zap.String("executionID", executionID),
						zap.Error(retryErr))
				} else {
					r.logger.Info("Successfully reported error to Temporal on retry",
						zap.String("workflowID", workflowID),
						zap.String("executionID", executionID))
				}
				retryCancel()
			}
			return reportErr
		}
	} else {
		// If we don't have workflow info, still ack the message since processing succeeded
		if ackErr := msg.Ack(); ackErr != nil {
			r.logger.Error("Error acking message after successful processing",
				zap.Error(ackErr))
			return ackErr
		}
	}

	return nil
}

// logConcurrencyStats periodically logs concurrency metrics for observability
func (r *Runner) logConcurrencyStats(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if r.limiter == nil {
				return
			}

			metrics := r.limiter.GetMetrics()
			avgWait := r.limiter.GetAverageWaitTime()
			circuitState := r.limiter.GetCircuitBreakerState()

			r.logger.Info("Concurrency metrics",
				zap.Int64("active_goroutines", r.limiter.CurrentActive()),
				zap.Int64("peak_concurrent", metrics.PeakConcurrent),
				zap.Int64("total_acquired", metrics.TotalAcquired),
				zap.Int64("total_released", metrics.TotalReleased),
				zap.Duration("avg_wait_time", avgWait),
				zap.String("circuit_breaker", circuitState),
			)
		case <-ctx.Done():
			return
		}
	}
}
