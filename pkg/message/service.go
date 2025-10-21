package message

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"go.uber.org/zap"
)

// JSContext defines the minimal subset of JetStream operations the service depends on.
// This allows tests to provide a mock without requiring a running NATS server.
type JSContext interface {
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	PullSubscribe(subj, durable string, opts ...nats.SubOpt) (JSSubscription, error)
	StreamInfo(stream string) (*nats.StreamInfo, error)
	AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error)
	ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error)
	AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error)
}

// JSSubscription abstracts operations used by the SDK from a subscription.
// Implemented by the real nats.Subscription via adapter and by test doubles.
type JSSubscription interface {
	Unsubscribe() error
	Drain() error
	IsValid() bool
	Pending() (int, int, error)
	Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error)
}

// WrapNATSJetStream adapts a nats.JetStreamContext to the JSContext interface.
func WrapNATSJetStream(js nats.JetStreamContext) JSContext {
	return &natsJSAdapter{js: js}
}

type natsJSAdapter struct {
	js nats.JetStreamContext
}

func (a *natsJSAdapter) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return a.js.Publish(subj, data, opts...)
}

func (a *natsJSAdapter) PullSubscribe(subj, durable string, opts ...nats.SubOpt) (JSSubscription, error) {
	sub, err := a.js.PullSubscribe(subj, durable, opts...)
	if err != nil {
		return nil, err
	}
	return &natsSubAdapter{sub: sub}, nil
}

func (a *natsJSAdapter) StreamInfo(stream string) (*nats.StreamInfo, error) {
	return a.js.StreamInfo(stream)
}

func (a *natsJSAdapter) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	return a.js.AddStream(cfg)
}

func (a *natsJSAdapter) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	return a.js.ConsumerInfo(stream, consumer)
}

func (a *natsJSAdapter) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	return a.js.AddConsumer(stream, cfg)
}

type natsSubAdapter struct {
	sub *nats.Subscription
}

func (s *natsSubAdapter) Unsubscribe() error         { return s.sub.Unsubscribe() }
func (s *natsSubAdapter) Drain() error               { return s.sub.Drain() }
func (s *natsSubAdapter) IsValid() bool              { return s.sub.IsValid() }
func (s *natsSubAdapter) Pending() (int, int, error) { return s.sub.Pending() }
func (s *natsSubAdapter) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return s.sub.Fetch(batch, opts...)
}

// MessageService provides methods for publishing and managing messages over JetStream.
// All operations use JetStream exclusively with proper acknowledgment handling.
type MessageService struct {
	js                JSContext
	logger            *zap.Logger
	maxDeliver        int    // Maximum number of delivery attempts before giving up (default: 5)
	publishMaxRetries int    // Maximum number of retry attempts for publish operations (default: 3)
	resultStream      string // JetStream stream name for publishing results (e.g., RESULTS_UAT)
	resultSubject     string // Subject for publishing results (e.g., result.uat)
}

// validateMessage performs strict validation on the message for callback operations
// Auto-populates CreatedAt and UpdatedAt if they are empty
func (s *MessageService) validateMessage(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	// Auto-populate timestamps if empty (framework responsibility, not service responsibility)
	now := time.Now().Format(time.RFC3339)
	if msg.CreatedAt == "" {
		msg.CreatedAt = now
	}
	if msg.UpdatedAt == "" {
		msg.UpdatedAt = now
	}

	if msg.Workflow == nil {
		return fmt.Errorf("message Workflow is required")
	}

	// Node is optional when Workflow is present (for workflow-level callbacks)
	// But if Node is present, Workflow must also be present
	if msg.Node != nil && msg.Workflow == nil {
		return fmt.Errorf("message Workflow is required when Node is present")
	}

	if msg.Payload == nil {
		return fmt.Errorf("message Payload is required")
	}

	return nil
}

// publishWithRetry attempts to publish a message with retry logic
func (s *MessageService) publishWithRetry(ctx context.Context, subject string, msg *Message, maxRetries int, retryDelay time.Duration) error {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("publish cancelled during retry: %w", ctx.Err())
			case <-time.After(retryDelay):
				// Continue with retry
			}
		}

		err := s.Publish(ctx, subject, msg)
		if err == nil {
			return nil // Success
		}

		lastErr = err
	}

	return fmt.Errorf("publish failed after %d attempts: %w", maxRetries+1, lastErr)
}

// NewMessageService creates a new message service with the given JetStream context.
// Any implementation that satisfies JSContext (including nats.JetStreamContext) can be used.
// The maxDeliver parameter controls the maximum number of delivery attempts for consumers.
// The publishMaxRetries parameter controls the maximum number of retry attempts for publish operations.
// The resultStream and resultSubject parameters configure where results are published (e.g., RESULTS_UAT, result.uat).
func NewMessageService(js JSContext, maxDeliver int, publishMaxRetries int, resultStream string, resultSubject string) (*MessageService, error) {
	if js == nil {
		return nil, fmt.Errorf("JetStream context cannot be nil")
	}

	// Use default if not set or invalid
	if maxDeliver == 0 {
		maxDeliver = 5 // Default: retry up to 5 times (2.5 minutes with 30s AckWait)
	}

	if publishMaxRetries == 0 {
		publishMaxRetries = 3 // Default: 3 retries for publish operations
	}

	if resultStream == "" {
		resultStream = "RESULTS" // Default result stream
	}

	if resultSubject == "" {
		resultSubject = "result" // Default result subject
	}

	logger, _ := zap.NewProduction()
	return &MessageService{
		js:                js,
		logger:            logger,
		maxDeliver:        maxDeliver,
		publishMaxRetries: publishMaxRetries,
		resultStream:      resultStream,
		resultSubject:     resultSubject,
	}, nil
}

// SetLogger sets a custom zap logger for the message service
func (s *MessageService) SetLogger(logger *zap.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// EnsureStream creates the JetStream stream if it doesn't exist, or validates it exists.
// This is a public method that can be called by runners and other components.
func (s *MessageService) EnsureStream(streamName string) error {
	// Check if stream exists
	streamInfo, err := s.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		if err == nats.ErrStreamNotFound {
			s.logger.Info("Creating JetStream stream",
				zap.String("stream", streamName))

			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.*", streamName)},
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s': %w", streamName, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.Strings("subjects", streamConfig.Subjects),
				zap.Duration("max_age", streamConfig.MaxAge),
				zap.Int64("max_msgs", streamConfig.MaxMsgs))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	} else {
		// Stream exists, log its status
		s.logger.Info("JetStream stream already exists",
			zap.String("stream", streamName),
			zap.Uint64("messages", streamInfo.State.Msgs))
	}

	return nil
}

// EnsureConsumer creates the JetStream consumer if it doesn't exist, or validates it exists.
// This is a public method that can be called by runners and other components.
func (s *MessageService) EnsureConsumer(streamName, consumerName string) error {
	// Try to get consumer info first
	consumerInfo, err := s.js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		// Consumer doesn't exist, create it
		if err == nats.ErrConsumerNotFound {
			s.logger.Info("Creating JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName))

			consumerConfig := &nats.ConsumerConfig{
				Durable:       consumerName,
				AckPolicy:     nats.AckExplicitPolicy,
				DeliverPolicy: nats.DeliverAllPolicy,
				MaxAckPending: 1000,
				MaxDeliver:    s.maxDeliver,
			}

			_, err = s.js.AddConsumer(streamName, consumerConfig)
			if err != nil {
				return fmt.Errorf("failed to create consumer '%s' in stream '%s': %w", consumerName, streamName, err)
			}

			s.logger.Info("Successfully created JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName),
				zap.Int("max_deliver", s.maxDeliver))
		} else {
			return fmt.Errorf("failed to get consumer info for '%s' in stream '%s': %w", consumerName, streamName, err)
		}
	} else {
		// Consumer exists, log its status
		s.logger.Info("JetStream consumer already exists",
			zap.String("stream", streamName),
			zap.String("consumer", consumerName),
			zap.Uint64("pending", consumerInfo.NumPending))
	}

	return nil
}

// ensureStreamForSubject ensures a stream exists that can handle the given subject.
// It extracts the stream name from the subject (first segment before dot) and creates
// the stream if it doesn't exist.
func (s *MessageService) ensureStreamForSubject(subject string) error {
	// Extract stream name from subject (first part before dot)
	streamName := subject
	if idx := len(subject); idx > 0 {
		// Find first dot to extract stream name
		for i, c := range subject {
			if c == '.' {
				streamName = subject[:i]
				break
			}
		}
	}

	// Check if stream exists
	_, err := s.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		if err == nats.ErrStreamNotFound {
			s.logger.Info("Creating JetStream stream for subject",
				zap.String("stream", streamName),
				zap.String("subject", subject))

			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{fmt.Sprintf("%s.>", streamName)},
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s' for subject '%s': %w", streamName, subject, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.String("subject_pattern", fmt.Sprintf("%s.>", streamName)))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	}

	return nil
}

// getMessageIdentifier creates a unique identifier for logging purposes
func (s *MessageService) getMessageIdentifier(msg *Message) string {
	// Prefer correlation ID if available
	if msg.CorrelationID != "" {
		return fmt.Sprintf("correlation:%s", msg.CorrelationID)
	}
	if msg.Workflow != nil {
		return fmt.Sprintf("workflow:%s/run:%s", msg.Workflow.WorkflowID, msg.Workflow.RunID)
	}
	if msg.Node != nil {
		return fmt.Sprintf("node:%s", msg.Node.NodeID)
	}
	if msg.Payload != nil && msg.Payload.Reference != "" {
		return fmt.Sprintf("payload:%s", msg.Payload.Reference)
	}
	return fmt.Sprintf("timestamp:%s", msg.CreatedAt)
}

// Publish publishes a message to the specified subject using JetStream.
// The message is persisted according to the stream's configuration.
// If no stream exists for the subject, one will be created automatically.
// Returns an error if the publish fails.
func (s *MessageService) Publish(ctx context.Context, subject string, msg *Message) error {
	if subject == "" {
		s.logger.Error("Publish failed: subject cannot be empty")
		return sdkerrors.NewValidationError("subject cannot be empty", "INVALID_SUBJECT", nil)
	}

	if msg == nil {
		s.logger.Error("Publish failed: message cannot be nil")
		return sdkerrors.NewValidationError("message cannot be nil", "INVALID_MESSAGE", nil)
	}

	// Ensure a stream exists for this subject
	if err := s.ensureStreamForSubject(subject); err != nil {
		s.logger.Error("Failed to ensure stream exists",
			zap.String("subject", subject),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to ensure stream exists", "STREAM_ENSURE_FAILED", err)
	}

	s.logger.Debug("Publishing message",
		zap.String("subject", subject),
		zap.String("message_identifier", s.getMessageIdentifier(msg)))

	data, err := msg.ToBytes()
	if err != nil {
		s.logger.Error("Failed to marshal message",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to marshal message", "MARSHAL_FAILED", err)
	}

	// Create a channel to handle publish result
	resultCh := make(chan error, 1)

	go func() {
		_, err := s.js.Publish(subject, data)
		resultCh <- err
	}()

	select {
	case <-ctx.Done():
		s.logger.Warn("Publish cancelled",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)),
			zap.Error(ctx.Err()))
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
	case err := <-resultCh:
		if err != nil {
			s.logger.Error("Failed to publish message to JetStream",
				zap.String("subject", subject),
				zap.String("message_identifier", s.getMessageIdentifier(msg)),
				zap.Error(err))
			return sdkerrors.NewInternalError("", "failed to publish message to JetStream", "PUBLISH_FAILED", err)
		}
		s.logger.Info("Message published successfully",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)))
		return nil
	}
}

// PullMessages pulls messages from a JetStream pull-based consumer.
// This method fetches messages in batches on demand, providing explicit flow control.
//
// Messages are NOT automatically acknowledged - the caller must handle acknowledgment
// by calling Ack(), Nak(), or Term() on the returned messages as appropriate.
// Use this method when you want explicit control over when messages are fetched and acknowledged.
//
// Parameters:
//   - stream: The name of the JetStream stream
//   - consumer: The name of the durable consumer
//   - batchSize: The maximum number of messages to fetch (defaults to 10 if <= 0)
//
// Returns the fetched messages or an error if the operation fails.
// Note: Returns empty slice (not error) when no messages are available within timeout.
func (s *MessageService) PullMessages(ctx context.Context, stream, consumer string, batchSize int) ([]*Message, error) {
	if stream == "" || consumer == "" {
		s.logger.Error("PullMessages failed: stream and consumer names are required")
		return nil, fmt.Errorf("stream and consumer names are required")
	}

	if batchSize <= 0 {
		batchSize = 10
	}

	s.logger.Debug("Pulling messages",
		zap.String("stream", stream),
		zap.String("consumer", consumer),
		zap.Int("batch_size", batchSize))

	// Create a channel to handle pull result
	type result struct {
		msgs []*Message
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		// Bind to existing consumer
		sub, err := s.js.PullSubscribe("", consumer, nats.Bind(stream, consumer))
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer sub.Unsubscribe()

		// Fetch messages with timeout - use context deadline if available, otherwise default to 3 seconds
		timeout := 3 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
				timeout = remaining
			}
		}

		natsMessages, err := sub.Fetch(batchSize, nats.MaxWait(timeout))
		if err != nil {
			// Check if this is a timeout error - this is normal when no messages are available
			if err == nats.ErrTimeout {
				// Return empty slice for timeout, not an error
				resultCh <- result{msgs: []*Message{}}
				return
			}
			resultCh <- result{err: err}
			return
		}

		messages := make([]*Message, 0, len(natsMessages))
		for _, natsMsg := range natsMessages {
			msg, err := FromNATSMsg(natsMsg)
			if err != nil {
				// Nak malformed messages
				_ = natsMsg.Nak()
				continue
			}
			// Do NOT acknowledge - let the application handle acknowledgment
			// Store the NATS message reference in the Message for later acknowledgment
			msg.natsMsg = natsMsg
			messages = append(messages, msg)
		}

		resultCh <- result{msgs: messages}
	}()

	select {
	case <-ctx.Done():
		// Use debug level for graceful shutdown, warn for unexpected cancellation
		if ctx.Err() == context.Canceled {
			s.logger.Debug("Pull messages cancelled during shutdown",
				zap.String("stream", stream),
				zap.String("consumer", consumer))
		} else {
			s.logger.Warn("Pull messages cancelled",
				zap.String("stream", stream),
				zap.String("consumer", consumer),
				zap.Error(ctx.Err()))
		}
		return nil, fmt.Errorf("pull cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			s.logger.Error("Failed to pull messages from JetStream",
				zap.String("stream", stream),
				zap.String("consumer", consumer),
				zap.Error(res.err))
			return nil, sdkerrors.NewInternalError("", "failed to pull messages from JetStream", "PULL_FAILED", res.err)
		}
		return res.msgs, nil
	}
}

// ResultType represents different types of results that can be published
type ResultType string

const (
	ResultTypeSuccess ResultType = "success"
	ResultTypeError   ResultType = "error"
	ResultTypeWarning ResultType = "warning"
	ResultTypeInfo    ResultType = "info"
)

// ReportSuccess publishes a success callback message for a workflow execution.
// It takes the result message directly and publishes it to the "result" subject.
// The source message (msg) will be acknowledged if the publish succeeds, or nak'd if it fails.
// The result message should already contain the workflow information and any result data.
// Returns an error if publishing fails or if the message validation fails.
func (s *MessageService) ReportSuccess(ctx context.Context, resultMessage Message, msg *nats.Msg) error {
	// Add result type metadata to the result message
	resultMessage.WithMetadata("result_type", string(ResultTypeSuccess))

	// If correlation ID is not set but we have workflow info, generate one
	if resultMessage.CorrelationID == "" && resultMessage.Workflow != nil {
		resultMessage.CorrelationID = fmt.Sprintf("%s-%s", resultMessage.Workflow.WorkflowID, resultMessage.Workflow.RunID)
	}

	s.logger.Info("Reporting success",
		zap.String("message_identifier", s.getMessageIdentifier(&resultMessage)),
		zap.String("correlation_id", resultMessage.CorrelationID),
		zap.String("workflow_id", func() string {
			if resultMessage.Workflow != nil {
				return resultMessage.Workflow.WorkflowID
			}
			return ""
		}()))

	// Validate message
	if err := s.validateMessage(&resultMessage); err != nil {
		s.logger.Error("Success report validation failed",
			zap.String("message_identifier", s.getMessageIdentifier(&resultMessage)),
			zap.Error(err))
		return fmt.Errorf("validation failed: %w", err)
	}

	// Attempt to publish with retries (using configured retry settings)
	if err := s.publishWithRetry(ctx, s.resultSubject, &resultMessage, s.publishMaxRetries, time.Second); err != nil {
		s.logger.Error("Failed to publish success report",
			zap.String("message_identifier", s.getMessageIdentifier(&resultMessage)),
			zap.Error(err))
		// If we have a source message and publishing failed, nak it
		if msg != nil {
			_ = msg.Nak()
		}
		return err
	}

	// If we have a source message and publishing succeeded, ack it
	if msg != nil {
		if err := msg.Ack(); err != nil {
			s.logger.Error("Failed to acknowledge source message after success report",
				zap.String("message_identifier", s.getMessageIdentifier(&resultMessage)),
				zap.Error(err))
			// Log but don't fail - the callback was published successfully
			return fmt.Errorf("failed to acknowledge source message: %w", err)
		}
	}

	s.logger.Info("Success report published and acknowledged",
		zap.String("message_identifier", s.getMessageIdentifier(&resultMessage)))
	return nil
}

// ReportError publishes an error callback message for a failed workflow execution.
// The message contains the provided error details and is published to the "result" subject
// with metadata indicating it represents a failed result.
//
// Error Classification:
//   - Internal errors (transient failures): NAK the message for retry
//   - Other errors (permanent failures like BadRequest, NotFound, etc.): ACK the message to prevent redelivery
//
// Parameters:
//   - workflowID: The unique identifier of the workflow that failed
//   - runID: The unique identifier of this specific workflow execution run
//   - err: The error that occurred (can be *AppError or regular error)
//   - msg: NATS message to acknowledge/nak after error reporting (can be nil)
//
// Returns an error if the message cannot be published.
func (s *MessageService) ReportError(ctx context.Context, workflowID, runID string, err error, msg *nats.Msg) error {
	// Determine if this is a transient or permanent failure
	isTransient := true // Default to transient (retry)
	errorMsg := err.Error()

	// Check if this is an AppError with a specific type
	if appErr, ok := err.(*sdkerrors.AppError); ok {
		// Only Internal errors are transient, all others are permanent
		isTransient = (appErr.Type == sdkerrors.Internal)
	}

	message := NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", errorMsg, fmt.Sprintf("error-%s-%s", workflowID, runID)).
		WithCorrelationID(fmt.Sprintf("%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeError)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID)).
		WithMetadata("error_classification", func() string {
			if isTransient {
				return "transient"
			}
			return "permanent"
		}())

	s.logger.Info("Reporting error",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("correlation_id", message.CorrelationID),
		zap.String("error_message", errorMsg),
		zap.Bool("is_transient", isTransient))

	// Validate message
	if err := s.validateMessage(message); err != nil {
		s.logger.Error("Error report validation failed",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Error(err))
		return fmt.Errorf("validation failed: %w", err)
	}

	// Attempt to publish with retries (using configured retry settings)
	if publishErr := s.publishWithRetry(ctx, s.resultSubject, message, s.publishMaxRetries, time.Second); publishErr != nil {
		s.logger.Error("Failed to publish error report",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Error(publishErr))
		// If we have a source message, nak it since both processing and reporting failed
		if msg != nil {
			_ = msg.Nak()
		}
		return publishErr
	}

	// Handle acknowledgment based on error type
	if msg != nil {
		if isTransient {
			// Transient failure: NAK for retry
			if nakErr := msg.Nak(); nakErr != nil {
				s.logger.Error("Failed to negatively acknowledge source message after transient error report",
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.Error(nakErr))
				return fmt.Errorf("failed to negatively acknowledge source message: %w", nakErr)
			}
			s.logger.Info("Transient error report published and source message nak'd for retry",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID))
		} else {
			// Permanent failure: ACK to prevent redelivery
			if ackErr := msg.Ack(); ackErr != nil {
				s.logger.Error("Failed to acknowledge source message after permanent error report",
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.Error(ackErr))
				return fmt.Errorf("failed to acknowledge source message: %w", ackErr)
			}
			s.logger.Info("Permanent error report published and source message ack'd to prevent redelivery",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID))
		}
	}

	return nil
}
