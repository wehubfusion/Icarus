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
	js     JSContext
	logger *zap.Logger
}

// validateMessage performs strict validation on the message for callback operations
func (s *MessageService) validateMessage(msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	if msg.CreatedAt == "" {
		return fmt.Errorf("message CreatedAt is required")
	}

	if msg.UpdatedAt == "" {
		return fmt.Errorf("message UpdatedAt is required")
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
func NewMessageService(js JSContext) (*MessageService, error) {
	if js == nil {
		return nil, fmt.Errorf("JetStream context cannot be nil")
	}

	logger, _ := zap.NewProduction()
	return &MessageService{
		js:     js,
		logger: logger,
	}, nil
}

// SetLogger sets a custom zap logger for the message service
func (s *MessageService) SetLogger(logger *zap.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// getMessageIdentifier creates a unique identifier for logging purposes
func (s *MessageService) getMessageIdentifier(msg *Message) string {
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

	s.logger.Info("Reporting success",
		zap.String("message_identifier", s.getMessageIdentifier(&resultMessage)),
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

	// Attempt to publish with retries (using default retry settings)
	if err := s.publishWithRetry(ctx, "result", &resultMessage, 3, time.Second); err != nil {
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
// Parameters:
//   - workflowID: The unique identifier of the workflow that failed
//   - runID: The unique identifier of this specific workflow execution run
//   - errorMsg: The error message or details describing what went wrong
//   - msg: NATS message to negatively acknowledge after error reporting (can be nil)
//
// Returns an error if the message cannot be published.
func (s *MessageService) ReportError(ctx context.Context, workflowID, runID string, errorMsg string, msg *nats.Msg) error {
	message := NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", errorMsg, fmt.Sprintf("error-%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeError)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID))

	s.logger.Info("Reporting error",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("error_message", errorMsg))

	// Validate message
	if err := s.validateMessage(message); err != nil {
		s.logger.Error("Error report validation failed",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Error(err))
		return fmt.Errorf("validation failed: %w", err)
	}

	// Attempt to publish with retries (using default retry settings)
	if err := s.publishWithRetry(ctx, "result", message, 3, time.Second); err != nil {
		s.logger.Error("Failed to publish error report",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Error(err))
		// If we have a source message, nak it since both processing and reporting failed
		if msg != nil {
			_ = msg.Nak()
		}
		return err
	}

	// If we have a source message and error reporting succeeded, still nak it
	// because the original processing failed (that's why we're reporting an error)
	if msg != nil {
		if err := msg.Nak(); err != nil {
			s.logger.Error("Failed to negatively acknowledge source message after error report",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.Error(err))
			// Log but don't fail - the error callback was published successfully
			return fmt.Errorf("failed to negatively acknowledge source message: %w", err)
		}
	}

	s.logger.Info("Error report published and source message nak'd",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID))
	return nil
}
