package message

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
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
	js JSContext
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

	return &MessageService{
		js: js,
	}, nil
}

// Publish publishes a message to the specified subject using JetStream.
// The message is persisted according to the stream's configuration.
// Returns an error if the publish fails.
func (s *MessageService) Publish(ctx context.Context, subject string, msg *Message) error {
	if subject == "" {
		return sdkerrors.NewValidationError("subject cannot be empty", "INVALID_SUBJECT", nil)
	}

	if msg == nil {
		return sdkerrors.NewValidationError("message cannot be nil", "INVALID_MESSAGE", nil)
	}

	data, err := msg.ToBytes()
	if err != nil {
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
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
	case err := <-resultCh:
		if err != nil {
			return sdkerrors.NewInternalError("", "failed to publish message to JetStream", "PUBLISH_FAILED", err)
		}
		return nil
	}
}

// PullMessages pulls messages from a JetStream pull-based consumer.
// This method fetches messages in batches on demand, providing explicit flow control.
//
// Messages are automatically acknowledged upon successful deserialization.
// Use this method when you want explicit control over when messages are fetched.
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
		return nil, fmt.Errorf("stream and consumer names are required")
	}

	if batchSize <= 0 {
		batchSize = 10
	}

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
			// Acknowledge successful deserialization
			_ = natsMsg.Ack()
			messages = append(messages, msg)
		}

		resultCh <- result{msgs: messages}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("pull cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
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
// The message contains the provided data and is published to the "result" subject
// with metadata indicating it represents a successful result.
//
// Parameters:
//   - workflowID: The unique identifier of the workflow that completed successfully
//   - runID: The unique identifier of this specific workflow execution run
//   - data: The success data or result payload to include in the message
//
// Returns an error if the message cannot be published.
func (s *MessageService) ReportSuccess(ctx context.Context, workflowID, runID string, data string) error {
	msg := NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", data, fmt.Sprintf("success-%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeSuccess)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID))

	// Validate message
	if err := s.validateMessage(msg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Attempt to publish with retries (using default retry settings)
	return s.publishWithRetry(ctx, "result", msg, 3, time.Second)
}

// ReportError publishes an error callback message for a failed workflow execution.
// The message contains the provided error details and is published to the "result" subject
// with metadata indicating it represents a failed result.
//
// Parameters:
//   - workflowID: The unique identifier of the workflow that failed
//   - runID: The unique identifier of this specific workflow execution run
//   - errorMsg: The error message or details describing what went wrong
//
// Returns an error if the message cannot be published.
func (s *MessageService) ReportError(ctx context.Context, workflowID, runID string, errorMsg string) error {
	msg := NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", errorMsg, fmt.Sprintf("error-%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeError)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID))

	// Validate message
	if err := s.validateMessage(msg); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Attempt to publish with retries (using default retry settings)
	return s.publishWithRetry(ctx, "result", msg, 3, time.Second)
}
