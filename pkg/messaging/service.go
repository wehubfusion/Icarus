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
	Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (JSSubscription, error)
	QueueSubscribe(subj, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (JSSubscription, error)
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

func (a *natsJSAdapter) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (JSSubscription, error) {
	sub, err := a.js.Subscribe(subj, cb, opts...)
	if err != nil {
		return nil, err
	}
	return &natsSubAdapter{sub: sub}, nil
}

func (a *natsJSAdapter) QueueSubscribe(subj, queue string, cb nats.MsgHandler, opts ...nats.SubOpt) (JSSubscription, error) {
	sub, err := a.js.QueueSubscribe(subj, queue, cb, opts...)
	if err != nil {
		return nil, err
	}
	return &natsSubAdapter{sub: sub}, nil
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

// MessageService provides methods for publishing, subscribing, and managing messages over JetStream.
// All operations use JetStream exclusively with proper acknowledgment handling.
type MessageService struct {
	js         JSContext
	middleware Middleware
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

// WithMiddleware adds middleware to the message service.
// Middleware will be applied to all subscription handlers.
func (s *MessageService) WithMiddleware(middleware Middleware) *MessageService {
	s.middleware = middleware
	return s
}

// Publish publishes a message to the specified subject using JetStream.
// The message is persisted according to the stream's configuration.
// Returns an error if the publish fails.
func (s *MessageService) Publish(ctx context.Context, subject string, msg *Message) error {
	if subject == "" {
		return sdkerrors.ErrInvalidSubject
	}

	if msg == nil {
		return sdkerrors.ErrInvalidMessage
	}

	data, err := msg.ToBytes()
	if err != nil {
		return sdkerrors.NewError("MARSHAL_FAILED", "failed to marshal message", err)
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
			return sdkerrors.NewError("PUBLISH_FAILED", "failed to publish message to JetStream", err)
		}
		return nil
	}
}

// Subscribe creates a push-based JetStream subscription to the specified subject.
// Messages are automatically pushed to the handler as they arrive.
// The handler must acknowledge messages using msg.Ack() or msg.Nak().
//
// Note: This requires a stream to be configured for the subject.
// Returns a Subscription that can be used to unsubscribe.
func (s *MessageService) Subscribe(ctx context.Context, subject string, handler Handler) (*Subscription, error) {
	if subject == "" {
		return nil, sdkerrors.ErrInvalidSubject
	}

	if handler == nil {
		return nil, sdkerrors.ErrInvalidHandler
	}

	// Apply middleware if set
	if s.middleware != nil {
		handler = s.middleware(handler)
	}

	// Create JetStream message handler wrapper
	natsHandler := func(natsMsg *nats.Msg) {
		msg, err := FromNATSMsg(natsMsg)
		if err != nil {
			fmt.Printf("Failed to deserialize message: %v\n", err)
			// Negatively acknowledge malformed messages
			_ = natsMsg.Nak()
			return
		}

		wrappedMsg := &NATSMsg{
			Message: msg,
			Subject: natsMsg.Subject,
			Reply:   natsMsg.Reply,
			natsMsg: natsMsg,
		}

		if err := handler(ctx, wrappedMsg); err != nil {
			fmt.Printf("Handler error: %v\n", err)
			// Handler is responsible for Ack/Nak, but if they forgot, we Nak
			_ = natsMsg.Nak()
		}
	}

	// Create push-based JetStream subscription
	sub, err := s.js.Subscribe(subject, natsHandler, nats.ManualAck())
	if err != nil {
		return nil, sdkerrors.NewError("SUBSCRIBE_FAILED", "failed to create JetStream subscription", err)
	}

	return &Subscription{
		sub:     sub,
		subject: subject,
	}, nil
}

// QueueSubscribe creates a queue-based JetStream subscription to the specified subject.
// Messages will be distributed among all queue subscribers with the same queue name.
// This provides load balancing across multiple consumers.
//
// The handler must acknowledge messages using msg.Ack() or msg.Nak().
//
// Note: This requires a stream to be configured for the subject.
// Returns a Subscription that can be used to unsubscribe.
func (s *MessageService) QueueSubscribe(ctx context.Context, subject, queue string, handler Handler) (*Subscription, error) {
	if subject == "" {
		return nil, sdkerrors.ErrInvalidSubject
	}

	if queue == "" {
		return nil, fmt.Errorf("queue name cannot be empty")
	}

	if handler == nil {
		return nil, sdkerrors.ErrInvalidHandler
	}

	// Apply middleware if set
	if s.middleware != nil {
		handler = s.middleware(handler)
	}

	// Create JetStream message handler wrapper
	natsHandler := func(natsMsg *nats.Msg) {
		msg, err := FromNATSMsg(natsMsg)
		if err != nil {
			fmt.Printf("Failed to deserialize message: %v\n", err)
			// Negatively acknowledge malformed messages
			_ = natsMsg.Nak()
			return
		}

		wrappedMsg := &NATSMsg{
			Message: msg,
			Subject: natsMsg.Subject,
			Reply:   natsMsg.Reply,
			natsMsg: natsMsg,
		}

		if err := handler(ctx, wrappedMsg); err != nil {
			fmt.Printf("Handler error: %v\n", err)
			// Handler is responsible for Ack/Nak, but if they forgot, we Nak
			_ = natsMsg.Nak()
		}
	}

	// Create queue-based JetStream subscription
	sub, err := s.js.QueueSubscribe(subject, queue, natsHandler, nats.ManualAck())
	if err != nil {
		return nil, sdkerrors.NewError("SUBSCRIBE_FAILED", "failed to create JetStream queue subscription", err)
	}

	return &Subscription{
		sub:     sub,
		subject: subject,
		queue:   queue,
	}, nil
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

		// Fetch messages with timeout
		natsMessages, err := sub.Fetch(batchSize, nats.MaxWait(5*time.Second))
		if err != nil {
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
			return nil, sdkerrors.NewError("PULL_FAILED", "failed to pull messages from JetStream", res.err)
		}
		return res.msgs, nil
	}
}

// PullMessagesWithHandler pulls messages and processes them with the provided handler.
// This is a convenience method that combines pulling and processing in one call.
// Messages are automatically acknowledged if the handler returns nil, or negatively
// acknowledged if the handler returns an error.
//
// Parameters:
//   - stream: The name of the JetStream stream
//   - consumer: The name of the durable consumer
//   - batchSize: The maximum number of messages to fetch
//   - handler: The function to process each message
//
// Returns the number of successfully processed messages or an error.
func (s *MessageService) PullMessagesWithHandler(ctx context.Context, stream, consumer string, batchSize int, handler Handler) (int, error) {
	if stream == "" || consumer == "" {
		return 0, fmt.Errorf("stream and consumer names are required")
	}

	if batchSize <= 0 {
		batchSize = 10
	}

	if handler == nil {
		return 0, sdkerrors.ErrInvalidHandler
	}

	// Apply middleware if set
	if s.middleware != nil {
		handler = s.middleware(handler)
	}

	// Create a channel to handle pull result
	type result struct {
		count int
		err   error
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

		// Fetch messages with timeout
		natsMessages, err := sub.Fetch(batchSize, nats.MaxWait(5*time.Second))
		if err != nil {
			resultCh <- result{err: err}
			return
		}

		successCount := 0
		for _, natsMsg := range natsMessages {
			msg, err := FromNATSMsg(natsMsg)
			if err != nil {
				// Nak malformed messages
				_ = natsMsg.Nak()
				continue
			}

			wrappedMsg := &NATSMsg{
				Message: msg,
				Subject: natsMsg.Subject,
				Reply:   natsMsg.Reply,
				natsMsg: natsMsg,
			}

			// Process message
			if err := handler(ctx, wrappedMsg); err != nil {
				// Nak on handler error if not already acked/naked
				_ = natsMsg.Nak()
			} else {
				// Ack on success if not already acked
				_ = natsMsg.Ack()
				successCount++
			}
		}

		resultCh <- result{count: successCount}
	}()

	select {
	case <-ctx.Done():
		return 0, fmt.Errorf("pull cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			return 0, sdkerrors.NewError("PULL_FAILED", "failed to pull messages from JetStream", res.err)
		}
		return res.count, nil
	}
}

// Subscription represents an active JetStream subscription.
type Subscription struct {
	sub     JSSubscription
	subject string
	queue   string
}

// Unsubscribe cancels the subscription and stops receiving messages.
func (s *Subscription) Unsubscribe() error {
	if s.sub == nil {
		return nil
	}
	return s.sub.Unsubscribe()
}

// Drain gracefully unsubscribes by processing any buffered messages first.
func (s *Subscription) Drain() error {
	if s.sub == nil {
		return nil
	}
	return s.sub.Drain()
}

// IsValid returns true if the subscription is still active.
func (s *Subscription) IsValid() bool {
	return s.sub != nil && s.sub.IsValid()
}

// Subject returns the subject this subscription is listening on.
func (s *Subscription) Subject() string {
	return s.subject
}

// Queue returns the queue name for queue subscriptions (empty for regular subscriptions).
func (s *Subscription) Queue() string {
	return s.queue
}

// PendingMessages returns the number of pending messages for this subscription.
func (s *Subscription) PendingMessages() (int, error) {
	if s.sub == nil {
		return 0, nil
	}
	msgs, _, err := s.sub.Pending()
	return msgs, err
}
