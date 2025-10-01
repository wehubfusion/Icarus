package message

import (
	"context"
	"fmt"
)

// Handler is a function that processes incoming JetStream messages.
// It receives a context and the message, and returns an error if processing fails.
//
// IMPORTANT: Handlers MUST acknowledge messages using msg.Ack() or msg.Nak()
// to indicate successful or failed processing. Failing to acknowledge messages
// will cause them to be redelivered according to the consumer's configuration.
//
// Example:
//
//	handler := func(ctx context.Context, msg *message.NATSMsg) error {
//	    // Process the message
//	    if err := processMessage(msg); err != nil {
//	        msg.Nak() // Message will be redelivered
//	        return err
//	    }
//	    msg.Ack() // Message successfully processed
//	    return nil
//	}
type Handler func(ctx context.Context, msg *NATSMsg) error

// HandlerFunc is an alias for Handler for convenience
type HandlerFunc Handler

// RequestHandler is a function that processes JetStream requests and returns a response.
// It receives a context and the request message, and returns a response message or an error.
//
// IMPORTANT: RequestHandlers MUST acknowledge messages appropriately using msg.Ack()
// or msg.Nak() to control redelivery behavior.
type RequestHandler func(ctx context.Context, request *NATSMsg) (*Message, error)

// Middleware is a function that wraps a handler to add additional functionality
type Middleware func(Handler) Handler

// Chain chains multiple middlewares together
func Chain(middlewares ...Middleware) Middleware {
	return func(h Handler) Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			h = middlewares[i](h)
		}
		return h
	}
}

// RecoveryMiddleware recovers from panics in message handlers
func RecoveryMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *NATSMsg) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic recovered: %v", r)
				}
			}()
			return next(ctx, msg)
		}
	}
}

// LoggingMiddleware logs message processing (placeholder for structured logging)
func LoggingMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *NATSMsg) error {
			// Create a message identifier from workflow or node information
			var msgID string
			if msg.Workflow != nil {
				msgID = fmt.Sprintf("workflow:%s/run:%s", msg.Workflow.WorkflowID, msg.Workflow.RunID)
			} else if msg.Node != nil {
				msgID = fmt.Sprintf("node:%s", msg.Node.NodeID)
			} else {
				msgID = "unknown"
			}

			fmt.Printf("Processing message: ID=%s, Subject=%s\n", msgID, msg.Subject)
			err := next(ctx, msg)
			if err != nil {
				fmt.Printf("Error processing message: ID=%s, Error=%v\n", msgID, err)
			} else {
				fmt.Printf("Successfully processed message: ID=%s\n", msgID)
			}
			return err
		}
	}
}

// ValidationMiddleware validates messages before processing
func ValidationMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *NATSMsg) error {
			if msg.Message == nil {
				return fmt.Errorf("message is nil")
			}
			if msg.CreatedAt == "" {
				return fmt.Errorf("message CreatedAt is empty")
			}
			if msg.UpdatedAt == "" {
				return fmt.Errorf("message UpdatedAt is empty")
			}
			// Validate that at least one of workflow, node, or payload is present
			if msg.Workflow == nil && msg.Node == nil && msg.Payload == nil {
				return fmt.Errorf("message must contain at least workflow, node, or payload information")
			}
			return next(ctx, msg)
		}
	}
}
