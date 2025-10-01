package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/amirhy/nats-sdk/pkg/client"
	message "github.com/amirhy/nats-sdk/pkg/messaging"
	"github.com/google/uuid"
)

func TestRecoveryMiddleware(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	// Apply recovery middleware
	c.Messages = c.Messages.WithMiddleware(message.RecoveryMiddleware())

	var panicRecovered bool
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		panicRecovered = true
		panic("test panic")
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)

	msg := message.NewWorkflowMessage("workflow-panic", uuid.New().String()).
		WithPayload("panic-test", "panic test", "ref-panic")
	err = c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Give time for panic recovery
	time.Sleep(200 * time.Millisecond)

	// The middleware should have caught the panic and prevented the test from crashing
	if !panicRecovered {
		t.Error("Expected panic to be recovered by middleware")
	}
}

func TestLoggingMiddleware(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	// Apply logging middleware
	c.Messages = c.Messages.WithMiddleware(message.LoggingMiddleware())

	var handlerCalled bool
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		handlerCalled = true
		msg.Ack()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)

	msg := message.NewWorkflowMessage("workflow-logging", uuid.New().String()).
		WithPayload("logging-test", "logging test", "ref-logging")
	err = c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !handlerCalled {
		t.Error("Expected handler to be called")
	}
	// Logging middleware output would appear in test logs
}

func TestValidationMiddleware(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	// Apply validation middleware
	c.Messages = c.Messages.WithMiddleware(message.ValidationMiddleware())

	var validHandlerCalled bool
	validHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		validHandlerCalled = true
		msg.Ack()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", validHandler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)

	// Test with valid message
	validMsg := message.NewWorkflowMessage("workflow-valid", uuid.New().String()).
		WithPayload("validation-test", "valid content", "ref-valid")
	err = c.Messages.Publish(ctx, "test.events.user.created", validMsg)
	if err != nil {
		t.Fatalf("Failed to publish valid message: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !validHandlerCalled {
		t.Error("Expected valid message handler to be called")
	}
}

func TestMultipleMiddleware(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	// Apply multiple middleware in chain
	c.Messages = c.Messages.WithMiddleware(message.Chain(
		message.RecoveryMiddleware(),
		message.LoggingMiddleware(),
		message.ValidationMiddleware(),
	))

	var handlerCalled bool
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		handlerCalled = true
		msg.Ack()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)

	msg := message.NewWorkflowMessage("workflow-multi", uuid.New().String()).
		WithPayload("multi-test", "multi middleware test", "ref-multi")
	err = c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !handlerCalled {
		t.Error("Expected handler to be called with multiple middleware")
	}
}

func TestMiddlewareChain(t *testing.T) {
	// Test the Chain function directly
	middleware1 := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			// Add prefix to payload data
			if msg.Payload != nil {
				msg.Payload.Data = "mw1:" + msg.Payload.Data
			}
			return next(ctx, msg)
		}
	}

	middleware2 := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			// Add another prefix
			if msg.Payload != nil {
				msg.Payload.Data = "mw2:" + msg.Payload.Data
			}
			return next(ctx, msg)
		}
	}

	// Chain the middleware
	chained := message.Chain(middleware1, middleware2)

	var result string
	finalHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		if msg.Payload != nil {
			result = msg.Payload.Data
		}
		return nil
	}

	// Apply chained middleware
	wrappedHandler := chained(finalHandler)

	// Create a test message
	testMsg := &message.NATSMsg{
		Message: &message.Message{
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			Payload: &message.Payload{
				Source:    "test",
				Data:      "original",
				Reference: "test-ref",
			},
			Metadata:  make(map[string]string),
			CreatedAt: "2025-01-01T00:00:00Z",
			UpdatedAt: "2025-01-01T00:00:00Z",
		},
	}

	// Execute the wrapped handler
	err := wrappedHandler(context.Background(), testMsg)
	if err != nil {
		t.Fatalf("Handler execution failed: %v", err)
	}

	// Verify middleware execution order (should be reversed in Chain)
	expected := "mw2:mw1:original"
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestCustomMiddleware(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	// Create custom middleware that adds metadata
	customMiddleware := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			// Add custom metadata
			if msg.Metadata == nil {
				msg.Metadata = make(map[string]string)
			}
			msg.Metadata["processed_by"] = "custom_middleware"
			msg.Metadata["timestamp"] = fmt.Sprintf("%d", time.Now().Unix())

			err := next(ctx, msg)
			if err != nil {
				// Add error metadata on failure
				msg.Metadata["error"] = err.Error()
			}
			return err
		}
	}

	c.Messages = c.Messages.WithMiddleware(customMiddleware)

	var receivedMsg *message.NATSMsg
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		receivedMsg = msg
		msg.Ack()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)

	msg := message.NewWorkflowMessage("workflow-custom", uuid.New().String()).
		WithPayload("custom-test", "custom middleware test", "ref-custom")
	err = c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if receivedMsg == nil {
		t.Fatal("No message received")
	}

	// Verify custom metadata was added
	if receivedMsg.Metadata["processed_by"] != "custom_middleware" {
		t.Errorf("Expected processed_by metadata, got: %v", receivedMsg.Metadata)
	}

	if receivedMsg.Metadata["timestamp"] == "" {
		t.Error("Expected timestamp metadata to be set")
	}
}

func TestMiddlewareErrorHandling(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	// Middleware that can fail
	errorMiddleware := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			// Check for error condition
			if msg.Payload != nil && strings.Contains(msg.Payload.Data, "error") {
				return fmt.Errorf("middleware detected error in payload data")
			}
			return next(ctx, msg)
		}
	}

	c.Messages = c.Messages.WithMiddleware(errorMiddleware)

	var handlerCalled bool
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		handlerCalled = true
		msg.Ack()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(100 * time.Millisecond)

	// Test with error content
	errorMsg := message.NewWorkflowMessage("workflow-error", uuid.New().String()).
		WithPayload("error-test", "this will cause error", "ref-error")
	err = c.Messages.Publish(ctx, "test.events.user.created", errorMsg)
	if err != nil {
		t.Fatalf("Failed to publish error message: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Handler should not have been called due to middleware error
	if handlerCalled {
		t.Error("Expected handler not to be called when middleware returns error")
	}
}

func TestMiddlewareOrder(t *testing.T) {
	// Test that middleware is applied in the correct order

	var executionOrder []string

	middlewareA := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			executionOrder = append(executionOrder, "A-start")
			err := next(ctx, msg)
			executionOrder = append(executionOrder, "A-end")
			return err
		}
	}

	middlewareB := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			executionOrder = append(executionOrder, "B-start")
			err := next(ctx, msg)
			executionOrder = append(executionOrder, "B-end")
			return err
		}
	}

	finalHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		executionOrder = append(executionOrder, "handler")
		return nil
	}

	// Chain middleware: A then B (B should wrap A)
	chained := message.Chain(middlewareA, middlewareB)
	wrappedHandler := chained(finalHandler)

	testMsg := &message.NATSMsg{
		Message: &message.Message{
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			Payload: &message.Payload{
				Source:    "test",
				Data:      "test",
				Reference: "test-ref",
			},
			Metadata:  make(map[string]string),
			CreatedAt: "2025-01-01T00:00:00Z",
			UpdatedAt: "2025-01-01T00:00:00Z",
		},
	}

	err := wrappedHandler(context.Background(), testMsg)
	if err != nil {
		t.Fatalf("Handler execution failed: %v", err)
	}

	// Verify execution order: A-start, B-start, handler, B-end, A-end
	expected := []string{"A-start", "B-start", "handler", "B-end", "A-end"}
	if len(executionOrder) != len(expected) {
		t.Fatalf("Expected %d executions, got %d: %v", len(expected), len(executionOrder), executionOrder)
	}

	for i, expectedStep := range expected {
		if executionOrder[i] != expectedStep {
			t.Errorf("Step %d: expected %s, got %s", i, expectedStep, executionOrder[i])
		}
	}
}
