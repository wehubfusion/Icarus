package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/message"
	"go.uber.org/zap"
)

// createTestNATSMsg creates a test NATSMsg for handler testing
func createTestNATSMsg(workflowID, runID string) *message.NATSMsg {
	msg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload( "test data")

	// Note: We don't need to create a real NATS message for this test
	// The NATSMsg wrapper is what we're testing

	return &message.NATSMsg{
		Message: msg,
		Subject: "test.subject",
		Reply:   "test.reply",
	}
}

func TestHandlerFunc(t *testing.T) {
	// Test that HandlerFunc is an alias for Handler
	var handler message.Handler = func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	var handlerFunc message.HandlerFunc = message.HandlerFunc(handler)

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	err := handlerFunc(ctx, msg)
	if err != nil {
		t.Errorf("HandlerFunc failed: %v", err)
	}
}

func TestChainMiddleware(t *testing.T) {
	callOrder := []string{}

	// Create middleware that records call order
	middleware1 := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			callOrder = append(callOrder, "middleware1-before")
			err := next(ctx, msg)
			callOrder = append(callOrder, "middleware1-after")
			return err
		}
	}

	middleware2 := func(next message.Handler) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			callOrder = append(callOrder, "middleware2-before")
			err := next(ctx, msg)
			callOrder = append(callOrder, "middleware2-after")
			return err
		}
	}

	baseHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		callOrder = append(callOrder, "handler")
		return nil
	}

	// Chain middlewares
	chained := message.Chain(middleware1, middleware2)
	finalHandler := chained(baseHandler)

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	err := finalHandler(ctx, msg)
	if err != nil {
		t.Errorf("Chained handler failed: %v", err)
	}

	expectedOrder := []string{
		"middleware1-before",
		"middleware2-before",
		"handler",
		"middleware2-after",
		"middleware1-after",
	}

	if len(callOrder) != len(expectedOrder) {
		t.Errorf("Expected %d calls, got %d", len(expectedOrder), len(callOrder))
	}

	for i, expected := range expectedOrder {
		if i >= len(callOrder) || callOrder[i] != expected {
			t.Errorf("Call order mismatch at position %d: expected %s, got %s",
				i, expected, func() string {
					if i < len(callOrder) {
						return callOrder[i]
					}
					return "missing"
				}())
		}
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	recovery := message.RecoveryMiddleware()

	// Test handler that panics
	panicHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		panic("test panic")
	}

	wrappedHandler := recovery(panicHandler)

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	err := wrappedHandler(ctx, msg)
	if err == nil {
		t.Error("Expected error from panic recovery, got nil")
	}

	if err.Error() != "panic recovered: test panic" {
		t.Errorf("Expected 'panic recovered: test panic', got '%s'", err.Error())
	}

	// Test handler that doesn't panic
	normalHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	wrappedNormalHandler := recovery(normalHandler)
	err = wrappedNormalHandler(ctx, msg)
	if err != nil {
		t.Errorf("Expected no error from normal handler, got: %v", err)
	}

	// Test handler that returns an error (should not be affected by recovery)
	errorHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		return errors.New("handler error")
	}

	wrappedErrorHandler := recovery(errorHandler)
	err = wrappedErrorHandler(ctx, msg)
	if err == nil {
		t.Error("Expected error from error handler, got nil")
	}
	if err.Error() != "handler error" {
		t.Errorf("Expected 'handler error', got '%s'", err.Error())
	}
}

func TestLoggingMiddleware(t *testing.T) {
	// Test with custom logger
	logger := zap.NewNop() // Use no-op logger to avoid output during tests
	logging := message.LoggingMiddleware(logger)

	successHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	wrappedHandler := logging(successHandler)

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	err := wrappedHandler(ctx, msg)
	if err != nil {
		t.Errorf("Expected no error from logging middleware, got: %v", err)
	}

	// Test with error handler
	errorHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		return errors.New("test error")
	}

	wrappedErrorHandler := logging(errorHandler)
	err = wrappedErrorHandler(ctx, msg)
	if err == nil {
		t.Error("Expected error from error handler, got nil")
	}
	if err.Error() != "test error" {
		t.Errorf("Expected 'test error', got '%s'", err.Error())
	}

	// Test with nil logger (should use default)
	loggingWithNil := message.LoggingMiddleware(nil)
	wrappedWithNil := loggingWithNil(successHandler)
	err = wrappedWithNil(ctx, msg)
	if err != nil {
		t.Errorf("Expected no error with nil logger, got: %v", err)
	}
}

func TestLoggingMiddlewareMessageIdentification(t *testing.T) {
	logger := zap.NewNop()
	logging := message.LoggingMiddleware(logger)

	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	wrappedHandler := logging(handler)
	ctx := context.Background()

	// Test with workflow message
	workflowMsg := createTestNATSMsg("workflow-123", "run-456")
	err := wrappedHandler(ctx, workflowMsg)
	if err != nil {
		t.Errorf("Expected no error with workflow message, got: %v", err)
	}

	// Test with node message
	nodeMsg := &message.NATSMsg{
		Message: message.NewMessage().WithNode("node-123", map[string]interface{}{"type": "test"}),
		Subject: "test.subject",
		Reply:   "test.reply",
	}
	err = wrappedHandler(ctx, nodeMsg)
	if err != nil {
		t.Errorf("Expected no error with node message, got: %v", err)
	}

	// Test with message without workflow or node
	basicMsg := &message.NATSMsg{
		Message: message.NewMessage().WithPayload( "data"),
		Subject: "test.subject",
		Reply:   "test.reply",
	}
	err = wrappedHandler(ctx, basicMsg)
	if err != nil {
		t.Errorf("Expected no error with basic message, got: %v", err)
	}
}

func TestValidationMiddleware(t *testing.T) {
	validation := message.ValidationMiddleware()

	successHandler := func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	wrappedHandler := validation(successHandler)
	ctx := context.Background()

	// Test with valid message
	validMsg := createTestNATSMsg("workflow-123", "run-456")
	err := wrappedHandler(ctx, validMsg)
	if err != nil {
		t.Errorf("Expected no error with valid message, got: %v", err)
	}

	// Test with nil message
	nilMsg := &message.NATSMsg{
		Message: nil,
		Subject: "test.subject",
		Reply:   "test.reply",
	}
	err = wrappedHandler(ctx, nilMsg)
	if err == nil {
		t.Error("Expected error with nil message, got nil")
	}
	if err.Error() != "message is nil" {
		t.Errorf("Expected 'message is nil', got '%s'", err.Error())
	}

	// Test with message missing CreatedAt
	invalidMsg := &message.NATSMsg{
		Message: &message.Message{
			Workflow:  &message.Workflow{WorkflowID: "test", RunID: "test"},
			UpdatedAt: "2023-01-01T00:00:00Z",
		},
		Subject: "test.subject",
		Reply:   "test.reply",
	}
	err = wrappedHandler(ctx, invalidMsg)
	if err == nil {
		t.Error("Expected error with missing CreatedAt, got nil")
	}
	if err.Error() != "message CreatedAt is empty" {
		t.Errorf("Expected 'message CreatedAt is empty', got '%s'", err.Error())
	}

	// Test with message missing UpdatedAt
	invalidMsg2 := &message.NATSMsg{
		Message: &message.Message{
			Workflow:  &message.Workflow{WorkflowID: "test", RunID: "test"},
			CreatedAt: "2023-01-01T00:00:00Z",
		},
		Subject: "test.subject",
		Reply:   "test.reply",
	}
	err = wrappedHandler(ctx, invalidMsg2)
	if err == nil {
		t.Error("Expected error with missing UpdatedAt, got nil")
	}
	if err.Error() != "message UpdatedAt is empty" {
		t.Errorf("Expected 'message UpdatedAt is empty', got '%s'", err.Error())
	}

	// Test with message missing workflow, node, and payload
	emptyMsg := &message.NATSMsg{
		Message: &message.Message{
			CreatedAt: "2023-01-01T00:00:00Z",
			UpdatedAt: "2023-01-01T00:00:00Z",
		},
		Subject: "test.subject",
		Reply:   "test.reply",
	}
	err = wrappedHandler(ctx, emptyMsg)
	if err == nil {
		t.Error("Expected error with empty message content, got nil")
	}
	expectedError := "message must contain at least workflow, node, or payload information"
	if err.Error() != expectedError {
		t.Errorf("Expected '%s', got '%s'", expectedError, err.Error())
	}
}

func TestMiddlewareChainWithAllMiddlewares(t *testing.T) {
	logger := zap.NewNop()

	// Create a handler that tracks execution
	executed := false
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		executed = true
		return nil
	}

	// Chain all middlewares
	chained := message.Chain(
		message.RecoveryMiddleware(),
		message.LoggingMiddleware(logger),
		message.ValidationMiddleware(),
	)

	finalHandler := chained(handler)

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	err := finalHandler(ctx, msg)
	if err != nil {
		t.Errorf("Expected no error from chained middlewares, got: %v", err)
	}

	if !executed {
		t.Error("Expected handler to be executed")
	}
}

func TestMiddlewareErrorPropagation(t *testing.T) {
	logger := zap.NewNop()

	// Create a handler that returns an error
	testError := errors.New("test handler error")
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		return testError
	}

	// Chain middlewares
	chained := message.Chain(
		message.RecoveryMiddleware(),
		message.LoggingMiddleware(logger),
		message.ValidationMiddleware(),
	)

	finalHandler := chained(handler)

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	err := finalHandler(ctx, msg)
	if err == nil {
		t.Error("Expected error to be propagated, got nil")
	}

	if err != testError {
		t.Errorf("Expected original error to be propagated, got: %v", err)
	}
}

func TestRequestHandler(t *testing.T) {
	// Test RequestHandler type - this is just a type alias test
	var requestHandler message.RequestHandler = func(_ context.Context, _ *message.NATSMsg) (*message.Message, error) {
		response := message.NewMessage().WithPayload( "test response")
		return response, nil
	}

	ctx := context.Background()
	msg := createTestNATSMsg("workflow-123", "run-456")

	response, err := requestHandler(ctx, msg)
	if err != nil {
		t.Errorf("RequestHandler failed: %v", err)
	}

	if response == nil {
		t.Error("Expected response message, got nil")
		return
	}

	if response.Payload == nil {
		t.Error("Expected response payload, got nil")
		return
	}

	if response.Payload.GetInlineData() != "test response" {
		t.Error("Expected response payload with 'test response'")
	}
}
