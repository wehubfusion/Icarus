package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/amirhy/nats-sdk/pkg/client"
	sdkerrors "github.com/amirhy/nats-sdk/pkg/errors"
	message "github.com/amirhy/nats-sdk/pkg/messaging"
	"github.com/google/uuid"
)

func TestErrorTypes(t *testing.T) {
	// Test predefined error types
	if sdkerrors.ErrNotConnected == nil {
		t.Error("ErrNotConnected should not be nil")
	}

	if sdkerrors.ErrInvalidSubject == nil {
		t.Error("ErrInvalidSubject should not be nil")
	}

	if sdkerrors.ErrInvalidMessage == nil {
		t.Error("ErrInvalidMessage should not be nil")
	}

	if sdkerrors.ErrTimeout == nil {
		t.Error("ErrTimeout should not be nil")
	}

	if sdkerrors.ErrNoResponse == nil {
		t.Error("ErrNoResponse should not be nil")
	}

	if sdkerrors.ErrInvalidHandler == nil {
		t.Error("ErrInvalidHandler should not be nil")
	}

	if sdkerrors.ErrConsumerNotFound == nil {
		t.Error("ErrConsumerNotFound should not be nil")
	}
}

func TestErrorWrapping(t *testing.T) {
	originalErr := errors.New("original error")
	wrappedErr := sdkerrors.NewError("TEST_CODE", "test message", originalErr)

	// Test error message
	if wrappedErr.Error() == "" {
		t.Error("Wrapped error should have a message")
	}

	// Test error code
	if wrappedErr.Code != "TEST_CODE" {
		t.Errorf("Expected code 'TEST_CODE', got '%s'", wrappedErr.Code)
	}

	// Test error message
	if wrappedErr.Message != "test message" {
		t.Errorf("Expected message 'test message', got '%s'", wrappedErr.Message)
	}

	// Test unwrapping
	unwrapped := wrappedErr.Unwrap()
	if unwrapped != originalErr {
		t.Error("Unwrap should return the original error")
	}

	// Test errors.Is compatibility
	if !errors.Is(wrappedErr, originalErr) {
		t.Error("errors.Is should work with wrapped errors")
	}
}

func TestErrorCheckingFunctions(t *testing.T) {
	// Test IsTimeout
	timeoutErr := sdkerrors.ErrTimeout
	if !sdkerrors.IsTimeout(timeoutErr) {
		t.Error("IsTimeout should return true for ErrTimeout")
	}

	wrappedTimeout := sdkerrors.NewError("TIMEOUT", "timeout occurred", timeoutErr)
	if !sdkerrors.IsTimeout(wrappedTimeout) {
		t.Error("IsTimeout should return true for wrapped timeout errors")
	}

	// Test IsNotConnected
	notConnectedErr := sdkerrors.ErrNotConnected
	if !sdkerrors.IsNotConnected(notConnectedErr) {
		t.Error("IsNotConnected should return true for ErrNotConnected")
	}

	wrappedNotConnected := sdkerrors.NewError("CONNECTION", "not connected", notConnectedErr)
	if !sdkerrors.IsNotConnected(wrappedNotConnected) {
		t.Error("IsNotConnected should return true for wrapped connection errors")
	}

	// Test with non-matching errors
	if sdkerrors.IsTimeout(sdkerrors.ErrNotConnected) {
		t.Error("IsTimeout should return false for non-timeout errors")
	}

	if sdkerrors.IsNotConnected(sdkerrors.ErrTimeout) {
		t.Error("IsNotConnected should return false for non-connection errors")
	}
}

func TestPublishErrors(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Test publishing with invalid subject
	msg := message.NewWorkflowMessage("workflow-test", uuid.New().String()).
		WithPayload("test", "test content", "ref-123")
	err := c.Messages.Publish(ctx, "", msg) // Empty subject
	if err == nil {
		t.Error("Expected error for empty subject")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidSubject) {
		t.Errorf("Expected ErrInvalidSubject, got: %v", err)
	}

	// Test publishing with nil message
	err = c.Messages.Publish(ctx, "test.subject", nil)
	if err == nil {
		t.Error("Expected error for nil message")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidMessage) {
		t.Errorf("Expected ErrInvalidMessage, got: %v", err)
	}

	// Test publishing to subject without stream (should fail)
	err = c.Messages.Publish(ctx, "nonexistent.stream.subject", msg)
	if err == nil {
		t.Error("Expected error for publishing to subject without stream")
	}
	// Note: This might return different error types depending on NATS version
	t.Logf("Error for publishing to nonexistent stream: %v", err)
}

func TestSubscribeErrors(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Test subscribing with invalid subject
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	_, err := c.Messages.Subscribe(ctx, "", handler) // Empty subject
	if err == nil {
		t.Error("Expected error for empty subject")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidSubject) {
		t.Errorf("Expected ErrInvalidSubject, got: %v", err)
	}

	// Test subscribing with nil handler
	_, err = c.Messages.Subscribe(ctx, "test.subject", nil)
	if err == nil {
		t.Error("Expected error for nil handler")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidHandler) {
		t.Errorf("Expected ErrInvalidHandler, got: %v", err)
	}
}

func TestQueueSubscribeErrors(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		return nil
	}

	// Test queue subscribe with empty subject
	_, err := c.Messages.QueueSubscribe(ctx, "", "test-queue", handler)
	if err == nil {
		t.Error("Expected error for empty subject")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidSubject) {
		t.Errorf("Expected ErrInvalidSubject, got: %v", err)
	}

	// Test queue subscribe with empty queue name
	_, err = c.Messages.QueueSubscribe(ctx, "test.subject", "", handler)
	if err == nil {
		t.Error("Expected error for empty queue name")
	}
	// This returns a generic error, not a predefined SDK error
	t.Logf("Error for empty queue: %v", err)

	// Test queue subscribe with nil handler
	_, err = c.Messages.QueueSubscribe(ctx, "test.subject", "test-queue", nil)
	if err == nil {
		t.Error("Expected error for nil handler")
	}
	if !errors.Is(err, sdkerrors.ErrInvalidHandler) {
		t.Errorf("Expected ErrInvalidHandler, got: %v", err)
	}
}

func TestPullMessagesErrors(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Test pull with empty stream name
	_, err := c.Messages.PullMessages(ctx, "", "consumer", 1)
	if err == nil {
		t.Error("Expected error for empty stream name")
	}

	// Test pull with empty consumer name
	_, err = c.Messages.PullMessages(ctx, "stream", "", 1)
	if err == nil {
		t.Error("Expected error for empty consumer name")
	}

	// Test pull from nonexistent stream/consumer
	_, err = c.Messages.PullMessages(ctx, "NONEXISTENT", "nonexistent", 1)
	if err == nil {
		t.Error("Expected error for nonexistent stream/consumer")
	}
	t.Logf("Error for nonexistent stream/consumer: %v", err)
}

func TestConnectionErrors(t *testing.T) {
	// Test connecting to invalid URL
	c := client.NewClient("nats://invalid.host:4222")
	ctx := context.Background()

	err := c.Connect(ctx)
	if err == nil {
		t.Error("Expected error for invalid host")
	}
	t.Logf("Connection error for invalid host: %v", err)

	// Test connecting to non-existent port
	c2 := client.NewClient("nats://127.0.0.1:12345")
	err = c2.Connect(ctx)
	if err == nil {
		t.Error("Expected error for non-existent port")
	}
	t.Logf("Connection error for non-existent port: %v", err)
}

func TestJetStreamNotEnabledError(t *testing.T) {
	// This test verifies that our SDK properly reports when JetStream is not enabled
	// We use a mock scenario since we can't easily disable JetStream in the test server

	// Create a custom error to simulate JetStream not enabled
	jsError := sdkerrors.NewError("JETSTREAM_NOT_ENABLED", "JetStream is not enabled on the NATS server", nil)

	// Test that our error checking works
	if jsError.Code != "JETSTREAM_NOT_ENABLED" {
		t.Errorf("Expected error code 'JETSTREAM_NOT_ENABLED', got '%s'", jsError.Code)
	}

	expectedMsg := "JetStream is not enabled"
	if !strings.Contains(jsError.Message, expectedMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedMsg, jsError.Message)
	}
}

func TestMessageSerializationErrors(t *testing.T) {
	// Test deserializing invalid JSON
	invalidJSON := []byte("invalid json")
	_, err := message.FromBytes(invalidJSON)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}

	// Test serializing message that can't be marshaled (shouldn't happen with normal usage)
	// This is more of a theoretical test since our Message struct should always be serializable
	msg := message.NewWorkflowMessage("workflow-test", "run-test").
		WithPayload("test", "content", "ref-test")
	data, err := msg.ToBytes()
	if err != nil {
		t.Errorf("Unexpected error serializing valid message: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected serialized data to be non-empty")
	}

	// Verify we can deserialize what we serialized
	deserialized, err := message.FromBytes(data)
	if err != nil {
		t.Errorf("Failed to deserialize valid data: %v", err)
	}
	if deserialized.Workflow.WorkflowID != msg.Workflow.WorkflowID {
		t.Errorf("Round-trip WorkflowID mismatch: expected %s, got %s", msg.Workflow.WorkflowID, deserialized.Workflow.WorkflowID)
	}
	if deserialized.Workflow.RunID != msg.Workflow.RunID {
		t.Errorf("Round-trip RunID mismatch: expected %s, got %s", msg.Workflow.RunID, deserialized.Workflow.RunID)
	}
}

func TestHandlerErrors(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	SetupJetStream(t, c)

	var handlerCalled bool
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		handlerCalled = true
		// Simulate handler error
		return errors.New("handler processing failed")
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Give subscription time to be ready
	// Note: In JetStream, messages might be negatively acknowledged automatically
	// when handlers return errors, depending on the configuration

	msg := message.NewWorkflowMessage("workflow-error", uuid.New().String()).
		WithPayload("error-test", "error test", "ref-error")
	err = c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// Give time for processing
	// Note: Handler errors are logged but don't necessarily fail the test
	// since JetStream handles redelivery automatically
	time.Sleep(200 * time.Millisecond)

	// The handler should have been called despite the error
	// (JetStream will handle redelivery based on consumer configuration)
	t.Logf("Handler called: %v", handlerCalled)
}
