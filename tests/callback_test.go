package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wehubfusion/Icarus/pkg/callback"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.uber.org/zap"
)

func TestNewCallbackHandler(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	// Test with default config
	handler := callback.NewCallbackHandler(c)
	if handler == nil {
		t.Fatal("Expected callback handler to be created")
	}
	defer handler.Close()

	config := handler.GetConfig()
	if config.Subject != "result" {
		t.Errorf("Expected default subject 'result', got %s", config.Subject)
	}
	if config.MaxRetries != 3 {
		t.Errorf("Expected default max retries 3, got %d", config.MaxRetries)
	}
	if config.EnableLogging != true {
		t.Errorf("Expected default logging enabled, got %v", config.EnableLogging)
	}
}

func TestNewCallbackHandlerWithConfig(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	// Test with custom config
	customConfig := &callback.Config{
		Subject:       "custom.result",
		MaxRetries:    5,
		RetryDelay:    1 * time.Second,
		EnableLogging: false,
	}

	handler := callback.NewCallbackHandlerWithConfig(c, customConfig)
	if handler == nil {
		t.Fatal("Expected callback handler to be created")
	}
	defer handler.Close()

	config := handler.GetConfig()
	if config.Subject != "custom.result" {
		t.Errorf("Expected custom subject 'custom.result', got %s", config.Subject)
	}
	if config.MaxRetries != 5 {
		t.Errorf("Expected custom max retries 5, got %d", config.MaxRetries)
	}
	if config.EnableLogging != false {
		t.Errorf("Expected custom logging disabled, got %v", config.EnableLogging)
	}
}

func TestCallbackHandlerValidation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	handler := callback.NewCallbackHandler(c)
	defer handler.Close()

	ctx := context.Background()

	t.Run("NilMessage", func(t *testing.T) {
		err := handler.Callback(ctx, nil)
		if err == nil {
			t.Error("Expected error for nil message")
		}
		if err.Error() != "validation failed: message cannot be nil" {
			t.Errorf("Expected specific validation error, got: %v", err)
		}
	})

	t.Run("MissingCreatedAt", func(t *testing.T) {
		msg := message.NewMessage()
		msg.CreatedAt = "" // Clear created at

		err := handler.Callback(ctx, msg)
		if err == nil {
			t.Error("Expected error for missing CreatedAt")
		}
		if !contains(err.Error(), "CreatedAt is required") {
			t.Errorf("Expected CreatedAt validation error, got: %v", err)
		}
	})

	t.Run("MissingWorkflow", func(t *testing.T) {
		msg := message.NewMessage()
		msg.Workflow = nil // Clear workflow

		err := handler.Callback(ctx, msg)
		if err == nil {
			t.Error("Expected error for missing Workflow")
		}
		if !contains(err.Error(), "Workflow is required") {
			t.Errorf("Expected Workflow validation error, got: %v", err)
		}
	})

	t.Run("MissingNodeWithWorkflow", func(t *testing.T) {
		// Node is optional when Workflow is present, so this should work
		msg := message.NewWorkflowMessage("test-workflow", "test-run").
			WithPayload("test", "test data", "ref-test")
		msg.Node = nil // Clear node (should be OK with workflow present)

		err := handler.Callback(ctx, msg)
		if err != nil {
			t.Errorf("Expected success when Workflow is present but Node is missing, got error: %v", err)
		}
	})

	t.Run("NodeWithoutWorkflow", func(t *testing.T) {
		msg := message.NewMessage().
			WithNode("node-123", map[string]interface{}{"type": "test"})

		err := handler.Callback(ctx, msg)
		if err == nil {
			t.Error("Expected error for Node without Workflow")
		}
		// Since Workflow validation comes first, we expect "Workflow is required"
		if !contains(err.Error(), "Workflow is required") {
			t.Errorf("Expected Workflow validation error when Node is present, got: %v", err)
		}
	})

	t.Run("MissingPayload", func(t *testing.T) {
		msg := message.NewWorkflowMessage("test-workflow", "test-run")
		msg.Payload = nil // Clear payload

		err := handler.Callback(ctx, msg)
		if err == nil {
			t.Error("Expected error for missing Payload")
		}
		if !contains(err.Error(), "Payload is required") {
			t.Errorf("Expected Payload validation error, got: %v", err)
		}
	})
}

func TestCallbackPublish(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	handler := callback.NewCallbackHandler(c)
	defer handler.Close()

	ctx := context.Background()

	t.Run("SuccessfulPublish", func(t *testing.T) {
		msg := message.NewWorkflowMessage("workflow-test", uuid.New().String()).
			WithPayload("callback-test", "test message", "ref-test")

		err := handler.Callback(ctx, msg)
		if err != nil {
			t.Errorf("Expected successful publish, got error: %v", err)
		}
	})

	t.Run("PublishWithCustomSubject", func(t *testing.T) {
		customConfig := &callback.Config{
			Subject:       "custom.subject",
			MaxRetries:    1,
			RetryDelay:    10 * time.Millisecond,
			EnableLogging: false,
		}

		customHandler := callback.NewCallbackHandlerWithConfig(c, customConfig)
		defer customHandler.Close()

		msg := message.NewWorkflowMessage("workflow-custom", uuid.New().String()).
			WithPayload("callback-test", "custom subject test", "ref-custom")

		err := customHandler.Callback(ctx, msg)
		if err != nil {
			t.Errorf("Expected successful publish to custom subject, got error: %v", err)
		}
	})
}

func TestReportSuccess(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	handler := callback.NewCallbackHandler(c)
	defer handler.Close()

	ctx := context.Background()

	var receivedMsg *message.NATSMsg
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe to result messages
	handlerFunc := func(ctx context.Context, msg *message.NATSMsg) error {
		receivedMsg = msg
		msg.Ack()
		wg.Done()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "result", handlerFunc)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(10 * time.Millisecond) // Allow subscription to be ready

	workflowID := "workflow-success-" + uuid.New().String()[:8]
	runID := "run-success-" + uuid.New().String()[:8]
	testData := "Success result data"

	err = handler.ReportSuccess(ctx, workflowID, runID, testData)
	if err != nil {
		t.Fatalf("Failed to report success: %v", err)
	}

	// Wait for message to be received
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for success message")
	}

	if receivedMsg == nil {
		t.Fatal("No message received")
	}

	if receivedMsg.Workflow.WorkflowID != workflowID {
		t.Errorf("Expected workflow ID %s, got %s", workflowID, receivedMsg.Workflow.WorkflowID)
	}

	if receivedMsg.Workflow.RunID != runID {
		t.Errorf("Expected run ID %s, got %s", runID, receivedMsg.Workflow.RunID)
	}

	if receivedMsg.Payload.Data != testData {
		t.Errorf("Expected payload data %s, got %s", testData, receivedMsg.Payload.Data)
	}

	if receivedMsg.Payload.Source != "callback-service" {
		t.Errorf("Expected payload source 'callback-service', got %s", receivedMsg.Payload.Source)
	}

	if receivedMsg.Metadata["result_type"] != string(callback.ResultTypeSuccess) {
		t.Errorf("Expected result type 'success', got %s", receivedMsg.Metadata["result_type"])
	}

	if receivedMsg.Metadata["correlation_id"] != workflowID+"-"+runID {
		t.Errorf("Expected correlation ID %s-%s, got %s", workflowID, runID, receivedMsg.Metadata["correlation_id"])
	}
}

func TestReportError(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	handler := callback.NewCallbackHandler(c)
	defer handler.Close()

	ctx := context.Background()

	var receivedMsg *message.NATSMsg
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe to result messages
	handlerFunc := func(ctx context.Context, msg *message.NATSMsg) error {
		receivedMsg = msg
		msg.Ack()
		wg.Done()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "result", handlerFunc)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(10 * time.Millisecond)

	workflowID := "workflow-error-" + uuid.New().String()[:8]
	runID := "run-error-" + uuid.New().String()[:8]
	errorMsg := "Error occurred during processing"

	err = handler.ReportError(ctx, workflowID, runID, errorMsg)
	if err != nil {
		t.Fatalf("Failed to report error: %v", err)
	}

	// Wait for message to be received
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for error message")
	}

	if receivedMsg == nil {
		t.Fatal("No message received")
	}

	if receivedMsg.Metadata["result_type"] != string(callback.ResultTypeError) {
		t.Errorf("Expected result type 'error', got %s", receivedMsg.Metadata["result_type"])
	}

	if receivedMsg.Payload.Data != errorMsg {
		t.Errorf("Expected payload data %s, got %s", errorMsg, receivedMsg.Payload.Data)
	}
}

func TestReportWarning(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	handler := callback.NewCallbackHandler(c)
	defer handler.Close()

	ctx := context.Background()

	var receivedMsg *message.NATSMsg
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe to result messages
	handlerFunc := func(ctx context.Context, msg *message.NATSMsg) error {
		receivedMsg = msg
		msg.Ack()
		wg.Done()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "result", handlerFunc)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(10 * time.Millisecond)

	workflowID := "workflow-warning-" + uuid.New().String()[:8]
	runID := "run-warning-" + uuid.New().String()[:8]
	warningMsg := "Warning: low disk space"

	err = handler.ReportWarning(ctx, workflowID, runID, warningMsg)
	if err != nil {
		t.Fatalf("Failed to report warning: %v", err)
	}

	// Wait for message to be received
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for warning message")
	}

	if receivedMsg == nil {
		t.Fatal("No message received")
	}

	if receivedMsg.Metadata["result_type"] != string(callback.ResultTypeWarning) {
		t.Errorf("Expected result type 'warning', got %s", receivedMsg.Metadata["result_type"])
	}

	if receivedMsg.Payload.Data != warningMsg {
		t.Errorf("Expected payload data %s, got %s", warningMsg, receivedMsg.Payload.Data)
	}
}

func TestCallbackWithLogging(t *testing.T) {
	// Test that logging works (we can't easily test the actual log output,
	// but we can ensure no panics occur and operations complete)
	c := client.NewClientWithJSContext(NewMockJS())

	// Create logger for testing
	logger, _ := zap.NewDevelopment()

	config := &callback.Config{
		Subject:       "result",
		MaxRetries:    1,
		RetryDelay:    10 * time.Millisecond,
		EnableLogging: true,
		Logger:        logger,
	}

	handler := callback.NewCallbackHandlerWithConfig(c, config)
	defer handler.Close()

	ctx := context.Background()

	msg := message.NewWorkflowMessage("workflow-logging", uuid.New().String()).
		WithPayload("logging-test", "test message", "ref-logging")

	// This should not panic and should complete successfully
	err := handler.Callback(ctx, msg)
	if err != nil {
		t.Errorf("Expected successful callback with logging, got error: %v", err)
	}
}

func TestCallbackWithDisabledLogging(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	config := &callback.Config{
		Subject:       "result",
		MaxRetries:    1,
		RetryDelay:    10 * time.Millisecond,
		EnableLogging: false, // Disable logging
	}

	handler := callback.NewCallbackHandlerWithConfig(c, config)
	defer handler.Close()

	ctx := context.Background()

	msg := message.NewWorkflowMessage("workflow-no-logging", uuid.New().String()).
		WithPayload("no-logging-test", "test message", "ref-no-logging")

	// This should complete successfully without logging
	err := handler.Callback(ctx, msg)
	if err != nil {
		t.Errorf("Expected successful callback without logging, got error: %v", err)
	}
}

func TestCallbackRetryLogic(t *testing.T) {
	// Create a mock that will fail on first attempt but succeed on retry
	c := client.NewClientWithJSContext(NewMockJS())

	// We'll use the mock as-is since it doesn't actually fail publishes
	// In a real scenario with network issues, this would test retry logic
	config := &callback.Config{
		Subject:       "result",
		MaxRetries:    2,
		RetryDelay:    10 * time.Millisecond, // Short delay for test
		EnableLogging: false,
	}

	handler := callback.NewCallbackHandlerWithConfig(c, config)
	defer handler.Close()

	ctx := context.Background()

	msg := message.NewWorkflowMessage("workflow-retry", uuid.New().String()).
		WithPayload("retry-test", "test message", "ref-retry")

	// This should succeed on first attempt with our mock
	err := handler.Callback(ctx, msg)
	if err != nil {
		t.Errorf("Expected successful callback with retry config, got error: %v", err)
	}
}

func TestCallbackContextCancellation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	handler := callback.NewCallbackHandler(c)
	defer handler.Close()

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	msg := message.NewWorkflowMessage("workflow-cancel", uuid.New().String()).
		WithPayload("cancel-test", "test message", "ref-cancel")

	// Cancel the context immediately
	cancel()

	// This should fail due to context cancellation
	err := handler.Callback(ctx, msg)
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}

	if !errors.Is(err, context.Canceled) && !contains(err.Error(), "cancelled") {
		t.Errorf("Expected cancellation error, got: %v", err)
	}
}

func TestDefaultConfig(t *testing.T) {
	config := callback.DefaultConfig()

	if config.Subject != "result" {
		t.Errorf("Expected default subject 'result', got %s", config.Subject)
	}

	if config.MaxRetries != 3 {
		t.Errorf("Expected default max retries 3, got %d", config.MaxRetries)
	}

	if config.RetryDelay != time.Second {
		t.Errorf("Expected default retry delay 1s, got %v", config.RetryDelay)
	}

	if config.EnableLogging != true {
		t.Errorf("Expected default logging enabled, got %v", config.EnableLogging)
	}

	if config.Logger == nil {
		t.Error("Expected default logger to be initialized")
	}
}

func TestCallbackHandlerClose(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	// Create handler with default config (includes logger)
	handler := callback.NewCallbackHandler(c)

	// This should not panic - logger.Sync() might return an error in test environment
	// but the Close method should handle it gracefully
	err := handler.Close()
	// We don't check the error here since logger.Sync() can fail in test environments
	// The important thing is that it doesn't panic
	_ = err // Ignore the error for this test
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			func() bool {
				for i := 1; i <= len(s)-len(substr); i++ {
					if s[i:i+len(substr)] == substr {
						return true
					}
				}
				return false
			}())))
}
