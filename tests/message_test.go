package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// With the in-memory mock JetStream, no server or stream setup is required.

func TestMessageCreation(t *testing.T) {
	// Test basic message creation
	msg := message.NewMessage()
	if msg.CreatedAt == "" {
		t.Error("Expected CreatedAt to be set")
	}
	if msg.UpdatedAt == "" {
		t.Error("Expected UpdatedAt to be set")
	}

	// Test workflow message creation
	workflowMsg := message.NewWorkflowMessage("workflow-123", "run-456")
	if workflowMsg.Workflow.WorkflowID != "workflow-123" {
		t.Errorf("Expected workflow ID 'workflow-123', got %s", workflowMsg.Workflow.WorkflowID)
	}
	if workflowMsg.Workflow.RunID != "run-456" {
		t.Errorf("Expected run ID 'run-456', got %s", workflowMsg.Workflow.RunID)
	}
}

func TestMessageWithComponents(t *testing.T) {
	msg := message.NewMessage().
		WithNode("node-123", map[string]interface{}{"type": "processor"}).
		WithPayload("source1", "test data", "ref-456").
		WithOutput("stream")

	if msg.Node.NodeID != "node-123" {
		t.Errorf("Expected node ID 'node-123', got %s", msg.Node.NodeID)
	}
	if msg.Node.Configuration.(map[string]interface{})["type"] != "processor" {
		t.Errorf("Expected node config type 'processor', got %v", msg.Node.Configuration)
	}
	if msg.Payload.Source != "source1" {
		t.Errorf("Expected payload source 'source1', got %s", msg.Payload.Source)
	}
	if msg.Payload.Data != "test data" {
		t.Errorf("Expected payload data 'test data', got %s", msg.Payload.Data)
	}
	if msg.Payload.Reference != "ref-456" {
		t.Errorf("Expected payload reference 'ref-456', got %s", msg.Payload.Reference)
	}
	if msg.Output.DestinationType != "stream" {
		t.Errorf("Expected output destination 'stream', got %s", msg.Output.DestinationType)
	}
}

func TestMessageSerialization(t *testing.T) {
	original := message.NewWorkflowMessage("workflow-123", "run-456").
		WithNode("node-789", map[string]interface{}{"priority": "high"}).
		WithPayload("test-source", "test data content", "ref-123").
		WithOutput("stream")

	// Serialize to bytes
	data, err := original.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize message: %v", err)
	}

	// Deserialize from bytes
	deserialized, err := message.FromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize message: %v", err)
	}

	// Compare fields
	if deserialized.Workflow.WorkflowID != original.Workflow.WorkflowID {
		t.Errorf("Workflow ID mismatch: expected %s, got %s", original.Workflow.WorkflowID, deserialized.Workflow.WorkflowID)
	}
	if deserialized.Workflow.RunID != original.Workflow.RunID {
		t.Errorf("Run ID mismatch: expected %s, got %s", original.Workflow.RunID, deserialized.Workflow.RunID)
	}
	if deserialized.Node.NodeID != original.Node.NodeID {
		t.Errorf("Node ID mismatch: expected %s, got %s", original.Node.NodeID, deserialized.Node.NodeID)
	}
	if deserialized.Payload.Source != original.Payload.Source {
		t.Errorf("Payload source mismatch: expected %s, got %s", original.Payload.Source, deserialized.Payload.Source)
	}
	if deserialized.Payload.Data != original.Payload.Data {
		t.Errorf("Payload data mismatch: expected %s, got %s", original.Payload.Data, deserialized.Payload.Data)
	}
	if deserialized.Output.DestinationType != original.Output.DestinationType {
		t.Errorf("Output destination mismatch: expected %s, got %s", original.Output.DestinationType, deserialized.Output.DestinationType)
	}
}

func TestClientConnection(t *testing.T) {
	// Using mock JS: no real connection; ensure service initializes via helper
	c := client.NewClientWithJSContext(NewMockJS())
	if c.Messages == nil {
		t.Fatal("Messages service should be initialized with mock JS")
	}
}

func TestMessagePublishing(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Test publishing a message
	msg := message.NewWorkflowMessage("workflow-test", uuid.New().String()).
		WithPayload("test", "test message", "ref-123")

	err := c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

func TestMessageSubscription(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	var receivedMsg *message.NATSMsg
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe to messages
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		receivedMsg = msg
		msg.Ack() // Acknowledge the message
		wg.Done()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Give subscription time to be ready
	time.Sleep(10 * time.Millisecond)

	// Publish a message
	testMsg := message.NewWorkflowMessage("workflow-sub", uuid.New().String()).
		WithPayload("test", "subscription test", "ref-sub")

	err = c.Messages.Publish(ctx, "test.events.user.created", testMsg)
	if err != nil {
		t.Fatalf("Failed to publish test message: %v", err)
	}

	// Wait for message to be received
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Verify received message
	if receivedMsg == nil {
		t.Fatal("No message received")
	}
	if receivedMsg.Workflow.WorkflowID != testMsg.Workflow.WorkflowID {
		t.Errorf("Workflow ID mismatch: expected %s, got %s", testMsg.Workflow.WorkflowID, receivedMsg.Workflow.WorkflowID)
	}
	if receivedMsg.Workflow.RunID != testMsg.Workflow.RunID {
		t.Errorf("Run ID mismatch: expected %s, got %s", testMsg.Workflow.RunID, receivedMsg.Workflow.RunID)
	}
	if receivedMsg.Payload.Data != testMsg.Payload.Data {
		t.Errorf("Payload data mismatch: expected %s, got %s", testMsg.Payload.Data, receivedMsg.Payload.Data)
	}
}

func TestPullMessages(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Publish a message first
	msg := message.NewWorkflowMessage("workflow-pull", uuid.New().String()).
		WithPayload("pull-test", "pull test message", "ref-pull")

	err := c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Wait for message to be stored
	time.Sleep(10 * time.Millisecond)

	// Pull messages
	messages, err := c.Messages.PullMessages(ctx, "TEST_EVENTS", "test_pull_consumer", 10)
	if err != nil {
		t.Fatalf("Failed to pull messages: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}

	if len(messages) > 0 {
		if messages[0].Workflow.WorkflowID != msg.Workflow.WorkflowID {
			t.Errorf("Workflow ID mismatch: expected %s, got %s", msg.Workflow.WorkflowID, messages[0].Workflow.WorkflowID)
		}
		if messages[0].Payload.Data != msg.Payload.Data {
			t.Errorf("Payload data mismatch: expected %s, got %s", msg.Payload.Data, messages[0].Payload.Data)
		}
	}
}

func TestMiddleware(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Add validation middleware
	c.Messages = c.Messages.WithMiddleware(message.ValidationMiddleware())

	var received bool
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		received = true
		msg.Ack()
		wg.Done()
		return nil
	}

	sub, err := c.Messages.Subscribe(ctx, "test.events.user.created", handler)
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	time.Sleep(10 * time.Millisecond)

	// Test with valid message
	validMsg := message.NewWorkflowMessage("workflow-middleware", uuid.New().String()).
		WithPayload("middleware-test", "valid content", "ref-middleware")

	err = c.Messages.Publish(ctx, "test.events.user.created", validMsg)
	if err != nil {
		t.Fatalf("Failed to publish valid message: %v", err)
	}

	// Wait for processing
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		if !received {
			t.Error("Expected message to be received")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message processing")
	}
}

func TestAcknowledgment(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	t.Run("TestAck", func(t *testing.T) {
		var ackCalled bool
		var wg sync.WaitGroup
		wg.Add(1)

		handler := func(ctx context.Context, msg *message.NATSMsg) error {
			err := msg.Ack()
			if err != nil {
				t.Errorf("Ack failed: %v", err)
			}
			ackCalled = true
			wg.Done()
			return nil
		}

		sub, err := c.Messages.Subscribe(ctx, "test.events.ack", handler)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		time.Sleep(10 * time.Millisecond)

		msg := message.NewWorkflowMessage("workflow-ack", uuid.New().String()).
			WithPayload("ack-test", "ack test", "ref-ack")
		err = c.Messages.Publish(ctx, "test.events.ack", msg)
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}

		wg.Wait()

		if !ackCalled {
			t.Error("Ack was not called")
		}
	})

	t.Run("TestNak", func(t *testing.T) {
		var nakCalled bool
		var wg sync.WaitGroup
		wg.Add(1)

		handler := func(ctx context.Context, msg *message.NATSMsg) error {
			err := msg.Nak()
			if err != nil {
				t.Errorf("Nak failed: %v", err)
			}
			nakCalled = true
			wg.Done()
			return nil
		}

		sub, err := c.Messages.Subscribe(ctx, "test.events.nak", handler)
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		time.Sleep(10 * time.Millisecond)

		msg := message.NewWorkflowMessage("workflow-nak", uuid.New().String()).
			WithPayload("nak-test", "nak test", "ref-nak")
		err = c.Messages.Publish(ctx, "test.events.nak", msg)
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}

		wg.Wait()

		if !nakCalled {
			t.Error("Nak was not called")
		}
	})
}

// Note: JetStream requirement testing is handled in client_test.go
// The SDK requires JetStream to be enabled on the NATS server.
// All tests in this file assume JetStream is available.
