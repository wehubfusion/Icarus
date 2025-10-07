package tests

import (
	"context"
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

// Note: JetStream requirement testing is handled in client_test.go
// The SDK requires JetStream to be enabled on the NATS server.
// All tests in this file assume JetStream is available.
