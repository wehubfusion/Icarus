package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
)

// SimpleProcessor implements the Processor interface for basic message handling
type SimpleProcessor struct {
	name string
}

func (p *SimpleProcessor) Process(ctx context.Context, msg *message.Message) error {
	// Extract message info
	workflowID := "unknown"
	runID := "unknown"
	data := "no data"

	if msg.Workflow != nil {
		workflowID = msg.Workflow.WorkflowID
		runID = msg.Workflow.RunID
	}
	if msg.Payload != nil {
		data = msg.Payload.Data
	}

	fmt.Printf("[%s] Processing: workflow=%s run=%s data=%s\n",
		p.name, workflowID, runID, data)

	// Simulate some processing time
	time.Sleep(2000 * time.Millisecond)

	// Simple success/failure logic based on message content
	if data == "fail" {
		return fmt.Errorf("simulated failure for message: %s", data)
	}

	fmt.Printf("[%s] ✓ Successfully processed message\n", p.name)
	return nil
}

func main() {
	fmt.Println("=== Simple Icarus Runner Example ===")

	// Connect to NATS
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()

	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Setup streams
	setupStreams(c.JetStream())

	// Create processor and runner
	processor := &SimpleProcessor{name: "Worker-1"}
	r := runner.NewRunner(c, processor, "WORKFLOW", "workflow-consumer", 5, 2)

	// Publish test messages
	publishTestMessages(ctx, c)

	// Run the runner for 10 seconds
	fmt.Println("Starting runner for 10 seconds...")
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := r.Run(runCtx); err != nil {
		log.Printf("Runner error: %v", err)
	}

	// Check results
	checkResults(ctx, c)
	fmt.Println("✓ Runner example completed!")
}

func setupStreams(js nats.JetStreamContext) {
	// Create WORKFLOW stream
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     "WORKFLOW",
		Subjects: []string{"workflow.>"},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		log.Printf("Warning: Could not create WORKFLOW stream: %v", err)
	}

	// Create RESULTS stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "RESULTS",
		Subjects: []string{"result"},
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		log.Printf("Warning: Could not create RESULTS stream: %v", err)
	}

	// Create consumer
	_, err = js.AddConsumer("WORKFLOW", &nats.ConsumerConfig{
		Durable:   "workflow-consumer",
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		log.Printf("Warning: Could not create consumer: %v", err)
	}
}

func publishTestMessages(ctx context.Context, c *client.Client) {
	fmt.Println("Publishing test messages...")

	workflowID := "test-workflow"
	runID := uuid.New().String()

	messages := []string{"task-1", "task-2", "fail", "task-3"}

	for _, data := range messages {
		msg := message.NewWorkflowMessage(workflowID, runID).
			WithPayload("test", data, fmt.Sprintf("ref-%s", data))

		subject := fmt.Sprintf("workflow.%s", workflowID)
		if err := c.Messages.Publish(ctx, subject, msg); err != nil {
			log.Printf("Failed to publish: %v", err)
		}
	}

	fmt.Printf("✓ Published %d messages\n", len(messages))
}

func checkResults(ctx context.Context, c *client.Client) {
	fmt.Println("Checking results...")

	results, err := c.Messages.PullMessages(ctx, "RESULTS", "results-consumer", 10)
	if err != nil {
		log.Printf("Failed to pull results: %v", err)
		return
	}

	fmt.Printf("Found %d result messages:\n", len(results))
	for i, result := range results {
		resultType := "unknown"
		if result.Metadata != nil {
			resultType = result.Metadata["result_type"]
		}
		content := "no content"
		if result.Payload != nil {
			content = result.Payload.Data
		}
		fmt.Printf("  %d. [%s] %s\n", i+1, resultType, content)
	}
}
