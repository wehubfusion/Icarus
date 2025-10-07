package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/client"
)

func main() {
	fmt.Println("Setting up JetStream streams and consumers...")

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Get JetStream context
	js := c.JetStream()
	if js == nil {
		log.Fatal("JetStream not available")
	}

	// Create RESULTS stream for callback reporting examples
	fmt.Println("Creating RESULTS stream...")
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "RESULTS",
		Description: "Results stream for success/error callback reporting",
		Subjects:    []string{"result"},
		Storage:     nats.FileStorage,
		Replicas:    1,
	})
	if err != nil {
		log.Printf("Warning: Could not create RESULTS stream (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created RESULTS stream")
	}

	// Create a consumer for the results stream to demonstrate callback reporting
	fmt.Println("Creating results consumer...")
	_, err = js.AddConsumer("RESULTS", &nats.ConsumerConfig{
		Durable:       "results-consumer",
		Description:   "Consumer for callback result messages",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf("Warning: Could not create results consumer (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created results consumer")
	}

	fmt.Println("\nJetStream setup complete!")
	fmt.Println("Running message examples...")

	// Example 1: Pull-based consumers with publish-then-pull workflow (JetStream)
	pullBasedConsumer()

	// Example 2: Callback reporting for success/error handling
	callbackReporting()
}

// pullBasedConsumer demonstrates pull-based message consumption using JetStream
// This example first publishes messages, then pulls them back
func pullBasedConsumer() {
	fmt.Println("=== Example 1: JetStream Pull-Based Consumer ===")

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// First, publish some messages to demonstrate the publish-then-pull workflow
	fmt.Println("Publishing messages to demonstrate publish-then-pull...")

	workflowID := "pull-demo"
	runID1 := uuid.New().String()
	runID2 := uuid.New().String()

	// Report success for first workflow run
	fmt.Printf("Reporting success for workflow %s run %s...\n", workflowID, runID1)
	if err := c.Messages.ReportSuccess(ctx, workflowID, runID1, "Task completed successfully"); err != nil {
		log.Printf("Failed to report success: %v", err)
	} else {
		fmt.Println("✓ Published success message")
	}

	// Report error for second workflow run
	fmt.Printf("Reporting error for workflow %s run %s...\n", workflowID, runID2)
	if err := c.Messages.ReportError(ctx, workflowID, runID2, "Task failed with validation error"); err != nil {
		log.Printf("Failed to report error: %v", err)
	} else {
		fmt.Println("✓ Published error message")
	}

	// Wait for messages to be published
	time.Sleep(200 * time.Millisecond)

	// Now pull the messages we just published
	streamName := "RESULTS"
	consumerName := "results-consumer"
	batchSize := 10

	fmt.Println("\nPulling messages from JetStream...")
	messages, err := c.Messages.PullMessages(ctx, streamName, consumerName, batchSize)
	if err != nil {
		log.Printf("Failed to pull messages: %v", err)
		return
	}

	if len(messages) == 0 {
		fmt.Println("No messages available in the stream.")
	} else {
		fmt.Printf("Successfully pulled %d messages:\n", len(messages))
		for i, msg := range messages {
			// Create a message identifier from workflow or node information
			var msgID string
			if msg.Workflow != nil {
				msgID = fmt.Sprintf("workflow:%s/run:%s", msg.Workflow.WorkflowID, msg.Workflow.RunID)
			} else if msg.Node != nil {
				msgID = fmt.Sprintf("node:%s", msg.Node.NodeID)
			} else {
				msgID = "unknown"
			}
			var content string
			if msg.Payload != nil {
				content = msg.Payload.Data
			}
			var resultType string
			if msg.Metadata != nil {
				resultType = msg.Metadata["result_type"]
			}
			fmt.Printf("  [%d] ID=%s, Type=%s, Content=%s\n",
				i+1, msgID, resultType, content)
		}
	}

	fmt.Println()
}

// callbackReporting demonstrates the callback reporting functionality for success and error handling
func callbackReporting() {
	fmt.Println("=== Example 2: Callback Reporting ===")

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Create workflow messages for processing
	workflowID := "callback-demo"
	runID1 := uuid.New().String()
	runID2 := uuid.New().String()
	runID3 := uuid.New().String()

	fmt.Printf("Processing workflow %s with multiple runs...\n", workflowID)

	// Simulate successful processing - report success
	fmt.Printf("Reporting success for run %s...\n", runID1)
	if err := c.Messages.ReportSuccess(ctx, workflowID, runID1, "Workflow completed successfully with result data"); err != nil {
		log.Printf("Failed to report success: %v", err)
	} else {
		fmt.Println("✓ Successfully reported success callback")
	}

	// Simulate another successful processing
	fmt.Printf("Reporting success for run %s...\n", runID2)
	if err := c.Messages.ReportSuccess(ctx, workflowID, runID2, "Another task completed successfully"); err != nil {
		log.Printf("Failed to report success: %v", err)
	} else {
		fmt.Println("✓ Successfully reported success callback")
	}

	// Simulate failed processing - report error
	fmt.Printf("Reporting error for run %s...\n", runID3)
	if err := c.Messages.ReportError(ctx, workflowID, runID3, "Workflow failed due to validation error: invalid input data"); err != nil {
		log.Printf("Failed to report error: %v", err)
	} else {
		fmt.Println("✓ Successfully reported error callback")
	}

	fmt.Println("Callback messages have been published to the RESULTS stream.")
	fmt.Println("Use pull-based consumers (like in Example 1) to retrieve these messages.")

	fmt.Println()
}
