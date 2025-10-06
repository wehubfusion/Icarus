package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/callback"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func main() {
	fmt.Println("Setting up JetStream streams and consumers for callback examples...")

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

	// Create RESULTS stream for callback results
	fmt.Println("Creating RESULTS stream...")
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "RESULTS",
		Description: "Results stream for callback messages",
		Subjects:    []string{"result", "result.*"},
		Storage:     nats.FileStorage,
		Replicas:    1,
	})
	if err != nil {
		log.Printf("Warning: Could not create RESULTS stream (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created RESULTS stream")
	}

	fmt.Println("\nJetStream setup complete!")
	fmt.Println("Running callback examples...")

	// Example 1: Basic callback handler usage
	basicCallbackExample(c)

	// Example 2: Callback handler with custom configuration
	customConfigCallbackExample(c)

	// Example 3: Callback handler with consumer for results
	callbackWithConsumerExample(c)
}

// basicCallbackExample demonstrates basic callback handler usage with default configuration
func basicCallbackExample(c *client.Client) {
	fmt.Println("=== Example 1: Basic Callback Handler ===")

	ctx := context.Background()

	// Create callback handler with default configuration
	callbackHandler := callback.NewCallbackHandler(c)
	defer callbackHandler.Close()

	// Generate workflow and run IDs for this example
	workflowID := "workflow-" + uuid.New().String()[:8]
	runID := "run-" + uuid.New().String()[:8]

	fmt.Printf("Using workflow ID: %s, run ID: %s\n", workflowID, runID)

	// Report success
	fmt.Println("Reporting success...")
	err := callbackHandler.ReportSuccess(ctx, workflowID, runID, "Workflow completed successfully with result: 42")
	if err != nil {
		log.Printf("Failed to report success: %v", err)
	} else {
		fmt.Println("✓ Success reported")
	}

	// Report warning
	fmt.Println("Reporting warning...")
	err = callbackHandler.ReportWarning(ctx, workflowID, runID, "Workflow completed with minor issues: low disk space")
	if err != nil {
		log.Printf("Failed to report warning: %v", err)
	} else {
		fmt.Println("✓ Warning reported")
	}

	// Report error
	fmt.Println("Reporting error...")
	err = callbackHandler.ReportError(ctx, workflowID, runID, "Workflow failed: connection timeout")
	if err != nil {
		log.Printf("Failed to report error: %v", err)
	} else {
		fmt.Println("✓ Error reported")
	}

	fmt.Println()
}

// customConfigCallbackExample demonstrates callback handler with custom configuration
func customConfigCallbackExample(c *client.Client) {
	fmt.Println("=== Example 2: Callback Handler with Custom Configuration ===")

	ctx := context.Background()

	// Create custom configuration
	config := &callback.Config{
		Subject:       "result.custom",
		MaxRetries:    5,
		RetryDelay:    500 * time.Millisecond,
		EnableLogging: true,
	}

	// Create callback handler with custom config
	callbackHandler := callback.NewCallbackHandlerWithConfig(c, config)
	defer callbackHandler.Close()

	// Generate workflow and run IDs
	workflowID := "workflow-custom-" + uuid.New().String()[:8]
	runID := "run-custom-" + uuid.New().String()[:8]

	fmt.Printf("Using custom subject: %s\n", config.Subject)
	fmt.Printf("Using workflow ID: %s, run ID: %s\n", workflowID, runID)

	// Test retry logic by simulating failures (using custom subject that might not exist)
	fmt.Println("Reporting success with custom config (demonstrating retry logic)...")
	err := callbackHandler.ReportSuccess(ctx, workflowID, runID, "Custom configuration test successful")
	if err != nil {
		log.Printf("Failed to report success with custom config: %v", err)
	} else {
		fmt.Println("✓ Success reported with custom config")
	}

	fmt.Println()
}

// callbackWithConsumerExample demonstrates callback handler with a consumer to process results
func callbackWithConsumerExample(c *client.Client) {
	fmt.Println("=== Example 3: Callback Handler with Result Consumer ===")

	ctx := context.Background()

	// Create callback handler
	callbackHandler := callback.NewCallbackHandler(c)
	defer callbackHandler.Close()

	// Create a consumer to listen for results
	fmt.Println("Setting up result consumer...")

	// Create a consumer for the results
	_, err := c.JetStream().AddConsumer("RESULTS", &nats.ConsumerConfig{
		Durable:       "result-consumer",
		Description:   "Consumer for processing callback results",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf("Warning: Could not create result consumer (might already exist): %v", err)
	}

	// Subscribe to results
	var receivedResults []string
	var resultHandler message.Handler = func(ctx context.Context, msg *message.NATSMsg) error {
		resultType := "unknown"
		if metadata, ok := msg.Metadata["result_type"]; ok {
			resultType = metadata
		}

		correlationID := "unknown"
		if metadata, ok := msg.Metadata["correlation_id"]; ok {
			correlationID = metadata
		}

		fmt.Printf("Received result [%s]: %s (correlation: %s)\n",
			resultType, msg.Payload.Data, correlationID)

		receivedResults = append(receivedResults, resultType)
		msg.Ack()
		return nil
	}

	subscription, err := c.Messages.Subscribe(ctx, "result", resultHandler)
	if err != nil {
		log.Fatalf("Failed to subscribe to results: %v", err)
	}
	defer subscription.Unsubscribe()

	// Give subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Generate workflow and run IDs
	workflowID := "workflow-consumer-" + uuid.New().String()[:8]
	runID := "run-consumer-" + uuid.New().String()[:8]

	fmt.Printf("Using workflow ID: %s, run ID: %s\n", workflowID, runID)

	// Send multiple results
	fmt.Println("Sending multiple results...")

	err = callbackHandler.ReportSuccess(ctx, workflowID, runID, "Task 1 completed successfully")
	if err != nil {
		log.Printf("Failed to report success: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // Brief pause between messages

	err = callbackHandler.ReportWarning(ctx, workflowID, runID, "Task 2 completed with warnings")
	if err != nil {
		log.Printf("Failed to report warning: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	err = callbackHandler.ReportError(ctx, workflowID, runID, "Task 3 failed with error")
	if err != nil {
		log.Printf("Failed to report error: %v", err)
	}

	// Wait for all messages to be processed
	fmt.Println("Waiting for results to be processed...")
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Processed %d results\n", len(receivedResults))
	fmt.Println()
}
