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

// StringProcessor implements the runner.Processor interface
type StringProcessor struct{}

func (p *StringProcessor) Process(ctx context.Context, msg *message.Message) error {
	// Extract payload data
	if msg.Payload == nil {
		return fmt.Errorf("message has no payload")
	}

	data := msg.Payload.Data
	fmt.Printf("Processing message: %s\n", data)

	// Simulate some processing that might fail
	if data == "fail" {
		return fmt.Errorf("simulated processing failure")
	}

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)
	return nil
}

func main() {
	fmt.Println("Setting up JetStream streams and consumers for runner example...")

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

	// Create WORKERS stream for runner tasks
	streamName := "WORKERS"
	consumerName := "task-consumer"
	fmt.Printf("Creating %s stream...\n", streamName)
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        streamName,
		Description: "Tasks stream for runner processing",
		Subjects:    []string{"tasks.>"},
		Storage:     nats.FileStorage,
		Replicas:    1,
	})
	if err != nil {
		log.Printf("Warning: Could not create %s stream (might already exist): %v", streamName, err)
	} else {
		fmt.Printf("✓ Created %s stream\n", streamName)
	}

	// Create RESULTS stream for success/error reporting
	resultsStream := "RESULTS"
	fmt.Printf("Creating %s stream...\n", resultsStream)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:        resultsStream,
		Description: "Results stream for success/error reporting",
		Subjects:    []string{"results.>"},
		Storage:     nats.FileStorage,
		Replicas:    1,
	})
	if err != nil {
		log.Printf("Warning: Could not create %s stream (might already exist): %v", resultsStream, err)
	} else {
		fmt.Printf("✓ Created %s stream\n", resultsStream)
	}

	// Create a pull consumer for the runner
	fmt.Println("Creating pull consumer...")
	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		Description:   "Pull consumer for runner processing",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf("Warning: Could not create pull consumer (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created pull consumer")
	}

	fmt.Println("\nJetStream setup complete!")
	fmt.Println("Starting runner example...")

	// Create the runner
	r := runner.NewRunner(c, streamName, consumerName, 10, 3)

	// Register the processor
	r.RegisterProcessor(&StringProcessor{})

	// Start the runner in a goroutine
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	go func() {
		if err := r.Run(runCtx); err != nil {
			log.Printf("Runner error: %v", err)
		}
	}()

	// Give the runner time to start
	time.Sleep(500 * time.Millisecond)

	// Publish some test messages
	fmt.Println("Publishing test messages...")

	// Success message
	successMsg := message.NewWorkflowMessage("test-workflow", uuid.New().String()).
		WithPayload("test-service", "hello world", "task-1").
		WithMetadata("type", "success")

	if err := c.Messages.Publish(ctx, "tasks.process", successMsg); err != nil {
		log.Printf("Failed to publish success message: %v", err)
	} else {
		fmt.Println("✓ Published success message")
	}

	// Failure message
	failureMsg := message.NewWorkflowMessage("test-workflow", uuid.New().String()).
		WithPayload("test-service", "fail", "task-2").
		WithMetadata("type", "failure")

	if err := c.Messages.Publish(ctx, "tasks.process", failureMsg); err != nil {
		log.Printf("Failed to publish failure message: %v", err)
	} else {
		fmt.Println("✓ Published failure message")
	}

	// Another success message
	successMsg2 := message.NewWorkflowMessage("test-workflow", uuid.New().String()).
		WithPayload("test-service", "goodbye world", "task-3").
		WithMetadata("type", "success")

	if err := c.Messages.Publish(ctx, "tasks.process", successMsg2); err != nil {
		log.Printf("Failed to publish second success message: %v", err)
	} else {
		fmt.Println("✓ Published second success message")
	}

	// Wait for processing to complete
	fmt.Println("Waiting for message processing...")
	time.Sleep(5 * time.Second)

	// Try to pull results to see success/error reports
	fmt.Println("Checking for results...")
	results, err := c.Messages.PullMessages(ctx, resultsStream, "results-consumer", 10)
	if err != nil {
		log.Printf("Failed to pull results: %v", err)
	} else {
		fmt.Printf("Found %d result messages:\n", len(results))
		for i, result := range results {
			var content string
			if result.Payload != nil {
				content = result.Payload.Data
			}
			var resultType string
			if result.Metadata != nil {
				resultType = result.Metadata["result_type"]
			}
			fmt.Printf("  [%d] Type: %s, Content: %s\n", i+1, resultType, content)
		}
	}

	fmt.Println("Runner example complete!")
}
