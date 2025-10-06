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

	// Create EVENTS stream for publish/subscribe examples
	fmt.Println("Creating EVENTS stream...")
	_, err := js.AddStream(&nats.StreamConfig{
		Name:        "EVENTS",
		Description: "Events stream for user actions",
		Subjects:    []string{"events.>"},
		Storage:     nats.FileStorage,
		Replicas:    1,
	})
	if err != nil {
		log.Printf("Warning: Could not create EVENTS stream (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created EVENTS stream")
	}

	// Create WORKERS stream for queue subscription examples
	fmt.Println("Creating WORKERS stream...")
	_, err = js.AddStream(&nats.StreamConfig{
		Name:        "WORKERS",
		Description: "Tasks stream for worker load balancing",
		Subjects:    []string{"tasks.>"},
		Storage:     nats.FileStorage,
		Replicas:    1,
	})
	if err != nil {
		log.Printf("Warning: Could not create WORKERS stream (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created WORKERS stream")
	}

	// Create a pull consumer for the pull-based consumer example
	fmt.Println("Creating pull consumer...")
	_, err = js.AddConsumer("EVENTS", &nats.ConsumerConfig{
		Durable:       "pull-consumer",
		Description:   "Pull consumer for batch message processing",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		log.Printf("Warning: Could not create pull consumer (might already exist): %v", err)
	} else {
		fmt.Println("✓ Created pull consumer")
	}

	fmt.Println("\nJetStream setup complete!")
	fmt.Println("Running message examples...")

	// Example 1: Basic publish and subscribe (JetStream)
	basicPubSub()

	// Example 2: Queue subscriptions for load balancing (JetStream)
	queueSubscription()

	// Example 3: Pull-based consumers (JetStream)
	pullBasedConsumer()
}

// basicPubSub demonstrates basic JetStream publish and subscribe operations
func basicPubSub() {
	fmt.Println("=== Example 1: JetStream Publish/Subscribe ===")

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Apply middleware to the Messages service
	c.Messages = c.Messages.WithMiddleware(
		message.Chain(
			message.RecoveryMiddleware(),
			message.LoggingMiddleware(),
			message.ValidationMiddleware(),
		),
	)

	// Subscribe to a subject using JetStream push-based subscription
	subject := "events.user.created"
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
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
		fmt.Printf("Received message: ID=%s, Content=%s, Metadata=%v\n",
			msgID, content, msg.Metadata)
		// Acknowledge the message
		msg.Ack()
		return nil
	}

	subscription, err := c.Messages.Subscribe(ctx, subject, handler)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer subscription.Unsubscribe()

	// Give subscription time to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish a message using JetStream
	msg := message.NewWorkflowMessage("user-events", uuid.New().String()).
		WithPayload("user-service", "User john@example.com created", "user-12345").
		WithMetadata("user_id", "12345").
		WithMetadata("email", "john@example.com")

	if err := c.Messages.Publish(ctx, subject, msg); err != nil {
		log.Fatalf("Failed to publish: %v", err)
	}

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	fmt.Println()
}

// queueSubscription demonstrates JetStream queue subscriptions for load balancing
func queueSubscription() {
	fmt.Println("=== Example 2: JetStream Queue Subscriptions ===")

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Create multiple queue subscribers (simulating multiple workers)
	subject := "tasks.process"
	queueName := "workers"

	handler := func(workerID int) message.Handler {
		return func(ctx context.Context, msg *message.NATSMsg) error {
			var content string
			if msg.Payload != nil {
				content = msg.Payload.Data
			}
			fmt.Printf("Worker %d processing message: %s\n", workerID, content)
			// Acknowledge the message
			msg.Ack()
			return nil
		}
	}

	// Create 3 workers in the same queue group using JetStream
	for i := 1; i <= 3; i++ {
		sub, err := c.Messages.QueueSubscribe(ctx, subject, queueName, handler(i))
		if err != nil {
			log.Fatalf("Failed to create queue subscriber: %v", err)
		}
		defer sub.Unsubscribe()
	}

	// Give subscriptions time to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish multiple messages - they will be distributed among workers
	for i := 1; i <= 6; i++ {
		msg := message.NewWorkflowMessage("task-queue", uuid.New().String()).
			WithPayload("task-service", fmt.Sprintf("Task %d", i), fmt.Sprintf("task-%d", i))
		if err := c.Messages.Publish(ctx, subject, msg); err != nil {
			log.Fatalf("Failed to publish: %v", err)
		}
	}

	// Wait for message processing
	time.Sleep(500 * time.Millisecond)
	fmt.Println()
}

// pullBasedConsumer demonstrates pull-based message consumption using JetStream
// Note: This requires JetStream streams and consumers to be set up (done automatically in main)
func pullBasedConsumer() {
	fmt.Println("=== Example 3: JetStream Pull-Based Consumer ===")

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222")
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Pull messages from JetStream stream
	streamName := "EVENTS"
	consumerName := "pull-consumer"
	batchSize := 10

	fmt.Println("Pulling messages from JetStream...")
	messages, err := c.Messages.PullMessages(ctx, streamName, consumerName, batchSize)
	if err != nil {
		log.Printf("Failed to pull messages: %v", err)
		fmt.Println("\nTo set up JetStream for this example:")
		fmt.Println("  1. Start NATS with JetStream: nats-server -js")
		fmt.Println("  2. Create a stream: nats stream add EVENTS --subjects 'events.>'")
		fmt.Println("  3. Create a consumer: nats consumer add EVENTS pull-consumer --pull")
		fmt.Println("  4. Publish some messages: nats pub events.test 'test message'")
		return
	}

	if len(messages) == 0 {
		fmt.Println("No messages available in the stream.")
		fmt.Println("Publish some messages using: nats pub events.test 'your message'")
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
			fmt.Printf("  [%d] ID=%s, Content=%s, Metadata=%v\n",
				i+1, msgID, content, msg.Metadata)
		}
	}

	fmt.Println()
}
