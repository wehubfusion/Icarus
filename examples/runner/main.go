package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
)

// PrintProcessor is an example processor that demonstrates message processing.
// It simulates processing time and occasionally fails to show error handling.
type PrintProcessor struct {
	name         string
	processDelay time.Duration
	failureRate  float64 // 0.0 to 1.0, probability of simulated failure
}

// NewPrintProcessor creates a new PrintProcessor with configurable behavior.
func NewPrintProcessor(name string, processDelay time.Duration, failureRate float64) *PrintProcessor {
	return &PrintProcessor{
		name:         name,
		processDelay: processDelay,
		failureRate:  failureRate,
	}
}

// Process implements the runner.Processor interface.
// It prints the message, simulates processing time, and occasionally fails.
func (p *PrintProcessor) Process(ctx context.Context, msg message.Message) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Print the message being processed
	var content string
	if msg.Payload != nil {
		content = msg.Payload.Data
	} else {
		content = "no payload"
	}
	fmt.Printf("[%s] Processing: %s\n", p.name, content)

	// Simulate processing time
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.processDelay):
	}

	// Simulate occasional failures for demonstration
	if p.failureRate > 0 {
		// Simple pseudo-random failure simulation based on message content hash
		var hash uint32
		if msg.Payload != nil && msg.Payload.Data != "" {
			for _, r := range msg.Payload.Data {
				hash = hash*31 + uint32(r)
			}
		} else {
			hash = 42 // default hash
		}

		if float64(hash%100)/100.0 < p.failureRate {
			err := fmt.Errorf("[%s] simulated processing failure for message: %s", p.name, content)
			log.Printf("Error: %v", err)
			return err
		}
	}

	fmt.Printf("[%s] Successfully processed: %s\n", p.name, content)
	return nil
}

// ValidationProcessor demonstrates a processor that validates messages.
type ValidationProcessor struct{}

// Process validates that messages are strings and meet basic criteria.
func (v *ValidationProcessor) Process(ctx context.Context, msg message.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if msg.Payload == nil || msg.Payload.Data == "" {
		return fmt.Errorf("validation failed: empty or missing payload")
	}

	fmt.Printf("[Validator] Validated: %s\n", msg.Payload.Data)
	return nil
}

// EnrichmentProcessor demonstrates adding additional processing steps.
type EnrichmentProcessor struct{}

// Process enriches the message with additional metadata.
func (e *EnrichmentProcessor) Process(ctx context.Context, msg message.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if msg.Payload == nil || msg.Payload.Data == "" {
		return fmt.Errorf("enrichment failed: empty or missing payload")
	}

	// Simulate enrichment by adding metadata
	enriched := fmt.Sprintf("%s [processed_at=%s]", msg.Payload.Data, time.Now().Format(time.RFC3339))
	fmt.Printf("[Enricher] Enriched: %s\n", enriched)

	// Note: In a real implementation, you might modify the message in place
	// or return enriched data. For this example, we just demonstrate the processing.
	return nil
}

func main() {
	fmt.Println("=== Icarus Runner - Concurrent Message Processing Demo ===")
	fmt.Println()

	// Example 1: Sequential Processing
	fmt.Println("1. Sequential Processing (Single Worker)")
	fmt.Println("=========================================")

	// Create a processor that simulates slow processing
	processor := NewPrintProcessor("Sequential", 100*time.Millisecond, 0.1)

	// Create runner with 0 workers (sequential)
	seqRunner := runner.NewRunner(processor, 0)

	// Validate configuration
	if err := seqRunner.ValidateConfig(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Create a channel of messages
	msgs := make(chan message.Message, 10)
	go func() {
		defer close(msgs)
		for i := 1; i <= 5; i++ {
			msg := message.NewMessage().WithPayload("demo", fmt.Sprintf("Message %d", i), fmt.Sprintf("msg-%d", i))
			msgs <- *msg
			time.Sleep(50 * time.Millisecond) // Simulate message arrival rate
		}
	}()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Run sequential processing
	fmt.Println("Processing messages sequentially...")
	start := time.Now()
	if err := seqRunner.Run(ctx, msgs); err != nil {
		log.Printf("Sequential processing error: %v", err)
	}
	fmt.Printf("Sequential processing completed in: %v\n", time.Since(start))
	fmt.Println()

	// Example 2: Concurrent Processing
	fmt.Println("2. Concurrent Processing (3 Workers)")
	fmt.Println("====================================")

	// Create a processor for concurrent processing
	concurrentProcessor := NewPrintProcessor("Concurrent", 200*time.Millisecond, 0.05)

	// Create runner with 3 workers
	concurrentRunner := runner.NewRunner(concurrentProcessor, 3)

	// Create messages channel
	concurrentMsgs := make(chan message.Message, 10)
	go func() {
		defer close(concurrentMsgs)
		for i := 1; i <= 8; i++ {
			msg := message.NewMessage().WithPayload("demo", fmt.Sprintf("Concurrent Message %d", i), fmt.Sprintf("msg-%d", i))
			concurrentMsgs <- *msg
			time.Sleep(30 * time.Millisecond) // Faster message arrival
		}
	}()

	// Create fresh context
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	// Run concurrent processing
	fmt.Println("Processing messages concurrently with 3 workers...")
	start = time.Now()
	if err := concurrentRunner.Run(ctx2, concurrentMsgs); err != nil {
		log.Printf("Concurrent processing error: %v", err)
	}
	fmt.Printf("Concurrent processing completed in: %v\n", time.Since(start))
	fmt.Println()

	// Example 3: Functional Processor
	fmt.Println("3. Functional Processor (Using ProcessorFunc)")
	fmt.Println("============================================")

	// Create a functional processor
	funcProcessor := runner.ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var content string
		if msg.Payload != nil {
			content = msg.Payload.Data
		} else {
			content = "no payload"
		}
		fmt.Printf("[Functional] Quick processing: %s\n", content)
		time.Sleep(50 * time.Millisecond) // Quick processing
		return nil
	})

	funcRunner := runner.NewRunner(funcProcessor, 2)

	// Create messages
	funcMsgs := make(chan message.Message, 5)
	go func() {
		defer close(funcMsgs)
		for i := 1; i <= 5; i++ {
			msg := message.NewMessage().WithPayload("demo", fmt.Sprintf("Func Message %d", i), fmt.Sprintf("msg-%d", i))
			funcMsgs <- *msg
		}
	}()

	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()

	fmt.Println("Processing with functional processor...")
	start = time.Now()
	if err := funcRunner.Run(ctx3, funcMsgs); err != nil {
		log.Printf("Functional processing error: %v", err)
	}
	fmt.Printf("Functional processing completed in: %v\n", time.Since(start))
	fmt.Println()

	// Example 4: Chained Processors (Middleware Pattern)
	fmt.Println("4. Chained Processors (Validation + Enrichment + Print)")
	fmt.Println("=====================================================")

	// Create individual processors
	validator := &ValidationProcessor{}
	enricher := &EnrichmentProcessor{}
	printer := NewPrintProcessor("Chained", 100*time.Millisecond, 0.0)

	// Chain them together using runner.Chain
	chainedProcessor := runner.Chain(validator, enricher, printer)
	chainedRunner := runner.NewRunner(chainedProcessor, 1)

	// Create messages
	chainedMsgs := make(chan message.Message, 4)
	go func() {
		defer close(chainedMsgs)
		messages := []string{"order-123", "user-456", "task-789", "event-101"}
		for i, msgData := range messages {
			msg := message.NewMessage().WithPayload("demo", msgData, fmt.Sprintf("msg-%d", i))
			chainedMsgs <- *msg
			time.Sleep(100 * time.Millisecond)
		}
	}()

	ctx4, cancel4 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel4()

	fmt.Println("Processing with chained processors...")
	start = time.Now()
	if err := chainedRunner.Run(ctx4, chainedMsgs); err != nil {
		log.Printf("Chained processing error: %v", err)
	}
	fmt.Printf("Chained processing completed in: %v\n", time.Since(start))
	fmt.Println()

	// Example 5: Context Cancellation Demo
	fmt.Println("5. Context Cancellation Demo")
	fmt.Println("============================")

	cancelProcessor := NewPrintProcessor("CancelDemo", 500*time.Millisecond, 0.0)
	cancelRunner := runner.NewRunner(cancelProcessor, 2)

	// Create messages that will take longer than the context timeout
	cancelMsgs := make(chan message.Message, 10)
	go func() {
		defer close(cancelMsgs)
		for i := 1; i <= 10; i++ {
			msg := message.NewMessage().WithPayload("demo", fmt.Sprintf("Slow Message %d", i), fmt.Sprintf("msg-%d", i))
			cancelMsgs <- *msg
		}
	}()

	// Create a short context to demonstrate cancellation
	ctx5, cancel5 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel5()

	fmt.Println("Processing with short timeout (1 second) - should cancel...")
	start = time.Now()
	if err := cancelRunner.Run(ctx5, cancelMsgs); err != nil {
		fmt.Printf("Expected cancellation: %v (after %v)\n", err, time.Since(start))
	}
	fmt.Println()

	// Example 6: Error Handling and Recovery
	fmt.Println("6. Error Handling and Recovery")
	fmt.Println("==============================")

	// Create a processor with high failure rate
	unreliableProcessor := NewPrintProcessor("Unreliable", 100*time.Millisecond, 0.7) // 70% failure rate
	unreliableRunner := runner.NewRunner(unreliableProcessor, 3)

	errorMsgs := make(chan message.Message, 6)
	go func() {
		defer close(errorMsgs)
		for i := 1; i <= 6; i++ {
			msg := message.NewMessage().WithPayload("demo", fmt.Sprintf("Error Test %d", i), fmt.Sprintf("msg-%d", i))
			errorMsgs <- *msg
		}
	}()

	ctx6, cancel6 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel6()

	fmt.Println("Processing with unreliable processor (70% failure rate)...")
	fmt.Println("Note: Errors are logged but processing continues...")
	start = time.Now()
	if err := unreliableRunner.Run(ctx6, errorMsgs); err != nil {
		log.Printf("Unreliable processing error: %v", err)
	}
	fmt.Printf("Unreliable processing completed in: %v\n", time.Since(start))

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
	fmt.Println("The Runner provides:")
	fmt.Println("- Sequential and concurrent message processing")
	fmt.Println("- Proper error handling and logging")
	fmt.Println("- Context cancellation support")
	fmt.Println("- Graceful worker shutdown")
	fmt.Println("- Extensible processor interface")
	fmt.Println("- Functional programming support")
	fmt.Println("- Processor chaining for middleware patterns")
}
