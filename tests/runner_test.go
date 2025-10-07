package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
)

// TestProcessor is a realistic processor for integration testing
type TestProcessor struct {
	name         string
	processDelay time.Duration
	shouldFail   bool
	failCount    int
	processed    []message.Message
	mu           sync.Mutex
}

func NewTestProcessor(name string, delay time.Duration) *TestProcessor {
	return &TestProcessor{
		name:         name,
		processDelay: delay,
		processed:    make([]message.Message, 0),
	}
}

func (p *TestProcessor) WithFailure(failCount int) *TestProcessor {
	p.shouldFail = true
	p.failCount = failCount
	return p
}

func (p *TestProcessor) Process(ctx context.Context, msg message.Message) error {
	// Check context cancellation first
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Simulate processing delay
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.processDelay):
	}

	// Record the message
	p.mu.Lock()
	p.processed = append(p.processed, msg)
	shouldFail := p.shouldFail && len(p.processed) <= p.failCount
	p.mu.Unlock()

	if shouldFail {
		return fmt.Errorf("[%s] simulated processing failure for message: %s", p.name, msg.Payload.Data)
	}

	return nil
}

func (p *TestProcessor) GetProcessed() []message.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]message.Message, len(p.processed))
	copy(result, p.processed)
	return result
}

func (p *TestProcessor) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.processed = p.processed[:0]
}

// TestRunnerEndToEndProcessing tests complete message processing workflows
func TestRunnerEndToEndProcessing(t *testing.T) {
	processor := NewTestProcessor("EndToEnd", 10*time.Millisecond)

	runner := runner.NewRunner(processor, 2)

	// Create a realistic message stream
	msgs := make(chan message.Message, 10)
	go func() {
		defer close(msgs)
		for i := 1; i <= 5; i++ {
			msg := message.NewMessage().
				WithPayload("test-source", fmt.Sprintf("message-%d", i), fmt.Sprintf("id-%d", i)).
				WithNode(fmt.Sprintf("node-%d", i), map[string]interface{}{
					"type": "processor",
					"config": map[string]string{
						"mode": "test",
					},
				}).
				WithOutput("processed-stream")

			msgs <- *msg
			time.Sleep(5 * time.Millisecond) // Simulate realistic message arrival
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Run the processing
	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error in end-to-end processing, got: %v", err)
	}

	// Verify all messages were processed
	processed := processor.GetProcessed()
	if len(processed) != 5 {
		t.Errorf("Expected 5 processed messages, got %d", len(processed))
	}

	// Verify message integrity
	for i, msg := range processed {
		expectedData := fmt.Sprintf("message-%d", i+1)
		expectedID := fmt.Sprintf("id-%d", i+1)
		expectedNodeID := fmt.Sprintf("node-%d", i+1)

		if msg.Payload.Data != expectedData {
			t.Errorf("Message %d: expected data %s, got %s", i, expectedData, msg.Payload.Data)
		}
		if msg.Payload.Reference != expectedID {
			t.Errorf("Message %d: expected reference %s, got %s", i, expectedID, msg.Payload.Reference)
		}
		if msg.Node.NodeID != expectedNodeID {
			t.Errorf("Message %d: expected node ID %s, got %s", i, expectedNodeID, msg.Node.NodeID)
		}
		if msg.Output.DestinationType != "processed-stream" {
			t.Errorf("Message %d: expected output destination 'processed-stream', got %s", i, msg.Output.DestinationType)
		}
	}
}

// TestRunnerConcurrentProcessingIntegration tests concurrent processing with realistic load
func TestRunnerConcurrentProcessingIntegration(t *testing.T) {
	processor := NewTestProcessor("Concurrent", 50*time.Millisecond)

	runner := runner.NewRunner(processor, 3)

	// Create a larger message stream to test concurrent processing
	msgs := make(chan message.Message, 20)
	go func() {
		defer close(msgs)
		for i := 1; i <= 12; i++ {
			msg := message.NewMessage().
				WithPayload("concurrent-test", fmt.Sprintf("concurrent-msg-%d", i), fmt.Sprintf("batch-%d", i)).
				WithNode("concurrent-node", map[string]interface{}{
					"batch":   true,
					"workers": 3,
				})

			msgs <- *msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := runner.Run(ctx, msgs)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Expected no error in concurrent processing, got: %v", err)
	}

	// Verify all messages were processed
	processed := processor.GetProcessed()
	if len(processed) != 12 {
		t.Errorf("Expected 12 processed messages, got %d", len(processed))
	}

	// With 3 workers and 50ms processing time, should complete faster than sequential
	// Sequential would take ~600ms, concurrent should be faster
	if duration > 400*time.Millisecond {
		t.Logf("Concurrent processing took %v - consider if this is expected", duration)
	}
}

// TestRunnerContextCancellationIntegration tests context cancellation in realistic scenarios
func TestRunnerContextCancellationIntegration(t *testing.T) {
	processor := NewTestProcessor("Cancellation", 200*time.Millisecond)

	runner := runner.NewRunner(processor, 2)

	// Create messages that will take longer than our context timeout
	msgs := make(chan message.Message, 10)
	go func() {
		defer close(msgs)
		for i := 1; i <= 10; i++ {
			msg := message.NewMessage().
				WithPayload("cancellation-test", fmt.Sprintf("slow-msg-%d", i), fmt.Sprintf("slow-id-%d", i))
			msgs <- *msg
		}
	}()

	// Short timeout to force cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err == nil {
		t.Error("Expected context cancellation error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected deadline exceeded error, got: %v", err)
	}

	// Should have processed some messages before cancellation
	processed := processor.GetProcessed()
	if len(processed) == 0 {
		t.Error("Expected some messages to be processed before cancellation")
	}
	if len(processed) >= 10 {
		t.Error("Expected cancellation to prevent processing all messages")
	}
}

// TestRunnerErrorHandlingIntegration tests error handling and recovery
func TestRunnerErrorHandlingIntegration(t *testing.T) {
	processor := NewTestProcessor("ErrorHandling", 20*time.Millisecond).WithFailure(2) // Fail first 2 messages

	runner := runner.NewRunner(processor, 1)

	msgs := make(chan message.Message, 5)
	go func() {
		defer close(msgs)
		for i := 1; i <= 5; i++ {
			msg := message.NewMessage().
				WithPayload("error-test", fmt.Sprintf("error-msg-%d", i), fmt.Sprintf("error-id-%d", i))
			msgs <- *msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Runner should not return error even when processors fail
	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error from runner (errors are logged), got: %v", err)
	}

	// All messages should be processed despite errors
	processed := processor.GetProcessed()
	if len(processed) != 5 {
		t.Errorf("Expected all 5 messages to be processed despite errors, got %d", len(processed))
	}
}

// TestRunnerFunctionalProcessorIntegration tests functional processors in integration
func TestRunnerFunctionalProcessorIntegration(t *testing.T) {
	var processed []message.Message
	var mu sync.Mutex

	// Create a functional processor that validates and transforms messages
	processor := runner.ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Validate message has required fields
		if msg.Payload == nil || msg.Payload.Data == "" {
			return errors.New("message missing payload")
		}

		// Transform message (simulate business logic)
		transformed := msg
		if msg.Node.NodeID != "" {
			transformed.Node.Configuration = map[string]interface{}{
				"processed": true,
				"timestamp": time.Now().Unix(),
			}
		}

		mu.Lock()
		processed = append(processed, transformed)
		mu.Unlock()

		return nil
	})

	runner := runner.NewRunner(processor, 2)

	msgs := make(chan message.Message, 4)
	go func() {
		defer close(msgs)
		for i := 1; i <= 4; i++ {
			msg := message.NewMessage().
				WithPayload("functional-test", fmt.Sprintf("func-msg-%d", i), fmt.Sprintf("func-id-%d", i)).
				WithNode(fmt.Sprintf("func-node-%d", i), map[string]interface{}{
					"stage": "input",
				})
			msgs <- *msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error with functional processor, got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 4 {
		t.Errorf("Expected 4 processed messages, got %d", len(processed))
	}

	// Verify transformation was applied
	for _, msg := range processed {
		if config, ok := msg.Node.Configuration.(map[string]interface{}); ok {
			if processed, exists := config["processed"]; !exists || processed != true {
				t.Error("Expected processed flag to be set to true")
			}
			if _, exists := config["timestamp"]; !exists {
				t.Error("Expected timestamp to be added")
			}
		} else {
			t.Error("Expected configuration to be updated")
		}
	}
}

// TestRunnerChainedProcessorsIntegration tests processor chaining in integration
func TestRunnerChainedProcessorsIntegration(t *testing.T) {
	var calls []string
	var mu sync.Mutex

	validator := runner.ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "validate")
		mu.Unlock()

		if msg.Payload == nil || msg.Payload.Data == "" {
			return errors.New("validation failed: empty payload")
		}
		return nil
	})

	transformer := runner.ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "transform")
		mu.Unlock()

		// Simulate transformation
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	persister := runner.ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "persist")
		mu.Unlock()

		return nil
	})

	chained := runner.Chain(validator, transformer, persister)
	runner := runner.NewRunner(chained, 1) // Use 1 worker to ensure sequential processing per message

	msgs := make(chan message.Message, 3)
	go func() {
		defer close(msgs)
		for i := 1; i <= 3; i++ {
			msg := message.NewMessage().
				WithPayload("chain-test", fmt.Sprintf("chain-msg-%d", i), fmt.Sprintf("chain-id-%d", i))
			msgs <- *msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error with chained processors, got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	// Should have 9 calls total (3 processors × 3 messages)
	expectedCalls := 9
	if len(calls) != expectedCalls {
		t.Errorf("Expected %d processor calls, got %d", expectedCalls, len(calls))
	}

	// Verify call order per message (validate -> transform -> persist)
	for i := 0; i < 3; i++ {
		baseIdx := i * 3
		if baseIdx+2 >= len(calls) {
			continue
		}
		if calls[baseIdx] != "validate" || calls[baseIdx+1] != "transform" || calls[baseIdx+2] != "persist" {
			t.Errorf("Expected validate->transform->persist sequence for message %d, got: %v", i,
				calls[baseIdx:baseIdx+3])
		}
	}
}

// TestRunnerWorkflowMessagesIntegration tests processing workflow messages
func TestRunnerWorkflowMessagesIntegration(t *testing.T) {
	processor := NewTestProcessor("Workflow", 15*time.Millisecond)

	runner := runner.NewRunner(processor, 1)

	msgs := make(chan message.Message, 3)
	go func() {
		defer close(msgs)

		// Create workflow messages
		workflowMsg1 := message.NewWorkflowMessage("workflow-123", "run-456").
			WithPayload("workflow-source", "task-1", "ref-1").
			WithNode("task-node", map[string]interface{}{"task": "process"})

		workflowMsg2 := message.NewWorkflowMessage("workflow-123", "run-456").
			WithPayload("workflow-source", "task-2", "ref-2").
			WithNode("result-node", map[string]interface{}{"result": "complete"})

		regularMsg := message.NewMessage().
			WithPayload("regular-source", "regular-data", "regular-ref")

		msgs <- *workflowMsg1
		msgs <- *workflowMsg2
		msgs <- *regularMsg
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error processing workflow messages, got: %v", err)
	}

	processed := processor.GetProcessed()
	if len(processed) != 3 {
		t.Errorf("Expected 3 processed messages, got %d", len(processed))
	}

	// Verify workflow messages
	if processed[0].Workflow.WorkflowID != "workflow-123" {
		t.Errorf("Expected workflow ID 'workflow-123', got %s", processed[0].Workflow.WorkflowID)
	}
	if processed[0].Workflow.RunID != "run-456" {
		t.Errorf("Expected run ID 'run-456', got %s", processed[0].Workflow.RunID)
	}
	if processed[1].Workflow.WorkflowID != "workflow-123" {
		t.Errorf("Expected workflow ID 'workflow-123', got %s", processed[1].Workflow.WorkflowID)
	}

	// Verify regular message doesn't have workflow data
	if processed[2].Workflow != nil {
		t.Error("Expected regular message to not have workflow data")
	}
}
