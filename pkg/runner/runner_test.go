package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// mockProcessor is a test processor that can be configured to simulate different behaviors
type mockProcessor struct {
	processDelay time.Duration
	shouldFail   bool
	failMessage  string
	processed    []message.Message
	mu           sync.Mutex
}

func newMockProcessor(delay time.Duration, shouldFail bool, failMessage string) *mockProcessor {
	return &mockProcessor{
		processDelay: delay,
		shouldFail:   shouldFail,
		failMessage:  failMessage,
		processed:    make([]message.Message, 0),
	}
}

func (m *mockProcessor) Process(ctx context.Context, msg message.Message) error {
	// Simulate processing delay
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.processDelay):
	}

	// Record the message
	m.mu.Lock()
	m.processed = append(m.processed, msg)
	m.mu.Unlock()

	if m.shouldFail {
		return errors.New(m.failMessage)
	}

	return nil
}

func (m *mockProcessor) getProcessed() []message.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]message.Message, len(m.processed))
	copy(result, m.processed)
	return result
}

func TestNewRunner(t *testing.T) {
	processor := newMockProcessor(0, false, "")
	runner := NewRunner(processor, 3)

	if runner.processor != processor {
		t.Error("Processor not set correctly")
	}
	if runner.numOfWorkers != 3 {
		t.Error("Number of workers not set correctly")
	}
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		processor := newMockProcessor(0, false, "")
		runner := NewRunner(processor, 3)

		err := runner.ValidateConfig()
		if err != nil {
			t.Errorf("Expected no error for valid config, got: %v", err)
		}
	})

	t.Run("nil processor", func(t *testing.T) {
		runner := NewRunner(nil, 1)

		err := runner.ValidateConfig()
		if err == nil {
			t.Error("Expected error for nil processor")
		}
		if !strings.Contains(err.Error(), "processor cannot be nil") {
			t.Errorf("Expected error message about nil processor, got: %v", err)
		}
	})

	t.Run("negative workers", func(t *testing.T) {
		processor := newMockProcessor(0, false, "")
		runner := NewRunner(processor, -1)

		err := runner.ValidateConfig()
		if err == nil {
			t.Error("Expected error for negative workers")
		}
		if !strings.Contains(err.Error(), "cannot be negative") {
			t.Errorf("Expected error message about negative workers, got: %v", err)
		}
	})
}

func TestSequentialProcessing(t *testing.T) {
	processor := newMockProcessor(10*time.Millisecond, false, "")
	runner := NewRunner(processor, 0) // Sequential

	msgs := make(chan message.Message, 3)
	go func() {
		defer close(msgs)
		for i := 1; i <= 3; i++ {
			msg := message.NewMessage().WithPayload("test", fmt.Sprintf("msg-%d", i), fmt.Sprintf("id-%d", i))
			msgs <- *msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	processed := processor.getProcessed()
	if len(processed) != 3 {
		t.Errorf("Expected 3 processed messages, got %d", len(processed))
	}

	// Check that messages were processed in order
	expected := []string{"msg-1", "msg-2", "msg-3"}
	for i, msg := range processed {
		if msg.Payload == nil || msg.Payload.Data != expected[i] {
			t.Errorf("Expected message %s at position %d, got %v", expected[i], i, msg)
		}
	}
}

func TestConcurrentProcessing(t *testing.T) {
	processor := newMockProcessor(50*time.Millisecond, false, "")
	runner := NewRunner(processor, 3) // Concurrent with 3 workers

	msgs := make(chan message.Message, 5)
	go func() {
		defer close(msgs)
		for i := 1; i <= 5; i++ {
			msg := message.NewMessage().WithPayload("test", fmt.Sprintf("msg-%d", i), fmt.Sprintf("id-%d", i))
			msgs <- *msg
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	processed := processor.getProcessed()
	if len(processed) != 5 {
		t.Errorf("Expected 5 processed messages, got %d", len(processed))
	}
}

func TestContextCancellation(t *testing.T) {
	processor := newMockProcessor(200*time.Millisecond, false, "") // Slow processing
	runner := NewRunner(processor, 2)

	msgs := make(chan message.Message, 10)
	go func() {
		defer close(msgs)
		for i := 1; i <= 10; i++ {
			msg := message.NewMessage().WithPayload("test", fmt.Sprintf("msg-%d", i), fmt.Sprintf("id-%d", i))
			msgs <- *msg
		}
	}()

	// Short context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err == nil {
		t.Error("Expected context cancellation error")
	}
	if !strings.Contains(err.Error(), "deadline exceeded") {
		t.Errorf("Expected deadline exceeded error, got: %v", err)
	}
}

func TestErrorHandling(t *testing.T) {
	processor := newMockProcessor(10*time.Millisecond, true, "test error")
	runner := NewRunner(processor, 1)

	msgs := make(chan message.Message, 2)
	go func() {
		defer close(msgs)
		msg1 := message.NewMessage().WithPayload("test", "msg-1", "id-1")
		msg2 := message.NewMessage().WithPayload("test", "msg-2", "id-2")
		msgs <- *msg1
		msgs <- *msg2
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Should not return error - errors are logged but processing continues
	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error (errors are logged), got: %v", err)
	}

	// All messages should still be processed despite errors
	processed := processor.getProcessed()
	if len(processed) != 2 {
		t.Errorf("Expected 2 processed messages despite errors, got %d", len(processed))
	}
}

func TestProcessorFunc(t *testing.T) {
	var processed []message.Message
	var mu sync.Mutex

	processor := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		mu.Lock()
		processed = append(processed, msg)
		mu.Unlock()

		return nil
	})

	runner := NewRunner(processor, 0)

	msgs := make(chan message.Message, 2)
	go func() {
		defer close(msgs)
		msg1 := message.NewMessage().WithPayload("test", "func-msg-1", "id-1")
		msg2 := message.NewMessage().WithPayload("test", "func-msg-2", "id-2")
		msgs <- *msg1
		msgs <- *msg2
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(processed) != 2 {
		t.Errorf("Expected 2 processed messages, got %d", len(processed))
	}
}

func TestChain(t *testing.T) {
	var calls []string
	var mu sync.Mutex

	processor1 := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "proc1")
		mu.Unlock()
		return nil
	})

	processor2 := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "proc2")
		mu.Unlock()
		return nil
	})

	processor3 := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "proc3")
		mu.Unlock()
		return nil
	})

	chained := Chain(processor1, processor2, processor3)
	runner := NewRunner(chained, 0)

	msgs := make(chan message.Message, 1)
	go func() {
		defer close(msgs)
		msg := message.NewMessage().WithPayload("test", "test-msg", "id-1")
		msgs <- *msg
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	expected := []string{"proc1", "proc2", "proc3"}
	if len(calls) != len(expected) {
		t.Errorf("Expected %d calls, got %d", len(expected), len(calls))
	}

	for i, call := range calls {
		if call != expected[i] {
			t.Errorf("Expected call %s at position %d, got %s", expected[i], i, call)
		}
	}
}

func TestChainStopsOnError(t *testing.T) {
	var calls []string
	var mu sync.Mutex

	processor1 := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "proc1")
		mu.Unlock()
		return nil
	})

	processor2 := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "proc2")
		mu.Unlock()
		return errors.New("proc2 failed")
	})

	processor3 := ProcessorFunc(func(ctx context.Context, msg message.Message) error {
		mu.Lock()
		calls = append(calls, "proc3") // Should not be called
		mu.Unlock()
		return nil
	})

	chained := Chain(processor1, processor2, processor3)
	runner := NewRunner(chained, 0)

	msgs := make(chan message.Message, 1)
	go func() {
		defer close(msgs)
		msg := message.NewMessage().WithPayload("test", "test-msg", "id-1")
		msgs <- *msg
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error from Run (errors are logged), got: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	expected := []string{"proc1", "proc2"} // proc3 should not be called
	if len(calls) != len(expected) {
		t.Errorf("Expected %d calls, got %d: %v", len(expected), len(calls), calls)
	}
}

func TestZeroMessages(t *testing.T) {
	processor := newMockProcessor(0, false, "")
	runner := NewRunner(processor, 1)

	msgs := make(chan message.Message)
	close(msgs) // Close immediately - no messages

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := runner.Run(ctx, msgs)
	if err != nil {
		t.Errorf("Expected no error for empty channel, got: %v", err)
	}
}

func BenchmarkSequentialProcessing(b *testing.B) {
	processor := newMockProcessor(1*time.Millisecond, false, "")
	runner := NewRunner(processor, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs := make(chan message.Message, 1)
		go func() {
			defer close(msgs)
			msg := message.NewMessage().WithPayload("test", fmt.Sprintf("msg-%d", i), fmt.Sprintf("id-%d", i))
			msgs <- *msg
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		runner.Run(ctx, msgs)
		cancel()
	}
}

func BenchmarkConcurrentProcessing(b *testing.B) {
	processor := newMockProcessor(1*time.Millisecond, false, "")
	runner := NewRunner(processor, 4)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs := make(chan message.Message, 1)
		go func() {
			defer close(msgs)
			msg := message.NewMessage().WithPayload("test", fmt.Sprintf("msg-%d", i), fmt.Sprintf("id-%d", i))
			msgs <- *msg
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		runner.Run(ctx, msgs)
		cancel()
	}
}
