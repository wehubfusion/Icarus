package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
	"go.uber.org/zap"
)

// createTestLogger creates a no-op logger for testing
func createTestLogger() *zap.Logger {
	return zap.NewNop()
}

// mockProcessor implements the Processor interface for testing
type mockProcessor struct {
	processFunc func(ctx context.Context, msg *message.Message) error
	callCount   int
	mu          sync.Mutex
}

func (m *mockProcessor) Process(ctx context.Context, msg *message.Message) error {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()

	if m.processFunc != nil {
		return m.processFunc(ctx, msg)
	}
	return nil
}

func (m *mockProcessor) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// mockJSContext implements message.JSContext for testing
type mockJSContext struct {
	messages    []*nats.Msg
	pullError   error
	reportError error
	mu          sync.Mutex
}

func (m *mockJSContext) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	// For "result" subject (reporting), return the configured error
	if subj == "result" && m.reportError != nil {
		return nil, m.reportError
	}
	return &nats.PubAck{Stream: "MOCK", Sequence: 1}, nil
}

func (m *mockJSContext) Subscribe(subj string, cb nats.MsgHandler, opts ...nats.SubOpt) (message.JSSubscription, error) {
	return &mockJSSubscription{}, nil
}

func (m *mockJSContext) PullSubscribe(subj, durable string, opts ...nats.SubOpt) (message.JSSubscription, error) {
	return &mockPullJSSubscription{owner: m, durable: durable}, nil
}

func (m *mockJSContext) addMessage(msg *message.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, _ := msg.ToBytes()
	natsMsg := &nats.Msg{
		Subject: "test.subject",
		Data:    data,
		Reply:   "reply.subject",
	}
	m.messages = append(m.messages, natsMsg)
}

func (m *mockJSContext) setPullError(err error) {
	m.pullError = err
}

func (m *mockJSContext) setReportError(err error) {
	m.reportError = err
}

// mockJSSubscription implements message.JSSubscription
type mockJSSubscription struct{}

func (m *mockJSSubscription) Unsubscribe() error         { return nil }
func (m *mockJSSubscription) Drain() error               { return nil }
func (m *mockJSSubscription) IsValid() bool              { return true }
func (m *mockJSSubscription) Pending() (int, int, error) { return 0, 0, nil }
func (m *mockJSSubscription) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return []*nats.Msg{}, nil
}

// mockPullJSSubscription implements pull-based subscription
type mockPullJSSubscription struct {
	owner   *mockJSContext
	durable string
}

func (m *mockPullJSSubscription) Unsubscribe() error         { return nil }
func (m *mockPullJSSubscription) Drain() error               { return nil }
func (m *mockPullJSSubscription) IsValid() bool              { return true }
func (m *mockPullJSSubscription) Pending() (int, int, error) { return 0, 0, nil }

func (m *mockPullJSSubscription) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	if m.owner.pullError != nil {
		return nil, m.owner.pullError
	}

	m.owner.mu.Lock()
	defer m.owner.mu.Unlock()

	if len(m.owner.messages) == 0 {
		return []*nats.Msg{}, nil
	}

	// Return up to batch messages
	size := batch
	if size > len(m.owner.messages) {
		size = len(m.owner.messages)
	}

	result := make([]*nats.Msg, size)
	copy(result, m.owner.messages[:size])

	// Remove returned messages
	m.owner.messages = m.owner.messages[size:]

	return result, nil
}

func newMockClient() *mockClientWrapper {
	mockJS := &mockJSContext{}
	c := client.NewClientWithJSContext(mockJS)
	return &mockClientWrapper{
		Client: c,
		mockJS: mockJS,
	}
}

// mockClientWrapper wraps the client to provide access to mock methods
type mockClientWrapper struct {
	*client.Client
	mockJS *mockJSContext
}

func (m *mockClientWrapper) addMessage(msg *message.Message) {
	m.mockJS.addMessage(msg)
}

func (m *mockClientWrapper) setPullError(err error) {
	m.mockJS.setPullError(err)
}

func (m *mockClientWrapper) setReportError(err error) {
	m.mockJS.setReportError(err)
}

func TestNewRunner(t *testing.T) {
	mockClient := newMockClient()
	mockProc := &mockProcessor{}

	stream := "test-stream"
	consumer := "test-consumer"
	batchSize := 5
	numWorkers := 3

	r, err := runner.NewRunner(mockClient.Client, mockProc, stream, consumer, batchSize, numWorkers, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner returned error: %v", err)
	}

	if r == nil {
		t.Fatal("NewRunner returned nil")
	}

	// We can't directly access private fields, but we can verify the runner works
	// by testing RegisterProcessor and Run methods
}

func TestNewRunnerValidation(t *testing.T) {
	mockClient := newMockClient()
	mockProc := &mockProcessor{}

	// Test nil client
	_, err := runner.NewRunner(nil, mockProc, "stream", "consumer", 1, 1, createTestLogger())
	if err == nil {
		t.Error("Expected error for nil client")
	}

	// Test nil processor
	_, err = runner.NewRunner(mockClient.Client, nil, "stream", "consumer", 1, 1, createTestLogger())
	if err == nil {
		t.Error("Expected error for nil processor")
	}

	// Test empty stream
	_, err = runner.NewRunner(mockClient.Client, mockProc, "", "consumer", 1, 1, createTestLogger())
	if err == nil {
		t.Error("Expected error for empty stream")
	}

	// Test empty consumer
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "", 1, 1, createTestLogger())
	if err == nil {
		t.Error("Expected error for empty consumer")
	}

	// Test invalid batch size
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "consumer", 0, 1, createTestLogger())
	if err == nil {
		t.Error("Expected error for zero batch size")
	}

	// Test invalid worker count
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "consumer", 1, 0, createTestLogger())
	if err == nil {
		t.Error("Expected error for zero workers")
	}

	// Test nil logger
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "consumer", 1, 1, nil)
	if err == nil {
		t.Error("Expected error for nil logger")
	}
}

func TestRunnerRunWithSuccessfulProcessor(t *testing.T) {
	mockClient := newMockClient()

	// Add test messages
	testMsg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("test", "test data", "ref-123")
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) error {
			// Simulate successful processing
			return nil
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 1, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}

	// Wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify the processor was called
	if mockProc.getCallCount() != 1 {
		t.Errorf("Expected processor to be called 1 time, got %d", mockProc.getCallCount())
	}
}

func TestRunnerRunWithFailingProcessor(t *testing.T) {
	mockClient := newMockClient()

	// Add test messages
	testMsg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("test", "test data", "ref-123")
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) error {
			// Simulate processing failure
			return errors.New("processing failed")
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 1, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}

	// Wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify the processor was called
	if mockProc.getCallCount() != 1 {
		t.Errorf("Expected processor to be called 1 time, got %d", mockProc.getCallCount())
	}
}

func TestRunnerRunWithMultipleMessages(t *testing.T) {
	mockClient := newMockClient()

	// Add multiple test messages
	for i := 0; i < 3; i++ {
		testMsg := message.NewWorkflowMessage("workflow-123", "run-456").
			WithPayload("test", "test data", "ref-123")
		mockClient.addMessage(testMsg)
	}

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) error {
			// Simulate successful processing with small delay
			time.Sleep(10 * time.Millisecond)
			return nil
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 2, 2, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}

	// Wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify the processor was called for all messages
	if mockProc.getCallCount() != 3 {
		t.Errorf("Expected processor to be called 3 times, got %d", mockProc.getCallCount())
	}
}

func TestRunnerRunWithPullError(t *testing.T) {
	mockClient := newMockClient()

	// Set up pull error
	mockClient.setPullError(errors.New("pull failed"))

	mockProc := &mockProcessor{}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 1, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The Run method should not return an error even if PullMessages fails,
	// it just logs the error and continues, but with timeout it returns context deadline exceeded
	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}
}

func TestRunnerRunWithReportError(t *testing.T) {
	mockClient := newMockClient()

	// Add test messages
	testMsg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("test", "test data", "ref-123")
	mockClient.addMessage(testMsg)

	// Set up report error
	mockClient.setReportError(errors.New("report failed"))

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) error {
			// Simulate processing failure to trigger error reporting
			return errors.New("processing failed")
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 1, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The Run method should not return an error even if reporting fails,
	// it just logs the error and continues, but with timeout it returns context deadline exceeded
	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}

	// Wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify the processor was called
	if mockProc.getCallCount() != 1 {
		t.Errorf("Expected processor to be called 1 time, got %d", mockProc.getCallCount())
	}
}

func TestRunnerRunContextCancellation(t *testing.T) {
	mockClient := newMockClient()

	// Add multiple messages to ensure processing takes some time
	for i := 0; i < 10; i++ {
		testMsg := message.NewWorkflowMessage("workflow-123", "run-456").
			WithPayload("test", "test data", "ref-123")
		mockClient.addMessage(testMsg)
	}

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) error {
			// Simulate slow processing
			time.Sleep(50 * time.Millisecond)
			return nil
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 1, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}

	// Wait for processing to complete
	time.Sleep(200 * time.Millisecond)

	// The context should have been cancelled, so we might not process all messages
	// but we should process at least some
	if mockProc.getCallCount() == 0 {
		t.Error("Expected processor to be called at least once")
	}
}

func TestRunnerRunEmptyMessages(t *testing.T) {
	mockClient := newMockClient()

	// No messages added, so PullMessages returns empty slice

	mockProc := &mockProcessor{}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 1, createTestLogger())
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runErr := r.Run(ctx)
	// With timeout context, we expect context deadline exceeded
	if runErr == nil {
		t.Error("Expected context deadline exceeded error")
	} else if runErr != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", runErr)
	}

	// Processor should not be called since there are no messages
	if mockProc.getCallCount() != 0 {
		t.Errorf("Expected processor to not be called, got %d calls", mockProc.getCallCount())
	}
}
