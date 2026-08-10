package tests

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/client"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
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
	processFunc func(ctx context.Context, msg *message.Message) (message.Message, error)
	callCount   int
	mu          sync.Mutex
}

func (m *mockProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()

	if m.processFunc != nil {
		return m.processFunc(ctx, msg)
	}
	// Return a default result message
	resultMessage := message.NewMessage().
		WithPayload(`{"status":"processed"}`)

	// Copy workflow information if it exists
	if msg.Workflow != nil {
		resultMessage.Workflow = msg.Workflow
		resultMessage.WithMetadata("temporal_workflow_id", msg.Workflow.WorkflowID)
		resultMessage.WithMetadata("temporal_run_id", msg.Workflow.RunID)
		resultMessage.WithMetadata("temporal_signal_name", "result")
	}

	return *resultMessage, nil
}

func (m *mockProcessor) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func newMockClient() *mockClientWrapper {
	mockJS := NewMockJS()
	c := client.NewClientWithJSContext(mockJS)
	return &mockClientWrapper{
		Client: c,
		mockJS: mockJS,
	}
}

// mockClientWrapper wraps the client to provide access to mock methods
type mockClientWrapper struct {
	*client.Client
	mockJS *MockJS
}

func (m *mockClientWrapper) addMessage(msg *message.Message) {
	m.mockJS.addMessage(msg)
}

func (m *mockClientWrapper) setConsumerError(err error) {
	m.mockJS.setConsumerError(err)
}

func (m *mockClientWrapper) setConsumerErrorBudget(err error, budget int) {
	m.mockJS.setConsumerErrorBudget(err, budget)
}

func (m *mockClientWrapper) stopActiveConsumes() int {
	return m.mockJS.stopActiveConsumes()
}

func (m *mockClientWrapper) consumeStartCount() int {
	return m.mockJS.consumeStartCount()
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
	r, err := runner.NewRunner(mockClient.Client, mockProc, stream, consumer, batchSize, 30*time.Second, createTestLogger(), nil, nil)
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
	_, err := runner.NewRunner(nil, mockProc, "stream", "consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err == nil {
		t.Error("Expected error for nil client")
	}

	// Test nil processor
	_, err = runner.NewRunner(mockClient.Client, nil, "stream", "consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err == nil {
		t.Error("Expected error for nil processor")
	}

	// Test empty stream
	_, err = runner.NewRunner(mockClient.Client, mockProc, "", "consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err == nil {
		t.Error("Expected error for empty stream")
	}

	// Test empty consumer
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err == nil {
		t.Error("Expected error for empty consumer")
	}

	// Test invalid batch size
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "consumer", 0, 30*time.Second, createTestLogger(), nil, nil)
	if err == nil {
		t.Error("Expected error for zero batch size")
	}

	// Test nil logger
	_, err = runner.NewRunner(mockClient.Client, mockProc, "stream", "consumer", 1, 30*time.Second, nil, nil, nil)
	if err == nil {
		t.Error("Expected error for nil logger")
	}
}

func TestRunnerRunWithSuccessfulProcessor(t *testing.T) {
	mockClient := newMockClient()

	// Add test messages
	testMsg := message.NewMessage().
		WithPayload("test data")
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate successful processing
			resultMessage := message.NewMessage().WithPayload(`{"status":"success"}`)
			if msg.Workflow != nil {
				resultMessage.Workflow = msg.Workflow
				resultMessage.WithMetadata("temporal_workflow_id", msg.Workflow.WorkflowID)
				resultMessage.WithMetadata("temporal_run_id", msg.Workflow.RunID)
				resultMessage.WithMetadata("temporal_signal_name", "result")
			}
			return *resultMessage, nil
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
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
		WithPayload("test data")
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate processing failure
			return message.Message{}, errors.New("processing failed")
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
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
		testMsg := message.NewMessage().
			WithPayload("test data")
		mockClient.addMessage(testMsg)
	}

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate successful processing with small delay
			time.Sleep(10 * time.Millisecond)
			resultMessage := message.NewMessage().WithPayload(`{"status":"success"}`)
			if msg.Workflow != nil {
				resultMessage.Workflow = msg.Workflow
				resultMessage.WithMetadata("temporal_workflow_id", msg.Workflow.WorkflowID)
				resultMessage.WithMetadata("temporal_run_id", msg.Workflow.RunID)
				resultMessage.WithMetadata("temporal_signal_name", "result")
			}
			return *resultMessage, nil
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 2, 30*time.Second, createTestLogger(), nil, nil)
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

func TestRunner_recoversFromConsumerFailure(t *testing.T) {
	mockClient := newMockClient()
	// NewRunner's EnsureStream/EnsureConsumer must succeed, so inject the error
	// budget after the runner is constructed.
	mockProc := &mockProcessor{}
	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	mockClient.setConsumerErrorBudget(errors.New("consumer info: nats: connection closed"), 3)

	testMsg := message.NewMessage().WithPayload("test data")
	mockClient.addMessage(testMsg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = r.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if mockProc.getCallCount() < 1 {
		t.Fatalf("expected processor to run after transient consumer resolution errors, got %d calls", mockProc.getCallCount())
	}
}

// TestRunner_restartsConsumeOnClosedWithoutErrHandler verifies that when Consume
// stops and Closed() fires without an ErrHandler callback (as nats.go does for
// e.g. consumer deleted), the supervision loop starts a new Consume and keeps
// processing — matching the old pull-loop continuous-retry behaviour.
func TestRunner_restartsConsumeOnClosedWithoutErrHandler(t *testing.T) {
	mockClient := newMockClient()
	mockProc := &mockProcessor{}
	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	mockClient.addMessage(message.NewMessage().WithPayload("before-stop"))
	deadline := time.Now().Add(3 * time.Second)
	for mockProc.getCallCount() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for first message before Consume stop")
		}
		time.Sleep(10 * time.Millisecond)
	}
	startsBefore := mockClient.consumeStartCount()
	if startsBefore < 1 {
		t.Fatalf("expected at least one Consume start, got %d", startsBefore)
	}

	if n := mockClient.stopActiveConsumes(); n < 1 {
		t.Fatalf("expected to stop at least one active Consume, stopped %d", n)
	}

	mockClient.addMessage(message.NewMessage().WithPayload("after-restart"))
	deadline = time.Now().Add(5 * time.Second)
	for mockProc.getCallCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for message after Closed()-triggered restart; calls=%d consumeStarts=%d",
				mockProc.getCallCount(), mockClient.consumeStartCount())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if mockClient.consumeStartCount() <= startsBefore {
		t.Fatalf("expected a new Consume after Closed(), starts before=%d after=%d",
			startsBefore, mockClient.consumeStartCount())
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not exit after cancel")
	}
}

func TestRunnerRunWithConsumerError(t *testing.T) {
	mockClient := newMockClient()

	mockProc := &mockProcessor{}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	// Set up a persistent consumer resolution error after runner creation
	mockClient.setConsumerError(errors.New("consumer resolution failed"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The Run method should not return an error even if the consumer cannot be
	// resolved; it just logs the error and retries, but with timeout it returns
	// context deadline exceeded
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
		WithPayload("test data")
	mockClient.addMessage(testMsg)

	// Set up report error
	mockClient.setReportError(errors.New("report failed"))

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate processing failure to trigger error reporting
			return message.Message{}, errors.New("processing failed")
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
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
		testMsg := message.NewMessage().
			WithPayload("test data")
		mockClient.addMessage(testMsg)
	}

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate slow processing
			time.Sleep(50 * time.Millisecond)
			resultMessage := message.NewMessage().WithPayload(`{"status":"success"}`)
			if msg.Workflow != nil {
				resultMessage.Workflow = msg.Workflow
				resultMessage.WithMetadata("temporal_workflow_id", msg.Workflow.WorkflowID)
				resultMessage.WithMetadata("temporal_run_id", msg.Workflow.RunID)
				resultMessage.WithMetadata("temporal_signal_name", "result")
			}
			return *resultMessage, nil
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
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

	// No messages added, so the consumer delivers nothing

	mockProc := &mockProcessor{}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
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

func TestRunnerMalformedMessageIsNakked(t *testing.T) {
	mockClient := newMockClient()
	malformed := mockClient.mockJS.addRawMessage("test.subject", []byte("not json"))

	mockProc := &mockProcessor{}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = r.Run(ctx)

	if mockProc.getCallCount() != 0 {
		t.Errorf("Expected processor not to be called for malformed message, got %d", mockProc.getCallCount())
	}
	if !malformed.wasNakked() {
		t.Error("Expected malformed message to be NAKked for redelivery")
	}
}

func TestRunnerProcessFailureObserverPermanentError(t *testing.T) {
	var observerCalls atomic.Int32
	mockClient := newMockClient()
	testMsg := message.NewWorkflowMessage("wf-1", "run-1").
		WithMetadata("execution_id", "wf-1-node-1-123").
		WithMetadata("client_id", "client-1").
		WithNode("parent-node", map[string]interface{}{}).
		WithPayload(`{}`)
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			return message.Message{}, sdkerrors.NewBadRequestError("boom", "BOOM", nil)
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil,
		runner.WithProcessFailureObserver(func(ctx context.Context, msg *message.Message, processErr error) error {
			observerCalls.Add(1)
			return nil
		}))
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)
	time.Sleep(150 * time.Millisecond)

	if mockProc.getCallCount() != 1 {
		t.Fatalf("expected processor called once, got %d", mockProc.getCallCount())
	}
	if observerCalls.Load() != 1 {
		t.Fatalf("expected process failure observer once, got %d", observerCalls.Load())
	}
}

func TestRunnerProcessFailureObserverInternalError(t *testing.T) {
	var observerCalls atomic.Int32
	mockClient := newMockClient()
	testMsg := message.NewWorkflowMessage("wf-1", "run-1").
		WithMetadata("execution_id", "wf-1-node-1-123").
		WithMetadata("client_id", "client-1").
		WithNode("parent-node", map[string]interface{}{}).
		WithPayload(`{}`)
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			return message.Message{}, sdkerrors.NewInternalError("", "internal", "INT", nil)
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil,
		runner.WithProcessFailureObserver(func(ctx context.Context, msg *message.Message, processErr error) error {
			observerCalls.Add(1)
			return nil
		}))
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)
	time.Sleep(150 * time.Millisecond)

	if observerCalls.Load() != 1 {
		t.Fatalf("expected process failure observer once after ReportError, got %d", observerCalls.Load())
	}
}

func TestRunnerProcessFailureObserverPlainError(t *testing.T) {
	var observerCalls atomic.Int32
	mockClient := newMockClient()
	testMsg := message.NewWorkflowMessage("wf-1", "run-1").
		WithMetadata("execution_id", "wf-1-node-1-123").
		WithMetadata("client_id", "client-1").
		WithNode("parent-node", map[string]interface{}{}).
		WithPayload(`{}`)
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			return message.Message{}, errors.New("plain failure")
		},
	}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil,
		runner.WithProcessFailureObserver(func(ctx context.Context, msg *message.Message, processErr error) error {
			observerCalls.Add(1)
			return nil
		}))
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)
	time.Sleep(150 * time.Millisecond)

	if observerCalls.Load() != 1 {
		t.Fatalf("expected process failure observer once for non-AppError after ReportError, got %d", observerCalls.Load())
	}
}
