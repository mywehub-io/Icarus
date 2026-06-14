package tests

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
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
		WithPayload( `{"status":"processed"}`)

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

// mockJSContext implements message.JSContext for testing
type mockJSContext struct {
	messages         []*nats.Msg
	pullError        error
	pullErrorBudget  int
	pullAttempts     int
	reportError      error
	mu               sync.Mutex
}

func (m *mockJSContext) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	// For result publishes (now always result.<env>.<exec>), return the configured error.
	if m.reportError != nil && (subj == "result" || strings.HasPrefix(subj, "result.")) {
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

func (m *mockJSContext) StreamInfo(stream string) (*nats.StreamInfo, error) {
	// Always return success for mock
	return &nats.StreamInfo{
		Config: nats.StreamConfig{Name: stream},
		State:  nats.StreamState{},
	}, nil
}

func (m *mockJSContext) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	// Always return success for mock
	return &nats.StreamInfo{
		Config: *cfg,
		State:  nats.StreamState{},
	}, nil
}

func (m *mockJSContext) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	// Always return success for mock
	return &nats.ConsumerInfo{
		Stream: stream,
		Name:   consumer,
		Config: nats.ConsumerConfig{Durable: consumer},
	}, nil
}

func (m *mockJSContext) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	// Always return success for mock
	return &nats.ConsumerInfo{
		Stream: stream,
		Name:   cfg.Durable,
		Config: *cfg,
	}, nil
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
	m.pullErrorBudget = 0
}

func (m *mockJSContext) setPullErrorBudget(err error, budget int) {
	m.pullError = err
	m.pullErrorBudget = budget
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
		m.owner.pullAttempts++
		if m.owner.pullErrorBudget == 0 || m.owner.pullAttempts <= m.owner.pullErrorBudget {
			return nil, m.owner.pullError
		}
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

func (m *mockClientWrapper) setPullErrorBudget(err error, budget int) {
	m.mockJS.setPullErrorBudget(err, budget)
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
		WithPayload( "test data")
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate successful processing
			resultMessage := message.NewMessage().WithPayload( `{"status":"success"}`)
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
		WithPayload( "test data")
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
			WithPayload( "test data")
		mockClient.addMessage(testMsg)
	}

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate successful processing with small delay
			time.Sleep(10 * time.Millisecond)
			resultMessage := message.NewMessage().WithPayload( `{"status":"success"}`)
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

func TestRunner_recoversFromPullFailure(t *testing.T) {
	mockClient := newMockClient()
	mockClient.setPullErrorBudget(errors.New("consumer info: nats: connection closed"), 3)

	testMsg := message.NewMessage().WithPayload("test data")
	mockClient.addMessage(testMsg)

	mockProc := &mockProcessor{}
	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
	if err != nil {
		t.Fatalf("NewRunner failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = r.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if mockProc.getCallCount() < 1 {
		t.Fatalf("expected processor to run after transient pull errors, got %d calls", mockProc.getCallCount())
	}
}

func TestRunnerRunWithPullError(t *testing.T) {
	mockClient := newMockClient()

	// Set up pull error
	mockClient.setPullError(errors.New("pull failed"))

	mockProc := &mockProcessor{}

	r, err := runner.NewRunner(mockClient.Client, mockProc, "test-stream", "test-consumer", 1, 30*time.Second, createTestLogger(), nil, nil)
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
		WithPayload( "test data")
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
			WithPayload( "test data")
		mockClient.addMessage(testMsg)
	}

	mockProc := &mockProcessor{
		processFunc: func(ctx context.Context, msg *message.Message) (message.Message, error) {
			// Simulate slow processing
			time.Sleep(50 * time.Millisecond)
			resultMessage := message.NewMessage().WithPayload( `{"status":"success"}`)
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

	// No messages added, so PullMessages returns empty slice

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
