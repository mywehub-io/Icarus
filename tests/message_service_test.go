package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wehubfusion/Icarus/pkg/client"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.uber.org/zap"
)

func TestMessageServiceCreation(t *testing.T) {
	// Test with valid JSContext
	mockJS := NewMockJS()
	service, err := message.NewMessageService(mockJS, 5, 3, "RESULTS", "result")
	if err != nil {
		t.Fatalf("NewMessageService failed: %v", err)
	}
	if service == nil {
		t.Fatal("Expected service to be created")
	}

	// Test with nil JSContext
	_, err = message.NewMessageService(nil, 5, 3, "RESULTS", "result")
	if err == nil {
		t.Error("Expected error for nil JSContext")
	}
}

func TestMessageServiceSetLogger(t *testing.T) {
	mockJS := NewMockJS()
	service, err := message.NewMessageService(mockJS, 5, 3, "RESULTS", "result")
	if err != nil {
		t.Fatalf("NewMessageService failed: %v", err)
	}

	// Test setting a custom logger
	logger := zap.NewNop()
	service.SetLogger(logger)

	// Test setting nil logger (should not panic)
	service.SetLogger(nil)
}

func TestMessageServiceReportSuccess(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Create a result message
	resultMessage := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("success result")

	// Create a mock JetStream message for acknowledgment
	jsMsg := newMockMsg("test.subject", []byte("test data"))

	// Test ReportSuccess (will fail without execution metadata, which is expected)
	err := c.Messages.ReportSuccess(ctx, *resultMessage, jsMsg)
	// Expect error: missing execution metadata
	if err == nil {
		t.Error("Expected error when execution metadata is missing")
	}

	// Test ReportSuccess without JetStream message (will also fail without execution metadata)
	err = c.Messages.ReportSuccess(ctx, *resultMessage, nil)
	if err == nil {
		t.Error("Expected error when execution metadata is missing")
	}
}

func TestMessageServiceReportSuccessAcksAfterPublish(t *testing.T) {
	mockJS := NewMockJS()
	c := client.NewClientWithJSContext(mockJS)
	ctx := context.Background()

	// Build a fully-populated result message (Payload carries execution context;
	// inline result data must be valid JSON)
	resultMessage := message.NewWorkflowMessage("workflow-123", "run-456").
		WithMetadata("execution_id", "workflow-123-node-1-1700000000000").
		WithNode("node-1", nil).
		WithPayload(`{"status":"success"}`)

	jsMsg := newMockMsg("test.subject", []byte("test data"))

	if err := c.Messages.ReportSuccess(ctx, *resultMessage, jsMsg); err != nil {
		t.Fatalf("ReportSuccess failed: %v", err)
	}

	// The result must be published to the flat result subject...
	subjects := mockJS.publishedSubjects()
	if len(subjects) != 1 || subjects[0] != "result" {
		t.Fatalf("published subjects = %v, want [result]", subjects)
	}
	// ...and the source message ACKed only after the publish succeeded
	if !jsMsg.wasAcked() {
		t.Error("Expected source message to be ACKed after successful result publish")
	}
	if jsMsg.wasNakked() {
		t.Error("Source message must not be NAKed on success")
	}
}

func TestMessageServiceReportError(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	workflowID := "workflow-123"
	runID := "run-456"
	errorMsg := fmt.Errorf("processing failed")

	// Create a mock JetStream message for acknowledgment
	executionID := "exec-" + uuid.New().String()

	jsMsg := newMockMsg("test.subject", []byte("test data"))

	// Test ReportError (should succeed with JetStream)
	err := c.Messages.ReportError(ctx, executionID, workflowID, runID, "", errorMsg, jsMsg)
	if err != nil {
		t.Errorf("ReportError failed: %v", err)
	}

	// Plain errors are transient (internal) → source message must be NAKed for redelivery
	if !jsMsg.wasNakked() {
		t.Error("Expected transient error to NAK the source message")
	}

	// Test ReportError without JetStream message (should also succeed)
	err = c.Messages.ReportError(ctx, executionID, workflowID, runID, "", errorMsg, nil)
	if err != nil {
		t.Errorf("ReportError failed: %v", err)
	}
}

func TestMessageServiceReportErrorPermanentAcks(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	jsMsg := newMockMsg("test.subject", []byte("test data"))

	permanentErr := sdkerrors.NewBadRequestError("bad input", "BAD_INPUT", nil)
	err := c.Messages.ReportError(ctx, "exec-1", "workflow-123", "run-456", "", permanentErr, jsMsg)
	if err != nil {
		t.Fatalf("ReportError failed: %v", err)
	}

	// Permanent (non-internal) errors must ACK to suppress redelivery
	if !jsMsg.wasAcked() {
		t.Error("Expected permanent error to ACK the source message")
	}
	if jsMsg.wasNakked() {
		t.Error("Permanent error must not NAK the source message")
	}
}

func TestMessageServiceReportSuccessValidation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Test with invalid message (missing workflow)
	invalidMessage := message.NewMessage().WithPayload("data")
	err := c.Messages.ReportSuccess(ctx, *invalidMessage, nil)
	if err == nil {
		t.Error("Expected validation error for message without workflow")
	}

	// Test with message missing execution metadata
	invalidMessage2 := &message.Message{
		Workflow: &message.Workflow{WorkflowID: "test", RunID: "test"},
		Payload: func() *message.Payload {
			data := "data"
			return &message.Payload{InlineData: &data}
		}(),
		UpdatedAt: time.Now().Format(time.RFC3339),
	}
	err = c.Messages.ReportSuccess(ctx, *invalidMessage2, nil)
	// Expect error: missing execution metadata
	if err == nil {
		t.Error("Expected error when execution metadata is missing")
	}

	// Test with message missing UpdatedAt and execution metadata
	invalidMessage3 := &message.Message{
		Workflow: &message.Workflow{WorkflowID: "test", RunID: "test"},
		Payload: func() *message.Payload {
			data := "data"
			return &message.Payload{InlineData: &data}
		}(),
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	err = c.Messages.ReportSuccess(ctx, *invalidMessage3, nil)
	// Expect error: missing execution metadata
	if err == nil {
		t.Error("Expected error when execution metadata is missing")
	}

	// Test with message missing payload
	invalidMessage4 := &message.Message{
		Workflow:  &message.Workflow{WorkflowID: "test", RunID: "test"},
		CreatedAt: time.Now().Format(time.RFC3339),
		UpdatedAt: time.Now().Format(time.RFC3339),
	}
	err = c.Messages.ReportSuccess(ctx, *invalidMessage4, nil)
	if err == nil {
		t.Error("Expected validation error for message without payload")
	}
}

func TestMessageServiceReportErrorValidation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	executionID := "exec-" + uuid.New().String()

	// Test with empty workflow ID
	err := c.Messages.ReportError(ctx, executionID, "", "run-123", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error for empty workflow ID")
	}

	// Test with empty run ID
	err = c.Messages.ReportError(ctx, executionID, "workflow-123", "", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error for empty run ID")
	}

	// Test with empty execution ID
	err = c.Messages.ReportError(ctx, "", "workflow-123", "run-123", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error for empty execution ID")
	}
}

// mockJSContextWithErrors extends MockJS to simulate publish errors on all subjects
type mockJSContextWithErrors struct {
	*MockJS
	publishError error
}

func (m *mockJSContextWithErrors) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if m.publishError != nil {
		return nil, m.publishError
	}
	return m.MockJS.Publish(ctx, subject, payload, opts...)
}

func TestMessageServiceReportWithPublishError(t *testing.T) {
	// Create mock with publish error
	mockJS := &mockJSContextWithErrors{
		MockJS:       NewMockJS(),
		publishError: errors.New("publish failed"),
	}

	c := client.NewClientWithJSContext(mockJS)
	ctx := context.Background()

	// Test ReportSuccess with publish error
	resultMessage := message.NewWorkflowMessage("workflow-123", "run-456").
		WithMetadata("execution_id", "workflow-123-node-1-1700000000000").
		WithNode("node-1", nil).
		WithPayload("success result")

	err := c.Messages.ReportSuccess(ctx, *resultMessage, nil)
	if err == nil {
		t.Error("Expected error when publish fails")
	}

	executionID := "exec-" + uuid.New().String()

	// Test ReportError with publish error
	err = c.Messages.ReportError(ctx, executionID, "workflow-123", "run-456", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error when publish fails")
	}
}

func TestMessageServiceContextCancellation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Test ReportSuccess with cancelled context
	resultMessage := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("success result")

	err := c.Messages.ReportSuccess(ctx, *resultMessage, nil)
	if err == nil {
		t.Error("Expected error for cancelled context in ReportSuccess")
	}

	executionID := "exec-" + uuid.New().String()

	// Test ReportError with cancelled context
	err = c.Messages.ReportError(ctx, executionID, "workflow-123", "run-456", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error for cancelled context in ReportError")
	}

	// Test PublishResult with cancelled context
	result := message.NewResultMessage("exec-1", "wf", "run", "node", "success")
	err = c.Messages.PublishResult(ctx, result)
	if err == nil {
		t.Error("Expected error for cancelled context in PublishResult")
	}
}

// capturingJSContext records publish subjects so tests can assert PublishResult
// wire format (flat, tenant-free result subject).
type capturingJSContext struct {
	*MockJS
	subjects []string
	mu       sync.Mutex
}

func (c *capturingJSContext) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	c.mu.Lock()
	c.subjects = append(c.subjects, subject)
	c.mu.Unlock()
	return c.MockJS.Publish(ctx, subject, payload, opts...)
}

// TestPublishResultUsesFlatSubject verifies that PublishResult publishes to the
// flat, tenant-free configured result subject verbatim — no environment or
// execution-id tokens. Zeus correlates results by run/execution ID, not by subject.
func TestPublishResultUsesFlatSubject(t *testing.T) {
	tests := []struct {
		name          string
		resultSubject string
	}{
		{"flat local", "result_development"},
		{"flat uat", "result_uat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := &capturingJSContext{MockJS: NewMockJS()}
			svc, err := message.NewMessageService(js, 5, 3, "RESULTS", tt.resultSubject)
			if err != nil {
				t.Fatalf("NewMessageService failed: %v", err)
			}
			svc.SetLogger(zap.NewNop())

			result := message.NewResultMessage("exec-1", "wf", "run", "node", "success")

			if err := svc.PublishResult(context.Background(), result); err != nil {
				t.Fatalf("PublishResult returned error: %v", err)
			}

			if got := js.subjects; len(got) != 1 || got[0] != tt.resultSubject {
				t.Fatalf("publish subjects = %v, want [%q]", got, tt.resultSubject)
			}
		})
	}
}

func TestExtractNodeIDFromExecutionID(t *testing.T) {
	tests := []struct {
		name        string
		executionID string
		workflowID  string
		want        string
	}{
		{
			name:        "Standard format with UUID workflow and node IDs",
			executionID: "4b45d6f2-ee47-490e-b5c4-86754d95aaa9-8ff422ee-5ed1-421c-8967-1dc1c996b895-1767712791219504964",
			workflowID:  "4b45d6f2-ee47-490e-b5c4-86754d95aaa9",
			want:        "8ff422ee-5ed1-421c-8967-1dc1c996b895",
		},
		{
			name:        "Different workflow and node IDs",
			executionID: "workflow-123-node-456-1234567890",
			workflowID:  "workflow-123",
			want:        "node-456",
		},
		{
			name:        "Node ID without dashes",
			executionID: "workflow-abc123-9876543210",
			workflowID:  "workflow",
			want:        "abc123",
		},
		{
			name:        "ExecutionID without matching workflowID prefix",
			executionID: "different-workflow-node-123-1234567890",
			workflowID:  "mismatch",
			want:        "different-workflow-node-123-1234567890",
		},
		{
			name:        "ExecutionID equals workflowID (no node or timestamp)",
			executionID: "workflow-123",
			workflowID:  "workflow-123",
			want:        "workflow-123",
		},
		{
			name:        "Complex node ID with multiple dashes",
			executionID: "wf-id-my-complex-node-id-with-dashes-9876543210",
			workflowID:  "wf-id",
			want:        "my-complex-node-id-with-dashes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := message.ExtractNodeIDFromExecutionID(tt.executionID, tt.workflowID)
			if got != tt.want {
				t.Errorf("ExtractNodeIDFromExecutionID() = %v, want %v", got, tt.want)
			}
		})
	}
}
