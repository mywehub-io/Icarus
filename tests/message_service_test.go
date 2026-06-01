package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/client"
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

	// Create a mock NATS message for acknowledgment
	natsMsg := &nats.Msg{
		Subject: "test.subject",
		Reply:   "test.reply",
		Data:    []byte("test data"),
	}

	// Test ReportSuccess (will fail without Temporal metadata, which is expected)
	err := c.Messages.ReportSuccess(ctx, *resultMessage, natsMsg)
	// Expect error: missing Temporal callback metadata
	if err == nil {
		t.Error("Expected error when Temporal callback metadata is missing")
	}

	// Test ReportSuccess without NATS message (will also fail without Temporal metadata)
	err = c.Messages.ReportSuccess(ctx, *resultMessage, nil)
	if err == nil {
		t.Error("Expected error when Temporal callback metadata is missing")
	}
}

func TestMessageServiceReportError(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	workflowID := "workflow-123"
	runID := "run-456"
	errorMsg := fmt.Errorf("processing failed")

	// Create a mock NATS message for acknowledgment
	executionID := "exec-" + uuid.New().String()

	natsMsg := &nats.Msg{
		Subject: "test.subject",
		Reply:   "test.reply",
		Data:    []byte("test data"),
	}

	// Test ReportError (should succeed with JetStream)
	err := c.Messages.ReportError(ctx, executionID, workflowID, runID, "", errorMsg, natsMsg)
	if err != nil {
		t.Errorf("ReportError failed: %v", err)
	}

	// Test ReportError without NATS message (should also succeed)
	err = c.Messages.ReportError(ctx, executionID, workflowID, runID, "", errorMsg, nil)
	if err != nil {
		t.Errorf("ReportError failed: %v", err)
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

	// Test with message missing Temporal callback metadata (new requirement)
	// Framework auto-populates timestamps but Temporal metadata is required
	invalidMessage2 := &message.Message{
		Workflow: &message.Workflow{WorkflowID: "test", RunID: "test"},
		Payload: func() *message.Payload {
			data := "data"
			return &message.Payload{InlineData: &data}
		}(),
		UpdatedAt: time.Now().Format(time.RFC3339),
	}
	err = c.Messages.ReportSuccess(ctx, *invalidMessage2, nil)
	// Expect error: missing Temporal callback metadata
	if err == nil {
		t.Error("Expected error when Temporal callback metadata is missing")
	}

	// Test with message missing UpdatedAt and Temporal metadata
	invalidMessage3 := &message.Message{
		Workflow: &message.Workflow{WorkflowID: "test", RunID: "test"},
		Payload: func() *message.Payload {
			data := "data"
			return &message.Payload{InlineData: &data}
		}(),
		CreatedAt: time.Now().Format(time.RFC3339),
	}
	err = c.Messages.ReportSuccess(ctx, *invalidMessage3, nil)
	// Expect error: missing Temporal callback metadata
	if err == nil {
		t.Error("Expected error when Temporal callback metadata is missing")
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

	// Test ReportError validation
	// These tests verify that errors are properly handled when client is missing

	executionID := "exec-" + uuid.New().String()

	// Test with empty workflow ID (will fail due to missing Temporal client)
	err := c.Messages.ReportError(ctx, executionID, "", "run-123", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error when Temporal client not initialized")
	}

	// Test with empty run ID (will fail due to missing Temporal client)
	err = c.Messages.ReportError(ctx, executionID, "workflow-123", "", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error when Temporal client not initialized")
	}
}

// mockJSContextWithErrors extends mockJSContext to simulate publish errors
type mockJSContextWithErrors struct {
	*mockJSContext
	publishError error
}

func (m *mockJSContextWithErrors) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if m.publishError != nil {
		return nil, m.publishError
	}
	return m.mockJSContext.Publish(subj, data, opts...)
}

func TestMessageServiceReportWithPublishError(t *testing.T) {
	// Create mock with publish error
	mockJS := &mockJSContextWithErrors{
		mockJSContext: &mockJSContext{},
		publishError:  errors.New("publish failed"),
	}

	c := client.NewClientWithJSContext(mockJS)
	ctx := context.Background()

	// Test ReportSuccess with publish error
	resultMessage := message.NewWorkflowMessage("workflow-123", "run-456").
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

func TestMessageServicePullMessagesValidation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Test with empty stream name
	_, err := c.Messages.PullMessages(ctx, "", "", "consumer", 10)
	if err == nil {
		t.Error("Expected error for empty stream name")
	}

	// Test with empty consumer name
	_, err = c.Messages.PullMessages(ctx, "", "stream", "", 10)
	if err == nil {
		t.Error("Expected error for empty consumer name")
	}

	// Test with zero batch size (should default to 10)
	messages, err := c.Messages.PullMessages(ctx, "", "stream", "consumer", 0)
	if err != nil {
		t.Errorf("PullMessages with zero batch size failed: %v", err)
	}
	// Should not error, batch size should be defaulted
	_ = messages
}

func TestMessageServicePublishValidation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	msg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("test data")

	// Test with empty subject
	err := c.Messages.Publish(ctx, "", msg)
	if err == nil {
		t.Error("Expected error for empty subject")
	}

	// Test with nil message
	err = c.Messages.Publish(ctx, "test.subject", nil)
	if err == nil {
		t.Error("Expected error for nil message")
	}
}

func TestMessageServiceContextCancellation(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("test data")

	// Test Publish with cancelled context
	err := c.Messages.Publish(ctx, "test.subject", msg)
	if err == nil {
		t.Error("Expected error for cancelled context in Publish")
	}

	// Test PullMessages with cancelled context
	_, err = c.Messages.PullMessages(ctx, "", "stream", "consumer", 10)
	if err == nil {
		t.Error("Expected error for cancelled context in PullMessages")
	}

	// Test ReportSuccess with cancelled context
	resultMessage := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("success result")

	err = c.Messages.ReportSuccess(ctx, *resultMessage, nil)
	if err == nil {
		t.Error("Expected error for cancelled context in ReportSuccess")
	}

	executionID := "exec-" + uuid.New().String()

	// Test ReportError with cancelled context
	err = c.Messages.ReportError(ctx, executionID, "workflow-123", "run-456", "", fmt.Errorf("error message"), nil)
	if err == nil {
		t.Error("Expected error for cancelled context in ReportError")
	}
}

func TestMessageServiceTimeout(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())

	// Create a context with very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for timeout
	time.Sleep(1 * time.Millisecond)

	msg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload("test data")

	// Test operations with timed out context
	err := c.Messages.Publish(ctx, "test.subject", msg)
	if err == nil {
		t.Error("Expected timeout error in Publish")
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
