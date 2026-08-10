package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
	"go.uber.org/zap"
)

// integrationProcessor implements the Processor interface for integration testing
type integrationProcessor struct {
	processedMessages []*message.Message
	shouldFail        bool
}

func (p *integrationProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
	if p == nil {
		return message.Message{}, errors.New("integration processor is nil")
	}
	p.processedMessages = append(p.processedMessages, msg)

	if p.shouldFail {
		return message.Message{}, errors.New("integration test failure")
	}

	// Create a result message, carrying the execution context forward so
	// ReportSuccess can publish the result (mirrors real processors)
	result := message.NewWorkflowMessage(msg.Workflow.WorkflowID, msg.Workflow.RunID)
	if execID := msg.Metadata["execution_id"]; execID != "" {
		result.WithMetadata("execution_id", execID)
	}
	result.WithPayload(`{"result":"processed successfully"}`)

	return *result, nil
}

func TestClientMessageServiceIntegration(t *testing.T) {
	// Create client with mock JetStream
	mockJS := NewMockJS()
	c := client.NewClientWithJSContext(mockJS)

	if c.Messages == nil {
		t.Fatal("Messages service should be initialized")
	}

	ctx := context.Background()

	// Test end-to-end message flow: seed a job -> consume -> report success
	workflowID := "integration-workflow-" + uuid.New().String()
	runID := "integration-run-" + uuid.New().String()

	// 1. Seed a job message as it would arrive from the stream
	msg := message.NewWorkflowMessage(workflowID, runID).
		WithMetadata("execution_id", workflowID+"-test-node-1700000000000").
		WithNode("test-node", map[string]interface{}{"type": "integration"}).
		WithPayload(`{"data":"test message data"}`).
		WithOutput("stream")

	data, err := msg.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize message: %v", err)
	}
	jsMsg := newMockMsg("integration.test.events", data)

	// 2. Convert as the runner would
	consumed, err := message.FromJetStreamMsg(jsMsg)
	if err != nil {
		t.Fatalf("FromJetStreamMsg failed: %v", err)
	}

	// 3. Verify message content
	if consumed.Workflow.WorkflowID != workflowID {
		t.Errorf("WorkflowID mismatch: expected %s, got %s", workflowID, consumed.Workflow.WorkflowID)
	}
	if consumed.Workflow.RunID != runID {
		t.Errorf("RunID mismatch: expected %s, got %s", runID, consumed.Workflow.RunID)
	}
	if consumed.Payload.GetInlineData() != `{"data":"test message data"}` {
		t.Errorf("Payload data mismatch: got %s", consumed.Payload.GetInlineData())
	}
	if consumed.Node.NodeID != "test-node" {
		t.Errorf("Node ID mismatch: expected 'test-node', got %s", consumed.Node.NodeID)
	}

	// 4. Report success and verify the result is published + source message ACKed
	if err := c.Messages.ReportSuccess(ctx, *consumed, consumed.GetJetStreamMsg()); err != nil {
		t.Fatalf("ReportSuccess failed: %v", err)
	}
	subjects := mockJS.publishedSubjects()
	if len(subjects) != 1 || subjects[0] != "result" {
		t.Fatalf("published subjects = %v, want [result]", subjects)
	}
	if !jsMsg.wasAcked() {
		t.Error("Expected source message to be ACKed after successful result publish")
	}
}

func TestRunnerIntegration(t *testing.T) {
	// Create client with mock JetStream
	mockJS := NewMockJS()
	c := client.NewClientWithJSContext(mockJS)

	// Create integration processor
	processor := &integrationProcessor{}

	// Create logger
	logger := zap.NewNop()

	// Create runner
	r, err := runner.NewRunner(
		c,
		processor,
		"integration-stream",
		"integration-consumer",
		2,              // batch size
		30*time.Second, // process timeout
		logger,
		nil, // no tracing config
		nil, // no limiter
	)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}

	// Add test messages to the mock queue
	for i := 0; i < 3; i++ {
		testMsg := message.NewWorkflowMessage("integration-workflow", "integration-run").
			WithPayload("test data")
		mockJS.addMessage(testMsg)
	}

	// Run the runner for a short time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = r.Run(ctx)
	// Expect timeout error since we're using a timeout context
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", err)
	}

	// Wait a bit for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify messages were processed
	if len(processor.processedMessages) == 0 {
		t.Error("Expected at least one message to be processed")
	}
}

func TestRunnerWithFailingProcessor(t *testing.T) {
	// Create client with mock JetStream
	mockJS := NewMockJS()
	c := client.NewClientWithJSContext(mockJS)

	// Create failing processor
	processor := &integrationProcessor{shouldFail: true}

	// Create logger
	logger := zap.NewNop()

	// Create runner
	r, err := runner.NewRunner(
		c,
		processor,
		"integration-stream",
		"integration-consumer",
		1,              // batch size
		30*time.Second, // process timeout
		logger,
		nil, // no tracing config
		nil, // no limiter
	)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}

	// Add a test message
	testMsg := message.NewWorkflowMessage("failing-workflow", "failing-run").
		WithPayload("test data")
	mockJS.addMessage(testMsg)

	// Run the runner for a short time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = r.Run(ctx)
	// Expect timeout error since we're using a timeout context
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", err)
	}

	// Wait a bit for processing to complete
	time.Sleep(100 * time.Millisecond)

	// Verify message was processed (even though it failed)
	if len(processor.processedMessages) == 0 {
		t.Error("Expected message to be processed (even if it failed)")
	}
}

func TestEndToEndWorkflow(t *testing.T) {
	// Test a complete end-to-end workflow through the runner: seed job message,
	// process it, and verify the result lands on the flat result subject.
	mockJS := NewMockJS()
	c := client.NewClientWithJSContext(mockJS)

	workflowID := "e2e-workflow-" + uuid.New().String()
	runID := "e2e-run-" + uuid.New().String()

	initialMsg := message.NewWorkflowMessage(workflowID, runID).
		WithMetadata("execution_id", workflowID+"-input-node-1700000000000").
		WithNode("input-node", map[string]interface{}{"type": "input"}).
		WithPayload("initial data").
		WithOutput("stream")
	mockJS.addMessage(initialMsg)

	processor := &integrationProcessor{}
	r, err := runner.NewRunner(c, processor, "e2e-stream", "e2e-consumer", 1, 30*time.Second, zap.NewNop(), nil, nil)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)

	if len(processor.processedMessages) != 1 {
		t.Fatalf("Expected 1 processed message, got %d", len(processor.processedMessages))
	}

	processedMsg := processor.processedMessages[0]
	if processedMsg.Workflow.WorkflowID != workflowID {
		t.Errorf("Workflow ID mismatch in processed message: expected %s, got %s",
			workflowID, processedMsg.Workflow.WorkflowID)
	}
	if processedMsg.Workflow.RunID != runID {
		t.Errorf("Run ID mismatch in processed message: expected %s, got %s",
			runID, processedMsg.Workflow.RunID)
	}

	// The runner reported success for the processed message; the result must be
	// published to the flat result subject.
	subjects := mockJS.publishedSubjects()
	if len(subjects) == 0 || subjects[0] != "result" {
		t.Errorf("Expected a result publish on 'result', got %v", subjects)
	}
}

func TestErrorReportingWorkflow(t *testing.T) {
	// Test error reporting workflow
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	workflowID := "error-workflow-" + uuid.New().String()
	runID := "error-run-" + uuid.New().String()
	executionID := "exec-" + uuid.New().String()

	// 1. Simulate a processing error
	errorMessage := fmt.Errorf("simulated processing error")

	err := c.Messages.ReportError(ctx, executionID, workflowID, runID, "", errorMessage, nil)
	if err != nil {
		t.Errorf("Failed to report error: %v", err)
	}

	// The error reporting should succeed even without a source message to nak
	// This tests the error reporting pathway in isolation
}
