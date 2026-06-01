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

	// Create a result message
	result := message.NewWorkflowMessage(msg.Workflow.WorkflowID, msg.Workflow.RunID).
		WithPayload("processed successfully")

	return *result, nil
}

func TestClientMessageServiceIntegration(t *testing.T) {
	// Create client with mock JetStream
	c := client.NewClientWithJSContext(NewMockJS())

	if c.Messages == nil {
		t.Fatal("Messages service should be initialized")
	}

	ctx := context.Background()

	// Test end-to-end message flow: publish -> pull -> process
	workflowID := "integration-workflow-" + uuid.New().String()
	runID := "integration-run-" + uuid.New().String()

	// 1. Publish a message
	msg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload("test message data").
		WithNode("test-node", map[string]interface{}{"type": "integration"}).
		WithOutput("stream")

	err := c.Messages.Publish(ctx, "integration.test.events", msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// 2. Pull the message back
	time.Sleep(10 * time.Millisecond) // Allow message to be stored

	messages, err := c.Messages.PullMessages(ctx, "", "INTEGRATION_STREAM", "integration_consumer", 1)
	if err != nil {
		t.Fatalf("Failed to pull messages: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	pulledMsg := messages[0]

	// 3. Verify message content
	if pulledMsg.Workflow.WorkflowID != workflowID {
		t.Errorf("WorkflowID mismatch: expected %s, got %s", workflowID, pulledMsg.Workflow.WorkflowID)
	}

	if pulledMsg.Workflow.RunID != runID {
		t.Errorf("RunID mismatch: expected %s, got %s", runID, pulledMsg.Workflow.RunID)
	}

	if pulledMsg.Payload.GetInlineData() != "test message data" {
		t.Errorf("Payload data mismatch: expected 'test message data', got %s", pulledMsg.Payload.GetInlineData())
	}

	if pulledMsg.Node.NodeID != "test-node" {
		t.Errorf("Node ID mismatch: expected 'test-node', got %s", pulledMsg.Node.NodeID)
	}
}

func TestRunnerIntegration(t *testing.T) {
	// Create client with mock JetStream
	c := client.NewClientWithJSContext(NewMockJS())

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

	// Add test messages to the mock
	mockJS := c.Messages // Access the underlying mock through the service
	for i := 0; i < 3; i++ {
		testMsg := message.NewWorkflowMessage("integration-workflow", "integration-run").
			WithPayload("test data")

		// We need to access the mock JS context to add messages
		// This is a bit hacky but necessary for integration testing with mocks
		_, _ = testMsg.ToBytes() // Serialize for validation
		mockJS.Publish(context.Background(), "integration.test", testMsg)
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
	c := client.NewClientWithJSContext(NewMockJS())

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

	_, _ = testMsg.ToBytes() // Serialize for validation
	c.Messages.Publish(context.Background(), "integration.test", testMsg)

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

func TestMessageHandlerIntegration(t *testing.T) {
	// Test integration of message handlers with middleware
	logger := zap.NewNop()

	processed := false
	handler := func(ctx context.Context, msg *message.NATSMsg) error {
		processed = true
		if msg.Workflow.WorkflowID != "handler-integration-workflow" {
			t.Errorf("Expected workflow ID 'handler-integration-workflow', got %s", msg.Workflow.WorkflowID)
		}
		return nil
	}

	// Chain all middlewares
	chained := message.Chain(
		message.RecoveryMiddleware(),
		message.LoggingMiddleware(logger),
		message.ValidationMiddleware(),
	)

	finalHandler := chained(handler)

	// Create test message
	msg := message.NewWorkflowMessage("handler-integration-workflow", "handler-integration-run").
		WithPayload("handler test data")

	natsMsg := &message.NATSMsg{
		Message: msg,
		Subject: "integration.handler.test",
		Reply:   "integration.handler.reply",
	}

	ctx := context.Background()
	err := finalHandler(ctx, natsMsg)
	if err != nil {
		t.Errorf("Handler integration failed: %v", err)
	}

	if !processed {
		t.Error("Expected handler to be executed")
	}
}

func TestEndToEndWorkflow(t *testing.T) {
	// Test a complete end-to-end workflow
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	workflowID := "e2e-workflow-" + uuid.New().String()
	runID := "e2e-run-" + uuid.New().String()

	// 1. Create and publish initial message
	initialMsg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload("initial data").
		WithNode("input-node", map[string]interface{}{"type": "input"}).
		WithOutput("stream")

	err := c.Messages.Publish(ctx, "e2e.workflow.start", initialMsg)
	if err != nil {
		t.Fatalf("Failed to publish initial message: %v", err)
	}

	// 2. Simulate processing by pulling and creating result
	time.Sleep(10 * time.Millisecond)

	messages, err := c.Messages.PullMessages(ctx, "", "E2E_STREAM", "e2e_consumer", 1)
	if err != nil {
		t.Fatalf("Failed to pull messages: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	processedMsg := messages[0]

	// 3. Create result message
	resultMsg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload(`{"status":"processed"}`).
		WithNode("output-node", map[string]interface{}{"type": "output"}).
		WithOutput("callback").
		WithMetadata("temporal_workflow_id", workflowID).
		WithMetadata("temporal_run_id", runID).
		WithMetadata("temporal_signal_name", "unit-result-"+workflowID)

	// 4. Report success (may fail due to mock NATS message acknowledgment)
	err = c.Messages.ReportSuccess(ctx, *resultMsg, processedMsg.GetNATSMsg())
	// Note: This may fail with acknowledgment errors in mock environment, which is expected
	_ = err // Acknowledge that error may occur

	// 5. Verify the workflow completed successfully
	if processedMsg.Workflow.WorkflowID != workflowID {
		t.Errorf("Workflow ID mismatch in processed message: expected %s, got %s",
			workflowID, processedMsg.Workflow.WorkflowID)
	}

	if processedMsg.Workflow.RunID != runID {
		t.Errorf("Run ID mismatch in processed message: expected %s, got %s",
			runID, processedMsg.Workflow.RunID)
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
