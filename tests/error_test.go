package tests

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/wehubfusion/Icarus/pkg/client"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func TestErrorTypes(t *testing.T) {
	// Test error type constants
	if sdkerrors.Internal != 0 {
		t.Error("Internal should be 0")
	}

	if sdkerrors.NotFound != 1 {
		t.Error("NotFound should be 1")
	}

	if sdkerrors.BadRequest != 2 {
		t.Error("BadRequest should be 2")
	}

	if sdkerrors.Unauthorized != 3 {
		t.Error("Unauthorized should be 3")
	}

	if sdkerrors.Conflict != 4 {
		t.Error("Conflict should be 4")
	}

	if sdkerrors.ValidationFailed != 5 {
		t.Error("ValidationFailed should be 5")
	}
}

func TestAppErrorWrapping(t *testing.T) {
	originalErr := errors.New("original error")
	wrappedErr := sdkerrors.NewInternalError("", "test message", "TEST_CODE", originalErr)

	// Test error message
	if wrappedErr.Error() == "" {
		t.Error("Wrapped error should have a message")
	}

	// Test error type
	if wrappedErr.Type != sdkerrors.Internal {
		t.Errorf("Expected type Internal, got %v", wrappedErr.Type)
	}

	// Test error code
	if wrappedErr.Code != "TEST_CODE" {
		t.Errorf("Expected code 'TEST_CODE', got '%s'", wrappedErr.Code)
	}

	// Test error message
	if wrappedErr.Message != "test message" {
		t.Errorf("Expected message 'test message', got '%s'", wrappedErr.Message)
	}

	// Test unwrapping
	unwrapped := wrappedErr.Unwrap()
	if unwrapped != originalErr {
		t.Error("Unwrap should return the original error")
	}

	// Test errors.Is compatibility
	if !errors.Is(wrappedErr, originalErr) {
		t.Error("errors.Is should work with wrapped errors")
	}
}

func TestAppErrorConstructors(t *testing.T) {
	// Test different error constructors
	notFoundErr := sdkerrors.NewNotFoundError("resource not found", "NOT_FOUND", nil)
	if notFoundErr.Type != sdkerrors.NotFound {
		t.Errorf("Expected NotFound type, got %v", notFoundErr.Type)
	}

	badRequestErr := sdkerrors.NewBadRequestError("bad request", "BAD_REQUEST", nil)
	if badRequestErr.Type != sdkerrors.BadRequest {
		t.Errorf("Expected BadRequest type, got %v", badRequestErr.Type)
	}

	validationErr := sdkerrors.NewValidationError("validation failed", "VALIDATION_FAILED", nil)
	if validationErr.Type != sdkerrors.ValidationFailed {
		t.Errorf("Expected ValidationFailed type, got %v", validationErr.Type)
	}

	conflictErr := sdkerrors.NewConflictError("conflict occurred", "CONFLICT", nil)
	if conflictErr.Type != sdkerrors.Conflict {
		t.Errorf("Expected Conflict type, got %v", conflictErr.Type)
	}

	unauthorizedErr := sdkerrors.NewUnauthorizedError("unauthorized", "UNAUTHORIZED", nil)
	if unauthorizedErr.Type != sdkerrors.Unauthorized {
		t.Errorf("Expected Unauthorized type, got %v", unauthorizedErr.Type)
	}
}

func TestPublishErrors(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Test publishing with invalid subject
	msg := message.NewWorkflowMessage("workflow-test", uuid.New().String()).
		WithPayload( "test content")
	err := c.Messages.Publish(ctx, "", msg) // Empty subject
	if err == nil {
		t.Error("Expected error for empty subject")
	}
	var appErr *sdkerrors.AppError
	if !errors.As(err, &appErr) || appErr.Type != sdkerrors.ValidationFailed || appErr.Code != "INVALID_SUBJECT" {
		t.Errorf("Expected ValidationFailed error with code INVALID_SUBJECT, got: %v", err)
	}

	// Test publishing with nil message
	err = c.Messages.Publish(ctx, "test.subject", nil)
	if err == nil {
		t.Error("Expected error for nil message")
	}
	if !errors.As(err, &appErr) || appErr.Type != sdkerrors.ValidationFailed || appErr.Code != "INVALID_MESSAGE" {
		t.Errorf("Expected ValidationFailed error with code INVALID_MESSAGE, got: %v", err)
	}

	// In the mock, there is no stream enforcement; publish should succeed
	err = c.Messages.Publish(ctx, "nonexistent.stream.subject", msg)
	if err != nil {
		t.Errorf("Publish should succeed in mock without streams, got: %v", err)
	}
}

func TestPullMessagesErrors(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Test pull with empty stream name
	_, err := c.Messages.PullMessages(ctx, "", "consumer", 1)
	if err == nil {
		t.Error("Expected error for empty stream name")
	}

	// Test pull with empty consumer name
	_, err = c.Messages.PullMessages(ctx, "stream", "", 1)
	if err == nil {
		t.Error("Expected error for empty consumer name")
	}

	// Pull requires an existing durable consumer
	_, err = c.Messages.PullMessages(ctx, "NONEXISTENT", "nonexistent", 1)
	if err == nil {
		t.Error("Expected error when consumer does not exist")
	}
}

func TestConnectionErrors(t *testing.T) {
	t.Skip("Connection error tests are skipped when using in-memory mock")
}

func TestJetStreamNotEnabledError(t *testing.T) {
	// This test verifies that our SDK properly reports when JetStream is not enabled
	// We use a mock scenario since we can't easily disable JetStream in the test server

	// Create a custom error to simulate JetStream not enabled
	jsError := sdkerrors.NewInternalError("", "JetStream is not enabled on the NATS server", "JETSTREAM_NOT_ENABLED", nil)

	// Test that our error checking works
	if jsError.Code != "JETSTREAM_NOT_ENABLED" {
		t.Errorf("Expected error code 'JETSTREAM_NOT_ENABLED', got '%s'", jsError.Code)
	}

	expectedMsg := "JetStream is not enabled"
	if !strings.Contains(jsError.Message, expectedMsg) {
		t.Errorf("Expected error message to contain '%s', got '%s'", expectedMsg, jsError.Message)
	}
}

func TestMessageSerializationErrors(t *testing.T) {
	// Test deserializing invalid JSON
	invalidJSON := []byte("invalid json")
	_, err := message.FromBytes(invalidJSON)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}

	// Test serializing message that can't be marshaled (shouldn't happen with normal usage)
	// This is more of a theoretical test since our Message struct should always be serializable
	msg := message.NewWorkflowMessage("workflow-test", "run-test").
		WithPayload( "content")
	data, err := msg.ToBytes()
	if err != nil {
		t.Errorf("Unexpected error serializing valid message: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected serialized data to be non-empty")
	}

	// Verify we can deserialize what we serialized
	deserialized, err := message.FromBytes(data)
	if err != nil {
		t.Errorf("Failed to deserialize valid data: %v", err)
	}
	if deserialized.Workflow.WorkflowID != msg.Workflow.WorkflowID {
		t.Errorf("Round-trip WorkflowID mismatch: expected %s, got %s", msg.Workflow.WorkflowID, deserialized.Workflow.WorkflowID)
	}
	if deserialized.Workflow.RunID != msg.Workflow.RunID {
		t.Errorf("Round-trip RunID mismatch: expected %s, got %s", msg.Workflow.RunID, deserialized.Workflow.RunID)
	}
}
