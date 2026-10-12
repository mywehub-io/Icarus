package message

import (
	"errors"
	"fmt"
	"testing"
)

type failedNodeErr struct{ cause error }

func (e *failedNodeErr) Error() string { return "processing error: " + e.cause.Error() }
func (e *failedNodeErr) Unwrap() error { return e.cause }
func (e *failedNodeErr) FailedNode() (string, string, string) {
	return "n1", "Reject order", "plugin-error"
}

type codedErr struct{ code, msg string }

func (e *codedErr) Error() string      { return e.code + ": " + e.msg }
func (e *codedErr) ErrorCode() string  { return e.code }
func (e *codedErr) RawMessage() string { return e.msg }

// TestErrorDetailOf pins the structured error detail ReportError attaches (workplans/connector D9).
func TestErrorDetailOf(t *testing.T) {
	if d := errorDetailOf(errors.New("plain")); d != nil {
		t.Fatalf("a plain error carries no detail, got %+v", d)
	}
	wrapped := fmt.Errorf("unit failed: %w", &failedNodeErr{cause: &codedErr{code: "E-42", msg: "bad order"}})
	d := errorDetailOf(wrapped)
	if d == nil {
		t.Fatal("expected a detail")
	}
	want := ErrorDetail{NodeID: "n1", NodeLabel: "Reject order", PluginType: "plugin-error", Code: "E-42", Message: "bad order"}
	if *d != want {
		t.Fatalf("got %+v, want %+v", *d, want)
	}
}
