package runtime

import (
	"errors"
	"fmt"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// An embedded node's failure names that node whatever its processor returned, so a result's error
// detail says which node failed (Olympus workplans/connector D9: a connector step's errorStep).
func TestEmbeddedNodeFailureNamesTheNode(t *testing.T) {
	cause := errors.New("node x: JavaScript execution error: boom")
	err := fmt.Errorf("embedded node processing failed: %w", &EmbeddedNodeFailureError{
		FailedNodeID: "n1", FailedNodeLabel: "Lookup", FailedNodePluginType: "plugin-js", Cause: cause})
	var named interface {
		FailedNode() (nodeID, label, pluginType string)
	}
	if !errors.As(err, &named) {
		t.Fatal("the failure must expose FailedNode")
	}
	if id, label, plugin := named.FailedNode(); id != "n1" || label != "Lookup" || plugin != "plugin-js" {
		t.Fatalf("FailedNode() = %q %q %q", id, label, plugin)
	}
}

// A coded failure is never retried: an author's Error step, or data that breaks a schema, fails
// the same way every time.
func TestCodedErrorIsPermanent(t *testing.T) {
	err := NewProcessingError("n1", "Start", "plugin-connector-input", -1, "execute",
		&CodedError{Code: "CONNECTOR_INPUT_INVALID", Message: "required field missing"})
	if message.IsTransientError(err) {
		t.Fatal("a coded failure must not be retried")
	}
}
