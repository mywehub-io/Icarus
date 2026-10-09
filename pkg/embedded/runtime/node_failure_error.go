package runtime

import "fmt"

// EmbeddedNodeFailureError wraps a failure from a single embedded node so callers can
// identify which node failed (ID + label) and unwrap the root cause.
type EmbeddedNodeFailureError struct {
	FailedNodeID         string
	FailedNodeLabel      string
	FailedNodePluginType string
	Cause                error
}

func (e *EmbeddedNodeFailureError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("node %s failed: %s", e.FailedNodeLabel, e.Cause.Error())
	}
	return fmt.Sprintf("node %s failed", e.FailedNodeLabel)
}

// FailedNode names the embedded node that failed, for message.ReportError's error detail, whatever
// error its processor returned.
func (e *EmbeddedNodeFailureError) FailedNode() (nodeID, label, pluginType string) {
	return e.FailedNodeID, e.FailedNodeLabel, e.FailedNodePluginType
}

// Unwrap returns the underlying processor error.
func (e *EmbeddedNodeFailureError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
