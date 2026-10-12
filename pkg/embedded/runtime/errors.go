package runtime

import "errors"

// Common errors used throughout the runtime.
var (
	// ErrNoExecutor is returned when no executor is registered for a plugin type.
	ErrNoExecutor = errors.New("no executor registered for plugin type")

	// ErrInvalidInput is returned when the input data is invalid.
	ErrInvalidInput = errors.New("invalid input data")

	// ErrInvalidConfig is returned when the node configuration is invalid.
	ErrInvalidConfig = errors.New("invalid node configuration")

	// ErrProcessingFailed is returned when node processing fails.
	ErrProcessingFailed = errors.New("node processing failed")

	// ErrArrayNotFound is returned when the expected array path is not found.
	ErrArrayNotFound = errors.New("array path not found in output")

	// ErrNotAnArray is returned when the path doesn't contain an array.
	ErrNotAnArray = errors.New("path does not contain an array")

	// ErrKeyNotFound is returned when a key is not found in the output.
	ErrKeyNotFound = errors.New("key not found in output")

	// ErrContextCancelled is returned when the context is cancelled.
	ErrContextCancelled = errors.New("context cancelled")

	// ErrCircuitOpen is returned when the circuit breaker is open.
	ErrCircuitOpen = errors.New("circuit breaker is open")

	// ErrSourceNodeNotFound is returned when the source node output is not found.
	ErrSourceNodeNotFound = errors.New("source node output not found")

	// ErrInvalidFieldMapping is returned when a field mapping is invalid.
	ErrInvalidFieldMapping = errors.New("invalid field mapping")

	// ErrRootArrayNotSupported is returned when input data has a root-level array instead of an object.
	ErrRootArrayNotSupported = errors.New("root of input data must be an object, not an array")
)

// ProcessingError wraps an error with additional context.
type ProcessingError struct {
	// NodeId is the ID of the node that caused the error
	NodeId string
	// NodeLabel is the human-readable name of the node
	NodeLabel string
	// PluginType is the type of the node
	PluginType string
	// ItemIndex is the index of the item being processed (-1 if not array)
	ItemIndex int
	// Phase indicates which phase of processing failed
	Phase string
	// Cause is the underlying error
	Cause error
}

// Error implements the error interface.
func (e *ProcessingError) Error() string {
	if e.ItemIndex >= 0 {
		return "processing error in node " + e.NodeLabel + " (" + e.NodeId + ") " +
			"[" + e.PluginType + "] " +
			"at item " + itoa(e.ItemIndex) + " " +
			"during " + e.Phase + ": " + e.Cause.Error()
	}
	return "processing error in node " + e.NodeLabel + " (" + e.NodeId + ") " +
		"[" + e.PluginType + "] " +
		"during " + e.Phase + ": " + e.Cause.Error()
}

// Unwrap returns the underlying error.
func (e *ProcessingError) Unwrap() error {
	return e.Cause
}

// FailedNode names the node that failed, for message.ReportError's error detail.
func (e *ProcessingError) FailedNode() (nodeID, label, pluginType string) {
	return e.NodeId, e.NodeLabel, e.PluginType
}

// CodedError is an error raised on purpose with an author-chosen code, by the Error plugin. The
// code travels as the run's error code (message.ResultError.Code), and the message stays exactly as
// the author wrote it (ResultError.Detail.Message), which a connector's error event carries
// unchanged (workplans/connector D9).
type CodedError struct {
	Code    string
	Message string
}

// Error renders the code ahead of the message, so the code is visible wherever the error text is.
func (e *CodedError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// ErrorCode returns the author's code.
func (e *CodedError) ErrorCode() string { return e.Code }

// Transient reports that a coded failure is never retried (message.TransientClassifier): an
// author's Error step, or data that breaks a schema, fails the same way every time.
func (e *CodedError) Transient() bool { return false }

// RawMessage returns the author's message without the code.
func (e *CodedError) RawMessage() string { return e.Message }

// NewProcessingError creates a new processing error.
func NewProcessingError(nodeId, nodeLabel, pluginType string, itemIndex int, phase string, cause error) *ProcessingError {
	return &ProcessingError{
		NodeId:     nodeId,
		NodeLabel:  nodeLabel,
		PluginType: pluginType,
		ItemIndex:  itemIndex,
		Phase:      phase,
		Cause:      cause,
	}
}

// itoa converts an int to a string without importing strconv.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	if i < 0 {
		return "-" + itoa(-i)
	}
	var b [20]byte
	bp := len(b) - 1
	for i > 0 {
		b[bp] = byte('0' + i%10)
		bp--
		i /= 10
	}
	return string(b[bp+1:])
}

// IsRetryableError determines if an error is retryable.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Context cancelled is not retryable
	if errors.Is(err, ErrContextCancelled) {
		return false
	}

	// Circuit open means system is overloaded, retry after backoff
	if errors.Is(err, ErrCircuitOpen) {
		return true
	}

	// Processing failed might be retryable
	if errors.Is(err, ErrProcessingFailed) {
		return true
	}

	return false
}

// IsPermanentError determines if an error is permanent (not retryable).
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	// Configuration errors are permanent
	if errors.Is(err, ErrInvalidConfig) {
		return true
	}

	// Invalid input is permanent
	if errors.Is(err, ErrInvalidInput) {
		return true
	}

	// Root array input is permanent
	if errors.Is(err, ErrRootArrayNotSupported) {
		return true
	}

	// Missing executor is permanent
	if errors.Is(err, ErrNoExecutor) {
		return true
	}

	return false
}
