package jsrunner

import (
	"bytes"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dop251/goja"
)

// maxExceptionMessageRunes bounds the exception message a script error carries. The message is
// the script's own text, and a script can put the item's values in it (`throw new Error("no ward
// for " + input.nhsNumber)`, or a runtime message quoting a value); the error becomes node output,
// log text, a span attribute and a Sentry event. 120 runes (counted with the type prefix) keeps a
// typical engine message such as "TypeError: Cannot read property 'x' of undefined" whole while
// cutting long dumps of data. It does not stop a short value from appearing: a script that puts
// data in what it throws still leaks that data.
// The full exception stays reachable, in process only, as ExecutionError.Err.
const maxExceptionMessageRunes = 120

// ExecutionError represents a JavaScript execution error
type ExecutionError struct {
	NodeID    string
	ItemIndex int
	Line      int
	Column    int
	Message   string
	Err       error
}

func (e *ExecutionError) Error() string {
	if e.ItemIndex >= 0 {
		if e.Line > 0 {
			return fmt.Sprintf("node %s: JavaScript execution error at item %d (line %d, col %d): %s",
				e.NodeID, e.ItemIndex, e.Line, e.Column, e.Message)
		}
		return fmt.Sprintf("node %s: JavaScript execution error at item %d: %s",
			e.NodeID, e.ItemIndex, e.Message)
	}
	if e.Line > 0 {
		return fmt.Sprintf("node %s: JavaScript execution error (line %d, col %d): %s",
			e.NodeID, e.Line, e.Column, e.Message)
	}
	return fmt.Sprintf("node %s: JavaScript execution error: %s", e.NodeID, e.Message)
}

func (e *ExecutionError) Unwrap() error {
	return e.Err
}

func NewExecutionError(nodeID, message string, itemIndex, line, column int, err error) *ExecutionError {
	return &ExecutionError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Line:      line,
		Column:    column,
		Message:   message,
		Err:       err,
	}
}

// newScriptExceptionError reports an exception the script threw as goja would, "TypeError: msg at
// script.js:3:7(5)", with the thrown value's text (type and message) bounded by
// maxExceptionMessageRunes. The position suffix is kept as goja writes it: readers of node errors
// (the wehub CLI's run diagnosis) parse it.
func newScriptExceptionError(nodeID string, itemIndex int, ex *goja.Exception) *ExecutionError {
	var b strings.Builder
	b.WriteString(truncateRunes(thrownText(ex.Value()), maxExceptionMessageRunes))
	// Exception.Error writes the first frame when it is script code or a named function.
	if st := ex.Stack(); len(st) > 0 && (st[0].SrcName() != "<native>" || st[0].FuncName() != "<native>") {
		var pos bytes.Buffer
		st[0].Write(&pos)
		b.WriteString(" at ")
		b.WriteString(pos.String())
	}
	return NewExecutionError(nodeID, b.String(), itemIndex, 0, 0, ex)
}

// thrownText is the thrown value's string form: "TypeError: msg" for an Error object.
func thrownText(v goja.Value) (s string) {
	if v == nil {
		return ""
	}
	// A thrown object's toString can itself throw; reading it then panics.
	defer func() {
		if recover() != nil {
			s = "thrown value"
		}
	}()
	return v.String()
}

// truncateRunes cuts s to at most n runes, marking a cut with "... (truncated)".
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n]) + "... (truncated)"
}

// TimeoutError represents a JavaScript execution timeout
type TimeoutError struct {
	NodeID    string
	ItemIndex int
	Message   string
	Timeout   time.Duration
}

func (e *TimeoutError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: %s at item %d (timeout: %v)",
			e.NodeID, e.Message, e.ItemIndex, e.Timeout)
	}
	return fmt.Sprintf("node %s: %s (timeout: %v)", e.NodeID, e.Message, e.Timeout)
}

func NewTimeoutError(nodeID, message string, itemIndex int, timeout time.Duration) *TimeoutError {
	return &TimeoutError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Message:   message,
		Timeout:   timeout,
	}
}

// ConfigError represents a configuration error
type ConfigError struct {
	NodeID  string
	Message string
	Err     error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("node %s: configuration error: %s", e.NodeID, e.Message)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

func NewConfigError(nodeID, message string, err error) *ConfigError {
	return &ConfigError{
		NodeID:  nodeID,
		Message: message,
		Err:     err,
	}
}

// SecurityError represents a security violation
type SecurityError struct {
	NodeID    string
	ItemIndex int
	Message   string
	Err       error
}

func (e *SecurityError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: security violation at item %d: %s",
			e.NodeID, e.ItemIndex, e.Message)
	}
	return fmt.Sprintf("node %s: security violation: %s", e.NodeID, e.Message)
}

func (e *SecurityError) Unwrap() error {
	return e.Err
}

func NewSecurityError(nodeID, message string, itemIndex int, err error) *SecurityError {
	return &SecurityError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Message:   message,
		Err:       err,
	}
}
