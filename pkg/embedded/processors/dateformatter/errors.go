package dateformatter

import "fmt"

// ConfigError represents a configuration validation error.
type ConfigError struct {
	NodeID  string
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("node %s: config error [%s]: %s", e.NodeID, e.Field, e.Message)
	}
	return fmt.Sprintf("node %s: config error: %s", e.NodeID, e.Message)
}

func NewConfigError(nodeID, field, message string) *ConfigError {
	return &ConfigError{NodeID: nodeID, Field: field, Message: message}
}

// ParseError represents a date parsing error.
type ParseError struct {
	NodeID    string
	ItemIndex int
	Input     string
	Format    string
	Message   string
	Err       error
}

// Error never echoes the value, nor the wrapped time.ParseError, which quotes it: a date can be
// a date of birth, and this text reaches node output and logs. The value's length is enough to
// tell an empty or truncated input from a wrong format.
func (e *ParseError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: parse error at item %d: failed to parse a %d-character value with format '%s': %s",
			e.NodeID, e.ItemIndex, len(e.Input), e.Format, e.Message)
	}
	return fmt.Sprintf("node %s: parse error: failed to parse a %d-character value with format '%s': %s",
		e.NodeID, len(e.Input), e.Format, e.Message)
}

func (e *ParseError) Unwrap() error { return e.Err }

func NewParseError(nodeID string, itemIndex int, input, format, message string, err error) *ParseError {
	return &ParseError{NodeID: nodeID, ItemIndex: itemIndex, Input: input, Format: format, Message: message, Err: err}
}

// TimezoneError represents a timezone-related error.
type TimezoneError struct {
	NodeID    string
	ItemIndex int
	Timezone  string
	Message   string
	Err       error
}

func (e *TimezoneError) Error() string {
	if e.ItemIndex >= 0 {
		if e.Err != nil {
			return fmt.Sprintf("node %s: timezone error at item %d [%s]: %s (%v)", e.NodeID, e.ItemIndex, e.Timezone, e.Message, e.Err)
		}
		return fmt.Sprintf("node %s: timezone error at item %d [%s]: %s", e.NodeID, e.ItemIndex, e.Timezone, e.Message)
	}
	if e.Err != nil {
		return fmt.Sprintf("node %s: timezone error [%s]: %s (%v)", e.NodeID, e.Timezone, e.Message, e.Err)
	}
	return fmt.Sprintf("node %s: timezone error [%s]: %s", e.NodeID, e.Timezone, e.Message)
}

func (e *TimezoneError) Unwrap() error { return e.Err }

func NewTimezoneError(nodeID string, itemIndex int, timezone, message string, err error) *TimezoneError {
	return &TimezoneError{NodeID: nodeID, ItemIndex: itemIndex, Timezone: timezone, Message: message, Err: err}
}

// InputError represents an invalid input error.
type InputError struct {
	NodeID    string
	ItemIndex int
	Field     string
	Message   string
}

func (e *InputError) Error() string {
	if e.ItemIndex >= 0 {
		if e.Field != "" {
			return fmt.Sprintf("node %s: input error at item %d [%s]: %s", e.NodeID, e.ItemIndex, e.Field, e.Message)
		}
		return fmt.Sprintf("node %s: input error at item %d: %s", e.NodeID, e.ItemIndex, e.Message)
	}
	if e.Field != "" {
		return fmt.Sprintf("node %s: input error [%s]: %s", e.NodeID, e.Field, e.Message)
	}
	return fmt.Sprintf("node %s: input error: %s", e.NodeID, e.Message)
}

func NewInputError(nodeID string, itemIndex int, field, message string) *InputError {
	return &InputError{NodeID: nodeID, ItemIndex: itemIndex, Field: field, Message: message}
}
