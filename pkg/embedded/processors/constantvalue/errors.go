package constantvalue

import "fmt"

// ConfigError represents a configuration validation error.
type ConfigError struct {
	NodeID  string
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("node %s: config error [%s]: %s", e.NodeID, e.Field, e.Message)
}

// NewConfigError creates a new ConfigError.
func NewConfigError(nodeID, field, message string) *ConfigError {
	return &ConfigError{
		NodeID:  nodeID,
		Field:   field,
		Message: message,
	}
}
