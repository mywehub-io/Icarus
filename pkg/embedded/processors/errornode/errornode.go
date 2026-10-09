package errornode

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// Config defines the configuration for the ErrorNode.
// It aligns with the Apollo node schema (seed-node-schemas.sql):
// - "label"
// - "default_error_message"
type Config struct {
	Label               string `json:"label"`
	DefaultErrorMessage string `json:"default_error_message"`
	// ErrorCode is the code when the "code" input is empty (workplans/connector D9).
	ErrorCode string `json:"error_code"`
}

// ErrorNode implements an embedded node that always produces an error
// with a configurable message. The message can come from:
// - input data field "message" (preferred)
// - config.DefaultErrorMessage
// - a generic fallback when both are empty
type ErrorNode struct {
	runtime.BaseNode

	// cfgCache holds the parsed configuration, read-only, shared by every worker of the unit.
	cfgCache runtime.ConfigCache[Config]
}

// NewErrorNode creates a new ErrorNode instance.
// It is registered under plugin type "plugin-error".
func NewErrorNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-error" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-error', got '%s'", config.PluginType)
	}

	return &ErrorNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Prepare implements runtime.ConfigPreparer: the configuration is parsed once for the unit.
func (n *ErrorNode) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process evaluates the configuration and input data, then returns
// an error output with the resolved message.
//
// Runtime behavior:
//   - When downstream nodes listen to this node's pluginError section,
//     the runtime will expose:
//       error: true
//       errorDescription: <resolved message>
//   - When there is no pluginError listener, the error bubbled from
//     this node will cause the unit/workflow to fail.
func (n *ErrorNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	cfgp, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		configErr := runtime.NewProcessingError(
			input.NodeId,
			input.Label,
			input.PluginType,
			input.ItemIndex,
			"config",
			fmt.Errorf("failed to parse configuration: %w", err),
		)
		return runtime.ErrorOutput(configErr)
	}
	cfg := *cfgp

	message := resolveMessage(input.Data, cfg.DefaultErrorMessage)
	if message == "" {
		message = fallbackErrorNodeMessage(cfg.Label, input.Label, input.NodeId)
	}

	var baseErr error = errors.New(message)
	if code := resolveCode(input.Data, cfg.ErrorCode); code != "" {
		baseErr = &runtime.CodedError{Code: code, Message: message}
	}
	procErr := runtime.NewProcessingError(
		input.NodeId,
		input.Label,
		input.PluginType,
		input.ItemIndex,
		"execute",
		baseErr,
	)

	return runtime.ErrorOutput(procErr)
}

// resolveCode returns the error code: the "code" input when it is a non-empty string, else the
// configured error_code, else "" (no code, as before codes existed).
func resolveCode(data map[string]interface{}, configured string) string {
	if data != nil {
		if s, ok := data["code"].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return strings.TrimSpace(configured)
}

// resolveMessage determines the final error message using, in order:
//   1. The "message" field from input data (if present and non-empty string)
//   2. The provided defaultErrorMessage (if non-empty)
//   3. Caller-provided generic fallback (handled by caller when this returns "")
func resolveMessage(data map[string]interface{}, defaultErrorMessage string) string {
	if data != nil {
		if v, ok := data["message"]; ok && v != nil {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	if defaultErrorMessage != "" {
		return defaultErrorMessage
	}
	return ""
}

// fallbackErrorNodeMessage is used when neither input "message" nor default_error_message is set.
// Prefer schema label, then runtime label, then node id so the error is attributable in UIs.
func fallbackErrorNodeMessage(schemaLabel, runtimeLabel, nodeID string) string {
	label := strings.TrimSpace(schemaLabel)
	if label == "" {
		label = strings.TrimSpace(runtimeLabel)
	}
	if label == "" {
		label = strings.TrimSpace(nodeID)
	}
	if label == "" {
		return "Error node (plugin-error): no message was provided"
	}
	return fmt.Sprintf("Error node %q (plugin-error): no message was provided", label)
}

