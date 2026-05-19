package constantvalue

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

const pluginType = "plugin-constant-value-generator"

// ConstantValueNode produces a fixed set of typed constant values as output fields.
type ConstantValueNode struct {
	runtime.BaseNode
}

// NewConstantValueNode creates a new constant value generator node.
func NewConstantValueNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != pluginType {
		return nil, fmt.Errorf("invalid plugin type: expected '%s', got '%s'", pluginType, config.PluginType)
	}
	return &ConstantValueNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Process emits each configured constant as an output field keyed by name.
// Input data is ignored; this node has no inputs.
func (n *ConstantValueNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err)))
	}

	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("invalid configuration: %v", err)))
	}

	output := make(map[string]interface{}, len(cfg.Constants))
	for _, entry := range cfg.Constants {
		val, err := entry.CastValue()
		if err != nil {
			return runtime.ErrorOutput(NewConfigError(n.NodeId(), entry.Name, err.Error()))
		}
		output[entry.Name] = val
	}

	return runtime.SuccessOutput(output)
}
