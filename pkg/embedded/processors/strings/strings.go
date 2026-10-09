package strings

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// StringsNode implements string actions for embedded.
type StringsNode struct {
	runtime.BaseNode

	// cfgCache holds the parsed configuration, read-only, shared by every worker of the unit.
	cfgCache runtime.ConfigCache[Config]
}

// NewStringsNode creates a new strings node.
func NewStringsNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-strings" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-strings', got '%s'", config.PluginType)
	}
	return &StringsNode{BaseNode: runtime.NewBaseNode(config)}, nil
}

// Prepare implements runtime.ConfigPreparer: the configuration is parsed once for the unit.
func (n *StringsNode) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process executes the configured string action.
func (n *StringsNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	cfgp, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err), err))
	}
	cfg := *cfgp

	if err := cfg.Validate(n.NodeId()); err != nil {
		return runtime.ErrorOutput(err)
	}

	result, err := executeAction(n.NodeId(), input.ItemIndex, cfg.Action, cfg.Params, input.Data)
	if err != nil {
		return runtime.ErrorOutput(err)
	}

	// Output schema: result
	return runtime.SuccessOutput(map[string]interface{}{"result": result})
}
