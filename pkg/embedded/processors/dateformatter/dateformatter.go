package dateformatter

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// DateFormatterNode implements date formatting for embedded.
type DateFormatterNode struct {
	runtime.BaseNode

	// cfgCache holds the parsed configuration, read-only, shared by every worker of the unit.
	cfgCache runtime.ConfigCache[Config]
}

// NewDateFormatterNode creates a new date formatter node.
func NewDateFormatterNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-date-formatter" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-date-formatter', got '%s'", config.PluginType)
	}
	return &DateFormatterNode{BaseNode: runtime.NewBaseNode(config)}, nil
}

// Prepare implements runtime.ConfigPreparer: the configuration is parsed once for the unit.
func (n *DateFormatterNode) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process formats dates according to config and input.
func (n *DateFormatterNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	cfgp, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err)))
	}
	cfg := *cfgp

	if err := cfg.Validate(n.NodeId()); err != nil {
		return runtime.ErrorOutput(err)
	}

	result, err := executeFormat(n.NodeId(), input.ItemIndex, input.Data, cfg)
	if err != nil {
		return runtime.ErrorOutput(err)
	}

	return runtime.SuccessOutput(result)
}
