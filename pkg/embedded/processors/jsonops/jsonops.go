package jsonops

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// JsonOpsNode implements JSON actions (parse and produce) for embedded
type JsonOpsNode struct {
	runtime.BaseNode

	// cfgCache holds the parsed configuration, read-only, shared by every worker of the unit.
	cfgCache runtime.ConfigCache[Config]
}

// NewJsonOpsNode creates a new jsonops node instance
func NewJsonOpsNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	// Validate plugin type
	if config.PluginType != "plugin-json-operations" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-json-operations', got '%s'", config.PluginType)
	}

	return &JsonOpsNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Prepare implements runtime.ConfigPreparer: the configuration is parsed once for the unit.
func (n *JsonOpsNode) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process executes the JSON action (parse or produce)
func (n *JsonOpsNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	// Parse configuration
	cfgp, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "failed to parse configuration", err))
	}
	cfg := *cfgp

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "invalid configuration", err))
	}

	// Route to appropriate action
	switch cfg.Action {
	case "parse":
		return n.executeParse(input, &cfg)
	case "produce":
		return n.executeProduce(input, &cfg)
	default:
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("unknown action: %s", cfg.Action),
			nil,
		))
	}
}
