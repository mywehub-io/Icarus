package simplecondition

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// SimpleConditionNode implements conditional logic evaluation for embeddedv2.
type SimpleConditionNode struct {
	runtime.BaseNode

	// cfgCache holds the parsed configuration, read-only, shared by every worker of the unit.
	cfgCache runtime.ConfigCache[Config]
}

// NewSimpleConditionNode creates a new simple condition node.
func NewSimpleConditionNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-simple-condition" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-simple-condition', got '%s'", config.PluginType)
	}
	return &SimpleConditionNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Prepare implements runtime.ConfigPreparer: the configuration is parsed once for the unit.
func (n *SimpleConditionNode) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process evaluates conditions and returns event-based routing output.
func (n *SimpleConditionNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	cfgp, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err)))
	}
	cfg := *cfgp

	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("invalid configuration: %v", err)))
	}

	results := make(map[string]bool)
	var warnings []string
	for _, manualInput := range cfg.ManualInputs {
		met, err, warning := n.evaluateCondition(input, manualInput)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		if warning != "" {
			warnings = append(warnings, warning)
			continue
		}
		results[manualInput.Name] = met
	}

	output := make(map[string]interface{})
	if len(warnings) > 0 {
		output["warning"] = strings.Join(warnings, " ")
		output["true"] = nil
		output["false"] = nil
		return runtime.SuccessOutput(output)
	}

	overallResult := n.calculateOverallResult(results, cfg.LogicOperator)
	if overallResult {
		output["true"] = input.Data
		output["false"] = nil
	} else {
		output["true"] = nil
		output["false"] = input.Data
	}

	return runtime.SuccessOutput(output)
}

func (n *SimpleConditionNode) evaluateCondition(input runtime.ProcessInput, manualInput ManualInput) (bool, error, string) {
	// Use name as field_path to extract value from input data
	// Strip leading slash if present (field mappings may include it)
	fieldPath := strings.TrimPrefix(manualInput.Name, "/")
	actualValue, exists := getValueFromPath(input.Data, fieldPath)

	if !exists {
		// Is Empty / Is NOT Empty: missing field is treated as empty
		if manualInput.Operator == OpIsEmpty {
			return true, nil, ""
		}
		if manualInput.Operator == OpIsNotEmpty {
			return false, nil, ""
		}
		// Special handling: if field doesn't exist and we're checking for empty string,
		// treat missing field as empty string (backward compatible with is_empty)
		if manualInput.Operator == OpEquals && manualInput.Value == "" {
			return true, nil, "" // Missing field equals empty string
		}

		// For all other cases, missing field: soft-fail with warning (no error)
		availableFields := make([]string, 0, len(input.Data))
		for key := range input.Data {
			availableFields = append(availableFields, key)
		}
		// Keys only: the values are the item itself, which can be patient data, and this
		// message becomes node output and log text.
		sort.Strings(availableFields)
		msg := fmt.Sprintf("field '%s' not found in input. Available fields: %v",
			fieldPath, availableFields)
		return false, nil, msg
	}

	// Is Empty / Is NOT Empty: only need actual value, no expected value
	if manualInput.Operator == OpIsEmpty {
		return isEmpty(actualValue), nil, ""
	}
	if manualInput.Operator == OpIsNotEmpty {
		return !isEmpty(actualValue), nil, ""
	}

	// Convert the expected value from string to the appropriate type
	expectedValue, err := manualInput.ConvertValue()
	if err != nil {
		return false, NewConfigError(
			n.NodeId(),
			"value",
			fmt.Sprintf("failed to convert value '%s' for condition '%s': %v", manualInput.Value, manualInput.Name, err),
		), ""
	}

	met, err := compareValues(
		n.NodeId(),
		input.ItemIndex,
		actualValue,
		expectedValue,
		manualInput.Operator,
		false, // case_insensitive removed from schema
	)
	if err != nil {
		return false, err, ""
	}

	return met, nil, ""
}

func (n *SimpleConditionNode) calculateOverallResult(results map[string]bool, logicOp LogicOperator) bool {
	if len(results) == 0 {
		return false
	}
	switch logicOp {
	case LogicAnd:
		for _, met := range results {
			if !met {
				return false
			}
		}
		return true
	case LogicOr:
		for _, met := range results {
			if met {
				return true
			}
		}
		return false
	default:
		return false
	}
}
