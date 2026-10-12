// Package connectorio implements the two ends of a connector action at run time (Olympus
// workplans/connector phases 2 and 6): plugin-connector-input, the entry of an action spliced into
// a consumer's run, and plugin-connector-result, the action's result. Each validates its input
// against the schema the action's contract names and outputs it unchanged, so the step's end node
// can take the result's output as its own.
package connectorio

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// Plugin types and the codes a mismatch fails with.
const (
	PluginInput  = "plugin-connector-input"
	PluginResult = "plugin-connector-result"

	CodeInputInvalid      = "CONNECTOR_INPUT_INVALID"
	CodeResultInvalid     = "CONNECTOR_RESULT_INVALID"
	CodeSchemaUnavailable = "CONNECTOR_SCHEMA_UNAVAILABLE"
)

// Config is what both rows carry: the schema id, and the schema Elysium enriches it with.
type Config struct {
	SchemaID string          `json:"schema_id"`
	Schema   json.RawMessage `json:"schema"`
}

// Node validates its input against its schema and passes it through.
type Node struct {
	runtime.BaseNode
	code     string
	cfgCache runtime.ConfigCache[Config]
}

// NewInputNode builds a plugin-connector-input node.
func NewInputNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	return newNode(config, PluginInput, CodeInputInvalid)
}

// NewResultNode builds a plugin-connector-result node.
func NewResultNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	return newNode(config, PluginResult, CodeResultInvalid)
}

func newNode(config runtime.EmbeddedNodeConfig, pluginType, code string) (runtime.EmbeddedNode, error) {
	if config.PluginType != pluginType {
		return nil, fmt.Errorf("invalid plugin type: expected '%s', got '%s'", pluginType, config.PluginType)
	}
	return &Node{BaseNode: runtime.NewBaseNode(config), code: code}, nil
}

// Prepare implements runtime.ConfigPreparer.
func (n *Node) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process validates input.Data against the schema and outputs it unchanged. A value the schema
// refuses fails the node with the row's code; the message lists what was wrong.
func (n *Node) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	cfg, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		return n.fail(input, &runtime.CodedError{Code: CodeSchemaUnavailable, Message: fmt.Sprintf("configuration could not be read: %v", err)})
	}
	data := input.Data
	if data == nil {
		data = map[string]interface{}{}
	}
	if len(cfg.Schema) == 0 || string(cfg.Schema) == "null" {
		if cfg.SchemaID == "" {
			return runtime.SuccessOutput(data) // no schema: nothing to check
		}
		return n.fail(input, &runtime.CodedError{Code: CodeSchemaUnavailable,
			Message: fmt.Sprintf("schema %s could not be read", cfg.SchemaID)})
	}

	if err := Validate(cfg.Schema, data, n.code); err != nil {
		return n.fail(input, err)
	}
	return runtime.SuccessOutput(data)
}

// Validate checks data against a schema definition, the same way for each format a contract's
// schema can have (NFR-10): a JSON object against a JSON schema; a CSV schema's rows (its /data
// port) against the CSV schema; an HL7 schema's message (its /value port) against the HL7 schema. A
// mismatch is a *runtime.CodedError with code, listing what was wrong. A value this function cannot
// read in place, such as a file reference, is not checked here: its port already carries the type.
func Validate(definition json.RawMessage, data map[string]interface{}, code string) error {
	engine := schema.Shared()
	var shape map[string]json.RawMessage
	_ = json.Unmarshal(definition, &shape)
	switch {
	case shape["columnHeaders"] != nil:
		raw, present := data["data"]
		if !present || raw == nil {
			return nil
		}
		rows, ok := raw.([]interface{})
		if !ok {
			return &runtime.CodedError{Code: code, Message: "data: the rows of a CSV input must be a list"}
		}
		doc, err := json.Marshal(rows)
		if err != nil {
			return &runtime.CodedError{Code: code, Message: fmt.Sprintf("rows could not be read: %v", err)}
		}
		vr, err := engine.ValidateCSVOnly(doc, definition)
		return verdict(vr, err, code)
	case shape["segments"] != nil:
		msg, ok := data["value"].(string)
		if !ok {
			return nil
		}
		vr, err := engine.ValidateHL7Only([]byte(msg), definition)
		return verdict(vr, err, code)
	}
	if _, err := engine.ParseJSONSchema(definition); err != nil {
		return nil
	}
	doc, err := json.Marshal(data)
	if err != nil {
		return &runtime.CodedError{Code: code, Message: fmt.Sprintf("input could not be read: %v", err)}
	}
	vr, err := engine.ValidateOnly(doc, definition)
	return verdict(vr, err, code)
}

func verdict(vr *schema.ValidationResult, err error, code string) error {
	if err != nil {
		return &runtime.CodedError{Code: code, Message: err.Error()}
	}
	if vr != nil && !vr.Valid {
		return &runtime.CodedError{Code: code, Message: vr.ErrorMessage()}
	}
	return nil
}

func (n *Node) fail(input runtime.ProcessInput, cause error) runtime.ProcessOutput {
	return runtime.ErrorOutput(runtime.NewProcessingError(input.NodeId, input.Label, input.PluginType, input.ItemIndex, "execute", cause))
}
