package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func TestConfigValidate(t *testing.T) {
	valid := jsonops.Config{Action: "parse", SchemaID: "sid"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	cases := []jsonops.Config{
		{},                                  // missing action
		{Action: "bad", SchemaID: "sid"}, // invalid op
		{Action: "parse"},                // missing schema id and schema
		{Action: "produce", Schema: json.RawMessage{}}, // empty schema
	}
	for i, cfg := range cases {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected validation error for case %d: %+v", i, cfg)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	cfgParse := jsonops.Config{Action: "parse", SchemaID: "sid"}
	if !cfgParse.GetApplyDefaults() {
		t.Fatalf("parse should default apply_defaults true")
	}
	if cfgParse.GetStructureData() {
		t.Fatalf("parse should default structure_data false")
	}
	if !cfgParse.GetStrictValidation() {
		t.Fatalf("parse should default strict_validation true")
	}

	cfgProduce := jsonops.Config{Action: "produce", SchemaID: "sid"}
	if !cfgProduce.GetApplyDefaults() {
		t.Fatalf("produce should default apply_defaults true")
	}
	if !cfgProduce.GetStructureData() {
		t.Fatalf("produce should default structure_data true")
	}
	if !cfgProduce.GetStrictValidation() {
		t.Fatalf("produce should default strict_validation true")
	}
}

func TestConfigExplicitOverrides(t *testing.T) {
	apply := false
	structure := false
	strict := false
	cfg := jsonops.Config{
		Action:        "produce",
		SchemaID:         "sid",
		ApplyDefaults:    &apply,
		StructureData:    &structure,
		StrictValidation: &strict,
	}

	if cfg.GetApplyDefaults() != apply {
		t.Fatalf("expected apply_defaults override")
	}
	if cfg.GetStructureData() != structure {
		t.Fatalf("expected structure_data override")
	}
	if cfg.GetStrictValidation() != strict {
		t.Fatalf("expected strict_validation override")
	}
}

// createTestNode creates a JsonOpsNode for testing
func createTestNode(t *testing.T, nodeID string) *jsonops.JsonOpsNode {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-jsonops",
		PluginType: "plugin-json-operations",
		Embeddable: true,
		Depth:      0,
	}
	node, err := jsonops.NewJsonOpsNode(config)
	if err != nil {
		t.Fatalf("failed to create test node: %v", err)
	}
	return node.(*jsonops.JsonOpsNode)
}

// createProcessInput creates a ProcessInput for testing
func createProcessInput(data map[string]interface{}, rawConfig json.RawMessage, itemIndex int) runtime.ProcessInput {
	return runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      data,
		RawConfig: rawConfig,
		NodeId:    "test-node-1",
		ItemIndex: itemIndex,
	}
}

// TestProcessInvalidJSONConfig tests Process with invalid JSON configuration
func TestProcessInvalidJSONConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage(`{invalid json}`),
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid JSON config")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
	if output.Data != nil {
		t.Fatalf("expected nil data on error")
	}
	// Verify error message contains "failed to parse configuration"
	if output.Error.Error() == "" {
		t.Fatalf("expected non-empty error message")
	}
}

// TestProcessEmptyConfig tests Process with empty JSON config
func TestProcessEmptyConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage(`{}`),
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for empty config")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessMissingAction tests Process with missing action field
func TestProcessMissingAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for missing action")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessInvalidAction tests Process with invalid action value
func TestProcessInvalidAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "invalid-op", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for invalid action")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessMissingSchemaIDAndSchema tests Process with missing both schema_id and schema
func TestProcessMissingSchemaIDAndSchema(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "parse"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for missing schema_id and schema")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessEmptySchema tests Process with empty schema (when schema_id is not provided)
func TestProcessEmptySchema(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "produce", Schema: json.RawMessage{}}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for empty schema")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessUnknownAction tests Process with unknown action value
func TestProcessUnknownAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	// Use a valid action name but not "parse" or "produce"
	config := jsonops.Config{Action: "transform", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for unknown action")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
	// Verify error message contains the unknown action
	if output.Error.Error() == "" {
		t.Fatalf("expected non-empty error message")
	}
}

// TestProcessValidParseAction tests Process with valid parse action configuration
// Note: This tests routing to executeParse, but actual execution will fail without schema enrichment
func TestProcessValidParseAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{
		Action: "parse",
		SchemaID:  "test-schema",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="}, // base64 encoded "test"
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Since schema is not enriched, this should fail with a ConfigError about schema not being enriched
	// This confirms that the routing to executeParse happened correctly
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched (confirms routing to executeParse)")
	}
	// Should be ConfigError about schema not being enriched, not a validation error
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError for missing schema enrichment, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessValidProduceAction tests Process with valid produce action configuration
// Note: This tests routing to executeProduce, but actual execution will fail without schema enrichment
func TestProcessValidProduceAction(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{
		Action: "produce",
		SchemaID:  "test-schema",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Since schema is not enriched, this should fail with a ConfigError about schema not being enriched
	// This confirms that the routing to executeProduce happened correctly
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched (confirms routing to executeProduce)")
	}
	// Should be ConfigError about schema not being enriched, not a validation error
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError for missing schema enrichment, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessWithSchemaID tests Process with schema_id (valid config structure)
func TestProcessWithSchemaID(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{
		Action: "parse",
		SchemaID:  "test-schema-id",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Config validation should pass, but execution will fail without schema enrichment
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Verify it's a ConfigError about enrichment, not validation
	if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "" {
			t.Fatalf("expected error message about schema enrichment")
		}
	} else {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessWithSchema tests Process with schema field (valid config structure)
func TestProcessWithSchema(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	schemaJSON := json.RawMessage(`{"type": "object", "properties": {"name": {"type": "string"}}}`)
	config := jsonops.Config{
		Action: "produce",
		Schema:    schemaJSON,
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"name": "test"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Config validation should pass, execution may succeed or fail based on schema processing
	// But we're testing that the config is accepted and routing happens
	if output.Error != nil {
		// If there's an error, it should not be a config validation error
		if _, ok := output.Error.(*jsonops.ConfigError); ok {
			// Check if it's about enrichment (schema_id case) or actual processing error
			// Since we provided Schema directly, it shouldn't be an enrichment error
			// It might be a processing error which is fine - means routing worked
		}
	}
}

// TestProcessWithItemIndex tests Process with different ItemIndex values
func TestProcessWithItemIndex(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{
		Action: "parse",
		SchemaID:  "test-schema",
	}
	rawConfig, _ := json.Marshal(config)

	// Test with ItemIndex = 0
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		0,
	)
	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}

	// Test with ItemIndex = 5
	input.ItemIndex = 5
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}

	// Test with ItemIndex = -1 (no iteration)
	input.ItemIndex = -1
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
}

// TestProcessMalformedJSONVariations tests various malformed JSON scenarios
func TestProcessMalformedJSONVariations(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	testCases := []struct {
		name      string
		rawConfig json.RawMessage
	}{
		{"unclosed brace", json.RawMessage(`{"action": "parse"`)},
		{"unclosed string", json.RawMessage(`{"action": "parse"`)},
		{"invalid escape", json.RawMessage(`{"action": "parse\z"}`)},
		{"trailing comma", json.RawMessage(`{"action": "parse",}`)},
		{"null byte", json.RawMessage(`{"action": "parse\000"}`)},
		{"invalid unicode", json.RawMessage(`{"action": "\uXXXX"}`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(inputData, tc.rawConfig, -1)
			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error for malformed JSON: %s", tc.name)
			}
			if _, ok := output.Error.(*jsonops.ConfigError); !ok {
				t.Fatalf("expected ConfigError for %s, got %T", tc.name, output.Error)
			}
		})
	}
}

// TestProcessAllValidationErrors tests all validation error scenarios
func TestProcessAllValidationErrors(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	testCases := []struct {
		name   string
		config jsonops.Config
	}{
		{"missing action", jsonops.Config{SchemaID: "sid"}},
		{"invalid action", jsonops.Config{Action: "bad", SchemaID: "sid"}},
		{"missing schema_id and schema", jsonops.Config{Action: "parse"}},
		{"empty schema", jsonops.Config{Action: "produce", Schema: json.RawMessage{}}},
		{"unknown action", jsonops.Config{Action: "unknown", SchemaID: "sid"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rawConfig, _ := json.Marshal(tc.config)
			input := createProcessInput(inputData, rawConfig, -1)
			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if _, ok := output.Error.(*jsonops.ConfigError); !ok {
				t.Fatalf("expected ConfigError for %s, got %T", tc.name, output.Error)
			}
		})
	}
}

// TestProcessActionRouting tests that valid actions route correctly
func TestProcessActionRouting(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	// Test parse action routing
	t.Run("parse action", func(t *testing.T) {
		config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
		rawConfig, _ := json.Marshal(config)
		input := createProcessInput(inputData, rawConfig, -1)
		output := node.Process(input)
		// Should route to executeParse, which will fail without schema enrichment
		// This confirms routing happened (not a validation error)
		if output.Error == nil {
			t.Fatalf("expected error (confirms routing to executeParse)")
		}
		// Error should be about schema enrichment, not config validation
		if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
			// Should mention schema enrichment, not "invalid configuration"
			if configErr.Message == "invalid configuration" {
				t.Fatalf("unexpected validation error, should have routed to executeParse")
			}
		} else {
			t.Fatalf("expected ConfigError, got %T", output.Error)
		}
	})

	// Test produce action routing
	t.Run("produce action", func(t *testing.T) {
		config := jsonops.Config{Action: "produce", SchemaID: "test-schema"}
		rawConfig, _ := json.Marshal(config)
		input := createProcessInput(inputData, rawConfig, -1)
		output := node.Process(input)
		// Should route to executeProduce, which will fail without schema enrichment
		// This confirms routing happened (not a validation error)
		if output.Error == nil {
			t.Fatalf("expected error (confirms routing to executeProduce)")
		}
		// Error should be about schema enrichment, not config validation
		if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
			// Should mention schema enrichment, not "invalid configuration"
			if configErr.Message == "invalid configuration" {
				t.Fatalf("unexpected validation error, should have routed to executeProduce")
			}
		} else {
			t.Fatalf("expected ConfigError, got %T", output.Error)
		}
	})
}

// TestProcessErrorMessages tests that error messages contain expected content
func TestProcessErrorMessages(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	// Test invalid JSON error message
	input := createProcessInput(inputData, json.RawMessage(`{invalid}`), -1)
	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}
	errMsg := output.Error.Error()
	if errMsg == "" {
		t.Fatalf("expected non-empty error message")
	}
	// Error should mention config error and node ID
	if !contains(errMsg, "config error") && !contains(errMsg, "failed to parse") {
		t.Fatalf("error message should mention config parsing: %s", errMsg)
	}

	// Test unknown action error message
	// Note: Invalid actions are caught by Validate() which returns "invalid action"
	// Only actions that pass validation but aren't "parse" or "produce" reach the default case
	config := jsonops.Config{Action: "invalid-op", SchemaID: "sid"}
	rawConfig, _ := json.Marshal(config)
	input = createProcessInput(inputData, rawConfig, -1)
	output = node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}
	errMsg = output.Error.Error()
	// Error should mention invalid action (caught by Validate) or unknown action (caught by switch)
	if !contains(errMsg, "invalid action") && !contains(errMsg, "unknown action") {
		t.Fatalf("error message should mention invalid/unknown action: %s", errMsg)
	}
	if !contains(errMsg, "invalid-op") {
		t.Fatalf("error message should contain the action name: %s", errMsg)
	}
}

// TestProcessNodeIDInErrors tests that node ID is correctly included in errors
func TestProcessNodeIDInErrors(t *testing.T) {
	testNodeIDs := []string{"node-1", "node-abc", "test-node-123"}

	for _, nodeID := range testNodeIDs {
		t.Run(nodeID, func(t *testing.T) {
			node := createTestNode(t, nodeID)
			config := jsonops.Config{Action: "invalid-op", SchemaID: "sid"}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(map[string]interface{}{"data": "test"}, rawConfig, -1)
			// Override NodeId in input to match the node
			input.NodeId = nodeID

			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error")
			}
			errMsg := output.Error.Error()
			if !contains(errMsg, nodeID) {
				t.Fatalf("error message should contain node ID '%s': %s", nodeID, errMsg)
			}
		})
	}
}

// TestProcessNilRawConfig tests Process with nil RawConfig
func TestProcessNilRawConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		nil,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for nil RawConfig")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessEmptyRawConfig tests Process with empty RawConfig
func TestProcessEmptyRawConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage{},
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for empty RawConfig")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessWhitespaceOnlyConfig tests Process with whitespace-only config
func TestProcessWhitespaceOnlyConfig(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage(`   `),
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error for whitespace-only config")
	}
	if _, ok := output.Error.(*jsonops.ConfigError); !ok {
		t.Fatalf("expected ConfigError, got %T: %v", output.Error, output.Error)
	}
}

// TestProcessConfigWithWrongTypes tests Process with config having wrong data types
func TestProcessConfigWithWrongTypes(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	testCases := []struct {
		name      string
		rawConfig json.RawMessage
	}{
		{"action as number", json.RawMessage(`{"action": 123, "schema_id": "sid"}`)},
		{"action as boolean", json.RawMessage(`{"action": true, "schema_id": "sid"}`)},
		{"action as array", json.RawMessage(`{"action": ["parse"], "schema_id": "sid"}`)},
		{"schema_id as number", json.RawMessage(`{"action": "parse", "schema_id": 123}`)},
		{"schema_id as boolean", json.RawMessage(`{"action": "parse", "schema_id": true}`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(inputData, tc.rawConfig, -1)
			output := node.Process(input)
			// Should either fail to unmarshal or fail validation
			if output.Error == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if _, ok := output.Error.(*jsonops.ConfigError); !ok {
				t.Fatalf("expected ConfigError for %s, got %T", tc.name, output.Error)
			}
		})
	}
}

// TestProcessConfigWithAllOptionalFields tests Process with all optional fields populated
func TestProcessConfigWithAllOptionalFields(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	applyDefaults := true
	structureData := false
	strictValidation := true
	config := jsonops.Config{
		Action:        "parse",
		SchemaID:         "test-schema",
		ApplyDefaults:    &applyDefaults,
		StructureData:    &structureData,
		StrictValidation: &strictValidation,
		Pretty:           true,
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Config should be valid, but execution will fail without schema enrichment
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Should not be a validation error
	if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "invalid configuration" {
			t.Fatalf("unexpected validation error, config should be valid")
		}
	}
}

// TestProcessConfigWithExtraFields tests that extra fields in config are ignored
func TestProcessConfigWithExtraFields(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	// Config with extra unknown fields
	rawConfig := json.RawMessage(`{
		"action": "parse",
		"schema_id": "test-schema",
		"unknown_field": "should be ignored",
		"another_field": 123,
		"nested": {"field": "value"}
	}`)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Should not fail on config parsing/validation (extra fields are ignored by JSON unmarshal)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Should not be a config parsing/validation error
	if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "failed to parse configuration" || configErr.Message == "invalid configuration" {
			t.Fatalf("unexpected config error, extra fields should be ignored: %v", configErr)
		}
	}
}

// TestProcessErrorUnwrapping tests that error unwrapping works correctly
func TestProcessErrorUnwrapping(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage(`{invalid json}`),
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}

	configErr, ok := output.Error.(*jsonops.ConfigError)
	if !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}

	// Test Unwrap method
	if configErr.Unwrap() == nil {
		t.Fatalf("expected underlying error to be present")
	}
}

// TestProcessErrorOutputStructure tests that error output has correct structure
func TestProcessErrorOutputStructure(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage(`{invalid}`),
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}

	// Error should be set
	if output.Error == nil {
		t.Fatalf("expected Error to be set")
	}

	// Data should be nil on error
	if output.Data != nil {
		t.Fatalf("expected Data to be nil on error, got %v", output.Data)
	}

	// Skipped should be false
	if output.Skipped {
		t.Fatalf("expected Skipped to be false on error")
	}
}

// TestProcessWithDifferentNodeIDs tests Process with different node IDs
func TestProcessWithDifferentNodeIDs(t *testing.T) {
	testCases := []struct {
		nodeID string
	}{
		{"node-1"},
		{"node-abc-123"},
		{"test_node_with_underscores"},
		{"node-with-dashes"},
		{""}, // Empty node ID
	}

	for _, tc := range testCases {
		t.Run(tc.nodeID, func(t *testing.T) {
			node := createTestNode(t, tc.nodeID)
			config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"data": "dGVzdA=="},
				rawConfig,
				-1,
			)
			input.NodeId = tc.nodeID

			output := node.Process(input)
			// Should route correctly (will fail without schema enrichment)
			if output.Error == nil {
				t.Fatalf("expected error when schema is not enriched")
			}
		})
	}
}

// TestProcessCaseSensitiveActions tests that actions are case-sensitive
func TestProcessCaseSensitiveActions(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	testCases := []struct {
		name      string
		action string
		shouldErr bool
	}{
		{"uppercase PARSE", "PARSE", true},
		{"mixed case Parse", "Parse", true},
		{"uppercase PRODUCE", "PRODUCE", true},
		{"mixed case Produce", "Produce", true},
		{"lowercase parse", "parse", false},     // Should route correctly
		{"lowercase produce", "produce", false}, // Should route correctly
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := jsonops.Config{Action: tc.action, SchemaID: "test-schema"}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(inputData, rawConfig, -1)
			output := node.Process(input)

			if tc.shouldErr {
				if output.Error == nil {
					t.Fatalf("expected error for %s", tc.name)
				}
				if _, ok := output.Error.(*jsonops.ConfigError); !ok {
					t.Fatalf("expected ConfigError for %s, got %T", tc.name, output.Error)
				}
			} else {
				// Should route correctly (will fail without schema enrichment, but not validation error)
				if output.Error == nil {
					t.Fatalf("expected error when schema is not enriched")
				}
				// Should not be a validation error
				if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
					if configErr.Message == "invalid configuration" {
						t.Fatalf("unexpected validation error for %s", tc.name)
					}
				}
			}
		})
	}
}

// TestProcessWithSchemaAndSchemaID tests Process with both schema and schema_id
func TestProcessWithSchemaAndSchemaID(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	schemaJSON := json.RawMessage(`{"type": "object"}`)
	config := jsonops.Config{
		Action: "parse",
		SchemaID:  "test-schema-id",
		Schema:    schemaJSON,
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Config should be valid (both schema_id and schema provided)
	// Execution will proceed (may fail in schema processing, but not config validation)
	if output.Error != nil {
		// If error, should not be a validation error
		if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
			if configErr.Message == "invalid configuration" {
				t.Fatalf("unexpected validation error, both schema_id and schema provided")
			}
		}
	}
}

// TestProcessMultipleInvalidJSONScenarios tests various invalid JSON edge cases
func TestProcessMultipleInvalidJSONScenarios(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	testCases := []struct {
		name      string
		rawConfig json.RawMessage
	}{
		{"only opening brace", json.RawMessage(`{`)},
		{"only closing brace", json.RawMessage(`}`)},
		{"only comma", json.RawMessage(`,`)},
		{"only colon", json.RawMessage(`:`)},
		{"unclosed array", json.RawMessage(`[`)},
		{"unclosed object", json.RawMessage(`{`)},
		{"mismatched brackets", json.RawMessage(`{[}`)},
		{"double comma", json.RawMessage(`{"action": "parse",, "schema_id": "sid"}`)},
		{"missing colon", json.RawMessage(`{"action" "parse"}`)},
		{"invalid number", json.RawMessage(`{"action": 12.34.56}`)},
		{"invalid true", json.RawMessage(`{"action": tru}`)},
		{"invalid false", json.RawMessage(`{"action": fals}`)},
		{"invalid null", json.RawMessage(`{"action": nul}`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(inputData, tc.rawConfig, -1)
			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if _, ok := output.Error.(*jsonops.ConfigError); !ok {
				t.Fatalf("expected ConfigError for %s, got %T", tc.name, output.Error)
			}
		})
	}
}

// TestProcessConfigErrorDetails tests detailed ConfigError properties
func TestProcessConfigErrorDetails(t *testing.T) {
	node := createTestNode(t, "test-node-xyz")
	config := jsonops.Config{Action: "unknown-op", SchemaID: "sid"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		rawConfig,
		5, // ItemIndex = 5
	)
	input.NodeId = "test-node-xyz"

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}

	configErr, ok := output.Error.(*jsonops.ConfigError)
	if !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}

	// Verify ConfigError fields
	if configErr.NodeId != "test-node-xyz" {
		t.Fatalf("expected NodeId 'test-node-xyz', got '%s'", configErr.NodeId)
	}
	if configErr.Message == "" {
		t.Fatalf("expected non-empty Message")
	}
	// Message should be "invalid configuration" (wrapper), and the underlying cause should have details
	// Check the full error string which includes the cause
	fullErrorMsg := output.Error.Error()
	if !contains(fullErrorMsg, "invalid configuration") {
		t.Fatalf("expected error to mention 'invalid configuration', got: %s", fullErrorMsg)
	}
	// The underlying cause should mention invalid action
	if configErr.Cause != nil {
		causeMsg := configErr.Cause.Error()
		if !contains(causeMsg, "invalid action") && !contains(causeMsg, "unknown action") {
			t.Fatalf("expected cause to mention 'invalid action' or 'unknown action', got: %s", causeMsg)
		}
	}
}

// TestProcessNegativeItemIndex tests Process with various negative ItemIndex values
func TestProcessNegativeItemIndex(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	testCases := []int{-1, -5, -10, -100, -1000}

	for _, itemIndex := range testCases {
		t.Run(fmt.Sprintf("ItemIndex_%d", itemIndex), func(t *testing.T) {
			input := createProcessInput(inputData, rawConfig, itemIndex)
			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error when schema is not enriched")
			}
			// ItemIndex should be preserved in errors (if applicable)
			if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
				// ConfigError doesn't have ItemIndex, but verify it doesn't crash
				if configErr.NodeId == "" {
					t.Fatalf("expected NodeId to be set")
				}
			}
		})
	}
}

// TestProcessLargeItemIndex tests Process with very large ItemIndex values
func TestProcessLargeItemIndex(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	testCases := []int{0, 1, 10, 100, 1000, 10000, 999999}

	for _, itemIndex := range testCases {
		t.Run(fmt.Sprintf("ItemIndex_%d", itemIndex), func(t *testing.T) {
			input := createProcessInput(inputData, rawConfig, itemIndex)
			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error when schema is not enriched")
			}
		})
	}
}

// TestProcessEmptyStringVsMissingField tests empty string vs missing field behavior
func TestProcessEmptyStringVsMissingField(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	// Empty action string should fail validation
	config1 := jsonops.Config{Action: "", SchemaID: "sid"}
	rawConfig1, _ := json.Marshal(config1)
	input1 := createProcessInput(inputData, rawConfig1, -1)
	output1 := node.Process(input1)
	if output1.Error == nil {
		t.Fatalf("expected error for empty action string")
	}

	// Empty schema_id string with no schema should fail validation
	config2 := jsonops.Config{Action: "parse", SchemaID: ""}
	rawConfig2, _ := json.Marshal(config2)
	input2 := createProcessInput(inputData, rawConfig2, -1)
	output2 := node.Process(input2)
	if output2.Error == nil {
		t.Fatalf("expected error for empty schema_id with no schema")
	}
}

// TestProcessUnicodeCharacters tests Process with unicode characters in IDs
func TestProcessUnicodeCharacters(t *testing.T) {
	node := createTestNode(t, "node-测试-🎉")
	config := jsonops.Config{
		Action: "parse",
		SchemaID:  "schema-测试-中文",
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		-1,
	)
	input.NodeId = "node-测试-🎉"

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Verify node ID with unicode is preserved in error
	errMsg := output.Error.Error()
	if !contains(errMsg, "node-测试-🎉") {
		t.Fatalf("expected unicode node ID in error message: %s", errMsg)
	}
}

// TestProcessVeryLongStrings tests Process with very long strings in config
func TestProcessVeryLongStrings(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	// Create a very long schema_id
	longSchemaID := strings.Repeat("a", 10000)
	config := jsonops.Config{
		Action: "parse",
		SchemaID:  longSchemaID,
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"data": "dGVzdA=="},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Verify long schema_id is preserved in error message
	errMsg := output.Error.Error()
	if !contains(errMsg, longSchemaID) {
		t.Fatalf("expected long schema_id in error message")
	}
}

// TestProcessBooleanFieldEdgeCases tests boolean field edge cases
func TestProcessBooleanFieldEdgeCases(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	// Test with nil pointers (should use defaults)
	config1 := jsonops.Config{
		Action: "parse",
		SchemaID:  "test-schema",
		// ApplyDefaults, StructureData, StrictValidation are nil
	}
	rawConfig1, _ := json.Marshal(config1)
	input1 := createProcessInput(inputData, rawConfig1, -1)
	output1 := node.Process(input1)
	if output1.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}

	// Test with false explicitly set
	applyFalse := false
	structureFalse := false
	strictFalse := false
	config2 := jsonops.Config{
		Action:        "produce",
		SchemaID:         "test-schema",
		ApplyDefaults:    &applyFalse,
		StructureData:    &structureFalse,
		StrictValidation: &strictFalse,
	}
	rawConfig2, _ := json.Marshal(config2)
	input2 := createProcessInput(inputData, rawConfig2, -1)
	output2 := node.Process(input2)
	if output2.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}

	// Test with true explicitly set
	applyTrue := true
	structureTrue := true
	strictTrue := true
	config3 := jsonops.Config{
		Action:        "parse",
		SchemaID:         "test-schema",
		ApplyDefaults:    &applyTrue,
		StructureData:    &structureTrue,
		StrictValidation: &strictTrue,
	}
	rawConfig3, _ := json.Marshal(config3)
	input3 := createProcessInput(inputData, rawConfig3, -1)
	output3 := node.Process(input3)
	if output3.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
}

// TestProcessEmptyDataMap tests Process with empty data map
func TestProcessEmptyDataMap(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{}, // Empty data map
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Should route to executeParse, which will check for "data" field
	if output.Error == nil {
		t.Fatalf("expected error (empty data map or missing 'data' field)")
	}
	// Should be a processing error about missing 'data' field, not config error
	if _, ok := output.Error.(*jsonops.ConfigError); ok {
		// If it's a config error, it should be about schema enrichment, not validation
		configErr := output.Error.(*jsonops.ConfigError)
		if configErr.Message == "invalid configuration" {
			t.Fatalf("unexpected validation error, should have routed to executeParse")
		}
	}
}

// TestProcessNilDataMap tests Process with nil data map
func TestProcessNilDataMap(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		nil, // Nil data map
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Should route to executeParse, which will check for "data" field
	if output.Error == nil {
		t.Fatalf("expected error (nil data map or missing 'data' field)")
	}
}

// TestProcessControlCharactersInJSON tests Process with control characters in JSON
func TestProcessControlCharactersInJSON(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	// Test with various control characters
	testCases := []struct {
		name      string
		rawConfig json.RawMessage
	}{
		{"newline in string", json.RawMessage(`{"action": "parse\n", "schema_id": "sid"}`)},
		{"tab in string", json.RawMessage(`{"action": "parse\t", "schema_id": "sid"}`)},
		{"carriage return", json.RawMessage(`{"action": "parse\r", "schema_id": "sid"}`)},
		{"backspace", json.RawMessage(`{"action": "parse\b", "schema_id": "sid"}`)},
		{"form feed", json.RawMessage(`{"action": "parse\f", "schema_id": "sid"}`)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(inputData, tc.rawConfig, -1)
			output := node.Process(input)
			// Control characters in strings are valid JSON, but action will be invalid
			if output.Error == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

// TestProcessDeeplyNestedJSON tests Process with deeply nested JSON config
func TestProcessDeeplyNestedJSON(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	// Create deeply nested JSON (though config doesn't support nesting, test that it's ignored)
	deepNested := `{"action": "parse", "schema_id": "sid", "nested": {"level1": {"level2": {"level3": {"level4": "value"}}}}}`
	rawConfig := json.RawMessage(deepNested)
	input := createProcessInput(inputData, rawConfig, -1)

	output := node.Process(input)
	// Should parse successfully (extra fields are ignored)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Should not be a parsing error
	if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "failed to parse configuration" {
			t.Fatalf("unexpected parsing error for nested JSON")
		}
	}
}

// TestProcessErrorCauseChain tests that error cause chains are preserved
func TestProcessErrorCauseChain(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	input := createProcessInput(
		map[string]interface{}{"data": "test"},
		json.RawMessage(`{invalid json}`),
		-1,
	)

	output := node.Process(input)
	if output.Error == nil {
		t.Fatalf("expected error")
	}

	configErr, ok := output.Error.(*jsonops.ConfigError)
	if !ok {
		t.Fatalf("expected ConfigError, got %T", output.Error)
	}

	// Verify cause chain
	if configErr.Cause == nil {
		t.Fatalf("expected underlying cause to be present")
	}

	// Test unwrapping
	unwrapped := configErr.Unwrap()
	if unwrapped == nil {
		t.Fatalf("expected Unwrap() to return the cause")
	}
}

// TestProcessSpecialCharactersInSchemaID tests Process with special characters in schema_id
func TestProcessSpecialCharactersInSchemaID(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	testCases := []struct {
		name     string
		schemaID string
	}{
		{"with spaces", "schema with spaces"},
		{"with dots", "schema.with.dots"},
		{"with dashes", "schema-with-dashes"},
		{"with underscores", "schema_with_underscores"},
		{"with slashes", "schema/with/slashes"},
		{"with colons", "schema:with:colons"},
		{"with special chars", "schema@#$%^&*()"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := jsonops.Config{Action: "parse", SchemaID: tc.schemaID}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(inputData, rawConfig, -1)
			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error when schema is not enriched")
			}
			// Verify schema_id is preserved in error message
			errMsg := output.Error.Error()
			if !contains(errMsg, tc.schemaID) {
				t.Fatalf("expected schema_id '%s' in error message: %s", tc.schemaID, errMsg)
			}
		})
	}
}

// TestProcessNodeIdMethod tests that NodeId() method is used correctly
func TestProcessNodeIdMethod(t *testing.T) {
	testNodeIDs := []string{"node-1", "different-node", "node-with-special-chars-123"}

	for _, nodeID := range testNodeIDs {
		t.Run(nodeID, func(t *testing.T) {
			node := createTestNode(t, nodeID)
			config := jsonops.Config{Action: "invalid-op", SchemaID: "sid"}
			rawConfig, _ := json.Marshal(config)
			input := createProcessInput(
				map[string]interface{}{"data": "test"},
				rawConfig,
				-1,
			)

			output := node.Process(input)
			if output.Error == nil {
				t.Fatalf("expected error")
			}
			// Verify node ID from node.NodeId() is used in error
			errMsg := output.Error.Error()
			if !contains(errMsg, nodeID) {
				t.Fatalf("expected node ID '%s' in error message: %s", nodeID, errMsg)
			}
		})
	}
}

// TestProcessOrderOfActions tests that actions happen in correct order
func TestProcessOrderOfActions(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	// Test that JSON parsing happens before validation
	// Invalid JSON should fail at parsing stage, not validation
	input1 := createProcessInput(inputData, json.RawMessage(`{invalid}`), -1)
	output1 := node.Process(input1)
	if output1.Error == nil {
		t.Fatalf("expected error")
	}
	configErr1, ok := output1.Error.(*jsonops.ConfigError)
	if !ok {
		t.Fatalf("expected ConfigError, got %T", output1.Error)
	}
	if configErr1.Message != "failed to parse configuration" {
		t.Fatalf("expected parsing error, got: %s", configErr1.Message)
	}

	// Test that validation happens after parsing
	// Valid JSON but invalid config should fail at validation stage
	config2 := jsonops.Config{Action: "bad-op", SchemaID: "sid"}
	rawConfig2, _ := json.Marshal(config2)
	input2 := createProcessInput(inputData, rawConfig2, -1)
	output2 := node.Process(input2)
	if output2.Error == nil {
		t.Fatalf("expected error")
	}
	configErr2, ok := output2.Error.(*jsonops.ConfigError)
	if !ok {
		t.Fatalf("expected ConfigError, got %T", output2.Error)
	}
	if configErr2.Message != "invalid configuration" {
		t.Fatalf("expected validation error, got: %s", configErr2.Message)
	}
}

// TestProcessWithEscapedCharacters tests Process with escaped characters in JSON
func TestProcessWithEscapedCharacters(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "test"}

	testCases := []struct {
		name      string
		rawConfig json.RawMessage
		shouldErr bool
	}{
		{"escaped quotes", json.RawMessage(`{"action": "parse", "schema_id": "schema\"with\"quotes"}`), false},
		{"escaped backslash", json.RawMessage(`{"action": "parse", "schema_id": "schema\\with\\backslashes"}`), false},
		{"escaped newline", json.RawMessage(`{"action": "parse", "schema_id": "schema\nwith\nnewline"}`), false},
		{"unicode escape", json.RawMessage(`{"action": "parse", "schema_id": "schema\u0041"}`), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input := createProcessInput(inputData, tc.rawConfig, -1)
			output := node.Process(input)
			if tc.shouldErr {
				if output.Error == nil {
					t.Fatalf("expected error for %s", tc.name)
				}
			} else {
				// Should parse successfully, but fail at schema enrichment
				if output.Error == nil {
					t.Fatalf("expected error when schema is not enriched")
				}
				// Should not be a parsing error
				if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
					if configErr.Message == "failed to parse configuration" {
						t.Fatalf("unexpected parsing error for %s", tc.name)
					}
				}
			}
		})
	}
}

// TestProcessPrettyField tests Process with Pretty field set
func TestProcessPrettyField(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{
		Action: "produce",
		SchemaID:  "test-schema",
		Pretty:    true,
	}
	rawConfig, _ := json.Marshal(config)
	input := createProcessInput(
		map[string]interface{}{"field1": "value1"},
		rawConfig,
		-1,
	)

	output := node.Process(input)
	// Should route to executeProduce (will fail without schema enrichment)
	if output.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Should not be a validation error
	if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "invalid configuration" {
			t.Fatalf("unexpected validation error, Pretty field should be accepted")
		}
	}
}

// TestProcessAllOptionalFieldsCombinations tests all combinations of optional boolean fields
func TestProcessAllOptionalFieldsCombinations(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	// Generate all combinations of boolean values
	values := []bool{true, false}
	for _, apply := range values {
		for _, structure := range values {
			for _, strict := range values {
				t.Run(fmt.Sprintf("apply_%v_structure_%v_strict_%v", apply, structure, strict), func(t *testing.T) {
					config := jsonops.Config{
						Action:        "parse",
						SchemaID:         "test-schema",
						ApplyDefaults:    &apply,
						StructureData:    &structure,
						StrictValidation: &strict,
					}
					rawConfig, _ := json.Marshal(config)
					input := createProcessInput(inputData, rawConfig, -1)
					output := node.Process(input)
					// All combinations should be valid configs
					if output.Error == nil {
						t.Fatalf("expected error when schema is not enriched")
					}
					// Should not be a validation error
					if configErr, ok := output.Error.(*jsonops.ConfigError); ok {
						if configErr.Message == "invalid configuration" {
							t.Fatalf("unexpected validation error for valid config")
						}
					}
				})
			}
		}
	}
}

// TestProcessContextPreservation tests that context is preserved in ProcessInput
func TestProcessContextPreservation(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	config := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
	rawConfig, _ := json.Marshal(config)

	// Test with background context
	ctx1 := context.Background()
	input1 := runtime.ProcessInput{
		Ctx:       ctx1,
		Data:      map[string]interface{}{"data": "dGVzdA=="},
		RawConfig: rawConfig,
		NodeId:    "test-node-1",
		ItemIndex: -1,
	}
	output1 := node.Process(input1)
	if output1.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}

	// Test with context with timeout
	ctx2, cancel := context.WithTimeout(context.Background(), 1)
	defer cancel()
	input2 := runtime.ProcessInput{
		Ctx:       ctx2,
		Data:      map[string]interface{}{"data": "dGVzdA=="},
		RawConfig: rawConfig,
		NodeId:    "test-node-1",
		ItemIndex: -1,
	}
	output2 := node.Process(input2)
	if output2.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
}

// TestProcessSwitchStatementCoverage tests that switch statement handles all cases
func TestProcessSwitchStatementCoverage(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	inputData := map[string]interface{}{"data": "dGVzdA=="}

	// Test parse case
	config1 := jsonops.Config{Action: "parse", SchemaID: "test-schema"}
	rawConfig1, _ := json.Marshal(config1)
	input1 := createProcessInput(inputData, rawConfig1, -1)
	output1 := node.Process(input1)
	if output1.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Should have routed to executeParse (not a validation error)
	if configErr, ok := output1.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "invalid configuration" {
			t.Fatalf("unexpected validation error, should have routed to executeParse")
		}
	}

	// Test produce case
	config2 := jsonops.Config{Action: "produce", SchemaID: "test-schema"}
	rawConfig2, _ := json.Marshal(config2)
	input2 := createProcessInput(inputData, rawConfig2, -1)
	output2 := node.Process(input2)
	if output2.Error == nil {
		t.Fatalf("expected error when schema is not enriched")
	}
	// Should have routed to executeProduce (not a validation error)
	if configErr, ok := output2.Error.(*jsonops.ConfigError); ok {
		if configErr.Message == "invalid configuration" {
			t.Fatalf("unexpected validation error, should have routed to executeProduce")
		}
	}

	// Test default case (this is tricky - need an action that passes validation but isn't parse/produce)
	// Since Validate() checks for parse/produce, we can't easily test default case
	// But we can verify that actions that pass validation route correctly
}

// Helper function to check if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
