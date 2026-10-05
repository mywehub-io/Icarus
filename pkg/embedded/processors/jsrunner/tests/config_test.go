package tests

import (
	"context"
	"encoding/json"
	"errors"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func TestConfigStrictValidationDefault(t *testing.T) {
	cfg := jsrunner.Config{}
	if !cfg.GetStrictValidation() {
		t.Fatalf("expected default strict_validation to be true, got false")
	}

	strictFalse := false
	cfgLenient := jsrunner.Config{StrictValidation: &strictFalse}
	if cfgLenient.GetStrictValidation() {
		t.Fatalf("expected strict_validation false when explicitly set to false")
	}

	strictTrue := true
	cfgStrict := jsrunner.Config{StrictValidation: &strictTrue}
	if !cfgStrict.GetStrictValidation() {
		t.Fatalf("expected strict_validation true when explicitly set to true")
	}
}

func TestConfigApplyDefaults(t *testing.T) {
	cfg := jsrunner.Config{}
	cfg.ApplyDefaults()

	if cfg.Timeout != time.Minute {
		t.Fatalf("expected default timeout 1m, got %v", cfg.Timeout)
	}
	if cfg.SecurityLevel != jsrunner.SecurityLevelStandard {
		t.Fatalf("expected default security level standard, got %s", cfg.SecurityLevel)
	}
	expectedUtils := jsrunner.DefaultUtilitiesByLevel[jsrunner.SecurityLevelStandard]
	if len(cfg.EnabledUtilities) != len(expectedUtils) {
		t.Fatalf("expected default utilities %v, got %v", expectedUtils, cfg.EnabledUtilities)
	}
	if cfg.MaxStackDepth != 100 {
		t.Fatalf("expected default max_stack_depth 100, got %d", cfg.MaxStackDepth)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := jsrunner.Config{Script: "console.log('ok')"}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}

	bad := []jsrunner.Config{
		{},                         // missing script
		{Script: "x", Timeout: -1}, // invalid timeout
		{Script: "x", Timeout: time.Second, SecurityLevel: "bad"}, // invalid security
		{Script: "x", Timeout: time.Second, MaxStackDepth: 0},     // invalid stack depth
	}
	for i, c := range bad {
		if err := c.Validate(); err == nil {
			t.Fatalf("expected validation error for config %d: %+v", i, c)
		}
	}
}

func TestConfigHasOutputSchema(t *testing.T) {
	cfg := jsrunner.Config{}
	if cfg.HasOutputSchema() {
		t.Fatalf("expected HasOutputSchema false by default")
	}
	cfg.OutputSchemaID = "id"
	if !cfg.HasOutputSchema() {
		t.Fatalf("expected HasOutputSchema true when OutputSchemaID set")
	}
	cfg = jsrunner.Config{OutputSchema: map[string]interface{}{"type": "object"}}
	if !cfg.HasOutputSchema() {
		t.Fatalf("expected HasOutputSchema true when OutputSchema set")
	}
}

func TestConfigUnmarshalJSONTimeout(t *testing.T) {
	data := []byte(`{"script":"x","timeout":"250ms"}`)
	var cfg jsrunner.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if cfg.Timeout != 250*time.Millisecond {
		t.Fatalf("expected timeout 250ms, got %v", cfg.Timeout)
	}
}

// ===== Tests for Process method (lines 32-82 of jsrunner.go) =====

// TestProcess_ConfigParseError tests scenario when RawConfig contains invalid JSON
func TestProcess_ConfigParseError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": "test"},
		RawConfig: []byte(`{invalid json}`),
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error == nil {
		t.Fatalf("expected error, got nil")
	}
	if output.Data != nil {
		t.Fatalf("expected data to be nil on error")
	}
}

// TestProcess_ConfigValidationError tests scenario when config is invalid
func TestProcess_ConfigValidationError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config without script (invalid)
	rawConfig, _ := json.Marshal(jsrunner.Config{
		Timeout: time.Second,
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": "test"},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error == nil {
		t.Fatalf("expected error, got nil")
	}
}

// TestProcess_InputSchemaValidationError tests scenario when input validation fails
func TestProcess_InputSchemaValidationError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	strictTrue := true
	// Config with input schema that requires a string field
	rawConfig, _ := json.Marshal(jsrunner.Config{
		Script:  "({ output: input.value })",
		Timeout: 5 * time.Second,
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"value": map[string]interface{}{
					"type": "string",
				},
			},
			"required": []interface{}{"value"},
		},
		StrictValidation: &strictTrue,
	})

	// Input data with wrong type (number instead of string)
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": 123},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error == nil {
		t.Fatalf("expected error due to input validation, got nil")
	}
}

// TestProcess_InputSchemaIDConfigured tests that input schema path is checked when input_schema_id is set
func TestProcess_InputSchemaIDConfigured(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config with input_schema_id but no enriched inputSchema (will fail validation check)
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":          "({ output: input.value })",
		"timeout":         "5s",
		"input_schema_id": "some-schema-id",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": "test"},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should fail because input_schema_id is set but schema is not enriched
	if output.Error == nil {
		t.Fatalf("expected error due to missing enriched schema, got success")
	}
}

// TestProcess_ScriptExecutionTimeout tests scenario when script execution times out
func TestProcess_ScriptExecutionTimeout(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config with very short timeout and infinite loop script. The config is JSON as Apollo sends
	// it: json.Marshal of a Config writes Timeout as nanoseconds, which UnmarshalJSON rejects, so
	// the node would fail on its config and never run the script.
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: []byte(`{"script":"while(true) {}","timeout":"100ms"}`),
		ItemIndex: 0,
	}

	goroutinesBefore := goruntime.NumGoroutine()
	start := time.Now()
	output := node.Process(input)
	elapsed := time.Since(start)

	var timeoutErr *jsrunner.TimeoutError
	if !errors.As(output.Error, &timeoutErr) {
		t.Fatalf("expected a TimeoutError, got %v", output.Error)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("expected the 100ms timeout to stop the script, took %v", elapsed)
	}

	// The interrupted script's goroutine exits instead of spinning on.
	deadline := time.Now().Add(2 * time.Second)
	for goruntime.NumGoroutine() > goroutinesBefore {
		if time.Now().After(deadline) {
			t.Fatalf("script goroutine still running after the timeout: %d goroutines, %d before",
				goruntime.NumGoroutine(), goroutinesBefore)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestProcess_ScriptExecutionError tests scenario when script has runtime error
func TestProcess_ScriptExecutionError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// One-line statements that throw: each must run and fail with its own message, not with a
	// SyntaxError from being auto-returned as an expression.
	for _, script := range []string{
		"throw new Error('test error')",
		"throw new Error('test error');",
		"if (true) { throw new Error('test error') }",
	} {
		rawConfig, _ := json.Marshal(map[string]interface{}{"script": script, "timeout": "5s"})

		input := runtime.ProcessInput{
			Ctx:       context.Background(),
			Data:      map[string]interface{}{},
			RawConfig: rawConfig,
			ItemIndex: 0,
		}

		output := node.Process(input)

		var execErr *jsrunner.ExecutionError
		if !errors.As(output.Error, &execErr) {
			t.Fatalf("%q: expected an ExecutionError, got %v", script, output.Error)
		}
		if msg := output.Error.Error(); !strings.Contains(msg, "test error") || strings.Contains(msg, "SyntaxError") {
			t.Fatalf("%q: expected the script's own error, got %q", script, msg)
		}
	}
}

// TestProcess_OneLineExpressionReturnsValue checks a one-line expression is still returned.
func TestProcess_OneLineExpressionReturnsValue(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	for script, want := range map[string]int64{"1 + 1": 2, "[1, 2, 3].length;": 3} {
		rawConfig, _ := json.Marshal(map[string]interface{}{"script": script})
		output := node.Process(runtime.ProcessInput{
			Ctx:       context.Background(),
			Data:      map[string]interface{}{},
			RawConfig: rawConfig,
		})
		if output.Error != nil {
			t.Fatalf("%q: unexpected error: %v", script, output.Error)
		}
		if got := output.Data["result"]; got != want {
			t.Fatalf("%q: expected result %d, got %v (%T)", script, want, got, got)
		}
	}
}

// TestProcess_OutputSchemaValidationError tests scenario when output validation fails
func TestProcess_OutputSchemaValidationError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config with output schema expecting string, but script returns number
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ output: 123 })",
		"timeout": "5s",
		"outputSchema": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"output": map[string]interface{}{
					"type": "string",
				},
			},
			"required": []interface{}{"output"},
		},
		"strict_validation": true,
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error == nil {
		t.Fatalf("expected error due to output validation, got nil")
	}
}

// TestProcess_OutputSchemaIDConfigured tests that output schema path is checked when output_schema_id is set
func TestProcess_OutputSchemaIDConfigured(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config with output_schema_id but no enriched outputSchema (will fail validation check)
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":           "({ output: 'test result' })",
		"timeout":          "5s",
		"output_schema_id": "some-schema-id",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should fail because output_schema_id is set but schema is not enriched
	if output.Error == nil {
		t.Fatalf("expected error due to missing enriched schema, got success")
	}
}

// TestProcess_SuccessWithoutSchemas tests successful execution without any schemas
func TestProcess_SuccessWithoutSchemas(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Simple config without any schemas
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.value * 2 })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": 42},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected data to be non-nil")
	}
	result, ok := output.Data["result"].(int64)
	if !ok {
		t.Fatalf("expected result to be int64")
	}
	if result != 84 {
		t.Fatalf("expected result to be 84, got %v", result)
	}
}

// TestProcess_SuccessWithString tests successful execution returning a string value
func TestProcess_SuccessWithString(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config that transforms input string
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ output: input.value.toUpperCase() })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": "hello"},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected data to be non-nil")
	}
	if output.Data["output"] != "HELLO" {
		t.Fatalf("expected output to be 'HELLO', got %v", output.Data["output"])
	}
}

// TestProcess_ScriptSyntaxError tests scenario when script has syntax error
func TestProcess_ScriptSyntaxError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Script with syntax error
	rawConfig, _ := json.Marshal(jsrunner.Config{
		Script:  "({ result: )",
		Timeout: 5 * time.Second,
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error == nil {
		t.Fatalf("expected error due to syntax error, got nil")
	}
}

// TestProcess_EmptyScriptResult tests scenario when script returns empty/undefined
func TestProcess_EmptyScriptResult(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Script that doesn't return anything
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "undefined",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	// Should return empty map when script returns undefined
	if output.Data == nil {
		t.Fatalf("expected data to be non-nil")
	}
}

// TestProcess_ContextCancellation tests scenario when context is cancelled
func TestProcess_ContextCancellation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Create context that will be cancelled immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Config with long-running script
	input := runtime.ProcessInput{
		Ctx:       ctx,
		Data:      map[string]interface{}{},
		RawConfig: []byte(`{"script":"while(true) {}","timeout":"5s"}`),
		ItemIndex: 0,
	}

	start := time.Now()
	output := node.Process(input)

	var timeoutErr *jsrunner.TimeoutError
	if !errors.As(output.Error, &timeoutErr) {
		t.Fatalf("expected the cancelled script to stop with a TimeoutError, got %v", output.Error)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("expected cancellation to stop the script at once, took %v", elapsed)
	}
}

// TestProcess_ComplexDataTypes tests processing with complex nested data structures
func TestProcess_ComplexDataTypes(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			users: input.users.map(u => ({
				name: u.name.toUpperCase(),
				age: u.age + 1
			}))
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"users": []interface{}{
				map[string]interface{}{"name": "alice", "age": 30},
				map[string]interface{}{"name": "bob", "age": 25},
			},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected data to be non-nil")
	}
}

// TestProcess_ManualInputs tests that manual inputs are injected into the script context
func TestProcess_ManualInputs(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Config with manual inputs
	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.value + manualInputs.extra })",
		"timeout": "5s",
		"manual_inputs": map[string]interface{}{
			"extra": 10,
		},
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": 5},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatalf("expected data to be non-nil")
	}
	result, ok := output.Data["result"].(int64)
	if !ok {
		t.Fatalf("expected result to be int64")
	}
	if result != 15 {
		t.Fatalf("expected result to be 15, got %v", result)
	}
}

// ===== Additional comprehensive test scenarios =====

// TestProcess_SecurityLevelStrict tests script execution with strict security level
func TestProcess_SecurityLevelStrict(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":         "({ message: 'strict mode' })",
		"timeout":        "5s",
		"security_level": "strict",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["message"] != "strict mode" {
		t.Fatalf("expected message 'strict mode', got %v", output.Data["message"])
	}
}

// TestProcess_SecurityLevelPermissive tests script execution with permissive security level
func TestProcess_SecurityLevelPermissive(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":         "({ message: 'permissive mode' })",
		"timeout":        "5s",
		"security_level": "permissive",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["message"] != "permissive mode" {
		t.Fatalf("expected message 'permissive mode', got %v", output.Data["message"])
	}
}

// TestProcess_MultilineScript tests execution of multi-line script
func TestProcess_MultilineScript(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			const x = input.value * 2;
			const y = x + 10;
			return { result: y };
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"value": 5},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	result, ok := output.Data["result"].(int64)
	if !ok {
		t.Fatalf("expected result to be int64")
	}
	if result != 20 {
		t.Fatalf("expected result to be 20, got %v", result)
	}
}

// TestProcess_ArrayReturnValue tests when script returns array (should be wrapped in result)
func TestProcess_ArrayReturnValue(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "[1, 2, 3, 4, 5]",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	// Non-map results should be wrapped in "result" key
	if output.Data["result"] == nil {
		t.Fatalf("expected result key to be present")
	}
}

// TestProcess_PrimitiveReturnValue tests when script returns primitive value
func TestProcess_PrimitiveReturnValue(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "42",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	// Primitive results should be wrapped in "result" key
	result, ok := output.Data["result"].(int64)
	if !ok {
		t.Fatalf("expected result to be int64")
	}
	if result != 42 {
		t.Fatalf("expected result to be 42, got %v", result)
	}
}

// TestProcess_NullReturnValue tests when script returns null
func TestProcess_NullReturnValue(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "null",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	// Null should return empty map
	if len(output.Data) != 0 {
		t.Fatalf("expected empty map for null result, got %v", output.Data)
	}
}

// TestProcess_EmptyInputData tests processing with empty input data
func TestProcess_EmptyInputData(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ hasInput: typeof input !== 'undefined' })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	hasInput, ok := output.Data["hasInput"].(bool)
	if !ok || !hasInput {
		t.Fatalf("expected hasInput to be true")
	}
}

// TestProcess_NestedObjectAccess tests accessing deeply nested object properties
func TestProcess_NestedObjectAccess(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ value: input.user.profile.name })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"user": map[string]interface{}{
				"profile": map[string]interface{}{
					"name": "John Doe",
				},
			},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["value"] != "John Doe" {
		t.Fatalf("expected value 'John Doe', got %v", output.Data["value"])
	}
}

// TestProcess_ArrayMapOperation tests mapping over array in input
func TestProcess_ArrayMapOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ doubled: input.numbers.map(n => n * 2) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numbers": []interface{}{1, 2, 3, 4, 5},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["doubled"] == nil {
		t.Fatalf("expected doubled array to be present")
	}
}

// TestProcess_ArrayFilterOperation tests filtering array in input
func TestProcess_ArrayFilterOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ evens: input.numbers.filter(n => n % 2 === 0) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numbers": []interface{}{1, 2, 3, 4, 5, 6},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["evens"] == nil {
		t.Fatalf("expected evens array to be present")
	}
}

// TestProcess_ArrayReduceOperation tests reducing array in input
func TestProcess_ArrayReduceOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ sum: input.numbers.reduce((acc, n) => acc + n, 0) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numbers": []interface{}{1, 2, 3, 4, 5},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	sum, ok := output.Data["sum"].(int64)
	if !ok {
		t.Fatalf("expected sum to be int64")
	}
	if sum != 15 {
		t.Fatalf("expected sum to be 15, got %v", sum)
	}
}

// TestProcess_StringManipulation tests various string operations
func TestProcess_StringManipulation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			upper: input.text.toUpperCase(),
			lower: input.text.toLowerCase(),
			length: input.text.length,
			reversed: input.text.split('').reverse().join('')
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"text": "Hello",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["upper"] != "HELLO" {
		t.Fatalf("expected upper 'HELLO', got %v", output.Data["upper"])
	}
	if output.Data["lower"] != "hello" {
		t.Fatalf("expected lower 'hello', got %v", output.Data["lower"])
	}
	if output.Data["reversed"] != "olleH" {
		t.Fatalf("expected reversed 'olleH', got %v", output.Data["reversed"])
	}
}

// TestProcess_ConditionalLogic tests if-else logic in script
func TestProcess_ConditionalLogic(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			category: input.age >= 18 ? 'adult' : 'minor',
			canVote: input.age >= 18
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"age": 25,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["category"] != "adult" {
		t.Fatalf("expected category 'adult', got %v", output.Data["category"])
	}
	canVote, ok := output.Data["canVote"].(bool)
	if !ok || !canVote {
		t.Fatalf("expected canVote to be true")
	}
}

// TestProcess_DateManipulation tests date operations in script
func TestProcess_DateManipulation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			timestamp: new Date().getTime(),
			hasDate: typeof Date !== 'undefined'
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	hasDate, ok := output.Data["hasDate"].(bool)
	if !ok || !hasDate {
		t.Fatalf("expected hasDate to be true")
	}
}

// TestProcess_JSONParsing tests JSON.parse in script
func TestProcess_JSONParsing(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			parsed: JSON.parse(input.jsonString)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"jsonString": `{"key": "value"}`,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["parsed"] == nil {
		t.Fatalf("expected parsed object to be present")
	}
}

// TestProcess_JSONStringify tests JSON.stringify in script
func TestProcess_JSONStringify(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			stringified: JSON.stringify(input.data)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"data": map[string]interface{}{"key": "value"},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	stringified, ok := output.Data["stringified"].(string)
	if !ok {
		t.Fatalf("expected stringified to be string")
	}
	if stringified != `{"key":"value"}` {
		t.Fatalf("expected stringified to be '{\"key\":\"value\"}', got %v", stringified)
	}
}

// TestProcess_MathOperations tests various Math operations
func TestProcess_MathOperations(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			max: Math.max(input.numbers[0], input.numbers[1], input.numbers[2]),
			min: Math.min(input.numbers[0], input.numbers[1], input.numbers[2]),
			round: Math.round(input.decimal),
			floor: Math.floor(input.decimal),
			ceil: Math.ceil(input.decimal)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numbers": []interface{}{5, 10, 3},
			"decimal": 4.6,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	max, _ := output.Data["max"].(int64)
	if max != 10 {
		t.Fatalf("expected max to be 10, got %v", max)
	}
	min, _ := output.Data["min"].(int64)
	if min != 3 {
		t.Fatalf("expected min to be 3, got %v", min)
	}
}

// TestProcess_TryCatchError tests error handling within script
func TestProcess_TryCatchError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			try {
				throw new Error('test error');
			} catch (e) {
				return { caught: true, message: e.message };
			}
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	caught, ok := output.Data["caught"].(bool)
	if !ok || !caught {
		t.Fatalf("expected caught to be true")
	}
	if output.Data["message"] != "test error" {
		t.Fatalf("expected message 'test error', got %v", output.Data["message"])
	}
}

// TestProcess_BooleanLogic tests boolean operations
func TestProcess_BooleanLogic(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			and: input.a && input.b,
			or: input.a || input.c,
			not: !input.c
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"a": true,
			"b": true,
			"c": false,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	and, ok := output.Data["and"].(bool)
	if !ok || !and {
		t.Fatalf("expected and to be true")
	}
	or, ok := output.Data["or"].(bool)
	if !ok || !or {
		t.Fatalf("expected or to be true")
	}
	not, ok := output.Data["not"].(bool)
	if !ok || !not {
		t.Fatalf("expected not to be true")
	}
}

// TestProcess_ObjectDestructuring tests object destructuring in script
func TestProcess_ObjectDestructuring(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			const { name, age } = input.user;
			return { userName: name, userAge: age };
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"user": map[string]interface{}{
				"name": "Alice",
				"age":  30,
			},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["userName"] != "Alice" {
		t.Fatalf("expected userName 'Alice', got %v", output.Data["userName"])
	}
}

// TestProcess_TemplateStrings tests template string usage
func TestProcess_TemplateStrings(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ greeting: `Hello, ${input.name}! You are ${input.age} years old.` })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"name": "Bob",
			"age":  25,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["greeting"] != "Hello, Bob! You are 25 years old." {
		t.Fatalf("expected greeting message, got %v", output.Data["greeting"])
	}
}

// TestProcess_SpreadOperator tests spread operator usage
func TestProcess_SpreadOperator(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			combined: [...input.arr1, ...input.arr2],
			merged: { ...input.obj1, ...input.obj2 }
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr1": []interface{}{1, 2},
			"arr2": []interface{}{3, 4},
			"obj1": map[string]interface{}{"a": 1},
			"obj2": map[string]interface{}{"b": 2},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["combined"] == nil {
		t.Fatalf("expected combined array to be present")
	}
	if output.Data["merged"] == nil {
		t.Fatalf("expected merged object to be present")
	}
}

// TestProcess_CustomTimeout tests custom timeout configuration
func TestProcess_CustomTimeout(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: 'quick' })",
		"timeout": "100ms",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["result"] != "quick" {
		t.Fatalf("expected result 'quick', got %v", output.Data["result"])
	}
}

// TestProcess_LargeDataProcessing tests processing large datasets
func TestProcess_LargeDataProcessing(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// Create large array
	largeArray := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		largeArray[i] = i
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ count: input.data.length, sum: input.data.reduce((a, b) => a + b, 0) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"data": largeArray,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	count, ok := output.Data["count"].(int64)
	if !ok || count != 1000 {
		t.Fatalf("expected count to be 1000, got %v", count)
	}
}

// TestProcess_UnicodeHandling tests unicode character handling
func TestProcess_UnicodeHandling(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ message: input.emoji + ' ' + input.text })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"emoji": "🚀",
			"text":  "Hello 世界",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["message"] != "🚀 Hello 世界" {
		t.Fatalf("expected unicode message, got %v", output.Data["message"])
	}
}

// TestProcess_RegexOperations tests regular expression operations
func TestProcess_RegexOperations(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			matches: input.text.match(/\d+/g),
			replaced: input.text.replace(/\d+/g, 'X')
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"text": "test123abc456",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["replaced"] != "testXabcX" {
		t.Fatalf("expected replaced 'testXabcX', got %v", output.Data["replaced"])
	}
}

// ===== Additional comprehensive edge case and feature tests =====

// TestProcess_ReferenceError tests accessing undefined variables
func TestProcess_ReferenceError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ value: undefinedVariable })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should fail with reference error
	if output.Error == nil {
		t.Fatalf("expected error for undefined variable, got success")
	}
}

// TestProcess_TypeCoercion tests JavaScript type coercion
func TestProcess_TypeCoercion(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			stringNum: '5' * 2,
			plusConcat: 5 + '5',
			boolToNum: Number(true),
			numToBool: Boolean(1)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["stringNum"].(int64) != 10 {
		t.Fatalf("expected stringNum to be 10")
	}
	if output.Data["plusConcat"] != "55" {
		t.Fatalf("expected plusConcat to be '55'")
	}
}

// TestProcess_Truthiness tests JavaScript truthiness/falsiness
func TestProcess_Truthiness(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			emptyString: Boolean(''),
			zero: Boolean(0),
			nullVal: Boolean(null),
			undefinedVal: Boolean(undefined),
			emptyArray: Boolean([]),
			emptyObject: Boolean({})
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	// Empty arrays and objects are truthy in JavaScript
	if output.Data["emptyArray"].(bool) != true {
		t.Fatalf("expected emptyArray to be true")
	}
	if output.Data["emptyObject"].(bool) != true {
		t.Fatalf("expected emptyObject to be true")
	}
}

// TestProcess_ArrowFunctions tests arrow function syntax
func TestProcess_ArrowFunctions(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			doubled: input.numbers.map(n => n * 2),
			sum: input.numbers.reduce((a, b) => a + b, 0)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numbers": []interface{}{1, 2, 3},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["doubled"] == nil {
		t.Fatalf("expected doubled array")
	}
}

// TestProcess_ObjectMethods tests Object static methods
func TestProcess_ObjectMethods(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			keys: Object.keys(input.obj),
			values: Object.values(input.obj),
			hasName: input.obj.hasOwnProperty('name')
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"obj": map[string]interface{}{
				"name": "Alice",
				"age":  30,
			},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["keys"] == nil {
		t.Fatalf("expected keys array")
	}
	hasName, ok := output.Data["hasName"].(bool)
	if !ok || !hasName {
		t.Fatalf("expected hasName to be true")
	}
}

// TestProcess_ArraySliceSplice tests array slice and splice
func TestProcess_ArraySliceSplice(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			sliced: input.arr.slice(1, 3),
			first: input.arr[0],
			last: input.arr[input.arr.length - 1]
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr": []interface{}{1, 2, 3, 4, 5},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["first"].(int64) != 1 {
		t.Fatalf("expected first to be 1")
	}
	if output.Data["last"].(int64) != 5 {
		t.Fatalf("expected last to be 5")
	}
}

// TestProcess_ArrayFindMethods tests find, findIndex, some, every
func TestProcess_ArrayFindMethods(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			found: input.arr.find(n => n > 3),
			foundIndex: input.arr.findIndex(n => n > 3),
			hasLarge: input.arr.some(n => n > 3),
			allPositive: input.arr.every(n => n > 0)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr": []interface{}{1, 2, 3, 4, 5},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["found"].(int64) != 4 {
		t.Fatalf("expected found to be 4")
	}
	allPositive, ok := output.Data["allPositive"].(bool)
	if !ok || !allPositive {
		t.Fatalf("expected allPositive to be true")
	}
}

// TestProcess_ArrayIncludes tests array includes method
func TestProcess_ArrayIncludes(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			hasThree: input.arr.includes(3),
			hasTen: input.arr.includes(10)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr": []interface{}{1, 2, 3, 4, 5},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	hasThree, ok := output.Data["hasThree"].(bool)
	if !ok || !hasThree {
		t.Fatalf("expected hasThree to be true")
	}
	hasTen, ok := output.Data["hasTen"].(bool)
	if !ok || hasTen {
		t.Fatalf("expected hasTen to be false")
	}
}

// TestProcess_StringMethods tests various string methods
func TestProcess_StringMethods(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			substring: input.text.substring(0, 5),
			indexOf: input.text.indexOf('World'),
			includes: input.text.includes('Hello'),
			startsWith: input.text.startsWith('Hello'),
			endsWith: input.text.endsWith('!')
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"text": "Hello World!",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["substring"] != "Hello" {
		t.Fatalf("expected substring 'Hello', got %v", output.Data["substring"])
	}
	includes, ok := output.Data["includes"].(bool)
	if !ok || !includes {
		t.Fatalf("expected includes to be true")
	}
}

// TestProcess_StringTrimPad tests trim and pad methods
func TestProcess_StringTrimPad(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			trimmed: input.text.trim(),
			padded: input.short.padStart(5, '0'),
			repeated: input.char.repeat(3)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"text":  "  hello  ",
			"short": "42",
			"char":  "x",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["trimmed"] != "hello" {
		t.Fatalf("expected trimmed 'hello', got %v", output.Data["trimmed"])
	}
	if output.Data["padded"] != "00042" {
		t.Fatalf("expected padded '00042', got %v", output.Data["padded"])
	}
	if output.Data["repeated"] != "xxx" {
		t.Fatalf("expected repeated 'xxx', got %v", output.Data["repeated"])
	}
}

// TestProcess_NaNAndInfinity tests special numeric values
func TestProcess_NaNAndInfinity(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			isNaN: isNaN(input.invalid),
			isFinite: isFinite(input.value),
			divided: input.value / 0
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"invalid": "not a number",
			"value":   42,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	isFinite, ok := output.Data["isFinite"].(bool)
	if !ok || !isFinite {
		t.Fatalf("expected isFinite to be true")
	}
}

// TestProcess_NegativeNumbers tests negative number handling
func TestProcess_NegativeNumbers(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			abs: Math.abs(input.negative),
			sign: Math.sign(input.negative),
			isNegative: input.negative < 0
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"negative": -42,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	abs, ok := output.Data["abs"].(int64)
	if !ok || abs != 42 {
		t.Fatalf("expected abs to be 42")
	}
	isNegative, ok := output.Data["isNegative"].(bool)
	if !ok || !isNegative {
		t.Fatalf("expected isNegative to be true")
	}
}

// TestProcess_FloatingPoint tests floating point operations
func TestProcess_FloatingPoint(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			sum: input.a + input.b,
			rounded: Math.round((input.a + input.b) * 100) / 100,
			fixed: (input.a + input.b).toFixed(2)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"a": 0.1,
			"b": 0.2,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["fixed"] != "0.30" {
		t.Fatalf("expected fixed to be '0.30', got %v", output.Data["fixed"])
	}
}

// TestProcess_EmptyArraysAndStrings tests empty collections
func TestProcess_EmptyArraysAndStrings(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			arrLength: input.arr.length,
			strLength: input.str.length,
			isEmpty: input.arr.length === 0 && input.str.length === 0
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr": []interface{}{},
			"str": "",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	isEmpty, ok := output.Data["isEmpty"].(bool)
	if !ok || !isEmpty {
		t.Fatalf("expected isEmpty to be true")
	}
}

// TestProcess_ArraySort tests array sorting
func TestProcess_ArraySort(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			sorted: [...input.arr].sort((a, b) => a - b),
			reversed: [...input.arr].sort((a, b) => b - a)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr": []interface{}{3, 1, 4, 1, 5, 9, 2, 6},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["sorted"] == nil {
		t.Fatalf("expected sorted array")
	}
}

// TestProcess_ArrayConcat tests array concatenation
func TestProcess_ArrayConcat(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			combined: input.arr1.concat(input.arr2),
			length: input.arr1.concat(input.arr2).length
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr1": []interface{}{1, 2, 3},
			"arr2": []interface{}{4, 5, 6},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	length, ok := output.Data["length"].(int64)
	if !ok || length != 6 {
		t.Fatalf("expected length to be 6")
	}
}

// TestProcess_NestedLoops tests nested iteration
func TestProcess_NestedLoops(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			const result = [];
			for (let i = 0; i < input.rows; i++) {
				for (let j = 0; j < input.cols; j++) {
					result.push([i, j]);
				}
			}
			return { matrix: result, count: result.length };
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"rows": 3,
			"cols": 3,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	count, ok := output.Data["count"].(int64)
	if !ok || count != 9 {
		t.Fatalf("expected count to be 9, got %v", count)
	}
}

// TestProcess_ObjectAssign tests Object.assign
func TestProcess_ObjectAssign(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			merged: Object.assign({}, input.obj1, input.obj2)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"obj1": map[string]interface{}{"a": 1, "b": 2},
			"obj2": map[string]interface{}{"b": 3, "c": 4},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	merged, ok := output.Data["merged"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected merged to be an object")
	}
	if merged["c"].(int64) != 4 {
		t.Fatalf("expected merged.c to be 4")
	}
}

// TestProcess_MultipleInputProperties tests complex input handling
func TestProcess_MultipleInputProperties(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			fullName: input.firstName + ' ' + input.lastName,
			age: input.age,
			email: input.email,
			isAdult: input.age >= 18,
			profile: {
				name: input.firstName,
				contact: input.email
			}
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"firstName": "John",
			"lastName":  "Doe",
			"age":       25,
			"email":     "john@example.com",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["fullName"] != "John Doe" {
		t.Fatalf("expected fullName 'John Doe', got %v", output.Data["fullName"])
	}
}

// TestProcess_ArrayOfObjects tests processing arrays of objects
func TestProcess_ArrayOfObjects(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			names: input.users.map(u => u.name),
			totalAge: input.users.reduce((sum, u) => sum + u.age, 0),
			adults: input.users.filter(u => u.age >= 18).length
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"users": []interface{}{
				map[string]interface{}{"name": "Alice", "age": 25},
				map[string]interface{}{"name": "Bob", "age": 17},
				map[string]interface{}{"name": "Charlie", "age": 30},
			},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	totalAge, ok := output.Data["totalAge"].(int64)
	if !ok || totalAge != 72 {
		t.Fatalf("expected totalAge to be 72, got %v", totalAge)
	}
	adults, ok := output.Data["adults"].(int64)
	if !ok || adults != 2 {
		t.Fatalf("expected adults to be 2, got %v", adults)
	}
}

// TestProcess_ChainedOperations tests method chaining
func TestProcess_ChainedOperations(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			result: input.numbers
				.filter(n => n > 2)
				.map(n => n * 2)
				.reduce((a, b) => a + b, 0)
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numbers": []interface{}{1, 2, 3, 4, 5},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	// Filter > 2: [3,4,5], map *2: [6,8,10], sum: 24
	result, ok := output.Data["result"].(int64)
	if !ok || result != 24 {
		t.Fatalf("expected result to be 24, got %v", result)
	}
}

// TestProcess_DefaultParameters tests default parameter values
func TestProcess_DefaultParameters(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			const greet = (name = 'Guest') => 'Hello, ' + name;
			return {
				withName: greet(input.name),
				withoutName: greet()
			};
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"name": "Alice",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["withName"] != "Hello, Alice" {
		t.Fatalf("expected 'Hello, Alice', got %v", output.Data["withName"])
	}
	if output.Data["withoutName"] != "Hello, Guest" {
		t.Fatalf("expected 'Hello, Guest', got %v", output.Data["withoutName"])
	}
}

// TestProcess_ShortCircuitEvaluation tests && and || short-circuit behavior
func TestProcess_ShortCircuitEvaluation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({
			withDefault: input.value || 'default',
			withValue: input.realValue || 'default',
			conditional: input.flag && input.message
		})`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"value":     nil,
			"realValue": "actual",
			"flag":      true,
			"message":   "hello",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	if output.Error != nil {
		t.Fatalf("expected success, got error: %v", output.Error)
	}
	if output.Data["withDefault"] != "default" {
		t.Fatalf("expected 'default', got %v", output.Data["withDefault"])
	}
	if output.Data["withValue"] != "actual" {
		t.Fatalf("expected 'actual', got %v", output.Data["withValue"])
	}
}

// ===== Comprehensive Error Handling Tests for Every Scenario =====

// TestProcess_Error_DivisionByZero tests division by zero handling
func TestProcess_Error_DivisionByZero(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.numerator / input.denominator })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"numerator":   10,
			"denominator": 0,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Division by zero in JavaScript returns Infinity, not an error
	if output.Error != nil {
		t.Fatalf("expected success (Infinity), got error: %v", output.Error)
	}
}

// TestProcess_Error_NullPropertyAccess tests accessing properties of null
func TestProcess_Error_NullPropertyAccess(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ value: input.obj.property })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"obj": nil,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when accessing property of null
	if output.Error == nil {
		t.Fatalf("expected error for null property access, got success")
	}
}

// TestProcess_Error_UndefinedPropertyAccess tests accessing undefined nested properties
func TestProcess_Error_UndefinedPropertyAccess(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ value: input.obj.nested.property })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"obj": map[string]interface{}{},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when accessing nested property of undefined
	if output.Error == nil {
		t.Fatalf("expected error for undefined nested property access, got success")
	}
}

// TestProcess_Error_ArrayIndexOutOfBounds tests accessing invalid array index
func TestProcess_Error_ArrayIndexOutOfBounds(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ value: input.arr[100] })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"arr": []interface{}{1, 2, 3},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// JavaScript returns undefined for out of bounds, not error
	if output.Error != nil {
		t.Fatalf("expected success (undefined), got error: %v", output.Error)
	}
	// Value should be undefined/nil
	if output.Data["value"] != nil {
		t.Fatalf("expected undefined value, got %v", output.Data["value"])
	}
}

// TestProcess_Error_InvalidJSONParse tests JSON.parse with invalid JSON
func TestProcess_Error_InvalidJSONParse(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ parsed: JSON.parse(input.invalidJson) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"invalidJson": "{invalid json}",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on invalid JSON
	if output.Error == nil {
		t.Fatalf("expected error for invalid JSON parse, got success")
	}
}

// TestProcess_Error_InvalidRegex tests invalid regex pattern
func TestProcess_Error_InvalidRegex(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.text.match(/[/)) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"text": "test",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on invalid regex
	if output.Error == nil {
		t.Fatalf("expected error for invalid regex, got success")
	}
}

// TestProcess_Error_CallingNonFunction tests calling a non-function
func TestProcess_Error_CallingNonFunction(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.notAFunction() })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"notAFunction": "string",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when calling non-function
	if output.Error == nil {
		t.Fatalf("expected error for calling non-function, got success")
	}
}

// TestProcess_Error_InfiniteRecursion tests stack overflow from infinite recursion
func TestProcess_Error_InfiniteRecursion(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			function recurse() {
				return recurse();
			}
			return { result: recurse() };
		`,
		"timeout": "100ms", // Short timeout to catch runaway recursion
	})

	// Create context with timeout to prevent test hanging
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	input := runtime.ProcessInput{
		Ctx:       ctx,
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error or timeout on infinite recursion
	if output.Error == nil {
		t.Fatalf("expected error for infinite recursion, got success")
	}
}

// TestProcess_Error_ThrowCustomError tests throwing custom errors
func TestProcess_Error_ThrowCustomError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "throw new Error('Custom error message')",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error with custom message
	if output.Error == nil {
		t.Fatalf("expected error, got success")
	}
}

// TestProcess_Error_ThrowString tests throwing non-Error objects
func TestProcess_Error_ThrowString(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "throw 'String error'",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error even when throwing strings
	if output.Error == nil {
		t.Fatalf("expected error, got success")
	}
}

// TestProcess_Error_TypeError tests type errors
func TestProcess_Error_TypeError(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.number.toUpperCase() })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"number": 42,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on type mismatch
	if output.Error == nil {
		t.Fatalf("expected type error, got success")
	}
}

// TestProcess_Error_MissingRequiredInput tests missing input data
func TestProcess_Error_MissingRequiredInput(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.requiredField.toUpperCase() })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{}, // Missing requiredField
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when required field is missing
	if output.Error == nil {
		t.Fatalf("expected error for missing required field, got success")
	}
}

// TestProcess_Error_InvalidArrayOperation tests invalid array operations
func TestProcess_Error_InvalidArrayOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.notAnArray.map(x => x * 2) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"notAnArray": "string",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when calling array method on non-array
	if output.Error == nil {
		t.Fatalf("expected error for invalid array operation, got success")
	}
}

// TestProcess_Error_InfiniteLoop tests timeout on infinite loop
func TestProcess_Error_InfiniteLoop(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "while(true) { /* infinite loop */ }",
		"timeout": "100ms",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should timeout
	if output.Error == nil {
		t.Fatalf("expected timeout error, got success")
	}
}

// TestProcess_Error_InvalidDateOperation tests invalid date operations
func TestProcess_Error_InvalidDateOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: new Date(input.invalid).toISOString() })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"invalid": "not a valid date string really",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Invalid dates result in "Invalid Date" but toISOString throws
	if output.Error == nil {
		t.Fatalf("expected error for invalid date, got success")
	}
}

// TestProcess_Error_InvalidOutputType tests wrong output type
func TestProcess_Error_InvalidOutputType(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({ output: 123 })`, // Returns number
		"outputSchema": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"output": map[string]interface{}{
					"type": "string", // Expects string
				},
			},
			"required": []interface{}{"output"},
		},
		"strict_validation": true,
		"timeout":           "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on output validation
	if output.Error == nil {
		t.Fatalf("expected output validation error, got success")
	}
}

// TestProcess_Error_MissingOutputField tests missing required output field
func TestProcess_Error_MissingOutputField(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({ other: 'value' })`, // Missing required field
		"outputSchema": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"required_field": map[string]interface{}{
					"type": "string",
				},
			},
			"required": []interface{}{"required_field"},
		},
		"strict_validation": true,
		"timeout":           "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on missing required output field
	if output.Error == nil {
		t.Fatalf("expected error for missing output field, got success")
	}
}

// TestProcess_Error_StringMethodOnNumber tests calling string method on number
func TestProcess_Error_StringMethodOnNumber(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.value.substring(0, 5) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"value": 12345,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on type mismatch
	if output.Error == nil {
		t.Fatalf("expected type error, got success")
	}
}

// TestProcess_Error_InvalidSplit tests split on non-string
func TestProcess_Error_InvalidSplit(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: input.value.split(',') })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"value": []interface{}{1, 2, 3},
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when calling string method on array
	if output.Error == nil {
		t.Fatalf("expected error, got success")
	}
}

// TestProcess_Error_CircularReference tests handling circular references
func TestProcess_Error_CircularReference(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			const obj = { a: 1 };
			obj.self = obj;
			return { result: JSON.stringify(obj) };
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on circular reference in JSON.stringify
	if output.Error == nil {
		t.Fatalf("expected error for circular reference, got success")
	}
}

// TestProcess_Error_InvalidMathOperation tests invalid math operations
func TestProcess_Error_InvalidMathOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ result: Math.sqrt(input.value) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"value": "not a number",
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Math operations on invalid types return NaN, not error
	if output.Error != nil {
		t.Fatalf("expected success (NaN), got error: %v", output.Error)
	}
}

// TestProcess_Error_UncaughtException tests uncaught exceptions
func TestProcess_Error_UncaughtException(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `
			function doSomething() {
				throw new Error('Uncaught error');
			}
			return { result: doSomething() };
		`,
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on uncaught exception
	if output.Error == nil {
		t.Fatalf("expected error for uncaught exception, got success")
	}
}

// TestProcess_Error_InvalidObjectOperation tests invalid object operations
func TestProcess_Error_InvalidObjectOperation(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "({ keys: Object.keys(input.notAnObject) })",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"notAnObject": nil,
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error when calling Object methods on null
	if output.Error == nil {
		t.Fatalf("expected error for Object.keys on null, got success")
	}
}

// TestProcess_Error_EmptyScript tests empty script error
func TestProcess_Error_EmptyScript(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script":  "",
		"timeout": "5s",
	})

	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on empty script (validation)
	if output.Error == nil {
		t.Fatalf("expected error for empty script, got success")
	}
}

// TestProcess_Error_InvalidInputSchema tests input schema mismatch
func TestProcess_Error_InvalidInputSchema(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{
		NodeId:     "test-node",
		PluginType: "plugin-js",
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	rawConfig, _ := json.Marshal(map[string]interface{}{
		"script": `({ output: input.value * 2 })`,
		"inputSchema": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"value": map[string]interface{}{
					"type": "string",
				},
			},
			"required": []interface{}{"value"},
		},
		"strict_validation": true,
		"timeout":           "5s",
	})

	input := runtime.ProcessInput{
		Ctx: context.Background(),
		Data: map[string]interface{}{
			"value": 123, // Number instead of string
		},
		RawConfig: rawConfig,
		ItemIndex: 0,
	}

	output := node.Process(input)

	// Should error on input validation
	if output.Error == nil {
		t.Fatalf("expected input validation error, got success")
	}
}
