package jsrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/dop251/goja"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// JSRunnerNode implements JavaScript execution for embedded
type JSRunnerNode struct {
	runtime.BaseNode

	// programs caches the compiled script (D20: compile once per node, new VM per item). The key
	// is the wrapped script; a node's script does not change, so this holds one entry. Workers
	// share the node, and a *goja.Program is safe to run on many VMs at once.
	programs sync.Map // string -> *compiledScript
}

type compiledScript struct {
	once sync.Once
	prog *goja.Program
	err  error
}

// program returns the compiled form of script, compiling it on first use.
func (n *JSRunnerNode) program(script string) (*goja.Program, error) {
	v, _ := n.programs.LoadOrStore(script, &compiledScript{})
	c := v.(*compiledScript)
	c.once.Do(func() { c.prog, c.err = goja.Compile("script.js", script, false) })
	return c.prog, c.err
}

// NewJSRunnerNode creates a new jsrunner node instance
func NewJSRunnerNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	// Validate plugin type
	if config.PluginType != "plugin-js" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-js', got '%s'", config.PluginType)
	}

	return &JSRunnerNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Process executes the JavaScript code
func (n *JSRunnerNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	// Parse configuration
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "failed to parse configuration", err))
	}

	// Apply defaults
	cfg.ApplyDefaults()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "invalid configuration", err))
	}

	// If input schema is configured, validate the input before executing script
	if cfg.HasInputSchema() {
		validatedInput, err := n.validateInput(input, &cfg)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		// Update input.Data with validated data
		input.Data = validatedInput
	}

	// Execute JavaScript with timeout
	ctx, cancel := context.WithTimeout(input.Ctx, cfg.Timeout)
	defer cancel()

	result, err := n.executeScript(ctx, input, &cfg)
	if err != nil {
		// Check for timeout
		if ctx.Err() == context.DeadlineExceeded {
			return runtime.ErrorOutput(NewTimeoutError(n.NodeId(), "script execution timed out", input.ItemIndex, cfg.Timeout))
		}
		return runtime.ErrorOutput(err)
	}

	// If output schema is configured, validate the result
	if cfg.HasOutputSchema() {
		validatedResult, err := n.validateOutput(result, &cfg, input.ItemIndex)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		return runtime.SuccessOutput(validatedResult)
	}

	// Return result as-is
	return runtime.SuccessOutput(result)
}

// executeScript executes the JavaScript code and returns the result
func (n *JSRunnerNode) executeScript(ctx context.Context, input runtime.ProcessInput, cfg *Config) (map[string]interface{}, error) {
	// Create a new VM
	vm := goja.New()

	// Create utility registry
	utilityRegistry := NewUtilityRegistry()

	// Apply security sandbox
	if err := CreateSecureContext(vm, cfg); err != nil {
		return nil, NewConfigError(n.NodeId(), "failed to create secure context", err)
	}

	// Register utilities
	if err := utilityRegistry.RegisterEnabled(vm, cfg); err != nil {
		return nil, NewConfigError(n.NodeId(), "failed to register utilities", err)
	}

	// The file helpers read and write the run's files (raw payloads).
	if err := registerFileHelpers(vm, input); err != nil {
		return nil, NewConfigError(n.NodeId(), "failed to register file helpers", err)
	}

	// Inject input data as 'input' global
	// Auto-unwrap: if input.Data is a map with single key "input" containing an array,
	// unwrap it to provide better UX for users writing JS scripts
	var inputData interface{} = input.Data
	if len(input.Data) == 1 {
		if nestedInput, hasInput := input.Data["input"]; hasInput {
			// Unwrap and pass the nested value directly
			inputData = nestedInput
		}
	}

	if err := vm.Set("input", inputData); err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to set input variable", input.ItemIndex, 0, 0, err)
	}

	// Inject input schema if provided (for reference in JS)
	if inputSchema := cfg.GetInputSchema(); inputSchema != nil {
		vm.Set("inputSchema", inputSchema)
	}

	// Inject output schema if provided (for reference in JS)
	if outputSchema := cfg.GetOutputSchema(); outputSchema != nil {
		vm.Set("outputSchema", outputSchema)
	}

	// Inject manual inputs if provided
	if len(cfg.ManualInputs) > 0 {
		vm.Set("manualInputs", cfg.ManualInputs)
	}

	// Wrap script for execution
	wrappedScript := wrapScript(cfg.Script)

	// Execute script with timeout
	resultChan := make(chan goja.Value, 1)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				if gojaErr, ok := r.(*goja.Exception); ok {
					errChan <- NewExecutionError(n.NodeId(), gojaErr.Error(), input.ItemIndex, 0, 0, gojaErr)
				} else {
					errChan <- NewExecutionError(n.NodeId(), fmt.Sprintf("%v", r), input.ItemIndex, 0, 0, nil)
				}
			}
		}()

		prog, err := n.program(wrappedScript)
		if err != nil {
			errChan <- NewExecutionError(n.NodeId(), err.Error(), input.ItemIndex, 0, 0, err)
			return
		}
		val, err := vm.RunProgram(prog)
		if err == nil {
			// Timer callbacks run here, on the VM's goroutine, after the synchronous script.
			utilityRegistry.RunDueTimers()
		}
		if err != nil {
			if gojaErr, ok := err.(*goja.Exception); ok {
				errChan <- NewExecutionError(n.NodeId(), gojaErr.Error(), input.ItemIndex, 0, 0, gojaErr)
			} else {
				errChan <- NewExecutionError(n.NodeId(), err.Error(), input.ItemIndex, 0, 0, err)
			}
			return
		}
		resultChan <- val
	}()

	// Wait for completion or timeout
	var val goja.Value
	select {
	case val = <-resultChan:
		// Success
	case execErr := <-errChan:
		return nil, execErr
	case <-ctx.Done():
		// Stop the script too: without this its goroutine keeps running after the node has
		// failed, and a runaway loop holds a CPU core until the pod restarts.
		vm.Interrupt("script execution timed out")
		return nil, NewTimeoutError(n.NodeId(), "script execution timed out", input.ItemIndex, cfg.Timeout)
	}

	// Cleanup utilities
	if err := utilityRegistry.CleanupEnabled(vm, cfg); err != nil {
		// Non-fatal, just log
	}

	// Export result
	if val == nil || val == goja.Undefined() || val == goja.Null() {
		return map[string]interface{}{}, nil
	}

	exported := val.Export()

	// Convert to map
	resultMap, ok := exported.(map[string]interface{})
	if !ok {
		// If result is not a map, wrap it
		resultMap = map[string]interface{}{
			"result": exported,
		}
	}

	return resultMap, nil
}

// validateInput validates the input data against the configured input schema
func (n *JSRunnerNode) validateInput(input runtime.ProcessInput, cfg *Config) (map[string]interface{}, error) {
	// Get input schema (enriched by Elysium)
	inputSchema := cfg.GetInputSchema()
	if inputSchema == nil {
		return nil, NewConfigError(
			n.NodeId(),
			fmt.Sprintf("input_schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.InputSchemaID),
			nil,
		)
	}

	// Detect if input schema expects ARRAY at root and handle wrapped arrays
	dataToValidate := input.Data
	shouldRewrap := false
	originalSchema := inputSchema

	// Parse input schema to check type
	var inputSchemaObj schema.Schema
	if schemaBytes, err := json.Marshal(inputSchema); err == nil {
		if err := json.Unmarshal(schemaBytes, &inputSchemaObj); err == nil {
			// Check if schema expects array at root and input has wrapped array
			if inputSchemaObj.Type == schema.TypeArray {
				if arrayVal, ok := input.Data["input"]; ok {
					// Wrap the array in a temporary structure for validation
					dataToValidate = map[string]interface{}{"root": arrayVal}
					shouldRewrap = true
					// Temporarily modify schema to expect {"root": array}
					inputSchema = map[string]interface{}{
						"type": "OBJECT",
						"properties": map[string]interface{}{
							"root": originalSchema,
						},
					}
				}
			}
		}
	}

	// Marshal input data to JSON
	inputJSON, err := json.Marshal(dataToValidate)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal input data", input.ItemIndex, 0, 0, err)
	}

	// Marshal schema to JSON for ProcessWithSchema
	schemaJSON, err := json.Marshal(inputSchema)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal input schema", input.ItemIndex, 0, 0, err)
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema (structure and validate)
	schemaResult, err := engine.ProcessWithSchema(
		inputJSON,
		schemaJSON,
		schema.ProcessOptions{
			ApplyDefaults: cfg.ApplySchemaDefaults,
			StructureData: cfg.StructureData,
		},
	)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "input schema processing failed", input.ItemIndex, 0, 0, err)
	}
	if !schemaResult.Valid && cfg.GetStrictValidation() {
		errorMessages := make([]string, len(schemaResult.Errors))
		for i, e := range schemaResult.Errors {
			errorMessages[i] = fmt.Sprintf("%s: %s", e.Path, e.Message)
		}
		return nil, NewExecutionError(
			n.NodeId(),
			fmt.Sprintf("input validation failed: %v", errorMessages),
			input.ItemIndex, 0, 0, nil,
		)
	}

	// Unmarshal validated data
	var validatedMap map[string]interface{}
	if err := json.Unmarshal(schemaResult.Data, &validatedMap); err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to unmarshal validated input data", input.ItemIndex, 0, 0, err)
	}

	// If we wrapped the array for validation, unwrap it back
	if shouldRewrap {
		if rootVal, ok := validatedMap["root"]; ok {
			return map[string]interface{}{"input": rootVal}, nil
		}
	}

	return validatedMap, nil
}

// validateOutput validates the script result against the configured schema
func (n *JSRunnerNode) validateOutput(result map[string]interface{}, cfg *Config, itemIndex int) (map[string]interface{}, error) {
	// Get output schema (enriched by Elysium)
	outputSchema := cfg.GetOutputSchema()
	if outputSchema == nil {
		return nil, NewConfigError(
			n.NodeId(),
			fmt.Sprintf("outputSchemaID '%s' was not enriched - ensure Elysium enrichment is configured", cfg.OutputSchemaID),
			nil,
		)
	}

	// Marshal result to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal result", itemIndex, 0, 0, err)
	}

	// Marshal schema to JSON for ProcessWithSchema
	schemaJSON, err := json.Marshal(outputSchema)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal output schema", itemIndex, 0, 0, err)
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema (structure and validate)
	schemaResult, err := engine.ProcessWithSchema(
		resultJSON,
		schemaJSON,
		schema.ProcessOptions{
			ApplyDefaults: cfg.ApplySchemaDefaults,
			StructureData: cfg.StructureData,
		},
	)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "output schema processing failed", itemIndex, 0, 0, err)
	}
	if !schemaResult.Valid && cfg.GetStrictValidation() {
		errorMessages := make([]string, len(schemaResult.Errors))
		for i, e := range schemaResult.Errors {
			errorMessages[i] = fmt.Sprintf("%s: %s", e.Path, e.Message)
		}
		return nil, NewExecutionError(
			n.NodeId(),
			fmt.Sprintf("output validation failed: %v", errorMessages),
			itemIndex, 0, 0, nil,
		)
	}

	// Unmarshal validated data
	var validatedMap map[string]interface{}
	if err := json.Unmarshal(schemaResult.Data, &validatedMap); err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to unmarshal validated data", itemIndex, 0, 0, err)
	}

	return validatedMap, nil
}

// wrapScript wraps the user script in an IIFE for safe execution
func wrapScript(script string) string {
	// Trim whitespace
	script = strings.TrimSpace(script)

	// Check if script has explicit return statement
	hasReturn := strings.Contains(script, "return")
	if hasReturn {
		// Script has explicit return - wrap as-is
		return fmt.Sprintf("(function() {\n%s\n})()", script)
	}

	// Check if script starts with object literal expression syntax
	// e.g., ({ key: value }) or ({...})
	if strings.HasPrefix(script, "({") && strings.HasSuffix(script, "})") {
		// Object literal expression - auto-return regardless of newlines
		return fmt.Sprintf("(function() { return %s; })()", script)
	}

	// Check if it's a single expression (array literal, primitive, etc.)
	// Single expressions typically don't have semicolons or have only trailing semicolons
	trimmedScript := strings.TrimSuffix(script, ";")
	hasSemicolon := strings.Contains(trimmedScript, ";")
	hasNewline := strings.Contains(script, "\n")

	if hasSemicolon || hasNewline {
		// Multi-statement script - wrap as-is
		return fmt.Sprintf("(function() {\n%s\n})()", script)
	}

	// Single expression - auto-return. A one-line statement such as `throw new Error("x");`
	// or `while (true) {}` does not parse after `return`, so it runs as written instead.
	asExpression := fmt.Sprintf("(function() { return %s; })()", script)
	if _, err := goja.Parse("", asExpression); err == nil {
		return asExpression
	}
	return fmt.Sprintf("(function() {\n%s\n})()", script)
}
