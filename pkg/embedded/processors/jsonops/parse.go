package jsonops

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// maxParseFileBytes caps a JSON document read from a file: the schema engine validates it whole
// (ValidateStream streams it in a later step).
const maxParseFileBytes = 64 << 20

// executeParse validates and transforms incoming JSON data against a schema
// Input: ProcessInput.Data["data"] - can be base64 string or raw JSON
// Output: Flattened schema fields (e.g., {"name": "Alice", "age": 30})
func (n *JsonOpsNode) executeParse(input runtime.ProcessInput, cfg *Config) runtime.ProcessOutput {
	// Extract "data" field from ProcessInput.Data
	dataField, hasData := input.Data["data"]
	if !hasData {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"input must contain a 'data' field",
			input.ItemIndex,
			nil,
		))
	}

	// Convert data to []byte for processing
	var dataToValidate []byte
	var err error

	// Raw payloads: a file a BYTE mapping delivered to "data" is the JSON document itself.
	fileData, isFile, fileErr := input.ReadTrustedFile("data", maxParseFileBytes)
	if fileErr != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "failed to read data file", input.ItemIndex, fileErr))
	}

	if isFile {
		dataToValidate = fileData
	} else {
		switch v := dataField.(type) {
		case string:
			// Data is base64-encoded string - decode it
			dataToValidate, err = base64.StdEncoding.DecodeString(v)
			if err != nil {
				return runtime.ErrorOutput(NewProcessingError(
					n.NodeId(),
					"parse",
					"failed to decode base64 data",
					input.ItemIndex,
					err,
				))
			}
		case []byte:
			// Data is already bytes
			dataToValidate = v
		default:
			// Data is JSON object/array - marshal it
			dataToValidate, err = json.Marshal(v)
			if err != nil {
				return runtime.ErrorOutput(NewProcessingError(
					n.NodeId(),
					"parse",
					"failed to marshal data field",
					input.ItemIndex,
					err,
				))
			}
		}
	}

	// Validate that schema is provided (enriched by Elysium)
	if len(cfg.Schema) == 0 {
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.SchemaID),
			nil,
		))
	}

	// Create schema engine
	engine := schema.Shared()

	// Process with schema
	result, err := engine.ProcessWithSchema(
		dataToValidate,
		cfg.Schema,
		schema.ProcessOptions{
			ApplyDefaults: cfg.GetApplyDefaults(),
			StructureData: cfg.GetStructureData(),
		},
	)
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"schema processing failed",
			input.ItemIndex,
			err,
		))
	}

	if !result.Valid && cfg.GetStrictValidation() {
		errorMessages := make([]string, len(result.Errors))
		for i, e := range result.Errors {
			errorMessages[i] = fmt.Sprintf("%s: %s", e.Path, e.Message)
		}
		return runtime.ErrorOutput(NewValidationError(
			n.NodeId(),
			"parse",
			"validation failed",
			input.ItemIndex,
			errorMessages,
		))
	}

	// Unmarshal validated data to map for output (for OBJECT schemas)
	var validatedMap map[string]interface{}
	if err := json.Unmarshal(result.Data, &validatedMap); err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"failed to unmarshal validated data",
			input.ItemIndex,
			err,
		))
	}

	// Return flattened schema fields directly (no "data" wrapper)
	// The runtime will flatten this with node-specific keys
	return runtime.SuccessOutput(validatedMap)
}
