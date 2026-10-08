package jsonops

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/records"
	"github.com/wehubfusion/Icarus/pkg/schema"
	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// maxParseFileBytes caps a JSON object document read from a file: the schema engine processes an
// object whole. A root array is streamed and has no cap.
const maxParseFileBytes = 64 << 20

// executeParse validates and transforms incoming JSON data against a schema
// Input: ProcessInput.Data["data"] - a file reference (byte port) or a JSON value
// Output: Flattened schema fields (e.g., {"name": "Alice", "age": 30}); for a schema whose root is
// an ARRAY, the items under runtime.RootArrayKey.
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

	// Validate that schema is provided (enriched by Elysium)
	if len(cfg.Schema) == 0 {
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.SchemaID),
			nil,
		))
	}

	engine := schema.Shared()
	if parsed, err := engine.ParseJSONSchema(cfg.Schema); err == nil && parsed.Type == schema.TypeArray {
		rc, ref, isFile, err := input.OpenTrustedFile("data")
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "failed to open data file", input.ItemIndex, err))
		}
		if isFile && ref.IsRecords() {
			// An .ndjson file is the records of the array, one item per line (raw payloads D7): each
			// line is validated as one item, as an item of a JSON array is.
			defer rc.Close()
			return n.parseRootArrayFrom(input, cfg, parsed, recordsSource(records.NewReader(rc)))
		}
		if !isFile {
			data, errOut := n.parseInputBytes(input, dataField)
			if errOut != nil {
				return *errOut
			}
			rc = io.NopCloser(bytes.NewReader(data))
		}
		defer rc.Close()
		return n.parseRootArray(input, cfg, parsed, rc)
	}

	// A root that is an object cannot be read from records: an .ndjson file is a list of items.
	if ref, ok := input.TrustedFile("data"); ok && ref.IsRecords() {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse",
			"the input is an .ndjson records file, which needs a schema whose root is ARRAY", input.ItemIndex, nil))
	}

	// Raw payloads: a file a BYTE mapping delivered to "data" is the JSON document itself.
	dataToValidate, isFile, fileErr := input.ReadTrustedFile("data", maxParseFileBytes)
	if fileErr != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "failed to read data file", input.ItemIndex, fileErr))
	}
	if !isFile {
		data, errOut := n.parseInputBytes(input, dataField)
		if errOut != nil {
			return *errOut
		}
		dataToValidate = data
	}

	// Process with schema
	byteDefaults := newByteDefaults(input)
	result, err := engine.ProcessWithSchema(
		dataToValidate,
		cfg.Schema,
		schema.ProcessOptions{
			ApplyDefaults: cfg.GetApplyDefaults(),
			StructureData: cfg.GetStructureData(),
			ByteDefault:   byteDefaults.hook(),
		},
	)
	if err == nil {
		err = byteDefaults.failure()
	}
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
		return n.validationFailed(input, "parse", result.Errors)
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

// parseInputBytes is the JSON document of a "data" value that is not a file: a JSON value. Text is
// refused.
func (n *JsonOpsNode) parseInputBytes(input runtime.ProcessInput, dataField interface{}) ([]byte, *runtime.ProcessOutput) {
	switch v := dataField.(type) {
	case string:
		// A byte port carries a file reference; text is never an encoding of one.
		out := runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "data must be a file: map a byte port (a file reference) into data", input.ItemIndex, nil))
		return nil, &out
	case []byte:
		return v, nil
	default:
		// Data is JSON object/array - marshal it
		data, err := json.Marshal(v)
		if err != nil {
			out := runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "failed to marshal data field", input.ItemIndex, err))
			return nil, &out
		}
		return data, nil
	}
}

func (n *JsonOpsNode) validationFailed(input runtime.ProcessInput, op string, errs []contracts.ValidationError) runtime.ProcessOutput {
	errorMessages := make([]string, len(errs))
	for i, e := range errs {
		errorMessages[i] = fmt.Sprintf("%s: %s", e.Path, e.Message)
	}
	return runtime.ErrorOutput(NewValidationError(
		n.NodeId(),
		op,
		"validation failed",
		input.ItemIndex,
		errorMessages,
	))
}

// parseRootArray parses a document whose schema root is an ARRAY one item at a time: the document
// is never held whole, only the transformed items.
func (n *JsonOpsNode) parseRootArray(input runtime.ProcessInput, cfg *Config, s *schema.Schema, r io.Reader) runtime.ProcessOutput {
	src, err := jsonArraySource(json.NewDecoder(r))
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "schema processing failed", input.ItemIndex, err))
	}
	return n.parseRootArrayFrom(input, cfg, s, src)
}

// parseRootArrayFrom is parseRootArray over any source of the array's items.
func (n *JsonOpsNode) parseRootArrayFrom(input runtime.ProcessInput, cfg *Config, s *schema.Schema, src itemSource) runtime.ProcessOutput {
	items := []interface{}{}
	byteDefaults := newByteDefaults(input)
	errs, err := streamItems(src, s, cfg, byteDefaults.hook(), func(item interface{}) error {
		items = append(items, item)
		return nil
	})
	if err == nil {
		err = byteDefaults.failure()
	}
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "parse", "schema processing failed", input.ItemIndex, err))
	}
	if len(errs) > 0 {
		return n.validationFailed(input, "parse", errs)
	}
	return runtime.SuccessOutput(map[string]interface{}{runtime.RootArrayKey: items})
}
