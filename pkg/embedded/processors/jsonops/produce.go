package jsonops

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/records"
	"github.com/wehubfusion/Icarus/pkg/schema"
	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// executeProduce validates, structures, and encodes JSON data
// Input: ProcessInput.Data["data"] - the JSON value, or a file holding a JSON array or its records
// Output: {"encoded": FileRef}; the run's files must be configured
func (n *JsonOpsNode) executeProduce(input runtime.ProcessInput, cfg *Config) runtime.ProcessOutput {
	// Validate that schema is provided (enriched by Elysium)
	if len(cfg.Schema) == 0 {
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.SchemaID),
			nil,
		))
	}

	// Parse schema to check root type
	engine := schema.Shared()
	parsedSchema, parseErr := engine.ParseJSONSchema(cfg.Schema)

	// Determine what data to process based on schema root type
	var dataToMarshal interface{} = input.Data

	// Only extract $items or single-key arrays if schema expects ARRAY at root
	schemaExpectsArray := parseErr == nil && parsedSchema != nil && parsedSchema.Type == schema.TypeArray

	if schemaExpectsArray && len(input.Data) == 1 {
		// Also check for single key containing an array (generic case)
		// Only extract if schema expects ARRAY at root
		for _, v := range input.Data {
			if arr, isArr := v.([]interface{}); isArr {
				dataToMarshal = arr
				break
			}
		}
	}

	// /encoded is a JSON file written as it is produced.
	if !input.WritesFile("encoded") {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "the output is written as a file but file storage is not configured", nil))
	}
	if arr, isArr := dataToMarshal.([]interface{}); isArr && schemaExpectsArray {
		return n.produceArrayFile(input, cfg, parsedSchema, sliceSource(arr))
	}
	// The array is a file: a JSON array or an .ndjson records file (such as ESR Query's output),
	// read item by item, so neither it nor the output is held whole.
	if schemaExpectsArray && len(input.Data) == 1 {
		for field := range input.Data {
			if ref, ok := input.TrustedFile(field); ok {
				return n.produceFromFile(input, cfg, parsedSchema, field, ref)
			}
		}
	}

	// Convert data to []byte for processing (root can be object or array)
	var dataToProcess []byte
	var err error

	dataToProcess, err = json.Marshal(dataToMarshal)
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"produce",
			"failed to marshal input data",
			input.ItemIndex,
			err,
		))
	}

	// Process with schema: apply defaults per config, structure and validate
	byteDefaults := newByteDefaults(input)
	result, err := engine.ProcessWithSchema(
		dataToProcess,
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
			"produce",
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
			"produce",
			"validation failed",
			input.ItemIndex,
			errorMessages,
		))
	}

	// Get processed JSON
	processedJSON := result.Data

	// Pretty print if requested
	if cfg.Pretty {
		var prettyData interface{}
		if err := json.Unmarshal(processedJSON, &prettyData); err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
				"failed to parse for pretty printing",
				input.ItemIndex,
				err,
			))
		}
		processedJSON, err = json.MarshalIndent(prettyData, "", "  ")
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
				"failed to pretty print",
				input.ItemIndex,
				err,
			))
		}
	}

	ref, err := input.WriteOutputFile("encoded", fileref.ContentTypeJSON, bytes.NewReader(processedJSON))
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "produce", "failed to write the output file", input.ItemIndex, err))
	}
	return runtime.SuccessOutput(map[string]interface{}{"encoded": ref})
}

// produceArrayFile writes a root-array document to the /encoded file item by item, as the
// whole-document path would write it (compact, or indented two spaces with pretty), without
// building the document in memory.
func (n *JsonOpsNode) produceArrayFile(input runtime.ProcessInput, cfg *Config, s *schema.Schema, src itemSource) runtime.ProcessOutput {
	pr, pw := io.Pipe()
	var validationErrs []contracts.ValidationError
	byteDefaults := newByteDefaults(input)
	go func() {
		bw := bufio.NewWriter(pw)
		count := 0
		errs, err := streamItems(src, s, cfg, byteDefaults.hook(), func(item interface{}) error {
			var b []byte
			var err error
			sep, lead := ",", ""
			if cfg.Pretty {
				b, err = json.MarshalIndent(item, "  ", "  ")
				sep, lead = ",\n  ", "[\n  "
			} else {
				b, err = json.Marshal(item)
				lead = "["
			}
			if err != nil {
				return err
			}
			if count == 0 {
				sep = lead
			}
			count++
			if _, err := bw.WriteString(sep); err != nil {
				return err
			}
			_, err = bw.Write(b)
			return err
		})
		if err == nil {
			err = byteDefaults.failure()
		}
		if err == nil && len(errs) > 0 {
			validationErrs = errs
			err = errValidationFailed
		}
		if err == nil {
			switch {
			case count == 0:
				_, err = bw.WriteString("[]")
			case cfg.Pretty:
				_, err = bw.WriteString("\n]")
			default:
				err = bw.WriteByte(']')
			}
		}
		if err == nil {
			err = bw.Flush()
		}
		pw.CloseWithError(err)
	}()

	ref, err := input.WriteOutputFile("encoded", fileref.ContentTypeJSON, pr)
	pr.Close()
	if errors.Is(err, errValidationFailed) {
		return n.validationFailed(input, "produce", validationErrs)
	}
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "produce", "failed to write the output file", input.ItemIndex, err))
	}
	return runtime.SuccessOutput(map[string]interface{}{"encoded": ref})
}

// produceFromFile produces the array held in the file at an input field. An .ndjson file is the
// records of the array, one item per line; any other file is a JSON array.
func (n *JsonOpsNode) produceFromFile(input runtime.ProcessInput, cfg *Config, s *schema.Schema, field string, ref fileref.FileRef) runtime.ProcessOutput {
	rc, _, _, err := input.OpenTrustedFile(field)
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "produce", "failed to open the input file", input.ItemIndex, err))
	}
	defer rc.Close()
	var src itemSource
	if ref.IsRecords() {
		src = recordsSource(records.NewReader(rc))
	} else {
		var serr error
		src, serr = jsonArraySource(json.NewDecoder(rc))
		if serr != nil {
			return runtime.ErrorOutput(NewProcessingError(n.NodeId(), "produce", "the input file is not a JSON array", input.ItemIndex, serr))
		}
	}
	return n.produceArrayFile(input, cfg, s, src)
}

var errValidationFailed = errors.New("validation failed")
