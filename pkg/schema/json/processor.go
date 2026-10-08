package json

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// JSONSchemaProcessor processes JSON payloads against a JSON schema definition.
type JSONSchemaProcessor struct {
	parser      *Parser
	validator   *Validator
	transformer *Transformer
}

// NewJSONSchemaProcessor creates a processor that uses the given parser, validator, and transformer.
func NewJSONSchemaProcessor(p *Parser, v *Validator, t *Transformer) *JSONSchemaProcessor {
	return &JSONSchemaProcessor{parser: p, validator: v, transformer: t}
}

// Type returns the format identifier for JSON.
func (p *JSONSchemaProcessor) Type() string {
	return string(contracts.FormatJSON)
}

// ParseSchema parses and validates the schema definition, returning a compiled schema.
func (p *JSONSchemaProcessor) ParseSchema(definition []byte) (contracts.CompiledSchema, error) {
	s, err := p.parser.Parse(definition)
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}
	return &compiledJSONSchema{schema: s}, nil
}

// Process runs the full JSON pipeline: parse input, apply defaults, structure, validate, marshal output.
func (p *JSONSchemaProcessor) Process(inputData []byte, cs contracts.CompiledSchema, options contracts.ProcessOptions) (*contracts.ProcessResult, error) {
	compiled, ok := cs.(*compiledJSONSchema)
	if !ok {
		return nil, fmt.Errorf("expected compiledJSONSchema, got %T", cs)
	}
	s := compiled.schema

	var data interface{}
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}

	if options.ApplyDefaults {
		var err error
		data, err = p.transformer.ApplyDefaultsWith(data, s, options.ByteDefault)
		if err != nil {
			return nil, fmt.Errorf("failed to apply defaults: %w", err)
		}
	}

	if options.StructureData {
		var err error
		data, err = p.transformer.StructureData(data, s)
		if err != nil {
			return nil, fmt.Errorf("failed to structure data: %w", err)
		}
	}

	validationResult := p.validator.ValidateWithOptions(data, s, options.CollectAllErrors)

	outputData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	errs, warns, infos := contracts.ApplyAndBucket(validationResult.Errors, options.CodeSeverityOverrides)
	result := &contracts.ProcessResult{
		Valid:     len(errs) == 0,
		Data:      outputData,
		Errors:    errs,
		Warnings:  warns,
		Infos:     infos,
	}
	return result, nil
}

// compiledJSONSchema holds a parsed JSON schema.
type compiledJSONSchema struct {
	schema *Schema
}

func (c *compiledJSONSchema) SchemaType() string { return string(contracts.FormatJSON) }
