package schema

import (
	"crypto/sha256"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
	"github.com/wehubfusion/Icarus/pkg/schema/csv"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7"
	schemajson "github.com/wehubfusion/Icarus/pkg/schema/json"
)

// Engine orchestrates schema-based data processing
type Engine struct {
	parser      *schemajson.Parser
	validator   *schemajson.Validator
	transformer *Transformer

	registry *ProcessorRegistry

	// compiledJSON caches parsed JSON schemas by the SHA-256 of their definition (raw payloads
	// phase 16): an embedded JSON Parser or Producer used to parse its schema for every item. A
	// parsed JSON schema is only read while processing (defaults are copied out), so one is shared
	// safely. Bounded; it starts again empty when full.
	compiledMu   sync.RWMutex
	compiledJSON map[[32]byte]contracts.CompiledSchema
}

const maxCompiledSchemas = 256

var defaultEngine = NewEngine()

// Shared returns a process-wide engine, so callers that process many items reuse its schema cache.
func Shared() *Engine { return defaultEngine }

// NewEngine creates a new schema engine with JSON, CSV, and HL7 processors registered.
func NewEngine() *Engine {
	parser := schemajson.NewParser()
	validator := schemajson.NewValidator()
	jsonTransformer := schemajson.NewTransformer()
	e := &Engine{
		parser:      parser,
		validator:   validator,
		transformer: &Transformer{j: jsonTransformer},
		registry:    NewProcessorRegistry(),
	}
	e.registry.Register(schemajson.NewJSONSchemaProcessor(parser, validator, jsonTransformer))
	e.registry.Register(csv.NewCSVSchemaProcessor(csv.NewParser()))
	e.registry.Register(hl7.NewHL7SchemaProcessor())
	return e
}

// NewHL7SchemaProcessor returns a new HL7 schema processor (re-exported for backward compatibility).
var NewHL7SchemaProcessor = hl7.NewHL7SchemaProcessor

// ParseJSONSchema parses a JSON schema definition and returns the compiled schema.
// Use this when you need to inspect schema properties (e.g. root type) before processing.
func (e *Engine) ParseJSONSchema(schemaDefinition []byte) (*Schema, error) {
	return e.parser.Parse(schemaDefinition)
}

// Process is the unified entry point for schema-based processing.
func (e *Engine) Process(
	inputData []byte,
	schemaDef []byte,
	format SchemaFormat,
	opts ProcessOptions,
) (*ProcessResult, error) {
	if format == "" {
		format = FormatJSON
	}
	proc, err := e.registry.Get(string(format))
	if err != nil {
		return nil, err
	}

	if format != FormatJSON {
		compiled, err := proc.ParseSchema(schemaDef)
		if err != nil {
			return nil, err
		}
		return proc.Process(inputData, compiled, opts)
	}

	key := sha256.Sum256(schemaDef)
	e.compiledMu.RLock()
	compiled, ok := e.compiledJSON[key]
	e.compiledMu.RUnlock()
	if !ok {
		parsed, err := proc.ParseSchema(schemaDef)
		if err != nil {
			return nil, err
		}
		compiled = parsed
		e.compiledMu.Lock()
		if e.compiledJSON == nil || len(e.compiledJSON) >= maxCompiledSchemas {
			e.compiledJSON = make(map[[32]byte]contracts.CompiledSchema)
		}
		e.compiledJSON[key] = compiled
		e.compiledMu.Unlock()
	}
	return proc.Process(inputData, compiled, opts)
}

// ProcessWithSchema is the main entry point for schema-based data processing
func (e *Engine) ProcessWithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	return e.Process(inputData, schemaDefinition, FormatJSON, options)
}

// ProcessCSVWithSchema processes a JSON array of row objects against a CSV schema
func (e *Engine) ProcessCSVWithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	return e.Process(inputData, schemaDefinition, FormatCSV, options)
}

// ProcessHL7WithSchema validates raw HL7 v2.x message bytes against an HL7 schema definition.
func (e *Engine) ProcessHL7WithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	return e.Process(inputData, schemaDefinition, FormatHL7, options)
}

// ValidateHL7Only validates raw HL7 v2.x message bytes against an HL7 schema definition.
func (e *Engine) ValidateHL7Only(inputData []byte, schemaDefinition []byte) (*ValidationResult, error) {
	result, err := e.Process(inputData, schemaDefinition, FormatHL7, ProcessOptions{CollectAllErrors: true})
	if err != nil {
		return nil, err
	}
	return &ValidationResult{
		Valid:    result.Valid,
		Errors:   result.Errors,
		Warnings: result.Warnings,
		Infos:    result.Infos,
	}, nil
}

// ValidateCSVOnly validates rows against a CSV schema without transformation (delegates through Process).
func (e *Engine) ValidateCSVOnly(inputData []byte, schemaDefinition []byte) (*ValidationResult, error) {
	result, err := e.Process(inputData, schemaDefinition, FormatCSV, ProcessOptions{CollectAllErrors: true})
	if err != nil {
		return nil, err
	}
	return &ValidationResult{Valid: result.Valid, Errors: result.Errors}, nil
}

// ValidateOnly validates data against schema without transformation (delegates through Process).
func (e *Engine) ValidateOnly(inputData []byte, schemaDefinition []byte) (*ValidationResult, error) {
	result, err := e.Process(inputData, schemaDefinition, FormatJSON, ProcessOptions{CollectAllErrors: true})
	if err != nil {
		return nil, err
	}
	return &ValidationResult{Valid: result.Valid, Errors: result.Errors}, nil
}

// TransformOnly applies defaults and structuring without validation.
func (e *Engine) TransformOnly(inputData []byte, schemaDefinition []byte) ([]byte, error) {
	result, err := e.Process(inputData, schemaDefinition, FormatJSON, ProcessOptions{
		ApplyDefaults: true,
		StructureData: true,
	})
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}
