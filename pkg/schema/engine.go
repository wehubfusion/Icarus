package schema

import (
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
}

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

	compiled, err := proc.ParseSchema(schemaDef)
	if err != nil {
		return nil, err
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
	return &ValidationResult{Valid: result.Valid, Errors: result.Errors}, nil
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
