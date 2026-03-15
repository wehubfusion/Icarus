package schema

import (
	"encoding/json"
	"fmt"

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

// GetTransformer returns the transformer instance (for advanced use cases)
func (e *Engine) GetTransformer() *Transformer {
	return e.transformer
}

// GetValidator returns the validator instance (for advanced use cases)
func (e *Engine) GetValidator() *Validator {
	return e.validator
}

// GetParser returns the parser instance (for advanced use cases)
func (e *Engine) GetParser() *Parser {
	return e.parser
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

// TransformOnly applies defaults and structuring without validation
func (e *Engine) TransformOnly(inputData []byte, schemaDefinition []byte) ([]byte, error) {
	schema, err := e.parser.Parse(schemaDefinition)
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}

	var data interface{}
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}

	data, err = e.transformer.ApplyDefaults(data, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to apply defaults: %w", err)
	}

	data, err = e.transformer.StructureData(data, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to structure data: %w", err)
	}

	return json.Marshal(data)
}
