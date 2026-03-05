package schema

import (
	"encoding/json"
	"fmt"
)

// Engine orchestrates schema-based data processing
type Engine struct {
	parser      *Parser
	validator   *Validator
	transformer *Transformer

	registry *ProcessorRegistry
}

// NewEngine creates a new schema engine with JSON and CSV processors registered.
func NewEngine() *Engine {
	e := &Engine{
		parser:      NewParser(),
		validator:   NewValidator(),
		transformer: NewTransformer(),
		registry:    NewProcessorRegistry(),
	}
	e.registry.Register(NewJSONSchemaProcessor(e.parser, e.validator, e.transformer))
	e.registry.Register(NewCSVSchemaProcessor(e.parser, e.validator, e.transformer))
	return e
}

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
	schemaID string,
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
// It coordinates parsing, transformation, and validation of data against a schema
func (e *Engine) ProcessWithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	return e.Process(inputData, "", schemaDefinition, FormatJSON, options)
}

// ProcessCSVWithSchema processes a JSON array of row objects against a CSV schema
// Expected inputData: JSON array of objects, one object per CSV row
func (e *Engine) ProcessCSVWithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	return e.Process(inputData, "", schemaDefinition, FormatCSV, options)
}

// ValidateCSVOnly validates rows against a CSV schema without transformation
func (e *Engine) ValidateCSVOnly(inputData []byte, schemaDefinition []byte) (*ValidationResult, error) {
	csvSchema, err := e.parser.ParseCSV(schemaDefinition)
	if err != nil {
		return nil, fmt.Errorf("csv schema parse error: %w", err)
	}

	var rows []map[string]interface{}
	if err := json.Unmarshal(inputData, &rows); err != nil {
		return nil, fmt.Errorf("invalid input JSON (expected array of objects): %w", err)
	}

	return e.validator.ValidateCSVRows(rows, csvSchema), nil
}

// ValidateOnly validates data against schema without transformation
func (e *Engine) ValidateOnly(inputData []byte, schemaDefinition []byte) (*ValidationResult, error) {
	// Parse schema
	schema, err := e.parser.Parse(schemaDefinition)
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}

	// Parse data
	var data interface{}
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}

	// Validate only
	return e.validator.Validate(data, schema), nil
}

// TransformOnly applies defaults and structuring without validation
func (e *Engine) TransformOnly(inputData []byte, schemaDefinition []byte) ([]byte, error) {
	// Parse schema
	schema, err := e.parser.Parse(schemaDefinition)
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}

	// Parse data
	var data interface{}
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}

	// Apply defaults
	data, err = e.transformer.ApplyDefaults(data, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to apply defaults: %w", err)
	}

	// Structure data
	data, err = e.transformer.StructureData(data, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to structure data: %w", err)
	}

	// Marshal output
	return json.Marshal(data)
}
