package schema

import (
	"encoding/json"
	"fmt"
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
	return string(FormatJSON)
}

// ParseSchema parses and validates the schema definition, returning a compiled schema.
func (p *JSONSchemaProcessor) ParseSchema(definition []byte) (CompiledSchema, error) {
	schema, err := p.parser.Parse(definition)
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}
	return &compiledJSONSchema{schema: schema}, nil
}

// Process runs the full JSON pipeline: parse input, apply defaults, structure, validate, marshal output.
func (p *JSONSchemaProcessor) Process(inputData []byte, cs CompiledSchema, options ProcessOptions) (*ProcessResult, error) {
	compiled, ok := cs.(*compiledJSONSchema)
	if !ok {
		return nil, fmt.Errorf("expected compiledJSONSchema, got %T", cs)
	}
	schema := compiled.schema

	// Step 1: Parse input data
	var data interface{}
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}

	// Step 2: Apply defaults (if enabled)
	if options.ApplyDefaults {
		var err error
		data, err = p.transformer.ApplyDefaults(data, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to apply defaults: %w", err)
		}
	}

	// Step 3: Structure data (if enabled)
	if options.StructureData {
		var err error
		data, err = p.transformer.StructureData(data, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to structure data: %w", err)
		}
	}

	// Step 4: Validate data against schema
	validationResult := p.validator.ValidateWithOptions(data, schema, options.CollectAllErrors)

	// Step 5: Check if validation failed in strict mode
	if !validationResult.Valid && options.StrictValidation {
		outputData, _ := json.Marshal(data)
		return &ProcessResult{
			Valid:  false,
			Data:   outputData,
			Errors: validationResult.Errors,
		}, fmt.Errorf("%s", validationResult.ErrorMessage())
	}

	// Step 6: Marshal output data
	outputData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return &ProcessResult{
		Valid:  validationResult.Valid,
		Data:   outputData,
		Errors: validationResult.Errors,
	}, nil
}

// compiledJSONSchema holds a parsed JSON schema.
type compiledJSONSchema struct {
	schema *Schema
}

func (c *compiledJSONSchema) SchemaType() string { return string(FormatJSON) }
