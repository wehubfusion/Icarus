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
}

// NewEngine creates a new schema engine
func NewEngine() *Engine {
	return &Engine{
		parser:      NewParser(),
		validator:   NewValidator(),
		transformer: NewTransformer(),
	}
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

// ProcessWithSchema is the main entry point for schema-based data processing
// It coordinates parsing, transformation, and validation of data against a schema
func (e *Engine) ProcessWithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	// Step 1: Parse schema definition
	schema, err := e.parser.Parse(schemaDefinition)
	if err != nil {
		return nil, fmt.Errorf("schema parse error: %w", err)
	}

	// Step 2: Parse input data
	var data interface{}
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}

	// Step 3: Apply defaults (if enabled)
	if options.ApplyDefaults {
		data, err = e.transformer.ApplyDefaults(data, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to apply defaults: %w", err)
		}
	}

	// Step 4: Structure data (if enabled)
	if options.StructureData {
		data, err = e.transformer.StructureData(data, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to structure data: %w", err)
		}
	}

	// Step 5: Validate data against schema
	validationResult := e.validator.Validate(data, schema)

	// Step 6: Check if validation failed in strict mode
	if !validationResult.Valid && options.StrictValidation {
		// Marshal data anyway for partial results
		outputData, _ := json.Marshal(data)
		return &ProcessResult{
			Valid:  false,
			Data:   outputData,
			Errors: validationResult.Errors,
		}, fmt.Errorf("validation failed with %d errors", len(validationResult.Errors))
	}

	// Step 7: Marshal output data
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

// ProcessCSVWithSchema processes a JSON array of row objects against a CSV schema
// Expected inputData: JSON array of objects, one object per CSV row
func (e *Engine) ProcessCSVWithSchema(
	inputData []byte,
	schemaDefinition []byte,
	options ProcessOptions,
) (*ProcessResult, error) {
	// Step 1: Parse CSV schema definition
	csvSchema, err := e.parser.ParseCSV(schemaDefinition)
	if err != nil {
		return nil, fmt.Errorf("csv schema parse error: %w", err)
	}

	// Step 2: Parse input data (array of objects)
	var rows []map[string]interface{}
	if err := json.Unmarshal(inputData, &rows); err != nil {
		return nil, fmt.Errorf("invalid input JSON (expected array of objects): %w", err)
	}

	// Step 3: Apply defaults (if enabled)
	if options.ApplyDefaults {
		rows, err = e.transformer.ApplyCSVDefaults(rows, csvSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to apply csv defaults: %w", err)
		}
	}

	// Step 4: Structure data (if enabled) to drop unknown fields
	if options.StructureData {
		rows, err = e.transformer.StructureCSVRows(rows, csvSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to structure csv rows: %w", err)
		}
	}

	// Step 5: Validate rows
	validationResult := e.validator.ValidateCSVRows(rows, csvSchema)

	// Step 6: Strict mode handling
	if !validationResult.Valid && options.StrictValidation {
		outputData, _ := json.Marshal(rows)
		return &ProcessResult{
			Valid:  false,
			Data:   outputData,
			Errors: validationResult.Errors,
		}, fmt.Errorf("csv validation failed with %d errors", len(validationResult.Errors))
	}

	// Step 7: Marshal output
	outputData, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return &ProcessResult{
		Valid:  validationResult.Valid,
		Data:   outputData,
		Errors: validationResult.Errors,
	}, nil
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
