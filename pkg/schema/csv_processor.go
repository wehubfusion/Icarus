package schema

import (
	"encoding/json"
	"fmt"
)

// CSVSchemaProcessor processes JSON arrays of row objects against a CSV schema definition.
type CSVSchemaProcessor struct {
	parser      *Parser
	validator   *Validator
	transformer *Transformer
}

// NewCSVSchemaProcessor creates a processor that uses the given parser, validator, and transformer.
func NewCSVSchemaProcessor(p *Parser, v *Validator, t *Transformer) *CSVSchemaProcessor {
	return &CSVSchemaProcessor{parser: p, validator: v, transformer: t}
}

// Type returns the format identifier for CSV.
func (p *CSVSchemaProcessor) Type() string {
	return string(FormatCSV)
}

// ParseSchema parses and validates the CSV schema definition, returning a compiled schema.
func (p *CSVSchemaProcessor) ParseSchema(definition []byte) (CompiledSchema, error) {
	csvSchema, err := p.parser.ParseCSV(definition)
	if err != nil {
		return nil, fmt.Errorf("csv schema parse error: %w", err)
	}
	return &compiledCSVSchema{schema: csvSchema, hash: contentHash(definition)}, nil
}

// Process runs the full CSV pipeline: parse input rows, apply defaults, structure, validate, marshal output.
func (p *CSVSchemaProcessor) Process(inputData []byte, cs CompiledSchema, options ProcessOptions) (*ProcessResult, error) {
	compiled, ok := cs.(*compiledCSVSchema)
	if !ok {
		return nil, fmt.Errorf("expected compiledCSVSchema, got %T", cs)
	}
	csvSchema := compiled.schema

	// Step 1: Parse input data (array of objects)
	var rows []map[string]interface{}
	if err := json.Unmarshal(inputData, &rows); err != nil {
		return nil, fmt.Errorf("invalid input JSON (expected array of objects): %w", err)
	}

	// Step 2: Apply defaults (if enabled)
	if options.ApplyDefaults {
		var err error
		rows, err = p.transformer.ApplyCSVDefaults(rows, csvSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to apply csv defaults: %w", err)
		}
	}

	// Step 3: Structure data (if enabled) to drop unknown fields
	if options.StructureData {
		var err error
		rows, err = p.transformer.StructureCSVRows(rows, csvSchema)
		if err != nil {
			return nil, fmt.Errorf("failed to structure csv rows: %w", err)
		}
	}

	// Step 4: Validate rows
	validationResult := p.validator.ValidateCSVRowsWithOptions(rows, csvSchema, options.CollectAllErrors)

	// Step 5: Strict mode handling
	if !validationResult.Valid && options.StrictValidation {
		outputData, _ := json.Marshal(rows)
		return &ProcessResult{
			Valid:  false,
			Data:   outputData,
			Errors: validationResult.Errors,
		}, fmt.Errorf("%s", validationResult.ErrorMessage())
	}

	// Step 6: Marshal output
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

// compiledCSVSchema holds a parsed CSV schema and its content hash.
type compiledCSVSchema struct {
	schema *CSVSchema
	hash   string
}

func (c *compiledCSVSchema) SchemaType() string  { return string(FormatCSV) }
func (c *compiledCSVSchema) ContentHash() string { return c.hash }
