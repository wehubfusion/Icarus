package csv

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// CSVSchemaProcessor processes JSON arrays of row objects against a CSV schema definition.
type CSVSchemaProcessor struct {
	parser *Parser
}

// NewCSVSchemaProcessor creates a processor that uses the given parser.
func NewCSVSchemaProcessor(p *Parser) *CSVSchemaProcessor {
	return &CSVSchemaProcessor{parser: p}
}

// Type returns the format identifier for CSV.
func (p *CSVSchemaProcessor) Type() string {
	return string(contracts.FormatCSV)
}

// ParseSchema parses and validates the CSV schema definition, returning a compiled schema.
func (p *CSVSchemaProcessor) ParseSchema(definition []byte) (contracts.CompiledSchema, error) {
	s, err := p.parser.ParseCSV(definition)
	if err != nil {
		return nil, fmt.Errorf("csv schema parse error: %w", err)
	}
	return &compiledCSVSchema{schema: s}, nil
}

// Process runs the full CSV pipeline: parse input rows, apply defaults, structure, validate, marshal output.
func (p *CSVSchemaProcessor) Process(inputData []byte, cs contracts.CompiledSchema, options contracts.ProcessOptions) (*contracts.ProcessResult, error) {
	compiled, ok := cs.(*compiledCSVSchema)
	if !ok {
		return nil, fmt.Errorf("expected compiledCSVSchema, got %T", cs)
	}
	s := compiled.schema

	var rows []map[string]interface{}
	if err := json.Unmarshal(inputData, &rows); err != nil {
		return nil, fmt.Errorf("invalid input JSON (expected array of objects): %w", err)
	}

	if options.ApplyDefaults {
		var err error
		rows, err = ApplyCSVDefaults(rows, s)
		if err != nil {
			return nil, fmt.Errorf("failed to apply csv defaults: %w", err)
		}
	}

	if options.StructureData {
		var err error
		rows, err = StructureCSVRows(rows, s)
		if err != nil {
			return nil, fmt.Errorf("failed to structure csv rows: %w", err)
		}
	}

	validationResult := ValidateCSVRowsWithOptions(rows, s, options.CollectAllErrors)

	if !validationResult.Valid && options.StrictValidation {
		outputData, _ := json.Marshal(rows)
		return &contracts.ProcessResult{
			Valid:  false,
			Data:   outputData,
			Errors: validationResult.Errors,
		}, fmt.Errorf("%s", validationResult.ErrorMessage())
	}

	outputData, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return &contracts.ProcessResult{
		Valid:  validationResult.Valid,
		Data:   outputData,
		Errors: validationResult.Errors,
	}, nil
}

// compiledCSVSchema holds a parsed CSV schema.
type compiledCSVSchema struct {
	schema *CSVSchema
}

func (c *compiledCSVSchema) SchemaType() string { return string(contracts.FormatCSV) }
