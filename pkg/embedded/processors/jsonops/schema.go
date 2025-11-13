package jsonops

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/schema"
)

// SchemaOptions defines options for schema processing
type SchemaOptions struct {
	ApplyDefaults    bool
	StructureData    bool
	StrictValidation bool
}

// processWithSchema applies schema-based validation and transformation using Icarus schema engine
func processWithSchema(inputJSON []byte, schemaDefinition json.RawMessage, schemaID string, options SchemaOptions) ([]byte, error) {
	// Validate input JSON
	if !isValidJSON(inputJSON) {
		return nil, fmt.Errorf("input is not valid JSON")
	}

	// Check that schema is provided (should be enriched by Elysium when using schema_id)
	if len(schemaDefinition) == 0 {
		if schemaID != "" {
			return nil, fmt.Errorf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", schemaID)
		}
		return nil, fmt.Errorf("schema definition is required")
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema
	result, err := engine.ProcessWithSchema(
		inputJSON,
		schemaDefinition,
		schema.ProcessOptions{
			ApplyDefaults:    options.ApplyDefaults,
			StructureData:    options.StructureData,
			StrictValidation: options.StrictValidation,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("schema processing failed: %w", err)
	}

	return result.Data, nil
}

// isValidJSON checks if the input is valid JSON
func isValidJSON(data []byte) bool {
	var js interface{}
	return json.Unmarshal(data, &js) == nil
}
