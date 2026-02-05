package jsonops

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// executeParse validates and transforms incoming JSON data against a schema
// Input: ProcessInput.Data["data"] - can be base64 string or raw JSON
// Output: Flattened schema fields (e.g., {"name": "Alice", "age": 30})
func (n *JsonOpsNode) executeParse(input runtime.ProcessInput, cfg *Config) runtime.ProcessOutput {
	// Extract "data" field from ProcessInput.Data
	dataField, hasData := input.Data["data"]
	if !hasData {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"input must contain a 'data' field",
			input.ItemIndex,
			nil,
		))
	}

	// Convert data to []byte for processing
	var dataToValidate []byte
	var err error

	switch v := dataField.(type) {
	case string:
		// Data is base64-encoded string - decode it
		dataToValidate, err = base64.StdEncoding.DecodeString(v)
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"parse",
				"failed to decode base64 data",
				input.ItemIndex,
				err,
			))
		}
	case []byte:
		// Data is already bytes
		dataToValidate = v
	default:
		// Data is JSON object/array - marshal it
		dataToValidate, err = json.Marshal(v)
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"parse",
				"failed to marshal data field",
				input.ItemIndex,
				err,
			))
		}
	}

	// Validate that schema is provided (enriched by Elysium)
	if len(cfg.Schema) == 0 {
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.SchemaID),
			nil,
		))
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema
	result, err := engine.ProcessWithSchema(
		dataToValidate,
		cfg.Schema,
		schema.ProcessOptions{
			ApplyDefaults:    cfg.GetApplyDefaults(),
			StructureData:    cfg.GetStructureData(),
			StrictValidation: cfg.GetStrictValidation(),
		},
	)
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"schema processing failed",
			input.ItemIndex,
			err,
		))
	}

	// Check validation result
	if !result.Valid && cfg.GetStrictValidation() {
		// Convert schema.ValidationError to strings
		errorMessages := make([]string, len(result.Errors))
		for i, err := range result.Errors {
			errorMessages[i] = fmt.Sprintf("%s: %s", err.Path, err.Message)
		}
		return runtime.ErrorOutput(NewValidationError(
			n.NodeId(),
			"parse",
			"validation failed",
			input.ItemIndex,
			errorMessages,
		))
	}

	// Parse the schema to check if it expects an array at root level
	var parsedSchema schema.Schema
	if err := json.Unmarshal(cfg.Schema, &parsedSchema); err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"failed to parse schema definition",
			input.ItemIndex,
			err,
		))
	}

	// Check if schema expects an array at root level
	if parsedSchema.Type == schema.TypeArray {
		// For array schemas, unmarshal to []interface{} and wrap it
		var arrayData []interface{}
		if err := json.Unmarshal(result.Data, &arrayData); err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"parse",
				"failed to unmarshal array data",
				input.ItemIndex,
				err,
			))
		}

		// Wrap array under reserved key for root-as-array
		return runtime.SuccessOutput(map[string]interface{}{
			runtime.RootArrayKey: arrayData,
		})
	}

	// Unmarshal validated data to map for output (for OBJECT schemas)
	var validatedMap map[string]interface{}
	if err := json.Unmarshal(result.Data, &validatedMap); err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"parse",
			"failed to unmarshal validated data",
			input.ItemIndex,
			err,
		))
	}

	// Return flattened schema fields directly (no "data" wrapper)
	// The runtime will flatten this with node-specific keys
	return runtime.SuccessOutput(validatedMap)
}
