package jsonops

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// executeProduce validates, structures, and base64-encodes JSON data
// Input: ProcessInput.Data["data"] - can be base64 string or raw JSON
// Output: {"encoded": "base64encoded..."}
func (n *JsonOpsNode) executeProduce(input runtime.ProcessInput, cfg *Config) runtime.ProcessOutput {
	// Extract "data" field from ProcessInput.Data
	dataField, hasData := input.Data["data"]
	if !hasData {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"produce",
			"input must contain a 'data' field",
			input.ItemIndex,
			nil,
		))
	}

	// Validate that schema is provided (enriched by Elysium)
	if len(cfg.Schema) == 0 {
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.SchemaID),
			nil,
		))
	}

	// Convert data to []byte for processing
	var dataToProcess []byte
	var err error

	switch v := dataField.(type) {
	case string:
		// Data is base64-encoded string - decode it
		dataToProcess, err = base64.StdEncoding.DecodeString(v)
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
				"failed to decode base64 data",
				input.ItemIndex,
				err,
			))
		}
	case []byte:
		// Data is already bytes
		dataToProcess = v
	default:
		// Data is JSON object/array - marshal it
		dataToProcess, err = json.Marshal(v)
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
				"failed to marshal data field",
				input.ItemIndex,
				err,
			))
		}
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema (no defaults on produce, structure and validate)
	result, err := engine.ProcessWithSchema(
		dataToProcess,
		cfg.Schema,
		schema.ProcessOptions{
			ApplyDefaults:    false, // Never apply defaults on produce
			StructureData:    cfg.GetStructureData(),
			StrictValidation: cfg.GetStrictValidation(),
		},
	)
	if err != nil {
		return runtime.ErrorOutput(NewProcessingError(
			n.NodeId(),
			"produce",
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
			"produce",
			"validation failed",
			input.ItemIndex,
			errorMessages,
		))
	}

	// Get processed JSON
	processedJSON := result.Data

	// Pretty print if requested
	if cfg.Pretty {
		var prettyData interface{}
		if err := json.Unmarshal(processedJSON, &prettyData); err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
				"failed to parse for pretty printing",
				input.ItemIndex,
				err,
			))
		}
		processedJSON, err = json.MarshalIndent(prettyData, "", "  ")
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
				"failed to pretty print",
				input.ItemIndex,
				err,
			))
		}
	}

	// Encode to base64
	encoded := base64.StdEncoding.EncodeToString(processedJSON)

	// Return with "encoded" key containing base64-encoded JSON (matches schema output_fields)
	return runtime.SuccessOutput(map[string]interface{}{
		"encoded": encoded,
	})
}
