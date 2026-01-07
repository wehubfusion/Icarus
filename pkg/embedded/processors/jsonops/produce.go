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
	// Validate that schema is provided (enriched by Elysium)
	if len(cfg.Schema) == 0 {
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("schema_id '%s' was not enriched - ensure Elysium enrichment is configured", cfg.SchemaID),
			nil,
		))
	}

	// Convert full input data to []byte for processing (root can be object or array)
	var dataToProcess []byte
	var err error

	dataToProcess, err = json.Marshal(input.Data)
		if err != nil {
			return runtime.ErrorOutput(NewProcessingError(
				n.NodeId(),
				"produce",
			"failed to marshal input data",
				input.ItemIndex,
				err,
			))
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
