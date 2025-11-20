package jsonops

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for JSON operations
type Executor struct{}

// NewExecutor creates a new JSON operations executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a JSON operation
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var jsonConfig Config
	if err := json.Unmarshal(config.Configuration, &jsonConfig); err != nil {
		return nil, fmt.Errorf("failed to parse jsonops configuration: %w", err)
	}

	// Validate configuration (operation is required)
	if err := jsonConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid jsonops configuration: %w", err)
	}

	// Route to appropriate operation handler
	switch jsonConfig.Operation {
	case "parse":
		return e.executeParse(config.Input, jsonConfig)

	case "produce":
		return e.executeProduce(config.Input, jsonConfig)

	default:
		return nil, fmt.Errorf("unknown operation: %s", jsonConfig.Operation)
	}
}

// executeParse parses and validates JSON against schema
func (e *Executor) executeParse(input []byte, config Config) ([]byte, error) {
	// Unmarshal input envelope
	var inputEnvelope map[string]interface{}
	if err := json.Unmarshal(input, &inputEnvelope); err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}

	dataField, hasData := inputEnvelope["data"]
	if !hasData {
		return nil, fmt.Errorf("input must contain a 'data' field")
	}

	var dataToValidate []byte

	// Check if data is a base64-encoded string (from JS runner)
	if encodedStr, isString := dataField.(string); isString {
		// Decode base64 string
		decoded, err := base64.StdEncoding.DecodeString(encodedStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 data: %w", err)
		}
		dataToValidate = decoded
	} else {
		// Data is already JSON object/array, marshal it
		marshaled, err := json.Marshal(dataField)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data field: %w", err)
		}
		dataToValidate = marshaled
	}

	// Process with schema
	validatedData, err := processWithSchema(dataToValidate, config.Schema, config.SchemaID, SchemaOptions{
		ApplyDefaults:    config.GetApplyDefaults(),
		StructureData:    config.GetStructureData(),
		StrictValidation: config.GetStrictValidation(),
	})
	if err != nil {
		return nil, err
	}

	// Unmarshal the validated data to wrap it
	var validatedResult interface{}
	if err := json.Unmarshal(validatedData, &validatedResult); err != nil {
		return nil, fmt.Errorf("failed to unmarshal validated data: %w", err)
	}

	return json.Marshal(validatedResult)
}

// executeProduce validates, structures, and encodes JSON to base64
func (e *Executor) executeProduce(input []byte, config Config) ([]byte, error) {
	// Extract data from input envelope
	var inputEnvelope map[string]interface{}
	if err := json.Unmarshal(input, &inputEnvelope); err != nil {
		return nil, fmt.Errorf("failed to parse input envelope: %w", err)
	}

	// Data is regular JSON (not encoded), marshal it to bytes
	dataToEncode, err := json.Marshal(inputEnvelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data field: %w", err)
	}

	// Process with schema (no defaults on produce)
	processedJSON, err := processWithSchema(dataToEncode, config.Schema, config.SchemaID, SchemaOptions{
		ApplyDefaults:    false, // Never apply defaults on produce
		StructureData:    config.GetStructureData(),
		StrictValidation: config.GetStrictValidation(),
	})
	if err != nil {
		return nil, err
	}

	// Pretty print if requested
	if config.Pretty {
		var prettyData interface{}
		if err := json.Unmarshal(processedJSON, &prettyData); err != nil {
			return nil, fmt.Errorf("failed to parse for pretty printing: %w", err)
		}
		processedJSON, err = json.MarshalIndent(prettyData, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to pretty print: %w", err)
		}
	}

	// Encode to base64
	encoded := base64.StdEncoding.EncodeToString(processedJSON)

	// Wrap result in data envelope following the standard convention
	wrappedResult := map[string]interface{}{
		"data": encoded,
	}

	return json.Marshal(wrappedResult)
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-jsonops"
}
