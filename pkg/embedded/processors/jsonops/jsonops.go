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
	// Process with schema
	result, err := processWithSchema(input, config.Schema, config.SchemaID, SchemaOptions{
		ApplyDefaults:    config.GetApplyDefaults(),
		StructureData:    config.GetStructureData(),
		StrictValidation: config.GetStrictValidation(),
	})
	if err != nil {
		return nil, err
	}

	// If schema processing returns a map that only contains a nested "data" object,
	// flatten it so downstream nodes can read the parsed payload from the root.
	// This is a conservative compatibility measure for plans that map payloads
	// into "/data" before parsing (avoids double-nesting in StandardOutput).
	var parsed map[string]interface{}
	if err := json.Unmarshal(result, &parsed); err == nil && len(parsed) == 1 {
		if inner, hasData := parsed["data"].(map[string]interface{}); hasData {
			result, _ = json.Marshal(inner)
		}
	}

	return result, nil
}

// executeProduce validates, structures, and encodes JSON to base64
func (e *Executor) executeProduce(input []byte, config Config) ([]byte, error) {
	// Process with schema (no defaults on produce)
	processedJSON, err := processWithSchema(input, config.Schema, config.SchemaID, SchemaOptions{
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

	// Return as JSON object with base64 data
	result := map[string]string{
		"result":   encoded,
		"encoding": "base64",
	}

	return json.Marshal(result)
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-jsonops"
}
