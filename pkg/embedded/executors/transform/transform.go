package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for transform operations
type Executor struct{}

// TransformConfig defines the configuration for transformation
type TransformConfig struct {
	Type            string                 `json:"type"` // "jsonpath", "passthrough", "static"
	Transformations []TransformOperation   `json:"transformations,omitempty"`
	StaticOutput    map[string]interface{} `json:"static_output,omitempty"`
}

// TransformOperation defines a single transformation operation
type TransformOperation struct {
	SourcePath      string `json:"source_path"`      // JSONPath to extract from input
	DestinationPath string `json:"destination_path"` // JSONPath to set in output
}

// NewExecutor creates a new transform executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a transform node
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var transformConfig TransformConfig
	if err := json.Unmarshal(config.Configuration, &transformConfig); err != nil {
		return nil, fmt.Errorf("failed to parse transform configuration: %w", err)
	}

	// Execute transformation based on type
	var output []byte
	var err error

	switch transformConfig.Type {
	case "passthrough":
		// Simply pass through the input
		output = config.Input

	case "static":
		// Return static output
		output, err = json.Marshal(transformConfig.StaticOutput)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal static output: %w", err)
		}

	case "jsonpath":
		// Apply JSONPath transformations
		output, err = e.applyJSONPathTransformations(config.Input, transformConfig.Transformations)
		if err != nil {
			return nil, fmt.Errorf("jsonpath transformation failed: %w", err)
		}

	default:
		return nil, fmt.Errorf("unknown transform type: %s", transformConfig.Type)
	}

	return output, nil
}

// applyJSONPathTransformations applies a series of JSONPath-based transformations
func (e *Executor) applyJSONPathTransformations(input []byte, transformations []TransformOperation) ([]byte, error) {
	if len(input) == 0 {
		input = []byte("{}")
	}

	if !json.Valid(input) {
		return nil, fmt.Errorf("input is not valid JSON")
	}

	inputJSON := string(input)
	outputJSON := "{}"

	for i, transform := range transformations {
		// Extract value from source using JSONPath
		sourceValue := gjson.Get(inputJSON, transform.SourcePath)

		if !sourceValue.Exists() {
			// Skip transformation if source path not found
			continue
		}

		// Set value in destination using JSONPath
		var err error
		outputJSON, err = sjson.Set(outputJSON, transform.DestinationPath, sourceValue.Value())
		if err != nil {
			return nil, fmt.Errorf("failed to set destination field %s (transformation %d): %w", transform.DestinationPath, i, err)
		}
	}

	// Compact the JSON to remove unnecessary whitespace
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(outputJSON)); err != nil {
		return []byte(outputJSON), nil // Return uncompacted if compaction fails
	}

	return buf.Bytes(), nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-transform"
}

