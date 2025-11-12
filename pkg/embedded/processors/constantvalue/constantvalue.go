package constantvalue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for constant value generation
type Executor struct{}

// NewExecutor creates a new constant value executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Config represents the configuration for constant value plugin
type Config struct {
	Fields []Field `json:"fields"`
}

// Field represents a single constant field
type Field struct {
	Name     string      `json:"name"`
	DataType string      `json:"dataType"`
	String   string      `json:"string,omitempty"`
	Number   json.Number `json:"number,omitempty"`
	Boolean  bool        `json:"boolean,omitempty"`
}

// Execute executes the constant value processor
// Simply extracts configured constant values and returns them as output
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var cfg Config
	if err := json.Unmarshal(config.Configuration, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Validate that we have at least one field
	if len(cfg.Fields) == 0 {
		return nil, fmt.Errorf("no fields configured for constant value")
	}

	// Build output map from constant fields
	output := make(map[string]interface{})
	for _, field := range cfg.Fields {
		switch field.DataType {
		case "STRING":
			output[field.Name] = field.String
		case "NUMBER":
			// Keep as json.Number for precision
			if field.Number != "" {
				output[field.Name] = field.Number
			} else {
				output[field.Name] = json.Number("0")
			}
		case "BOOLEAN":
			output[field.Name] = field.Boolean
		default:
			// Default to string if type unknown
			output[field.Name] = field.String
		}
	}

	// Marshal output
	outputBytes, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return outputBytes, nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-constant-value"
}

