package errornode

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// Config defines the configuration for the ErrorNode.
// It aligns with the Apollo node schema (seed-node-schemas.sql):
// - "label"
// - "default_error_message"
type Config struct {
	Label               string `json:"label"`
	DefaultErrorMessage string `json:"default_error_message"`
}

// ErrorNode implements an embedded node that always produces an error
// with a configurable message. The message can come from:
// - input data field "message" (preferred)
// - config.DefaultErrorMessage
// - a generic fallback when both are empty
type ErrorNode struct {
	runtime.BaseNode
}

// NewErrorNode creates a new ErrorNode instance.
// It is registered under plugin type "plugin-error".
func NewErrorNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-error" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-error', got '%s'", config.PluginType)
	}

	return &ErrorNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Process evaluates the configuration and input data, then returns
// an error output with the resolved message.
//
// Runtime behavior:
//   - When downstream nodes listen to this node's pluginError section,
//     the runtime will expose:
//       error: true
//       errorDescription: <resolved message>
//   - When there is no pluginError listener, the error bubbled from
//     this node will cause the unit/workflow to fail.
func (n *ErrorNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		configErr := runtime.NewProcessingError(
			input.NodeId,
			input.Label,
			input.PluginType,
			input.ItemIndex,
			"config",
			fmt.Errorf("failed to parse configuration: %w", err),
		)
		return runtime.ErrorOutput(configErr)
	}

	message := resolveMessage(input.Data, cfg.DefaultErrorMessage)
	if message == "" {
		message = "Error node triggered without message"
	}

	baseErr := errors.New(message)
	procErr := runtime.NewProcessingError(
		input.NodeId,
		input.Label,
		input.PluginType,
		input.ItemIndex,
		"execute",
		baseErr,
	)

	return runtime.ErrorOutput(procErr)
}

// resolveMessage determines the final error message using, in order:
//   1. The "message" field from input data (if present and non-empty string)
//   2. The provided defaultErrorMessage (if non-empty)
//   3. Caller-provided generic fallback (handled by caller when this returns "")
func resolveMessage(data map[string]interface{}, defaultErrorMessage string) string {
	if data != nil {
		if v, ok := data["message"]; ok && v != nil {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	if defaultErrorMessage != "" {
		return defaultErrorMessage
	}
	return ""
}

