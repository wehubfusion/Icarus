package dateformatter

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embeddedv2/runtime"
)

// DateFormatterNode implements date formatting for embeddedv2.
type DateFormatterNode struct {
	runtime.BaseNode
}

// NewDateFormatterNode creates a new date formatter node.
func NewDateFormatterNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-date-formatter" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-date-formatter', got '%s'", config.PluginType)
	}
	return &DateFormatterNode{BaseNode: runtime.NewBaseNode(config)}, nil
}

// Process formats dates according to config and input.
func (n *DateFormatterNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err)))
	}

	if err := cfg.Validate(n.NodeId()); err != nil {
		return runtime.ErrorOutput(err)
	}

	result, err := executeFormat(n.NodeId(), input.ItemIndex, input.Data, cfg)
	if err != nil {
		return runtime.ErrorOutput(err)
	}

	return runtime.SuccessOutput(result)
}
