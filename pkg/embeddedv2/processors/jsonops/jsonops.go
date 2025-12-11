package jsonops

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embeddedv2/runtime"
)

// JsonOpsNode implements JSON operations (parse and produce) for embeddedv2
type JsonOpsNode struct {
	runtime.BaseNode
}

// NewJsonOpsNode creates a new jsonops node instance
func NewJsonOpsNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	// Validate plugin type
	if config.PluginType != "plugin-json-operations" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-json-operations', got '%s'", config.PluginType)
	}

	return &JsonOpsNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Process executes the JSON operation (parse or produce)
func (n *JsonOpsNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	// Parse configuration
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "failed to parse configuration", err))
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "invalid configuration", err))
	}

	// Route to appropriate operation
	switch cfg.Operation {
	case "parse":
		return n.executeParse(input, &cfg)
	case "produce":
		return n.executeProduce(input, &cfg)
	default:
		return runtime.ErrorOutput(NewConfigError(
			n.NodeId(),
			fmt.Sprintf("unknown operation: %s", cfg.Operation),
			nil,
		))
	}
}
