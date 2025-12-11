package strings

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embeddedv2/runtime"
)

// StringsNode implements string operations for embeddedv2.
type StringsNode struct {
	runtime.BaseNode
}

// NewStringsNode creates a new strings node.
func NewStringsNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-strings" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-strings', got '%s'", config.PluginType)
	}
	return &StringsNode{BaseNode: runtime.NewBaseNode(config)}, nil
}

// Process executes the configured string operation.
func (n *StringsNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err), err))
	}

	if err := cfg.Validate(n.NodeId()); err != nil {
		return runtime.ErrorOutput(err)
	}

	result, err := executeOperation(n.NodeId(), input.ItemIndex, cfg.Operation, cfg.Params, input.Data)
	if err != nil {
		return runtime.ErrorOutput(err)
	}

	// Output schema: result
	return runtime.SuccessOutput(map[string]interface{}{"result": result})
}
