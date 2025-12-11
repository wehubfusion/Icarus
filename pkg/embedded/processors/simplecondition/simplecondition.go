package simplecondition

import (
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// SimpleConditionNode implements conditional logic evaluation for embeddedv2.
type SimpleConditionNode struct {
	runtime.BaseNode
}

// NewSimpleConditionNode creates a new simple condition node.
func NewSimpleConditionNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-simple-condition" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-simple-condition', got '%s'", config.PluginType)
	}
	return &SimpleConditionNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Process evaluates conditions and returns event-based routing output.
func (n *SimpleConditionNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err)))
	}

	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("invalid configuration: %v", err)))
	}

	results := make(map[string]bool)
	for _, condition := range cfg.Conditions {
		met, err := n.evaluateCondition(input, condition)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		results[condition.Name] = met
	}

	overallResult := n.calculateOverallResult(results, cfg.LogicOperator)

	output := make(map[string]interface{})
	if overallResult {
		output["true"] = input.Data
		output["false"] = nil
	} else {
		output["true"] = nil
		output["false"] = input.Data
	}

	return runtime.SuccessOutput(output)
}

func (n *SimpleConditionNode) evaluateCondition(input runtime.ProcessInput, condition Condition) (bool, error) {
	actualValue, exists := getValueFromPath(input.Data, condition.FieldPath)

	if !exists {
		if condition.Operator == OpIsEmpty {
			return true, nil
		}
		if condition.Operator == OpIsNotEmpty {
			return false, nil
		}
		return false, NewEvaluationError(
			n.NodeId(),
			input.ItemIndex,
			condition.Name,
			fmt.Sprintf("field '%s' not found in input", condition.FieldPath),
		)
	}

	met, err := compareValues(
		n.NodeId(),
		input.ItemIndex,
		actualValue,
		condition.ExpectedValue,
		condition.Operator,
		condition.CaseInsensitive,
	)
	if err != nil {
		return false, err
	}

	return met, nil
}

func (n *SimpleConditionNode) calculateOverallResult(results map[string]bool, logicOp LogicOperator) bool {
	if len(results) == 0 {
		return false
	}
	switch logicOp {
	case LogicAnd:
		for _, met := range results {
			if !met {
				return false
			}
		}
		return true
	case LogicOr:
		for _, met := range results {
			if met {
				return true
			}
		}
		return false
	default:
		return false
	}
}
