package simplecondition

import (
	"encoding/json"
	"fmt"
	"strings"

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
	for _, manualInput := range cfg.ManualInputs {
		met, err := n.evaluateCondition(input, manualInput)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		results[manualInput.Name] = met
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

func (n *SimpleConditionNode) evaluateCondition(input runtime.ProcessInput, manualInput ManualInput) (bool, error) {
	// Use name as field_path to extract value from input data
	// Strip leading slash if present (field mappings may include it)
	fieldPath := strings.TrimPrefix(manualInput.Name, "/")
	actualValue, exists := getValueFromPath(input.Data, fieldPath)

	if !exists {
		// Special handling: if field doesn't exist and we're checking for empty string,
		// treat missing field as empty string (backward compatible with is_empty)
		if manualInput.Operator == OpEquals && manualInput.Value == "" {
			return true, nil // Missing field equals empty string
		}

		// For all other cases, missing field is an error
		// Debug: Log available fields
		availableFields := make([]string, 0, len(input.Data))
		for key := range input.Data {
			availableFields = append(availableFields, key)
		}

		return false, NewEvaluationError(
			n.NodeId(),
			input.ItemIndex,
			manualInput.Name,
			fmt.Sprintf("field '%s' not found in input. Available fields: %v. Input data: %+v",
				fieldPath, availableFields, input.Data),
		)
	}

	// Convert the expected value from string to the appropriate type
	expectedValue, err := manualInput.ConvertValue()
	if err != nil {
		return false, NewConfigError(
			n.NodeId(),
			"value",
			fmt.Sprintf("failed to convert value '%s' for condition '%s': %v", manualInput.Value, manualInput.Name, err),
		)
	}

	met, err := compareValues(
		n.NodeId(),
		input.ItemIndex,
		actualValue,
		expectedValue,
		manualInput.Operator,
		false, // case_insensitive removed from schema
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
