package simplecondition

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for simple condition evaluation
type Executor struct{}

// NewExecutor creates a new simple condition executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes the simple condition processor
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var cfg Config
	if err := json.Unmarshal(config.Configuration, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Evaluate all conditions
	results := make(map[string]ConditionResult)
	for _, condition := range cfg.Conditions {
		result := e.evaluateCondition(condition, config.Input)
		results[condition.Name] = result
	}

	// Calculate overall result based on logic operator
	overallResult := e.calculateOverallResult(results, cfg.LogicOperator)

	// Build output
	output := Output{
		Result:     overallResult,
		Conditions: results,
		Summary: Summary{
			TotalConditions: len(cfg.Conditions),
			MetConditions:   countMetConditions(results),
			UnmetConditions: countUnmetConditions(results),
			LogicOperator:   cfg.LogicOperator,
		},
	}

	// Marshal output
	outputBytes, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal output: %w", err)
	}

	return outputBytes, nil
}

// evaluateCondition evaluates a single condition
func (e *Executor) evaluateCondition(condition Condition, input []byte) ConditionResult {
	result := ConditionResult{
		Name:          condition.Name,
		Operator:      condition.Operator,
		ExpectedValue: condition.ExpectedValue,
		Met:           false,
	}

	// Extract value from input
	actualValue, exists := getValueFromPath(input, condition.FieldPath)
	result.ActualValue = actualValue

	// Handle non-existent fields
	if !exists {
		// For existence operators, non-existent means empty
		if condition.Operator == OpIsEmpty {
			result.Met = true
			return result
		}
		if condition.Operator == OpIsNotEmpty {
			result.Met = false
			return result
		}

		// For other operators, non-existent field is an error
		result.Error = fmt.Sprintf("field '%s' not found in input", condition.FieldPath)
		return result
	}

	// Perform comparison
	met, err := compareValues(actualValue, condition.ExpectedValue, condition.Operator, condition.CaseInsensitive)
	if err != nil {
		result.Error = err.Error()
		return result
	}

	result.Met = met
	return result
}

// calculateOverallResult calculates the overall result based on logic operator
func (e *Executor) calculateOverallResult(results map[string]ConditionResult, logicOp LogicOperator) bool {
	if len(results) == 0 {
		return false
	}

	switch logicOp {
	case LogicAnd:
		// All conditions must be met
		for _, result := range results {
			if !result.Met {
				return false
			}
		}
		return true

	case LogicOr:
		// At least one condition must be met
		for _, result := range results {
			if result.Met {
				return true
			}
		}
		return false

	default:
		return false
	}
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-simple-condition"
}
