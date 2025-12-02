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
// It evaluates conditions on the input and outputs event flags for routing
// When used in iteration, receives single items and outputs single results
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

	// Parse input data
	var inputData interface{}
	if err := json.Unmarshal(config.Input, &inputData); err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}

	// Evaluate all conditions on the input
	results := make(map[string]ConditionResult)
	for _, condition := range cfg.Conditions {
		result := e.evaluateCondition(condition, config.Input)
		results[condition.Name] = result
	}

	// Calculate overall result based on logic operator
	overallResult := e.calculateOverallResult(results, cfg.LogicOperator)

	// Build output with event endpoints for conditional routing
	// The "true" and "false" endpoints contain the input data when their condition is met
	// This allows downstream nodes to receive the data through event-based routing
	output := map[string]interface{}{
		"result":     overallResult,
		"conditions": results,
		"summary": Summary{
			TotalConditions: len(cfg.Conditions),
			MetConditions:   countMetConditions(results),
			UnmetConditions: countUnmetConditions(results),
			LogicOperator:   cfg.LogicOperator,
		},
		// Pass input data through on the matching event path
		// This enables downstream nodes to receive filtered data
		"data": inputData,
	}

	// Set event outputs - the matching event gets the input data, the other is null
	// This enables event-based routing in the runtime
	if overallResult {
		output["true"] = inputData // Condition matched - pass data to "true" consumers
		output["false"] = nil      // Condition matched - "false" consumers get nothing
	} else {
		output["true"] = nil        // Condition not matched - "true" consumers get nothing
		output["false"] = inputData // Condition not matched - pass data to "false" consumers
	}

	return json.Marshal(output)
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
