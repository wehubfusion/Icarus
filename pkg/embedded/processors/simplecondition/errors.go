package simplecondition

import "fmt"

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config error [%s]: %s", e.Field, e.Message)
}

// ComparisonError represents an error during comparison operations
type ComparisonError struct {
	Operator string
	Message  string
}

func (e *ComparisonError) Error() string {
	return fmt.Sprintf("comparison error [%s]: %s", e.Operator, e.Message)
}

// EvaluationError represents an error during condition evaluation
type EvaluationError struct {
	ConditionName string
	Message       string
}

func (e *EvaluationError) Error() string {
	return fmt.Sprintf("evaluation error [%s]: %s", e.ConditionName, e.Message)
}
