package simplecondition

import "fmt"

// ConfigError represents a configuration validation error
type ConfigError struct {
	NodeID  string
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("node %s: config error [%s]: %s", e.NodeID, e.Field, e.Message)
}

func NewConfigError(nodeID, field, message string) *ConfigError {
	return &ConfigError{
		NodeID:  nodeID,
		Field:   field,
		Message: message,
	}
}

// ComparisonError represents an error during comparison operations
type ComparisonError struct {
	NodeID    string
	ItemIndex int
	Operator  string
	Message   string
}

func (e *ComparisonError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: comparison error at item %d [%s]: %s",
e.NodeID, e.ItemIndex, e.Operator, e.Message)
	}
	return fmt.Sprintf("node %s: comparison error [%s]: %s", e.NodeID, e.Operator, e.Message)
}

func NewComparisonError(nodeID string, itemIndex int, operator, message string) *ComparisonError {
	return &ComparisonError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Operator:  operator,
		Message:   message,
	}
}

// EvaluationError represents an error during condition evaluation
type EvaluationError struct {
	NodeID        string
	ItemIndex     int
	ConditionName string
	Message       string
}

func (e *EvaluationError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: evaluation error at item %d [%s]: %s",
e.NodeID, e.ItemIndex, e.ConditionName, e.Message)
	}
	return fmt.Sprintf("node %s: evaluation error [%s]: %s", e.NodeID, e.ConditionName, e.Message)
}

func NewEvaluationError(nodeID string, itemIndex int, conditionName, message string) *EvaluationError {
	return &EvaluationError{
		NodeID:        nodeID,
		ItemIndex:     itemIndex,
		ConditionName: conditionName,
		Message:       message,
	}
}
