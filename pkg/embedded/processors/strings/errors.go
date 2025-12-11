package strings

import "fmt"

// ConfigError represents a configuration validation error.
type ConfigError struct {
	NodeID  string
	Field   string
	Message string
	Err     error
}

func (e *ConfigError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("node %s: config error [%s]: %s", e.NodeID, e.Field, e.Message)
	}
	return fmt.Sprintf("node %s: config error: %s", e.NodeID, e.Message)
}

func (e *ConfigError) Unwrap() error { return e.Err }

func NewConfigError(nodeID, field, message string, err error) *ConfigError {
	return &ConfigError{NodeID: nodeID, Field: field, Message: message, Err: err}
}

// OperationError represents an execution error during string operations.
type OperationError struct {
	NodeID    string
	ItemIndex int
	Operation string
	Message   string
	Err       error
}

func (e *OperationError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: operation '%s' error at item %d: %s", e.NodeID, e.Operation, e.ItemIndex, e.Message)
	}
	return fmt.Sprintf("node %s: operation '%s' error: %s", e.NodeID, e.Operation, e.Message)
}

func (e *OperationError) Unwrap() error { return e.Err }

func NewOperationError(nodeID string, itemIndex int, op, message string, err error) *OperationError {
	return &OperationError{NodeID: nodeID, ItemIndex: itemIndex, Operation: op, Message: message, Err: err}
}
