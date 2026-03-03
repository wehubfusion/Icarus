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

// ActionError represents an execution error during string actions.
type ActionError struct {
	NodeID    string
	ItemIndex int
	Action    string
	Message   string
	Err       error
}

func (e *ActionError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: action '%s' error at item %d: %s", e.NodeID, e.Action, e.ItemIndex, e.Message)
	}
	return fmt.Sprintf("node %s: action '%s' error: %s", e.NodeID, e.Action, e.Message)
}

func (e *ActionError) Unwrap() error { return e.Err }

func NewActionError(nodeID string, itemIndex int, action, message string, err error) *ActionError {
	return &ActionError{NodeID: nodeID, ItemIndex: itemIndex, Action: action, Message: message, Err: err}
}
