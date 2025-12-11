package jsrunner

import (
	"fmt"
	"time"
)

// ExecutionError represents a JavaScript execution error
type ExecutionError struct {
	NodeID    string
	ItemIndex int
	Line      int
	Column    int
	Message   string
	Err       error
}

func (e *ExecutionError) Error() string {
	if e.ItemIndex >= 0 {
		if e.Line > 0 {
			return fmt.Sprintf("node %s: JavaScript execution error at item %d (line %d, col %d): %s",
				e.NodeID, e.ItemIndex, e.Line, e.Column, e.Message)
		}
		return fmt.Sprintf("node %s: JavaScript execution error at item %d: %s",
			e.NodeID, e.ItemIndex, e.Message)
	}
	if e.Line > 0 {
		return fmt.Sprintf("node %s: JavaScript execution error (line %d, col %d): %s",
			e.NodeID, e.Line, e.Column, e.Message)
	}
	return fmt.Sprintf("node %s: JavaScript execution error: %s", e.NodeID, e.Message)
}

func (e *ExecutionError) Unwrap() error {
	return e.Err
}

func NewExecutionError(nodeID, message string, itemIndex, line, column int, err error) *ExecutionError {
	return &ExecutionError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Line:      line,
		Column:    column,
		Message:   message,
		Err:       err,
	}
}

// TimeoutError represents a JavaScript execution timeout
type TimeoutError struct {
	NodeID    string
	ItemIndex int
	Message   string
	Timeout   time.Duration
}

func (e *TimeoutError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: %s at item %d (timeout: %v)",
			e.NodeID, e.Message, e.ItemIndex, e.Timeout)
	}
	return fmt.Sprintf("node %s: %s (timeout: %v)", e.NodeID, e.Message, e.Timeout)
}

func NewTimeoutError(nodeID, message string, itemIndex int, timeout time.Duration) *TimeoutError {
	return &TimeoutError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Message:   message,
		Timeout:   timeout,
	}
}

// ConfigError represents a configuration error
type ConfigError struct {
	NodeID  string
	Message string
	Err     error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("node %s: configuration error: %s", e.NodeID, e.Message)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

func NewConfigError(nodeID, message string, err error) *ConfigError {
	return &ConfigError{
		NodeID:  nodeID,
		Message: message,
		Err:     err,
	}
}

// SecurityError represents a security violation
type SecurityError struct {
	NodeID    string
	ItemIndex int
	Message   string
	Err       error
}

func (e *SecurityError) Error() string {
	if e.ItemIndex >= 0 {
		return fmt.Sprintf("node %s: security violation at item %d: %s",
			e.NodeID, e.ItemIndex, e.Message)
	}
	return fmt.Sprintf("node %s: security violation: %s", e.NodeID, e.Message)
}

func (e *SecurityError) Unwrap() error {
	return e.Err
}

func NewSecurityError(nodeID, message string, itemIndex int, err error) *SecurityError {
	return &SecurityError{
		NodeID:    nodeID,
		ItemIndex: itemIndex,
		Message:   message,
		Err:       err,
	}
}
