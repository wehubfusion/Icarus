package jsonops

import "fmt"

// ProcessingError represents an error that occurred during JSON processing
// It includes context from the embedded runtime
type ProcessingError struct {
	NodeId    string
	Operation string
	Message   string
	ItemIndex int // -1 if not in iteration
	Cause     error
}

func (e *ProcessingError) Error() string {
	base := fmt.Sprintf("jsonops node '%s' operation '%s': %s", e.NodeId, e.Operation, e.Message)

	if e.ItemIndex >= 0 {
		base = fmt.Sprintf("%s (item %d)", base, e.ItemIndex)
	}

	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", base, e.Cause)
	}

	return base
}

func (e *ProcessingError) Unwrap() error {
	return e.Cause
}

// NewProcessingError creates a new processing error
func NewProcessingError(nodeId, operation, message string, itemIndex int, cause error) *ProcessingError {
	return &ProcessingError{
		NodeId:    nodeId,
		Operation: operation,
		Message:   message,
		ItemIndex: itemIndex,
		Cause:     cause,
	}
}

// ValidationError represents a schema validation failure
type ValidationError struct {
	NodeId    string
	Operation string
	Message   string
	ItemIndex int
	Errors    []string
}

func (e *ValidationError) Error() string {
	base := fmt.Sprintf("jsonops node '%s' validation failed: %s", e.NodeId, e.Message)

	if e.ItemIndex >= 0 {
		base = fmt.Sprintf("%s (item %d)", base, e.ItemIndex)
	}

	if len(e.Errors) > 0 {
		return fmt.Sprintf("%s - errors: %v", base, e.Errors)
	}

	return base
}

// NewValidationError creates a new validation error
func NewValidationError(nodeId, operation, message string, itemIndex int, errors []string) *ValidationError {
	return &ValidationError{
		NodeId:    nodeId,
		Operation: operation,
		Message:   message,
		ItemIndex: itemIndex,
		Errors:    errors,
	}
}

// ConfigError represents a configuration error
type ConfigError struct {
	NodeId  string
	Message string
	Cause   error
}

func (e *ConfigError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("jsonops node '%s' config error: %s: %v", e.NodeId, e.Message, e.Cause)
	}
	return fmt.Sprintf("jsonops node '%s' config error: %s", e.NodeId, e.Message)
}

func (e *ConfigError) Unwrap() error {
	return e.Cause
}

// NewConfigError creates a new config error
func NewConfigError(nodeId, message string, cause error) *ConfigError {
	return &ConfigError{
		NodeId:  nodeId,
		Message: message,
		Cause:   cause,
	}
}
