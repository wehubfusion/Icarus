package jsonops

import "fmt"

// OperationError represents an error that occurred during a JSON operation
type OperationError struct {
	Operation string
	Message   string
	Path      string
	Cause     error
}

func (e *OperationError) Error() string {
	if e.Path != "" && e.Cause != nil {
		return fmt.Sprintf("%s operation failed at path '%s': %s: %v", e.Operation, e.Path, e.Message, e.Cause)
	}
	if e.Path != "" {
		return fmt.Sprintf("%s operation failed at path '%s': %s", e.Operation, e.Path, e.Message)
	}
	if e.Cause != nil {
		return fmt.Sprintf("%s operation failed: %s: %v", e.Operation, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s operation failed: %s", e.Operation, e.Message)
}

func (e *OperationError) Unwrap() error {
	return e.Cause
}

// ValidationError represents an error that occurred during schema validation
type ValidationError struct {
	Path    string
	Message string
	Errors  []string
}

func (e *ValidationError) Error() string {
	if len(e.Errors) > 0 {
		return fmt.Sprintf("validation failed: %s (errors: %v)", e.Message, e.Errors)
	}
	if e.Path != "" {
		return fmt.Sprintf("validation failed at path '%s': %s", e.Path, e.Message)
	}
	return fmt.Sprintf("validation failed: %s", e.Message)
}
