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

// ParseError represents an error that occurred during parse operation
type ParseError struct {
	Path    string
	Message string
	Cause   error
}

func (e *ParseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("parse error at path '%s': %s: %v", e.Path, e.Message, e.Cause)
	}
	return fmt.Sprintf("parse error at path '%s': %s", e.Path, e.Message)
}

func (e *ParseError) Unwrap() error {
	return e.Cause
}

// RenderError represents an error that occurred during render operation
type RenderError struct {
	Field   string
	Message string
	Cause   error
}

func (e *RenderError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("render error for field '%s': %s: %v", e.Field, e.Message, e.Cause)
	}
	return fmt.Sprintf("render error for field '%s': %s", e.Field, e.Message)
}

func (e *RenderError) Unwrap() error {
	return e.Cause
}

// QueryError represents an error that occurred during query operation
type QueryError struct {
	Query   string
	Message string
	Cause   error
}

func (e *QueryError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("query error for '%s': %s: %v", e.Query, e.Message, e.Cause)
	}
	return fmt.Sprintf("query error for '%s': %s", e.Query, e.Message)
}

func (e *QueryError) Unwrap() error {
	return e.Cause
}

// TransformError represents an error that occurred during transform operation
type TransformError struct {
	Type    string
	Path    string
	Message string
	Cause   error
}

func (e *TransformError) Error() string {
	if e.Path != "" && e.Cause != nil {
		return fmt.Sprintf("transform error (%s) at path '%s': %s: %v", e.Type, e.Path, e.Message, e.Cause)
	}
	if e.Path != "" {
		return fmt.Sprintf("transform error (%s) at path '%s': %s", e.Type, e.Path, e.Message)
	}
	if e.Cause != nil {
		return fmt.Sprintf("transform error (%s): %s: %v", e.Type, e.Message, e.Cause)
	}
	return fmt.Sprintf("transform error (%s): %s", e.Type, e.Message)
}

func (e *TransformError) Unwrap() error {
	return e.Cause
}

// ValidationError represents an error that occurred during JSON Schema validation
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
