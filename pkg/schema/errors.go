package schema

import "fmt"

// SchemaError represents a schema-related error
type SchemaError struct {
	Message string
	Code    string
	Err     error
}

// Error implements the error interface
func (e *SchemaError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Unwrap returns the underlying error
func (e *SchemaError) Unwrap() error {
	return e.Err
}

// NewSchemaError creates a new schema error
func NewSchemaError(message, code string, err error) *SchemaError {
	return &SchemaError{
		Message: message,
		Code:    code,
		Err:     err,
	}
}

// ParseError creates a schema parsing error
func ParseError(err error) *SchemaError {
	return &SchemaError{
		Message: "Schema parsing failed",
		Code:    "SCHEMA_PARSE_ERROR",
		Err:     err,
	}
}

// ValidationFailedError creates a validation error
func ValidationFailedError(errors []ValidationError) *SchemaError {
	return &SchemaError{
		Message: fmt.Sprintf("Validation failed with %d errors", len(errors)),
		Code:    "VALIDATION_FAILED",
		Err:     nil,
	}
}

// TransformError creates a transformation error
func TransformError(operation string, err error) *SchemaError {
	return &SchemaError{
		Message: fmt.Sprintf("Transform operation '%s' failed", operation),
		Code:    "TRANSFORM_ERROR",
		Err:     err,
	}
}
