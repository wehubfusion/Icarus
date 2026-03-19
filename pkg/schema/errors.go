package schema

import "fmt"

// SchemaError represents a schema-related error with a machine-readable code.
type SchemaError struct {
	Message string
	Code    string
	Err     error
}

// Error implements the error interface.
func (e *SchemaError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Unwrap returns the underlying error.
func (e *SchemaError) Unwrap() error {
	return e.Err
}

// NewSchemaError creates a new schema error.
func NewSchemaError(message, code string, err error) *SchemaError {
	return &SchemaError{
		Message: message,
		Code:    code,
		Err:     err,
	}
}
