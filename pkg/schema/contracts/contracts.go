package contracts

import (
	"fmt"
	"strings"
)

// SchemaFormat identifies the schema definition format (JSON, CSV, HL7).
type SchemaFormat string

const (
	FormatJSON SchemaFormat = "JSON"
	FormatCSV  SchemaFormat = "CSV"
	FormatHL7  SchemaFormat = "HL7"
)

// ValidationError represents a single validation error
type ValidationError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// ValidationResult holds the result of validation
type ValidationResult struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors,omitempty"`
}

// ErrorMessage returns a single well-formatted error string for the validation result.
func (r *ValidationResult) ErrorMessage() string {
	if r == nil || r.Valid || len(r.Errors) == 0 {
		return ""
	}
	n := len(r.Errors)
	if n == 1 {
		return fmt.Sprintf("schema validation failed: %s: %s", r.Errors[0].Path, r.Errors[0].Message)
	}
	const maxShow = 10
	parts := make([]string, 0, maxShow+1)
	for i := 0; i < n && i < maxShow; i++ {
		parts = append(parts, fmt.Sprintf("%s: %s", r.Errors[i].Path, r.Errors[i].Message))
	}
	msg := fmt.Sprintf("schema validation failed with %d errors: %s", n, strings.Join(parts, "; "))
	if n > maxShow {
		msg += fmt.Sprintf(" ... and %d more", n-maxShow)
	}
	return msg
}

// ProcessOptions controls schema processing behavior.
type ProcessOptions struct {
	ApplyDefaults    bool
	StructureData    bool
	StrictValidation bool
	CollectAllErrors bool
	AllowExtraFields bool
}

// ProcessResult contains the result of schema processing
type ProcessResult struct {
	Valid  bool              `json:"valid"`
	Data   []byte            `json:"data"`
	Errors []ValidationError `json:"errors,omitempty"`
}

// SchemaProcessor is the extension point for all schema formats.
type SchemaProcessor interface {
	Type() string
	ParseSchema(definition []byte) (CompiledSchema, error)
	Process(inputData []byte, schema CompiledSchema, opts ProcessOptions) (*ProcessResult, error)
}

// CompiledSchema is the opaque result of ParseSchema.
type CompiledSchema interface {
	SchemaType() string
}
