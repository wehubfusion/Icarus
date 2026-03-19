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

type Severity string

const (
	SeverityError   Severity = "ERROR"
	SeverityWarning Severity = "WARNING"
	SeverityInfo    Severity = "INFO"
)

type ValidationMode string

const (
	ValidationModeStrict  ValidationMode = "STRICT"
	ValidationModeNormal  ValidationMode = "NORMAL"
	ValidationModeLenient ValidationMode = "LENIENT"
)

// ValidationIssue represents a single validation finding (error, warning, or info).
type ValidationIssue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Code    string `json:"code"`
	// Severity defaults to ERROR for backward compatibility when omitted by processors.
	Severity Severity `json:"severity"`
}

// ValidationError is preserved for backward compatibility (all previous findings were treated as errors).
type ValidationError = ValidationIssue

// ValidationResult holds the result of validation
type ValidationResult struct {
	Valid    bool              `json:"valid"`
	Errors   []ValidationError `json:"errors,omitempty"`
	Warnings []ValidationIssue `json:"warnings,omitempty"`
	Infos    []ValidationIssue `json:"infos,omitempty"`
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
	Mode             ValidationMode
	CollectAllErrors bool
}

// ProcessResult contains the result of schema processing
type ProcessResult struct {
	Valid    bool              `json:"valid"`
	Data     []byte            `json:"data"`
	Errors   []ValidationError `json:"errors,omitempty"`
	Warnings []ValidationIssue `json:"warnings,omitempty"`
	Infos    []ValidationIssue `json:"infos,omitempty"`
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
