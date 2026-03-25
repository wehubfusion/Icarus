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

// EffectiveMode returns opts.Mode when it is STRICT, NORMAL, or LENIENT; otherwise NORMAL.
func EffectiveMode(opts ProcessOptions) ValidationMode {
	if opts.Mode != "" {
		m := ValidationMode(strings.ToUpper(strings.TrimSpace(string(opts.Mode))))
		switch m {
		case ValidationModeStrict, ValidationModeNormal, ValidationModeLenient:
			return m
		}
	}
	return ValidationModeNormal
}

// StrictProcessError returns a non-nil error when mode is STRICT and the result is invalid
// (Valid is false). The populated ProcessResult is still returned alongside this error.
func StrictProcessError(result *ProcessResult, mode ValidationMode) error {
	if mode != ValidationModeStrict || result == nil || result.Valid {
		return nil
	}
	vr := &ValidationResult{Valid: false, Errors: result.Errors}
	msg := vr.ErrorMessage()
	if msg == "" {
		msg = "schema validation failed"
	}
	nw, ni := len(result.Warnings), len(result.Infos)
	if nw > 0 || ni > 0 {
		msg = fmt.Sprintf("%s (also %d warning(s), %d info finding(s))", msg, nw, ni)
	}
	return fmt.Errorf("%s", msg)
}

// ProcessOptions controls schema processing behavior.
//
// Use Mode to control severity classification (where implemented) and fail-on-invalid:
//   - ValidationModeStrict:  invalid result returns a Go error via StrictProcessError
//     in addition to a populated ProcessResult. HL7 elevates several issue codes to
//     ERROR. HL7_CUSTOM_RULE_RUNTIME_ERROR uses each rule's configured severity (default
//     WARNING when the rule omits severity), distinct from HL7_CUSTOM_RULE_VIOLATION
//     (rule evaluated and failed).
//   - ValidationModeNormal:  invalid result is in the payload only (err == nil).
//   - ValidationModeLenient: invalid result is in the payload only (err == nil); HL7 downgrades codes.
//
// Deprecated: StrictValidation is no longer used by any processor and will be removed in a
// future release. Set Mode: ValidationModeStrict instead.
type ProcessOptions struct {
	ApplyDefaults    bool
	StructureData    bool
	Mode             ValidationMode
	CollectAllErrors bool

	// Deprecated: use Mode: ValidationModeStrict instead. Kept for JSON/config
	// backward-compatibility only; processors no longer inspect this field.
	StrictValidation bool `json:"strict_validation,omitempty"`
}

// ProcessResult contains the result of schema processing.
//
// Valid is false when Errors contains at least one issue (ERROR severity).
// Warnings and infos do not affect Valid. For HL7, HL7_CUSTOM_RULE_RUNTIME_ERROR
// defaults to WARNING when a rule omits severity; if the rule sets severity to ERROR,
// the issue is an error and affects Valid.
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
