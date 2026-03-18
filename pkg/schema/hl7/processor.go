package hl7

import (
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

func effectiveMode(opts contracts.ProcessOptions) contracts.ValidationMode {
	if opts.Mode != "" {
		m := contracts.ValidationMode(strings.ToUpper(strings.TrimSpace(string(opts.Mode))))
		switch m {
		case contracts.ValidationModeStrict, contracts.ValidationModeNormal, contracts.ValidationModeLenient:
			return m
		default:
			return contracts.ValidationModeNormal
		}
	}
	if opts.StrictValidation {
		return contracts.ValidationModeStrict
	}
	return contracts.ValidationModeNormal
}

func resolveSeverity(code string, mode contracts.ValidationMode) contracts.Severity {
	// Defaults are NORMAL mode, based on NIST/CDC/HL7apy patterns.
	normal := func(code string) contracts.Severity {
		switch code {
		case "HL7_EMPTY_MESSAGE", "HL7_INVALID_MSH", "HL7_INVALID_SCHEMA",
			"HL7_MISSING_REQUIRED", "HL7_MESSAGE_TYPE_MISMATCH", "HL7_REPETITION_VIOLATION",
			"HL7_DATATYPE":
			return contracts.SeverityError
		case "HL7_VERSION_MISMATCH", "HL7_REQUIRED", "HL7_NOT_USED", "HL7_LENGTH",
			"HL7_UNEXPECTED_SEGMENT":
			return contracts.SeverityWarning
		case "HL7_EXTRA_FIELD", "HL7_EXTRA_COMPONENT", "HL7_EXTRA_SUBCOMPONENT":
			return contracts.SeverityInfo
		default:
			return contracts.SeverityError
		}
	}

	switch mode {
	case contracts.ValidationModeStrict:
		switch code {
		case "HL7_EXTRA_FIELD", "HL7_EXTRA_COMPONENT", "HL7_EXTRA_SUBCOMPONENT":
			return contracts.SeverityError
		case "HL7_VERSION_MISMATCH", "HL7_REQUIRED", "HL7_NOT_USED", "HL7_LENGTH",
			"HL7_UNEXPECTED_SEGMENT":
			return contracts.SeverityError
		default:
			return normal(code)
		}
	case contracts.ValidationModeLenient:
		switch code {
		case "HL7_MISSING_REQUIRED", "HL7_MESSAGE_TYPE_MISMATCH", "HL7_REPETITION_VIOLATION",
			"HL7_DATATYPE", "HL7_NOT_USED":
			return contracts.SeverityWarning
		case "HL7_VERSION_MISMATCH", "HL7_REQUIRED", "HL7_LENGTH",
			"HL7_UNEXPECTED_SEGMENT", "HL7_EXTRA_FIELD", "HL7_EXTRA_COMPONENT", "HL7_EXTRA_SUBCOMPONENT":
			return contracts.SeverityInfo
		default:
			return normal(code)
		}
	default:
		return normal(code)
	}
}

func bucketize(issues *[]contracts.ValidationIssue, err contracts.ValidationError, mode contracts.ValidationMode) contracts.Severity {
	sev := err.Severity
	if sev == "" {
		sev = resolveSeverity(err.Code, mode)
	}
	e := contracts.ValidationIssue{
		Path:     err.Path,
		Message:  err.Message,
		Code:     err.Code,
		Severity: sev,
	}
	*issues = append(*issues, e)
	return sev
}

func splitBuckets(all []contracts.ValidationIssue) (errs, warns, infos []contracts.ValidationIssue) {
	for _, e := range all {
		switch e.Severity {
		case contracts.SeverityInfo:
			infos = append(infos, e)
		case contracts.SeverityWarning:
			warns = append(warns, e)
		default:
			errs = append(errs, e)
		}
	}
	return errs, warns, infos
}

// HL7SchemaProcessor implements SchemaProcessor for HL7 v2.x messages.
type HL7SchemaProcessor struct{}

// NewHL7SchemaProcessor returns a new HL7 schema processor.
func NewHL7SchemaProcessor() *HL7SchemaProcessor {
	return &HL7SchemaProcessor{}
}

// Type implements contracts.SchemaProcessor.
func (p *HL7SchemaProcessor) Type() string {
	return string(contracts.FormatHL7)
}

// ParseSchema implements contracts.SchemaProcessor.
func (p *HL7SchemaProcessor) ParseSchema(definition []byte) (contracts.CompiledSchema, error) {
	compiled, err := ParseHL7Schema(definition)
	if err != nil {
		return nil, fmt.Errorf("hl7 schema parse error: %w", err)
	}
	reg, err := datatypes.GetRegistry()
	if err != nil {
		return nil, fmt.Errorf("hl7 datatype registry load error: %w", err)
	}
	return &CompiledHL7Schema{Schema: compiled, Registry: reg}, nil
}

// Process implements contracts.SchemaProcessor. Validation only; Data is the original input.
func (p *HL7SchemaProcessor) Process(inputData []byte, compiled contracts.CompiledSchema, opts contracts.ProcessOptions) (*contracts.ProcessResult, error) {
	c, ok := compiled.(*CompiledHL7Schema)
	if !ok {
		return nil, fmt.Errorf("expected *hl7.CompiledHL7Schema, got %T", compiled)
	}
	mode := effectiveMode(opts)
	msg, err := ParseMessage(inputData)
	if err != nil {
		code := "HL7_INVALID_MSH"
		if err == ErrEmptyMessage {
			code = "HL7_EMPTY_MESSAGE"
		}
		issue := contracts.ValidationIssue{
			Path:     "message",
			Message:  err.Error(),
			Code:     code,
			Severity: resolveSeverity(code, mode),
		}
		result := &contracts.ProcessResult{
			Valid:  false,
			Data:   inputData,
			Errors: []contracts.ValidationIssue{issue},
		}
		if opts.StrictValidation {
			return result, fmt.Errorf("%s", (&contracts.ValidationResult{Valid: false, Errors: result.Errors}).ErrorMessage())
		}
		return result, nil
	}
	match := MatchMessage(msg, c)
	var all []contracts.ValidationIssue
	for _, e := range match.Errors {
		sev := bucketize(&all, contracts.ValidationError{Path: e.Path, Message: e.Message, Code: e.Code}, mode)
		if !opts.CollectAllErrors && sev == contracts.SeverityError {
			errs, warns, infos := splitBuckets(all)
			return &contracts.ProcessResult{Valid: false, Data: inputData, Errors: errs, Warnings: warns, Infos: infos}, nil
		}
	}
	for _, e := range ValidateMessageTypeAndVersion(msg, c.Schema) {
		sev := bucketize(&all, contracts.ValidationError{Path: e.Path, Message: e.Message, Code: e.Code}, mode)
		if !opts.CollectAllErrors && sev == contracts.SeverityError {
			errs, warns, infos := splitBuckets(all)
			return &contracts.ProcessResult{Valid: false, Data: inputData, Errors: errs, Warnings: warns, Infos: infos}, nil
		}
	}
	fieldErrs := ValidateMatchResult(match, msg, true, c.Registry)
	for _, e := range fieldErrs {
		sev := bucketize(&all, contracts.ValidationError{Path: e.Path, Message: e.Message, Code: e.Code}, mode)
		if !opts.CollectAllErrors && sev == contracts.SeverityError {
			break
		}
	}
	errs, warns, infos := splitBuckets(all)
	valid := len(errs) == 0
	result := &contracts.ProcessResult{Valid: valid, Data: inputData, Errors: errs, Warnings: warns, Infos: infos}
	if !valid && opts.StrictValidation {
		return result, fmt.Errorf("%s", (&contracts.ValidationResult{Valid: false, Errors: errs}).ErrorMessage())
	}
	return result, nil
}
