package hl7

import (
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

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
	return &CompiledHL7Schema{Schema: compiled}, nil
}

// Process implements contracts.SchemaProcessor.
// Note: ProcessResult.Data always contains the original raw HL7 input bytes unchanged.
// HL7 processing is validation-only; no transformation or structuring is applied.
func (p *HL7SchemaProcessor) Process(inputData []byte, compiled contracts.CompiledSchema, opts contracts.ProcessOptions) (*contracts.ProcessResult, error) {
	c, ok := compiled.(*CompiledHL7Schema)
	if !ok {
		return nil, fmt.Errorf("expected *hl7.CompiledHL7Schema, got %T", compiled)
	}
	msg, err := ParseMessage(inputData)
	if err != nil {
		// Map specific parse errors to distinct error codes so callers can
		// distinguish "message was empty" (HL7_EMPTY_MESSAGE) from "message
		// does not start with a valid MSH segment" (HL7_INVALID_MSH). (BUG-22)
		code := "HL7_INVALID_MSH"
		if err == ErrEmptyMessage {
			code = "HL7_EMPTY_MESSAGE"
		}
		return &contracts.ProcessResult{
			Valid:  false,
			Data:   inputData,
			Errors: []contracts.ValidationError{{Path: "message", Message: err.Error(), Code: code}},
		}, nil
	}
	match := MatchMessage(msg, c)
	var allErrs []contracts.ValidationError
	// When CollectAllErrors is false, stop at the very first error across ALL phases
	// (structural, type/version, and field-level). Previously, structural errors from
	// MatchMessage were always appended in full before CollectAllErrors was consulted,
	// meaning two missing required segments produced two errors even with CollectAllErrors=false.
	for _, e := range match.Errors {
		allErrs = append(allErrs, contracts.ValidationError{Path: e.Path, Message: e.Message, Code: e.Code})
		if !opts.CollectAllErrors {
			return &contracts.ProcessResult{Valid: false, Data: inputData, Errors: allErrs}, nil
		}
	}
	for _, e := range ValidateMessageTypeAndVersion(msg, c.Schema) {
		allErrs = append(allErrs, contracts.ValidationError{Path: e.Path, Message: e.Message, Code: e.Code})
		if !opts.CollectAllErrors {
			return &contracts.ProcessResult{Valid: false, Data: inputData, Errors: allErrs}, nil
		}
	}
	fieldErrs := ValidateMatchResult(match, msg, opts.CollectAllErrors, opts.AllowExtraFields)
	for _, e := range fieldErrs {
		allErrs = append(allErrs, contracts.ValidationError{Path: e.Path, Message: e.Message, Code: e.Code})
	}
	valid := len(allErrs) == 0
	result := &contracts.ProcessResult{Valid: valid, Data: inputData, Errors: allErrs}
	if !valid && opts.StrictValidation {
		return result, fmt.Errorf("%s", (&contracts.ValidationResult{Valid: false, Errors: allErrs}).ErrorMessage())
	}
	return result, nil
}
