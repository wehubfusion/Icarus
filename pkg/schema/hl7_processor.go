package schema

import (
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7"
)

// compiledHL7Schema wraps hl7.CompiledHL7Schema to implement CompiledSchema.
type compiledHL7Schema struct {
	inner *hl7.CompiledHL7Schema
}

func (c *compiledHL7Schema) SchemaType() string {
	return string(FormatHL7)
}

// HL7SchemaProcessor implements SchemaProcessor for HL7 v2.x messages.
type HL7SchemaProcessor struct{}

// NewHL7SchemaProcessor returns a new HL7 schema processor.
func NewHL7SchemaProcessor() *HL7SchemaProcessor {
	return &HL7SchemaProcessor{}
}

// Type implements SchemaProcessor.
func (p *HL7SchemaProcessor) Type() string {
	return string(FormatHL7)
}

// ParseSchema implements SchemaProcessor.
func (p *HL7SchemaProcessor) ParseSchema(definition []byte) (CompiledSchema, error) {
	compiled, err := hl7.ParseHL7Schema(definition)
	if err != nil {
		return nil, fmt.Errorf("hl7 schema parse error: %w", err)
	}
	return &compiledHL7Schema{inner: &hl7.CompiledHL7Schema{Schema: compiled}}, nil
}

// Process implements SchemaProcessor.
func (p *HL7SchemaProcessor) Process(inputData []byte, compiled CompiledSchema, opts ProcessOptions) (*ProcessResult, error) {
	c, ok := compiled.(*compiledHL7Schema)
	if !ok {
		return nil, fmt.Errorf("expected *compiledHL7Schema, got %T", compiled)
	}
	msg, err := hl7.ParseMessage(inputData)
	if err != nil {
		return &ProcessResult{
			Valid:  false,
			Data:   inputData,
			Errors: []ValidationError{{Path: "message", Message: err.Error(), Code: "HL7_INVALID_MSH"}},
		}, nil
	}
	match := hl7.MatchMessage(msg, c.inner)
	var allErrs []ValidationError
	for _, e := range match.Errors {
		allErrs = append(allErrs, ValidationError{Path: e.Path, Message: e.Message, Code: e.Code})
	}
	fieldErrs := hl7.ValidateMatchResult(match, msg, opts.CollectAllErrors)
	for _, e := range fieldErrs {
		allErrs = append(allErrs, ValidationError{Path: e.Path, Message: e.Message, Code: e.Code})
	}
	valid := len(allErrs) == 0
	result := &ProcessResult{Valid: valid, Data: inputData, Errors: allErrs}
	if !valid && opts.StrictValidation {
		return result, fmt.Errorf("%s", (&ValidationResult{Valid: false, Errors: allErrs}).ErrorMessage())
	}
	return result, nil
}
