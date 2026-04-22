package hl7

import (
	"fmt"
	"strings"

	celhl7 "github.com/wehubfusion/Icarus/pkg/cel/hl7"
	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
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
	reg, err := datatypes.GetRegistry()
	if err != nil {
		return nil, fmt.Errorf("hl7 datatype registry load error: %w", err)
	}
	out := &CompiledHL7Schema{Schema: compiled, Registry: reg}
	if len(compiled.Rules) == 0 {
		return out, nil
	}
	eng, err := celhl7.Engine()
	if err != nil {
		return nil, fmt.Errorf("cel hl7 engine: %w", err)
	}
	rules, err := celhl7.CompileHL7Rules(eng, compiled.Rules)
	if err != nil {
		return nil, fmt.Errorf("hl7 cel rules compile: %w", err)
	}
	out.CELValidation = &CompiledCELValidation{Engine: eng, Rules: rules}
	return out, nil
}

// Process implements contracts.SchemaProcessor. Validation only; Data is the original input.
//
// The function runs in two phases:
//  1. Collect all raw validation issues from every source (segment matching,
//     header/version checks, field validation, CEL rules) into a flat list.
//  2. Apply opts.CodeSeverityOverrides to every raw issue in a single pass,
//     routing each into the correct severity bucket or dropping it entirely
//     when the resolved severity is DROP.
func (p *HL7SchemaProcessor) Process(inputData []byte, compiled contracts.CompiledSchema, opts contracts.ProcessOptions) (*contracts.ProcessResult, error) {
	c, ok := compiled.(*CompiledHL7Schema)
	if !ok {
		return nil, fmt.Errorf("expected *hl7.CompiledHL7Schema, got %T", compiled)
	}

	// ── parse-error fast path ────────────────────────────────────────────────
	// Invalid/empty MSH is treated as a single raw issue; we still honour the
	// override map for it before returning early.
	msg, err := ParseMessage(inputData)
	if err != nil {
		code := "HL7_INVALID_MSH"
		if err == ErrEmptyMessage {
			code = "HL7_EMPTY_MESSAGE"
		}
		raw := []contracts.ValidationIssue{
			{Path: "message", Message: err.Error(), Code: code},
		}
		errs, warns, infos := contracts.ApplyAndBucket(raw, opts.CodeSeverityOverrides)
		valid := len(errs) == 0
		result := &contracts.ProcessResult{Valid: valid, Data: inputData, Errors: errs, Warnings: warns, Infos: infos}
		return result, nil
	}

	// ── phase 1: collect all raw issues ─────────────────────────────────────
	var raw []contracts.ValidationIssue

	match := MatchMessage(msg, c)
	for _, e := range match.Errors {
		raw = append(raw, contracts.ValidationIssue{Path: e.Path, Message: e.Message, Code: e.Code})
	}

	for _, e := range ValidateMessageTypeAndVersion(msg, c.Schema) {
		raw = append(raw, contracts.ValidationIssue{Path: e.Path, Message: e.Message, Code: e.Code})
	}

	for _, e := range ValidateMatchResult(match, msg, opts.CollectAllErrors, c.Registry) {
		raw = append(raw, contracts.ValidationIssue{Path: e.Path, Message: e.Message, Code: e.Code})
	}

	if cv := c.CELValidation; cv != nil && len(cv.Rules) > 0 {
		iter := &celhl7.HL7ScopeIterator{Msg: msg, Reg: c.Registry}
		violations, evalErrs := cv.Engine.EvaluateRules(cv.Rules, iter)

		for _, v := range violations {
			// CEL rule violations carry their own severity; preserve it so that
			// per-rule severity takes precedence over the override map.
			raw = append(raw, contracts.ValidationIssue{
				Path:     v.Path,
				Message:  v.Message,
				Code:     "HL7_CUSTOM_RULE_VIOLATION",
				Severity: celSeverity(v.Severity),
			})
		}

		// Eval errors use the iterator HL7 path when available (e.g. REL[1]-5);
		// otherwise rule[id].expr so the failure is still attributable.
		for _, e := range evalErrs {
			path := strings.TrimSpace(e.Path)
			if path == "" {
				path = fmt.Sprintf("rule[%s].%s", e.RuleID, e.Expr)
			}
			errMsg := e.Err.Error()
			if e.RuleName != "" {
				errMsg = fmt.Sprintf("%s: %s", e.RuleName, errMsg)
			}
			raw = append(raw, contracts.ValidationIssue{
				Path:     path,
				Message:  errMsg,
				Code:     "HL7_CUSTOM_RULE_RUNTIME_ERROR",
				Severity: celRuntimeIssueSeverity(e.RuleSeverity),
			})
		}
	}

	// ── phase 2: apply overrides and bucket ──────────────────────────────────
	errs, warns, infos := contracts.ApplyAndBucket(raw, opts.CodeSeverityOverrides)
	valid := len(errs) == 0
	result := &contracts.ProcessResult{Valid: valid, Data: inputData, Errors: errs, Warnings: warns, Infos: infos}
	return result, nil
}

// celSeverity converts the string severity stored on a CEL Violation to the
// contracts.Severity type used throughout the processor pipeline.
func celSeverity(s string) contracts.Severity {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "WARNING":
		return contracts.SeverityWarning
	case "INFO":
		return contracts.SeverityInfo
	default:
		return contracts.SeverityError
	}
}

// celRuntimeIssueSeverity maps a rule's configured severity to the value stored
// on a raw HL7_CUSTOM_RULE_RUNTIME_ERROR issue. An empty string means "defer to
// the override map / ERROR default" (handled in contracts.ApplyAndBucket).
func celRuntimeIssueSeverity(ruleSeverity string) contracts.Severity {
	if strings.TrimSpace(ruleSeverity) == "" {
		return ""
	}
	return celSeverity(ruleSeverity)
}
