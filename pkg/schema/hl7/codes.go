package hl7

import "strings"

// KnownErrorCodes is the complete set of HL7 error codes the engine can emit.
// It is the single source of truth for any caller that needs to validate or filter
// code-severity override maps before passing them to Process.
var KnownErrorCodes = map[string]struct{}{
	"HL7_EMPTY_MESSAGE":             {},
	"HL7_INVALID_MSH":               {},
	"HL7_INVALID_SCHEMA":            {},
	"HL7_INVALID_MESSAGE":           {},
	"HL7_MISSING_REQUIRED":          {},
	"HL7_MESSAGE_TYPE_MISMATCH":     {},
	"HL7_REPETITION_VIOLATION":      {},
	"HL7_DATATYPE":                  {},
	"HL7_NOT_USED":                  {},
	"HL7_VERSION_MISMATCH":          {},
	"HL7_REQUIRED":                  {},
	"HL7_LENGTH":                    {},
	"HL7_UNEXPECTED_SEGMENT":        {},
	"HL7_EXTRA_FIELD":               {},
	"HL7_EXTRA_COMPONENT":           {},
	"HL7_EXTRA_SUBCOMPONENT":        {},
	"HL7_CUSTOM_RULE_VIOLATION":     {},
	"HL7_CUSTOM_RULE_RUNTIME_ERROR": {},
}

// IsKnownErrorCode reports whether code is a valid HL7 engine error code.
// The check is case-insensitive.
func IsKnownErrorCode(code string) bool {
	_, ok := KnownErrorCodes[strings.ToUpper(strings.TrimSpace(code))]
	return ok
}
