package json

import "strings"

// KnownErrorCodes is the complete set of validation codes the JSON schema processor
// can emit. It is the single source of truth for callers that need to validate or
// pre-filter CodeSeverityOverrides maps before passing them to Process.
var KnownErrorCodes = map[string]struct{}{
	"REQUIRED":        {},
	"TYPE_MISMATCH":   {},
	"MIN_ITEMS":       {},
	"MAX_ITEMS":       {},
	"DUPLICATE_ITEM":  {},
	"MIN_VALUE":       {},
	"MAX_VALUE":       {},
	"MIN_LENGTH":      {},
	"MAX_LENGTH":      {},
	"INVALID_PATTERN": {},
	"PATTERN_MISMATCH": {},
	"FORMAT_MISMATCH": {},
	"UNKNOWN_FORMAT":  {},
	"ENUM_MISMATCH":   {},
	"INVALID_UUID":    {},
	"MIN_DATE":        {},
	"MAX_DATE":        {},
	"INVALID_BASE64":  {},
}

// IsKnownErrorCode reports whether code is a valid JSON schema validation code.
// The check is case-insensitive.
func IsKnownErrorCode(code string) bool {
	_, ok := KnownErrorCodes[strings.ToUpper(strings.TrimSpace(code))]
	return ok
}
