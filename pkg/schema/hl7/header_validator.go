package hl7

import (
	"fmt"
	"strings"
)

// versionsMatch returns true when a and b refer to the same HL7 version.
// Trailing ".0" segments are stripped before comparing so that "2.5" and
// "2.5.0" are treated as equivalent, while "2.5" and "2.5.1" are not.
// Comparison is case-insensitive.
func versionsMatch(a, b string) bool {
	return strings.EqualFold(normalizeVersion(a), normalizeVersion(b))
}

func normalizeVersion(v string) string {
	for strings.HasSuffix(v, ".0") {
		v = strings.TrimSuffix(v, ".0")
	}
	return v
}

// ValidateMessageTypeAndVersion checks MSH-9 and MSH-12 against schema messageType and version.
// Enforcement is lenient: if schema.MessageType or schema.Version is empty, the corresponding check is skipped.
func ValidateMessageTypeAndVersion(msg *Message, schema *HL7Schema) []ValidationError {
	var errs []ValidationError
	if schema == nil || msg == nil {
		return errs
	}
	if schema.MessageType != "" {
		want := strings.TrimSpace(schema.MessageType)
		got := strings.TrimSpace(msg.Get("MSH-9"))
		normalized := got
		if idx := strings.Index(got, "^"); idx >= 0 {
			part1 := strings.TrimSpace(got[:idx])
			rest := strings.TrimSpace(got[idx+1:])
			part2 := rest
			if idx2 := strings.Index(rest, "^"); idx2 >= 0 {
				part2 = strings.TrimSpace(rest[:idx2])
			}
			normalized = part1 + "_" + part2
		}
		if !strings.EqualFold(want, normalized) {
			errs = append(errs, ValidationError{
				Path: "MSH-9", Message: fmt.Sprintf("message type must be %q, got %q", want, got), Code: "HL7_MESSAGE_TYPE_MISMATCH",
			})
		}
	}
	if schema.Version != "" {
		want := strings.TrimSpace(schema.Version)
		got := strings.TrimSpace(msg.Get("MSH-12"))
		if !versionsMatch(want, got) {
			errs = append(errs, ValidationError{
				Path: "MSH-12", Message: fmt.Sprintf("version must be %q, got %q", want, got), Code: "HL7_VERSION_MISMATCH",
			})
		}
	}
	return errs
}
