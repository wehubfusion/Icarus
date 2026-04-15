package hl7

import (
	"fmt"
	"strings"
)

// versionsMatch returns true when a and b refer to the same HL7 version.
// Trailing ".0" segments are stripped and a leading "v"/"V" prefix is removed
// before comparing, so "2.5", "2.5.0", "v2.5", and "V2.5.0" are all
// treated as equivalent, while "2.5" and "2.5.1" are not.
// Comparison is case-insensitive. Nonsense strings without a digit (e.g. "v" alone)
// never match a real version.
func versionsMatch(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	na := normalizeVersion(a)
	nb := normalizeVersion(b)
	return strings.EqualFold(na, nb)
}

func normalizeVersion(v string) string {
	v = strings.TrimSpace(v)
	// Strip optional leading "v"/"V" only when followed by a digit (e.g. "v2.5.1", not bare "v").
	if len(v) >= 2 && (v[0] == 'v' || v[0] == 'V') {
		v = v[1:]
	}
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
