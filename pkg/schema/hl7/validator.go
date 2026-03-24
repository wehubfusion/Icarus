package hl7

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

// ValidateMatchResult runs field-level validation on matched segments.
func ValidateMatchResult(match MatchResult, msg *Message, collectAll bool, reg *datatypes.Registry) []ValidationError {
	var errs []ValidationError
	version := ""
	if msg != nil {
		version = strings.TrimSpace(msg.Get("MSH-12"))
	}
	for _, m := range match.Matched {
		if m.Segment == nil || m.SchemaDef == nil {
			continue
		}
		for fi := range m.SchemaDef.Fields {
			fdef := &m.SchemaDef.Fields[fi]
			fieldErrs := validateField(m.Segment, fdef, m.Segment.Name, msg, reg, version)
			for _, e := range fieldErrs {
				errs = append(errs, e)
				if !collectAll {
					return errs
				}
			}
		}
		maxFieldNum := 0
		for _, fdef := range m.SchemaDef.Fields {
			n := parseFieldNumberFromPosition(fdef.Position)
			if n > maxFieldNum {
				maxFieldNum = n
			}
		}
		for fn := maxFieldNum + 1; fn <= len(m.Segment.Fields); fn++ {
			f, ok := m.Segment.FieldAt(fn)
			if !ok || isFieldEffectivelyEmpty(f) {
				continue
			}
			errs = append(errs, ValidationError{
				Path:    fmt.Sprintf("%s-%d", m.Segment.Name, fn),
				Message: "segment has more fields than schema allows",
				Code:    "HL7_EXTRA_FIELD",
			})
			if !collectAll {
				return errs
			}
		}
	}
	return errs
}

func isFieldEffectivelyEmpty(f Field) bool {
	for _, rep := range f.Repetitions {
		if repetitionHasNonEmptyValue(rep) {
			return false
		}
	}
	return true
}

func repetitionHasNonEmptyValue(rep Repetition) bool {
	for _, c := range rep.Components {
		for _, sc := range c.Subcomponents {
			if sc.Value != "" {
				return true
			}
		}
	}
	return false
}

func validateField(seg *Segment, fdef *HL7FieldDef, segName string, msg *Message, reg *datatypes.Registry, version string) []ValidationError {
	var errs []ValidationError
	fieldNum := parseFieldNumberFromPosition(fdef.Position)
	if fieldNum <= 0 {
		return errs
	}
	path := fmt.Sprintf("%s-%d", segName, fieldNum)
	f, ok := seg.FieldAt(fieldNum)
	if !ok {
		if isRequired(fdef.Usage) {
			errs = append(errs, ValidationError{
				Path: path, Message: "required field missing", Code: "HL7_MISSING_REQUIRED",
			})
		}
		return errs
	}
	usageField := strings.ToUpper(strings.TrimSpace(fdef.Usage))
	if usageField == UsageNotUsed || usageField == UsageWithdrawn {
		for _, rep := range f.Repetitions {
			if repetitionHasNonEmptyValue(rep) {
				errs = append(errs, ValidationError{
					Path: path, Message: "field must not be present or must be empty (X/W)", Code: "HL7_NOT_USED",
				})
				break // one error per field is sufficient
			}
		}
	}
	if strings.ToUpper(strings.TrimSpace(fdef.Usage)) == UsageRequired {
		if isFieldEffectivelyEmpty(f) {
			errs = append(errs, ValidationError{
				Path: path, Message: "required field must be non-empty", Code: "HL7_REQUIRED",
			})
		}
	}
	maxRep := parseRptMax(fdef.Rpt)
	if strings.TrimSpace(fdef.Rpt) != "" && maxRep > 0 && len(f.Repetitions) > maxRep {
		errs = append(errs, ValidationError{
			Path: path, Message: fmt.Sprintf("field has %d repetitions, max allowed is %s", len(f.Repetitions), fdef.Rpt), Code: "HL7_REPETITION_VIOLATION",
		})
	}
	// MSH-2 encoding length is message-defined (4 or 5 chars); skip schema length check.
	isMSH2 := segName == "MSH" && fieldNum == 2
	if fdef.Length > 0 && !isMSH2 {
		for ri, rep := range f.Repetitions {
			// Use message-specific delimiters when joining components, not the default '^'.
			val := truncateAtMarker(repetitionStringWithDelimiters(rep, msg), msg)
			if utf8.RuneCountInString(val) > fdef.Length {
				p := path
				if len(f.Repetitions) > 1 {
					p = fmt.Sprintf("%s(%d)", path, ri+1)
				}
				errs = append(errs, ValidationError{
					Path: p, Message: fmt.Sprintf("length %d exceeds maximum %d", utf8.RuneCountInString(val), fdef.Length), Code: "HL7_LENGTH",
				})
			}
		}
	}
	// TODO(terminology): fdef.TableID carries the HL7 table identifier (e.g. "0076", "0085").
	// Value-set validation against these tables requires an external terminology service.
	// When that service is available, add a check here:
	//   if fdef.TableID != nil && !terminology.IsInTable(*fdef.TableID, fieldRawValue) { … }

	effectiveType := fdef.DataType
	if strings.ToUpper(fdef.DataType) == "VARIES" {
		// KNOWN LIMITATION: VARIES fields (e.g. OBX-5) carry a runtime type declared
		// in a sibling field (e.g. OBX-2). Resolving that at validation time requires
		// passing the live message into schema validation, which is not yet done.
		// Until then, VARIES is treated as ST (free-text string) so at least length
		// checks still apply. Use CEL validateAs('OBX-5', msg('OBX-2')) for
		// proper per-message type enforcement.
		effectiveType = "ST"
	}
	for ri, rep := range f.Repetitions {
		repPath := path
		if len(f.Repetitions) > 1 {
			repPath = fmt.Sprintf("%s(%d)", path, ri+1)
		}
		errs = append(errs, validateDataType(effectiveType, rep, repPath, fdef.Length, msg, reg, version)...)
	}
	return errs
}

func parseFieldNumberFromPosition(position string) int {
	position = strings.TrimSpace(position)
	for _, sep := range []string{"-", "."} {
		if i := strings.LastIndex(position, sep); i >= 0 && i+1 < len(position) {
			tail := strings.TrimSpace(position[i+1:])
			// Compound forms like "PID-3.1" have a component suffix after the field
			// number. Strip it so Atoi sees just "3", not "3.1" (which would fail).
			if dot := strings.IndexByte(tail, '.'); dot >= 0 {
				tail = tail[:dot]
			}
			n, err := strconv.Atoi(tail)
			if err != nil || n <= 0 {
				continue // try next separator
			}
			return n
		}
	}
	n, _ := strconv.Atoi(position)
	return n
}

// truncateAtMarker removes content at and after the truncation delimiter from val.
// This implements the HL7 v2.7+ rule: content after '#' (or the configured truncation
// character) was intentionally shortened by the sender and must not be counted for
// length-conformance checks. If no truncation delimiter is configured, val is returned as-is.
func truncateAtMarker(val string, msg *Message) string {
	if msg == nil || msg.Delimiters.Truncation == 0 {
		return val
	}
	if idx := strings.IndexByte(val, msg.Delimiters.Truncation); idx >= 0 {
		return val[:idx]
	}
	return val
}

func parseComponentNumberFromPosition(position string) int {
	i := strings.LastIndex(position, ".")
	if i >= 0 && i+1 < len(position) {
		n, _ := strconv.Atoi(strings.TrimSpace(position[i+1:]))
		return n
	}
	n, _ := strconv.Atoi(position)
	return n
}
