package hl7

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

// ValidateMatchResult runs field-level validation on matched segments.
func ValidateMatchResult(match MatchResult, msg *Message, collectAll bool, allowExtraFields bool, reg *datatypes.Registry) []ValidationError {
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
			fieldErrs := validateField(m.Segment, fdef, m.Segment.Name, msg, allowExtraFields, reg, version)
			for _, e := range fieldErrs {
				errs = append(errs, e)
				if !collectAll {
					return errs
				}
			}
		}
		if !allowExtraFields {
			maxFieldNum := 0
			for _, fdef := range m.SchemaDef.Fields {
				n := parseFieldNumberFromPosition(fdef.Position)
				if n > maxFieldNum {
					maxFieldNum = n
				}
			}
			for fn := maxFieldNum + 1; fn <= len(m.Segment.Fields); fn++ {
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
	}
	return errs
}

func validateField(seg *Segment, fdef *HL7FieldDef, segName string, msg *Message, allowExtraFields bool, reg *datatypes.Registry, version string) []ValidationError {
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
			if rep.String() != "" {
				errs = append(errs, ValidationError{
					Path: path, Message: "field must not be present or must be empty (X/W)", Code: "HL7_NOT_USED",
				})
				break // one error per field is sufficient
			}
		}
	}
	if strings.ToUpper(strings.TrimSpace(fdef.Usage)) == UsageRequired {
		stripped := strings.TrimLeft(f.String(), "^&~")
		if stripped == "" {
			errs = append(errs, ValidationError{
				Path: path, Message: "required field must be non-empty", Code: "HL7_REQUIRED",
			})
		}
	}
	maxRep := parseRptMax(fdef.Rpt)
	if maxRep > 0 && len(f.Repetitions) > maxRep {
		errs = append(errs, ValidationError{
			Path: path, Message: fmt.Sprintf("field has %d repetitions, max allowed is %s", len(f.Repetitions), fdef.Rpt), Code: "HL7_REPETITION_VIOLATION",
		})
	}
	// MSH-2 encoding length is message-defined (4 or 5 chars); skip schema length check.
	isMSH2 := segName == "MSH" && fieldNum == 2
	if fdef.Length > 0 && !isMSH2 {
		for ri, rep := range f.Repetitions {
			val := rep.String()
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
	// tableId/value-set validation requires an external terminology service and is not implemented.

	effectiveType := fdef.DataType
	if strings.ToUpper(fdef.DataType) == "VARIES" {
		effectiveType = ResolveVariesDataType(segName, strconv.Itoa(fieldNum), seg, msg)
		if effectiveType == "" {
			effectiveType = "ST" // fallback to string
		}
	}
	for ri, rep := range f.Repetitions {
		repPath := path
		if len(f.Repetitions) > 1 {
			repPath = fmt.Sprintf("%s(%d)", path, ri+1)
		}
		errs = append(errs, validateDataType(effectiveType, rep, repPath, fdef.Length, msg, reg, version, allowExtraFields)...)
	}
	return errs
}

func parseFieldNumberFromPosition(position string) int {
	position = strings.TrimSpace(position)
	for _, sep := range []string{"-", "."} {
		if i := strings.LastIndex(position, sep); i >= 0 && i+1 < len(position) {
			n, _ := strconv.Atoi(strings.TrimSpace(position[i+1:]))
			return n
		}
	}
	n, _ := strconv.Atoi(position)
	return n
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
