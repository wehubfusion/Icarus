// Package primitive validates HL7 primitive datatypes without depending on pkg/schema/hl7,
// so callers like pkg/cel/hl7 can avoid import cycles.
package primitive

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

var (
	reDT  = regexp.MustCompile(`^\d{4}(\d{2}(\d{2})?)?$`)
	reTM  = regexp.MustCompile(`^\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?([+-]\d{4})?$`)
	reDTM = regexp.MustCompile(`^\d{4}(\d{2}(\d{2}(\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?)?)?)?([+-]\d{4})?$`)
	reNM  = regexp.MustCompile(`^[-+]?\d*\.?\d+$`)
	reSI  = regexp.MustCompile(`^\d+$`)
)

// FieldError describes a primitive datatype validation failure (same shape as hl7.ValidationError).
type FieldError struct {
	Path, Message, Code string
}

func (e *FieldError) Error() string { return e.Message }

// ValidatePrimitive validates HL7 primitive/simple data types used at field or component level.
// HL7 v2.x primitives (per Ch 2.A): ST, TX, FT, NM, SI, SN, DT, TM, DTM/TS, ID, IS, MO, GTS, NR.
func ValidatePrimitive(dataType, value, path string, _ int) *FieldError {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	if value == "" || value == `""` {
		return nil
	}
	switch dt {
	case "ST", "TX", "FT", "": // string types
		return nil
	case "NM":
		if !reNM.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be numeric", Code: "HL7_DATATYPE"}
		}
	case "SI":
		if !reSI.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be integer", Code: "HL7_DATATYPE"}
		}
	case "SN":
		return nil
	case "DT":
		if !reDT.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be date (YYYY[MM[DD]])", Code: "HL7_DATATYPE"}
		}
		if err := validateDTCalendarRange(value, path); err != nil {
			return err
		}
	case "TM":
		if !reTM.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be time (HH[MM[SS]])", Code: "HL7_DATATYPE"}
		}
		if err := validateTMRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "DTM", "TS":
		if !reDTM.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be timestamp (DTM/TS format)", Code: "HL7_DATATYPE"}
		}
		if err := validateDTMCalendarRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "ID", "IS":
		return nil
	case "MO":
		parts := strings.SplitN(value, "^", 2)
		qty := strings.TrimSpace(parts[0])
		if qty != "" && !reNM.MatchString(qty) {
			return &FieldError{Path: path, Message: "MO quantity must be numeric", Code: "HL7_DATATYPE"}
		}
	case "GTS":
		return nil
	case "NR":
		parts := strings.SplitN(value, "^", 2)
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" && !reNM.MatchString(p) {
				return &FieldError{Path: path, Message: "NR low/high value must be numeric", Code: "HL7_DATATYPE"}
			}
		}
	default:
		// Unrecognized or composite-used-as-leaf: accept
	}
	return nil
}

// ValidatePrimitiveType reports whether value conforms to the primitive HL7 datatype.
// Empty values and VARIES are treated as valid. Composite types skip leaf primitive checks.
func ValidatePrimitiveType(typeID string, value string, reg *datatypes.Registry, version string) bool {
	tid := strings.ToUpper(strings.TrimSpace(typeID))
	if value == "" || value == `""` {
		return true
	}
	if tid == "" || tid == "VARIES" {
		return true
	}
	if reg != nil {
		if def, ok := reg.Lookup(version, tid); ok && def != nil && def.IsComposite {
			return true
		}
	}
	return ValidatePrimitive(tid, value, "", 0) == nil
}

func validateDTCalendarRange(value, path string) *FieldError {
	if len(value) >= 6 {
		mm, _ := strconv.Atoi(value[4:6])
		if mm < 1 || mm > 12 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid month %02d in date", mm), Code: "HL7_DATATYPE"}
		}
	}
	if len(value) == 8 {
		dd, _ := strconv.Atoi(value[6:8])
		if dd < 1 || dd > 31 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid day %02d in date", dd), Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func validateTMRange(value, path string) *FieldError {
	digits := stripTZOffset(value)
	if len(digits) >= 2 {
		hh, _ := strconv.Atoi(digits[0:2])
		if hh > 23 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid hour %02d in time", hh), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 4 {
		mm, _ := strconv.Atoi(digits[2:4])
		if mm > 59 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid minute %02d in time", mm), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 6 {
		ssPart := digits[4:6]
		ss, _ := strconv.Atoi(ssPart)
		if ss > 59 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid second %02d in time", ss), Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func validateDTMCalendarRange(value, path string) *FieldError {
	digits := stripTZOffset(value)
	if len(digits) >= 6 {
		mm, _ := strconv.Atoi(digits[4:6])
		if mm < 1 || mm > 12 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid month %02d in timestamp", mm), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 8 {
		dd, _ := strconv.Atoi(digits[6:8])
		if dd < 1 || dd > 31 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid day %02d in timestamp", dd), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 10 {
		hh, _ := strconv.Atoi(digits[8:10])
		if hh > 23 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid hour %02d in timestamp", hh), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 12 {
		min, _ := strconv.Atoi(digits[10:12])
		if min > 59 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid minute %02d in timestamp", min), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 14 {
		ss, _ := strconv.Atoi(digits[12:14])
		if ss > 59 {
			return &FieldError{Path: path, Message: fmt.Sprintf("invalid second %02d in timestamp", ss), Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func stripTZOffset(value string) string {
	s := value
	for _, sign := range []byte{'+', '-'} {
		if idx := strings.LastIndexByte(s, sign); idx > 0 {
			s = s[:idx]
			break
		}
	}
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		s = s[:idx]
	}
	return s
}

func validateTZOffset(value, path string) *FieldError {
	var tzIdx int = -1
	for i := len(value) - 5; i >= 1; i-- {
		if value[i] == '+' || value[i] == '-' {
			tzIdx = i
			break
		}
	}
	if tzIdx < 0 {
		return nil
	}
	tz := value[tzIdx+1:]
	if len(tz) != 4 {
		return nil
	}
	tzH, err1 := strconv.Atoi(tz[0:2])
	tzM, err2 := strconv.Atoi(tz[2:4])
	if err1 != nil || err2 != nil {
		return nil
	}
	if tzH > 14 || (tzH == 14 && tzM > 0) {
		return &FieldError{
			Path:    path,
			Message: fmt.Sprintf("invalid timezone offset %c%s: hours must be 00-14", value[tzIdx], tz),
			Code:    "HL7_DATATYPE",
		}
	}
	if tzM > 59 {
		return &FieldError{
			Path:    path,
			Message: fmt.Sprintf("invalid timezone offset %c%s: minutes must be 00-59", value[tzIdx], tz),
			Code:    "HL7_DATATYPE",
		}
	}
	return nil
}
