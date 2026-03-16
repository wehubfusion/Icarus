package hl7

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	reDT  = regexp.MustCompile(`^\d{4}(\d{2}(\d{2})?)?$`)
	reTM  = regexp.MustCompile(`^\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?([+-]\d{4})?$`)
	reDTM = regexp.MustCompile(`^\d{4}(\d{2}(\d{2}(\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?)?)?)?([+-]\d{4})?$`)
	reNM  = regexp.MustCompile(`^[-+]?\d*\.?\d+$`)
	reSI  = regexp.MustCompile(`^\d+$`)
)

// ValidateMessageTypeAndVersion checks MSH-9 and MSH-12 against schema messageType and version.
func ValidateMessageTypeAndVersion(msg *Message, schema *HL7Schema) []ValidationError {
	var errs []ValidationError
	if schema == nil {
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
		if want != got {
			errs = append(errs, ValidationError{
				Path: "MSH-12", Message: fmt.Sprintf("version must be %q, got %q", want, got), Code: "HL7_VERSION_MISMATCH",
			})
		}
	}
	return errs
}

// ValidateMatchResult runs field-level validation on matched segments.
func ValidateMatchResult(match MatchResult, msg *Message, collectAll bool, allowExtraFields bool) []ValidationError {
	var errs []ValidationError
	for _, m := range match.Matched {
		if m.Segment == nil || m.SchemaDef == nil {
			continue
		}
		for fi := range m.SchemaDef.Fields {
			fdef := &m.SchemaDef.Fields[fi]
			fieldErrs := validateField(m.Segment, fdef, m.Segment.Name, msg, allowExtraFields)
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

func validateField(seg *Segment, fdef *HL7FieldDef, segName string, msg *Message, allowExtraFields bool) []ValidationError {
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
		if len(fdef.Components) == 0 {
			val := rep.String()
			if err := validatePrimitive(effectiveType, val, repPath, fdef.Length); err != nil {
				errs = append(errs, *err)
			}
		} else {
			for ci := range fdef.Components {
				cdef := &fdef.Components[ci]
				compNum := parseComponentNumberFromPosition(cdef.Position)
				if compNum <= 0 {
					continue
				}
			c, ok := rep.ComponentAt(compNum)
			cPath := fmt.Sprintf("%s.%d", repPath, compNum)
			if !ok {
				if isRequired(cdef.Usage) {
					errs = append(errs, ValidationError{
						Path: cPath, Message: "required component missing", Code: "HL7_REQUIRED",
					})
				}
				continue
			}
			usageComp := strings.ToUpper(strings.TrimSpace(cdef.Usage))
			if (usageComp == UsageNotUsed || usageComp == UsageWithdrawn) && c.String() != "" {
				errs = append(errs, ValidationError{
					Path: cPath, Message: "component must not be present or must be empty (X/W)", Code: "HL7_NOT_USED",
				})
			}
			if strings.ToUpper(strings.TrimSpace(cdef.Usage)) == UsageRequired {
				if strings.TrimLeft(c.String(), "&") == "" {
					errs = append(errs, ValidationError{
						Path: cPath, Message: "required component must be non-empty", Code: "HL7_REQUIRED",
					})
				}
			}
			if cdef.Length > 0 && utf8.RuneCountInString(c.String()) > cdef.Length {
				errs = append(errs, ValidationError{
					Path: cPath, Message: fmt.Sprintf("length %d exceeds maximum %d", utf8.RuneCountInString(c.String()), cdef.Length), Code: "HL7_LENGTH",
				})
			}
				if len(cdef.SubComponents) == 0 {
					if err := validatePrimitive(cdef.DataType, c.String(), cPath, cdef.Length); err != nil {
						errs = append(errs, *err)
					}
				} else {
					for si := range cdef.SubComponents {
						sdef := &cdef.SubComponents[si]
						subNum := parseComponentNumberFromPosition(sdef.Position)
						if subNum <= 0 {
							continue
						}
					sc, ok := c.SubcomponentAt(subNum)
					sPath := fmt.Sprintf("%s.%d", cPath, subNum)
					if !ok {
						if isRequired(sdef.Usage) {
							errs = append(errs, ValidationError{
								Path: sPath, Message: "required subcomponent missing", Code: "HL7_REQUIRED",
							})
						}
						continue
					}
				usageSub := strings.ToUpper(strings.TrimSpace(sdef.Usage))
				if (usageSub == UsageNotUsed || usageSub == UsageWithdrawn) && sc.Value != "" {
					errs = append(errs, ValidationError{
						Path: sPath, Message: "subcomponent must not be present or must be empty (X/W)", Code: "HL7_NOT_USED",
					})
				}
				if strings.ToUpper(strings.TrimSpace(sdef.Usage)) == UsageRequired && sc.Value == "" {
					errs = append(errs, ValidationError{
						Path: sPath, Message: "required subcomponent must be non-empty", Code: "HL7_REQUIRED",
					})
				}
					if sdef.Length > 0 && utf8.RuneCountInString(sc.Value) > sdef.Length {
						errs = append(errs, ValidationError{
							Path: sPath, Message: fmt.Sprintf("length %d exceeds maximum %d", utf8.RuneCountInString(sc.Value), sdef.Length), Code: "HL7_LENGTH",
						})
					}
						if err := validatePrimitive(sdef.DataType, sc.Value, sPath, sdef.Length); err != nil {
							errs = append(errs, *err)
						}
					}
				}
			}
			if !allowExtraFields {
				maxCompNum := 0
				for _, cdef := range fdef.Components {
					n := parseComponentNumberFromPosition(cdef.Position)
					if n > maxCompNum {
						maxCompNum = n
					}
				}
				for compNum := maxCompNum + 1; compNum <= len(rep.Components); compNum++ {
					cPath := fmt.Sprintf("%s.%d", repPath, compNum)
					errs = append(errs, ValidationError{
						Path: cPath, Message: "field has more components than schema allows", Code: "HL7_EXTRA_COMPONENT",
					})
				}
			}
		}
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

// validatePrimitive validates HL7 primitive/simple data types used at field or component level.
// HL7 v2.x primitives (per Ch 2.A): ST, TX, FT, NM, SI, SN, DT, TM, DTM/TS, ID, IS, MO, GTS, NR.
// Composite types (e.g. CE, CX, HD) are validated via their components; only leaf types reach here.
func validatePrimitive(dataType, value, path string, _ int) *ValidationError {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	if value == "" || value == `""` {
		return nil
	}
	switch dt {
	case "ST", "TX", "FT", "": // string types
		return nil
	case "NM":
		if !reNM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be numeric", Code: "HL7_DATATYPE"}
		}
	case "SI":
		if !reSI.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be integer", Code: "HL7_DATATYPE"}
		}
	case "SN":
		return nil
	case "DT":
		if !reDT.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be date (YYYY[MM[DD]])", Code: "HL7_DATATYPE"}
		}
		if err := validateDTCalendarRange(value, path); err != nil {
			return err
		}
	case "TM":
		if !reTM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be time (HH[MM[SS]])", Code: "HL7_DATATYPE"}
		}
		if err := validateTMRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "DTM", "TS":
		if !reDTM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be timestamp (DTM/TS format)", Code: "HL7_DATATYPE"}
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
		// MO = Quantity (NM) ^ Denomination (ID). Quantity required when present; denomination optional.
		parts := strings.SplitN(value, "^", 2)
		qty := strings.TrimSpace(parts[0])
		if qty != "" && !reNM.MatchString(qty) {
			return &ValidationError{Path: path, Message: "MO quantity must be numeric", Code: "HL7_DATATYPE"}
		}
	case "GTS":
		// General Timing Specification: follows ST formatting rules (HL7 2.A.32).
		return nil
	case "NR":
		// Numeric range: Low (NM) ^ High (NM); either component may be null for unbounded.
		parts := strings.SplitN(value, "^", 2)
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" && !reNM.MatchString(p) {
				return &ValidationError{Path: path, Message: "NR low/high value must be numeric", Code: "HL7_DATATYPE"}
			}
		}
	default:
		// Unrecognized or composite-used-as-leaf: accept to avoid breaking schemas with extended types.
	}
	return nil
}

func validateDTCalendarRange(value, path string) *ValidationError {
	if len(value) >= 6 {
		mm, _ := strconv.Atoi(value[4:6])
		if mm < 1 || mm > 12 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid month %02d in date", mm), Code: "HL7_DATATYPE"}
		}
	}
	if len(value) == 8 {
		dd, _ := strconv.Atoi(value[6:8])
		if dd < 1 || dd > 31 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid day %02d in date", dd), Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func validateTMRange(value, path string) *ValidationError {
	digits := stripTZOffset(value)
	if len(digits) >= 2 {
		hh, _ := strconv.Atoi(digits[0:2])
		if hh > 23 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid hour %02d in time", hh), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 4 {
		mm, _ := strconv.Atoi(digits[2:4])
		if mm > 59 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid minute %02d in time", mm), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 6 {
		ssPart := digits[4:6]
		ss, _ := strconv.Atoi(ssPart)
		if ss > 59 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid second %02d in time", ss), Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func validateDTMCalendarRange(value, path string) *ValidationError {
	digits := stripTZOffset(value)
	if len(digits) >= 6 {
		mm, _ := strconv.Atoi(digits[4:6])
		if mm < 1 || mm > 12 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid month %02d in timestamp", mm), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 8 {
		dd, _ := strconv.Atoi(digits[6:8])
		if dd < 1 || dd > 31 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid day %02d in timestamp", dd), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 10 {
		hh, _ := strconv.Atoi(digits[8:10])
		if hh > 23 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid hour %02d in timestamp", hh), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 12 {
		min, _ := strconv.Atoi(digits[10:12])
		if min > 59 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid minute %02d in timestamp", min), Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 14 {
		ss, _ := strconv.Atoi(digits[12:14])
		if ss > 59 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid second %02d in timestamp", ss), Code: "HL7_DATATYPE"}
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

func validateTZOffset(value, path string) *ValidationError {
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
		return &ValidationError{
			Path:    path,
			Message: fmt.Sprintf("invalid timezone offset %c%s: hours must be 00-14", value[tzIdx], tz),
			Code:    "HL7_DATATYPE",
		}
	}
	if tzM > 59 {
		return &ValidationError{
			Path:    path,
			Message: fmt.Sprintf("invalid timezone offset %c%s: minutes must be 00-59", value[tzIdx], tz),
			Code:    "HL7_DATATYPE",
		}
	}
	return nil
}
