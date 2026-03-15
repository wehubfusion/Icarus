package hl7

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Pre-compiled regexes for HL7 primitive datatype validation (per HL7 v2.5.1).
var (
	// DT: YYYY[MM[DD]] — structural match; calendar range validated separately.
	reDT = regexp.MustCompile(`^\d{4}(\d{2}(\d{2})?)?$`)
	// TM: HH[MM[SS[.s]]][+/-ZZZZ] — structural match; range validated separately.
	reTM = regexp.MustCompile(`^\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?([+-]\d{4})?$`)
	// DTM/TS: YYYY[MM[DD[HH[MM[SS[.s]]]]]][+/-ZZZZ] — structural match; range validated separately.
	reDTM = regexp.MustCompile(`^\d{4}(\d{2}(\d{2}(\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?)?)?)?([+-]\d{4})?$`)
	reNM  = regexp.MustCompile(`^[-+]?\d*\.?\d+$`) // NM: numeric per HL7 §2.A.46 — no leading/trailing whitespace
	reSI  = regexp.MustCompile(`^\d+$`)            // SI: Sequence ID per HL7 §2.A.71 — non-negative integer, no sign/whitespace
)

// ValidateMessageTypeAndVersion checks MSH-9 and MSH-12 against schema messageType and version when set.
// Returns 0–2 errors (HL7_MESSAGE_TYPE_MISMATCH, HL7_VERSION_MISMATCH) when schema has non-empty values and message differs.
func ValidateMessageTypeAndVersion(msg *Message, schema *HL7Schema) []ValidationError {
	var errs []ValidationError
	if schema == nil {
		return errs
	}
	if schema.MessageType != "" {
		want := strings.TrimSpace(schema.MessageType)
		got := strings.TrimSpace(msg.Get("MSH-9"))
		// MSH-9 is sent as MSG.1^MSG.2^MSG.3 (e.g. "ADT^A01^ADT_A01"); schema messageType is typically "ADT_A01"
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

// ValidateMatchResult runs field-level validation on matched segments and returns all errors.
// When allowExtraFields is true, HL7_EXTRA_FIELD and HL7_EXTRA_COMPONENT are not reported.
// When collectAll is false, validation stops at the very first error (one error returned).
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
		// Strict: message segment has more fields than schema defines (skip if allowExtraFields)
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

// validateField validates one field definition against the segment.
// When allowExtraFields is true, HL7_EXTRA_COMPONENT is not reported.
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
	// X: field must be absent or empty across ALL repetitions.
	// Using f.String() (rep[0] only) would miss data in rep[1+].
	if strings.ToUpper(strings.TrimSpace(fdef.Usage)) == UsageNotUsed {
		for _, rep := range f.Repetitions {
			if rep.String() != "" {
				errs = append(errs, ValidationError{
					Path: path, Message: "field must not be present or must be empty (X)", Code: "HL7_NOT_USED",
				})
				break // one error per field is sufficient
			}
		}
	}
	// R: must be present and non-empty.
	// RE (Required But May Be Empty): value may be empty — no HL7_REQUIRED when empty.
	// For composite fields, f.String() returns e.g. "^^^" when all components are empty.
	// We strip all component/subcomponent delimiters before the emptiness check so that
	// a field containing only separators is treated as effectively empty. (BUG-21)
	if strings.ToUpper(strings.TrimSpace(fdef.Usage)) == UsageRequired {
		stripped := strings.TrimLeft(f.String(), "^&~")
		if stripped == "" {
			errs = append(errs, ValidationError{
				Path: path, Message: "required field must be non-empty", Code: "HL7_REQUIRED",
			})
		}
	}
	// Repetition count vs rpt
	maxRep := parseRptMax(fdef.Rpt)
	if maxRep > 0 && len(f.Repetitions) > maxRep {
		errs = append(errs, ValidationError{
			Path: path, Message: fmt.Sprintf("field has %d repetitions, max allowed is %s", len(f.Repetitions), fdef.Rpt), Code: "HL7_REPETITION_VIOLATION",
		})
	}
	// Length (0 = unconstrained); HL7 length is in characters, not bytes.
	// MSH-2 (encoding characters) is exempt from length checks: its valid length is either
	// 4 (v2.1–2.6) or 5 (v2.7+ with truncation char), determined by the message, not
	// the schema.  Applying a schema-level length constraint against MSH-2 produces false
	// positives when v2.7+ messages are validated against a v2.5 schema. (BUG-24)
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
	// TODO: tableId — no terminology service for now; skip table/value-set validation
	// When a terminology service is available, validate field value against tableId here.

	// Datatype validation (including VARIES resolved via VariesResolver; pass current segment so e.g. OBX-5 uses OBX-2 from same segment)
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
			// Primitive or single value
			val := rep.String()
			if err := validatePrimitive(effectiveType, val, repPath, fdef.Length); err != nil {
				errs = append(errs, *err)
			}
		} else {
			// Composite: validate each component
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
			// X: component must be absent or empty (not used). (BUG-27)
			if strings.ToUpper(strings.TrimSpace(cdef.Usage)) == UsageNotUsed && c.String() != "" {
				errs = append(errs, ValidationError{
					Path: cPath, Message: "component must not be present or must be empty (X)", Code: "HL7_NOT_USED",
				})
			}
			// R component present but empty.
			// c.String() for a component whose wire value is "&&" (all subcomponent separators)
			// returns "&&" ≠ "", so a bare TrimSpace check misses it. Strip leading
			// subcomponent separators before the emptiness test — same pattern as BUG-21. (BUG-26)
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
				// X: subcomponent must be absent or empty (not used). (BUG-27)
				if strings.ToUpper(strings.TrimSpace(sdef.Usage)) == UsageNotUsed && sc.Value != "" {
					errs = append(errs, ValidationError{
						Path: sPath, Message: "subcomponent must not be present or must be empty (X)", Code: "HL7_NOT_USED",
					})
				}
				// R subcomponent present but empty
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
			// Strict: message has more components than schema defines (by max schema position, not count); skip if allowExtraFields
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
	// "PID-3" or "PID.3" or "MSH-9"
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
	// "CX.1" -> 1, "HD.1" -> 1
	i := strings.LastIndex(position, ".")
	if i >= 0 && i+1 < len(position) {
		n, _ := strconv.Atoi(strings.TrimSpace(position[i+1:]))
		return n
	}
	n, _ := strconv.Atoi(position)
	return n
}

func validatePrimitive(dataType, value, path string, _ int) *ValidationError {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	// HL7 §2.5.3.4: two consecutive double-quotes (`""`) denote explicit null (delete/erase semantics).
	// Treat them as an empty value — no further datatype validation applies.
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
	case "SN": // Structured Numeric: [comparator] num [/ num2]; accept non-empty (full format would need parser)
		return nil
	case "DT": // date YYYY, YYYYMM, or YYYYMMDD per HL7 §2.A.21
		if !reDT.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be date (YYYY[MM[DD]])", Code: "HL7_DATATYPE"}
		}
		if err := validateDTCalendarRange(value, path); err != nil {
			return err
		}
	case "TM": // time HH[MM[SS[.s]]][+/-ZZZZ] per HL7 §2.A.75
		if !reTM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be time (HH[MM[SS]])", Code: "HL7_DATATYPE"}
		}
		if err := validateTMRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "DTM", "TS": // DTM/TS: full timestamp YYYY[MM[DD[HH[MM[SS[.s]]]]]][+/-ZZZZ]
		if !reDTM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be timestamp (DTM/TS format)", Code: "HL7_DATATYPE"}
		}
		if err := validateDTMCalendarRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "ID", "IS": // coded values; table validation requires external terminology service
		return nil
	default:
		// unknown type: accept
	}
	return nil
}

// validateDTCalendarRange checks that YYYY[MM[DD]] has valid month (01-12) and day (01-31) ranges.
// Does not validate day-in-month correctness (e.g. Feb 30) as that requires leap-year logic.
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

// validateTMRange checks that HH[MM[SS...]][±ZZZZ] has valid hour (00-23), minute (00-59), second (00-59) ranges.
// Strip timezone offset before parsing.
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
		// Extract SS (before any fractional seconds)
		ssPart := digits[4:6]
		ss, _ := strconv.Atoi(ssPart)
		if ss > 59 {
			return &ValidationError{Path: path, Message: fmt.Sprintf("invalid second %02d in time", ss), Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

// validateDTMCalendarRange checks calendar ranges for a DTM/TS value.
func validateDTMCalendarRange(value, path string) *ValidationError {
	digits := stripTZOffset(value)
	// Date part: YYYY[MM[DD]]
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
	// Time part: HH[MM[SS[.s]]]
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

// stripTZOffset removes a trailing [+-]DDDD timezone offset and fractional seconds from a DTM/TM value,
// returning only the digit prefix for range checking.
func stripTZOffset(value string) string {
	s := value
	// Remove trailing timezone: last [+-]DDDD
	for _, sign := range []byte{'+', '-'} {
		if idx := strings.LastIndexByte(s, sign); idx > 0 {
			s = s[:idx]
			break
		}
	}
	// Remove fractional seconds: everything from the first '.'
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		s = s[:idx]
	}
	return s
}

// validateTZOffset checks that a trailing [+-]HHMM timezone offset in a TM/DTM value has
// valid hours (00-14) and minutes (00-59). Per ISO 8601, valid offsets range from -1400 to +1400.
func validateTZOffset(value, path string) *ValidationError {
	var tzIdx int = -1
	// Find the last + or - that is the timezone sign (must be after at least 2 digit chars)
	for i := len(value) - 5; i >= 1; i-- {
		if value[i] == '+' || value[i] == '-' {
			tzIdx = i
			break
		}
	}
	if tzIdx < 0 {
		return nil // no timezone offset present
	}
	tz := value[tzIdx+1:]
	if len(tz) != 4 {
		return nil // structural regex already guards 4-digit format
	}
	tzH, err1 := strconv.Atoi(tz[0:2])
	tzM, err2 := strconv.Atoi(tz[2:4])
	if err1 != nil || err2 != nil {
		return nil // non-numeric: structural regex should have caught this
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
