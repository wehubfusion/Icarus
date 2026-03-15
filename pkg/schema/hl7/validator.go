package hl7

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Pre-compiled regexes for HL7 primitive datatype validation (per HL7 v2.5.1).
var (
	reDT  = regexp.MustCompile(`^\d{4}(\d{2}(\d{2})?)?$`)                                                                       // DT: YYYY, YYYYMM, or YYYYMMDD
	reTM  = regexp.MustCompile(`^\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?([+-]\d{4})?$`)                                               // TM: HH[MM[SS[.s]]][+/-ZZZZ]
	reDTM = regexp.MustCompile(`^\d{4}(\d{2}(\d{2}(\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?)?)?)?([+-]\d{4})?$`)                         // DTM/TS: full timestamp
	reNM  = regexp.MustCompile(`^\s*[-+]?\d*\.?\d+\s*$`)                                                                         // NM: numeric
	reSI  = regexp.MustCompile(`^\s*[-+]?\d+\s*$`)                                                                               // SI: integer
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
		if !strings.EqualFold(want, got) {
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
// Validation is schema-driven: "required component missing" (HL7_REQUIRED) is reported when the
// schema marks that component as R (or RE) and the message has no value at that 1-based component
// index (e.g. message has "ORU^R01" with two components, schema defines MSG.1–MSG.3 with MSG.3 R).
func ValidateMatchResult(match MatchResult, msg *Message, collectAll bool, allowExtraFields bool) []ValidationError {
	var errs []ValidationError
	for _, m := range match.Matched {
		if m.Segment == nil || m.SchemaDef == nil {
			continue
		}
		for fi := range m.SchemaDef.Fields {
			fdef := &m.SchemaDef.Fields[fi]
			fieldErrs := validateField(m.Segment, fdef, m.Segment.Name, msg, allowExtraFields)
			errs = append(errs, fieldErrs...)
			if !collectAll && len(errs) > 0 {
				return errs
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
					Path: fmt.Sprintf("%s-%d", m.Segment.Name, fn),
					Message: "segment has more fields than schema allows",
					Code:    "HL7_EXTRA_FIELD",
				})
				if !collectAll && len(errs) > 0 {
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
	// X: should be absent or empty; if present, we still validate structure but could warn
	if strings.ToUpper(strings.TrimSpace(fdef.Usage)) == UsageNotUsed && f.String() != "" {
		errs = append(errs, ValidationError{
			Path: path, Message: "field must not be present or must be empty (X)", Code: "HL7_NOT_USED",
		})
	}
	// R: must be present and non-empty. RE (Required But May Be Empty): must be present, value may be empty — so we only report HL7_REQUIRED for R when value is empty.
	if strings.ToUpper(strings.TrimSpace(fdef.Usage)) == UsageRequired && f.String() == "" {
		errs = append(errs, ValidationError{
			Path: path, Message: "required field must be non-empty", Code: "HL7_REQUIRED",
		})
	}
	// Repetition count vs rpt
	maxRep := parseRptMax(fdef.Rpt)
	if maxRep > 0 && len(f.Repetitions) > maxRep {
		errs = append(errs, ValidationError{
			Path: path, Message: fmt.Sprintf("field has %d repetitions, max allowed is %s", len(f.Repetitions), fdef.Rpt), Code: "HL7_REPETITION_VIOLATION",
		})
	}
	// Length (0 = unconstrained)
	if fdef.Length > 0 {
		for ri, rep := range f.Repetitions {
			val := rep.String()
			if len(val) > fdef.Length {
				p := path
				if len(f.Repetitions) > 1 {
					p = fmt.Sprintf("%s(%d)", path, ri+1)
				}
				errs = append(errs, ValidationError{
					Path: p, Message: fmt.Sprintf("length %d exceeds maximum %d", len(val), fdef.Length), Code: "HL7_LENGTH",
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
				if cdef.Length > 0 && len(c.String()) > cdef.Length {
					errs = append(errs, ValidationError{
						Path: cPath, Message: fmt.Sprintf("length exceeds maximum %d", cdef.Length), Code: "HL7_LENGTH",
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
						if sdef.Length > 0 && len(sc.Value) > sdef.Length {
							errs = append(errs, ValidationError{
								Path: sPath, Message: fmt.Sprintf("length exceeds maximum %d", sdef.Length), Code: "HL7_LENGTH",
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
	if value == "" {
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
	case "TM": // time HH[MM[SS[.s]]][+/-ZZZZ] per HL7 §2.A.75
		if !reTM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be time (HH[MM[SS]])", Code: "HL7_DATATYPE"}
		}
	case "DTM", "TS": // DTM/TS: full timestamp YYYY[MM[DD[HH[MM[SS[.s]]]]]][+/-ZZZZ]
		if !reDTM.MatchString(value) {
			return &ValidationError{Path: path, Message: "value must be timestamp (DTM/TS format)", Code: "HL7_DATATYPE"}
		}
	case "ID", "IS": // coded values; table validation requires external terminology service
		return nil
	default:
		// unknown type: accept
	}
	return nil
}
