package hl7

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

var (
	reDT  = regexp.MustCompile(`^\d{4}(\d{2}(\d{2})?)?$`)
	reTM  = regexp.MustCompile(`^\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?([+-]\d{4})?$`)
	reDTM = regexp.MustCompile(`^\d{4}(\d{2}(\d{2}(\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?)?)?)?([+-]\d{4})?$`)
	reNM  = regexp.MustCompile(`^[-+]?\d*\.?\d+$`)
	reSI  = regexp.MustCompile(`^\d+$`)
)

type leafDef struct {
	DataType string
	Length   int
}

func validateDataType(typeID string, rep Repetition, path string, length int, msg *Message, reg *datatypes.Registry, version string) []ValidationError {
	tid := strings.ToUpper(strings.TrimSpace(typeID))
	if tid == "" {
		tid = "ST"
	}

	// Primitive or unknown type: validate the whole repetition as a scalar.
	if reg == nil {
		val := repetitionStringWithDelimiters(rep, msg)
		if err := validatePrimitive(tid, val, path, length); err != nil {
			return []ValidationError{*err}
		}
		return nil
	}

	def, ok := reg.Lookup(version, tid)
	if !ok || def == nil || !def.IsComposite {
		val := repetitionStringWithDelimiters(rep, msg)
		if err := validatePrimitive(tid, val, path, length); err != nil {
			return []ValidationError{*err}
		}
		return nil
	}

	// Composite at field level: each component is separated by the component delimiter (^).
	var errs []ValidationError
	if len(rep.Components) > len(def.Components) {
		for compNum := len(def.Components) + 1; compNum <= len(rep.Components); compNum++ {
			c, ok := rep.ComponentAt(compNum)
			if !ok {
				continue
			}
			if strings.TrimLeft(componentStringWithDelimiters(c, msg), "&") == "" {
				continue
			}
			errs = append(errs, ValidationError{
				Path: fmt.Sprintf("%s.%d", path, compNum), Message: "field has more components than datatype definition allows", Code: "HL7_EXTRA_COMPONENT",
			})
		}
	}

	for _, cdef := range def.Components {
		compNum := parseComponentNumberFromPosition(cdef.Position)
		if compNum <= 0 {
			continue
		}
		cPath := fmt.Sprintf("%s.%d", path, compNum)
		c, ok := rep.ComponentAt(compNum)
		if !ok {
			continue
		}

		compVal := componentStringWithDelimiters(c, msg)
		if cdef.Length > 0 && utf8.RuneCountInString(compVal) > cdef.Length {
			errs = append(errs, ValidationError{
				Path: cPath, Message: fmt.Sprintf("length %d exceeds maximum %d", utf8.RuneCountInString(compVal), cdef.Length), Code: "HL7_LENGTH",
			})
		}

		childType := strings.ToUpper(strings.TrimSpace(cdef.DataType))
		childDef, childIsComposite := reg.Lookup(version, childType)
		if childIsComposite && childDef != nil && childDef.IsComposite {
			leaves := flattenLeaves(childType, reg, version, map[string]bool{})
			errs = append(errs, validateLeavesAgainstSubcomponents(leaves, c, cPath)...)
			continue
		}

		// Primitive component: validate as scalar. Extra subcomponents are reported when strict.
		if len(c.Subcomponents) > 1 {
			for subNum := 2; subNum <= len(c.Subcomponents); subNum++ {
				if strings.TrimSpace(c.Subcomponents[subNum-1].Value) == "" {
					continue
				}
				errs = append(errs, ValidationError{
					Path: fmt.Sprintf("%s.%d", cPath, subNum), Message: "component has more subcomponents than datatype definition allows", Code: "HL7_EXTRA_SUBCOMPONENT",
				})
			}
		}
		if err := validatePrimitive(childType, compVal, cPath, cdef.Length); err != nil {
			errs = append(errs, *err)
		}
	}
	return errs
}

func validateComponentLeaf(typeID string, c Component, path string, length int, msg *Message, reg *datatypes.Registry, version string) []ValidationError {
	tid := strings.ToUpper(strings.TrimSpace(typeID))
	if tid == "" {
		tid = "ST"
	}
	if reg == nil {
		if err := validatePrimitive(tid, componentStringWithDelimiters(c, msg), path, length); err != nil {
			return []ValidationError{*err}
		}
		return nil
	}

	def, ok := reg.Lookup(version, tid)
	if !ok || def == nil || !def.IsComposite {
		if err := validatePrimitive(tid, componentStringWithDelimiters(c, msg), path, length); err != nil {
			return []ValidationError{*err}
		}
		return nil
	}

	leaves := flattenLeaves(tid, reg, version, map[string]bool{})
	return validateLeavesAgainstSubcomponents(leaves, c, path)
}

func flattenLeaves(typeID string, reg *datatypes.Registry, version string, visiting map[string]bool) []leafDef {
	tid := strings.ToUpper(strings.TrimSpace(typeID))
	if tid == "" {
		tid = "ST"
	}
	if visiting[tid] {
		return nil
	}
	def, ok := reg.Lookup(version, tid)
	if !ok || def == nil || !def.IsComposite {
		return []leafDef{{DataType: tid, Length: 0}}
	}

	visiting[tid] = true
	defer func() { visiting[tid] = false }()

	var leaves []leafDef
	for _, c := range def.Components {
		ct := strings.ToUpper(strings.TrimSpace(c.DataType))
		if ct == "" {
			ct = "ST"
		}
		child, childOk := reg.Lookup(version, ct)
		if childOk && child != nil && child.IsComposite {
			leaves = append(leaves, flattenLeaves(ct, reg, version, visiting)...)
			continue
		}
		leaves = append(leaves, leafDef{DataType: ct, Length: c.Length})
	}
	return leaves
}

func validateLeavesAgainstSubcomponents(leaves []leafDef, c Component, path string) []ValidationError {
	var errs []ValidationError
	if len(leaves) == 0 {
		return errs
	}

	if len(c.Subcomponents) > len(leaves) {
		for subNum := len(leaves) + 1; subNum <= len(c.Subcomponents); subNum++ {
			if strings.TrimSpace(c.Subcomponents[subNum-1].Value) == "" {
				continue
			}
			errs = append(errs, ValidationError{
				Path: fmt.Sprintf("%s.%d", path, subNum), Message: "component has more subcomponents than datatype definition allows", Code: "HL7_EXTRA_SUBCOMPONENT",
			})
		}
	}

	for i, leaf := range leaves {
		subNum := i + 1
		sPath := fmt.Sprintf("%s.%d", path, subNum)
		var val string
		if subNum <= len(c.Subcomponents) {
			val = c.Subcomponents[subNum-1].Value
		}
		if leaf.Length > 0 && utf8.RuneCountInString(val) > leaf.Length {
			errs = append(errs, ValidationError{Path: sPath, Message: fmt.Sprintf("length %d exceeds maximum %d", utf8.RuneCountInString(val), leaf.Length), Code: "HL7_LENGTH"})
		}
		if err := validatePrimitive(leaf.DataType, val, sPath, leaf.Length); err != nil {
			errs = append(errs, *err)
		}
	}
	return errs
}

func repetitionStringWithDelimiters(r Repetition, msg *Message) string {
	if len(r.Components) == 0 {
		return ""
	}
	if len(r.Components) == 1 {
		return componentStringWithDelimiters(r.Components[0], msg)
	}
	d := DefaultDelimiters()
	if msg != nil {
		d = msg.Delimiters
	}
	parts := make([]string, 0, len(r.Components))
	for _, c := range r.Components {
		parts = append(parts, componentStringWithDelimiters(c, msg))
	}
	return strings.Join(parts, string(d.Component))
}

func componentStringWithDelimiters(c Component, msg *Message) string {
	if len(c.Subcomponents) == 0 {
		return ""
	}
	if len(c.Subcomponents) == 1 {
		return c.Subcomponents[0].Value
	}
	d := DefaultDelimiters()
	if msg != nil {
		d = msg.Delimiters
	}
	parts := make([]string, 0, len(c.Subcomponents))
	for _, sc := range c.Subcomponents {
		parts = append(parts, sc.Value)
	}
	return strings.Join(parts, string(d.Subcomponent))
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
		// General Timing Specification: follows ST formatting rules.
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
