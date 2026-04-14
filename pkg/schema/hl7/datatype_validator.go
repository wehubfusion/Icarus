package hl7

import (
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/primitive"
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
		if err := primitive.ValidatePrimitive(tid, val, path, length); err != nil {
			return []ValidationError{*err}
		}
		return nil
	}

	def, ok := reg.Lookup(version, tid)
	if !ok || def == nil || !def.IsComposite {
		val := repetitionStringWithDelimiters(rep, msg)
		if err := primitive.ValidatePrimitive(tid, val, path, length); err != nil {
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

		childType := strings.ToUpper(strings.TrimSpace(cdef.DataType))
		childDef, childIsComposite := reg.Lookup(version, childType)
		if childIsComposite && childDef != nil && childDef.IsComposite {
			leaves := flattenLeaves(childType, reg, version, map[string]bool{})
			errs = append(errs, validateLeavesAgainstSubcomponents(leaves, c, cPath)...)
			continue
		}

		// Primitive component: report extra subcomponents, then validate only
		// the first subcomponent value as the scalar. Using the full joined string
		// (e.g. "3.5&2.1") would cause false positives on NM/DT/etc checks.
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
		primitiveVal := ""
		if len(c.Subcomponents) > 0 {
			primitiveVal = c.Subcomponents[0].Value
		}
		if err := primitive.ValidatePrimitive(childType, primitiveVal, cPath, cdef.Length); err != nil {
			errs = append(errs, *err)
		}
	}
	return errs
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
		if err := primitive.ValidatePrimitive(leaf.DataType, val, sPath, leaf.Length); err != nil {
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

// ValidatePrimitiveType reports whether value conforms to the primitive HL7 datatype.
// Empty values and VARIES are treated as valid. Composite types skip leaf primitive checks.
func ValidatePrimitiveType(typeID string, value string, reg *datatypes.Registry, version string) bool {
	return primitive.ValidatePrimitiveType(typeID, value, reg, version)
}
