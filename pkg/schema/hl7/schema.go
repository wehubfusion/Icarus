// Package hl7 provides HL7 v2.x message parsing and validation for the Icarus schema engine.
// Validation is definition-driven (no version or message-type hardcoding).
// Aligns with HAPI HL7 (https://hapifhir.github.io/hapi-hl7v2/) validation behavior.
package hl7

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// CompiledHL7Schema holds a validated HL7 schema for use by the processor.
type CompiledHL7Schema struct {
	Schema *HL7Schema
}

// SchemaType returns the format identifier for the schema engine registry.
func (c *CompiledHL7Schema) SchemaType() string {
	return "HL7"
}

const maxHL7RecursionDepth = 20

// HL7 usage values for schema definition (segment, field, component).
const (
	UsageRequired      = "R"  // Required: element must be present
	UsageRequiredEmpty = "RE" // Required but may be empty
	UsageOptional      = "O"  // Optional
	UsageConditional   = "C"  // Conditional
	UsageBackward      = "B"  // Backward compatible / deprecated
	UsageNotUsed       = "X"  // Not supported; must be absent or ignored
	UsageWithdrawn     = "W"  // Withdrawn (optional withdrawn; HL7 v2.7+)
)

var (
	validUsage = map[string]bool{
		UsageRequired: true, UsageRequiredEmpty: true, UsageOptional: true,
		UsageConditional: true, UsageBackward: true, UsageNotUsed: true, UsageWithdrawn: true,
	}
	rptRegex = regexp.MustCompile(`^$|^1$|^\*$|^[1-9][0-9]*$`)
)

// HL7Schema represents a custom HL7 schema definition (Morpheus-compatible JSON).
type HL7Schema struct {
	MessageType string          `json:"messageType"`
	Version     string          `json:"version"`
	Description string          `json:"description"`
	Segments    []HL7SegmentDef `json:"segments"`
}

// HL7SegmentDef represents a segment or segment group.
type HL7SegmentDef struct {
	Name     string           `json:"name"`
	LongName string           `json:"longName"`
	Usage    string           `json:"usage"`
	Rpt      string           `json:"rpt"`
	IsGroup  bool             `json:"isGroup"`
	Segments []*HL7SegmentDef `json:"segments"`
	Fields   []HL7FieldDef    `json:"fields"`
}

// HL7FieldDef represents a field in an HL7 schema definition.
type HL7FieldDef struct {
	Position   string            `json:"position"`
	Name       string            `json:"name,omitempty"`
	Length     int               `json:"length"`
	DataType   string            `json:"dataType"`
	Usage      string            `json:"usage"`
	Rpt        string            `json:"rpt"`
	TableID    *string           `json:"tableId,omitempty"`
	Components []HL7ComponentDef `json:"components"`
}

// HL7ComponentDef represents a component. Rpt at component level is not used in validation (HL7 spec).
// When unmarshaling from Morpheus JSON, "rpt" if present is ignored.
type HL7ComponentDef struct {
	Position      string               `json:"position"`
	Name          string               `json:"name,omitempty"`
	Length        int                  `json:"length"`
	DataType      string               `json:"dataType"`
	Usage         string               `json:"usage"`
	TableID       *string              `json:"tableId,omitempty"`
	SubComponents []HL7SubcomponentDef `json:"subComponents,omitempty"`
}

// HL7SubcomponentDef represents a subcomponent. Rpt at subcomponent level is not used (HL7 spec).
type HL7SubcomponentDef struct {
	Position string  `json:"position"`
	Name     string  `json:"name,omitempty"`
	Length   int     `json:"length"`
	DataType string  `json:"dataType"`
	Usage    string  `json:"usage"`
	TableID  *string `json:"tableId,omitempty"`
}

// Validate validates the HL7 schema definition (structural only).
func (h *HL7Schema) Validate() error {
	if h.Segments == nil {
		return fmt.Errorf("HL7 definition must have 'segments' array")
	}
	if len(h.Segments) == 0 {
		return fmt.Errorf("HL7 definition 'segments' must contain at least one segment")
	}
	for i := range h.Segments {
		if err := h.validateSegment(&h.Segments[i], 0, fmt.Sprintf("segments[%d]", i)); err != nil {
			return err
		}
	}
	return nil
}

func (h *HL7Schema) validateSegment(seg *HL7SegmentDef, depth int, path string) error {
	if depth > maxHL7RecursionDepth {
		return fmt.Errorf("%s: segment nesting exceeds maximum depth (%d)", path, maxHL7RecursionDepth)
	}
	if strings.TrimSpace(seg.Name) == "" {
		return fmt.Errorf("%s: segment 'name' is required and cannot be empty", path)
	}
	if u := strings.TrimSpace(seg.Usage); u != "" && !validUsage[strings.ToUpper(u)] {
		return fmt.Errorf("%s.usage: must be one of R, RE, O, C, B, X, W (got %q)", path, seg.Usage)
	}
	if r := strings.TrimSpace(seg.Rpt); r != "" && !rptRegex.MatchString(r) {
		return fmt.Errorf("%s.rpt: must be empty, 1, *, or a positive integer (got %q)", path, seg.Rpt)
	}
	if seg.IsGroup {
		if len(seg.Segments) == 0 {
			return fmt.Errorf("%s: group segment must have non-empty 'segments'", path)
		}
		for i, sub := range seg.Segments {
			if sub == nil {
				return fmt.Errorf("%s.segments[%d]: segment cannot be null", path, i)
			}
			if err := h.validateSegment(sub, depth+1, fmt.Sprintf("%s.segments[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	}
	for i := range seg.Fields {
		if err := h.validateField(&seg.Fields[i], fmt.Sprintf("%s.fields[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (h *HL7Schema) validateField(f *HL7FieldDef, path string) error {
	if strings.TrimSpace(f.Position) == "" {
		return fmt.Errorf("%s: field 'position' is required and cannot be empty", path)
	}
	if u := strings.TrimSpace(f.Usage); u != "" && !validUsage[strings.ToUpper(u)] {
		return fmt.Errorf("%s.usage: must be one of R, RE, O, C, B, X, W (got %q)", path, f.Usage)
	}
	if r := strings.TrimSpace(f.Rpt); r != "" && !rptRegex.MatchString(r) {
		return fmt.Errorf("%s.rpt: must be empty, 1, *, or a positive integer (got %q)", path, f.Rpt)
	}
	for i := range f.Components {
		if err := h.validateComponent(&f.Components[i], fmt.Sprintf("%s.components[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (h *HL7Schema) validateComponent(c *HL7ComponentDef, path string) error {
	if strings.TrimSpace(c.Position) == "" {
		return fmt.Errorf("%s: component 'position' is required and cannot be empty", path)
	}
	if u := strings.TrimSpace(c.Usage); u != "" && !validUsage[strings.ToUpper(u)] {
		return fmt.Errorf("%s.usage: must be one of R, RE, O, C, B, X, W (got %q)", path, c.Usage)
	}
	for i := range c.SubComponents {
		if err := h.validateSubcomponent(&c.SubComponents[i], fmt.Sprintf("%s.subComponents[%d]", path, i)); err != nil {
			return err
		}
	}
	return nil
}

func (h *HL7Schema) validateSubcomponent(s *HL7SubcomponentDef, path string) error {
	if strings.TrimSpace(s.Position) == "" {
		return fmt.Errorf("%s: subcomponent 'position' is required and cannot be empty", path)
	}
	if u := strings.TrimSpace(s.Usage); u != "" && !validUsage[strings.ToUpper(u)] {
		return fmt.Errorf("%s.usage: must be one of R, RE, O, C, B, X, W (got %q)", path, s.Usage)
	}
	return nil
}

// ParseHL7Schema parses JSON definition bytes into HL7Schema and validates.
func ParseHL7Schema(definition []byte) (*HL7Schema, error) {
	var schema HL7Schema
	if err := json.Unmarshal(definition, &schema); err != nil {
		return nil, fmt.Errorf("invalid HL7 schema JSON: %w", err)
	}
	if err := schema.Validate(); err != nil {
		return nil, fmt.Errorf("invalid HL7 schema: %w", err)
	}
	return &schema, nil
}
