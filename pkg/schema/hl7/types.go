package hl7

import (
	"strconv"
	"strings"
)

// Delimiters holds the separator characters from MSH (positions 4-8, optionally 9).
type Delimiters struct {
	Field        byte
	Component    byte
	Repetition   byte
	Escape       byte
	Subcomponent byte
	Truncation   byte
}

// DefaultDelimiters returns standard HL7 delimiters.
func DefaultDelimiters() Delimiters {
	return Delimiters{
		Field:        '|',
		Component:    '^',
		Repetition:   '~',
		Escape:       '\\',
		Subcomponent: '&',
		Truncation:   '#',
	}
}

// EncodingCharacters returns the 4-char encoding (MSH-2).
func (d Delimiters) EncodingCharacters() string {
	return string([]byte{d.Component, d.Repetition, d.Escape, d.Subcomponent})
}

// EncodingCharactersWithTruncation returns encoding including truncation (HL7 v2.7+).
func (d Delimiters) EncodingCharactersWithTruncation() string {
	return string([]byte{d.Component, d.Repetition, d.Escape, d.Subcomponent, d.Truncation})
}

// HasTruncation returns true if truncation character is set.
func (d Delimiters) HasTruncation() bool {
	return d.Truncation != 0
}

// Subcomponent is the lowest level of HL7 data.
type Subcomponent struct {
	Value string
}

// Component contains subcomponents.
type Component struct {
	Subcomponents []Subcomponent
}

// String returns the component value (first subcomponent, or all subcomponents joined with the
// default subcomponent separator '&'). Note: uses the default delimiter set. If the message uses
// a non-standard subcomponent separator, the re-encoded value will differ from the wire bytes.
// Callers that need the original delimiter-aware value should access Subcomponents directly.
func (c Component) String() string {
	if len(c.Subcomponents) == 0 {
		return ""
	}
	if len(c.Subcomponents) == 1 {
		return c.Subcomponents[0].Value
	}
	parts := make([]string, len(c.Subcomponents))
	for i, sc := range c.Subcomponents {
		parts[i] = sc.Value
	}
	return strings.Join(parts, string(DefaultDelimiters().Subcomponent))
}

// SubcomponentAt returns subcomponent at 1-based index.
func (c Component) SubcomponentAt(index int) (Subcomponent, bool) {
	if index < 1 || index > len(c.Subcomponents) {
		return Subcomponent{}, false
	}
	return c.Subcomponents[index-1], true
}

// Repetition contains components.
type Repetition struct {
	Components []Component
}

// String returns the repetition value with components joined by the default component separator '^'.
// Non-standard component separators (rare in practice) will produce a differently encoded string.
func (r Repetition) String() string {
	if len(r.Components) == 0 {
		return ""
	}
	if len(r.Components) == 1 {
		return r.Components[0].String()
	}
	d := DefaultDelimiters()
	parts := make([]string, len(r.Components))
	for i, c := range r.Components {
		parts[i] = c.String()
	}
	return strings.Join(parts, string(d.Component))
}

// ComponentAt returns component at 1-based index.
func (r Repetition) ComponentAt(index int) (Component, bool) {
	if index < 1 || index > len(r.Components) {
		return Component{}, false
	}
	return r.Components[index-1], true
}

// Field contains repetitions.
type Field struct {
	Repetitions []Repetition
}

// String returns the first repetition value.
func (f Field) String() string {
	if len(f.Repetitions) == 0 {
		return ""
	}
	return f.Repetitions[0].String()
}

// RepetitionAt returns repetition at 1-based index.
func (f Field) RepetitionAt(index int) (Repetition, bool) {
	if index < 1 || index > len(f.Repetitions) {
		return Repetition{}, false
	}
	return f.Repetitions[index-1], true
}

// ComponentAt returns component from first repetition at 1-based index.
func (f Field) ComponentAt(index int) (Component, bool) {
	if len(f.Repetitions) == 0 {
		return Component{}, false
	}
	return f.Repetitions[0].ComponentAt(index)
}

// Segment is a single segment (e.g. MSH, PID).
type Segment struct {
	Name   string
	Fields []Field
}

// FieldAt returns field at 1-based index.
func (s Segment) FieldAt(index int) (Field, bool) {
	if index < 1 || index > len(s.Fields) {
		return Field{}, false
	}
	return s.Fields[index-1], true
}

// Message is a complete parsed HL7 message.
type Message struct {
	Segments     []Segment
	Delimiters   Delimiters
	Raw          string
	CharacterSet string
}

// SegmentByName returns the first segment with the given name.
func (m *Message) SegmentByName(name string) (*Segment, int) {
	for i := range m.Segments {
		if m.Segments[i].Name == name {
			return &m.Segments[i], i
		}
	}
	return nil, -1
}

// Get returns the value at HL7 location (e.g. "MSH-12", "PID-3(2).1").
func (m *Message) Get(location string) string {
	segName, field, rep, comp, sub := parseLocation(location)
	seg, _ := m.SegmentByName(segName)
	if seg == nil {
		return ""
	}
	f, ok := seg.FieldAt(field)
	if !ok {
		return ""
	}
	if rep > 0 {
		r, ok := f.RepetitionAt(rep)
		if !ok {
			return ""
		}
		if comp > 0 {
			c, ok := r.ComponentAt(comp)
			if !ok {
				return ""
			}
			if sub > 0 {
				sc, ok := c.SubcomponentAt(sub)
				if !ok {
					return ""
				}
				return sc.Value
			}
			return c.String()
		}
		return r.String()
	}
	if comp > 0 {
		c, ok := f.ComponentAt(comp)
		if !ok {
			return ""
		}
		if sub > 0 {
			sc, ok := c.SubcomponentAt(sub)
			if !ok {
				return ""
			}
			return sc.Value
		}
		return c.String()
	}
	return f.String()
}

// parseLocation parses HL7 location e.g. "MSH-12", "PID-3(2).1" into segment name and 1-based indices.
func parseLocation(loc string) (seg string, field, rep, comp, sub int) {
	parts := strings.SplitN(loc, "-", 2)
	if len(parts) < 2 {
		return "", 0, 0, 0, 0
	}
	seg = parts[0]
	rest := parts[1]
	if idx := strings.Index(rest, "("); idx != -1 {
		if endIdx := strings.Index(rest, ")"); endIdx > idx {
			rep, _ = strconv.Atoi(rest[idx+1 : endIdx])
			rest = rest[:idx] + rest[endIdx+1:]
		}
	}
	dotParts := strings.Split(rest, ".")
	if len(dotParts) >= 1 {
		field, _ = strconv.Atoi(dotParts[0])
	}
	if len(dotParts) >= 2 {
		comp, _ = strconv.Atoi(dotParts[1])
	}
	if len(dotParts) >= 3 {
		sub, _ = strconv.Atoi(dotParts[2])
	}
	return
}
