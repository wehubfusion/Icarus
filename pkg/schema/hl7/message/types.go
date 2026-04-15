package message

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

// effectiveDelimiters returns d when it looks parsed from a real message (field separator set),
// otherwise default delimiters for joining in isolation (e.g. tests, FieldValueOnSegment).
func effectiveDelimiters(d Delimiters) Delimiters {
	if d.Field != 0 {
		return d
	}
	return DefaultDelimiters()
}

// Subcomponent is the lowest level of HL7 data.
type Subcomponent struct {
	Value string
}

// Component contains subcomponents.
type Component struct {
	Subcomponents []Subcomponent
}

// FormatWithDelimiters joins subcomponents using d's subcomponent separator (on-the-wire style).
func (c Component) FormatWithDelimiters(d Delimiters) string {
	d = effectiveDelimiters(d)
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
	return strings.Join(parts, string(d.Subcomponent))
}

// String returns the component value (subcomponents joined by default '&').
// For values from a parsed message, prefer FormatWithDelimiters(msg.Delimiters) so separators match the wire format.
func (c Component) String() string {
	return c.FormatWithDelimiters(DefaultDelimiters())
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

// FormatWithDelimiters joins components using d's component separator.
func (r Repetition) FormatWithDelimiters(d Delimiters) string {
	d = effectiveDelimiters(d)
	if len(r.Components) == 0 {
		return ""
	}
	if len(r.Components) == 1 {
		return r.Components[0].FormatWithDelimiters(d)
	}
	parts := make([]string, len(r.Components))
	for i, c := range r.Components {
		parts[i] = c.FormatWithDelimiters(d)
	}
	return strings.Join(parts, string(d.Component))
}

// String returns the repetition value (components joined by default '^').
// For values from a parsed message, prefer FormatWithDelimiters(msg.Delimiters).
func (r Repetition) String() string {
	return r.FormatWithDelimiters(DefaultDelimiters())
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

// FormatWithDelimiters formats the first repetition using d.
func (f Field) FormatWithDelimiters(d Delimiters) string {
	if len(f.Repetitions) == 0 {
		return ""
	}
	return f.Repetitions[0].FormatWithDelimiters(d)
}

// String returns the first repetition value (default delimiters).
// For values from a parsed message, prefer FormatWithDelimiters(msg.Delimiters).
func (f Field) String() string {
	return f.FormatWithDelimiters(DefaultDelimiters())
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
// Name matching is case-insensitive (e.g. "pid-3" and "PID-3" both resolve PID).
func (m *Message) SegmentByName(name string) (*Segment, int) {
	if m == nil {
		return nil, -1
	}
	want := strings.TrimSpace(name)
	for i := range m.Segments {
		if strings.EqualFold(m.Segments[i].Name, want) {
			return &m.Segments[i], i
		}
	}
	return nil, -1
}

// Get returns the value at HL7 location (e.g. "MSH-12", "PID-3(2).1").
func (m *Message) Get(location string) string {
	if m == nil {
		return ""
	}
	segName, field, rep, comp, sub := parseLocation(location)
	seg, _ := m.SegmentByName(segName)
	if seg == nil {
		return ""
	}
	return getFieldValueDelimited(seg, field, rep, comp, sub, m.Delimiters)
}

// NthSegmentByName returns the n-th segment (1-based) with the given name, or nil.
func (m *Message) NthSegmentByName(name string, n int) *Segment {
	if m == nil || n < 1 {
		return nil
	}
	want := strings.ToUpper(strings.TrimSpace(name))
	count := 0
	for i := range m.Segments {
		if strings.EqualFold(m.Segments[i].Name, want) {
			count++
			if count == n {
				return &m.Segments[i]
			}
		}
	}
	return nil
}

// SegmentInstanceCount returns how many segments with the given name appear in order.
func (m *Message) SegmentInstanceCount(name string) int {
	if m == nil {
		return 0
	}
	want := strings.ToUpper(strings.TrimSpace(name))
	n := 0
	for i := range m.Segments {
		if strings.EqualFold(m.Segments[i].Name, want) {
			n++
		}
	}
	return n
}

// GetAtSegmentInstance returns the value at loc when loc refers to scopeSeg; the segment
// instance is the instanceIdx1-th occurrence (1-based). If loc refers to another segment,
// falls back to Get (first matching segment).
func (m *Message) GetAtSegmentInstance(scopeSeg string, instanceIdx1 int, loc string) string {
	if m == nil {
		return ""
	}
	if strings.TrimSpace(scopeSeg) == "" {
		return m.Get(loc)
	}
	segName, field, rep, comp, sub := parseLocation(loc)
	if segName == "" {
		return ""
	}
	if strings.EqualFold(segName, scopeSeg) {
		seg := m.NthSegmentByName(scopeSeg, instanceIdx1)
		if seg == nil {
			return ""
		}
		return getFieldValueDelimited(seg, field, rep, comp, sub, m.Delimiters)
	}
	return m.Get(loc)
}

// FieldValueOnSegment returns the value at HL7 location for a specific segment instance.
func FieldValueOnSegment(seg *Segment, location string) string {
	if seg == nil {
		return ""
	}
	segName, field, rep, comp, sub := parseLocation(location)
	if segName == "" || !strings.EqualFold(segName, seg.Name) {
		return ""
	}
	return getFieldValueDelimited(seg, field, rep, comp, sub, Delimiters{})
}

func getFieldValueDelimited(seg *Segment, field, rep, comp, sub int, d Delimiters) string {
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
			return c.FormatWithDelimiters(d)
		}
		return r.FormatWithDelimiters(d)
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
		return c.FormatWithDelimiters(d)
	}
	return f.FormatWithDelimiters(d)
}

// LocationParts parses an HL7 location (e.g. "MSH-12", "PID-3(2).1") into segment name and 1-based indices.
func LocationParts(loc string) (seg string, field, rep, comp, sub int) {
	return parseLocation(loc)
}

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
