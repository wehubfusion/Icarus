package hl7

import (
	"fmt"
	"strconv"
	"strings"
)

// ValidationError represents a single HL7 validation error (Path, Message, Code).
type ValidationError struct {
	Path    string
	Message string
	Code    string
}

// MatchedSegment pairs a message segment with its schema definition for validation.
type MatchedSegment struct {
	MessageSegmentIndex int
	Segment             *Segment
	SchemaDef           *HL7SegmentDef
}

// MatchResult holds the result of structure matching.
type MatchResult struct {
	Matched []MatchedSegment
	Errors  []ValidationError
}

// isZSegment returns true if the segment name is a Z-segment (custom/private segment per HL7).
func isZSegment(name string) bool {
	if name == "" {
		return false
	}
	c := name[0]
	return c == 'Z' || c == 'z'
}

// MatchMessage matches the message segments against the schema segment/group tree in order.
// Returns matched segment pairs and structure errors (missing required, unexpected segment, wrong order, rpt violation).
// Z-segments (names starting with Z) that are not in the schema are skipped during matching and not reported as unexpected.
func MatchMessage(msg *Message, compiled *CompiledHL7Schema) MatchResult {
	var result MatchResult
	if compiled == nil || compiled.Schema == nil {
		result.Errors = append(result.Errors, ValidationError{
			Path: "message", Message: "no schema", Code: "HL7_INVALID_SCHEMA",
		})
		return result
	}
	msgIdx, errs := matchSegments(msg, 0, compiled.Schema.Segments, &result)
	if msgIdx < len(msg.Segments) {
		// Report unexpected segments only for non-Z-segments (Z-segments are allowed as local extensions).
		// Track per-name occurrence so the path is "NK1[2]" (2nd NK1) not "NK1-3" (misleading field ref).
		occurrences := make(map[string]int)
		for i := msgIdx; i < len(msg.Segments); i++ {
			name := msg.Segments[i].Name
			if isZSegment(name) {
				continue
			}
			occurrences[name]++
			path := name
			if occurrences[name] > 1 {
				path = fmt.Sprintf("%s[%d]", name, occurrences[name])
			}
			result.Errors = append(result.Errors, ValidationError{
				Path:    path,
				Message: "unexpected segment; not in schema order",
				Code:    "HL7_UNEXPECTED_SEGMENT",
			})
		}
	}
	result.Errors = append(result.Errors, errs...)
	return result
}

// matchSegments tries to consume message segments starting at msgIdx by matching schemaSegs.
// Returns the new msgIdx and any errors. Appends to result.Matched.
func matchSegments(msg *Message, msgIdx int, schemaSegs []HL7SegmentDef, result *MatchResult) (int, []ValidationError) {
	var errs []ValidationError
	for si := range schemaSegs {
		def := &schemaSegs[si]
		if def.IsGroup {
			// Group: repeat 0..rpt times; each repetition consumes a sequence of nested segments.
			maxRep := parseRptMax(def.Rpt)
			rep := 0
			nested := derefSegmentDefs(def.Segments)
			for rep < maxRep {
				nextIdx, groupErrs := matchSegments(msg, msgIdx, nested, result)
				// Stop when no progress was made (no segments consumed) — whether or not
				// there were errors. Without this, an all-optional group that never matches
				// would loop forever because no error is generated to trigger the break.
				if nextIdx == msgIdx {
					break
				}
				// Propagate errors from within the group (e.g. required segments that were
				// absent after the group partially matched). Previously _ = groupErrs silently
				// discarded them, meaning a required segment inside a partially-matched group
				// would never be reported as missing. (BUG-20)
				errs = append(errs, groupErrs...)
				msgIdx = nextIdx
				rep++
				if rep < maxRep && nextIdx >= len(msg.Segments) {
					break
				}
			}
			if isRequired(def.Usage) && rep == 0 {
				errs = append(errs, ValidationError{
					Path:    "segments." + def.Name,
					Message: "required group must appear at least once",
					Code:    "HL7_MISSING_REQUIRED",
				})
			}
			continue
		}
		// Leaf segment: match by name. Skip Z-segments that are not the current expected segment (passthrough).
		for msgIdx < len(msg.Segments) && isZSegment(msg.Segments[msgIdx].Name) && msg.Segments[msgIdx].Name != def.Name {
			msgIdx++
		}
		minRep, maxRep := segmentRep(def.Usage, def.Rpt)
		consumed := 0
		for msgIdx < len(msg.Segments) && msg.Segments[msgIdx].Name == def.Name && consumed < maxRep {
			result.Matched = append(result.Matched, MatchedSegment{
				MessageSegmentIndex: msgIdx,
				Segment:             &msg.Segments[msgIdx],
				SchemaDef:           def,
			})
			consumed++
			msgIdx++
		}
		if consumed < minRep {
			errs = append(errs, ValidationError{
				Path:    def.Name,
				Message: fmt.Sprintf("required segment %s must appear at least %d time(s)", def.Name, minRep),
				Code:    "HL7_MISSING_REQUIRED",
			})
		}
		// Consume excess repetitions beyond maxRep and report them precisely as
		// HL7_REPETITION_VIOLATION rather than leaving them to be reported as the
		// misleading HL7_UNEXPECTED_SEGMENT by the caller. (BUG-23)
		excess := 0
		for msgIdx < len(msg.Segments) && msg.Segments[msgIdx].Name == def.Name {
			excess++
			msgIdx++
		}
		if excess > 0 {
			errs = append(errs, ValidationError{
				Path:    def.Name,
				Message: fmt.Sprintf("segment %s appears %d time(s), max allowed is %d", def.Name, consumed+excess, maxRep),
				Code:    "HL7_REPETITION_VIOLATION",
			})
		}
	}
	return msgIdx, errs
}

// isRequired returns true when an absent element (field, component, subcomponent, or segment)
// should be reported as a validation error.
// Only R (Required) is strictly required to be present; RE (Required But May Be Empty) MAY be
// absent — per HL7 2.5 conformance §B.8, RE means "required if known" and absence is allowed.
func isRequired(usage string) bool {
	u := strings.ToUpper(strings.TrimSpace(usage))
	return u == UsageRequired
}

func parseRptMax(rpt string) int {
	rpt = trim(rpt)
	if rpt == "" || rpt == "0" {
		return 0
	}
	if rpt == "*" || strings.EqualFold(rpt, "unbounded") {
		return 1 << 30
	}
	n, err := strconv.Atoi(rpt)
	if err != nil || n <= 0 {
		// unrecognised or non-positive: treat as unlimited (fail-open) rather than silently capping at 1
		return 1 << 30
	}
	return n
}

func segmentRep(usage, rpt string) (min, max int) {
	max = parseRptMax(rpt)
	if max == 0 {
		max = 1
	}
	if isRequired(usage) {
		min = 1
	}
	return min, max
}

func derefSegmentDefs(pts []*HL7SegmentDef) []HL7SegmentDef {
	if len(pts) == 0 {
		return nil
	}
	out := make([]HL7SegmentDef, 0, len(pts))
	for _, p := range pts {
		if p != nil {
			out = append(out, *p)
		}
	}
	return out
}

func trim(s string) string {
	if s == "" {
		return ""
	}
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
}
