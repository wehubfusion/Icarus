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

// MatchMessage matches message segments against the schema; Z-segments not in schema are skipped.
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

func matchSegments(msg *Message, msgIdx int, schemaSegs []HL7SegmentDef, result *MatchResult) (int, []ValidationError) {
	var errs []ValidationError
	for si := range schemaSegs {
		def := &schemaSegs[si]
		if def.IsGroup {
			maxRep := parseRptMax(def.Rpt)
			rep := 0
			nested := derefSegmentDefs(def.Segments)
			for rep < maxRep {
				nextIdx, groupErrs := matchSegments(msg, msgIdx, nested, result)
				if nextIdx == msgIdx {
					break
				}
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
