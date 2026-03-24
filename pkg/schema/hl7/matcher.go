package hl7

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/primitive"
)

// ValidationError represents a single HL7 validation error (Path, Message, Code).
// It is aliased to the primitive validation error to avoid duplicated types.
type ValidationError = primitive.FieldError

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

// computeGroupStart returns the set of segment names that can legitimately open one iteration
// of a group.  It walks the group's segment list and collects names until it reaches the first
// required (R) definition — nothing after that can be the very first segment of a new iteration.
// Nested sub-groups are recursed into so their possible opening segments are included.
func computeGroupStart(segs []HL7SegmentDef) map[string]bool {
	result := make(map[string]bool)
	for _, seg := range segs {
		if seg.IsGroup {
			for k := range computeGroupStart(derefSegmentDefs(seg.Segments)) {
				result[k] = true
			}
		} else {
			result[seg.Name] = true
		}
		// Once a required segment is reached, no later definition can be the opener.
		if isRequired(seg.Usage) {
			break
		}
	}
	return result
}

// computeReserve counts how many consecutive occurrences of segName must be left in the
// message stream for later schema nodes (siblings that come after the current position in
// schemaSegs) so they are not greedily consumed by the current node.
//
// A "later" required group whose can-start set includes segName counts as 1 reservation
// (it needs at least one occurrence to open an iteration).
// A "later" required leaf with the same name also counts as 1 reservation.
func computeReserve(laterSegs []HL7SegmentDef, segName string) int {
	reserve := 0
	for _, seg := range laterSegs {
		if seg.IsGroup {
			if isRequired(seg.Usage) {
				cs := computeGroupStart(derefSegmentDefs(seg.Segments))
				if cs[segName] {
					reserve++
				}
			}
		} else if seg.Name == segName && isRequired(seg.Usage) {
			reserve++
		}
	}
	return reserve
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
	msgIdx, errs := matchSegments(msg, 0, compiled.Schema.Segments, &result, 0)
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

// matchSegments matches a message segment stream to a schema segment list.
func matchSegments(msg *Message, msgIdx int, schemaSegs []HL7SegmentDef, result *MatchResult, depth int) (int, []ValidationError) {
	var errs []ValidationError
	for si := range schemaSegs {
		def := &schemaSegs[si]
		if def.IsGroup {
			maxRep := parseRptMax(def.Rpt)
			if trim(def.Rpt) == "" {
				maxRep = 1 << 30
			} else if maxRep == 0 {
				maxRep = 1
			}
			rep := 0
			nested := derefSegmentDefs(def.Segments)
			canStart := computeGroupStart(nested)
			for rep < maxRep {
				if msgIdx < len(msg.Segments) && !canStart[msg.Segments[msgIdx].Name] {
					break
				}
				nextIdx, groupErrs := matchSegments(msg, msgIdx, nested, result, depth+1)
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

		available := 0
		for tmpIdx := msgIdx; tmpIdx < len(msg.Segments) && msg.Segments[tmpIdx].Name == def.Name; tmpIdx++ {
			available++
		}

		reserve := computeReserve(schemaSegs[si+1:], def.Name)

		canConsume := available - reserve
		if canConsume < 0 {
			canConsume = 0
		}
		if canConsume > maxRep {
			canConsume = maxRep
		}

		consumed := 0
		for msgIdx < len(msg.Segments) && msg.Segments[msgIdx].Name == def.Name && consumed < canConsume {
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

		if depth == 0 {
			uncaptured := available - consumed
			trueExcess := uncaptured - reserve
			if trueExcess > 0 {
				for i := 0; i < trueExcess; i++ {
					msgIdx++
				}
				errs = append(errs, ValidationError{
					Path:    def.Name,
					Message: fmt.Sprintf("segment %s appears %d time(s), max allowed is %d", def.Name, consumed+trueExcess, maxRep),
					Code:    "HL7_REPETITION_VIOLATION",
				})
			}
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
	// Empty rpt means "unspecified" (no max constraint).
	if trim(rpt) == "" {
		max = 1 << 30
	} else {
		max = parseRptMax(rpt)
		if max == 0 {
			max = 1
		}
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

func trim(s string) string { return strings.TrimSpace(s) }
