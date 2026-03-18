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

// matchSegments is the recursive core of the matcher.
//
// depth == 0  — processing top-level (direct schema children).
// depth  > 0  — inside at least one group.
//
// Two mechanisms work together to handle "multiple OBX positions in the same schema":
//
//  1. can-start guard (groups): before each group iteration attempt, verify that the current
//     message segment is a valid opening segment for the group.  This prevents a later,
//     unrelated segment from accidentally starting a new group iteration (e.g. an OBX that
//     cannot open ORDER because ORDER requires ORC first).
//
//  2. reserve (leaf segments): before consuming a leaf, count how many occurrences of its
//     name are still needed by later siblings in the same schema list and hold that many
//     back.  This lets a standalone ORC (rpt:*) leave one ORC for a following required
//     ORDER group.
//
//  3. depth-gated excess detection: HL7_REPETITION_VIOLATION is emitted only at depth == 0
//     (top-level excess).  Inside a group the parent's iteration loop absorbs additional
//     occurrences through repeated iterations, so consuming them here would produce a false
//     violation and prevent those extra iterations.
func matchSegments(msg *Message, msgIdx int, schemaSegs []HL7SegmentDef, result *MatchResult, depth int) (int, []ValidationError) {
	var errs []ValidationError
	for si := range schemaSegs {
		def := &schemaSegs[si]
		if def.IsGroup {
			maxRep := parseRptMax(def.Rpt)
			rep := 0
			nested := derefSegmentDefs(def.Segments)
			// Pre-compute which segment names can legitimately open this group so we do not
			// accidentally start a new iteration with a segment that belongs to a later schema
			// node (e.g. OBX that cannot open ORDER_OBSERVATION because OBR is required first).
			canStart := computeGroupStart(nested)
			for rep < maxRep {
				// Only attempt an iteration when the current segment can open the group.
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

		// Count how many consecutive occurrences of this segment are available in the message.
		available := 0
		for tmpIdx := msgIdx; tmpIdx < len(msg.Segments) && msg.Segments[tmpIdx].Name == def.Name; tmpIdx++ {
			available++
		}

		// Compute how many occurrences to reserve for later siblings in this schema list.
		reserve := computeReserve(schemaSegs[si+1:], def.Name)

		// The number we may actually consume here: at most (available − reserve), further
		// capped by the schema's rpt maximum.
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

		// Excess detection: only emitted at the top level (depth == 0).
		//
		// Inside a group (depth > 0) any extra same-name segments are deliberately left in
		// the message stream so the parent group's iteration loop can pick them up in the
		// next iteration — that is what makes OBSERVATION (rpt:*) / OBX (rpt:1) work for
		// 48 consecutive OBX segments.
		//
		// At depth == 0 we know there is no outer group to absorb them, so we consume the
		// true excess and emit HL7_REPETITION_VIOLATION.  "True excess" excludes the
		// `reserve` occurrences that will be legitimately consumed by later schema siblings.
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
