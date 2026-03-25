package celhl7

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	celgo "github.com/google/cel-go/cel"
	icel "github.com/wehubfusion/Icarus/pkg/cel"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
)

// ─── scope inference ──────────────────────────────────────────────────────────

// hl7LocationRe matches quoted HL7 location strings inside CEL expressions,
// e.g. 'OBX-5', 'PID-3'. Capture group 1 is the segment name.
var hl7LocationRe = regexp.MustCompile(`'([A-Z][A-Z0-9]{2,3})-\d`)

// uniqueSegments scans an expression for quoted HL7 location literals and
// returns the set of distinct segment names referenced (e.g. {"OBX", "PID"}).
func uniqueSegments(expr string) map[string]bool {
	segs := map[string]bool{}
	for _, m := range hl7LocationRe.FindAllStringSubmatch(expr, -1) {
		segs[m[1]] = true
	}
	return segs
}

// inferScopeFromExprs derives the iteration segment scope from the rule's CEL
// expressions (when + assert, in their final expanded form).
//
// Priority order:
//  1. Assert references exactly one segment → use it (assert-first rule).
//     Cross-segment references in `when` fall back to Message.Get which returns
//     the first occurrence — correct for singleton segments (PID, MSH, EVN…).
//  2. When alone names exactly one segment → use it (common guard pattern).
//  3. Frequency count across both expressions: segment cited most often is the
//     strongest signal of iteration intent → use it when there is a unique max.
//  4. Genuine expression-level tie (equal citation counts) → return "", tiedSegs
//     so the caller can break the tie using actual message instance counts.
//     The caller picks the segment with the most instances in the message,
//     maximising per-instance coverage.  If no literals at all → "", nil.
func inferScopeFromExprs(when, assert string) (scope string, tiedSegs []string) {
	// Step 1: derive scope from assert alone.
	if assertSegs := uniqueSegments(assert); len(assertSegs) == 1 {
		for seg := range assertSegs {
			return seg, nil
		}
	}
	// Step 2: derive scope from when alone.
	if whenSegs := uniqueSegments(when); len(whenSegs) == 1 {
		for seg := range whenSegs {
			return seg, nil
		}
	}
	// Step 3: most-referenced segment across both expressions wins.
	combined := map[string]int{}
	for _, m := range hl7LocationRe.FindAllStringSubmatch(when+" "+assert, -1) {
		combined[m[1]]++
	}
	if len(combined) == 0 {
		return "", nil
	}
	maxCount := 0
	for _, cnt := range combined {
		if cnt > maxCount {
			maxCount = cnt
		}
	}
	var tied []string
	for seg, cnt := range combined {
		if cnt == maxCount {
			tied = append(tied, seg)
		}
	}
	if len(tied) == 1 {
		return tied[0], nil
	}
	// Step 4: expression-level tie — let the caller break it with message context.
	return "", tied
}

// pickByMessageInstances selects from candidates the segment with the most
// instances in msg. Alphabetical order breaks equal-count ties for determinism.
// Returns the first candidate alphabetically when msg is nil.
func pickByMessageInstances(candidates []string, msg *hl7msg.Message) string {
	best, bestCnt := "", -1
	for _, seg := range candidates {
		cnt := 0
		if msg != nil {
			cnt = msg.SegmentInstanceCount(seg)
		}
		if cnt > bestCnt || (cnt == bestCnt && (best == "" || seg < best)) {
			best, bestCnt = seg, cnt
		}
	}
	return best
}

// ─── bind context ─────────────────────────────────────────────────────────────

// bindCtx carries per-evaluation message state for CEL function implementations.
type bindCtx struct {
	msg          *hl7msg.Message
	scopeSeg     string
	instanceIdx0 int
	reg          *datatypes.Registry
	version      string
}

func newBindCtx(msg *hl7msg.Message, scopeSeg string, instanceIdx0 int, reg *datatypes.Registry) *bindCtx {
	v := ""
	if msg != nil {
		v = strings.TrimSpace(msg.Get("MSH-12"))
	}
	return &bindCtx{
		msg:          msg,
		scopeSeg:     strings.TrimSpace(scopeSeg),
		instanceIdx0: instanceIdx0,
		reg:          reg,
		version:      v,
	}
}

// bindMessage returns EnvOptions that bind all HL7 helper functions to ctx.
func bindMessage(msg *hl7msg.Message, scopeSeg string, instanceIdx0 int, reg *datatypes.Registry) []celgo.EnvOption {
	ctx := newBindCtx(msg, scopeSeg, instanceIdx0, reg)
	return hl7EnvOptions(ctx)
}

// ─── iterator ─────────────────────────────────────────────────────────────────

// HL7ScopeIterator implements icel.ScopeIterator for HL7 messages.
// It infers the iteration segment for each rule's expressions,
// caching the result per rule ID so inference runs at most once per rule.
//
// Scope resolution is fully deterministic — no rule ever falls back to
// message scope when any segment literal is present in the rule text:
//  1. assert-first: single-segment assert → scope (cross-refs in when use first-occurrence).
//  2. when-fallback: single-segment when → scope.
//  3. frequency: segment cited most often across both expressions → scope.
//  4. expression tie: tied segments → pick whichever has the most instances in
//     the message (maximises per-instance coverage); alphabetical tiebreak.
//  5. no literals at all → message scope (rule intentionally global, runs once).
type HL7ScopeIterator struct {
	Msg        *hl7msg.Message
	Reg        *datatypes.Registry
	scopeCache sync.Map // rule.ID → string
}

var _ icel.ScopeIterator = (*HL7ScopeIterator)(nil)

// scope returns the fully-resolved iteration segment for rule, computing and
// caching on first call.
func (it *HL7ScopeIterator) scope(rule icel.InputRule) string {
	if v, ok := it.scopeCache.Load(rule.ID); ok {
		return v.(string)
	}
	s, tiedSegs := inferScopeFromExprs(rule.When, rule.Assert)
	if s == "" && len(tiedSegs) > 0 {
		// Expression-level tie: use message instance counts to break it so
		// the most-repeating segment drives iteration (maximum coverage).
		s = pickByMessageInstances(tiedSegs, it.Msg)
	}
	it.scopeCache.Store(rule.ID, s)
	return s
}

// IterationCount returns how many times the rule should run.
// Returns 0 when the target segment is absent from the message (rule is skipped).
//
// Message-scoped rules (scope == "") only occur when the rule contains no
// quoted HL7 location literals at all. They run exactly once; helper functions
// resolve fields via Message.Get which returns the first occurrence of each
// segment. All other rules have a concrete segment scope resolved by the
// 4-step inference chain and run once per instance of that segment.
func (it *HL7ScopeIterator) IterationCount(rule icel.InputRule) int {
	scope := it.scope(rule)
	if scope == "" {
		return 1
	}
	if it.Msg == nil {
		return 0
	}
	return it.Msg.SegmentInstanceCount(scope)
}

// EnvOptionsAt returns EnvOptions that bind HL7 helper functions for the i-th instance (0-based).
func (it *HL7ScopeIterator) EnvOptionsAt(rule icel.InputRule, index int) []celgo.EnvOption {
	return bindMessage(it.Msg, it.scope(rule), index, it.Reg)
}

// ErrorPath builds a qualified path like OBX[2]-5 for segment-scoped rules,
// or returns the raw errorPath unchanged for message-scoped rules.
// If errorPath is empty for a segment-scoped rule, the segment name with an
// instance suffix (e.g. "OBX[2]") is used so the path is always meaningful.
// Message-scoped rules (no quoted HL7 literals in when/assert) also need a
// non-empty path when errorPath is omitted — otherwise issues are unattributable.
func (it *HL7ScopeIterator) ErrorPath(rule icel.InputRule, index int) string {
	base := strings.TrimSpace(rule.ErrorPath)
	scope := it.scope(rule)
	if scope == "" {
		// Message-scoped: return the declared errorPath when set.
		if base != "" {
			return base
		}
		id := strings.TrimSpace(rule.ID)
		if id != "" {
			return fmt.Sprintf("rule[%s]", id)
		}
		return "message"
	}
	if base == "" {
		// Segment-scoped but no errorPath declared: use the segment name + instance.
		return fmt.Sprintf("%s[%d]", scope, index+1)
	}
	parts := strings.SplitN(base, "-", 2)
	if len(parts) == 2 {
		return parts[0] + fmt.Sprintf("[%d]", index+1) + "-" + parts[1]
	}
	return base + fmt.Sprintf("[%d]", index+1)
}
