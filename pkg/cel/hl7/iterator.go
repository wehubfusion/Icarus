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
//  1. Assert references exactly one segment → use it.
//     Any cross-segment references in `when` resolve to first-occurrence via
//     Message.Get, which is correct for singleton segments (PID, MSH, EVN…).
//  2. Assert is empty or multi-segment → try when alone; if it names exactly
//     one segment, use it (common pattern: single-segment guard expression).
//  3. Both are multi-segment → count occurrences of each segment across the
//     combined expression. The segment cited most often is the strongest
//     signal of the author's iteration intent. If there is a unique maximum,
//     return that segment and resolve without ambiguity.
//  4. Genuinely tied (two or more segments equally cited) or no literals →
//     message scope (run once). A warning is emitted at runtime when a
//     referenced segment actually repeats in the message.
func inferScopeFromExprs(when, assert string) string {
	// Step 1: derive scope from assert alone.
	if assertSegs := uniqueSegments(assert); len(assertSegs) == 1 {
		for seg := range assertSegs {
			return seg
		}
	}
	// Step 2: derive scope from when alone.
	if whenSegs := uniqueSegments(when); len(whenSegs) == 1 {
		for seg := range whenSegs {
			return seg
		}
	}
	// Step 3: most-referenced segment across both expressions wins.
	combined := map[string]int{}
	for _, m := range hl7LocationRe.FindAllStringSubmatch(when+" "+assert, -1) {
		combined[m[1]]++
	}
	maxCount, winner, tied := 0, "", false
	for seg, cnt := range combined {
		switch {
		case cnt > maxCount:
			maxCount, winner, tied = cnt, seg, false
		case cnt == maxCount:
			tied = true
		}
	}
	if !tied && winner != "" {
		return winner
	}
	// Step 4: genuinely tied or no literals → message scope.
	return ""
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
// It infers the iteration scope (segment name) from each rule's expressions,
// caching the result per rule ID so the regex scan runs at most once per rule.
// Use ScopeAmbiguities() after evaluation to retrieve any rules where scope
// fell to message-level while a referenced segment actually repeats.
type HL7ScopeIterator struct {
	Msg        *hl7msg.Message
	Reg        *datatypes.Registry
	scopeCache sync.Map       // rule.ID → string
	ambiguous  map[string]bool // rule IDs with unresolvable scope over repeating segments
}

var _ icel.ScopeIterator = (*HL7ScopeIterator)(nil)

// scope returns the inferred scope for a rule, computing it once and caching.
// If scope cannot be inferred (message-level) and a referenced segment repeats
// in the message, the rule ID is recorded in ambiguous for later reporting.
func (it *HL7ScopeIterator) scope(rule icel.InputRule) string {
	if v, ok := it.scopeCache.Load(rule.ID); ok {
		return v.(string)
	}
	s := inferScopeFromExprs(rule.When, rule.Assert)
	// Detect coverage gap: scope is message-level but a referenced segment
	// repeats, meaning per-instance evaluation was likely the intent.
	if s == "" && it.Msg != nil {
		all := rule.When + " " + rule.Assert
		for seg := range uniqueSegments(all) {
			if it.Msg.SegmentInstanceCount(seg) > 1 {
				if it.ambiguous == nil {
					it.ambiguous = make(map[string]bool)
				}
				it.ambiguous[rule.ID] = true
				break
			}
		}
	}
	it.scopeCache.Store(rule.ID, s)
	return s
}

// ScopeAmbiguities returns the IDs of rules that could not have their scope
// inferred to a single segment (both when and assert reference multiple
// segments) while at least one of those segments repeats in the message.
// These rules execute once at message scope, seeing only the first instance
// of every repeated segment — a silent coverage gap.
// Call this after EvaluateRules to surface the issue as a diagnostic.
func (it *HL7ScopeIterator) ScopeAmbiguities() []string {
	if len(it.ambiguous) == 0 {
		return nil
	}
	ids := make([]string, 0, len(it.ambiguous))
	for id := range it.ambiguous {
		ids = append(ids, id)
	}
	return ids
}

// IterationCount returns how many times the rule should run.
// Returns 0 when the target segment is absent (rule is skipped).
//
// MESSAGE-SCOPED RULES (scope == ""): the rule runs exactly once. All helper
// functions (msg, valued, validateAs, …) resolve fields via Message.Get which
// returns the FIRST occurrence of any repeated segment (e.g. the first OBX).
// This is by design for rules that operate on the message as a whole, but is a
// silent correctness trap for rules that reference a repeating segment without
// forcing segment scope via a single quoted literal such as 'OBX-5'.
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
func (it *HL7ScopeIterator) ErrorPath(rule icel.InputRule, index int) string {
	base := strings.TrimSpace(rule.ErrorPath)
	scope := it.scope(rule)
	if scope == "" {
		// Message-scoped: return the raw errorPath (may be empty — caller must handle).
		return base
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
