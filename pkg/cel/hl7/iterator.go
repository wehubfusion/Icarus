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

// inferScopeFromExprs derives the iteration segment scope from the rule's CEL
// expressions (when + assert, in their final expanded form).
//
// Every quoted HL7 location literal (e.g. 'OBX-5', 'PID-3') is scanned.
// If all references point to the same segment, that segment is returned so the
// iterator runs the rule once per instance of that segment.
// Mixed references (e.g. both 'OBX-5' and 'PID-3') or no quoted literals at
// all result in message scope (returns ""), where the rule runs exactly once
// and helper functions like msg() only see the FIRST instance of each segment.
// Rule authors must ensure all quoted locations refer to a single segment if
// per-row iteration over a repeating segment is required.
func inferScopeFromExprs(when, assert string) string {
	all := when + " " + assert
	segs := map[string]bool{}
	for _, m := range hl7LocationRe.FindAllStringSubmatch(all, -1) {
		segs[m[1]] = true
	}
	if len(segs) == 1 {
		for seg := range segs {
			return seg
		}
	}
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
type HL7ScopeIterator struct {
	Msg        *hl7msg.Message
	Reg        *datatypes.Registry
	scopeCache sync.Map // rule.ID → string
}

var _ icel.ScopeIterator = (*HL7ScopeIterator)(nil)

// scope returns the inferred scope for a rule, computing it once and caching.
func (it *HL7ScopeIterator) scope(rule icel.InputRule) string {
	if v, ok := it.scopeCache.Load(rule.ID); ok {
		return v.(string)
	}
	s := inferScopeFromExprs(rule.When, rule.Assert)
	it.scopeCache.Store(rule.ID, s)
	return s
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
func (it *HL7ScopeIterator) ErrorPath(rule icel.InputRule, index int) string {
	base := strings.TrimSpace(rule.ErrorPath)
	if it.scope(rule) == "" {
		return base
	}
	parts := strings.SplitN(base, "-", 2)
	if len(parts) == 2 {
		return parts[0] + fmt.Sprintf("[%d]", index+1) + "-" + parts[1]
	}
	return base + fmt.Sprintf("[%d]", index+1)
}
