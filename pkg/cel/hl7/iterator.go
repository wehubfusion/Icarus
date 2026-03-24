package celhl7

import (
	"fmt"
	"strings"

	celgo "github.com/google/cel-go/cel"
	icel "github.com/wehubfusion/Icarus/pkg/cel"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
)

// HL7ScopeIterator implements icel.ScopeIterator for HL7 messages.
// It infers the iteration scope (segment name) from each rule's expressions at runtime.
type HL7ScopeIterator struct {
	Msg *hl7msg.Message
	Reg *datatypes.Registry
}

var _ icel.ScopeIterator = (*HL7ScopeIterator)(nil)

// IterationCount returns how many times the rule should run.
// Scope is inferred from the rule expressions: if a single segment is referenced
// (e.g. 'OBX-5') the rule runs once per segment instance; otherwise it runs once
// at message level. Returns 0 when the target segment is not present in the message.
func (it *HL7ScopeIterator) IterationCount(rule icel.InputRule) int {
	scope := inferScopeFromExprs(rule.When, rule.Assert)
	if scope == "" {
		return 1 // message-scoped: always run once
	}
	if it.Msg == nil {
		return 0
	}
	return it.Msg.SegmentInstanceCount(scope) // 0 → segment absent, rule is skipped
}

// ProgramOptionsAt binds HL7 helpers for the i-th instance (0-based) of the rule's scope.
func (it *HL7ScopeIterator) ProgramOptionsAt(rule icel.InputRule, index int) []celgo.ProgramOption {
	scope := inferScopeFromExprs(rule.When, rule.Assert)
	return BindMessage(it.Msg, scope, index, it.Reg)
}

// ErrorPath builds a qualified path like OBX[2]-5 when the rule is segment-scoped,
// or returns the raw errorPath unchanged for message-scoped rules.
func (it *HL7ScopeIterator) ErrorPath(rule icel.InputRule, index int) string {
	base := strings.TrimSpace(rule.ErrorPath)
	if inferScopeFromExprs(rule.When, rule.Assert) == "" {
		return base
	}
	parts := strings.SplitN(base, "-", 2)
	if len(parts) == 2 {
		return parts[0] + fmt.Sprintf("[%d]", index+1) + "-" + parts[1]
	}
	return base + fmt.Sprintf("[%d]", index+1)
}
