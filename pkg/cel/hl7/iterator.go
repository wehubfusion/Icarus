package celhl7

import (
	"fmt"
	"strings"

	celgo "github.com/google/cel-go/cel"
	icel "github.com/wehubfusion/Icarus/pkg/cel"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

// HL7ScopeIterator implements icel.ScopeIterator for HL7 messages.
type HL7ScopeIterator struct {
	Msg *hl7msg.Message
	Reg *datatypes.Registry
}

var _ icel.ScopeIterator = (*HL7ScopeIterator)(nil)

// ScopeCount returns 1 for message scope, or the number of segment instances for a scoped segment.
func (it *HL7ScopeIterator) ScopeCount(scope string) int {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return 1
	}
	if it.Msg == nil {
		return 0
	}
	return it.Msg.SegmentInstanceCount(scope)
}

// ProgramOptionsAt binds HL7 helpers for the given scope instance (0-based index).
func (it *HL7ScopeIterator) ProgramOptionsAt(scope string, index int) []celgo.ProgramOption {
	return BindMessage(it.Msg, strings.TrimSpace(scope), index, it.Reg)
}

// ErrorPath builds a path like OBX[1]-5 when scope is set.
func (it *HL7ScopeIterator) ErrorPath(rule icel.InputRule, index int) string {
	base := strings.TrimSpace(rule.ErrorPath)
	if base == "" {
		base = rule.Name
	}
	sc := strings.TrimSpace(rule.Scope)
	if sc == "" {
		return base
	}
	parts := strings.SplitN(base, "-", 2)
	if len(parts) == 2 {
		return parts[0] + fmt.Sprintf("[%d]", index+1) + "-" + parts[1]
	}
	return base + fmt.Sprintf("[%d]", index+1)
}
