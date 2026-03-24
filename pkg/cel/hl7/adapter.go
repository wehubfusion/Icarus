package celhl7

import (
	"strings"

	celgo "github.com/google/cel-go/cel"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
)

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
	return &bindCtx{msg: msg, scopeSeg: strings.TrimSpace(scopeSeg), instanceIdx0: instanceIdx0, reg: reg, version: v}
}

// BindMessage returns EnvOptions that bind all HL7 helper functions to the given message context.
// Using Function()+UnaryBinding/BinaryBinding/FunctionBinding is the non-deprecated replacement
// for the old celgo.Functions([]*functions.Overload) ProgramOption.
func BindMessage(msg *hl7msg.Message, scopeSeg string, instanceIdx0 int, reg *datatypes.Registry) []celgo.EnvOption {
	ctx := newBindCtx(msg, scopeSeg, instanceIdx0, reg)
	return hl7EnvOptions(ctx)
}
