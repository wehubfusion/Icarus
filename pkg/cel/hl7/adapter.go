package celhl7

import (
	"strings"

	celgo "github.com/google/cel-go/cel"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
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

// BindMessage returns ProgramOptions that bind all HL7 helper functions to the given message context.
func BindMessage(msg *hl7msg.Message, scopeSeg string, instanceIdx0 int, reg *datatypes.Registry) []celgo.ProgramOption {
	ctx := newBindCtx(msg, scopeSeg, instanceIdx0, reg)
	return []celgo.ProgramOption{celgo.Functions(hl7ProgramOverloads(ctx)...)}
}

func msgGet(ctx *bindCtx, loc string) string {
	if ctx.msg == nil {
		return ""
	}
	scope := strings.TrimSpace(ctx.scopeSeg)
	if scope != "" {
		seg, _, _, _, _ := hl7msg.LocationParts(loc)
		if seg != "" && strings.EqualFold(seg, scope) {
			return ctx.msg.GetAtSegmentInstance(scope, ctx.instanceIdx0+1, loc)
		}
	}
	return ctx.msg.Get(loc)
}
