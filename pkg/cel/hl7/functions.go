package celhl7

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/google/cel-go/common/functions"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/primitive"
)

func stringVal(v ref.Val) string { return fmt.Sprint(v.Value()) }

func intVal(v ref.Val) int {
	switch x := v.(type) {
	case celtypes.Int:
		return int(x)
	default:
		i, _ := strconv.Atoi(stringVal(v))
		return i
	}
}

// hl7ProgramOverloads returns runtime bindings for all HL7 helpers (must match hl7FunctionDeclarations).
func hl7ProgramOverloads(ctx *bindCtx) []*functions.Overload {
	return []*functions.Overload{
		{Operator: "msg_string", Unary: func(v ref.Val) ref.Val { return celtypes.String(msgGet(ctx, stringVal(v))) }},
		{Operator: "valued_string", Unary: func(v ref.Val) ref.Val {
			s := msgGet(ctx, stringVal(v))
			return celtypes.Bool(strings.TrimSpace(s) != "" && s != `""`)
		}},
		{Operator: "segCount_string", Unary: func(v ref.Val) ref.Val {
			if ctx.msg == nil {
				return celtypes.Int(0)
			}
			return celtypes.Int(ctx.msg.SegmentInstanceCount(stringVal(v)))
		}},
		{Operator: "repCount_string", Unary: func(v ref.Val) ref.Val {
			return celtypes.Int(repCountImpl(ctx, stringVal(v)))
		}},
		{Operator: "validateAs_string_string", Binary: func(lhs, rhs ref.Val) ref.Val {
			return celtypes.Bool(validateAsImpl(ctx, stringVal(lhs), stringVal(rhs)))
		}},
		{Operator: "matchesPattern_string_string", Binary: func(lhs, rhs ref.Val) ref.Val {
			return celtypes.Bool(matchesPatternImpl(ctx, stringVal(lhs), stringVal(rhs)))
		}},
		{Operator: "toDTM_string", Unary: func(v ref.Val) ref.Val {
			s := msgGet(ctx, stringVal(v))
			t, err := parseHL7DTM(s)
			if err != nil {
				return celtypes.Timestamp{Time: time.Unix(0, 0).UTC()}
			}
			return celtypes.Timestamp{Time: t}
		}},
		{Operator: "toNumber_string", Unary: func(v ref.Val) ref.Val {
			s := msgGet(ctx, stringVal(v))
			if strings.TrimSpace(s) == "" {
				return celtypes.Double(0)
			}
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return celtypes.Double(0)
			}
			return celtypes.Double(f)
		}},
		{Operator: "msgAt_string_int_string", Function: func(args ...ref.Val) ref.Val {
			if len(args) < 3 {
				return celtypes.String("")
			}
			return celtypes.String(msgAtImpl(ctx.msg, stringVal(args[0]), intVal(args[1]), stringVal(args[2])))
		}},
		{Operator: "segIndices_string", Unary: func(v ref.Val) ref.Val {
			return segIndicesImpl(ctx.msg, stringVal(v))
		}},
		{Operator: "msgInGroup_string_int_string", Function: func(args ...ref.Val) ref.Val {
			if len(args) < 3 {
				return celtypes.String("")
			}
			return celtypes.String(msgInGroupImpl(ctx.msg, stringVal(args[0]), intVal(args[1]), stringVal(args[2])))
		}},
	}
}

func repCountImpl(ctx *bindCtx, loc string) int {
	if ctx.msg == nil {
		return 0
	}
	segName, field, _, _, _ := hl7msg.LocationParts(loc)
	if segName == "" || field <= 0 {
		return 0
	}
	var seg *hl7msg.Segment
	if ctx.scopeSeg != "" && strings.EqualFold(segName, ctx.scopeSeg) {
		seg = ctx.msg.NthSegmentByName(ctx.scopeSeg, ctx.instanceIdx0+1)
	} else {
		seg = ctx.msg.NthSegmentByName(segName, 1)
	}
	if seg == nil {
		return 0
	}
	f, ok := seg.FieldAt(field)
	if !ok {
		return 0
	}
	return len(f.Repetitions)
}

func validateAsImpl(ctx *bindCtx, typeOrLoc, valueLoc string) bool {
	if ctx.msg == nil || ctx.reg == nil {
		return true
	}
	typeOrLoc = strings.TrimSpace(typeOrLoc)
	valueLoc = strings.TrimSpace(valueLoc)
	var tid string
	if strings.Contains(typeOrLoc, "-") {
		tid = strings.TrimSpace(strings.ToUpper(msgGet(ctx, typeOrLoc)))
	} else {
		tid = strings.TrimSpace(strings.ToUpper(typeOrLoc))
	}
	val := msgGet(ctx, valueLoc)
	return primitive.ValidatePrimitiveType(tid, val, ctx.reg, ctx.version)
}

func matchesPatternImpl(ctx *bindCtx, loc, pattern string) bool {
	val := msgGet(ctx, loc)
	if strings.TrimSpace(val) == "" {
		return true
	}
	re, err := regexp2.Compile(pattern, regexp2.None)
	if err != nil {
		return false
	}
	re.MatchTimeout = 50 * time.Millisecond
	ok, _ := re.MatchString(val)
	return ok
}

func msgAtImpl(msg *hl7msg.Message, seg string, idx int, loc string) string {
	if msg == nil || idx < 1 {
		return ""
	}
	s := msg.NthSegmentByName(seg, idx)
	if s == nil {
		return ""
	}
	return hl7msg.FieldValueOnSegment(s, loc)
}

func segIndicesImpl(msg *hl7msg.Message, seg string) ref.Val {
	if msg == nil {
		return celtypes.NewDynamicList(celtypes.DefaultTypeAdapter, []any{})
	}
	n := msg.SegmentInstanceCount(seg)
	if n == 0 {
		return celtypes.NewDynamicList(celtypes.DefaultTypeAdapter, []any{})
	}
	elts := make([]any, n)
	for i := 0; i < n; i++ {
		elts[i] = int64(i + 1)
	}
	return celtypes.DefaultTypeAdapter.NativeToValue(elts)
}

func msgInGroupImpl(msg *hl7msg.Message, groupStart string, groupIdx int, loc string) string {
	if msg == nil {
		return ""
	}
	groupStart = strings.ToUpper(strings.TrimSpace(groupStart))
	segName, _, _, _, _ := hl7msg.LocationParts(loc)
	if segName == "" {
		return ""
	}
	if strings.EqualFold(segName, groupStart) {
		s := msg.NthSegmentByName(groupStart, groupIdx+1)
		if s == nil {
			return ""
		}
		return hl7msg.FieldValueOnSegment(s, loc)
	}
	var starts []int
	for i := range msg.Segments {
		if strings.EqualFold(msg.Segments[i].Name, groupStart) {
			starts = append(starts, i)
		}
	}
	if groupIdx < 0 || groupIdx >= len(starts) {
		return ""
	}
	start := starts[groupIdx]
	end := len(msg.Segments)
	if groupIdx+1 < len(starts) {
		end = starts[groupIdx+1]
	}
	want := strings.ToUpper(strings.TrimSpace(segName))
	for i := start; i < end; i++ {
		if strings.EqualFold(msg.Segments[i].Name, want) {
			return hl7msg.FieldValueOnSegment(&msg.Segments[i], loc)
		}
	}
	return ""
}
