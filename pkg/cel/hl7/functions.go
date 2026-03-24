package celhl7

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	celgo "github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/primitive"
)

// compiledPatternCache stores compiled regexp2 patterns keyed by pattern string.
// Rules typically use constant patterns, so compiling once per unique pattern
// avoids repeated compilation on every rule evaluation.
var compiledPatternCache sync.Map // string → *regexp2.Regexp

func stringVal(v ref.Val) string { return fmt.Sprint(v.Value()) }


func msgGet(ctx *bindCtx, loc string) string {
	if ctx.msg == nil {
		return ""
	}
	if ctx.scopeSeg != "" {
		seg, _, _, _, _ := hl7msg.LocationParts(loc)
		if seg != "" && strings.EqualFold(seg, ctx.scopeSeg) {
			return ctx.msg.GetAtSegmentInstance(ctx.scopeSeg, ctx.instanceIdx0+1, loc)
		}
	}
	return ctx.msg.Get(loc)
}

// hl7EnvOptions returns EnvOptions that bind all HL7 helper implementations to ctx.
// Each Function() call overrides the LateFunctionBinding() placeholder declared in engine.go.
func hl7EnvOptions(ctx *bindCtx) []celgo.EnvOption {
	return []celgo.EnvOption{
		celgo.Function("msg",
			celgo.Overload("msg_string", []*celgo.Type{celgo.StringType}, celgo.StringType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					return celtypes.String(msgGet(ctx, stringVal(v)))
				}))),
		celgo.Function("valued",
			celgo.Overload("valued_string", []*celgo.Type{celgo.StringType}, celgo.BoolType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					// Trim first so that a value like `  ""  ` (space-padded HL7 blank)
					// is treated identically to `""`.
					s := strings.TrimSpace(msgGet(ctx, stringVal(v)))
					return celtypes.Bool(s != "" && s != `""`)
				}))),
		celgo.Function("validateAs",
			celgo.Overload("validateAs_string_string", []*celgo.Type{celgo.StringType, celgo.StringType}, celgo.BoolType,
				celgo.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
					// lhs = HL7 location to validate ('OBX-5', 'OBX-5.1', ...)
					// rhs = type code as a plain string — either a literal ('NM', 'DT')
					//       or the result of msg('OBX-2') evaluated by CEL first
					return celtypes.Bool(validateAsImpl(ctx, stringVal(lhs), stringVal(rhs)))
				}))),
		celgo.Function("matchesPattern",
			celgo.Overload("matchesPattern_string_string", []*celgo.Type{celgo.StringType, celgo.StringType}, celgo.BoolType,
				celgo.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
					ok, err := matchesPatternImpl(ctx, stringVal(lhs), stringVal(rhs))
					if err != nil {
						// Returning cel-go's `error` ref.Val forces CEL evaluation to fail,
						// which the generic engine surfaces as EvalError (HL7_CEL_EVAL_ERROR).
						return celtypes.WrapErr(err)
					}
					return celtypes.Bool(ok)
				}))),
		celgo.Function("toDTM",
			celgo.Overload("toDTM_string", []*celgo.Type{celgo.StringType}, celgo.TimestampType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					s := msgGet(ctx, stringVal(v))
					t, err := parseHL7DTM(s)
					if err != nil {
						return celtypes.WrapErr(err)
					}
					return celtypes.Timestamp{Time: t}
				}))),
		celgo.Function("toNumber",
			celgo.Overload("toNumber_string", []*celgo.Type{celgo.StringType}, celgo.DoubleType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					s := strings.TrimSpace(msgGet(ctx, stringVal(v)))
					if s == "" {
						return celtypes.Double(0)
					}
					f, err := strconv.ParseFloat(s, 64)
					if err != nil {
						// Non-numeric content is a data error, not a missing-field case.
						// WrapErr surfaces this as HL7_CEL_EVAL_ERROR rather than silently
						// treating "ABC" as 0 and producing a wrong comparison result.
						return celtypes.WrapErr(fmt.Errorf("toNumber: %q is not a valid number", s))
					}
					return celtypes.Double(f)
				}))),
	}
}

// validateAsImpl checks whether the value at loc conforms to typeCode.
//
// loc      — HL7 location resolved via msgGet ('OBX-5', 'OBX-5.1', …).
// typeCode — HL7 primitive type code as a plain string: either a literal
//
//	('NM', 'DT') or already evaluated by CEL (result of msg('OBX-2')).
func validateAsImpl(ctx *bindCtx, loc, typeCode string) bool {
	if ctx.msg == nil {
		return true
	}
	val := msgGet(ctx, strings.TrimSpace(loc))
	tid := strings.ToUpper(strings.TrimSpace(typeCode))
	return primitive.ValidatePrimitiveType(tid, val, ctx.reg, ctx.version)
}

func matchesPatternImpl(ctx *bindCtx, loc, pattern string) (bool, error) {
	val := strings.TrimSpace(msgGet(ctx, loc))
	if val == "" {
		return true, nil
	}
	re, err := compiledPattern(pattern)
	if err != nil {
		return false, err
	}
	ok, _ := re.MatchString(val)
	return ok, nil
}

// compiledPattern returns a cached *regexp2.Regexp for pattern, compiling it on
// first use. The timeout is set once at compile time so every match respects it
// without per-call overhead.
func compiledPattern(pattern string) (*regexp2.Regexp, error) {
	if v, ok := compiledPatternCache.Load(pattern); ok {
		return v.(*regexp2.Regexp), nil
	}
	re, err := regexp2.Compile(pattern, regexp2.None)
	if err != nil {
		return nil, err
	}
	re.MatchTimeout = 50 * time.Millisecond
	// Store only on success; a bad pattern stays uncached so callers always get the error.
	compiledPatternCache.Store(pattern, re)
	return re, nil
}

// parseHL7DTM parses an HL7 DTM/TS string (partial precision allowed) into UTC.
//
// HL7 DTM format: YYYY[MM[DD[HH[MM[SS[.S[S[S[S]]]]]]]]][+/-ZZZZ]
//
// Partial values are zero-extended (e.g. "202601" → 2026-01-01 00:00:00 UTC).
// A timezone offset, when present, is parsed and applied so the returned time
// is always in UTC. An empty string returns the zero time without error.
func parseHL7DTM(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}

	// Extract and apply timezone offset suffix (+/-HHMM), if present.
	offsetSecs := 0
	body := s
	for _, sign := range []byte{'+', '-'} {
		if idx := strings.LastIndexByte(body, sign); idx > 0 {
			tail := body[idx+1:]
			if len(tail) == 4 {
				if mins, err := strconv.Atoi(tail); err == nil {
					hh := mins / 100
					mm := mins % 100
					total := hh*3600 + mm*60
					if sign == '-' {
						offsetSecs = -total
					} else {
						offsetSecs = total
					}
					body = body[:idx]
					break
				}
			}
		}
	}

	// Strip fractional seconds (.SSSS).
	if dot := strings.IndexByte(body, '.'); dot >= 0 {
		body = body[:dot]
	}

	// Collect only digit characters and zero-pad to 14 digits (YYYYMMDDHHmmss).
	digits := make([]byte, 0, 14)
	for _, c := range body {
		if c >= '0' && c <= '9' {
			digits = append(digits, byte(c))
		}
	}
	for len(digits) < 14 {
		digits = append(digits, '0')
	}
	if len(digits) > 14 {
		digits = digits[:14]
	}

	y, _ := strconv.Atoi(string(digits[0:4]))
	mo, _ := strconv.Atoi(string(digits[4:6]))
	d, _ := strconv.Atoi(string(digits[6:8]))
	h, _ := strconv.Atoi(string(digits[8:10]))
	mi, _ := strconv.Atoi(string(digits[10:12]))
	sec, _ := strconv.Atoi(string(digits[12:14]))

	if y == 0 {
		return time.Time{}, fmt.Errorf("invalid HL7 DTM value: %q", s)
	}

	// Build in the declared local timezone, then convert to UTC.
	loc := time.FixedZone("", offsetSecs)
	t := time.Date(y, time.Month(mo), d, h, mi, sec, 0, loc)
	return t.UTC(), nil
}
