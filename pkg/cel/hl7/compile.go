package celhl7

import (
	"fmt"
	"regexp"
	"strings"

	icel "github.com/wehubfusion/Icarus/pkg/cel"
)

// reHL7Location matches valid HL7 v2 field/component/sub-component location tokens
// (e.g. "MSH-9", "PID-3(2).1", "OBX-5"). Used to guard Require/Forbid strings
// before embedding them in generated CEL expressions.
var reHL7Location = regexp.MustCompile(`^[A-Z][A-Z0-9]{2}(-\d+(\(\d+\))?(\.\d+(\.\d+)?)?)?$`)

// ValidateCELRuleMetadata checks rule IDs and assertion modes without compiling CEL.
func ValidateCELRuleMetadata(rules []CELRule) error {
	seen := map[string]bool{}
	for i, r := range rules {
		if err := validateCELRuleStruct(&r, i, seen); err != nil {
			return err
		}
	}
	return nil
}

// CompileHL7Rules validates rule metadata and compiles CEL expressions.
// Scope is not stored — it is inferred dynamically at evaluation time by HL7ScopeIterator.
func CompileHL7Rules(engine *icel.Engine, rules []CELRule) ([]icel.CompiledRule, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	if engine == nil {
		return nil, fmt.Errorf("cel engine is nil")
	}
	if err := ValidateCELRuleMetadata(rules); err != nil {
		return nil, err
	}
	var inputs []icel.InputRule
	for _, r := range rules {
		assert, err := expandAssert(r)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, icel.InputRule{
			ID:        r.ID,
			Name:      r.Name,
			When:      r.When,
			Assert:    assert,
			Message:   r.Message,
			ErrorPath: r.ErrorPath,
			Severity:  r.Severity,
		})
	}
	return engine.Compile(inputs)
}

func validateCELRuleStruct(r *CELRule, idx int, seen map[string]bool) error {
	path := fmt.Sprintf("rules[%d]", idx)
	id := strings.TrimSpace(r.ID)
	if id == "" {
		return fmt.Errorf("%s: rule id is required", path)
	}
	if seen[id] {
		return fmt.Errorf("%s: duplicate rule id %q", path, id)
	}
	seen[id] = true
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("%s: rule name is required", path)
	}
	n := 0
	if strings.TrimSpace(r.Assert) != "" {
		n++
	}
	if req := strings.TrimSpace(r.Require); req != "" {
		n++
		if !reHL7Location.MatchString(req) {
			return fmt.Errorf("%s: require value %q is not a valid HL7 location (expected e.g. OBX-5 or PID-3.1)", path, req)
		}
	}
	if fbd := strings.TrimSpace(r.Forbid); fbd != "" {
		n++
		if !reHL7Location.MatchString(fbd) {
			return fmt.Errorf("%s: forbid value %q is not a valid HL7 location (expected e.g. OBX-5 or PID-3.1)", path, fbd)
		}
	}
	if n != 1 {
		return fmt.Errorf("%s: exactly one of assert, require, forbid must be set", path)
	}
	if s := strings.TrimSpace(r.Severity); s != "" {
		switch strings.ToUpper(s) {
		case "ERROR", "WARNING", "INFO":
		default:
			return fmt.Errorf("%s: severity must be ERROR, WARNING, or INFO", path)
		}
	}
	return nil
}

func expandAssert(r CELRule) (string, error) {
	if strings.TrimSpace(r.Assert) != "" {
		return r.Assert, nil
	}
	if strings.TrimSpace(r.Require) != "" {
		return fmt.Sprintf("valued('%s')", strings.TrimSpace(r.Require)), nil
	}
	if strings.TrimSpace(r.Forbid) != "" {
		return fmt.Sprintf("!valued('%s')", strings.TrimSpace(r.Forbid)), nil
	}
	return "", fmt.Errorf("rule %s: no assertion", r.ID)
}
