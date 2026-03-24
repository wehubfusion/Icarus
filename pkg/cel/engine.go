package cel

import (
	"fmt"
	"strings"

	celgo "github.com/google/cel-go/cel"
)

// Engine wraps a CEL environment for compile-once / evaluate-many usage.
type Engine struct {
	env *celgo.Env
}

// NewEnv returns the underlying cel.Env (for advanced use).
func (e *Engine) Env() *celgo.Env { return e.env }

// NewEngine builds an engine from cel environment options (function declarations, stdlib, etc.).
func NewEngine(opts ...celgo.EnvOption) (*Engine, error) {
	env, err := celgo.NewEnv(opts...)
	if err != nil {
		return nil, err
	}
	return &Engine{env: env}, nil
}

// Compile parses and type-checks all rules. Call once per schema load.
func (e *Engine) Compile(rules []InputRule) ([]CompiledRule, error) {
	var out []CompiledRule
	for _, r := range rules {
		if strings.TrimSpace(r.Assert) == "" {
			return nil, fmt.Errorf("rule %s: assert expression is required", r.ID)
		}
		whenAst, err := e.compileExpr(r.When)
		if err != nil {
			return nil, fmt.Errorf("rule %s when: %w", r.ID, err)
		}
		assertAst, err := e.compileExpr(r.Assert)
		if err != nil {
			return nil, fmt.Errorf("rule %s assert: %w", r.ID, err)
		}
		if assertAst == nil {
			return nil, fmt.Errorf("rule %s: assert compiled to nil", r.ID)
		}
		if !assertAst.OutputType().IsAssignableType(celgo.BoolType) {
			return nil, fmt.Errorf("rule %s: assert must return bool, got %s", r.ID, assertAst.OutputType())
		}
		if whenAst != nil && !whenAst.OutputType().IsAssignableType(celgo.BoolType) {
			return nil, fmt.Errorf("rule %s: when must return bool, got %s", r.ID, whenAst.OutputType())
		}
		out = append(out, CompiledRule{Rule: r, WhenAst: whenAst, AssertAst: assertAst})
	}
	return out, nil
}

func (e *Engine) compileExpr(expr string) (*celgo.Ast, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, nil
	}
	ast, iss := e.env.Parse(expr)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	checked, iss := e.env.Check(ast)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	return checked, nil
}

// EvaluateRules runs compiled rules against the iterator (typically one message).
func (e *Engine) EvaluateRules(rules []CompiledRule, iter ScopeIterator) ([]Violation, []EvalError) {
	var violations []Violation
	var evalErrs []EvalError
	for _, cr := range rules {
		n := iter.IterationCount(cr.Rule)
		if n == 0 {
			continue
		}
		for i := 0; i < n; i++ {
			// Extend the base env with per-instance runtime bindings (e.g. live HL7 message
			// context). This replaces the deprecated celgo.Functions() ProgramOption.
			boundEnv, err := e.env.Extend(iter.EnvOptionsAt(cr.Rule, i)...)
			if err != nil {
				evalErrs = append(evalErrs, EvalError{RuleID: cr.Rule.ID, Expr: "when", Err: err})
				continue
			}
			if cr.WhenAst != nil {
				whenPrg, err := boundEnv.Program(cr.WhenAst)
				if err != nil {
					evalErrs = append(evalErrs, EvalError{RuleID: cr.Rule.ID, Expr: "when", Err: err})
					continue
				}
				ok, err := e.evalBool(whenPrg)
				if err != nil {
					evalErrs = append(evalErrs, EvalError{RuleID: cr.Rule.ID, Expr: "when", Err: err})
					continue
				}
				if !ok {
					continue
				}
			}
			assertPrg, err := boundEnv.Program(cr.AssertAst)
			if err != nil {
				evalErrs = append(evalErrs, EvalError{RuleID: cr.Rule.ID, Expr: "assert", Err: err})
				continue
			}
			ok, err := e.evalBool(assertPrg)
			if err != nil {
				evalErrs = append(evalErrs, EvalError{RuleID: cr.Rule.ID, Expr: "assert", Err: err})
				continue
			}
			if ok {
				continue
			}
			msg := strings.TrimSpace(cr.Rule.Message)
			if msg == "" {
				msg = "rule assertion failed"
			}
			sev := strings.TrimSpace(cr.Rule.Severity)
			if sev == "" {
				sev = "ERROR"
			}
			violations = append(violations, Violation{
				RuleID:   cr.Rule.ID,
				RuleName: cr.Rule.Name,
				Path:     iter.ErrorPath(cr.Rule, i),
				Message:  msg,
				Severity: sev,
			})
		}
	}
	return violations, evalErrs
}

func (e *Engine) evalBool(prg celgo.Program) (bool, error) {
	out, _, err := prg.Eval(celgo.NoVars())
	if err != nil {
		return false, err
	}
	if b, ok := out.Value().(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("expected bool result, got %v", out)
}
