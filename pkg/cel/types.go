package cel

import celgo "github.com/google/cel-go/cel"

// InputRule is the user-facing normalized rule fed to the engine.
type InputRule struct {
	ID, Name, When, Assert string
	Message                string
	ErrorPath              string
	Severity               string
}

// CompiledRule holds type-checked ASTs for one rule.
// Iteration scope is inferred dynamically by the ScopeIterator at evaluation time.
type CompiledRule struct {
	Rule      InputRule
	WhenAst   *celgo.Ast
	AssertAst *celgo.Ast
}

// Violation is a failed custom rule assertion.
type Violation struct {
	RuleID   string
	RuleName string
	Path     string
	Message  string
	Severity string
}

// EvalError is a failure during binding or CEL expression evaluation.
type EvalError struct {
	RuleID string
	Expr   string // "bind", "when", or "assert"
	Err    error
}

// ScopeIterator supplies per-instance bindings and error paths for evaluation.
// The implementor is responsible for all domain-specific iteration logic
// (e.g. inferring which segment a rule targets from its expression text).
type ScopeIterator interface {
	// IterationCount returns how many times the rule should run.
	// Return 0 to skip the rule entirely (e.g. the target segment is absent).
	IterationCount(rule InputRule) int
	// EnvOptionsAt returns EnvOptions that bind runtime function implementations
	// for the i-th instance (0-based). The engine extends its base Env with these
	// options before creating the CEL program for this instance.
	EnvOptionsAt(rule InputRule, index int) []celgo.EnvOption
	// ErrorPath builds the validation issue path for the i-th instance.
	ErrorPath(rule InputRule, index int) string
}
