package cel

// Violation is a failed custom rule assertion.
type Violation struct {
	RuleID   string
	RuleName string
	Path     string
	Message  string
	Severity string
}

// EvalError is a failure to evaluate a CEL expression (parse/runtime).
type EvalError struct {
	RuleID string
	Expr   string // "when" or "assert"
	Err    error
}
