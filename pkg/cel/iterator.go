package cel

import celgo "github.com/google/cel-go/cel"

// ScopeIterator supplies per-scope-instance program bindings and error paths for evaluation.
type ScopeIterator interface {
	// ScopeCount returns how many times the rule should run for this scope.
	// Empty scope means message-level: always 1.
	ScopeCount(scope string) int
	ProgramOptionsAt(scope string, index int) []celgo.ProgramOption
	// ErrorPath returns the validation issue path for the rule at the given 0-based index.
	ErrorPath(rule InputRule, index int) string
}
