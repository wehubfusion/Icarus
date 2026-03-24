package cel

import celgo "github.com/google/cel-go/cel"

// ScopeIterator supplies per-instance program bindings and error paths for evaluation.
// The implementor is responsible for inferring the iteration scope from the rule
// (e.g. by scanning HL7 location references in the expressions).
type ScopeIterator interface {
	// IterationCount returns how many times the rule should run.
	// Return 0 to skip the rule entirely (e.g. the target segment is absent).
	IterationCount(rule InputRule) int
	// EnvOptionsAt returns EnvOptions that bind runtime function implementations for
	// the i-th instance (0-based). The engine extends its base Env with these options
	// before creating the CEL program for this instance.
	EnvOptionsAt(rule InputRule, index int) []celgo.EnvOption
	// ErrorPath builds the validation issue path for the i-th instance.
	ErrorPath(rule InputRule, index int) string
}
