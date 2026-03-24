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
