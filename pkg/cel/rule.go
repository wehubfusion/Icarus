package cel

import celgo "github.com/google/cel-go/cel"

// InputRule is the normalized rule fed to the engine (after domain-specific expansion).
type InputRule struct {
	ID, Name, Scope, When, Assert string
	Message                       string
	ErrorPath                     string
	Severity                      string
}

// CompiledRule holds type-checked ASTs for one rule.
type CompiledRule struct {
	Rule      InputRule
	WhenAst   *celgo.Ast
	AssertAst *celgo.Ast
}
