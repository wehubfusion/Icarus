package celhl7

import (
	"sync"

	celgo "github.com/google/cel-go/cel"
	icel "github.com/wehubfusion/Icarus/pkg/cel"
)

// NewHL7Engine returns a fresh CEL engine with stdlib and HL7 helper declarations.
// Implementations are supplied at evaluation time via Env.Extend (late-bound).
// Unit tests that need an isolated environment should call this directly.
func NewHL7Engine() (*icel.Engine, error) {
	opts := append([]celgo.EnvOption{celgo.StdLib()}, hl7FunctionDeclarations()...)
	return icel.NewEngine(opts...)
}

// Engine returns the process-wide singleton HL7 CEL engine.
// Unit tests that need a fresh/isolated CEL environment should call NewHL7Engine() directly.
func Engine() (*icel.Engine, error) {
	hl7EngineOnce.Do(func() {
		hl7Engine, hl7EngineErr = NewHL7Engine()
	})
	return hl7Engine, hl7EngineErr
}

var (
	hl7EngineOnce sync.Once
	hl7Engine     *icel.Engine
	hl7EngineErr  error
)

// hl7FunctionDeclarations registers type signatures with LateFunctionBinding so the
// type-checker accepts calls to these functions without needing implementations at
// compile time. Implementations are merged in at evaluation time via Env.Extend.
func hl7FunctionDeclarations() []celgo.EnvOption {
	return []celgo.EnvOption{
		celgo.Function("msg", celgo.Overload("msg_string", []*celgo.Type{celgo.StringType}, celgo.StringType, celgo.LateFunctionBinding())),
		celgo.Function("valued", celgo.Overload("valued_string", []*celgo.Type{celgo.StringType}, celgo.BoolType, celgo.LateFunctionBinding())),
		celgo.Function("segCount", celgo.Overload("segCount_string", []*celgo.Type{celgo.StringType}, celgo.IntType, celgo.LateFunctionBinding())),
		celgo.Function("repCount", celgo.Overload("repCount_string", []*celgo.Type{celgo.StringType}, celgo.IntType, celgo.LateFunctionBinding())),
		celgo.Function("validateAs", celgo.Overload("validateAs_string_string", []*celgo.Type{celgo.StringType, celgo.StringType}, celgo.BoolType, celgo.LateFunctionBinding())),
		celgo.Function("matchesPattern", celgo.Overload("matchesPattern_string_string", []*celgo.Type{celgo.StringType, celgo.StringType}, celgo.BoolType, celgo.LateFunctionBinding())),
		celgo.Function("toDTM", celgo.Overload("toDTM_string", []*celgo.Type{celgo.StringType}, celgo.TimestampType, celgo.LateFunctionBinding())),
		celgo.Function("toNumber", celgo.Overload("toNumber_string", []*celgo.Type{celgo.StringType}, celgo.DoubleType, celgo.LateFunctionBinding())),
	}
}
