package celhl7

import (
	icel "github.com/wehubfusion/Icarus/pkg/cel"
	celgo "github.com/google/cel-go/cel"
)

// NewHL7Engine returns a CEL engine with stdlib and HL7 helper declarations (late-bound at program creation).
func NewHL7Engine() (*icel.Engine, error) {
	opts := []celgo.EnvOption{celgo.StdLib()}
	opts = append(opts, hl7FunctionDeclarations()...)
	return icel.NewEngine(opts...)
}

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
		celgo.Function("msgAt", celgo.Overload("msgAt_string_int_string", []*celgo.Type{celgo.StringType, celgo.IntType, celgo.StringType}, celgo.StringType, celgo.LateFunctionBinding())),
		celgo.Function("segIndices", celgo.Overload("segIndices_string", []*celgo.Type{celgo.StringType}, celgo.ListType(celgo.IntType), celgo.LateFunctionBinding())),
		celgo.Function("msgInGroup", celgo.Overload("msgInGroup_string_int_string", []*celgo.Type{celgo.StringType, celgo.IntType, celgo.StringType}, celgo.StringType, celgo.LateFunctionBinding())),
	}
}
