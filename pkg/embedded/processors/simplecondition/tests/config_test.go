package tests

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
)

func TestConfigValidateDefaultsAndDuplicates(t *testing.T) {
	cfg := simplecondition.Config{
		ManualInputs: []simplecondition.ManualInput{
			{Name: "c1", Type: simplecondition.DataTypeString, Value: "x", Operator: simplecondition.OpEquals},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	if cfg.LogicOperator != simplecondition.LogicAnd {
		t.Fatalf("expected default logic operator AND, got %s", cfg.LogicOperator)
	}

	// duplicate names should fail
	dup := simplecondition.Config{
		LogicOperator: simplecondition.LogicOr,
		ManualInputs: []simplecondition.ManualInput{
			{Name: "dup", Type: simplecondition.DataTypeNumber, Value: "1", Operator: simplecondition.OpEquals},
			{Name: "dup", Type: simplecondition.DataTypeNumber, Value: "2", Operator: simplecondition.OpEquals},
		},
	}
	if err := dup.Validate(); err == nil {
		t.Fatalf("expected duplicate condition name error")
	}
}

func TestConditionValidateErrors(t *testing.T) {
	cases := []simplecondition.ManualInput{
		{}, // missing all fields
		{Name: "c", Type: "", Value: "x", Operator: simplecondition.OpEquals},                            // missing type
		{Name: "c", Type: simplecondition.DataTypeString, Value: "x", Operator: ""},                      // missing operator
		{Name: "", Type: simplecondition.DataTypeString, Value: "x", Operator: simplecondition.OpEquals}, // missing name
		{Name: "c", Type: "invalid", Value: "x", Operator: simplecondition.OpEquals},                     // invalid type
		{Name: "c", Type: simplecondition.DataTypeString, Value: "x", Operator: "bad"},                   // invalid operator
	}
	for i, input := range cases {
		if err := input.Validate(); err == nil {
			t.Fatalf("expected validation error for case %d", i)
		}
	}

	// Test that empty value is allowed (e.g., checking if field equals "")
	validEmpty := simplecondition.ManualInput{
		Name:     "test",
		Type:     simplecondition.DataTypeString,
		Value:    "",
		Operator: simplecondition.OpEquals,
	}
	if err := validEmpty.Validate(); err != nil {
		t.Fatalf("expected empty value to be valid, got error: %v", err)
	}
}

func TestOperatorTypeCompatibility(t *testing.T) {
	// Test that string-only operators fail with non-string types
	stringOnlyOps := []simplecondition.ComparisonOperator{
		simplecondition.OpContains,
		simplecondition.OpNotContains,
		simplecondition.OpStartsWith,
		simplecondition.OpEndsWith,
		simplecondition.OpRegex,
	}

	for _, op := range stringOnlyOps {
		// Should fail with number type
		input := simplecondition.ManualInput{
			Name:     "test",
			Type:     simplecondition.DataTypeNumber,
			Value:    "123",
			Operator: op,
		}
		if err := input.Validate(); err == nil {
			t.Fatalf("expected validation error for operator %s with number type", op)
		}

		// Should fail with boolean type
		input.Type = simplecondition.DataTypeBoolean
		if err := input.Validate(); err == nil {
			t.Fatalf("expected validation error for operator %s with boolean type", op)
		}
	}

	// Test that comparison operators fail with boolean type
	comparisonOps := []simplecondition.ComparisonOperator{
		simplecondition.OpGreaterThan,
		simplecondition.OpLessThan,
		simplecondition.OpGreaterThanOrEqual,
		simplecondition.OpLessThanOrEqual,
	}

	for _, op := range comparisonOps {
		input := simplecondition.ManualInput{
			Name:     "test",
			Type:     simplecondition.DataTypeBoolean,
			Value:    "true",
			Operator: op,
		}
		if err := input.Validate(); err == nil {
			t.Fatalf("expected validation error for operator %s with boolean type", op)
		}
	}
}

func TestValueConversion(t *testing.T) {
	// Test string conversion
	input := simplecondition.ManualInput{
		Name:     "test",
		Type:     simplecondition.DataTypeString,
		Value:    "hello",
		Operator: simplecondition.OpEquals,
	}
	val, err := input.ConvertValue()
	if err != nil {
		t.Fatalf("unexpected error converting string: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %v", val)
	}

	// Test number conversion
	input.Type = simplecondition.DataTypeNumber
	input.Value = "123.45"
	val, err = input.ConvertValue()
	if err != nil {
		t.Fatalf("unexpected error converting number: %v", err)
	}
	if f, ok := val.(float64); !ok || f != 123.45 {
		t.Fatalf("expected 123.45, got %v", val)
	}

	// Test boolean conversion
	input.Type = simplecondition.DataTypeBoolean
	input.Value = "true"
	val, err = input.ConvertValue()
	if err != nil {
		t.Fatalf("unexpected error converting boolean: %v", err)
	}
	if b, ok := val.(bool); !ok || !b {
		t.Fatalf("expected true, got %v", val)
	}

	// Test invalid number conversion
	input.Type = simplecondition.DataTypeNumber
	input.Value = "not a number"
	_, err = input.ConvertValue()
	if err == nil {
		t.Fatalf("expected error converting invalid number")
	}

	// Test invalid boolean conversion
	input.Type = simplecondition.DataTypeBoolean
	input.Value = "maybe"
	_, err = input.ConvertValue()
	if err == nil {
		t.Fatalf("expected error converting invalid boolean")
	}
}
