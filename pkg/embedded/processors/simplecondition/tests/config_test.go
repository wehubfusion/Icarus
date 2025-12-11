package tests

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
)

func TestConfigValidateDefaultsAndDuplicates(t *testing.T) {
	cfg := simplecondition.Config{
		Conditions: []simplecondition.Condition{
			{Name: "c1", FieldPath: "a", Operator: simplecondition.OpEquals, ExpectedValue: "x"},
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
		Conditions: []simplecondition.Condition{
			{Name: "dup", FieldPath: "a", Operator: simplecondition.OpEquals, ExpectedValue: 1},
			{Name: "dup", FieldPath: "b", Operator: simplecondition.OpEquals, ExpectedValue: 2},
		},
	}
	if err := dup.Validate(); err == nil {
		t.Fatalf("expected duplicate condition name error")
	}
}

func TestConditionValidateErrors(t *testing.T) {
	cases := []simplecondition.Condition{
		{},
		{Name: "c", FieldPath: "", Operator: simplecondition.OpEquals},
		{Name: "c", FieldPath: "a", Operator: "bad"},
		{Name: "c", FieldPath: "a", Operator: simplecondition.OpEquals}, // missing expected_value
	}
	for i, cond := range cases {
		if err := cond.Validate(); err == nil {
			t.Fatalf("expected validation error for case %d", i)
		}
	}

	// operators that do not require expected_value
	cond := simplecondition.Condition{Name: "c", FieldPath: "a", Operator: simplecondition.OpIsEmpty}
	if err := cond.Validate(); err != nil {
		t.Fatalf("expected is_empty to be valid without expected_value, got %v", err)
	}
}
