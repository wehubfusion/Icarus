package simplecondition

import "fmt"

// ComparisonOperator defines the type of comparison to perform.
type ComparisonOperator string

const (
	// Equality operators
	OpEquals    ComparisonOperator = "equals"
	OpNotEquals ComparisonOperator = "not_equals"

	// Numeric comparison operators
	OpGreaterThan        ComparisonOperator = "greater_than"
	OpLessThan           ComparisonOperator = "less_than"
	OpGreaterThanOrEqual ComparisonOperator = "greater_than_or_equal"
	OpLessThanOrEqual    ComparisonOperator = "less_than_or_equal"

	// String operators
	OpContains    ComparisonOperator = "contains"
	OpNotContains ComparisonOperator = "not_contains"
	OpStartsWith  ComparisonOperator = "starts_with"
	OpEndsWith    ComparisonOperator = "ends_with"
	OpRegex       ComparisonOperator = "regex"

	// Collection operators
	OpIn    ComparisonOperator = "in"
	OpNotIn ComparisonOperator = "not_in"

	// Existence operators
	OpIsEmpty    ComparisonOperator = "is_empty"
	OpIsNotEmpty ComparisonOperator = "is_not_empty"
)

// DataType specifies how to interpret values.
type DataType string

const (
	DataTypeAuto    DataType = "auto"
	DataTypeString  DataType = "string"
	DataTypeNumber  DataType = "number"
	DataTypeBoolean DataType = "boolean"
	DataTypeNull    DataType = "null"
)

// LogicOperator defines how to combine multiple conditions.
type LogicOperator string

const (
	LogicAnd LogicOperator = "AND"
	LogicOr  LogicOperator = "OR"
)

// Config defines the configuration for the simple condition processor.
type Config struct {
	LogicOperator LogicOperator `json:"logic_operator,omitempty"`
	Conditions    []Condition   `json:"conditions"`
}

// Condition defines a single condition to evaluate.
type Condition struct {
	Name            string             `json:"name"`
	FieldPath       string             `json:"field_path"`
	Operator        ComparisonOperator `json:"operator"`
	ExpectedValue   interface{}        `json:"expected_value,omitempty"`
	DataType        DataType           `json:"data_type,omitempty"`
	CaseInsensitive bool               `json:"case_insensitive,omitempty"`
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.LogicOperator == "" {
		c.LogicOperator = LogicAnd
	}
	if c.LogicOperator != LogicAnd && c.LogicOperator != LogicOr {
		return fmt.Errorf("invalid logic operator '%s', must be 'AND' or 'OR'", c.LogicOperator)
	}
	if len(c.Conditions) == 0 {
		return fmt.Errorf("at least one condition must be specified")
	}

	names := make(map[string]bool)
	for i, cond := range c.Conditions {
		if err := cond.Validate(); err != nil {
			return fmt.Errorf("condition at index %d: %w", i, err)
		}
		if names[cond.Name] {
			return fmt.Errorf("duplicate condition name '%s'", cond.Name)
		}
		names[cond.Name] = true
	}
	return nil
}

// Validate checks if a condition is valid.
func (c *Condition) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("condition name is required")
	}
	if c.FieldPath == "" {
		return fmt.Errorf("field_path is required")
	}
	if c.Operator == "" {
		return fmt.Errorf("operator is required")
	}

	validOperators := []ComparisonOperator{
		OpEquals, OpNotEquals,
		OpGreaterThan, OpLessThan, OpGreaterThanOrEqual, OpLessThanOrEqual,
		OpContains, OpNotContains, OpStartsWith, OpEndsWith, OpRegex,
		OpIn, OpNotIn,
		OpIsEmpty, OpIsNotEmpty,
	}

	valid := false
	if len(validOperators) > 0 {
		for _, op := range validOperators {
			if c.Operator == op {
				valid = true
				break
			}
		}
	}
	if !valid {
		return fmt.Errorf("unsupported operator '%s'", c.Operator)
	}

	if c.Operator != OpIsEmpty && c.Operator != OpIsNotEmpty && c.ExpectedValue == nil {
		return fmt.Errorf("expected_value is required for operator '%s'", c.Operator)
	}

	if c.DataType == "" {
		c.DataType = DataTypeAuto
	}
	return nil
}
