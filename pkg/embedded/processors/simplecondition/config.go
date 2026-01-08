package simplecondition

import (
	"fmt"
	"strconv"
)

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
)

// DataType specifies how to interpret values.
type DataType string

const (
	DataTypeString  DataType = "string"
	DataTypeNumber  DataType = "number"
	DataTypeBoolean DataType = "boolean"
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
	ManualInputs  []ManualInput `json:"manual_inputs"`
}

// ManualInput defines a single condition input from the new schema.
type ManualInput struct {
	Name     string             `json:"name"`     // Used as field_path to extract value from input data
	Type     DataType           `json:"type"`     // string, number, or boolean
	Value    string             `json:"value"`    // Expected value as string (will be converted based on Type)
	Operator ComparisonOperator `json:"operator"` // Comparison operator
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.LogicOperator == "" {
		c.LogicOperator = LogicAnd
	}
	if c.LogicOperator != LogicAnd && c.LogicOperator != LogicOr {
		return fmt.Errorf("invalid logic operator '%s', must be 'AND' or 'OR'", c.LogicOperator)
	}
	if len(c.ManualInputs) == 0 {
		return fmt.Errorf("at least one condition must be specified")
	}

	names := make(map[string]bool)
	for i, input := range c.ManualInputs {
		if err := input.Validate(); err != nil {
			return fmt.Errorf("condition at index %d: %w", i, err)
		}
		if names[input.Name] {
			return fmt.Errorf("duplicate condition name '%s'", input.Name)
		}
		names[input.Name] = true
	}
	return nil
}

// Validate checks if a manual input is valid.
func (m *ManualInput) Validate() error {
	if m.Name == "" {
		return fmt.Errorf("name is required")
	}
	if m.Type == "" {
		return fmt.Errorf("type is required")
	}
	if m.Type != DataTypeString && m.Type != DataTypeNumber && m.Type != DataTypeBoolean {
		return fmt.Errorf("invalid type '%s', must be 'string', 'number', or 'boolean'", m.Type)
	}
	if m.Operator == "" {
		return fmt.Errorf("operator is required")
	}
	// Note: Value can be empty string (e.g., checking if field equals "")

	validOperators := []ComparisonOperator{
		OpEquals, OpNotEquals,
		OpGreaterThan, OpLessThan, OpGreaterThanOrEqual, OpLessThanOrEqual,
		OpContains, OpNotContains, OpStartsWith, OpEndsWith, OpRegex,
	}

	valid := false
	for _, op := range validOperators {
		if m.Operator == op {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("unsupported operator '%s'", m.Operator)
	}

	// Validate operator-type compatibility
	if err := validateOperatorTypeCompatibility(m.Operator, m.Type); err != nil {
		return err
	}

	return nil
}

// validateOperatorTypeCompatibility checks if the operator is compatible with the data type.
func validateOperatorTypeCompatibility(operator ComparisonOperator, dataType DataType) error {
	switch operator {
	case OpEquals, OpNotEquals:
		// Supported for all types
		return nil
	case OpGreaterThan, OpLessThan, OpGreaterThanOrEqual, OpLessThanOrEqual:
		// Only supported for string and number
		if dataType != DataTypeString && dataType != DataTypeNumber {
			return fmt.Errorf("operator '%s' is only supported for 'string' and 'number' types, got '%s'", operator, dataType)
		}
	case OpContains, OpNotContains, OpStartsWith, OpEndsWith, OpRegex:
		// Only supported for string
		if dataType != DataTypeString {
			return fmt.Errorf("operator '%s' is only supported for 'string' type, got '%s'", operator, dataType)
		}
	default:
		return fmt.Errorf("unsupported operator '%s'", operator)
	}
	return nil
}

// ConvertValue converts the string value to the appropriate type based on DataType.
func (m *ManualInput) ConvertValue() (interface{}, error) {
	switch m.Type {
	case DataTypeString:
		return m.Value, nil
	case DataTypeNumber:
		// Try to parse as float64
		result, err := strconv.ParseFloat(m.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert value '%s' to number: %w", m.Value, err)
		}
		return result, nil
	case DataTypeBoolean:
		switch m.Value {
		case "true", "True", "TRUE", "1":
			return true, nil
		case "false", "False", "FALSE", "0":
			return false, nil
		default:
			return nil, fmt.Errorf("cannot convert value '%s' to boolean (expected 'true' or 'false')", m.Value)
		}
	default:
		return nil, fmt.Errorf("unsupported type '%s'", m.Type)
	}
}
