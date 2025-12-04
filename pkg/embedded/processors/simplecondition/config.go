package simplecondition

import (
	"fmt"
	"strings"
)

// ComparisonOperator defines the type of comparison to perform
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

// DataType specifies how to interpret values
type DataType string

const (
	DataTypeAuto    DataType = "auto"
	DataTypeString  DataType = "string"
	DataTypeNumber  DataType = "number"
	DataTypeBoolean DataType = "boolean"
	DataTypeNull    DataType = "null"
)

// LogicOperator defines how to combine multiple conditions
type LogicOperator string

const (
	LogicAnd LogicOperator = "AND"
	LogicOr  LogicOperator = "OR"
)

// InputType defines the data type for manual inputs
type InputType string

const (
	InputTypeString InputType = "string"
	InputTypeNumber InputType = "number"
	InputTypeDate   InputType = "date"
)

// ManualInput defines an input field that the condition receives
type ManualInput struct {
	// Name is the field name (used in conditions field_path)
	Name string `json:"name"`
	// Type is the data type (string, number, date)
	Type InputType `json:"type"`
}

// Config defines the configuration for the simple condition processor
type Config struct {
	// ManualInputs defines the input fields the condition receives
	// These are mapped from upstream node outputs via field mapping
	ManualInputs []ManualInput `json:"manual_inputs,omitempty"`

	// LogicOperator specifies how to combine multiple conditions (AND/OR)
	// Default: "AND"
	LogicOperator LogicOperator `json:"logic_operator,omitempty"`

	// Conditions is the list of conditions to evaluate
	Conditions []Condition `json:"conditions"`
}

// Condition defines a single condition to evaluate
type Condition struct {
	// Name is the unique identifier for this condition
	Name string `json:"name"`

	// FieldPath is the JSON path to the field to evaluate (supports dot notation and array indexing)
	FieldPath string `json:"field_path"`

	// Operator is the comparison operator to use
	Operator ComparisonOperator `json:"operator"`

	// ExpectedValue is the value to compare against (not used for existence operators)
	ExpectedValue interface{} `json:"expected_value,omitempty"`

	// DataType specifies how to interpret the values
	// Default: "auto"
	DataType DataType `json:"data_type,omitempty"`

	// CaseInsensitive makes string comparisons case-insensitive
	// Default: false
	CaseInsensitive bool `json:"case_insensitive,omitempty"`
}

// Output defines the structure of the processor output
type Output struct {
	// Result is the overall result (true if all conditions met for AND, or any condition met for OR)
	Result bool `json:"result"`

	// Conditions contains the results for each individual condition
	Conditions map[string]ConditionResult `json:"conditions"`

	// Summary provides aggregate statistics
	Summary Summary `json:"summary"`
}

// ConditionResult represents the result of evaluating a single condition
type ConditionResult struct {
	// Name is the condition name
	Name string `json:"name"`

	// Met indicates whether the condition was satisfied
	Met bool `json:"met"`

	// ActualValue is the value extracted from the input
	ActualValue interface{} `json:"actual_value"`

	// ExpectedValue is the value the condition was compared against
	ExpectedValue interface{} `json:"expected_value,omitempty"`

	// Operator is the comparison operator used
	Operator ComparisonOperator `json:"operator"`

	// Error contains any error message if evaluation failed
	Error string `json:"error,omitempty"`
}

// Summary provides aggregate statistics about condition evaluation
type Summary struct {
	// TotalConditions is the total number of conditions evaluated
	TotalConditions int `json:"total_conditions"`

	// MetConditions is the number of conditions that were satisfied
	MetConditions int `json:"met_conditions"`

	// UnmetConditions is the number of conditions that were not satisfied
	UnmetConditions int `json:"unmet_conditions"`

	// LogicOperator is the logic operator used to combine conditions
	LogicOperator LogicOperator `json:"logic_operator"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	// Set default logic operator
	if c.LogicOperator == "" {
		c.LogicOperator = LogicAnd
	}

	// Validate logic operator
	if c.LogicOperator != LogicAnd && c.LogicOperator != LogicOr {
		return &ConfigError{
			Field:   "logic_operator",
			Message: fmt.Sprintf("invalid logic operator '%s', must be 'AND' or 'OR'", c.LogicOperator),
		}
	}

	// Validate conditions
	if len(c.Conditions) == 0 {
		return &ConfigError{
			Field:   "conditions",
			Message: "at least one condition must be specified",
		}
	}

	// Check for duplicate condition names
	names := make(map[string]bool)
	for i, cond := range c.Conditions {
		if err := cond.Validate(); err != nil {
			return fmt.Errorf("condition at index %d: %w", i, err)
		}

		if names[cond.Name] {
			return &ConfigError{
				Field:   "conditions",
				Message: fmt.Sprintf("duplicate condition name '%s'", cond.Name),
			}
		}
		names[cond.Name] = true
	}

	return nil
}

// Validate checks if a condition is valid
func (c *Condition) Validate() error {
	// Validate name
	if c.Name == "" {
		return &ConfigError{
			Field:   "name",
			Message: "condition name cannot be empty",
		}
	}

	// Validate field path
	if c.FieldPath == "" {
		return &ConfigError{
			Field:   "field_path",
			Message: fmt.Sprintf("field_path cannot be empty for condition '%s'", c.Name),
		}
	}

	// Validate operator
	if !isValidOperator(c.Operator) {
		return &ConfigError{
			Field:   "operator",
			Message: fmt.Sprintf("invalid operator '%s' for condition '%s'", c.Operator, c.Name),
		}
	}

	// Validate expected value (not required for existence operators)
	if !isExistenceOperator(c.Operator) && c.ExpectedValue == nil {
		return &ConfigError{
			Field:   "expected_value",
			Message: fmt.Sprintf("expected_value is required for operator '%s' in condition '%s'", c.Operator, c.Name),
		}
	}

	// Set default data type
	if c.DataType == "" {
		c.DataType = DataTypeAuto
	}

	// Validate data type
	if !isValidDataType(c.DataType) {
		return &ConfigError{
			Field:   "data_type",
			Message: fmt.Sprintf("invalid data_type '%s' for condition '%s'", c.DataType, c.Name),
		}
	}

	return nil
}

// isValidOperator checks if an operator is valid
func isValidOperator(op ComparisonOperator) bool {
	switch op {
	case OpEquals, OpNotEquals,
		OpGreaterThan, OpLessThan, OpGreaterThanOrEqual, OpLessThanOrEqual,
		OpContains, OpNotContains, OpStartsWith, OpEndsWith, OpRegex,
		OpIn, OpNotIn,
		OpIsEmpty, OpIsNotEmpty:
		return true
	default:
		return false
	}
}

// isExistenceOperator checks if an operator is an existence operator
func isExistenceOperator(op ComparisonOperator) bool {
	return op == OpIsEmpty || op == OpIsNotEmpty
}

// isValidDataType checks if a data type is valid
func isValidDataType(dt DataType) bool {
	switch dt {
	case DataTypeAuto, DataTypeString, DataTypeNumber, DataTypeBoolean, DataTypeNull:
		return true
	default:
		return false
	}
}

// normalizeString normalizes a string for comparison
func normalizeString(s string, caseInsensitive bool) string {
	if caseInsensitive {
		return strings.ToLower(s)
	}
	return s
}
