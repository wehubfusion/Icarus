package simplecondition

import (
	"fmt"
	"regexp"
	"strings"
)

// compareValues dispatches to the appropriate comparison function based on operator
func compareValues(actualValue, expectedValue interface{}, operator ComparisonOperator, caseInsensitive bool) (bool, error) {
	switch operator {
	case OpEquals:
		return compareEquals(actualValue, expectedValue, caseInsensitive)
	case OpNotEquals:
		result, err := compareEquals(actualValue, expectedValue, caseInsensitive)
		return !result, err
	case OpGreaterThan:
		return compareGreaterThan(actualValue, expectedValue)
	case OpLessThan:
		return compareLessThan(actualValue, expectedValue)
	case OpGreaterThanOrEqual:
		return compareGreaterThanOrEqual(actualValue, expectedValue)
	case OpLessThanOrEqual:
		return compareLessThanOrEqual(actualValue, expectedValue)
	case OpContains:
		return compareContains(actualValue, expectedValue, caseInsensitive)
	case OpNotContains:
		result, err := compareContains(actualValue, expectedValue, caseInsensitive)
		return !result, err
	case OpStartsWith:
		return compareStartsWith(actualValue, expectedValue, caseInsensitive)
	case OpEndsWith:
		return compareEndsWith(actualValue, expectedValue, caseInsensitive)
	case OpRegex:
		return compareRegex(actualValue, expectedValue)
	case OpIn:
		return compareIn(actualValue, expectedValue, caseInsensitive)
	case OpNotIn:
		result, err := compareIn(actualValue, expectedValue, caseInsensitive)
		return !result, err
	case OpIsEmpty:
		return isEmptyValue(actualValue), nil
	case OpIsNotEmpty:
		return !isEmptyValue(actualValue), nil
	default:
		return false, &ComparisonError{
			Operator: string(operator),
			Message:  "unsupported operator",
		}
	}
}

// compareEquals checks if two values are equal
func compareEquals(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	return valuesEqual(actualValue, expectedValue, caseInsensitive), nil
}

// compareGreaterThan checks if actual > expected (numeric)
func compareGreaterThan(actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpGreaterThan),
			Message:  fmt.Sprintf("actual value conversion failed: %v", err),
		}
	}

	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpGreaterThan),
			Message:  fmt.Sprintf("expected value conversion failed: %v", err),
		}
	}

	return actual > expected, nil
}

// compareLessThan checks if actual < expected (numeric)
func compareLessThan(actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpLessThan),
			Message:  fmt.Sprintf("actual value conversion failed: %v", err),
		}
	}

	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpLessThan),
			Message:  fmt.Sprintf("expected value conversion failed: %v", err),
		}
	}

	return actual < expected, nil
}

// compareGreaterThanOrEqual checks if actual >= expected (numeric)
func compareGreaterThanOrEqual(actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpGreaterThanOrEqual),
			Message:  fmt.Sprintf("actual value conversion failed: %v", err),
		}
	}

	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpGreaterThanOrEqual),
			Message:  fmt.Sprintf("expected value conversion failed: %v", err),
		}
	}

	return actual >= expected, nil
}

// compareLessThanOrEqual checks if actual <= expected (numeric)
func compareLessThanOrEqual(actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpLessThanOrEqual),
			Message:  fmt.Sprintf("actual value conversion failed: %v", err),
		}
	}

	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpLessThanOrEqual),
			Message:  fmt.Sprintf("expected value conversion failed: %v", err),
		}
	}

	return actual <= expected, nil
}

// compareContains checks if actual string contains expected substring
func compareContains(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	actual := toString(actualValue)
	expected := toString(expectedValue)

	if caseInsensitive {
		actual = strings.ToLower(actual)
		expected = strings.ToLower(expected)
	}

	return strings.Contains(actual, expected), nil
}

// compareStartsWith checks if actual string starts with expected prefix
func compareStartsWith(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	actual := toString(actualValue)
	expected := toString(expectedValue)

	if caseInsensitive {
		actual = strings.ToLower(actual)
		expected = strings.ToLower(expected)
	}

	return strings.HasPrefix(actual, expected), nil
}

// compareEndsWith checks if actual string ends with expected suffix
func compareEndsWith(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	actual := toString(actualValue)
	expected := toString(expectedValue)

	if caseInsensitive {
		actual = strings.ToLower(actual)
		expected = strings.ToLower(expected)
	}

	return strings.HasSuffix(actual, expected), nil
}

// compareRegex checks if actual string matches expected regex pattern
func compareRegex(actualValue, expectedValue interface{}) (bool, error) {
	actual := toString(actualValue)
	pattern := toString(expectedValue)

	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpRegex),
			Message:  fmt.Sprintf("invalid regex pattern '%s': %v", pattern, err),
		}
	}

	return re.MatchString(actual), nil
}

// compareIn checks if actual value is in expected collection
func compareIn(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	// Convert expected value to slice
	expectedSlice, err := toSlice(expectedValue)
	if err != nil {
		return false, &ComparisonError{
			Operator: string(OpIn),
			Message:  fmt.Sprintf("expected value must be a collection: %v", err),
		}
	}

	// Check if actual value is in the slice
	for _, item := range expectedSlice {
		if valuesEqual(actualValue, item, caseInsensitive) {
			return true, nil
		}
	}

	return false, nil
}
