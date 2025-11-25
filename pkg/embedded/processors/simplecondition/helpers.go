package simplecondition

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tidwall/gjson"
)

// getValueFromPath extracts a value from a JSON object using a path
// Supports dot notation, array indexing, and other gjson syntax
func getValueFromPath(data []byte, path string) (interface{}, bool) {
	result := gjson.GetBytes(data, path)
	if !result.Exists() {
		return nil, false
	}

	return result.Value(), true
}

// isEmptyValue checks if a value is considered empty
// Empty means: nil, empty string, empty array, empty object, or false
func isEmptyValue(value interface{}) bool {
	if value == nil {
		return true
	}

	switch v := value.(type) {
	case string:
		return v == ""
	case []interface{}:
		return len(v) == 0
	case map[string]interface{}:
		return len(v) == 0
	case bool:
		return !v
	case float64:
		return v == 0
	case int:
		return v == 0
	case int64:
		return v == 0
	default:
		return false
	}
}

// toFloat64 converts a value to float64
// Handles int, int64, float64, and string representations of numbers
func toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string '%s' to number: %w", v, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot convert type %T to number", value)
	}
}

// toString converts a value to string
func toString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case float64:
		// Remove trailing zeros for cleaner display
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		return strconv.FormatBool(v)
	default:
		// For complex types, use JSON representation
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// toSlice converts a value to a slice
func toSlice(value interface{}) ([]interface{}, error) {
	switch v := value.(type) {
	case []interface{}:
		return v, nil
	case []string:
		result := make([]interface{}, len(v))
		for i, s := range v {
			result[i] = s
		}
		return result, nil
	case []int:
		result := make([]interface{}, len(v))
		for i, n := range v {
			result[i] = n
		}
		return result, nil
	case []float64:
		result := make([]interface{}, len(v))
		for i, f := range v {
			result[i] = f
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot convert type %T to slice", value)
	}
}

// countMetConditions counts how many conditions were met
func countMetConditions(results map[string]ConditionResult) int {
	count := 0
	for _, result := range results {
		if result.Met {
			count++
		}
	}
	return count
}

// countUnmetConditions counts how many conditions were not met
func countUnmetConditions(results map[string]ConditionResult) int {
	count := 0
	for _, result := range results {
		if !result.Met {
			count++
		}
	}
	return count
}

// mustMarshalJSON marshals a value to JSON, panicking on error
// Used for internal data that should always marshal successfully
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal JSON: %v", err))
	}
	return b
}

// valuesEqual checks if two values are equal
func valuesEqual(a, b interface{}, caseInsensitive bool) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Try string comparison first if case insensitive
	if caseInsensitive {
		aStr := toString(a)
		bStr := toString(b)
		return normalizeString(aStr, true) == normalizeString(bStr, true)
	}

	// Try numeric comparison
	aFloat, aErr := toFloat64(a)
	bFloat, bErr := toFloat64(b)
	if aErr == nil && bErr == nil {
		return aFloat == bFloat
	}

	// Try string comparison
	aStr := toString(a)
	bStr := toString(b)
	if aStr == bStr {
		return true
	}

	// Direct comparison
	return a == b
}
