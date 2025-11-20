package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
)

// Helper function to create a condition configuration
func createConditionConfig(t *testing.T, conditions []map[string]interface{}, logicOp string) []byte {
	config := map[string]interface{}{
		"conditions": conditions,
	}
	if logicOp != "" {
		config["logic_operator"] = logicOp
	}

	configBytes, err := json.Marshal(config)
	require.NoError(t, err, "Failed to marshal condition config")
	return configBytes
}

// Helper function to execute condition processor and return parsed output
func executeCondition(t *testing.T, executor *simplecondition.Executor, config, input []byte) map[string]interface{} {
	result, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: config,
		Input:         input,
	})
	require.NoError(t, err, "Failed to execute condition")

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err, "Failed to unmarshal output")

	return output
}

// Helper function to check if a specific condition was met
func assertConditionMet(t *testing.T, output map[string]interface{}, conditionName string, expectedMet bool) {
	conditions, ok := output["conditions"].(map[string]interface{})
	require.True(t, ok, "conditions field not found or not a map")

	condition, ok := conditions[conditionName].(map[string]interface{})
	require.True(t, ok, "condition %s not found", conditionName)

	met, ok := condition["met"].(bool)
	require.True(t, ok, "met field not found or not a bool")

	assert.Equal(t, expectedMet, met, "condition %s met status", conditionName)
}

// Helper function to check the overall result
func assertOverallResult(t *testing.T, output map[string]interface{}, expected bool) {
	result, ok := output["result"].(bool)
	require.True(t, ok, "result field not found or not a bool")
	assert.Equal(t, expected, result, "overall result")
}

// Helper function to get condition result
func getConditionResult(t *testing.T, output map[string]interface{}, conditionName string) map[string]interface{} {
	conditions, ok := output["conditions"].(map[string]interface{})
	require.True(t, ok, "conditions field not found or not a map")

	condition, ok := conditions[conditionName].(map[string]interface{})
	require.True(t, ok, "condition %s not found", conditionName)

	return condition
}

// TestSimpleCondition_Equals tests equality operator
func TestSimpleCondition_Equals(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name            string
		input           string
		fieldPath       string
		expectedValue   interface{}
		caseInsensitive bool
		expectedMet     bool
	}{
		{
			name:          "string equals case-sensitive",
			input:         `{"name": "Alice"}`,
			fieldPath:     "name",
			expectedValue: "Alice",
			expectedMet:   true,
		},
		{
			name:          "string equals case-sensitive mismatch",
			input:         `{"name": "Alice"}`,
			fieldPath:     "name",
			expectedValue: "alice",
			expectedMet:   false,
		},
		{
			name:            "string equals case-insensitive",
			input:           `{"name": "Alice"}`,
			fieldPath:       "name",
			expectedValue:   "alice",
			caseInsensitive: true,
			expectedMet:     true,
		},
		{
			name:          "numeric equals integer",
			input:         `{"age": 30}`,
			fieldPath:     "age",
			expectedValue: float64(30),
			expectedMet:   true,
		},
		{
			name:          "numeric equals float",
			input:         `{"price": 99.99}`,
			fieldPath:     "price",
			expectedValue: 99.99,
			expectedMet:   true,
		},
		{
			name:          "boolean equals true",
			input:         `{"active": true}`,
			fieldPath:     "active",
			expectedValue: true,
			expectedMet:   true,
		},
		{
			name:          "boolean equals false",
			input:         `{"active": false}`,
			fieldPath:     "active",
			expectedValue: false,
			expectedMet:   true,
		},
		{
			name:          "type mismatch string vs number",
			input:         `{"age": "30"}`,
			fieldPath:     "age",
			expectedValue: 30,
			expectedMet:   true, // String "30" is converted to number 30
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       "equals",
				"expected_value": tt.expectedValue,
			}
			if tt.caseInsensitive {
				condition["case_insensitive"] = true
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertConditionMet(t, output, "check_value", tt.expectedMet)
			assertOverallResult(t, output, tt.expectedMet)
		})
	}
}

// TestSimpleCondition_NotEquals tests not equals operator
func TestSimpleCondition_NotEquals(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name          string
		input         string
		fieldPath     string
		expectedValue interface{}
		expectedMet   bool
	}{
		{
			name:          "string not equals",
			input:         `{"name": "Alice"}`,
			fieldPath:     "name",
			expectedValue: "Bob",
			expectedMet:   true,
		},
		{
			name:          "string not equals same value",
			input:         `{"name": "Alice"}`,
			fieldPath:     "name",
			expectedValue: "Alice",
			expectedMet:   false,
		},
		{
			name:          "numeric not equals",
			input:         `{"age": 30}`,
			fieldPath:     "age",
			expectedValue: float64(25),
			expectedMet:   true,
		},
		{
			name:          "boolean not equals",
			input:         `{"active": true}`,
			fieldPath:     "active",
			expectedValue: false,
			expectedMet:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       "not_equals",
				"expected_value": tt.expectedValue,
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertConditionMet(t, output, "check_value", tt.expectedMet)
			assertOverallResult(t, output, tt.expectedMet)
		})
	}
}

// TestSimpleCondition_NumericComparisons tests numeric comparison operators
func TestSimpleCondition_NumericComparisons(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name          string
		input         string
		fieldPath     string
		operator      string
		expectedValue interface{}
		expectedMet   bool
		shouldError   bool
	}{
		// Greater than tests
		{
			name:          "greater than - integer true",
			input:         `{"age": 30}`,
			fieldPath:     "age",
			operator:      "greater_than",
			expectedValue: float64(25),
			expectedMet:   true,
		},
		{
			name:          "greater than - integer false",
			input:         `{"age": 30}`,
			fieldPath:     "age",
			operator:      "greater_than",
			expectedValue: float64(35),
			expectedMet:   false,
		},
		{
			name:          "greater than - float true",
			input:         `{"price": 99.99}`,
			fieldPath:     "price",
			operator:      "greater_than",
			expectedValue: 50.5,
			expectedMet:   true,
		},
		{
			name:          "greater than - string number",
			input:         `{"score": "85"}`,
			fieldPath:     "score",
			operator:      "greater_than",
			expectedValue: float64(80),
			expectedMet:   true,
		},
		// Less than tests
		{
			name:          "less than - integer true",
			input:         `{"age": 25}`,
			fieldPath:     "age",
			operator:      "less_than",
			expectedValue: float64(30),
			expectedMet:   true,
		},
		{
			name:          "less than - integer false",
			input:         `{"age": 35}`,
			fieldPath:     "age",
			operator:      "less_than",
			expectedValue: float64(30),
			expectedMet:   false,
		},
		// Greater than or equal tests
		{
			name:          "gte - equal boundary",
			input:         `{"age": 30}`,
			fieldPath:     "age",
			operator:      "greater_than_or_equal",
			expectedValue: float64(30),
			expectedMet:   true,
		},
		{
			name:          "gte - greater than boundary",
			input:         `{"age": 31}`,
			fieldPath:     "age",
			operator:      "greater_than_or_equal",
			expectedValue: float64(30),
			expectedMet:   true,
		},
		{
			name:          "gte - less than boundary",
			input:         `{"age": 29}`,
			fieldPath:     "age",
			operator:      "greater_than_or_equal",
			expectedValue: float64(30),
			expectedMet:   false,
		},
		// Less than or equal tests
		{
			name:          "lte - equal boundary",
			input:         `{"age": 30}`,
			fieldPath:     "age",
			operator:      "less_than_or_equal",
			expectedValue: float64(30),
			expectedMet:   true,
		},
		{
			name:          "lte - less than boundary",
			input:         `{"age": 29}`,
			fieldPath:     "age",
			operator:      "less_than_or_equal",
			expectedValue: float64(30),
			expectedMet:   true,
		},
		{
			name:          "lte - greater than boundary",
			input:         `{"age": 31}`,
			fieldPath:     "age",
			operator:      "less_than_or_equal",
			expectedValue: float64(30),
			expectedMet:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       tt.operator,
				"expected_value": tt.expectedValue,
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")

			if tt.shouldError {
				_, err := executor.Execute(context.Background(), embedded.NodeConfig{
					Configuration: config,
					Input:         []byte(tt.input),
				})
				assert.Error(t, err)
				return
			}

			output := executeCondition(t, executor, config, []byte(tt.input))
			assertConditionMet(t, output, "check_value", tt.expectedMet)
		})
	}
}

// TestSimpleCondition_StringOperations tests string comparison operators
func TestSimpleCondition_StringOperations(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name            string
		input           string
		fieldPath       string
		operator        string
		expectedValue   interface{}
		caseInsensitive bool
		expectedMet     bool
	}{
		// Contains tests
		{
			name:          "contains - case sensitive match",
			input:         `{"message": "Hello World"}`,
			fieldPath:     "message",
			operator:      "contains",
			expectedValue: "World",
			expectedMet:   true,
		},
		{
			name:          "contains - case sensitive no match",
			input:         `{"message": "Hello World"}`,
			fieldPath:     "message",
			operator:      "contains",
			expectedValue: "world",
			expectedMet:   false,
		},
		{
			name:            "contains - case insensitive match",
			input:           `{"message": "Hello World"}`,
			fieldPath:       "message",
			operator:        "contains",
			expectedValue:   "world",
			caseInsensitive: true,
			expectedMet:     true,
		},
		{
			name:          "contains - empty string",
			input:         `{"message": ""}`,
			fieldPath:     "message",
			operator:      "contains",
			expectedValue: "test",
			expectedMet:   false,
		},
		// Not contains tests
		{
			name:          "not contains - match",
			input:         `{"message": "Hello World"}`,
			fieldPath:     "message",
			operator:      "not_contains",
			expectedValue: "Goodbye",
			expectedMet:   true,
		},
		{
			name:          "not contains - no match",
			input:         `{"message": "Hello World"}`,
			fieldPath:     "message",
			operator:      "not_contains",
			expectedValue: "World",
			expectedMet:   false,
		},
		// Starts with tests
		{
			name:          "starts with - case sensitive match",
			input:         `{"url": "https://example.com"}`,
			fieldPath:     "url",
			operator:      "starts_with",
			expectedValue: "https://",
			expectedMet:   true,
		},
		{
			name:          "starts with - case sensitive no match",
			input:         `{"url": "https://example.com"}`,
			fieldPath:     "url",
			operator:      "starts_with",
			expectedValue: "http://",
			expectedMet:   false,
		},
		{
			name:            "starts with - case insensitive match",
			input:           `{"url": "HTTPS://example.com"}`,
			fieldPath:       "url",
			operator:        "starts_with",
			expectedValue:   "https://",
			caseInsensitive: true,
			expectedMet:     true,
		},
		// Ends with tests
		{
			name:          "ends with - case sensitive match",
			input:         `{"filename": "document.pdf"}`,
			fieldPath:     "filename",
			operator:      "ends_with",
			expectedValue: ".pdf",
			expectedMet:   true,
		},
		{
			name:          "ends with - case sensitive no match",
			input:         `{"filename": "document.pdf"}`,
			fieldPath:     "filename",
			operator:      "ends_with",
			expectedValue: ".PDF",
			expectedMet:   false,
		},
		{
			name:            "ends with - case insensitive match",
			input:           `{"filename": "document.PDF"}`,
			fieldPath:       "filename",
			operator:        "ends_with",
			expectedValue:   ".pdf",
			caseInsensitive: true,
			expectedMet:     true,
		},
		// Special characters
		{
			name:          "contains - special characters",
			input:         `{"text": "Price: $99.99!"}`,
			fieldPath:     "text",
			operator:      "contains",
			expectedValue: "$99.99",
			expectedMet:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       tt.operator,
				"expected_value": tt.expectedValue,
			}
			if tt.caseInsensitive {
				condition["case_insensitive"] = true
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertConditionMet(t, output, "check_value", tt.expectedMet)
		})
	}
}

// TestSimpleCondition_Regex tests regex operator
func TestSimpleCondition_Regex(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name            string
		input           string
		fieldPath       string
		pattern         string
		expectedMet     bool
		shouldHaveError bool
	}{
		{
			name:        "regex - email pattern match",
			input:       `{"email": "test@example.com"}`,
			fieldPath:   "email",
			pattern:     `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
			expectedMet: true,
		},
		{
			name:        "regex - email pattern no match",
			input:       `{"email": "invalid-email"}`,
			fieldPath:   "email",
			pattern:     `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
			expectedMet: false,
		},
		{
			name:        "regex - phone number pattern",
			input:       `{"phone": "+1-555-123-4567"}`,
			fieldPath:   "phone",
			pattern:     `^\+\d{1,3}-\d{3}-\d{3}-\d{4}$`,
			expectedMet: true,
		},
		{
			name:        "regex - digit pattern",
			input:       `{"code": "ABC123"}`,
			fieldPath:   "code",
			pattern:     `\d{3}`,
			expectedMet: true,
		},
		{
			name:            "regex - invalid pattern",
			input:           `{"value": "test"}`,
			fieldPath:       "value",
			pattern:         `[invalid(`,
			shouldHaveError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       "regex",
				"expected_value": tt.pattern,
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			if tt.shouldHaveError {
				// Check that the condition has an error
				condResult := getConditionResult(t, output, "check_value")
				errorMsg, hasError := condResult["error"].(string)
				assert.True(t, hasError, "Expected error in condition result")
				assert.NotEmpty(t, errorMsg)
			} else {
				assertConditionMet(t, output, "check_value", tt.expectedMet)
			}
		})
	}
}

// TestSimpleCondition_InOperator tests in and not_in operators
func TestSimpleCondition_InOperator(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name          string
		input         string
		fieldPath     string
		operator      string
		expectedValue interface{}
		expectedMet   bool
	}{
		{
			name:          "in - string array match",
			input:         `{"status": "active"}`,
			fieldPath:     "status",
			operator:      "in",
			expectedValue: []interface{}{"active", "pending", "inactive"},
			expectedMet:   true,
		},
		{
			name:          "in - string array no match",
			input:         `{"status": "deleted"}`,
			fieldPath:     "status",
			operator:      "in",
			expectedValue: []interface{}{"active", "pending", "inactive"},
			expectedMet:   false,
		},
		{
			name:          "in - numeric array match",
			input:         `{"code": 200}`,
			fieldPath:     "code",
			operator:      "in",
			expectedValue: []interface{}{float64(200), float64(201), float64(204)},
			expectedMet:   true,
		},
		{
			name:          "in - mixed array match",
			input:         `{"value": "test"}`,
			fieldPath:     "value",
			operator:      "in",
			expectedValue: []interface{}{"test", float64(123), true},
			expectedMet:   true,
		},
		{
			name:          "not_in - match",
			input:         `{"status": "deleted"}`,
			fieldPath:     "status",
			operator:      "not_in",
			expectedValue: []interface{}{"active", "pending", "inactive"},
			expectedMet:   true,
		},
		{
			name:          "not_in - no match",
			input:         `{"status": "active"}`,
			fieldPath:     "status",
			operator:      "not_in",
			expectedValue: []interface{}{"active", "pending", "inactive"},
			expectedMet:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       tt.operator,
				"expected_value": tt.expectedValue,
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertConditionMet(t, output, "check_value", tt.expectedMet)
		})
	}
}

// TestSimpleCondition_ExistenceOperators tests is_empty and is_not_empty operators
func TestSimpleCondition_ExistenceOperators(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name        string
		input       string
		fieldPath   string
		operator    string
		expectedMet bool
	}{
		// is_empty tests
		{
			name:        "is_empty - null value",
			input:       `{"value": null}`,
			fieldPath:   "value",
			operator:    "is_empty",
			expectedMet: true,
		},
		{
			name:        "is_empty - empty string",
			input:       `{"value": ""}`,
			fieldPath:   "value",
			operator:    "is_empty",
			expectedMet: true,
		},
		{
			name:        "is_empty - empty array",
			input:       `{"value": []}`,
			fieldPath:   "value",
			operator:    "is_empty",
			expectedMet: true,
		},
		{
			name:        "is_empty - empty object",
			input:       `{"value": {}}`,
			fieldPath:   "value",
			operator:    "is_empty",
			expectedMet: true,
		},
		{
			name:        "is_empty - missing field",
			input:       `{"other": "value"}`,
			fieldPath:   "value",
			operator:    "is_empty",
			expectedMet: true,
		},
		{
			name:        "is_empty - non-empty string",
			input:       `{"value": "test"}`,
			fieldPath:   "value",
			operator:    "is_empty",
			expectedMet: false,
		},
		// is_not_empty tests
		{
			name:        "is_not_empty - non-empty string",
			input:       `{"value": "test"}`,
			fieldPath:   "value",
			operator:    "is_not_empty",
			expectedMet: true,
		},
		{
			name:        "is_not_empty - number",
			input:       `{"value": 42}`,
			fieldPath:   "value",
			operator:    "is_not_empty",
			expectedMet: true,
		},
		{
			name:        "is_not_empty - boolean",
			input:       `{"value": true}`,
			fieldPath:   "value",
			operator:    "is_not_empty",
			expectedMet: true,
		},
		{
			name:        "is_not_empty - empty string",
			input:       `{"value": ""}`,
			fieldPath:   "value",
			operator:    "is_not_empty",
			expectedMet: false,
		},
		{
			name:        "is_not_empty - null value",
			input:       `{"value": null}`,
			fieldPath:   "value",
			operator:    "is_not_empty",
			expectedMet: false,
		},
		{
			name:        "is_not_empty - missing field",
			input:       `{"other": "value"}`,
			fieldPath:   "value",
			operator:    "is_not_empty",
			expectedMet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":       "check_value",
				"field_path": tt.fieldPath,
				"operator":   tt.operator,
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertConditionMet(t, output, "check_value", tt.expectedMet)
		})
	}
}

// TestSimpleCondition_ANDLogic tests AND logic operator
func TestSimpleCondition_ANDLogic(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name           string
		input          string
		conditions     []map[string]interface{}
		expectedResult bool
		expectedMet    map[string]bool
	}{
		{
			name:  "all conditions met",
			input: `{"age": 30, "active": true, "name": "Alice"}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": float64(18),
				},
				{
					"name":           "check_active",
					"field_path":     "active",
					"operator":       "equals",
					"expected_value": true,
				},
				{
					"name":           "check_name",
					"field_path":     "name",
					"operator":       "equals",
					"expected_value": "Alice",
				},
			},
			expectedResult: true,
			expectedMet: map[string]bool{
				"check_age":    true,
				"check_active": true,
				"check_name":   true,
			},
		},
		{
			name:  "one condition fails",
			input: `{"age": 15, "active": true, "name": "Alice"}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": float64(18),
				},
				{
					"name":           "check_active",
					"field_path":     "active",
					"operator":       "equals",
					"expected_value": true,
				},
			},
			expectedResult: false,
			expectedMet: map[string]bool{
				"check_age":    false,
				"check_active": true,
			},
		},
		{
			name:  "all conditions fail",
			input: `{"age": 15, "active": false}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": float64(18),
				},
				{
					"name":           "check_active",
					"field_path":     "active",
					"operator":       "equals",
					"expected_value": true,
				},
			},
			expectedResult: false,
			expectedMet: map[string]bool{
				"check_age":    false,
				"check_active": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createConditionConfig(t, tt.conditions, "AND")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertOverallResult(t, output, tt.expectedResult)
			for condName, expectedMet := range tt.expectedMet {
				assertConditionMet(t, output, condName, expectedMet)
			}
		})
	}
}

// TestSimpleCondition_ORLogic tests OR logic operator
func TestSimpleCondition_ORLogic(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name           string
		input          string
		conditions     []map[string]interface{}
		expectedResult bool
		expectedMet    map[string]bool
	}{
		{
			name:  "all conditions met",
			input: `{"age": 30, "active": true}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": float64(18),
				},
				{
					"name":           "check_active",
					"field_path":     "active",
					"operator":       "equals",
					"expected_value": true,
				},
			},
			expectedResult: true,
			expectedMet: map[string]bool{
				"check_age":    true,
				"check_active": true,
			},
		},
		{
			name:  "one condition met",
			input: `{"age": 30, "active": false}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": float64(18),
				},
				{
					"name":           "check_active",
					"field_path":     "active",
					"operator":       "equals",
					"expected_value": true,
				},
			},
			expectedResult: true,
			expectedMet: map[string]bool{
				"check_age":    true,
				"check_active": false,
			},
		},
		{
			name:  "no conditions met",
			input: `{"age": 15, "active": false}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": float64(18),
				},
				{
					"name":           "check_active",
					"field_path":     "active",
					"operator":       "equals",
					"expected_value": true,
				},
			},
			expectedResult: false,
			expectedMet: map[string]bool{
				"check_age":    false,
				"check_active": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createConditionConfig(t, tt.conditions, "OR")
			output := executeCondition(t, executor, config, []byte(tt.input))

			assertOverallResult(t, output, tt.expectedResult)
			for condName, expectedMet := range tt.expectedMet {
				assertConditionMet(t, output, condName, expectedMet)
			}
		})
	}
}

// TestSimpleCondition_PathExtraction tests nested path extraction
func TestSimpleCondition_PathExtraction(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name          string
		input         string
		fieldPath     string
		operator      string
		expectedValue interface{}
		expectedMet   bool
		shouldError   bool
	}{
		{
			name:          "simple field",
			input:         `{"name": "Alice"}`,
			fieldPath:     "name",
			operator:      "equals",
			expectedValue: "Alice",
			expectedMet:   true,
		},
		{
			name:          "nested path dot notation",
			input:         `{"user": {"profile": {"email": "alice@example.com"}}}`,
			fieldPath:     "user.profile.email",
			operator:      "equals",
			expectedValue: "alice@example.com",
			expectedMet:   true,
		},
		{
			name:          "array indexing",
			input:         `{"items": [{"name": "first"}, {"name": "second"}]}`,
			fieldPath:     "items.0.name",
			operator:      "equals",
			expectedValue: "first",
			expectedMet:   true,
		},
		{
			name:          "deep nesting",
			input:         `{"a": {"b": {"c": {"d": {"e": {"f": "deep"}}}}}}`,
			fieldPath:     "a.b.c.d.e.f",
			operator:      "equals",
			expectedValue: "deep",
			expectedMet:   true,
		},
		{
			name:          "array second element",
			input:         `{"result": {"users": [{"id": 1}, {"id": 2}, {"id": 3}]}}`,
			fieldPath:     "data.users.2.id",
			operator:      "equals",
			expectedValue: float64(3),
			expectedMet:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			condition := map[string]interface{}{
				"name":           "check_value",
				"field_path":     tt.fieldPath,
				"operator":       tt.operator,
				"expected_value": tt.expectedValue,
			}

			config := createConditionConfig(t, []map[string]interface{}{condition}, "AND")

			if tt.shouldError {
				_, err := executor.Execute(context.Background(), embedded.NodeConfig{
					Configuration: config,
					Input:         []byte(tt.input),
				})
				assert.Error(t, err)
				return
			}

			output := executeCondition(t, executor, config, []byte(tt.input))
			assertConditionMet(t, output, "check_value", tt.expectedMet)
		})
	}
}

// TestSimpleCondition_ConfigValidation tests configuration validation
func TestSimpleCondition_ConfigValidation(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name          string
		config        map[string]interface{}
		shouldError   bool
		errorContains string
	}{
		{
			name: "valid configuration",
			config: map[string]interface{}{
				"logic_operator": "AND",
				"conditions": []map[string]interface{}{
					{
						"name":           "test",
						"field_path":     "value",
						"operator":       "equals",
						"expected_value": "test",
					},
				},
			},
			shouldError: false,
		},
		{
			name: "empty conditions array",
			config: map[string]interface{}{
				"conditions": []map[string]interface{}{},
			},
			shouldError:   true,
			errorContains: "at least one condition",
		},
		{
			name: "missing condition name",
			config: map[string]interface{}{
				"conditions": []map[string]interface{}{
					{
						"field_path":     "value",
						"operator":       "equals",
						"expected_value": "test",
					},
				},
			},
			shouldError:   true,
			errorContains: "name cannot be empty",
		},
		{
			name: "missing field path",
			config: map[string]interface{}{
				"conditions": []map[string]interface{}{
					{
						"name":           "test",
						"operator":       "equals",
						"expected_value": "test",
					},
				},
			},
			shouldError:   true,
			errorContains: "field_path cannot be empty",
		},
		{
			name: "invalid operator",
			config: map[string]interface{}{
				"conditions": []map[string]interface{}{
					{
						"name":           "test",
						"field_path":     "value",
						"operator":       "invalid_op",
						"expected_value": "test",
					},
				},
			},
			shouldError:   true,
			errorContains: "invalid operator",
		},
		{
			name: "missing expected_value for equals",
			config: map[string]interface{}{
				"conditions": []map[string]interface{}{
					{
						"name":       "test",
						"field_path": "value",
						"operator":   "equals",
					},
				},
			},
			shouldError:   true,
			errorContains: "expected_value is required",
		},
		{
			name: "duplicate condition names",
			config: map[string]interface{}{
				"conditions": []map[string]interface{}{
					{
						"name":           "test",
						"field_path":     "value1",
						"operator":       "equals",
						"expected_value": "test1",
					},
					{
						"name":           "test",
						"field_path":     "value2",
						"operator":       "equals",
						"expected_value": "test2",
					},
				},
			},
			shouldError:   true,
			errorContains: "duplicate condition name",
		},
		{
			name: "invalid logic operator",
			config: map[string]interface{}{
				"logic_operator": "XOR",
				"conditions": []map[string]interface{}{
					{
						"name":           "test",
						"field_path":     "value",
						"operator":       "equals",
						"expected_value": "test",
					},
				},
			},
			shouldError:   true,
			errorContains: "invalid logic operator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configBytes, err := json.Marshal(tt.config)
			require.NoError(t, err)

			_, err = executor.Execute(context.Background(), embedded.NodeConfig{
				Configuration: configBytes,
				Input:         []byte(`{}`),
			})

			if tt.shouldError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestSimpleCondition_EdgeCases tests edge cases and unusual inputs
func TestSimpleCondition_EdgeCases(t *testing.T) {
	executor := simplecondition.NewExecutor()

	tests := []struct {
		name        string
		input       string
		conditions  []map[string]interface{}
		expectError bool
	}{
		{
			name:  "empty input JSON",
			input: `{}`,
			conditions: []map[string]interface{}{
				{
					"name":       "check_value",
					"field_path": "value",
					"operator":   "is_empty",
				},
			},
			expectError: false,
		},
		{
			name:  "unicode characters in strings",
			input: `{"message": "Hello 世界 🌍"}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_message",
					"field_path":     "message",
					"operator":       "contains",
					"expected_value": "世界",
				},
			},
			expectError: false,
		},
		{
			name:  "very large number",
			input: `{"value": 999999999999999}`,
			conditions: []map[string]interface{}{
				{
					"name":           "check_value",
					"field_path":     "value",
					"operator":       "greater_than",
					"expected_value": float64(1000000),
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createConditionConfig(t, tt.conditions, "AND")

			_, err := executor.Execute(context.Background(), embedded.NodeConfig{
				Configuration: config,
				Input:         []byte(tt.input),
			})

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestSimpleCondition_Integration tests real-world scenarios
func TestSimpleCondition_Integration(t *testing.T) {
	executor := simplecondition.NewExecutor()

	t.Run("user validation scenario", func(t *testing.T) {
		input := `{
			"user": {
				"email": "john@example.com",
				"age": 25,
				"country": "US",
				"active": true,
				"role": "admin"
			}
		}`

		conditions := []map[string]interface{}{
			{
				"name":           "valid_email",
				"field_path":     "user.email",
				"operator":       "contains",
				"expected_value": "@",
			},
			{
				"name":           "adult",
				"field_path":     "user.age",
				"operator":       "greater_than_or_equal",
				"expected_value": float64(18),
			},
			{
				"name":           "active_user",
				"field_path":     "user.active",
				"operator":       "equals",
				"expected_value": true,
			},
			{
				"name":           "authorized_role",
				"field_path":     "user.role",
				"operator":       "in",
				"expected_value": []interface{}{"admin", "moderator"},
			},
		}

		config := createConditionConfig(t, conditions, "AND")
		output := executeCondition(t, executor, config, []byte(input))

		assertOverallResult(t, output, true)
		assertConditionMet(t, output, "valid_email", true)
		assertConditionMet(t, output, "adult", true)
		assertConditionMet(t, output, "active_user", true)
		assertConditionMet(t, output, "authorized_role", true)
	})

	t.Run("complex OR logic scenario", func(t *testing.T) {
		input := `{
			"status": "pending",
			"priority": "high",
			"assigned": false
		}`

		conditions := []map[string]interface{}{
			{
				"name":           "is_urgent",
				"field_path":     "priority",
				"operator":       "equals",
				"expected_value": "high",
			},
			{
				"name":           "is_assigned",
				"field_path":     "assigned",
				"operator":       "equals",
				"expected_value": true,
			},
		}

		config := createConditionConfig(t, conditions, "OR")
		output := executeCondition(t, executor, config, []byte(input))

		// Should be true because priority is high (OR logic)
		assertOverallResult(t, output, true)
		assertConditionMet(t, output, "is_urgent", true)
		assertConditionMet(t, output, "is_assigned", false)
	})
}

// TestSimpleCondition_OutputFormat tests output structure
func TestSimpleCondition_OutputFormat(t *testing.T) {
	executor := simplecondition.NewExecutor()

	input := `{"age": 30, "active": true}`
	conditions := []map[string]interface{}{
		{
			"name":           "check_age",
			"field_path":     "age",
			"operator":       "greater_than",
			"expected_value": float64(18),
		},
		{
			"name":           "check_active",
			"field_path":     "active",
			"operator":       "equals",
			"expected_value": true,
		},
	}

	config := createConditionConfig(t, conditions, "AND")
	output := executeCondition(t, executor, config, []byte(input))

	// Verify result field exists and is bool
	result, hasResult := output["result"].(bool)
	assert.True(t, hasResult, "result field should exist and be bool")
	assert.True(t, result)

	// Verify conditions map exists
	conditionsMap, hasConditions := output["conditions"].(map[string]interface{})
	assert.True(t, hasConditions, "conditions field should exist and be a map")
	assert.Len(t, conditionsMap, 2, "should have 2 conditions")

	// Verify individual condition structure
	checkAge := getConditionResult(t, output, "check_age")
	assert.Equal(t, "check_age", checkAge["name"])
	assert.Equal(t, true, checkAge["met"])
	assert.Equal(t, float64(30), checkAge["actual_value"])
	assert.Equal(t, float64(18), checkAge["expected_value"])
	assert.Equal(t, "greater_than", checkAge["operator"])

	// Verify summary structure
	summary, hasSummary := output["summary"].(map[string]interface{})
	assert.True(t, hasSummary, "summary field should exist")
	assert.Equal(t, float64(2), summary["total_conditions"])
	assert.Equal(t, float64(2), summary["met_conditions"])
	assert.Equal(t, float64(0), summary["unmet_conditions"])
	assert.Equal(t, "AND", summary["logic_operator"])
}
