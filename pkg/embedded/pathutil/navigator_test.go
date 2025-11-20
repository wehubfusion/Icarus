package pathutil

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractResultFromOutput(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		expectedOutput   string
		isStandardOutput bool
	}{
		{
			name: "extract data from StandardOutput",
			input: `{
				"_meta": {"status": "success"},
				"_events": {"success": true},
				"result": {"name": "John", "age": 30}
			}`,
			expectedOutput:   `{"name": "John", "age": 30}`,
			isStandardOutput: true,
		},
		{
			name: "extract null data returns empty object",
			input: `{
				"_meta": {"status": "failed"},
				"_events": {"error": true},
				"result": null
			}`,
			expectedOutput:   `{}`,
			isStandardOutput: true,
		},
		{
			name:             "non-StandardOutput returns as-is",
			input:            `{"name": "John", "age": 30}`,
			expectedOutput:   `{"name": "John", "age": 30}`,
			isStandardOutput: false,
		},
		{
			name: "extract array data",
			input: `{
				"_meta": {"status": "success"},
				"result": [{"name": "John"}, {"name": "Jane"}]
			}`,
			expectedOutput:   `[{"name": "John"}, {"name": "Jane"}]`,
			isStandardOutput: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, isStandardOutput := extractResultFromOutput([]byte(tt.input))

			assert.Equal(t, tt.isStandardOutput, isStandardOutput)

			// Compare JSON structure (order may differ)
			var resultData, expectedData interface{}
			require.NoError(t, json.Unmarshal(result, &resultData))
			require.NoError(t, json.Unmarshal([]byte(tt.expectedOutput), &expectedData))

			assert.Equal(t, expectedData, resultData)
		})
	}
}

func TestNavigatePath_ExtractDataFirst(t *testing.T) {
	standardOutput := `{
		"_meta": {"status": "success", "node_id": "test-node"},
		"_events": {"success": true},
		"result": {
			"name": "John",
			"age": 30,
			"user": {
				"email": "john@example.com"
			}
		}
	}`

	tests := []struct {
		name     string
		path     string
		expected interface{}
		exists   bool
	}{
		{
			name:     "navigate /name in extracted data",
			path:     "/name",
			expected: "John",
			exists:   true,
		},
		{
			name:     "navigate /age in extracted data",
			path:     "/age",
			expected: float64(30), // JSON numbers are float64
			exists:   true,
		},
		{
			name:     "navigate /user/email in extracted data",
			path:     "/user/email",
			expected: "john@example.com",
			exists:   true,
		},
		// Note: /result now means "access field named result in extracted data", not "return entire result"
		// Use "" or "/" to get entire extracted result
		{
			name:     "navigate non-existent field",
			path:     "/nonexistent",
			expected: nil,
			exists:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, exists := NavigatePath([]byte(standardOutput), tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_EventTriggers(t *testing.T) {
	standardOutput := `{
		"_meta": {"status": "success"},
		"_events": {"success": true, "error": null},
		"result": {"name": "John"}
	}`

	tests := []struct {
		name     string
		path     string
		expected interface{}
		exists   bool
	}{
		{
			name:     "access _events/success",
			path:     "_events/success",
			expected: true,
			exists:   true,
		},
		{
			name:     "access _events/error",
			path:     "_events/error",
			expected: nil,
			exists:   true,
		},
		{
			name:     "access _meta/status",
			path:     "_meta/status",
			expected: "success",
			exists:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, exists := NavigatePath([]byte(standardOutput), tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_ArrayIteration(t *testing.T) {
	standardOutput := `{
		"_meta": {"status": "success"},
		"result": [
			{"name": "John", "age": 30},
			{"name": "Jane", "age": 25}
		]
	}`

	tests := []struct {
		name     string
		path     string
		expected interface{}
		exists   bool
	}{
		// Note: /result//name would look for a field named "result" in extracted data, then iterate
		// Since result IS the extracted data (not a field in it), use //name directly
		{
			name:     "navigate //name in extracted data array",
			path:     "//name",
			expected: []interface{}{"John", "Jane"},
			exists:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, exists := NavigatePath([]byte(standardOutput), tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_BackwardCompatibility(t *testing.T) {
	// Non-StandardOutput format should still work
	nonStandardOutput := `{"name": "John", "age": 30}`

	tests := []struct {
		name     string
		path     string
		expected interface{}
		exists   bool
	}{
		{
			name:     "navigate /name in non-StandardOutput",
			path:     "/name",
			expected: "John",
			exists:   true,
		},
		{
			name:     "navigate /age in non-StandardOutput",
			path:     "/age",
			expected: float64(30),
			exists:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, exists := NavigatePath([]byte(nonStandardOutput), tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_EmptyPath(t *testing.T) {
	standardOutput := `{
		"_meta": {"status": "success"},
		"_events": {"success": true},
		"result": {
			"Assignment": [{"id": 1}, {"id": 2}],
			"Person": [{"name": "John"}]
		}
	}`

	nonStandardOutput := `{
		"Assignment": [{"id": 1}, {"id": 2}],
		"Person": [{"name": "John"}]
	}`

	tests := []struct {
		name     string
		input    string
		path     string
		expected interface{}
		exists   bool
	}{
		{
			name:  "empty path with StandardOutput extracts entire data field",
			input: standardOutput,
			path:  "",
			expected: map[string]interface{}{
				"Assignment": []interface{}{
					map[string]interface{}{"id": float64(1)},
					map[string]interface{}{"id": float64(2)},
				},
				"Person": []interface{}{
					map[string]interface{}{"name": "John"},
				},
			},
			exists: true,
		},
		{
			name:  "root path / with StandardOutput extracts entire data field",
			input: standardOutput,
			path:  "/",
			expected: map[string]interface{}{
				"Assignment": []interface{}{
					map[string]interface{}{"id": float64(1)},
					map[string]interface{}{"id": float64(2)},
				},
				"Person": []interface{}{
					map[string]interface{}{"name": "John"},
				},
			},
			exists: true,
		},
		{
			name:  "empty path with non-StandardOutput returns entire object",
			input: nonStandardOutput,
			path:  "",
			expected: map[string]interface{}{
				"Assignment": []interface{}{
					map[string]interface{}{"id": float64(1)},
					map[string]interface{}{"id": float64(2)},
				},
				"Person": []interface{}{
					map[string]interface{}{"name": "John"},
				},
			},
			exists: true,
		},
		{
			name:  "root path / with non-StandardOutput returns entire object",
			input: nonStandardOutput,
			path:  "/",
			expected: map[string]interface{}{
				"Assignment": []interface{}{
					map[string]interface{}{"id": float64(1)},
					map[string]interface{}{"id": float64(2)},
				},
				"Person": []interface{}{
					map[string]interface{}{"name": "John"},
				},
			},
			exists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, exists := NavigatePath([]byte(tt.input), tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}
