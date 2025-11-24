package pathutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/output"
)

func TestNavigatePath_ExtractDataFirst(t *testing.T) {
	// Create StandardOutput with result data
	standardOutput := &StandardOutput{
		Meta: output.MetaData{
			Status: "success",
			NodeID: "test-node",
		},
		Events: output.EventEndpoints{
			Success: true,
		},
		Result: map[string]interface{}{
			"name": "John",
			"age":  float64(30),
			"user": map[string]interface{}{
				"email": "john@example.com",
			},
		},
	}

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
			value, exists := NavigatePath(standardOutput, tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_EventTriggers(t *testing.T) {
	standardOutput := &StandardOutput{
		Meta: output.MetaData{
			Status: "success",
		},
		Events: output.EventEndpoints{
			Success: true,
			Error:   nil,
		},
		Result: map[string]interface{}{
			"name": "John",
		},
	}

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
			value, exists := NavigatePath(standardOutput, tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_ArrayIteration(t *testing.T) {
	standardOutput := &StandardOutput{
		Meta: output.MetaData{
			Status: "success",
		},
		Result: []interface{}{
			map[string]interface{}{"name": "John", "age": float64(30)},
			map[string]interface{}{"name": "Jane", "age": float64(25)},
		},
	}

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
			value, exists := NavigatePath(standardOutput, tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_BackwardCompatibility(t *testing.T) {
	// Test with plain result data (no StandardOutput wrapping)
	nonStandardOutput := &StandardOutput{
		Result: map[string]interface{}{"name": "John", "age": float64(30)},
	}

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
			value, exists := NavigatePath(nonStandardOutput, tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}

func TestNavigatePath_EmptyPath(t *testing.T) {
	standardOutput := &StandardOutput{
		Meta: output.MetaData{
			Status: "success",
		},
		Events: output.EventEndpoints{
			Success: true,
		},
		Result: map[string]interface{}{
			"Assignment": []interface{}{
				map[string]interface{}{"id": float64(1)},
				map[string]interface{}{"id": float64(2)},
			},
			"Person": []interface{}{
				map[string]interface{}{"name": "John"},
			},
		},
	}

	nonStandardOutput := &StandardOutput{
		Result: map[string]interface{}{
			"Assignment": []interface{}{
				map[string]interface{}{"id": float64(1)},
				map[string]interface{}{"id": float64(2)},
			},
			"Person": []interface{}{
				map[string]interface{}{"name": "John"},
			},
		},
	}

	tests := []struct {
		name     string
		input    *StandardOutput
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
			value, exists := NavigatePath(tt.input, tt.path)

			assert.Equal(t, tt.exists, exists)
			if exists {
				assert.Equal(t, tt.expected, value)
			}
		})
	}
}
