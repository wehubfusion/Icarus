package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
)

// Test Parse Operation
func TestJSONOps_Parse(t *testing.T) {
	executor := jsonops.NewExecutor()

	tests := []struct {
		name        string
		input       string
		paths       map[string]string
		expected    map[string]interface{}
		shouldError bool
	}{
		{
			name:  "extract simple paths",
			input: `{"user": {"name": "Alice", "age": 30}, "active": true}`,
			paths: map[string]string{
				"userName": "user.name",
				"userAge":  "user.age",
				"isActive": "active",
			},
			expected: map[string]interface{}{
				"userName": "Alice",
				"userAge":  float64(30),
				"isActive": true,
			},
		},
		{
			name:  "extract nested paths",
			input: `{"data": {"items": [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}]}}`,
			paths: map[string]string{
				"firstItem": "data.items.0",
				"firstName": "data.items.0.name",
			},
			expected: map[string]interface{}{
				"firstItem": map[string]interface{}{"id": float64(1), "name": "Item 1"},
				"firstName": "Item 1",
			},
		},
		{
			name:  "handle missing paths gracefully",
			input: `{"user": {"name": "Alice"}}`,
			paths: map[string]string{
				"userName": "user.name",
				"userAge":  "user.age",
			},
			expected: map[string]interface{}{
				"userName": "Alice",
				"userAge":  nil,
			},
		},
		{
			name:  "support slash notation",
			input: `{"data": {"user": {"profile": {"email": "alice@example.com"}}}}`,
			paths: map[string]string{
				"email": "/data/user/profile/email",
			},
			expected: map[string]interface{}{
				"email": "alice@example.com",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createConfig(t, "parse", map[string]interface{}{
				"paths": tt.paths,
			})

			result, err := executor.Execute(context.Background(), embedded.NodeConfig{
				Configuration: config,
				Input:         []byte(tt.input),
			})

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			var output map[string]interface{}
			err = json.Unmarshal(result, &output)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, output)
		})
	}
}

// Test Render Operation
func TestJSONOps_Render(t *testing.T) {
	executor := jsonops.NewExecutor()

	tests := []struct {
		name        string
		input       string
		template    map[string]interface{}
		expected    map[string]interface{}
		shouldError bool
	}{
		{
			name:  "render with placeholders",
			input: `{"firstName": "Alice", "lastName": "Smith", "age": 30}`,
			template: map[string]interface{}{
				"fullName": "{{firstName}} {{lastName}}",
				"age":      "{{age}}",
			},
			expected: map[string]interface{}{
				"fullName": "Alice Smith",
				"age":      float64(30), // Single placeholder returns typed value
			},
		},
		{
			name:  "render with direct value references",
			input: `{"user": {"name": "Bob", "score": 95}}`,
			template: map[string]interface{}{
				"userName":  "{{user.name}}",
				"userScore": "{{user.score}}",
			},
			expected: map[string]interface{}{
				"userName":  "Bob",
				"userScore": float64(95),
			},
		},
		{
			name:  "render nested structures",
			input: `{"user": {"name": "Charlie", "email": "charlie@example.com"}}`,
			template: map[string]interface{}{
				"profile": map[string]interface{}{
					"displayName": "{{user.name}}",
					"contact":     "{{user.email}}",
				},
			},
			expected: map[string]interface{}{
				"profile": map[string]interface{}{
					"displayName": "Charlie",
					"contact":     "charlie@example.com",
				},
			},
		},
		{
			name:  "render arrays",
			input: `{"count": 3, "status": "active"}`,
			template: map[string]interface{}{
				"items": []interface{}{
					"{{status}}",
					"{{count}}",
				},
			},
			expected: map[string]interface{}{
				"items": []interface{}{
					"active",
					float64(3), // Single placeholder returns typed value
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := createConfig(t, "render", map[string]interface{}{
				"template": tt.template,
			})

			result, err := executor.Execute(context.Background(), embedded.NodeConfig{
				Configuration: config,
				Input:         []byte(tt.input),
			})

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			var output map[string]interface{}
			err = json.Unmarshal(result, &output)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, output)
		})
	}
}

// Test Query Operation
func TestJSONOps_Query(t *testing.T) {
	executor := jsonops.NewExecutor()

	tests := []struct {
		name        string
		input       string
		path        string
		multiple    bool
		paths       []string
		expected    interface{}
		shouldError bool
	}{
		{
			name:     "query simple path",
			input:    `{"user": {"name": "Alice", "age": 30}}`,
			path:     "user.name",
			expected: map[string]interface{}{"result": "Alice"},
		},
		{
			name:     "query with wildcard",
			input:    `{"items": [{"price": 10}, {"price": 20}, {"price": 30}]}`,
			path:     "items.*.price",
			expected: map[string]interface{}{"result": []interface{}{float64(10), float64(20), float64(30)}},
		},
		{
			name:     "query array element",
			input:    `{"items": [{"id": 1}, {"id": 2}, {"id": 3}]}`,
			path:     "items.1",
			expected: map[string]interface{}{"result": map[string]interface{}{"id": float64(2)}},
		},
		{
			name:     "query multiple paths",
			input:    `{"user": {"name": "Bob", "age": 25}, "active": true}`,
			multiple: true,
			paths:    []string{"user.name", "user.age", "active"},
			expected: map[string]interface{}{
				"result": map[string]interface{}{
					"user.name": "Bob",
					"user.age":  float64(25),
					"active":    true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]interface{}{}
			if tt.multiple {
				params["multiple"] = true
				params["paths"] = tt.paths
			} else {
				params["path"] = tt.path
			}

			config := createConfig(t, "query", params)

			result, err := executor.Execute(context.Background(), embedded.NodeConfig{
				Configuration: config,
				Input:         []byte(tt.input),
			})

			if tt.shouldError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			var output map[string]interface{}
			err = json.Unmarshal(result, &output)
			require.NoError(t, err)

			assert.Equal(t, tt.expected, output)
		})
	}
}

// Test Transform Operation
func TestJSONOps_Transform(t *testing.T) {
	executor := jsonops.NewExecutor()

	t.Run("set operation", func(t *testing.T) {
		config := createConfig(t, "transform", map[string]interface{}{
			"type":  "set",
			"path":  "user.email",
			"value": "alice@example.com",
		})

		input := `{"user": {"name": "Alice", "age": 30}}`
		result, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(input),
		})

		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.Equal(t, "alice@example.com", output["user"].(map[string]interface{})["email"])
	})

	t.Run("delete operation", func(t *testing.T) {
		config := createConfig(t, "transform", map[string]interface{}{
			"type": "delete",
			"path": "user.age",
		})

		input := `{"user": {"name": "Alice", "age": 30}}`
		result, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(input),
		})

		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		user := output["user"].(map[string]interface{})
		assert.Equal(t, "Alice", user["name"])
		assert.NotContains(t, user, "age")
	})

	t.Run("merge operation", func(t *testing.T) {
		mergeData := map[string]interface{}{
			"user": map[string]interface{}{
				"email": "alice@example.com",
				"age":   31,
			},
			"active": true,
		}

		mergeDataJSON, _ := json.Marshal(mergeData)

		config := createConfig(t, "transform", map[string]interface{}{
			"type":       "merge",
			"merge_data": json.RawMessage(mergeDataJSON),
		})

		input := `{"user": {"name": "Alice", "age": 30}}`
		result, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(input),
		})

		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		user := output["user"].(map[string]interface{})
		assert.Equal(t, "Alice", user["name"])
		assert.Equal(t, float64(31), user["age"])
		assert.Equal(t, "alice@example.com", user["email"])
		assert.Equal(t, true, output["active"])
	})
}

// Test Validate Operation
func TestJSONOps_Validate(t *testing.T) {
	executor := jsonops.NewExecutor()

	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type": "string",
			},
			"age": map[string]interface{}{
				"type":    "number",
				"minimum": 0,
			},
		},
		"required": []string{"name", "age"},
	}

	schemaJSON, _ := json.Marshal(schema)

	t.Run("valid data", func(t *testing.T) {
		config := createConfig(t, "validate", map[string]interface{}{
			"schema": json.RawMessage(schemaJSON),
		})

		input := `{"name": "Alice", "age": 30}`
		result, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(input),
		})

		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.Equal(t, true, output["valid"])
	})

	t.Run("invalid data", func(t *testing.T) {
		config := createConfig(t, "validate", map[string]interface{}{
			"schema": json.RawMessage(schemaJSON),
		})

		input := `{"name": "Alice"}` // missing required field "age"
		result, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(input),
		})

		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.Equal(t, false, output["valid"])
		assert.NotNil(t, output["errors"])
	})
}

// Test Configuration Validation
func TestJSONOps_ConfigValidation(t *testing.T) {
	executor := jsonops.NewExecutor()

	t.Run("missing operation", func(t *testing.T) {
		config := []byte(`{"params": {}}`)
		_, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(`{}`),
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "operation cannot be empty")
	})

	t.Run("invalid operation", func(t *testing.T) {
		config := []byte(`{"operation": "invalid", "params": {}}`)
		_, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(`{}`),
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid operation")
	})

	t.Run("parse without paths", func(t *testing.T) {
		config := []byte(`{"operation": "parse", "params": {}}`)
		_, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(`{}`),
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "at least one path must be specified")
	})
}

// Test Edge Cases
func TestJSONOps_EdgeCases(t *testing.T) {
	executor := jsonops.NewExecutor()

	t.Run("empty JSON object", func(t *testing.T) {
		config := createConfig(t, "parse", map[string]interface{}{
			"paths": map[string]string{
				"value": "missing.path",
			},
		})

		result, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(`{}`),
		})

		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.Nil(t, output["value"])
	})

	t.Run("invalid JSON input", func(t *testing.T) {
		config := createConfig(t, "parse", map[string]interface{}{
			"paths": map[string]string{
				"value": "some.path",
			},
		})

		_, err := executor.Execute(context.Background(), embedded.NodeConfig{
			Configuration: config,
			Input:         []byte(`not valid json`),
		})

		assert.Error(t, err)
	})
}

// Helper function to create configuration
func createConfig(t *testing.T, operation string, params map[string]interface{}) []byte {
	paramsJSON, err := json.Marshal(params)
	require.NoError(t, err)

	configMap := map[string]interface{}{
		"operation": operation,
		"params":    json.RawMessage(paramsJSON),
	}

	config, err := json.Marshal(configMap)
	require.NoError(t, err)

	return config
}
