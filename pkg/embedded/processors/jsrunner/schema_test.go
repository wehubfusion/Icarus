package jsrunner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
)

func TestExecutor_SchemaAccess(t *testing.T) {
	tests := []struct {
		name           string
		script         string
		schema         map[string]interface{}
		input          map[string]interface{}
		expectedOutput string
		expectError    bool
	}{
		{
			name:   "Access schema properties",
			script: "JSON.stringify(schema.properties)",
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type": "STRING",
					},
					"age": map[string]interface{}{
						"type": "NUMBER",
					},
				},
			},
			input:          map[string]interface{}{},
			expectedOutput: `{"age":{"type":"NUMBER"},"name":{"type":"STRING"}}`,
			expectError:    false,
		},
		{
			name: "Access schema type",
			script: `
				if (schema.type === "OBJECT") {
					"type is OBJECT";
				} else {
					"type is not OBJECT";
				}
			`,
			schema: map[string]interface{}{
				"type": "OBJECT",
			},
			input:          map[string]interface{}{},
			expectedOutput: "type is OBJECT",
			expectError:    false,
		},
		{
			name: "Use schema to validate input",
			script: `
				var requiredFields = Object.keys(schema.properties).filter(function(key) {
					return schema.properties[key].required === true;
				});
				
				var missingFields = requiredFields.filter(function(field) {
					return !input.hasOwnProperty(field);
				});
				
				if (missingFields.length > 0) {
					"Missing fields: " + missingFields.join(", ");
				} else {
					"All required fields present";
				}
			`,
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type":     "STRING",
						"required": true,
					},
					"age": map[string]interface{}{
						"type":     "NUMBER",
						"required": false,
					},
				},
			},
			input: map[string]interface{}{
				"name": "John",
			},
			expectedOutput: "All required fields present",
			expectError:    false,
		},
		{
			name: "Schema-driven transformation",
			script: `
				var output = {};
				
				for (var key in schema.properties) {
					var fieldSchema = schema.properties[key];
					var value = input[key];
					
					// Apply default if value is missing
					if (value === undefined && fieldSchema.default !== undefined) {
						output[key] = fieldSchema.default;
					} else {
						output[key] = value;
					}
				}
				
				JSON.stringify(output);
			`,
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{
						"type": "STRING",
					},
					"status": map[string]interface{}{
						"type":    "STRING",
						"default": "active",
					},
					"count": map[string]interface{}{
						"type":    "NUMBER",
						"default": 0,
					},
				},
			},
			input: map[string]interface{}{
				"name": "Test",
			},
			expectedOutput: `{"count":0,"name":"Test","status":"active"}`,
			expectError:    false,
		},
		{
			name: "No schema provided - schema undefined",
			script: `
				if (typeof schema === 'undefined') {
					"schema is undefined";
				} else {
					"schema is defined";
				}
			`,
			schema:         nil,
			input:          map[string]interface{}{},
			expectedOutput: "schema is undefined",
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewExecutor()
			defer executor.Close()

			// Prepare configuration
			config := Config{
				Script: tt.script,
			}

			// Add schema if provided
			if tt.schema != nil {
				schemaBytes, err := json.Marshal(tt.schema)
				require.NoError(t, err)
				config.SchemaDefinition = schemaBytes
			}

			configBytes, err := json.Marshal(config)
			require.NoError(t, err)

			// Prepare input
			inputBytes, err := json.Marshal(tt.input)
			require.NoError(t, err)

			// Execute
			nodeConfig := embedded.NodeConfig{
				Configuration: configBytes,
				Input:         inputBytes,
			}

			result, err := executor.Execute(context.Background(), nodeConfig)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedOutput, string(result))
			}
		})
	}
}

func TestExecutor_RawByteOutput(t *testing.T) {
	tests := []struct {
		name           string
		script         string
		expectedOutput []byte
		expectError    bool
	}{
		{
			name:           "String output becomes bytes",
			script:         `"Hello World"`,
			expectedOutput: []byte("Hello World"),
			expectError:    false,
		},
		{
			name:           "Number output marshaled to JSON",
			script:         `42`,
			expectedOutput: []byte("42"),
			expectError:    false,
		},
		{
			name:           "Object output marshaled to JSON",
			script:         `({name: "John", age: 30})`,
			expectedOutput: []byte(`{"age":30,"name":"John"}`),
			expectError:    false,
		},
		{
			name:           "Array output marshaled to JSON",
			script:         `[1, 2, 3, 4, 5]`,
			expectedOutput: []byte(`[1,2,3,4,5]`),
			expectError:    false,
		},
		{
			name:           "Boolean output marshaled to JSON",
			script:         `true`,
			expectedOutput: []byte("true"),
			expectError:    false,
		},
		{
			name:           "Null output marshaled to JSON",
			script:         `null`,
			expectedOutput: []byte("null"),
			expectError:    false,
		},
		{
			name: "Complex object with nested structures",
			script: `({
				user: {
					name: "Alice",
					roles: ["admin", "user"]
				},
				timestamp: 1234567890
			})`,
			expectedOutput: []byte(`{"timestamp":1234567890,"user":{"name":"Alice","roles":["admin","user"]}}`),
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewExecutor()
			defer executor.Close()

			config := Config{
				Script: tt.script,
			}

			configBytes, err := json.Marshal(config)
			require.NoError(t, err)

			nodeConfig := embedded.NodeConfig{
				Configuration: configBytes,
				Input:         []byte(`{}`),
			}

			result, err := executor.Execute(context.Background(), nodeConfig)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedOutput, result)
			}
		})
	}
}

func TestExecutor_SchemaEnrichment_Integration(t *testing.T) {
	// This test simulates what happens after Elysium enrichment:
	// schema_id is replaced with schema definition
	executor := NewExecutor()
	defer executor.Close()

	// Define a schema (this would come from Morpheus via Elysium enrichment)
	schema := map[string]interface{}{
		"type": "OBJECT",
		"properties": map[string]interface{}{
			"id": map[string]interface{}{
				"type": "STRING",
			},
			"email": map[string]interface{}{
				"type":     "STRING",
				"required": true,
			},
			"age": map[string]interface{}{
				"type": "NUMBER",
				"min":  0,
				"max":  150,
			},
		},
	}

	schemaBytes, err := json.Marshal(schema)
	require.NoError(t, err)

	// Script that uses the schema
	script := `
		// Validate required fields
		var errors = [];
		
		for (var key in schema.properties) {
			var field = schema.properties[key];
			var value = input[key];
			
			if (field.required && value === undefined) {
				errors.push("Missing required field: " + key);
			}
			
			if (field.type === "NUMBER" && value !== undefined) {
				if (field.min !== undefined && value < field.min) {
					errors.push(key + " is below minimum: " + value + " < " + field.min);
				}
				if (field.max !== undefined && value > field.max) {
					errors.push(key + " is above maximum: " + value + " > " + field.max);
				}
			}
		}
		
		if (errors.length > 0) {
			JSON.stringify({valid: false, errors: errors});
		} else {
			JSON.stringify({valid: true, data: input});
		}
	`

	config := Config{
		Script:           script,
		SchemaDefinition: schemaBytes,
	}

	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	t.Run("Valid input", func(t *testing.T) {
		input := map[string]interface{}{
			"id":    "123",
			"email": "test@example.com",
			"age":   25,
		}

		inputBytes, err := json.Marshal(input)
		require.NoError(t, err)

		nodeConfig := embedded.NodeConfig{
			Configuration: configBytes,
			Input:         inputBytes,
		}

		result, err := executor.Execute(context.Background(), nodeConfig)
		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.True(t, output["valid"].(bool))
		assert.NotNil(t, output["data"])
	})

	t.Run("Missing required field", func(t *testing.T) {
		input := map[string]interface{}{
			"id":  "123",
			"age": 25,
		}

		inputBytes, err := json.Marshal(input)
		require.NoError(t, err)

		nodeConfig := embedded.NodeConfig{
			Configuration: configBytes,
			Input:         inputBytes,
		}

		result, err := executor.Execute(context.Background(), nodeConfig)
		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.False(t, output["valid"].(bool))
		assert.Contains(t, output["errors"], "Missing required field: email")
	})

	t.Run("Value out of range", func(t *testing.T) {
		input := map[string]interface{}{
			"id":    "123",
			"email": "test@example.com",
			"age":   200,
		}

		inputBytes, err := json.Marshal(input)
		require.NoError(t, err)

		nodeConfig := embedded.NodeConfig{
			Configuration: configBytes,
			Input:         inputBytes,
		}

		result, err := executor.Execute(context.Background(), nodeConfig)
		require.NoError(t, err)

		var output map[string]interface{}
		err = json.Unmarshal(result, &output)
		require.NoError(t, err)

		assert.False(t, output["valid"].(bool))
		errors := output["errors"].([]interface{})
		found := false
		for _, e := range errors {
			if s, ok := e.(string); ok && s == "age is above maximum: 200 > 150" {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected age validation error")
	})
}

