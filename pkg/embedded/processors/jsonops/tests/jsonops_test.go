package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
)

func wrapJSONOpsInput(t *testing.T, data interface{}) []byte {
	t.Helper()
	envelope := map[string]interface{}{"data": data}
	bytes, err := json.Marshal(envelope)
	require.NoError(t, err)
	return bytes
}

func decodeJSONOpsEnvelope(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()
	var wrapper map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &wrapper))

	dataField, ok := wrapper["data"].(string)
	require.True(t, ok, "output missing data field")

	decoded, err := base64.StdEncoding.DecodeString(dataField)
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded, &result))
	return result
}

func sampleSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "OBJECT",
		"properties": map[string]interface{}{
			"email": map[string]interface{}{
				"type":     "STRING",
				"required": true,
			},
			"age": map[string]interface{}{
				"type": "NUMBER",
			},
			"status": map[string]interface{}{
				"type":    "STRING",
				"default": "active",
			},
		},
	}
}

func TestJSONOpsParseValidEnvelope(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())

	cfg := map[string]interface{}{
		"operation": "parse",
		"schema":    json.RawMessage(schemaBytes),
	}
	cfgBytes, _ := json.Marshal(cfg)

	input := wrapJSONOpsInput(t, map[string]interface{}{
		"email": "user@example.com",
		"age":   30,
	})

	out, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &result))
	require.Equal(t, "user@example.com", result["email"])
	require.Equal(t, float64(30), result["age"])
	require.Equal(t, "active", result["status"], "default applied")
}

func TestJSONOpsParseMissingSchema(t *testing.T) {
	executor := jsonops.NewExecutor()
	cfg := map[string]interface{}{"operation": "parse"}
	cfgBytes, _ := json.Marshal(cfg)

	input := wrapJSONOpsInput(t, map[string]interface{}{"email": "user@example.com"})

	_, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.Error(t, err)
}

func TestJSONOpsParseRequiresDataField(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())
	cfg := map[string]interface{}{"operation": "parse", "schema": json.RawMessage(schemaBytes)}
	cfgBytes, _ := json.Marshal(cfg)

	_, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         []byte(`{"email":"user@example.com"}`),
	})
	require.Error(t, err)
}

func TestJSONOpsProduceWrapsBase64(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())
	cfg := map[string]interface{}{"operation": "produce", "schema": json.RawMessage(schemaBytes)}
	cfgBytes, _ := json.Marshal(cfg)

	input := []byte(`{"email":"user@example.com","age":40,"extra":"ignore"}`)

	out, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.NoError(t, err)

	decoded := decodeJSONOpsEnvelope(t, out)
	require.Equal(t, "user@example.com", decoded["email"])
	require.Nil(t, decoded["extra"], "structure_data should drop unknown fields")
}

func TestJSONOpsProducePretty(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())
	cfg := map[string]interface{}{
		"operation": "produce",
		"schema":    json.RawMessage(schemaBytes),
		"pretty":    true,
	}
	cfgBytes, _ := json.Marshal(cfg)

	input := []byte(`{"email":"user@example.com"}`)

	out, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.NoError(t, err)

	var wrapper map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &wrapper))
	data := wrapper["data"].(string)
	decoded, err := base64.StdEncoding.DecodeString(data)
	require.NoError(t, err)
	require.Contains(t, string(decoded), "\n", "pretty output should contain newlines")
}

func TestJSONOpsProduceStrictValidation(t *testing.T) {
	executor := jsonops.NewExecutor()
	schema := map[string]interface{}{
		"type": "OBJECT",
		"properties": map[string]interface{}{
			"email": map[string]interface{}{
				"type":     "STRING",
				"required": true,
				"validation": map[string]interface{}{
					"format": "email",
				},
			},
		},
	}
	schemaBytes, _ := json.Marshal(schema)

	cfg := map[string]interface{}{
		"operation":         "produce",
		"schema":            json.RawMessage(schemaBytes),
		"strict_validation": true,
	}
	cfgBytes, _ := json.Marshal(cfg)

	input := []byte(`{"email":"not-an-email"}`)

	_, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.Error(t, err)
}

func TestAutoWrapForIteration(t *testing.T) {
	tests := []struct {
		name           string
		input          interface{} // The data field content
		schema         map[string]interface{}
		autoWrap       *bool
		expectError    bool
		validateOutput func(t *testing.T, output []byte)
	}{
		{
			name:  "Array of strings with STRING schema - auto-wrap enabled (default)",
			input: []interface{}{"manual", "saml2", "manual"},
			schema: map[string]interface{}{
				"type": "STRING",
			},
			autoWrap:    nil, // Use default (true)
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 3 {
					t.Errorf("Expected 3 items, got %d", len(result))
				}
				// Verify content is preserved
				if result[0] != "manual" || result[1] != "saml2" || result[2] != "manual" {
					t.Errorf("Expected [manual, saml2, manual], got %v", result)
				}
			},
		},
		{
			name:  "Array of numbers with NUMBER schema - auto-wrap enabled (default)",
			input: []interface{}{float64(0), float64(1), float64(1), float64(0)},
			schema: map[string]interface{}{
				"type": "NUMBER",
			},
			autoWrap:    nil, // Use default (true)
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 4 {
					t.Errorf("Expected 4 items, got %d", len(result))
				}
				// Verify content is preserved
				expected := []float64{0, 1, 1, 0}
				for i, v := range result {
					if v.(float64) != expected[i] {
						t.Errorf("Expected %v at index %d, got %v", expected[i], i, v)
					}
				}
			},
		},
		{
			name:  "Array with STRING schema - auto-wrap explicitly enabled",
			input: []interface{}{"value1", "value2"},
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"field": map[string]interface{}{"type": "STRING"},
				},
			},
			autoWrap:    func() *bool { b := true; return &b }(), // Explicitly enabled
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 2 {
					t.Errorf("Expected 2 items, got %d", len(result))
				}
			},
		},
		{
			name:  "Array with STRING schema - auto-wrap disabled passes through",
			input: []interface{}{"manual", "saml2"},
			schema: map[string]interface{}{
				"type": "STRING",
			},
			autoWrap:    func() *bool { b := false; return &b }(), // Explicitly disabled
			expectError: false,                                    // Schema engine is lenient, passes data through
			validateOutput: func(t *testing.T, output []byte) {
				// When auto-wrap is disabled, array data passes through even with STRING schema
				// (schema engine is lenient and doesn't strictly enforce root type)
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 2 {
					t.Errorf("Expected 2 items, got %d", len(result))
				}
			},
		},
		{
			name:  "Array with ARRAY schema - no wrapping needed",
			input: []interface{}{float64(1), float64(2), float64(3)},
			schema: map[string]interface{}{
				"type":  "ARRAY",
				"items": map[string]interface{}{"type": "NUMBER"},
			},
			autoWrap:    nil,
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 3 {
					t.Errorf("Expected 3 items, got %d", len(result))
				}
			},
		},
		{
			name:  "Object with OBJECT schema - no wrapping needed",
			input: map[string]interface{}{"auth": "manual"},
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"auth": map[string]interface{}{"type": "STRING"},
				},
			},
			autoWrap:    nil,
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result map[string]interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if result["auth"] != "manual" {
					t.Errorf("Expected auth=manual, got %v", result["auth"])
				}
			},
		},
		{
			name:  "Empty array with STRING schema - auto-wrap enabled",
			input: []interface{}{},
			schema: map[string]interface{}{
				"type": "STRING",
			},
			autoWrap:    nil,
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 0 {
					t.Errorf("Expected empty array, got %d items", len(result))
				}
			},
		},
		{
			name: "Array of objects with OBJECT schema - auto-wrap enabled",
			input: []interface{}{
				map[string]interface{}{"name": "Alice"},
				map[string]interface{}{"name": "Bob"},
			},
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{"type": "STRING"},
				},
			},
			autoWrap:    nil,
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 2 {
					t.Errorf("Expected 2 items, got %d", len(result))
				}
				// Verify first item
				firstItem, ok := result[0].(map[string]interface{})
				if !ok {
					t.Fatalf("Expected first item to be object")
				}
				if firstItem["name"] != "Alice" {
					t.Errorf("Expected first name=Alice, got %v", firstItem["name"])
				}
			},
		},
		{
			name:  "REAL WORKFLOW: Primitive string array with OBJECT schema - wraps into objects",
			input: []interface{}{"manual", "saml2", "manual"},
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"auth": map[string]interface{}{"type": "STRING"},
				},
			},
			autoWrap:    nil, // Use default (true)
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []map[string]interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 3 {
					t.Errorf("Expected 3 items, got %d", len(result))
				}
				// Verify each primitive is wrapped into object with property name
				expected := []string{"manual", "saml2", "manual"}
				for i, item := range result {
					if item["auth"] != expected[i] {
						t.Errorf("Expected auth=%s at index %d, got %v", expected[i], i, item["auth"])
					}
				}
			},
		},
		{
			name:  "REAL WORKFLOW: Primitive number array with OBJECT schema - wraps into objects",
			input: []interface{}{float64(0), float64(1), float64(1), float64(0)},
			schema: map[string]interface{}{
				"type": "OBJECT",
				"properties": map[string]interface{}{
					"data": map[string]interface{}{"type": "NUMBER"},
				},
			},
			autoWrap:    nil, // Use default (true)
			expectError: false,
			validateOutput: func(t *testing.T, output []byte) {
				var result []map[string]interface{}
				if err := json.Unmarshal(output, &result); err != nil {
					t.Fatalf("Failed to unmarshal output: %v", err)
				}
				if len(result) != 4 {
					t.Errorf("Expected 4 items, got %d", len(result))
				}
				// Verify each primitive is wrapped into object with property name
				expected := []float64{0, 1, 1, 0}
				for i, item := range result {
					if item["data"] != expected[i] {
						t.Errorf("Expected data=%v at index %d, got %v", expected[i], i, item["data"])
					}
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create executor
			executor := jsonops.NewExecutor()

			// Create config
			schemaBytes, err := json.Marshal(tt.schema)
			require.NoError(t, err)

			cfg := map[string]interface{}{
				"operation": "parse",
				"schema":    json.RawMessage(schemaBytes),
			}
			if tt.autoWrap != nil {
				cfg["auto_wrap_for_iteration"] = *tt.autoWrap
			}
			cfgBytes, err := json.Marshal(cfg)
			require.NoError(t, err)

			// Create input envelope with data field
			input := wrapJSONOpsInput(t, tt.input)

			// Execute parse
			output, err := executor.Execute(context.Background(), embedded.NodeConfig{
				Configuration: cfgBytes,
				Input:         input,
			})

			// Check error expectation
			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				return
			}
			require.NoError(t, err, "Unexpected error")

			// Validate output if provided
			if tt.validateOutput != nil {
				tt.validateOutput(t, output)
			}
		})
	}
}
