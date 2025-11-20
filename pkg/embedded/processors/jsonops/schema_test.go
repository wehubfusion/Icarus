package jsonops

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Helper function to create test executor
func createTestExecutor() *Executor {
	return NewExecutor()
}

// Helper function to create test schema
func createTestSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "OBJECT",
		"properties": map[string]interface{}{
			"email": map[string]interface{}{
				"type":     "STRING",
				"required": true,
				"validation": map[string]interface{}{
					"format": "email",
				},
			},
			"age": map[string]interface{}{
				"type": "NUMBER",
				"validation": map[string]interface{}{
					"minimum": float64(0),
					"maximum": float64(150),
				},
			},
			"status": map[string]interface{}{
				"type":    "STRING",
				"default": "active",
			},
		},
	}
}

// TestParseOperation_ValidInput tests parse operation with valid input
func TestParseOperation_ValidInput(t *testing.T) {
	executor := createTestExecutor()
	schema := createTestSchema()
	schemaBytes, _ := json.Marshal(schema)

	config := map[string]interface{}{
		"operation": "parse",
		"schema":    json.RawMessage(schemaBytes),
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com", "age": 30}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify output is valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Expected valid JSON output, got error: %v", err)
	}

	if result["email"] != "user@example.com" {
		t.Errorf("Expected email to be preserved, got %v", result["email"])
	}
}

// TestParseOperation_WithDefaults tests parse operation applies defaults
func TestParseOperation_WithDefaults(t *testing.T) {
	executor := createTestExecutor()
	schema := createTestSchema()
	schemaBytes, _ := json.Marshal(schema)

	config := map[string]interface{}{
		"operation":      "parse",
		"schema":         json.RawMessage(schemaBytes),
		"apply_defaults": true,
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com", "age": 25}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(output, &result)

	// Verify default was applied
	if result["status"] != "active" {
		t.Errorf("Expected status default 'active', got %v", result["status"])
	}
}

// TestParseOperation_StructureData tests parse operation with structure_data enabled
func TestParseOperation_StructureData(t *testing.T) {
	executor := createTestExecutor()
	schema := createTestSchema()
	schemaBytes, _ := json.Marshal(schema)

	structureTrue := true
	config := Config{
		Operation:     "parse",
		Schema:        schemaBytes,
		StructureData: &structureTrue,
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com", "age": 30, "extra_field": "should be removed"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(output, &result)

	// Verify extra field was removed
	if result["extra_field"] != nil {
		t.Error("Expected extra_field to be removed by structure_data")
	}

	// Verify schema fields are preserved
	if result["email"] != "user@example.com" {
		t.Error("Expected email to be preserved")
	}
}

// TestParseOperation_InvalidJSON tests parse operation with invalid JSON
func TestParseOperation_InvalidJSON(t *testing.T) {
	executor := createTestExecutor()
	schemaBytes := []byte(`{"type": "OBJECT"}`)

	config := map[string]interface{}{
		"operation": "parse",
		"schema":    json.RawMessage(schemaBytes),
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`invalid json {{{`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	_, err := executor.Execute(context.Background(), nodeConfig)
	if err == nil {
		t.Fatal("Expected error for invalid JSON input, got nil")
	}

	if !strings.Contains(err.Error(), "not valid JSON") {
		t.Errorf("Expected error message about invalid JSON, got: %s", err.Error())
	}
}

// TestParseOperation_MissingSchema tests parse operation without schema
func TestParseOperation_MissingSchema(t *testing.T) {
	executor := createTestExecutor()

	config := map[string]interface{}{
		"operation": "parse",
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	_, err := executor.Execute(context.Background(), nodeConfig)
	if err == nil {
		t.Fatal("Expected error for missing schema, got nil")
	}

	if !strings.Contains(err.Error(), "schema_id") && !strings.Contains(err.Error(), "schema") {
		t.Errorf("Expected error about missing schema, got: %s", err.Error())
	}
}

// TestParseOperation_SchemaIDNotEnriched tests parse with unenriched schema_id
func TestParseOperation_SchemaIDNotEnriched(t *testing.T) {
	executor := createTestExecutor()

	config := map[string]interface{}{
		"operation": "parse",
		"schema_id": "test-schema-id",
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	_, err := executor.Execute(context.Background(), nodeConfig)
	if err == nil {
		t.Fatal("Expected error for unenriched schema_id, got nil")
	}

	if !strings.Contains(err.Error(), "not enriched") {
		t.Errorf("Expected error about unenriched schema_id, got: %s", err.Error())
	}
}

// TestProduceOperation_ValidInput tests produce operation with valid input
func TestProduceOperation_ValidInput(t *testing.T) {
	executor := createTestExecutor()
	schema := createTestSchema()
	schemaBytes, _ := json.Marshal(schema)

	config := map[string]interface{}{
		"operation": "produce",
		"schema":    json.RawMessage(schemaBytes),
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com", "age": 30, "status": "active"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Parse output
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("Expected valid JSON output, got error: %v", err)
	}

	// Verify output structure
	if result["encoding"] != "base64" {
		t.Errorf("Expected encoding to be 'base64', got %v", result["encoding"])
	}

	// Decode base64 result
	encodedData, ok := result["result"].(string)
	if !ok {
		t.Fatal("Expected result field to be string")
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(encodedData)
	if err != nil {
		t.Fatalf("Failed to decode base64: %v", err)
	}

	// Verify decoded data
	var decodedData map[string]interface{}
	if err := json.Unmarshal(decodedBytes, &decodedData); err != nil {
		t.Fatalf("Failed to parse decoded JSON: %v", err)
	}

	if decodedData["email"] != "user@example.com" {
		t.Errorf("Expected email to be preserved, got %v", decodedData["email"])
	}
}

// TestProduceOperation_WithPretty tests produce operation with pretty formatting
func TestProduceOperation_WithPretty(t *testing.T) {
	executor := createTestExecutor()
	schema := createTestSchema()
	schemaBytes, _ := json.Marshal(schema)

	config := map[string]interface{}{
		"operation": "produce",
		"schema":    json.RawMessage(schemaBytes),
		"pretty":    true,
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com", "age": 30, "status": "active"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(output, &result)

	// Decode base64 result
	encodedData := result["result"].(string)
	decodedBytes, _ := base64.StdEncoding.DecodeString(encodedData)

	// Check if pretty formatted (contains newlines)
	if !strings.Contains(string(decodedBytes), "\n") {
		t.Error("Expected pretty formatted JSON with newlines")
	}
}

// TestProduceOperation_StructureData tests produce removes extra fields by default
func TestProduceOperation_StructureData(t *testing.T) {
	executor := createTestExecutor()
	schema := createTestSchema()
	schemaBytes, _ := json.Marshal(schema)

	config := map[string]interface{}{
		"operation": "produce",
		"schema":    json.RawMessage(schemaBytes),
		// structure_data defaults to true for produce
	}
	configBytes, _ := json.Marshal(config)

	input := []byte(`{"email": "user@example.com", "age": 30, "status": "active", "extra_field": "should be removed"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	var result map[string]interface{}
	json.Unmarshal(output, &result)

	// Decode and verify
	encodedData := result["result"].(string)
	decodedBytes, _ := base64.StdEncoding.DecodeString(encodedData)

	var decodedData map[string]interface{}
	json.Unmarshal(decodedBytes, &decodedData)

	// Verify extra field was removed
	if decodedData["extra_field"] != nil {
		t.Error("Expected extra_field to be removed by structure_data (default true for produce)")
	}
}

// TestProduceOperation_StrictValidation tests produce fails with invalid data in strict mode
func TestProduceOperation_StrictValidation(t *testing.T) {
	executor := createTestExecutor()
	
	// Schema requiring valid email
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

	strictTrue := true
	config := Config{
		Operation:        "produce",
		Schema:           schemaBytes,
		StrictValidation: &strictTrue,
	}
	configBytes, _ := json.Marshal(config)

	// Invalid email format
	input := []byte(`{"email": "not-an-email"}`)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         input,
	}

	_, err := executor.Execute(context.Background(), nodeConfig)
	if err == nil {
		t.Fatal("Expected validation error in strict mode, got nil")
	}
}

// TestConfigValidation_InvalidOperation tests config validation with invalid operation
func TestConfigValidation_InvalidOperation(t *testing.T) {
	executor := createTestExecutor()

	config := map[string]interface{}{
		"operation": "invalid_operation",
		"schema_id": "test-schema",
	}
	configBytes, _ := json.Marshal(config)

	nodeConfig := embedded.NodeConfig{
		Configuration: configBytes,
		Input:         []byte(`{}`),
	}

	_, err := executor.Execute(context.Background(), nodeConfig)
	if err == nil {
		t.Fatal("Expected error for invalid operation, got nil")
	}

	if !strings.Contains(err.Error(), "invalid operation") {
		t.Errorf("Expected error about invalid operation, got: %s", err.Error())
	}
}

// TestConfigDefaults tests that operation-specific defaults are applied correctly
func TestConfigDefaults(t *testing.T) {
	tests := []struct {
		name              string
		operation         string
		expectedDefaults  bool
		expectedStructure bool
		expectedStrict    bool
	}{
		{
			name:              "parse defaults",
			operation:         "parse",
			expectedDefaults:  true,
			expectedStructure: false,
			expectedStrict:    false,
		},
		{
			name:              "produce defaults",
			operation:         "produce",
			expectedDefaults:  false,
			expectedStructure: true,
			expectedStrict:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Operation: tt.operation,
				SchemaID:  "test-schema",
			}

			if config.GetApplyDefaults() != tt.expectedDefaults {
				t.Errorf("Expected ApplyDefaults=%v, got %v", tt.expectedDefaults, config.GetApplyDefaults())
			}

			if config.GetStructureData() != tt.expectedStructure {
				t.Errorf("Expected StructureData=%v, got %v", tt.expectedStructure, config.GetStructureData())
			}

			if config.GetStrictValidation() != tt.expectedStrict {
				t.Errorf("Expected StrictValidation=%v, got %v", tt.expectedStrict, config.GetStrictValidation())
			}
		})
	}
}
