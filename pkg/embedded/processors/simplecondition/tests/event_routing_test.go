package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
)

func TestSimpleCondition_EventOutputs(t *testing.T) {
	executor := simplecondition.NewExecutor()

	t.Run("Condition TRUE - true event fires", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "AND",
			"conditions": []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "user.age",
					"operator":       "greater_than_or_equal",
					"expected_value": 18,
				},
			},
		}

		configBytes, _ := json.Marshal(config)
		input := `{"user": {"age": 25}}`

		nodeConfig := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input),
		}

		output, err := executor.Execute(context.Background(), nodeConfig)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Parse output
		var result map[string]interface{}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		// Check overall result is true
		if resultVal, ok := result["result"].(bool); !ok || !resultVal {
			t.Errorf("Expected result to be true, got: %v", result["result"])
		}

		// Check true event fires (has truthy value)
		if trueVal := result["true"]; trueVal != true {
			t.Errorf("Expected 'true' event to fire (be true), got: %v", trueVal)
		}

		// Check false event does NOT fire (is null)
		if falseVal := result["false"]; falseVal != nil {
			t.Errorf("Expected 'false' event to NOT fire (be nil), got: %v", falseVal)
		}
	})

	t.Run("Condition FALSE - false event fires", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "AND",
			"conditions": []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "user.age",
					"operator":       "greater_than_or_equal",
					"expected_value": 18,
				},
			},
		}

		configBytes, _ := json.Marshal(config)
		input := `{"user": {"age": 15}}`  // Age is LESS than 18

		nodeConfig := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input),
		}

		output, err := executor.Execute(context.Background(), nodeConfig)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Parse output
		var result map[string]interface{}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		// Check overall result is false
		if resultVal, ok := result["result"].(bool); ok && resultVal {
			t.Errorf("Expected result to be false, got: %v", result["result"])
		}

		// Check false event fires (has truthy value)
		if falseVal := result["false"]; falseVal != true {
			t.Errorf("Expected 'false' event to fire (be true), got: %v", falseVal)
		}

		// Check true event does NOT fire (is null)
		if trueVal := result["true"]; trueVal != nil {
			t.Errorf("Expected 'true' event to NOT fire (be nil), got: %v", trueVal)
		}
	})

	t.Run("Both endpoints always present", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "OR",
			"conditions": []map[string]interface{}{
				{
					"name":           "check_status",
					"field_path":     "status",
					"operator":       "equals",
					"expected_value": "active",
				},
			},
		}

		configBytes, _ := json.Marshal(config)
		input := `{"status": "active"}`

		nodeConfig := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input),
		}

		output, err := executor.Execute(context.Background(), nodeConfig)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		// Parse output
		var result map[string]interface{}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		// Both endpoints must be present in output
		if _, hasTrue := result["true"]; !hasTrue {
			t.Error("Expected 'true' field to be present in output")
		}

		if _, hasFalse := result["false"]; !hasFalse {
			t.Error("Expected 'false' field to be present in output")
		}
	})

	t.Run("Multiple conditions AND - all must pass for true event", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "AND",
			"conditions": []map[string]interface{}{
				{
					"name":           "check_age",
					"field_path":     "age",
					"operator":       "greater_than",
					"expected_value": 18,
				},
				{
					"name":           "check_email",
					"field_path":     "email",
					"operator":       "is_not_empty",
				},
			},
		}

		configBytes, _ := json.Marshal(config)

		// Both conditions pass
		input1 := `{"age": 25, "email": "test@example.com"}`
		nodeConfig1 := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input1),
		}

		output1, err := executor.Execute(context.Background(), nodeConfig1)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		var result1 map[string]interface{}
		json.Unmarshal(output1, &result1)

		if result1["true"] != true {
			t.Error("Expected true event to fire when all AND conditions met")
		}

		// One condition fails
		input2 := `{"age": 25, "email": ""}`
		nodeConfig2 := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input2),
		}

		output2, err := executor.Execute(context.Background(), nodeConfig2)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		var result2 map[string]interface{}
		json.Unmarshal(output2, &result2)

		if result2["false"] != true {
			t.Error("Expected false event to fire when AND condition not fully met")
		}
	})

	t.Run("Multiple conditions OR - any can pass for true event", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "OR",
			"conditions": []map[string]interface{}{
				{
					"name":           "check_status_active",
					"field_path":     "status",
					"operator":       "equals",
					"expected_value": "active",
				},
				{
					"name":           "check_status_pending",
					"field_path":     "status",
					"operator":       "equals",
					"expected_value": "pending",
				},
			},
		}

		configBytes, _ := json.Marshal(config)

		// First condition passes
		input1 := `{"status": "active"}`
		nodeConfig1 := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input1),
		}

		output1, err := executor.Execute(context.Background(), nodeConfig1)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		var result1 map[string]interface{}
		json.Unmarshal(output1, &result1)

		if result1["true"] != true {
			t.Error("Expected true event to fire when OR condition met")
		}

		// No conditions pass
		input2 := `{"status": "inactive"}`
		nodeConfig2 := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input2),
		}

		output2, err := executor.Execute(context.Background(), nodeConfig2)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		var result2 map[string]interface{}
		json.Unmarshal(output2, &result2)

		if result2["false"] != true {
			t.Error("Expected false event to fire when no OR conditions met")
		}
	})

	t.Run("Event endpoints are boolean type", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "AND",
			"conditions": []map[string]interface{}{
				{
					"name":         "always_true",
					"field_path":   "value",
					"operator":     "is_not_empty",
				},
			},
		}

		configBytes, _ := json.Marshal(config)
		input := `{"value": "test"}`

		nodeConfig := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input),
		}

		output, err := executor.Execute(context.Background(), nodeConfig)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal(output, &result)

		// Check true is boolean true (not string, not number)
		if trueVal, ok := result["true"].(bool); !ok || !trueVal {
			t.Errorf("Expected 'true' to be boolean true, got: %T %v", result["true"], result["true"])
		}

		// Check false is nil (not boolean false)
		if result["false"] != nil {
			t.Errorf("Expected 'false' to be nil, got: %T %v", result["false"], result["false"])
		}
	})
}

func TestSimpleCondition_EventOutputStructure(t *testing.T) {
	executor := simplecondition.NewExecutor()

	t.Run("Output contains all required fields", func(t *testing.T) {
		config := map[string]interface{}{
			"logic_operator": "AND",
			"conditions": []map[string]interface{}{
				{
					"name":           "test",
					"field_path":     "value",
					"operator":       "equals",
					"expected_value": "test",
				},
			},
		}

		configBytes, _ := json.Marshal(config)
		input := `{"value": "test"}`

		nodeConfig := embedded.NodeConfig{
			NodeID:        "test-condition",
			PluginType:    "plugin-simple-condition",
			Configuration: configBytes,
			Input:         []byte(input),
		}

		output, err := executor.Execute(context.Background(), nodeConfig)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		// Check all required fields are present
		requiredFields := []string{"result", "conditions", "summary", "true", "false"}
		for _, field := range requiredFields {
			if _, exists := result[field]; !exists {
				t.Errorf("Expected output to contain field '%s'", field)
			}
		}
	})
}

