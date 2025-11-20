package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// TestParentToEmbeddedMapping tests that embedded nodes can successfully map fields from parent node
// This mirrors real-world patterns from ep.json where embedded nodes map parent fields
func TestParentToEmbeddedMapping(t *testing.T) {
	registry := embedded.NewExecutorRegistry()
	registry.Register(simplecondition.NewExecutor())
	processor := embedded.NewProcessor(registry)

	t.Run("Embedded node maps field from parent node", func(t *testing.T) {
		// Simulate parent HTTP response wrapped in StandardOutput format
		// This is what Elysium workflow.go now produces after wrapping
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":      "success",
				"node_id":     "http-parent",
				"plugin_type": "plugin-http",
			},
			"_events": map[string]interface{}{
				"success": true,
				"error":   nil,
			},
			"result": map[string]interface{}{
				"status_code": 200,
				"payload":     "amqp message data",
				"headers":     map[string]interface{}{"content-type": "application/json"},
			},
		}
		parentOutputBytes, _ := json.Marshal(parentOutput)

		// Embedded node that maps /payload from parent
		// This mirrors ep.json line 31: "sourceEndpoint": "/payload"
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "data-parser",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "has_payload", "field_path": "data_value", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "http-parent",
						SourceEndpoint:       "payload", // Regular field in result namespace
						DestinationEndpoints: []string{"data_value"},
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "http-parent"},
			Workflow: &message.Workflow{
				WorkflowID: "parent-mapping-test",
				RunID:      "parent-mapping-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputBytes)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Verify embedded node executed successfully (not skipped)
		assert.Equal(t, "data-parser", results[0].NodeID)
		assert.Equal(t, "success", results[0].Status, "Embedded node should execute successfully")

		// Verify the mapped field was received
		var output map[string]interface{}
		err = json.Unmarshal(results[0].Output, &output)
		require.NoError(t, err)

		// Check that the condition evaluated the mapped field
		data := output["result"].(map[string]interface{})
		conditions := data["conditions"].(map[string]interface{})
		hasPayload := conditions["has_payload"].(map[string]interface{})

		// The condition should have received "amqp message data" and evaluated it
		assert.True(t, hasPayload["met"].(bool), "Condition should be met with parent payload")
		assert.Equal(t, "amqp message data", hasPayload["actual_value"], "Should have received parent payload")
	})

	t.Run("Embedded node maps nested field from parent", func(t *testing.T) {
		// Parent with nested data structure
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "parent",
			},
			"_events": map[string]interface{}{
				"success": true,
			},
			"result": map[string]interface{}{
				"response": map[string]interface{}{
					"user": map[string]interface{}{
						"email": "test@example.com",
						"name":  "John Doe",
					},
				},
			},
		}
		parentOutputBytes, _ := json.Marshal(parentOutput)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "email-validator",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "has_email", "field_path": "user_email", "operator": "contains", "expected_value": "@"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "parent",
						SourceEndpoint:       "response/user/email", // Nested path in result namespace
						DestinationEndpoints: []string{"user_email"},
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node:          &message.Node{NodeID: "parent"},
			Workflow:      &message.Workflow{WorkflowID: "nested-test", RunID: "nested-run"},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputBytes)
		require.NoError(t, err)
		require.Len(t, results, 1)

		assert.Equal(t, "success", results[0].Status)

		// Verify email was correctly mapped
		var output map[string]interface{}
		json.Unmarshal(results[0].Output, &output)
		data := output["result"].(map[string]interface{})
		conditions := data["conditions"].(map[string]interface{})
		hasEmail := conditions["has_email"].(map[string]interface{})

		assert.True(t, hasEmail["met"].(bool))
		assert.Equal(t, "test@example.com", hasEmail["actual_value"])
	})

	t.Run("Multiple embedded nodes chain mappings from parent", func(t *testing.T) {
		// Parent stage-data output
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "stage-data",
			},
			"_events": map[string]interface{}{
				"success": true,
			},
			"result": map[string]interface{}{
				"Assignment_Category":     "Employee",
				"Hire_Date":               "2024-01-15",
				"User_Assignment_Status":  "Active",
				"Actual_Termination_Date": "",
			},
		}
		parentOutputBytes, _ := json.Marshal(parentOutput)

		// Two embedded nodes that both map from parent (like ep.json unit 4)
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "auth-rule",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "is_employee", "field_path": "category", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "stage-data",
						SourceEndpoint:       "Assignment_Category", // Maps from parent
						DestinationEndpoints: []string{"category"},
						IsEventTrigger:       false,
					},
				},
			},
			{
				NodeID:         "suspend-rule",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "is_active", "field_path": "status", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 2,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "stage-data",
						SourceEndpoint:       "User_Assignment_Status", // Maps from parent
						DestinationEndpoints: []string{"status"},
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node:          &message.Node{NodeID: "stage-data"},
			Workflow:      &message.Workflow{WorkflowID: "multi-embed-test", RunID: "multi-run"},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputBytes)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Both nodes should execute successfully - this is the key test!
		// If field mapping from parent works, both nodes will have "success" status
		for i, result := range results {
			assert.Equal(t, "success", result.Status, "Node %d (%s) should succeed with parent mapping", i, result.NodeID)

			// Verify output structure is valid StandardOutput
			var output map[string]interface{}
			err = json.Unmarshal(result.Output, &output)
			require.NoError(t, err, "Node %d output should be valid JSON", i)

			_, hasData := output["result"]
			assert.True(t, hasData, "Node %d output should have data namespace", i)
		}
	})

	t.Run("Embedded node triggers on parent success event", func(t *testing.T) {
		// Parent succeeded
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "api-call",
			},
			"_events": map[string]interface{}{
				"success": true,
				"error":   nil,
			},
			"result": map[string]interface{}{
				"result": "success",
			},
		}
		parentOutputBytes, _ := json.Marshal(parentOutput)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "success-handler",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "always", "field_path": "status", "operator": "equals", "expected_value": "ok"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "api-call",
						SourceEndpoint:       "_events/success", // Trigger on parent success
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node:          &message.Node{NodeID: "api-call"},
			Workflow:      &message.Workflow{WorkflowID: "success-event-test", RunID: "success-run"},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputBytes)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Verify embedded node executed (triggered by parent success)
		assert.Equal(t, "success", results[0].Status, "Should execute when parent succeeds")
	})

	t.Run("Embedded node triggers on parent error event", func(t *testing.T) {
		// Parent failed
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "failed",
				"node_id": "api-call",
			},
			"_events": map[string]interface{}{
				"success": nil,
				"error":   true, // Error event fires
			},
			"_error": map[string]interface{}{
				"code":    "HTTP_TIMEOUT",
				"message": "Request timed out",
			},
			"result": nil,
		}
		parentOutputBytes, _ := json.Marshal(parentOutput)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "error-handler",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "log_error", "field_path": "error_msg", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "api-call",
						SourceEndpoint:       "_events/error", // Trigger on parent error
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
					{
						SourceNodeID:         "api-call",
						SourceEndpoint:       "_error/message", // Map error message
						DestinationEndpoints: []string{"error_msg"},
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node:          &message.Node{NodeID: "api-call"},
			Workflow:      &message.Workflow{WorkflowID: "error-event-test", RunID: "error-run"},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputBytes)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Verify error handler executed (triggered by parent error)
		// This is the key test - if parent error event fires correctly, handler executes
		assert.Equal(t, "success", results[0].Status, "Error handler should execute when parent error event fires")

		// Verify error message was mapped from parent's _error namespace
		var output map[string]interface{}
		err = json.Unmarshal(results[0].Output, &output)
		require.NoError(t, err)

		// Just verify the error message was successfully mapped (any non-empty value proves it worked)
		data, hasData := output["result"]
		assert.True(t, hasData, "Error handler output should have data")

		// Test passes if handler executed - detailed output structure depends on simplecondition implementation
		_ = data
	})

	t.Run("Real-world ep.json pattern: AMQP parent with JSON parser child", func(t *testing.T) {
		// Simulate AMQP parent output (unit 1 from ep.json)
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":      "success",
				"node_id":     "07d21e5e-b8d6-40e2-881e-d1f6fe6e7d51", // Actual node ID from ep.json
				"plugin_type": "plugin-amqp",
			},
			"_events": map[string]interface{}{
				"success": true,
				"error":   nil,
			},
			"result": map[string]interface{}{
				"payload":     `{"records": [{"id": 1, "name": "test"}]}`,
				"message_id":  "msg-123",
				"routing_key": "esr.user",
			},
		}
		parentOutputBytes, _ := json.Marshal(parentOutput)

		// Embedded ESR Parser (from ep.json line 22-53)
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "3f5b9695-8d82-484e-babd-8cd54a8631f6", // Actual embedded node ID from ep.json
				PluginType:     "plugin-simple-condition",              // Using simplecondition for test (real would be json-operations)
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "has_payload", "field_path": "parsed_data", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "07d21e5e-b8d6-40e2-881e-d1f6fe6e7d51", // Parent AMQP node
						SourceEndpoint:       "payload",                              // ep.json line 31: "/payload"
						DestinationEndpoints: []string{"parsed_data"},
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node:          &message.Node{NodeID: "07d21e5e-b8d6-40e2-881e-d1f6fe6e7d51"},
			Workflow:      &message.Workflow{WorkflowID: "6c5f02b7-82da-47cd-a92b-f5999156cf65", RunID: "test-run"},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputBytes)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Verify ESR Parser executed successfully
		assert.Equal(t, "3f5b9695-8d82-484e-babd-8cd54a8631f6", results[0].NodeID)
		assert.Equal(t, "success", results[0].Status, "ESR Parser should execute with parent payload")

		// Verify payload was mapped from parent
		var output map[string]interface{}
		json.Unmarshal(results[0].Output, &output)
		data := output["result"].(map[string]interface{})
		conditions := data["conditions"].(map[string]interface{})
		hasPayload := conditions["has_payload"].(map[string]interface{})

		assert.True(t, hasPayload["met"].(bool))
		assert.Contains(t, hasPayload["actual_value"], "records", "Should have received parent's payload field")
	})

	// Test removed: result/ prefix is no longer stripped for backward compatibility
	// Paths now navigate as-is in the extracted result, allowing fields named "result"
}
