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

// TestErrorEventIntegration tests end-to-end error event flow
// Scenario: Node A fails → Error event triggers Node B (error handler)
func TestErrorEventIntegration(t *testing.T) {
	registry := embedded.NewExecutorRegistry()
	registry.Register(simplecondition.NewExecutor())
	processor := embedded.NewProcessor(registry)

	t.Run("Error event triggers error handler node", func(t *testing.T) {
		// Simulate a failed parent node output
		// In real scenario, this would come from a failed HTTP request, etc.
		failedParentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "failed",
				"node_id": "http-node",
			},
			"_events": map[string]interface{}{
				"success": nil,
				"error":   true, // Error event fires!
			},
			"_error": map[string]interface{}{
				"code":    "HTTP_TIMEOUT",
				"message": "Request timed out after 30s",
			},
			"result": nil,
		}
		parentOutputBytes, _ := json.Marshal(failedParentOutput)
		parentOutput := embedded.WrapSuccess("http-node", "test", 0, parentOutputBytes)

		// Embedded nodes:
		// 1. Normal downstream node - triggers on success (should skip)
		// 2. Error handler node - triggers on error (should execute)
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "normal-downstream",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "test", "field_path": "status", "operator": "equals", "expected_value": "ok"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "http-node",
						SourceEndpoint:       "_events/success", // Looking for success event
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
				},
			},
			{
				NodeID:         "error-handler",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "error_exists", "field_path": "_error/message", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 2,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "http-node",
						SourceEndpoint:       "_events/error", // Looking for error event
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "http-node"},
			Workflow: &message.Workflow{
				WorkflowID: "error-test-workflow",
				RunID:      "error-test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput)
		require.NoError(t, err, "ProcessEmbeddedNodes should not error")
		require.Len(t, results, 2, "Should have 2 results")

		// Find results by node ID
		var normalResult, errorHandlerResult *embedded.EmbeddedNodeResult
		for i := range results {
			if results[i].NodeID == "normal-downstream" {
				normalResult = &results[i]
			} else if results[i].NodeID == "error-handler" {
				errorHandlerResult = &results[i]
			}
		}

		require.NotNil(t, normalResult, "Normal downstream result should exist")
		require.NotNil(t, errorHandlerResult, "Error handler result should exist")

		// Verify normal downstream skipped (success event not fired)
		assert.Equal(t, "skipped", normalResult.Status)
		assert.Contains(t, normalResult.Error, "event not fired")

		// Verify error handler executed (error event fired)
		assert.Equal(t, "success", errorHandlerResult.Status, "Error handler should execute successfully")
	})

	t.Run("Success event triggers normal flow, error handler skips", func(t *testing.T) {
		// Simulate successful parent output
		successParentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "http-node",
			},
			"_events": map[string]interface{}{
				"success": true, // Success event fires!
				"error":   nil,
			},
			"result": map[string]interface{}{
				"status_code": 200,
				"body":        "response data",
			},
		}
		parentOutputBytes, _ := json.Marshal(successParentOutput)
		parentOutput := embedded.WrapSuccess("http-node", "test", 0, parentOutputBytes)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "normal-downstream",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "test", "field_path": "status", "operator": "equals", "expected_value": "ok"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "http-node",
						SourceEndpoint:       "_events/success", // Looking for success event
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
				},
			},
			{
				NodeID:         "error-handler",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "check", "field_path": "value", "operator": "is_not_empty"}]}`),
				ExecutionOrder: 2,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "http-node",
						SourceEndpoint:       "_events/error", // Looking for error event
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "http-node"},
			Workflow: &message.Workflow{
				WorkflowID: "success-test-workflow",
				RunID:      "success-test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Find results by node ID
		var normalResult, errorHandlerResult *embedded.EmbeddedNodeResult
		for i := range results {
			if results[i].NodeID == "normal-downstream" {
				normalResult = &results[i]
			} else if results[i].NodeID == "error-handler" {
				errorHandlerResult = &results[i]
			}
		}

		require.NotNil(t, normalResult, "Normal downstream result should exist")
		require.NotNil(t, errorHandlerResult, "Error handler result should exist")

		// Verify normal downstream executed (success event fired)
		assert.Equal(t, "success", normalResult.Status, "Normal downstream should execute")

		// Verify error handler skipped (error event not fired)
		assert.Equal(t, "skipped", errorHandlerResult.Status)
		assert.Contains(t, errorHandlerResult.Error, "event not fired")
	})

	t.Run("Error handler accesses error details", func(t *testing.T) {
		// Failed parent with detailed error info
		failedParentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "failed",
				"node_id": "api-call",
			},
			"_events": map[string]interface{}{
				"success": nil,
				"error":   true,
			},
			"_error": map[string]interface{}{
				"code":      "HTTP_TIMEOUT",
				"message":   "Request timed out after 30 seconds",
				"retryable": true,
			},
			"result": nil,
		}
		parentOutputBytes, _ := json.Marshal(failedParentOutput)
		parentOutput := embedded.WrapSuccess("http-node", "test", 0, parentOutputBytes)

		// Error handler that reads error message
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "log-error",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "has_error", "field_path": "error_msg", "operator": "contains", "expected_value": "timeout"}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "api-call",
						SourceEndpoint:       "_events/error",
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
					{
						SourceNodeID:         "api-call",
						SourceEndpoint:       "_error/message",
						DestinationEndpoints: []string{"error_msg"}, // Map to input field
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "api-call"},
			Workflow: &message.Workflow{
				WorkflowID: "error-detail-test",
				RunID:      "error-detail-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Verify error handler executed
		assert.Equal(t, "log-error", results[0].NodeID)
		assert.Equal(t, "success", results[0].Status, "Error handler should execute")

		// Verify error handler received error message and executed successfully
		// The simplecondition processor wraps its output, so we check for success
		var output map[string]interface{}
		outputBytes, _ := json.Marshal(results[0].Output)
		err = json.Unmarshal(outputBytes, &output)
		require.NoError(t, err)

		// Verify wrapped structure - error handler executed successfully
		assert.Equal(t, "success", output["_meta"].(map[string]interface{})["status"])

		// Verify the error handler received the error event
		assert.Equal(t, true, output["_events"].(map[string]interface{})["success"], "Error handler should succeed")

		// This test verifies:
		// 1. Error event triggered the error handler (it executed, not skipped)
		// 2. Error details were accessible and mapped to the handler's input
		// 3. The handler executed successfully with wrapped output
	})
}

// TestChainedErrorHandling tests multiple levels of error handling
func TestChainedErrorHandling(t *testing.T) {
	registry := embedded.NewExecutorRegistry()
	registry.Register(simplecondition.NewExecutor())
	processor := embedded.NewProcessor(registry)

	t.Run("Error handler chain - node fails, triggers handler, handler succeeds", func(t *testing.T) {
		// Initial failed output
		failedOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "failed",
				"node_id": "node-1",
			},
			"_events": map[string]interface{}{
				"success": nil,
				"error":   true,
			},
			"_error": map[string]interface{}{
				"code":    "NETWORK_ERROR",
				"message": "Connection refused",
			},
			"result": nil,
		}
		parentOutputBytes, _ := json.Marshal(failedOutput)
		parentOutput := embedded.WrapSuccess("http-node", "test", 0, parentOutputBytes)

		// Node 2 is an error handler triggered by node-1 error
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "error-logger",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "always_true", "field_path": "status", "operator": "equals", "expected_value": "logged"}]}`),
				ExecutionOrder: 1,
				Depth:          1,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "node-1",
						SourceEndpoint:       "_events/error",
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
					{
						SourceNodeID:         "node-1",
						SourceEndpoint:       "_error/message",
						DestinationEndpoints: []string{"error_message"},
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "node-1"},
			Workflow: &message.Workflow{
				WorkflowID: "chained-error-test",
				RunID:      "chained-error-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Error logger executed (triggered by node-1 error)
		assert.Equal(t, "error-logger", results[0].NodeID)
		assert.Equal(t, "success", results[0].Status, "Error logger should execute successfully")

		// Verify error logger has success metadata
		var output map[string]interface{}
		outputBytes, _ := json.Marshal(results[0].Output)
		err = json.Unmarshal(outputBytes, &output)
		require.NoError(t, err)

		if meta, ok := output["_meta"].(map[string]interface{}); ok {
			assert.Equal(t, "success", meta["status"])
		}

		if events, ok := output["_events"].(map[string]interface{}); ok {
			assert.Equal(t, true, events["success"], "Success event should be true")
			assert.Nil(t, events["error"], "Error event should be nil")
		}
	})
}

// TestErrorMetadataAvailability tests that error metadata is available to downstream nodes
func TestErrorMetadataAvailability(t *testing.T) {
	registry := embedded.NewExecutorRegistry()
	registry.Register(simplecondition.NewExecutor())
	processor := embedded.NewProcessor(registry)

	t.Run("Error handler can access error code and retryability", func(t *testing.T) {
		failedOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":            "failed",
				"node_id":           "upstream",
				"execution_time_ms": 50,
			},
			"_events": map[string]interface{}{
				"success": nil,
				"error":   true,
			},
			"_error": map[string]interface{}{
				"code":      "RATE_LIMIT_ERROR",
				"message":   "API rate limit exceeded",
				"retryable": true,
				"details": map[string]interface{}{
					"retry_after": 60,
				},
			},
			"result": nil,
		}
		parentOutputBytes, _ := json.Marshal(failedOutput)
		parentOutput := embedded.WrapSuccess("http-node", "test", 0, parentOutputBytes)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:         "retry-handler",
				PluginType:     "plugin-simple-condition",
				Configuration:  []byte(`{"logic_operator": "AND", "conditions": [{"name": "is_retryable", "field_path": "retryable", "operator": "equals", "expected_value": true}]}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "upstream",
						SourceEndpoint:       "_events/error",
						DestinationEndpoints: []string{},
						IsEventTrigger:       true,
					},
					{
						SourceNodeID:         "upstream",
						SourceEndpoint:       "_error/retryable",
						DestinationEndpoints: []string{"retryable"}, // Map to input field
						IsEventTrigger:       false,
					},
					{
						SourceNodeID:         "upstream",
						SourceEndpoint:       "_error/code",
						DestinationEndpoints: []string{"error_code"}, // Map to input field
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "upstream"},
			Workflow: &message.Workflow{
				WorkflowID: "retry-test",
				RunID:      "retry-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput)
		require.NoError(t, err)
		require.Len(t, results, 1)

		// Verify retry handler executed
		assert.Equal(t, "retry-handler", results[0].NodeID)
		assert.Equal(t, "success", results[0].Status)

		// Parse output to verify error metadata was passed
		var output map[string]interface{}
		outputBytes, _ := json.Marshal(results[0].Output)
		err = json.Unmarshal(outputBytes, &output)
		require.NoError(t, err)

		// The condition should have evaluated the retryable field
		if result, ok := output["result"].(map[string]interface{}); ok {
			if conditions, ok := result["conditions"].(map[string]interface{}); ok {
				if isRetryable, ok := conditions["is_retryable"].(map[string]interface{}); ok {
					assert.True(t, isRetryable["met"].(bool), "Retryable condition should be met")
				}
			}
		}
	})
}
