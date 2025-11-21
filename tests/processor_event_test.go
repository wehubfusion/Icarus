package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func TestProcessor_EventTriggerLogic(t *testing.T) {
	registry := embedded.NewExecutorRegistry()
	registry.Register(simplecondition.NewExecutor())
	processor := embedded.NewProcessor(registry)

	t.Run("Embedded node skips when event not fired (null)", func(t *testing.T) {
		// Parent output result data (without StandardOutput wrapper)
		parentResult := map[string]interface{}{
			"result": false,
			"true":   nil, // Event NOT fired (null)
			"false":  true,
		}
		parentResultBytes, _ := json.Marshal(parentResult)

		// Wrap in StandardOutput format
		parentOutput := embedded.WrapSuccess("parent-node", "test", 0, parentResultBytes)

		// Embedded node with event trigger on /true
		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:     "conditional-node",
				PluginType: "plugin-simple-condition",
				Configuration: []byte(`{
					"logic_operator": "AND",
					"conditions": [
						{
							"name": "test",
							"field_path": "value",
							"operator": "is_not_empty"
						}
					]
				}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "parent-node",
						SourceEndpoint:       "true", // Path to event (no data/ prefix needed)
						DestinationEndpoints: []string{"conditional-node"},
						DataType:             "EVENT",
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "parent-node"},
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
		EmbeddedNodes: embeddedNodes,
	}

	results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput, map[string][]string{})
	if err != nil {
		t.Fatalf("ProcessEmbeddedNodes failed: %v", err)
	}		// Node should be skipped
		if results[0].Status != "skipped" {
			t.Errorf("Expected node to be skipped when event is null, got status: %s", results[0].Status)
		}
	})

	t.Run("Embedded node skips when event is false", func(t *testing.T) {
		// Parent output with event false (wrapped in StandardOutput format)
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "parent-node",
			},
			"_events": map[string]interface{}{
				"success": true,
				"error":   nil,
			},
			"result": map[string]interface{}{
				"trigger": false, // Boolean false
			},
		}
		parentResultBytes, _ := json.Marshal(parentOutput)
		parentOutputWrapped := embedded.WrapSuccess("parent-node", "test", 0, parentResultBytes)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:     "conditional-node",
				PluginType: "plugin-simple-condition",
				Configuration: []byte(`{
					"logic_operator": "AND",
					"conditions": [
						{
							"name": "test",
							"field_path": "value",
							"operator": "is_not_empty"
						}
					]
				}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "parent-node",
						SourceEndpoint:       "/trigger",
						DestinationEndpoints: []string{"conditional-node"},
						DataType:             "EVENT",
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "parent-node"},
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputWrapped, map[string][]string{})
		if err != nil {
			t.Fatalf("ProcessEmbeddedNodes failed: %v", err)
		}

		if results[0].Status != "skipped" {
			t.Errorf("Expected node to be skipped when trigger is false, got status: %s", results[0].Status)
		}
	})

	t.Run("Embedded node skips when event is empty string", func(t *testing.T) {
		// Parent output with empty string (wrapped in StandardOutput format)
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "parent-node",
			},
			"_events": map[string]interface{}{
				"success": true,
				"error":   nil,
			},
			"result": map[string]interface{}{
				"trigger": "", // Empty string
			},
		}
		parentResultBytes, _ := json.Marshal(parentOutput)
		parentOutputWrapped := embedded.WrapSuccess("parent-node", "test", 0, parentResultBytes)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:     "conditional-node",
				PluginType: "plugin-simple-condition",
				Configuration: []byte(`{
					"logic_operator": "AND",
					"conditions": [
						{
							"name": "test",
							"field_path": "value",
							"operator": "is_not_empty"
						}
					]
				}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "parent-node",
						SourceEndpoint:       "trigger", // Path to event (no data/ prefix needed)
						DestinationEndpoints: []string{"conditional-node"},
						DataType:             "EVENT",
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "parent-node"},
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputWrapped, map[string][]string{})
		if err != nil {
			t.Fatalf("ProcessEmbeddedNodes failed: %v", err)
		}

		if results[0].Status != "skipped" {
			t.Errorf("Expected node to be skipped when trigger is empty string, got status: %s", results[0].Status)
		}
	})

	t.Run("Embedded node skips when endpoint doesn't exist", func(t *testing.T) {
		// Parent output without trigger endpoint (wrapped in StandardOutput format)
		parentOutput := map[string]interface{}{
			"_meta": map[string]interface{}{
				"status":  "success",
				"node_id": "parent-node",
			},
			"_events": map[string]interface{}{
				"success": true,
				"error":   nil,
			},
			"result": map[string]interface{}{
				"other": "data",
			},
		}
		parentResultBytes, _ := json.Marshal(parentOutput)
		parentOutputWrapped := embedded.WrapSuccess("parent-node", "test", 0, parentResultBytes)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:     "conditional-node",
				PluginType: "plugin-simple-condition",
				Configuration: []byte(`{
					"logic_operator": "AND",
					"conditions": [
						{
							"name": "test",
							"field_path": "value",
							"operator": "is_not_empty"
						}
					]
				}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "parent-node",
						SourceEndpoint:       "missing", // Path to missing field (no data/ prefix needed)
						DestinationEndpoints: []string{"conditional-node"},
						DataType:             "EVENT",
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "parent-node"},
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputWrapped, map[string][]string{})
		if err != nil {
			t.Fatalf("ProcessEmbeddedNodes failed: %v", err)
		}

		if results[0].Status != "skipped" {
			t.Errorf("Expected node to be skipped when endpoint missing, got status: %s", results[0].Status)
		}
	})

	t.Run("Embedded node skips when source node not found", func(t *testing.T) {
		parentOutput := embedded.WrapSuccess("parent-node", "test", 0, []byte(`{}`))

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:     "conditional-node",
				PluginType: "plugin-simple-condition",
				Configuration: []byte(`{
					"logic_operator": "AND",
					"conditions": [
						{
							"name": "test",
							"field_path": "value",
							"operator": "is_not_empty"
						}
					]
				}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "non-existent-node", // Source doesn't exist
						SourceEndpoint:       "/trigger",
						DestinationEndpoints: []string{"conditional-node"},
						DataType:             "EVENT",
						IsEventTrigger:       true,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "parent-node"},
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutput, map[string][]string{})
		if err != nil {
			t.Fatalf("ProcessEmbeddedNodes failed: %v", err)
		}

		if results[0].Status != "skipped" {
			t.Errorf("Expected node to be skipped when source not found, got status: %s", results[0].Status)
		}
	})

	t.Run("Embedded node executes when event is truthy", func(t *testing.T) {
		// Parent output with truthy event
		parentResult := map[string]interface{}{
			"trigger": true, // Truthy value
			"value":   "test",
		}
		parentResultBytes, _ := json.Marshal(parentResult)
		parentOutputWrapped := embedded.WrapSuccess("parent-node", "test", 0, parentResultBytes)

		embeddedNodes := []message.EmbeddedNode{
			{
				NodeID:     "conditional-node",
				PluginType: "plugin-simple-condition",
				Configuration: []byte(`{
					"logic_operator": "AND",
					"conditions": [
						{
							"name": "test",
							"field_path": "value",
							"operator": "is_not_empty"
						}
					]
				}`),
				ExecutionOrder: 1,
				Depth:          0,
				FieldMappings: []message.FieldMapping{
					{
						SourceNodeID:         "parent-node",
						SourceEndpoint:       "trigger", // Path to truthy event (no data/ prefix needed)
						DestinationEndpoints: []string{"conditional-node"},
						DataType:             "EVENT",
						IsEventTrigger:       true,
					},
					{
						SourceNodeID:         "parent-node",
						SourceEndpoint:       "value", // Path to field (no data/ prefix needed)
						DestinationEndpoints: []string{"/value"},
						DataType:             "FIELD",
						IsEventTrigger:       false,
					},
				},
			},
		}

		msg := &message.Message{
			Node: &message.Node{NodeID: "parent-node"},
			Workflow: &message.Workflow{
				WorkflowID: "test-workflow",
				RunID:      "test-run",
			},
			EmbeddedNodes: embeddedNodes,
		}

		results, err := processor.ProcessEmbeddedNodes(context.Background(), msg, parentOutputWrapped, map[string][]string{})
		if err != nil {
			t.Fatalf("ProcessEmbeddedNodes failed: %v", err)
		}

		if results[0].Status != "success" {
			t.Errorf("Expected node to execute when trigger is truthy, got status: %s (error: %s)",
				results[0].Status, results[0].Error)
		}
	})
}
