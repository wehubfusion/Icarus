package embedded

import (
	"encoding/json"
	"testing"
)

func TestSmartStorage_BasicOperations(t *testing.T) {
	storage := NewSmartStorage(nil)

	// Test Set and Get
	testResult := map[string]interface{}{
		"status": "success",
		"data":   "test data",
	}

	err := storage.Set("node1", testResult, nil)
	if err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	entry, err := storage.Get("node1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	var retrievedResult map[string]interface{}
	if err := json.Unmarshal(entry.Result, &retrievedResult); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if retrievedResult["status"] != "success" {
		t.Errorf("Expected status=success, got %v", retrievedResult["status"])
	}
}

func TestSmartStorage_WithIterationContext(t *testing.T) {
	storage := NewSmartStorage(nil)

	testResult := map[string]interface{}{
		"result": []interface{}{"item1", "item2", "item3"},
	}

	iterationContext := &IterationContext{
		IsArray:     true,
		ArrayPath:   "/result",
		ArrayLength: 3,
	}

	err := storage.Set("array-node", testResult, iterationContext)
	if err != nil {
		t.Fatalf("Failed to set with iteration context: %v", err)
	}

	entry, err := storage.Get("array-node")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if entry.IterationContext == nil {
		t.Fatal("Expected iteration context to be set")
	}

	if entry.IterationContext.ArrayLength != 3 {
		t.Errorf("Expected array length 3, got %d", entry.IterationContext.ArrayLength)
	}
}

func TestSmartStorage_ConsumerTracking(t *testing.T) {
	// Build consumer graph
	consumers := map[string][]string{
		"node1": {"node2", "node3"},
		"node2": {"node4"},
	}

	storage := NewSmartStorageWithConsumers(consumers, nil)

	// Set node1 result
	testResult := map[string]interface{}{"data": "test"}
	err := storage.Set("node1", testResult, nil)
	if err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	// Verify node1 exists
	if !storage.Has("node1") {
		t.Fatal("Expected node1 to exist")
	}

	// Mark consumed by node2
	storage.MarkConsumed("node1", "node2")

	// node1 should still exist (node3 hasn't consumed yet)
	if !storage.Has("node1") {
		t.Fatal("Expected node1 to still exist after partial consumption")
	}

	// Mark consumed by node3
	storage.MarkConsumed("node1", "node3")

	// node1 should be auto-cleaned now (all consumers satisfied)
	if storage.Has("node1") {
		t.Fatal("Expected node1 to be auto-cleaned after all consumers satisfied")
	}
}

func TestSmartStorage_Serialization(t *testing.T) {
	storage := NewSmartStorage(nil)

	testResult := map[string]interface{}{
		"status": "success",
		"value":  42,
	}

	err := storage.Set("node1", testResult, nil)
	if err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	// Serialize
	serialized, err := json.Marshal(storage)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Deserialize into new storage
	newStorage := NewSmartStorage(nil)
	if err := json.Unmarshal(serialized, newStorage); err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	// Verify data is preserved
	entry, err := newStorage.Get("node1")
	if err != nil {
		t.Fatalf("Failed to get from deserialized storage: %v", err)
	}

	var retrievedResult map[string]interface{}
	if err := json.Unmarshal(entry.Result, &retrievedResult); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if retrievedResult["status"] != "success" {
		t.Errorf("Expected status=success, got %v", retrievedResult["status"])
	}

	// Verify value is correct type (JSON numbers are float64)
	if val, ok := retrievedResult["value"].(float64); !ok || val != 42 {
		t.Errorf("Expected value=42, got %v", retrievedResult["value"])
	}
}

func TestSmartStorage_GetStats(t *testing.T) {
	consumers := map[string][]string{
		"node1": {"node2"},
		"node2": {"node3"},
	}

	storage := NewSmartStorageWithConsumers(consumers, nil)

	// Add some entries
	storage.Set("node1", map[string]interface{}{"data": "test1"}, nil)
	storage.Set("node2", map[string]interface{}{"data": "test2"}, &IterationContext{
		IsArray:     true,
		ArrayLength: 5,
	})

	stats := storage.GetStats()

	if stats["total_entries"] != 2 {
		t.Errorf("Expected 2 entries, got %v", stats["total_entries"])
	}

	if stats["entries_with_iteration_context"] != 1 {
		t.Errorf("Expected 1 entry with iteration context, got %v", stats["entries_with_iteration_context"])
	}

	if stats["consumer_graph_size"] != 2 {
		t.Errorf("Expected consumer graph size 2, got %v", stats["consumer_graph_size"])
	}
}

func TestSmartStorage_ExtractResult(t *testing.T) {
	storage := NewSmartStorage(nil)

	testOutput := &StandardOutput{
		Meta: MetaData{
			Status: "success",
			NodeID: "test-node",
		},
		Events: EventEndpoints{
			Success: true,
		},
		Result: map[string]interface{}{
			"message": "hello",
		},
	}

	// Store as StandardOutput
	err := storage.Set("node1", testOutput, nil)
	if err != nil {
		t.Fatalf("Failed to set: %v", err)
	}

	// Extract as StandardOutput
	retrievedOutput, err := storage.GetResultAsStandardOutput("node1")
	if err != nil {
		t.Fatalf("Failed to extract StandardOutput: %v", err)
	}

	if retrievedOutput.Meta.Status != "success" {
		t.Errorf("Expected status=success, got %s", retrievedOutput.Meta.Status)
	}

	if retrievedOutput.Meta.NodeID != "test-node" {
		t.Errorf("Expected node_id=test-node, got %s", retrievedOutput.Meta.NodeID)
	}
}
