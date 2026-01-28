package runtime

import (
	"context"
	"testing"
)

// TestSingleLevelIteration tests basic single-level array iteration
func TestSingleLevelIteration(t *testing.T) {
	// Initialize iteration stack
	iterStack := NewIterationStack()

	if iterStack.IsActive() {
		t.Error("New stack should not be active")
	}

	if iterStack.Depth() != 0 {
		t.Errorf("New stack depth should be 0, got %d", iterStack.Depth())
	}

	// Create first-level iteration context
	ctx1 := &NestedIterationContext{
		SourceNodeId:      "node1",
		InitiatedByNodeId: "node2",
		ArrayPath:         "data",
		TotalItems:        3,
		Items:             []interface{}{"item1", "item2", "item3"},
	}

	iterStack.Push(ctx1)

	if !iterStack.IsActive() {
		t.Error("Stack should be active after push")
	}

	if iterStack.Depth() != 1 {
		t.Errorf("Stack depth should be 1, got %d", iterStack.Depth())
	}

	if ctx1.Depth != 0 {
		t.Errorf("First context depth should be 0, got %d", ctx1.Depth)
	}

	current := iterStack.Current()
	if current == nil {
		t.Fatal("Current context should not be nil")
	}

	if current.SourceNodeId != "node1" {
		t.Errorf("Expected source node1, got %s", current.SourceNodeId)
	}

	iterStack.Pop()

	if iterStack.IsActive() {
		t.Error("Stack should not be active after pop")
	}
}

// TestTwoLevelNesting tests nested iteration (data → assignments)
func TestTwoLevelNesting(t *testing.T) {
	iterStack := NewIterationStack()

	// First level: data
	ctx1 := &NestedIterationContext{
		SourceNodeId:      "node1",
		InitiatedByNodeId: "node2",
		ArrayPath:         "data",
		TotalItems:        2,
		Items:             []interface{}{map[string]interface{}{"id": 1}, map[string]interface{}{"id": 2}},
		CurrentIndex:      0,
	}
	iterStack.Push(ctx1)

	if iterStack.Depth() != 1 {
		t.Errorf("Expected depth 1, got %d", iterStack.Depth())
	}

	// Second level: assignments
	ctx2 := &NestedIterationContext{
		SourceNodeId:      "node3",
		InitiatedByNodeId: "node4",
		ArrayPath:         "assignments",
		TotalItems:        3,
		Items:             []interface{}{"a1", "a2", "a3"},
		CurrentIndex:      0,
	}
	iterStack.Push(ctx2)

	if iterStack.Depth() != 2 {
		t.Errorf("Expected depth 2, got %d", iterStack.Depth())
	}

	if ctx2.Depth != 1 {
		t.Errorf("Second context depth should be 1, got %d", ctx2.Depth)
	}

	indices := iterStack.GetCurrentIndices()
	if len(indices) != 2 {
		t.Errorf("Expected 2 indices, got %d", len(indices))
	}
	if indices[0] != 0 || indices[1] != 0 {
		t.Errorf("Expected indices [0,0], got %v", indices)
	}

	// Update indices
	ctx1.CurrentIndex = 1
	ctx2.CurrentIndex = 2

	indices = iterStack.GetCurrentIndices()
	if indices[0] != 1 || indices[1] != 2 {
		t.Errorf("Expected indices [1,2], got %v", indices)
	}

	iterStack.Pop()
	iterStack.Pop()

	if iterStack.IsActive() {
		t.Error("Stack should be empty")
	}
}

// TestThreeLevelNesting tests three-level nesting (data → assignments → topics)
func TestThreeLevelNesting(t *testing.T) {
	iterStack := NewIterationStack()

	// Level 1: data
	ctx1 := &NestedIterationContext{
		SourceNodeId: "node1",
		ArrayPath:    "data",
		TotalItems:   2,
		Items:        []interface{}{map[string]interface{}{"id": 1}, map[string]interface{}{"id": 2}},
	}
	iterStack.Push(ctx1)

	// Level 2: assignments
	ctx2 := &NestedIterationContext{
		SourceNodeId: "node2",
		ArrayPath:    "assignments",
		TotalItems:   3,
		Items:        []interface{}{"a1", "a2", "a3"},
	}
	iterStack.Push(ctx2)

	// Level 3: topics
	ctx3 := &NestedIterationContext{
		SourceNodeId: "node3",
		ArrayPath:    "topics",
		TotalItems:   4,
		Items:        []interface{}{"t1", "t2", "t3", "t4"},
	}
	iterStack.Push(ctx3)

	if iterStack.Depth() != 3 {
		t.Errorf("Expected depth 3, got %d", iterStack.Depth())
	}

	// Check each context depth
	if ctx1.Depth != 0 {
		t.Errorf("ctx1 depth should be 0, got %d", ctx1.Depth)
	}
	if ctx2.Depth != 1 {
		t.Errorf("ctx2 depth should be 1, got %d", ctx2.Depth)
	}
	if ctx3.Depth != 2 {
		t.Errorf("ctx3 depth should be 2, got %d", ctx3.Depth)
	}

	// Test GetContextForDepth
	retrieved := iterStack.GetContextForDepth(0)
	if retrieved == nil || retrieved.ArrayPath != "data" {
		t.Error("Failed to retrieve context at depth 0")
	}

	retrieved = iterStack.GetContextForDepth(1)
	if retrieved == nil || retrieved.ArrayPath != "assignments" {
		t.Error("Failed to retrieve context at depth 1")
	}

	retrieved = iterStack.GetContextForDepth(2)
	if retrieved == nil || retrieved.ArrayPath != "topics" {
		t.Error("Failed to retrieve context at depth 2")
	}
}

// TestFourLevelNesting tests four-level nesting (data → assignments → topics → chapters)
func TestFourLevelNesting(t *testing.T) {
	iterStack := NewIterationStack()

	// Build 4 levels
	levels := []struct {
		nodeId string
		path   string
		items  int
	}{
		{"node1", "data", 2},
		{"node2", "assignments", 3},
		{"node3", "topics", 4},
		{"node4", "chapters", 5},
	}

	for _, level := range levels {
		ctx := &NestedIterationContext{
			SourceNodeId: level.nodeId,
			ArrayPath:    level.path,
			TotalItems:   level.items,
			Items:        make([]interface{}, level.items),
		}
		iterStack.Push(ctx)
	}

	if iterStack.Depth() != 4 {
		t.Errorf("Expected depth 4, got %d", iterStack.Depth())
	}

	// Verify all contexts
	for i, level := range levels {
		ctx := iterStack.GetContextForDepth(i)
		if ctx == nil {
			t.Fatalf("Context at depth %d should not be nil", i)
		}
		if ctx.ArrayPath != level.path {
			t.Errorf("Expected path %s at depth %d, got %s", level.path, i, ctx.ArrayPath)
		}
		if ctx.TotalItems != level.items {
			t.Errorf("Expected %d items at depth %d, got %d", level.items, i, ctx.TotalItems)
		}
		if ctx.Depth != i {
			t.Errorf("Expected depth %d, got %d", i, ctx.Depth)
		}
	}
}

// TestDuplicatePrevention tests that duplicate iteration contexts are prevented
func TestDuplicatePrevention(t *testing.T) {
	iterStack := NewIterationStack()

	ctx := &NestedIterationContext{
		SourceNodeId: "node1",
		ArrayPath:    "data",
		TotalItems:   3,
		Items:        []interface{}{"a", "b", "c"},
	}
	iterStack.Push(ctx)

	// Check if already iterating from same source and path
	if !iterStack.IsIteratingFrom("node1", "data") {
		t.Error("Should detect existing iteration from node1/data")
	}

	// Different path should return false
	if iterStack.IsIteratingFrom("node1", "other") {
		t.Error("Should not detect iteration for different path")
	}

	// Different node should return false
	if iterStack.IsIteratingFrom("node2", "data") {
		t.Error("Should not detect iteration for different node")
	}

	// Add second level with different source
	ctx2 := &NestedIterationContext{
		SourceNodeId: "node2",
		ArrayPath:    "items",
		TotalItems:   2,
		Items:        []interface{}{"x", "y"},
	}
	iterStack.Push(ctx2)

	// Both should be detected
	if !iterStack.IsIteratingFrom("node1", "data") {
		t.Error("Should still detect first iteration")
	}
	if !iterStack.IsIteratingFrom("node2", "items") {
		t.Error("Should detect second iteration")
	}
}

// TestOutputIsolation tests that array items have isolated outputs
func TestOutputIsolation(t *testing.T) {
	store := NewNodeOutputStore()

	// Set output for a node
	output1 := map[string]interface{}{
		"name": "John",
		"age":  30,
	}
	store.SetSingleOutput("node1", output1)

	// Retrieve without context
	retrieved := store.GetOutputAtContext("node1", nil)
	if retrieved == nil {
		t.Fatal("Should retrieve output without context")
	}
	if retrieved["name"] != "John" {
		t.Error("Output mismatch")
	}

	// Create iteration context
	iterStack := NewIterationStack()
	ctx := &NestedIterationContext{
		SourceNodeId: "node1",
		ArrayPath:    "data",
		CurrentIndex: 0,
		TotalItems:   2,
	}
	iterStack.Push(ctx)

	// Set context-specific output for index 0
	contextOutput := map[string]interface{}{
		"name": "Jane",
		"age":  25,
	}
	store.SetOutputAtContext("node2", contextOutput, iterStack)

	// Retrieve with context should get context-specific output
	retrieved = store.GetOutputAtContext("node2", iterStack)
	if retrieved == nil {
		t.Fatal("Should retrieve context-specific output")
	}
	if retrieved["name"] != "Jane" {
		t.Error("Context output mismatch")
	}

	// Change index and set different output
	ctx.CurrentIndex = 1
	contextOutput2 := map[string]interface{}{
		"name": "Bob",
		"age":  35,
	}
	store.SetOutputAtContext("node2", contextOutput2, iterStack)

	// Should get the output for index 1
	retrieved = store.GetOutputAtContext("node2", iterStack)
	if retrieved == nil {
		t.Fatal("Should retrieve output with different index")
	}
	if retrieved["name"] != "Bob" {
		t.Error("Should get output for index 1")
	}

	// Change back to index 0 and verify isolation
	ctx.CurrentIndex = 0
	retrieved = store.GetOutputAtContext("node2", iterStack)
	if retrieved == nil {
		t.Fatal("Should retrieve output for index 0")
	}
	if retrieved["name"] != "Jane" {
		t.Error("Should get original output for index 0 - outputs are isolated")
	}
}

// TestParseNestedArrayPath tests the array path parsing
func TestParseNestedArrayPath(t *testing.T) {
	tests := []struct {
		path     string
		expected []ArrayPathSegment
	}{
		{
			path: "/data//name",
			expected: []ArrayPathSegment{
				{Path: "data", IsArray: true},
				{Path: "name", IsArray: false},
			},
		},
		{
			path: "/data//assignments//name",
			expected: []ArrayPathSegment{
				{Path: "data", IsArray: true},
				{Path: "assignments", IsArray: true},
				{Path: "name", IsArray: false},
			},
		},
		{
			path: "/data//assignments//topics//chapters//title",
			expected: []ArrayPathSegment{
				{Path: "data", IsArray: true},
				{Path: "assignments", IsArray: true},
				{Path: "topics", IsArray: true},
				{Path: "chapters", IsArray: true},
				{Path: "title", IsArray: false},
			},
		},
		{
			path:     "",
			expected: nil,
		},
		{
			path: "/simple/path",
			expected: []ArrayPathSegment{
				{Path: "simple/path", IsArray: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := ParseNestedArrayPath(tt.path)

			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d segments, got %d", len(tt.expected), len(result))
			}

			for i, seg := range result {
				if seg.Path != tt.expected[i].Path {
					t.Errorf("Segment %d: expected path %s, got %s", i, tt.expected[i].Path, seg.Path)
				}
				if seg.IsArray != tt.expected[i].IsArray {
					t.Errorf("Segment %d: expected IsArray %v, got %v", i, tt.expected[i].IsArray, seg.IsArray)
				}
			}
		})
	}
}

// TestNestedIterationConfigDefaults tests that default config values are set
func TestNestedIterationConfigDefaults(t *testing.T) {
	config := NestedIterationConfig{}

	if config.MaxIterationDepth != 0 {
		t.Error("Uninitialized MaxIterationDepth should be 0")
	}
	if config.MaxActiveCombinations != 0 {
		t.Error("Uninitialized MaxActiveCombinations should be 0")
	}
}

// TestCurrentIterationItem tests storing and retrieving current iteration items
func TestCurrentIterationItem(t *testing.T) {
	store := NewNodeOutputStore()

	item := map[string]interface{}{
		"id":   123,
		"name": "Test Item",
	}

	store.SetCurrentIterationItem("node1", item, 5)

	retrieved, index := store.GetCurrentIterationItem("node1")
	if retrieved == nil {
		t.Fatal("Should retrieve iteration item")
	}
	if index != 5 {
		t.Errorf("Expected index 5, got %d", index)
	}
	if retrieved["id"] != 123 {
		t.Error("Item data mismatch")
	}

	// Non-existent node should return nil, -1
	retrieved, index = store.GetCurrentIterationItem("nonexistent")
	if retrieved != nil {
		t.Error("Should return nil for non-existent node")
	}
	if index != -1 {
		t.Error("Should return -1 for non-existent node")
	}
}

// TestBatchResultInitialization tests BatchResult structure
func TestBatchResultInitialization(t *testing.T) {
	ctx := context.Background()
	item := BatchItem{
		Index: 0,
		Data:  map[string]interface{}{"test": "data"},
	}

	result := BatchResult{
		Index:  item.Index,
		Output: make(map[string]interface{}),
		Items:  []map[string]interface{}{},
	}

	if result.Index != 0 {
		t.Error("Index mismatch")
	}
	if result.Output == nil {
		t.Error("Output should be initialized")
	}
	if result.Items == nil {
		t.Error("Items should be initialized")
	}
	if result.HasItems() {
		t.Error("Should not have items initially")
	}

	// Add items
	result.Items = make([]map[string]interface{}, 2)
	for i := range result.Items {
		result.Items[i] = make(map[string]interface{})
	}

	if !result.HasItems() {
		t.Error("Should have items after initialization")
	}

	_ = ctx // Use ctx to avoid unused variable error
}
