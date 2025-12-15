// Package runtime provides the core types and interfaces for embedded node processing.
package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
)

// FieldMapping represents the mapping configuration between nodes.
// It defines how data flows from a source node to a destination node.
type FieldMapping struct {
	// SourceNodeId is the ID of the node providing the data
	SourceNodeId string `json:"sourceNodeId"`
	// SourceNodeLabel is the human-readable label of the source node
	SourceNodeLabel string `json:"sourceNodeLabel"`
	// SourceEndpoint is the path to the data in the source node's output
	// Uses "/" for simple paths and "//" for array iteration (e.g., "/data//name")
	SourceEndpoint string `json:"sourceEndpoint"`
	// SourceSectionId identifies which section of the source output to use
	SourceSectionId string `json:"sourceSectionId"`
	// DestinationEndpoints are the paths where data should be placed in the destination
	DestinationEndpoints []string `json:"destinationEndpoints"`
	// DestinationSectionId identifies which section of the destination to populate
	DestinationSectionId string `json:"destinationSectionId"`
	// DataType indicates the type of mapping (e.g., "FIELD", "EVENT")
	DataType string `json:"dataType"`
	// IsEventTrigger indicates if this mapping triggers conditional execution
	IsEventTrigger bool `json:"isEventTrigger,omitempty"`
	// Iterate indicates if this mapping should iterate over array items
	Iterate bool `json:"iterate"`
}

// IsEvent returns true if this mapping is an event trigger
func (f FieldMapping) IsEvent() bool {
	return f.DataType == "EVENT"
}

// HasArrayNotation returns true if the source endpoint contains //
func (f FieldMapping) HasArrayNotation() bool {
	return strings.Contains(f.SourceEndpoint, "//")
}

// EmbeddedNodeConfig represents the configuration for an embedded node in the execution plan.
type EmbeddedNodeConfig struct {
	// NodeId is the unique identifier for this node
	NodeId string `json:"nodeId"`
	// Label is the human-readable name for this node
	Label string `json:"label"`
	// PluginType identifies which processor handles this node
	PluginType string `json:"pluginType"`
	// Embeddable indicates if this node can be embedded in a unit
	Embeddable bool `json:"embeddable"`
	// Depth represents the node's position in the execution graph
	Depth int `json:"depth"`
	// ExecutionOrder determines the sequence of execution within a unit
	ExecutionOrder int `json:"executionOrder"`
	// FieldMappings define how data flows into this node
	FieldMappings []FieldMapping `json:"fieldMappings"`
	// NodeConfig contains the node-specific configuration
	NodeConfig NodeConfig `json:"nodeConfig"`
}

// HasIterateMappings returns true if any field mapping has iterate:true
func (e EmbeddedNodeConfig) HasIterateMappings() bool {
	for _, m := range e.FieldMappings {
		if m.Iterate && !m.IsEvent() {
			return true
		}
	}
	return false
}

// GetIterateMappings returns mappings with iterate:true (excluding events)
func (e EmbeddedNodeConfig) GetIterateMappings() []FieldMapping {
	var mappings []FieldMapping
	for _, m := range e.FieldMappings {
		if m.Iterate && !m.IsEvent() {
			mappings = append(mappings, m)
		}
	}
	return mappings
}

// GetEventMappings returns event trigger mappings
func (e EmbeddedNodeConfig) GetEventMappings() []FieldMapping {
	var events []FieldMapping
	for _, m := range e.FieldMappings {
		if m.IsEvent() && m.IsEventTrigger {
			events = append(events, m)
		}
	}
	return events
}

// GetFieldMappings returns non-event field mappings
func (e EmbeddedNodeConfig) GetFieldMappings() []FieldMapping {
	var fields []FieldMapping
	for _, m := range e.FieldMappings {
		if !m.IsEvent() {
			fields = append(fields, m)
		}
	}
	return fields
}

// NodeConfig contains the detailed configuration for a node.
type NodeConfig struct {
	// NodeId is the unique identifier (matches parent EmbeddedNodeConfig.NodeId)
	NodeId string `json:"node_id"`
	// WorkflowId is the ID of the workflow this node belongs to
	WorkflowId string `json:"workflow_id"`
	// NodeSchemaId is the ID of the schema defining this node's structure
	NodeSchemaId string `json:"node_schema_id"`
	// Config contains the node-specific configuration as raw JSON
	Config json.RawMessage `json:"config"`
	// CreatedAt is the creation timestamp
	CreatedAt string `json:"created_at,omitempty"`
	// UpdatedAt is the last update timestamp
	UpdatedAt string `json:"updated_at,omitempty"`
}

// ExecutionUnit represents a group of nodes to be executed together.
// It contains a parent node and its embedded nodes.
type ExecutionUnit struct {
	// Index is the position of this unit in the execution plan
	Index int `json:"index"`
	// NodeId is the ID of the parent node
	NodeId string `json:"nodeId"`
	// Label is the human-readable name of the parent node
	Label string `json:"label"`
	// Type is the node type (e.g., "plugin")
	Type string `json:"type"`
	// PluginType identifies which processor handles the parent node
	PluginType string `json:"pluginType"`
	// Depth represents the unit's position in the execution graph
	Depth int `json:"depth"`
	// Dependencies lists the node IDs this unit depends on
	Dependencies []string `json:"dependencies"`
	// FieldMappings define how data flows into this unit
	FieldMappings []FieldMapping `json:"fieldMappings"`
	// EmbeddedNodes are the nodes to be processed within this unit
	EmbeddedNodes []EmbeddedNodeConfig `json:"embeddedNodes"`
	// NodeConfig contains the parent node's configuration
	NodeConfig NodeConfig `json:"nodeConfig"`
	// IsTrigger indicates if this unit is a workflow trigger
	IsTrigger bool `json:"isTrigger,omitempty"`
}

// StandardUnitOutput represents the flattened output of a unit.
// Keys are formatted as "nodeId-/path" for objects or "nodeId-/arrayPath//field" for array items.
type StandardUnitOutput struct {
	// Single contains non-iterated node outputs (always populated)
	// For non-iteration: contains all outputs
	// For iteration: contains pre-iteration (shared) outputs only
	Single map[string]interface{} `json:"single"`

	// Array contains per-iteration outputs (empty slice if no iteration)
	// Each item contains outputs from nodes that ran in iteration
	Array []map[string]interface{} `json:"array"`
}

// NewSingleOutput creates output for non-iterated processing
func NewSingleOutput(data map[string]interface{}) *StandardUnitOutput {
	if data == nil {
		data = make(map[string]interface{})
	}
	return &StandardUnitOutput{
		Single: data,
		Array:  []map[string]interface{}{},
	}
}

// NewIteratedOutput creates output for iterated processing
func NewIteratedOutput(data map[string]interface{}, items []map[string]interface{}) *StandardUnitOutput {
	if data == nil {
		data = make(map[string]interface{})
	}
	if items == nil {
		items = []map[string]interface{}{}
	}
	return &StandardUnitOutput{
		Single: data,
		Array:  items,
	}
}

// HasIteration returns true if iteration occurred
func (o *StandardUnitOutput) HasIteration() bool {
	return len(o.Array) > 0
}

// Len returns the number of items (1 if no iteration)
func (o *StandardUnitOutput) Len() int {
	if o.HasIteration() {
		return len(o.Array)
	}
	return 1
}

// GetValue retrieves a value by key, checking both Single and Array[index]
func (o *StandardUnitOutput) GetValue(key string, index int) (interface{}, bool) {
	if o.HasIteration() {
		// Check item first
		if index >= 0 && index < len(o.Array) {
			if val, ok := o.Array[index][key]; ok {
				return val, true
			}
		}
	}
	// Fallback to Single
	if val, ok := o.Single[key]; ok {
		return val, true
	}
	return nil, false
}

// GetItem returns merged Single + Array[index]
func (o *StandardUnitOutput) GetItem(index int) map[string]interface{} {
	if !o.HasIteration() {
		return o.Single
	}

	if index < 0 || index >= len(o.Array) {
		return nil
	}

	result := make(map[string]interface{}, len(o.Single)+len(o.Array[index]))
	for k, v := range o.Single {
		result[k] = v
	}
	for k, v := range o.Array[index] {
		result[k] = v
	}
	return result
}

// GetAllItems returns all items with Single merged in
func (o *StandardUnitOutput) GetAllItems() []map[string]interface{} {
	if !o.HasIteration() {
		if o.Single != nil {
			return []map[string]interface{}{o.Single}
		}
		return nil
	}

	result := make([]map[string]interface{}, len(o.Array))
	for i := range o.Array {
		result[i] = o.GetItem(i)
	}
	return result
}

// GetAllValues collects all values for a key across Array
func (o *StandardUnitOutput) GetAllValues(key string) []interface{} {
	if !o.HasIteration() {
		if val, ok := o.Single[key]; ok {
			return []interface{}{val}
		}
		return nil
	}

	values := make([]interface{}, 0, len(o.Array))
	for _, item := range o.Array {
		if val, ok := item[key]; ok {
			values = append(values, val)
		}
	}
	return values
}

// ProcessInput contains all data needed for an embedded node to process.
type ProcessInput struct {
	// Ctx is the context for cancellation and timeouts
	Ctx context.Context
	// Data is the input data built from field mappings
	Data map[string]interface{}
	// Config is the node-specific configuration (parsed from NodeConfig.Config)
	Config map[string]interface{}
	// RawConfig is the original raw JSON configuration
	RawConfig json.RawMessage
	// NodeId is the ID of the node being processed
	NodeId string
	// PluginType is the type of processor handling this node
	PluginType string
	// Label is the human-readable name of the node
	Label string
	// ItemIndex is the current iteration index (-1 if not iterating)
	ItemIndex int
	// TotalItems is the total number of items in iteration (0 if not iterating)
	TotalItems int
	// IsIteration indicates if this is part of an iteration
	IsIteration bool
	// IterationPath is the array path being iterated (e.g., "data")
	IterationPath string
}

// ProcessOutput contains the result of embedded node processing.
type ProcessOutput struct {
	// Data is the output data from the node
	Data map[string]interface{}
	// Error is set if processing failed
	Error error
	// Skipped indicates if the node was skipped (e.g., due to event trigger)
	Skipped bool
	// SkipReason provides context for why the node was skipped
	SkipReason string
}

// BatchItem represents a single item to be processed in concurrent execution.
type BatchItem struct {
	// Index is the original position in the array (for result ordering)
	Index int
	// Data is the item data to process
	Data map[string]interface{}
}

// BatchResult represents the result of processing a batch item.
type BatchResult struct {
	// Index is the original position (for result ordering)
	Index int
	// Output is the flattened output for shared/pre-iteration data
	Output map[string]interface{}
	// Items contains per-iteration outputs (empty if no mid-flow iteration)
	Items []map[string]interface{}
	// Error is set if processing failed
	Error error
}

// HasItems returns true if there are iteration items
func (r BatchResult) HasItems() bool {
	return len(r.Items) > 0
}

// IterationState holds information about an active iteration within a subflow.
type IterationState struct {
	// IsActive indicates if we're currently in an iteration
	IsActive bool
	// ArrayPath is the path to the array being iterated (e.g., "data")
	ArrayPath string
	// SourceNodeId is the node that produced the array
	SourceNodeId string
	// InitiatedByNodeId is the node that started the iteration (has iterate:true)
	InitiatedByNodeId string
	// TotalItems is the total number of items
	TotalItems int
	// Items are the actual array items
	Items []interface{}
}

// NodeOutputStore stores outputs from executed nodes during subflow processing.
// It tracks single outputs vs iterated outputs separately.
// All map accesses are protected by a mutex for concurrent access safety.
type NodeOutputStore struct {
	mu sync.RWMutex
	// SingleOutputs stores non-iterated node outputs
	// Key: nodeId
	SingleOutputs map[string]map[string]interface{}
	// IteratedOutputs stores outputs from nodes that ran in iteration
	// Key: nodeId, Value: array of outputs (one per iteration)
	IteratedOutputs map[string][]map[string]interface{}
	// IterationInfo tracks which nodes produced arrays and their state
	IterationInfo map[string]IterationState
	// CurrentIterationItems stores the current item being processed for each iteration source
	// Key: sourceNodeId, Value: {item, index}
	CurrentIterationItems map[string]currentIterationItem
}

// currentIterationItem holds the current item and its index for iteration
type currentIterationItem struct {
	Item  map[string]interface{}
	Index int
}

// NewNodeOutputStore creates a new output store
func NewNodeOutputStore() *NodeOutputStore {
	return &NodeOutputStore{
		SingleOutputs:         make(map[string]map[string]interface{}),
		IteratedOutputs:       make(map[string][]map[string]interface{}),
		IterationInfo:         make(map[string]IterationState),
		CurrentIterationItems: make(map[string]currentIterationItem),
	}
}

// SetSingleOutput stores a single (non-iterated) output
func (s *NodeOutputStore) SetSingleOutput(nodeId string, output map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SingleOutputs[nodeId] = output
}

// SetIteratedOutputs stores all iterated outputs at once
func (s *NodeOutputStore) SetIteratedOutputs(nodeId string, outputs []map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IteratedOutputs[nodeId] = outputs
}

// GetOutput retrieves output for a node, optionally at a specific index
// If index is -1, returns single output; otherwise returns iterated output at index
func (s *NodeOutputStore) GetOutput(nodeId string, index int) (map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Check iterated outputs first if index >= 0
	if index >= 0 {
		if outputs, ok := s.IteratedOutputs[nodeId]; ok {
			if index < len(outputs) {
				return outputs[index], true
			}
			return nil, false
		}
	}
	// Fall back to single output
	if output, ok := s.SingleOutputs[nodeId]; ok {
		return output, true
	}
	return nil, false
}

// GetAllIteratedOutputs returns all outputs for a node that ran in iteration
func (s *NodeOutputStore) GetAllIteratedOutputs(nodeId string) ([]map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	outputs, ok := s.IteratedOutputs[nodeId]
	return outputs, ok
}

// HasIteratedOutput checks if a node has iterated outputs
func (s *NodeOutputStore) HasIteratedOutput(nodeId string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.IteratedOutputs[nodeId]
	return ok
}

// SetIterationInfo stores iteration info for a node
func (s *NodeOutputStore) SetIterationInfo(nodeId string, info IterationState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.IterationInfo[nodeId] = info
}

// GetIterationInfo retrieves iteration info for a node
func (s *NodeOutputStore) GetIterationInfo(nodeId string) (IterationState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.IterationInfo[nodeId]
	return info, ok
}

// GetAllSingleOutputs returns all single (non-iterated) outputs
// Returns a copy to prevent external mutation of the internal map
func (s *NodeOutputStore) GetAllSingleOutputs() map[string]map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]map[string]interface{}, len(s.SingleOutputs))
	for k, v := range s.SingleOutputs {
		result[k] = v
	}
	return result
}

// SetCurrentIterationItem stores the current iteration item for a source node
func (s *NodeOutputStore) SetCurrentIterationItem(sourceNodeId string, item map[string]interface{}, index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentIterationItems[sourceNodeId] = currentIterationItem{
		Item:  item,
		Index: index,
	}
}

// GetCurrentIterationItem retrieves the current iteration item for a source node
func (s *NodeOutputStore) GetCurrentIterationItem(sourceNodeId string) (map[string]interface{}, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ci, ok := s.CurrentIterationItems[sourceNodeId]; ok {
		return ci.Item, ci.Index
	}
	return nil, -1
}

// IterationContext holds information about array iteration at the parent level.
type IterationContext struct {
	// IsArrayIteration indicates if we're iterating over an array
	IsArrayIteration bool
	// ArrayPath is the path to the array (e.g., "data" from "/data//field")
	ArrayPath string
	// TotalItems is the number of items to iterate over
	TotalItems int
}

// DestinationStructure holds information about the destination format.
type DestinationStructure struct {
	// HasArrayDest indicates if any destination uses array notation (//)
	HasArrayDest bool
	// ArrayPath is the array path from the destination (e.g., "data")
	ArrayPath string
}

// (SuccessOutput, ErrorOutput and SkippedOutput are provided by specific node helpers in base_node.go)
