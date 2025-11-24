package storage

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/logging"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/output"
)

// SmartStorage provides thread-safe storage for all node outputs (parent + embedded)
// with automatic cleanup when all consumers have read the data
type SmartStorage struct {
	data      map[string]*StorageEntry
	consumers map[string][]string // nodeID -> list of consumer nodeIDs
	mu        sync.RWMutex
	logger    logging.Logger
}

// StorageEntry represents a single node's output in storage
type StorageEntry struct {
	NodeID           string                 `json:"node_id"`
	Result           json.RawMessage        `json:"result"`           // Raw JSON for type-agnostic storage
	IterationContext *IterationContext      `json:"iteration_context,omitempty"`
	Consumers        []string               `json:"consumers"`         // List of nodes that need this result
	ConsumedBy       []string               `json:"consumed_by"`       // List of nodes that have read this result
	Timestamp        time.Time              `json:"timestamp"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// IterationContext stores metadata about array outputs
type IterationContext struct {
	IsArray     bool   `json:"is_array"`
	ArrayPath   string `json:"array_path"`   // Path to array in result (e.g., "/data", "/result")
	ArrayLength int    `json:"array_length"`
}

// NewSmartStorage creates a new smart storage instance
func NewSmartStorage(logger logging.Logger) *SmartStorage {
	if logger == nil {
		logger = &logging.NoOpLogger{}
	}
	return &SmartStorage{
		data:      make(map[string]*StorageEntry),
		consumers: make(map[string][]string),
		mu:        sync.RWMutex{},
		logger:    logger,
	}
}

// NewSmartStorageWithConsumers creates storage with pre-built consumer graph
func NewSmartStorageWithConsumers(consumers map[string][]string, logger logging.Logger) *SmartStorage {
	if logger == nil {
		logger = &logging.NoOpLogger{}
	}
	return &SmartStorage{
		data:      make(map[string]*StorageEntry),
		consumers: consumers,
		mu:        sync.RWMutex{},
		logger:    logger,
	}
}

// Set stores a node's result with optional iteration context
func (s *SmartStorage) Set(nodeID string, result interface{}, iterationContext *IterationContext) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Marshal result to JSON for type-agnostic storage
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result for node %s: %w", nodeID, err)
	}

	// Get consumers for this node from pre-built graph
	consumers := s.consumers[nodeID]
	if consumers == nil {
		consumers = []string{}
	}

	entry := &StorageEntry{
		NodeID:           nodeID,
		Result:           resultJSON,
		IterationContext: iterationContext,
		Consumers:        consumers,
		ConsumedBy:       []string{},
		Timestamp:        time.Now(),
		Metadata:         make(map[string]interface{}),
	}

	s.data[nodeID] = entry
	
	s.logger.Debug(fmt.Sprintf(
		"[SmartStorage] Stored result for node %s (has_iteration_context=%v, consumers=%d)",
		nodeID,
		iterationContext != nil,
		len(consumers),
	))

	return nil
}

// Get retrieves a node's result from storage
func (s *SmartStorage) Get(nodeID string) (*StorageEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[nodeID]
	if !exists {
		return nil, fmt.Errorf("no result found for node %s", nodeID)
	}

	return entry, nil
}

// Has checks if a node's result exists in storage
func (s *SmartStorage) Has(nodeID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.data[nodeID]
	return exists
}

// MarkConsumed marks that a consumer has read this node's result
// Automatically deletes the entry if all consumers have read it
func (s *SmartStorage) MarkConsumed(sourceNodeID, consumerNodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[sourceNodeID]
	if !exists {
		return
	}

	// Check if this consumer was expected
	isExpectedConsumer := false
	for _, expectedConsumer := range entry.Consumers {
		if expectedConsumer == consumerNodeID {
			isExpectedConsumer = true
			break
		}
	}

	if !isExpectedConsumer {
		s.logger.Debug(fmt.Sprintf(
			"[SmartStorage] Consumer %s not in expected list for node %s",
			consumerNodeID,
			sourceNodeID,
		))
		return
	}

	// Check if already consumed by this consumer
	for _, consumed := range entry.ConsumedBy {
		if consumed == consumerNodeID {
			return // Already marked
		}
	}

	// Mark as consumed
	entry.ConsumedBy = append(entry.ConsumedBy, consumerNodeID)

	s.logger.Debug(fmt.Sprintf(
		"[SmartStorage] Node %s consumed by %s (%d/%d consumers)",
		sourceNodeID,
		consumerNodeID,
		len(entry.ConsumedBy),
		len(entry.Consumers),
	))

	// Check if all consumers have consumed
	if len(entry.ConsumedBy) >= len(entry.Consumers) {
		delete(s.data, sourceNodeID)
		s.logger.Debug(fmt.Sprintf(
			"[SmartStorage] Auto-cleaned node %s (all consumers satisfied)",
			sourceNodeID,
		))
	}
}

// AddConsumer adds a consumer to the consumer graph
// This is used during initialization when building the consumer graph
func (s *SmartStorage) AddConsumer(sourceNodeID, consumerNodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.consumers[sourceNodeID] == nil {
		s.consumers[sourceNodeID] = []string{}
	}

	// Check if already exists
	for _, existing := range s.consumers[sourceNodeID] {
		if existing == consumerNodeID {
			return
		}
	}

	s.consumers[sourceNodeID] = append(s.consumers[sourceNodeID], consumerNodeID)
}

// GetAll returns all entries in storage (for debugging/inspection)
func (s *SmartStorage) GetAll() map[string]*StorageEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a copy to avoid external modification
	copy := make(map[string]*StorageEntry, len(s.data))
	for k, v := range s.data {
		copy[k] = v
	}

	return copy
}

// Size returns the number of entries in storage
func (s *SmartStorage) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}

// Clear removes all entries from storage (for testing)
func (s *SmartStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = make(map[string]*StorageEntry)
	s.logger.Debug("[SmartStorage] Cleared all entries")
}

// MarshalJSON implements json.Marshaler for Temporal serialization
func (s *SmartStorage) MarshalJSON() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return json.Marshal(map[string]interface{}{
		"data":      s.data,
		"consumers": s.consumers,
	})
}

// UnmarshalJSON implements json.Unmarshaler for Temporal deserialization
func (s *SmartStorage) UnmarshalJSON(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var temp struct {
		Data      map[string]*StorageEntry `json:"data"`
		Consumers map[string][]string      `json:"consumers"`
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	s.data = temp.Data
	s.consumers = temp.Consumers

	return nil
}

// GetStats returns storage statistics
func (s *SmartStorage) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalConsumers := 0
	totalConsumed := 0
	entriesWithIterationContext := 0

	for _, entry := range s.data {
		totalConsumers += len(entry.Consumers)
		totalConsumed += len(entry.ConsumedBy)
		if entry.IterationContext != nil {
			entriesWithIterationContext++
		}
	}

	return map[string]interface{}{
		"total_entries":                 len(s.data),
		"total_consumers":               totalConsumers,
		"total_consumed":                totalConsumed,
		"entries_with_iteration_context": entriesWithIterationContext,
		"consumer_graph_size":           len(s.consumers),
	}
}

// ExtractResult unmarshals the result from a storage entry into a target interface
func (s *SmartStorage) ExtractResult(nodeID string, target interface{}) error {
	entry, err := s.Get(nodeID)
	if err != nil {
		return err
	}

	return json.Unmarshal(entry.Result, target)
}

// GetResultAsStandardOutput extracts result as StandardOutput
func (s *SmartStorage) GetResultAsStandardOutput(nodeID string) (*output.StandardOutput, error) {
	var stdOutput output.StandardOutput
	err := s.ExtractResult(nodeID, &stdOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to extract StandardOutput for node %s: %w", nodeID, err)
	}
	return &stdOutput, nil
}

// GetWithIterationIndex retrieves a specific array item from a node's result
// Returns error if the node is not an array or if index is out of bounds
func (s *SmartStorage) GetWithIterationIndex(nodeID string, index int) (*StorageEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[nodeID]
	if !exists {
		return nil, fmt.Errorf("no result found for node %s", nodeID)
	}

	// Verify this is an array result
	if entry.IterationContext == nil || !entry.IterationContext.IsArray {
		return nil, fmt.Errorf("node %s does not have array output (cannot use iteration index)", nodeID)
	}

	// Validate index bounds
	if index < 0 || index >= entry.IterationContext.ArrayLength {
		return nil, fmt.Errorf(
			"iteration index %d out of bounds for node %s (array length: %d)",
			index,
			nodeID,
			entry.IterationContext.ArrayLength,
		)
	}

	s.logger.Debug(fmt.Sprintf(
		"[SmartStorage] Retrieved node %s at iteration index %d (array length: %d)",
		nodeID,
		index,
		entry.IterationContext.ArrayLength,
	))

	return entry, nil
}

// GetIterationContext retrieves only the iteration context for a node
func (s *SmartStorage) GetIterationContext(nodeID string) (*IterationContext, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[nodeID]
	if !exists {
		return nil, fmt.Errorf("no result found for node %s", nodeID)
	}

	return entry.IterationContext, nil
}
