package output

import "sync"

// OutputRegistry stores outputs from all executed nodes in a unit
// It enables multi-source field mapping where nodes can read from
// the parent node or any previously executed embedded node by ID
// Thread-safe for concurrent access
type OutputRegistry struct {
	outputs map[string]*StandardOutput
	mu      sync.RWMutex
}

// NewOutputRegistry creates a new output registry
func NewOutputRegistry() *OutputRegistry {
	return &OutputRegistry{
		outputs: make(map[string]*StandardOutput),
	}
}

// Set stores output for a node identified by nodeID
func (r *OutputRegistry) Set(nodeID string, output *StandardOutput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outputs[nodeID] = output
}

// Get retrieves output for a node identified by nodeID
// Returns the output and a boolean indicating whether the node exists
func (r *OutputRegistry) Get(nodeID string) (*StandardOutput, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	output, exists := r.outputs[nodeID]
	return output, exists
}

// Has checks if output exists for a node identified by nodeID
func (r *OutputRegistry) Has(nodeID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.outputs[nodeID]
	return exists
}

// GetAll returns all stored outputs as a map (nodeID -> output)
// Returns a copy to prevent external modification of internal state
func (r *OutputRegistry) GetAll() map[string]*StandardOutput {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]*StandardOutput, len(r.outputs))
	for k, v := range r.outputs {
		result[k] = v
	}
	return result
}
