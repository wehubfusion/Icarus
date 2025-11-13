package embedded

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/sjson"
	"github.com/wehubfusion/Icarus/pkg/embedded/pathutil"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// FieldMapper handles data flow between nodes within a unit
type FieldMapper struct{}

// NewFieldMapper creates a new field mapper
func NewFieldMapper() *FieldMapper {
	return &FieldMapper{}
}

// ApplyMappings applies field mappings from multiple source nodes to destination input
// It uses the OutputRegistry to look up source outputs by sourceNodeId
func (fm *FieldMapper) ApplyMappings(
	outputRegistry *OutputRegistry,
	mappings []message.FieldMapping,
	destinationInput []byte,
) ([]byte, error) {
	if len(mappings) == 0 {
		return destinationInput, nil
	}

	// Parse destination input as JSON (or use empty object if invalid)
	var destJSON string
	if len(destinationInput) > 0 && json.Valid(destinationInput) {
		destJSON = string(destinationInput)
	} else {
		destJSON = "{}"
	}

	// Group mappings by source node ID for efficient processing
	mappingsBySource := fm.groupMappingsBySource(mappings)

	// Apply mappings from each source node
	for sourceNodeID, sourceMappings := range mappingsBySource {
		// Get source output from registry
		sourceOutput, exists := outputRegistry.Get(sourceNodeID)
		if !exists {
			// Source node hasn't executed yet or doesn't exist
			// Skip these mappings (allows for optional dependencies)
			continue
		}

		// Validate source output is JSON
		if !json.Valid(sourceOutput) {
			return nil, fmt.Errorf("source output from node %s is not valid JSON", sourceNodeID)
		}

		// Apply each mapping from this source
		for _, mapping := range sourceMappings {
			// Extract value from source using namespace-aware path navigation
			sourceValue, exists := pathutil.NavigatePath(sourceOutput, mapping.SourceEndpoint)
			if !exists {
				// Skip if source field not found (allows for optional fields)
				continue
			}

			// Handle iterate flag for array processing
			if mapping.Iterate {
				// Check if value is an array
				if arr, ok := sourceValue.([]interface{}); ok {
					// For iterate mode, map the entire array to each destination
					for _, destEndpoint := range mapping.DestinationEndpoints {
						var err error
						destJSON, err = sjson.Set(destJSON, destEndpoint, arr)
						if err != nil {
							return nil, fmt.Errorf("failed to set destination field %s: %w", destEndpoint, err)
						}
					}
				} else {
					// Not an array, map as single value
					for _, destEndpoint := range mapping.DestinationEndpoints {
						var err error
						destJSON, err = sjson.Set(destJSON, destEndpoint, sourceValue)
						if err != nil {
							return nil, fmt.Errorf("failed to set destination field %s: %w", destEndpoint, err)
						}
					}
				}
			} else {
				// Map value to each destination endpoint
				for _, destEndpoint := range mapping.DestinationEndpoints {
					var err error
					destJSON, err = sjson.Set(destJSON, destEndpoint, sourceValue)
					if err != nil {
						return nil, fmt.Errorf("failed to set destination field %s: %w", destEndpoint, err)
					}
				}
			}
		}
	}

	return []byte(destJSON), nil
}

// groupMappingsBySource groups field mappings by their source node ID
func (fm *FieldMapper) groupMappingsBySource(mappings []message.FieldMapping) map[string][]message.FieldMapping {
	grouped := make(map[string][]message.FieldMapping)
	for _, mapping := range mappings {
		sourceID := mapping.SourceNodeID
		grouped[sourceID] = append(grouped[sourceID], mapping)
	}
	return grouped
}

// MergeInputs merges multiple input sources into a single JSON object
func (fm *FieldMapper) MergeInputs(inputs ...[]byte) ([]byte, error) {
	if len(inputs) == 0 {
		return []byte("{}"), nil
	}

	// Start with first input
	result := "{}"
	if len(inputs[0]) > 0 && json.Valid(inputs[0]) {
		result = string(inputs[0])
	}

	// Merge remaining inputs
	for i := 1; i < len(inputs); i++ {
		if len(inputs[i]) == 0 || !json.Valid(inputs[i]) {
			continue
		}

		// Parse both JSON objects
		var resultMap, inputMap map[string]interface{}
		if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
			return nil, fmt.Errorf("failed to parse result JSON: %w", err)
		}
		if err := json.Unmarshal(inputs[i], &inputMap); err != nil {
			return nil, fmt.Errorf("failed to parse input JSON: %w", err)
		}

		// Merge input into result (input overwrites result)
		for k, v := range inputMap {
			resultMap[k] = v
		}

		// Marshal back to JSON
		merged, err := json.Marshal(resultMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal merged JSON: %w", err)
		}
		result = string(merged)
	}

	return []byte(result), nil
}

