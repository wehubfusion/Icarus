package embedded

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// FieldMapper handles data flow between nodes within a unit
type FieldMapper struct{}

// NewFieldMapper creates a new field mapper
func NewFieldMapper() *FieldMapper {
	return &FieldMapper{}
}

// ApplyMappings applies field mappings from source node output to destination input
func (fm *FieldMapper) ApplyMappings(
	sourceOutput []byte,
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

	// Parse source output as JSON
	if !json.Valid(sourceOutput) {
		return nil, fmt.Errorf("source output is not valid JSON")
	}
	sourceJSON := string(sourceOutput)

	// Apply each mapping
	for _, mapping := range mappings {
		// Extract value from source using JSONPath
		sourceValue := gjson.Get(sourceJSON, mapping.SourceEndpoint)
		if !sourceValue.Exists() {
			// Skip if source field not found
			continue
		}

		// Handle iterate flag for array processing
		if mapping.Iterate && sourceValue.IsArray() {
			// For iterate mode, we map the entire array to each destination
			for _, destEndpoint := range mapping.DestinationEndpoints {
				var err error
				destJSON, err = sjson.Set(destJSON, destEndpoint, sourceValue.Value())
				if err != nil {
					return nil, fmt.Errorf("failed to set destination field %s: %w", destEndpoint, err)
				}
			}
		} else {
			// Map value to each destination endpoint
			for _, destEndpoint := range mapping.DestinationEndpoints {
				var err error
				destJSON, err = sjson.Set(destJSON, destEndpoint, sourceValue.Value())
				if err != nil {
					return nil, fmt.Errorf("failed to set destination field %s: %w", destEndpoint, err)
				}
			}
		}
	}

	return []byte(destJSON), nil
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

