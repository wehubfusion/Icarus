package runtime

import (
	"fmt"
	"strings"
)

// DefaultOutputResolver is the default implementation of OutputResolver.
type DefaultOutputResolver struct{}

// NewOutputResolver creates a new output resolver.
func NewOutputResolver() *DefaultOutputResolver {
	return &DefaultOutputResolver{}
}

// ResolveValue finds a value in the output based on source mapping.
func (r *DefaultOutputResolver) ResolveValue(
	output *StandardUnitOutput,
	sourceNodeId string,
	sourceEndpoint string,
) (interface{}, error) {
	key := sourceNodeId + "-" + sourceEndpoint
	// If there are per-item results, collect values from all items
	if len(output.Array) > 0 {
		values := make([]interface{}, 0, len(output.Array))
		for _, item := range output.Array {
			if val, exists := item[key]; exists {
				values = append(values, val)
			}
		}
		if len(values) > 0 {
			return values, nil
		}
		return nil, fmt.Errorf("%w: %s not found in array output", ErrKeyNotFound, key)
	}

	// Single object
	if output.Single != nil {
		if val, exists := output.Single[key]; exists {
			return val, nil
		}
	}

	return nil, fmt.Errorf("%w: %s not found in output", ErrKeyNotFound, key)
}

// BuildInputForUnit builds the complete input for a unit based on its field mappings.
func (r *DefaultOutputResolver) BuildInputForUnit(
	previousOutput *StandardUnitOutput,
	unit ExecutionUnit,
) (map[string]interface{}, error) {
	destStructure := r.analyzeDestinationStructure(unit.FieldMappings)

	if destStructure.HasArrayDest {
		return r.buildArrayInput(previousOutput, unit, destStructure)
	}

	return r.buildSingleInput(previousOutput, unit)
}

// analyzeDestinationStructure determines if destination expects array format.
func (r *DefaultOutputResolver) analyzeDestinationStructure(mappings []FieldMapping) DestinationStructure {
	ds := DestinationStructure{}

	for _, mapping := range mappings {
		for _, dest := range mapping.DestinationEndpoints {
			if strings.Contains(dest, "//") {
				ds.HasArrayDest = true
				// Extract array path
				parts := strings.Split(strings.TrimPrefix(dest, "/"), "//")
				if len(parts) > 0 && parts[0] != "" {
					ds.ArrayPath = parts[0]
				}
				return ds
			}
		}
	}

	return ds
}

// buildSingleInput builds input as a single object.
func (r *DefaultOutputResolver) buildSingleInput(
	output *StandardUnitOutput,
	unit ExecutionUnit,
) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for _, mapping := range unit.FieldMappings {
		if mapping.SourceNodeId == "" || mapping.SourceEndpoint == "" {
			continue
		}

		value, err := r.ResolveValue(output, mapping.SourceNodeId, mapping.SourceEndpoint)
		if err != nil {
			continue // Skip missing values
		}

		for _, dest := range mapping.DestinationEndpoints {
			SetNestedValue(result, dest, value)
		}
	}

	return result, nil
}

// buildArrayInput builds input with array structure.
func (r *DefaultOutputResolver) buildArrayInput(
	output *StandardUnitOutput,
	unit ExecutionUnit,
	destStructure DestinationStructure,
) (map[string]interface{}, error) {
	// If there are no per-item results, wrap the single object into an array
	if len(output.Array) == 0 {
		single, err := r.buildSingleInput(output, unit)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			destStructure.ArrayPath: []interface{}{single},
		}, nil
	}

	// Build array of objects matching source items length
	resultArray := make([]map[string]interface{}, len(output.Array))
	for i := range resultArray {
		resultArray[i] = make(map[string]interface{})
	}

	for _, mapping := range unit.FieldMappings {
		if mapping.SourceNodeId == "" || mapping.SourceEndpoint == "" {
			continue
		}

		sourceKey := mapping.SourceNodeId + "-" + mapping.SourceEndpoint

		// Get value from each item
		for i, item := range output.Array {
			if val, exists := item[sourceKey]; exists {
				for _, dest := range mapping.DestinationEndpoints {
					fieldName := ExtractFieldFromDestination(dest)
					resultArray[i][fieldName] = val
				}
			}
		}
	}

	// Wrap in the array path
	arrayPath := destStructure.ArrayPath
	if arrayPath == "" {
		arrayPath = "data"
	}

	return map[string]interface{}{
		arrayPath: resultArray,
	}, nil
}

// GetAllKeysForNode returns all output keys belonging to a specific node.
func (r *DefaultOutputResolver) GetAllKeysForNode(output *StandardUnitOutput, nodeId string) []string {
	prefix := nodeId + "-"
	var keys []string

	if len(output.Array) > 0 {
		// Get keys from first item (all items have same structure)
		for key := range output.Array[0] {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
	} else if output.Single != nil {
		for key := range output.Single {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
	}

	return keys
}

// GetNodeIdsInOutput returns all unique node IDs present in the output.
func (r *DefaultOutputResolver) GetNodeIdsInOutput(output *StandardUnitOutput) []string {
	nodeIds := make(map[string]struct{})

	extractNodeId := func(key string) {
		if idx := strings.Index(key, "-/"); idx > 0 {
			nodeIds[key[:idx]] = struct{}{}
		}
	}

	if len(output.Array) > 0 {
		for _, item := range output.Array {
			for key := range item {
				extractNodeId(key)
			}
		}
	} else if output.Single != nil {
		for key := range output.Single {
			extractNodeId(key)
		}
	}

	result := make([]string, 0, len(nodeIds))
	for id := range nodeIds {
		result = append(result, id)
	}
	return result
}

// Ensure DefaultOutputResolver implements OutputResolver
var _ OutputResolver = (*DefaultOutputResolver)(nil)
