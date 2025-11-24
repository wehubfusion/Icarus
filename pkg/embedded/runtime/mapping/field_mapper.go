package mapping

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/logging"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/output"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/pathutil"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/storage"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// FieldMapper handles data flow between nodes within a unit
type FieldMapper struct {
	logger logging.Logger
}

// NewFieldMapper creates a new field mapper
func NewFieldMapper(logger logging.Logger) *FieldMapper {
	if logger == nil {
		logger = &logging.NoOpLogger{}
	}
	return &FieldMapper{
		logger: logger,
	}
}

// ApplyMappings applies field mappings from multiple source nodes to destination input
// It uses the SmartStorage to look up source outputs by sourceNodeId
// currentIterationIndex: when >= 0, extract array items at this specific index from array sources
func (fm *FieldMapper) ApplyMappings(
	storage *storage.SmartStorage,
	mappings []message.FieldMapping,
	destinationInput []byte,
	currentIterationIndex int,
) ([]byte, error) {
	if len(mappings) == 0 {
		return destinationInput, nil
	}

	// Parse destination input as JSON map (or use empty map if invalid)
	var destMap map[string]interface{}
	if len(destinationInput) > 0 && json.Valid(destinationInput) {
		if err := json.Unmarshal(destinationInput, &destMap); err != nil {
			return nil, fmt.Errorf("failed to parse destination input: %w", err)
		}
	} else {
		destMap = make(map[string]interface{})
	}

	// Group mappings by source node ID for efficient processing
	mappingsBySource := fm.groupMappingsBySource(mappings)

	// Apply mappings from each source node
	for sourceNodeID, sourceMappings := range mappingsBySource {
		// Check if source has iteration context (array output)
		iterationCtx, _ := storage.GetIterationContext(sourceNodeID)

		// Check if ANY mapping from this source has iterate flag
		hasIterateFlag := false
		for _, m := range sourceMappings {
			if m.Iterate {
				hasIterateFlag = true
				break
			}
		}

		var sourceOutput *output.StandardOutput
		var err error

		// Extract array items ONLY if: source is array AND has iterate flag AND valid iteration index
		if iterationCtx != nil && iterationCtx.IsArray && hasIterateFlag && currentIterationIndex >= 0 {
			// Validate iteration index and get array result
			_, err := storage.GetWithIterationIndex(sourceNodeID, currentIterationIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to get source %s at iteration index %d: %w",
					sourceNodeID, currentIterationIndex, err)
			}

			// Get the full source output
			sourceOutput, err = storage.GetResultAsStandardOutput(sourceNodeID)
			if err != nil {
				return nil, fmt.Errorf("failed to get source output for %s after index validation: %w", sourceNodeID, err)
			}

			// Extract the specific array item from result
			if arrayResult, ok := sourceOutput.Result.([]interface{}); ok {
				if currentIterationIndex < len(arrayResult) {
					// Create new StandardOutput with single item instead of array
					sourceOutput = &output.StandardOutput{
						Meta:   sourceOutput.Meta,
						Events: sourceOutput.Events,
						Error:  sourceOutput.Error,
						Result: arrayResult[currentIterationIndex],
					}
					fm.logger.Debug("Extracted array item from source",
						logging.Field{Key: "source_node_id", Value: sourceNodeID},
						logging.Field{Key: "iteration_index", Value: currentIterationIndex},
						logging.Field{Key: "array_length", Value: len(arrayResult)})
				}
			}
		} else {
			// Normal case: get source output as-is
			// This includes: non-array sources, array sources without iterate flag, or no iteration index
			sourceOutput, err = storage.GetResultAsStandardOutput(sourceNodeID)
			if err != nil {
				// Source node hasn't executed yet or doesn't exist
				// Skip these mappings (allows for optional dependencies)
				fm.logger.Debug("Source node not available (optional dependency)",
					logging.Field{Key: "source_node_id", Value: sourceNodeID})
				continue
			}
		}

		// Validate source output is not nil
		if sourceOutput == nil {
			return nil, fmt.Errorf("source output from node %s is nil", sourceNodeID)
		}

		// Apply each mapping from this source
		for _, mapping := range sourceMappings {
			// Extract value from source using namespace-aware path navigation
			// Convert to pathutil.StandardOutput to avoid circular dependency
			sourceValue, exists := pathutil.NavigatePath(toPathutilStandardOutput(sourceOutput), mapping.SourceEndpoint)
			if !exists {
				// Skip if source field not found (allows for optional fields)
				fm.logger.Debug("Source field not found",
					logging.Field{Key: "source_node_id", Value: sourceNodeID},
					logging.Field{Key: "source_endpoint", Value: mapping.SourceEndpoint},
				)
				continue
			}
			fm.logger.Info("Extracted value from source",
				logging.Field{Key: "source_node_id", Value: sourceNodeID},
				logging.Field{Key: "source_endpoint", Value: mapping.SourceEndpoint},
				logging.Field{Key: "value", Value: sourceValue},
			)

			// Map value to each destination endpoint
			for _, destEndpoint := range mapping.DestinationEndpoints {
				fm.setFieldAtPath(destMap, destEndpoint, sourceValue)
			}
		}
	}

	// Debug: Log final mapped result
	finalResult, _ := json.Marshal(destMap)
	fm.logger.Info("Final mapped result",
		logging.Field{Key: "result", Value: string(finalResult)},
	)

	// Marshal back to JSON
	result, err := json.Marshal(destMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal result: %w", err)
	}

	return result, nil
}

// setFieldAtPath sets a field value in a map using a path with slash notation
// Supports // notation for collection traversal (applies value to each item in collection)
func (fm *FieldMapper) setFieldAtPath(data map[string]interface{}, path string, value interface{}) {
	if path == "" {
		// Root level - merge if value is map
		if valueMap, ok := value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				data[k] = v
			}
		}
		return
	}

	// Normalize path (remove leading slash)
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		// Root level after normalization
		if valueMap, ok := value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				data[k] = v
			}
		}
		return
	}

	// Check if path contains // for collection traversal
	if strings.Contains(path, "//") {
		fm.setFieldAtPathWithCollectionTraversal(data, path, value)
		return
	}

	// Split path and create nested structure
	parts := strings.Split(path, "/")
	current := data

	for i, part := range parts {
		if part == "" {
			continue
		}

		if i == len(parts)-1 {
			// Last part - set value
			current[part] = value
		} else {
			// Intermediate part - ensure map exists
			if _, exists := current[part]; !exists {
				current[part] = make(map[string]interface{})
			}
			if nextMap, ok := current[part].(map[string]interface{}); ok {
				current = nextMap
			} else {
				// Can't traverse further - overwrite with new map
				current[part] = make(map[string]interface{})
				current = current[part].(map[string]interface{})
			}
		}
	}
}

// setFieldAtPathWithCollectionTraversal handles paths with // notation
// Example: /user//city means set "city" field in each item of the "user" array
func (fm *FieldMapper) setFieldAtPathWithCollectionTraversal(data map[string]interface{}, path string, value interface{}) {
	// Split on // to get: [collection_path, field_path_in_items]
	parts := strings.SplitN(path, "//", 2)
	if len(parts) != 2 {
		// Malformed path - fall back to regular set
		fm.setFieldAtPath(data, path, value)
		return
	}

	collectionPath := strings.Trim(parts[0], "/")
	fieldPath := strings.Trim(parts[1], "/")

	if collectionPath == "" || fieldPath == "" {
		return
	}

	// Navigate to the collection
	collectionParts := strings.Split(collectionPath, "/")
	current := data

	// Process all parts to reach the collection
	for i, part := range collectionParts {
		if part == "" {
			continue
		}

		isLast := i == len(collectionParts)-1

		if _, exists := current[part]; !exists {
			// Path doesn't exist - cannot set field in non-existent collection
			return
		}

		if isLast {
			// This is the collection - apply value to each item
			if collection, ok := current[part].([]interface{}); ok {
				if len(collection) > 0 {
					fm.setFieldInEachItem(collection, fieldPath, value)
				}
			}
			return
		}

		// Not the last part - traverse deeper
		if nextMap, ok := current[part].(map[string]interface{}); ok {
			current = nextMap
		} else {
			// Can't traverse further (not a map)
			return
		}
	}
}

// toPathutilStandardOutput converts runtime output to the lightweight pathutil representation.
func toPathutilStandardOutput(output *output.StandardOutput) *pathutil.StandardOutput {
	if output == nil {
		return nil
	}
	return &pathutil.StandardOutput{
		Meta:   output.Meta,
		Events: output.Events,
		Error:  output.Error,
		Result: output.Result,
	}
}

// setFieldInEachItem sets a field in each item of a collection
func (fm *FieldMapper) setFieldInEachItem(collection []interface{}, fieldPath string, value interface{}) {
	for i, item := range collection {
		if itemMap, ok := item.(map[string]interface{}); ok {
			// Apply value to this item
			fm.setFieldAtPath(itemMap, fieldPath, value)
			collection[i] = itemMap
		}
	}
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
