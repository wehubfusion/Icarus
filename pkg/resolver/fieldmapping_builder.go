package resolver

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// getMapKeys returns the keys of a map for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// getSourceNodeIDs returns the node IDs from source results map
func getSourceNodeIDs(sourceResults map[string]*SourceResult) []string {
	ids := make([]string, 0, len(sourceResults))
	for k := range sourceResults {
		ids = append(ids, k)
	}
	return ids
}

// BuildInputParams contains the data required to build unit input from field mappings.
type BuildInputParams struct {
	UnitNodeID    string
	FieldMappings []message.FieldMapping
	SourceResults map[string]*SourceResult
	TriggerData   []byte
}

// Global cache for array extractions to avoid re-traversing large arrays
var extractionCache = struct {
	cache map[string]interface{}
}{
	cache: make(map[string]interface{}),
}

func buildInputFromMappings(params BuildInputParams) ([]byte, error) {
	// Clear extraction cache for this build
	extractionCache.cache = make(map[string]interface{})

	if len(params.FieldMappings) == 0 {
		if len(params.TriggerData) > 0 {
			return params.TriggerData, nil
		}
		return []byte("{}"), nil
	}

	inputData := make(map[string]interface{})
	arraySources := make(map[string]int)
	collectionSources := make(map[string]interface{})

	type sourceInfo struct {
		count                int
		sourceCollectionPath string
	}
	collectionPathSources := make(map[string]map[string]*sourceInfo)

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		var sourceCollectionPath string
		if strings.Contains(mapping.SourceEndpoint, "//") {
			parts := strings.SplitN(mapping.SourceEndpoint, "//", 2)
			if len(parts) == 2 {
				sourceCollectionPath = strings.Trim(parts[0], "/")
			}
		}

		for _, destEndpoint := range mapping.DestinationEndpoints {
			if strings.Contains(destEndpoint, "//") {
				parts := strings.SplitN(destEndpoint, "//", 2)
				if len(parts) == 2 {
					collectionPath := strings.Trim(parts[0], "/")
					if collectionPath != "" {
						if collectionPathSources[collectionPath] == nil {
							collectionPathSources[collectionPath] = make(map[string]*sourceInfo)
						}

						if info := collectionPathSources[collectionPath][mapping.SourceNodeID]; info == nil {
							collectionPathSources[collectionPath][mapping.SourceNodeID] = &sourceInfo{
								count:                1,
								sourceCollectionPath: sourceCollectionPath,
							}
						} else {
							info.count++
							if info.sourceCollectionPath == "" && sourceCollectionPath != "" {
								info.sourceCollectionPath = sourceCollectionPath
							}
						}
					}
				}
			}
		}
	}

	for collectionPath, sourceInfos := range collectionPathSources {
		var primarySourceID string
		var primarySourceCollectionPath string
		maxCount := 0
		for sourceID, info := range sourceInfos {
			if info.count > maxCount {
				maxCount = info.count
				primarySourceID = sourceID
				primarySourceCollectionPath = info.sourceCollectionPath
			}
		}

		if primarySourceID == "" {
			continue
		}

		var sourceResult *SourceResult
		if result, exists := params.SourceResults[primarySourceID]; exists {
			sourceResult = result
		} else {
			for _, potential := range params.SourceResults {
				if potential.ProjectedFields != nil {
					if _, hasNode := potential.ProjectedFields[primarySourceID]; hasNode {
						sourceResult = potential
						break
					}
				}
			}
		}

		if sourceResult != nil && sourceResult.ProjectedFields != nil {
			if nodeFields, hasNode := sourceResult.ProjectedFields[primarySourceID]; hasNode {
				sourceCollectionPath := primarySourceCollectionPath
				if sourceCollectionPath == "" {
					sourceCollectionPath = "data"
				}
				arrayField := extractFromPath(nodeFields, "/"+sourceCollectionPath)
				if arr, isArr := arrayField.([]interface{}); isArr {
					collectionSources[collectionPath] = arr
				}
			}
		}
	}

	for collectionPath, sourceArray := range collectionSources {
		setFieldAtPath(inputData, collectionPath, sourceArray)
	}

	// Track failures for detailed error messages
	failedMappings := make([]string, 0)

	// OPTIMIZATION: Group field mappings by source node and collection path for batch extraction
	type batchKey struct {
		sourceNodeID     string
		collectionPath   string
		sourceCollection string
	}
	batchedExtractions := make(map[batchKey][]struct {
		mapping   message.FieldMapping
		fieldPath string
	})

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		// Check if this is a collection traversal mapping (has //)
		if strings.Contains(mapping.SourceEndpoint, "//") {
			parts := strings.SplitN(mapping.SourceEndpoint, "//", 2)
			if len(parts) == 2 {
				sourceCollectionPath := strings.Trim(parts[0], "/")
				fieldPath := strings.Trim(parts[1], "/")

				// Determine destination collection path
				for _, destEndpoint := range mapping.DestinationEndpoints {
					if strings.Contains(destEndpoint, "//") {
						destParts := strings.SplitN(destEndpoint, "//", 2)
						if len(destParts) == 2 {
							destCollectionPath := strings.Trim(destParts[0], "/")
							if destCollectionPath != "" {
								key := batchKey{
									sourceNodeID:     mapping.SourceNodeID,
									collectionPath:   destCollectionPath,
									sourceCollection: sourceCollectionPath,
								}
								batchedExtractions[key] = append(batchedExtractions[key], struct {
									mapping   message.FieldMapping
									fieldPath string
								}{mapping, fieldPath})
								break
							}
						}
					}
				}
			}
		}
	}

	// Process batched extractions first
	// Use (sourceNodeID, sourceEndpoint) as key - pointer comparison fails because range reuses loop vars
	type mappingKey struct {
		sourceNodeID   string
		sourceEndpoint string
	}
	batchProcessedMappings := make(map[mappingKey]bool)

	for key, batch := range batchedExtractions {
		if len(batch) <= 1 {
			continue // No benefit from batching
		}

		// Get source result
		var sourceResult *SourceResult
		if result, exists := params.SourceResults[key.sourceNodeID]; exists {
			sourceResult = result
		}

		if sourceResult == nil || sourceResult.ProjectedFields == nil {
			continue
		}

		nodeFields, hasNode := sourceResult.ProjectedFields[key.sourceNodeID]
		if !hasNode {
			continue
		}

		// Get the source collection array
		sourceCollPath := key.sourceCollection
		if sourceCollPath == "" {
			sourceCollPath = "data"
		}
		collection := navigateMap(nodeFields, sourceCollPath)
		if collection == nil {
			continue
		}

		collectionArray, ok := collection.([]interface{})
		if !ok {
			continue
		}

		arrayLen := len(collectionArray)

		// Extract all fields in ONE pass
		fieldResults := make(map[string][]interface{})
		for _, b := range batch {
			fieldResults[b.fieldPath] = make([]interface{}, arrayLen)
		}

		for i, item := range collectionArray {
			if itemMap, ok := item.(map[string]interface{}); ok {
				for _, b := range batch {
					fieldValue := navigateMap(itemMap, b.fieldPath)
					fieldResults[b.fieldPath][i] = fieldValue
				}
			}
		}

		// Now set the results for each mapping and mark as processed
		for _, b := range batch {
			result := fieldResults[b.fieldPath]
			for _, destEndpoint := range b.mapping.DestinationEndpoints {
				if strings.Contains(destEndpoint, "//") {
					setFieldAtPath(inputData, destEndpoint, result)
				}
			}
			batchProcessedMappings[mappingKey{
				sourceNodeID:   b.mapping.SourceNodeID,
				sourceEndpoint: b.mapping.SourceEndpoint,
			}] = true
		}
	}

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		// Skip if already processed in batch
		if batchProcessedMappings[mappingKey{
			sourceNodeID:   mapping.SourceNodeID,
			sourceEndpoint: mapping.SourceEndpoint,
		}] {
			continue
		}

		var sourceResult *SourceResult

		if result, exists := params.SourceResults[mapping.SourceNodeID]; exists {
			sourceResult = result
		} else {
			for _, potential := range params.SourceResults {
				if potential.ProjectedFields != nil {
					if _, hasNode := potential.ProjectedFields[mapping.SourceNodeID]; hasNode {
						sourceResult = potential
						break
					}
				}
			}
		}

		if sourceResult == nil {
			failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' not found in source results (available nodes: %v)",
				mapping.SourceNodeID, getSourceNodeIDs(params.SourceResults)))
			continue
		}

		if strings.ToLower(sourceResult.Status) != "success" {
			failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' has status '%s' (expected 'success')",
				mapping.SourceNodeID, sourceResult.Status))
			continue
		}

		var sourceData interface{}
		var hasIterationContext bool
		var arrayLength int
		var arrayPath string

		if sourceResult.ProjectedFields != nil {
			var nodeFields map[string]interface{}
			var hasNode bool

			if nodeFields, hasNode = sourceResult.ProjectedFields[mapping.SourceNodeID]; !hasNode {
				if sourceResult.NodeID == mapping.SourceNodeID {
					if selfFields, ok := sourceResult.ProjectedFields[mapping.SourceNodeID]; ok {
						nodeFields = selfFields
						hasNode = true
					} else if allFields, ok := sourceResult.ProjectedFields[""]; ok {
						nodeFields = allFields
						hasNode = true
					} else if len(sourceResult.ProjectedFields) == 1 {
						for _, fields := range sourceResult.ProjectedFields {
							nodeFields = fields
							hasNode = true
							break
						}
					}
				}
			}

			if hasNode && nodeFields != nil {
				if sourceResult.IterationMetadata != nil {
					if iterCtx, exists := sourceResult.IterationMetadata[mapping.SourceNodeID]; exists && iterCtx != nil {
						hasIterationContext = iterCtx.IsArray
						arrayLength = iterCtx.ArrayLength
						arrayPath = iterCtx.ArrayPath
						if arrayPath != "" {
							sourceData = extractFromPath(nodeFields, arrayPath)
						}
					}
				}

				if sourceData == nil {
					sourceData = extractFromPath(nodeFields, mapping.SourceEndpoint)

					// Fallback: If sourceData is nil, try to find the field inside /data or /result array
					// This handles embedded processors that output arrays stored under "data" key
					// when the field mapping expects /field but data is at /data//field
					if sourceData == nil {
						sourcePath := strings.Trim(mapping.SourceEndpoint, "/")
						if sourcePath != "" && !strings.Contains(sourcePath, "/") {
							// Simple field path like /auth - try to extract from /data array first
							dataArray := extractFromPath(nodeFields, "/data")
							if arr, isArr := dataArray.([]interface{}); isArr && len(arr) > 0 {
								// Check if first item has the field we're looking for
								if firstItem, ok := arr[0].(map[string]interface{}); ok {
									if _, hasField := firstItem[sourcePath]; hasField {
										traversalPath := "/data//" + sourcePath
										sourceData = extractFromPath(nodeFields, traversalPath)
									}
								}
							}

							// Try /result array if /data didn't work
							if sourceData == nil {
								resultArray := extractFromPath(nodeFields, "/result")
								if arr, isArr := resultArray.([]interface{}); isArr && len(arr) > 0 {
									if firstItem, ok := arr[0].(map[string]interface{}); ok {
										if _, hasField := firstItem[sourcePath]; hasField {
											traversalPath := "/result//" + sourcePath
											sourceData = extractFromPath(nodeFields, traversalPath)
										}
									}
								}
							}
						} else if strings.Contains(mapping.SourceEndpoint, "//") {
							// Already a traversal path - try as-is
							sourceData = extractFromPath(nodeFields, mapping.SourceEndpoint)
						}
					}

					// Special case: If sourceData is an array of objects where each object has
					// a single field matching the sourcePath, extract those values.
					// This handles /data returning [{data:0}, {data:0}] when we want [0, 0]
					if sourceData != nil {
						sourcePath := strings.Trim(mapping.SourceEndpoint, "/")
						if arr, isArr := sourceData.([]interface{}); isArr && len(arr) > 0 {
							if firstItem, ok := arr[0].(map[string]interface{}); ok {
								// Check if items have a field matching the path we looked for
								if val, hasField := firstItem[sourcePath]; hasField {
									// Extract the nested field from each item
									extracted := make([]interface{}, len(arr))
									allExtracted := true
									for i, item := range arr {
										if itemMap, ok := item.(map[string]interface{}); ok {
											if v, exists := itemMap[sourcePath]; exists {
												extracted[i] = v
											} else {
												allExtracted = false
												break
											}
										} else {
											// Item is not a map, might be the value itself
											extracted[i] = item
										}
									}
									// Only use extracted if we successfully got all values
									// and the extracted values are different (not the same objects)
									if allExtracted && val != arr[0] {
										sourceData = extracted
									}
								}
							}
						}
					}
				}
			} else {
				// Node fields not found
				availableNodeIDs := make([]string, 0, len(sourceResult.ProjectedFields))
				for k := range sourceResult.ProjectedFields {
					availableNodeIDs = append(availableNodeIDs, k)
				}
				failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' has no projected fields (available node IDs in projected fields: %v)",
					mapping.SourceNodeID, availableNodeIDs))
			}
		} else {
			// Source result has no projected fields
			failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' result has no projected fields",
				mapping.SourceNodeID))
		}

		if sourceData == nil {
			// Track the failure with available field keys
			fieldKeys := []string{}
			if sourceResult.ProjectedFields != nil {
				if nodeFields, hasNode := sourceResult.ProjectedFields[mapping.SourceNodeID]; hasNode {
					fieldKeys = getMapKeys(nodeFields)
				}
			}
			failedMappings = append(failedMappings, fmt.Sprintf("failed to extract '%s' from source node '%s' (available fields: %v)",
				mapping.SourceEndpoint, mapping.SourceNodeID, fieldKeys))
			continue
		}

		if mapping.Iterate && hasIterationContext && arrayLength > 0 {
			if !isArray(sourceData) {
				continue
			}
			if existingLength, exists := arraySources[mapping.SourceNodeID]; exists {
				if existingLength != arrayLength {
					return nil, fmt.Errorf("array length mismatch for source %s", mapping.SourceNodeID)
				}
			} else {
				arraySources[mapping.SourceNodeID] = arrayLength
			}
		} else if mapping.Iterate && isArray(sourceData) {
			// Legacy behaviour: allow iterate flag without metadata
		}

		for _, destEndpoint := range mapping.DestinationEndpoints {
			if destEndpoint == "" {
				// Empty destination: only merge at root when value is a map
				if sourceMap, ok := sourceData.(map[string]interface{}); ok {
					for k, v := range sourceMap {
						inputData[k] = v
					}
				}
			} else {
				setFieldAtPath(inputData, destEndpoint, sourceData)
			}
		}
	}

	if len(arraySources) > 1 {
		var expectedLength int
		var firstSource string
		mismatch := make([]string, 0)
		for sourceID, length := range arraySources {
			if firstSource == "" {
				firstSource = sourceID
				expectedLength = length
			} else if length != expectedLength {
				mismatch = append(mismatch,
					fmt.Sprintf("%s has length %d (expected %d from %s)",
						sourceID, length, expectedLength, firstSource))
			}
		}

		if len(mismatch) > 0 {
			return nil, fmt.Errorf("array length mismatch in iterate sources for unit %s: %s",
				params.UnitNodeID, strings.Join(mismatch, "; "))
		}
	}

	if len(inputData) == 0 {
		// No field mappings succeeded - return detailed error instead of falling back to trigger data
		if len(failedMappings) > 0 {
			return nil, fmt.Errorf("field mapping resolution failed: no data could be extracted. Failures: %s",
				strings.Join(failedMappings, "; "))
		}
		// No field mappings at all
		if len(params.FieldMappings) == 0 {
			return []byte("{}"), nil
		}
		// All mappings were event triggers
		return nil, fmt.Errorf("field mapping resolution failed: no non-event-trigger field mappings were provided")
	}

	result, err := json.Marshal(inputData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mapped input: %w", err)
	}

	return result, nil
}

func setFieldAtPath(data map[string]interface{}, path string, value interface{}) {
	if path == "" {
		return
	}

	path = strings.TrimPrefix(path, "/")
	if path == "" {
		// Only merge at root when value is a map; no hardcoded key
		if valueMap, ok := value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				data[k] = v
			}
		}
		return
	}

	if strings.Contains(path, "//") {
		setFieldAtPathWithCollectionTraversal(data, path, value)
		return
	}

	parts := strings.Split(path, "/")
	current := data

	for i, part := range parts {
		if part == "" {
			continue
		}

		if i == len(parts)-1 {
			current[part] = value
		} else {
			if _, exists := current[part]; !exists {
				current[part] = make(map[string]interface{})
			}
			if nextMap, ok := current[part].(map[string]interface{}); ok {
				current = nextMap
			} else {
				return
			}
		}
	}
}

func setFieldAtPathWithCollectionTraversal(data map[string]interface{}, path string, value interface{}) {
	parts := strings.SplitN(path, "//", 2)
	if len(parts) != 2 {
		setFieldAtPath(data, path, value)
		return
	}

	collectionPath := strings.Trim(parts[0], "/")
	fieldPath := strings.Trim(parts[1], "/")

	if collectionPath == "" || fieldPath == "" {
		return
	}

	collectionParts := strings.Split(collectionPath, "/")
	current := data

	for i, part := range collectionParts {
		if part == "" {
			continue
		}
		isLast := i == len(collectionParts)-1

		if _, exists := current[part]; !exists {
			return
		}

		if isLast {
			if collection, ok := current[part].([]interface{}); ok {
				if len(collection) > 0 {
					setFieldInEachItem(collection, fieldPath, value)
				}
			}
			return
		}

		if nextMap, ok := current[part].(map[string]interface{}); ok {
			current = nextMap
		} else {
			return
		}
	}
}

func setFieldInEachItem(collection []interface{}, fieldPath string, value interface{}) {
	if valueArray, ok := value.([]interface{}); ok && len(valueArray) > 0 {
		if len(valueArray) != len(collection) {
			minLen := len(collection)
			if len(valueArray) < minLen {
				minLen = len(valueArray)
			}
			for i := 0; i < minLen; i++ {
				if itemMap, ok := collection[i].(map[string]interface{}); ok {
					setFieldAtPath(itemMap, fieldPath, valueArray[i])
					collection[i] = itemMap
				}
			}
		} else {
			for i, item := range collection {
				if itemMap, ok := item.(map[string]interface{}); ok {
					setFieldAtPath(itemMap, fieldPath, valueArray[i])
					collection[i] = itemMap
				}
			}
		}
	} else {
		for i, item := range collection {
			if itemMap, ok := item.(map[string]interface{}); ok {
				setFieldAtPath(itemMap, fieldPath, value)
				collection[i] = itemMap
			}
		}
	}
}

func isArray(value interface{}) bool {
	switch value.(type) {
	case []interface{}, []string, []int, []float64, []bool:
		return true
	default:
		return false
	}
}

func extractFromPath(fields map[string]interface{}, path string) interface{} {
	if fields == nil {
		return nil
	}

	originalPath := path
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return fields
	}

	if strings.Contains(path, "//") {
		return extractWithCollectionTraversal(fields, path)
	}

	// First, try direct key lookup (for projected fields stored with leading slash keys, e.g., "/payload")
	// This handles the case where the map key is exactly the path (with or without leading slash)
	if value, exists := fields[originalPath]; exists {
		return value
	}
	if originalPath != path {
		if value, exists := fields[path]; exists {
			return value
		}
	}

	// Special namespaces are accessed directly
	if strings.HasPrefix(path, "_meta") ||
		strings.HasPrefix(path, "_events") ||
		strings.HasPrefix(path, "_error") {
		return navigateMap(fields, path)
	}

	// Data is spread at root level by sourceResultsFromResultFile()
	// Field mappings like /data//hire_date find "data" directly at root
	return navigateMap(fields, path)
}

func extractWithCollectionTraversal(fields map[string]interface{}, path string) interface{} {
	// Check cache first to avoid re-traversing large arrays
	// Use path only as key since fields map is the same ProjectedFields map for all extractions in one build
	cacheKey := path
	if cached, exists := extractionCache.cache[cacheKey]; exists {
		return cached
	}

	parts := strings.SplitN(path, "//", 2)
	if len(parts) != 2 {
		return extractFromPath(fields, path)
	}

	collectionPath := strings.Trim(parts[0], "/")
	fieldPath := strings.Trim(parts[1], "/")

	if collectionPath == "" || fieldPath == "" {
		return nil
	}

	// Data is spread at root level - access collection directly
	// For /data//Assignment_Category: collectionPath="data", fieldPath="Assignment_Category"
	collection := navigateMap(fields, collectionPath)
	if collection == nil {
		return nil
	}

	collectionArray, ok := collection.([]interface{})
	if !ok {
		return nil
	}

	arrayLen := len(collectionArray)

	result := make([]interface{}, arrayLen)
	hasValues := false
	for i, item := range collectionArray {
		if itemMap, ok := item.(map[string]interface{}); ok {
			fieldValue := navigateMap(itemMap, fieldPath)
			if fieldValue != nil {
				result[i] = fieldValue
				hasValues = true
			}
		}
	}

	if !hasValues {
		extractionCache.cache[cacheKey] = nil
		return nil
	}

	// Cache the result to avoid reprocessing for subsequent field mappings
	extractionCache.cache[cacheKey] = result
	return result
}

func navigateMap(data map[string]interface{}, path string) interface{} {
	if data == nil {
		return nil
	}

	parts := strings.Split(path, "/")
	current := interface{}(data)

	for _, part := range parts {
		if part == "" {
			continue
		}

		if m, ok := current.(map[string]interface{}); ok {
			var exists bool
			current, exists = m[part]
			if !exists {
				return nil
			}
			continue
		}
		return nil
	}

	return current
}
