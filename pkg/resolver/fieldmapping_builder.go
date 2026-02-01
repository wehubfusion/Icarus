package resolver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// hasFlatKeyData checks if any SourceResult contains raw flat keys
func hasFlatKeyData(sourceResults map[string]*SourceResult) map[string]interface{} {
	for _, result := range sourceResults {
		if len(result.RawFlatKeys) > 0 {
			return result.RawFlatKeys
		}
	}
	return nil
}

// keyInfo holds information about a flat key match
type keyInfo struct {
	fullKey string
	indices []int // Array indices extracted from path
	value   interface{}
}

// extractFromFlatKeys extracts values directly from flat-key format
// Returns extracted data matching the destination structure, or nil if not found
func extractFromFlatKeys(
	flatKeys map[string]interface{},
	sourceNodeID string,
	sourceEndpoint string,
	destEndpoint string,
	iterate bool,
) interface{} {
	prefix := sourceNodeID + "-/"

	// Handle empty source endpoint - extract entire node output
	if sourceEndpoint == "" || sourceEndpoint == "/" {
		// Look for root key "nodeId-/" which contains the full output
		rootKey := sourceNodeID + "-/"
		if rootValue, exists := flatKeys[rootKey]; exists {
			return rootValue
		}
		// Fall through to check for other patterns
	}

	// Check for complete nested structure stored at "nodeId-/path" (without indices)
	// This handles nodes like 71bf0d05 where entire arrays are stored
	fullPathKey := prefix + strings.TrimPrefix(sourceEndpoint, "/")
	if fullPathData, exists := flatKeys[fullPathKey]; exists {
		// Found complete nested structure
		// If destination has collection traversal (//), extract from nested
		if strings.Contains(destEndpoint, "//") {
			return extractFromNestedStructure(fullPathData, sourceEndpoint, destEndpoint)
		}
		// Otherwise return directly
		return fullPathData
	}

	// Collect matching flat keys with their indices
	var matches []keyInfo

	for key, value := range flatKeys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		pathPart := key[len(prefix):]

		// Extract indices from path like "/data[0]/assignments[1]/details/topics[0]/courseName[0]"
		indices := extractArrayIndices(pathPart)

		if len(indices) > 0 {
			matches = append(matches, keyInfo{
				fullKey: key,
				indices: indices,
				value:   value,
			})
		}
	}

	if len(matches) == 0 {
		return nil
	}

	// Handle iterate flag: return just the values array
	if iterate {
		// Find max index to size the result array
		maxIdx := -1
		for _, match := range matches {
			if len(match.indices) > 0 && match.indices[0] > maxIdx {
				maxIdx = match.indices[0]
			}
		}

		if maxIdx < 0 {
			return nil
		}

		// Place values at their correct indices
		result := make([]interface{}, maxIdx+1)
		for _, match := range matches {
			if len(match.indices) > 0 {
				idx := match.indices[0]
				result[idx] = match.value
			}
		}
		return result
	}

	// Build nested structure based on destination path and indices
	destParts := parsePathSegments(destEndpoint)
	return buildStructureFromFlatKeys(matches, destParts)
}

// extractFromNestedStructure extracts values from a complete nested data structure
// Used when flat keys contain full nested arrays instead of indexed flat keys
// Example: "71bf0d05.../data" contains full array, need to extract "/data//chapters"
func extractFromNestedStructure(data interface{}, sourceEndpoint string, destEndpoint string) interface{} {
	if !strings.Contains(destEndpoint, "//") {
		return data
	}

	// Handle multi-level collection traversal recursively
	// "/data//assignments//title" means: for each item in data, for each assignment, get title

	sourcePathClean := strings.Trim(sourceEndpoint, "/")
	destPathClean := strings.TrimPrefix(destEndpoint, "/")

	// Find the first // to split at
	firstDoubleSlash := strings.Index(destPathClean, "//")
	if firstDoubleSlash < 0 {
		return data
	}

	// Split into: collection path + rest
	collectionPath := destPathClean[:firstDoubleSlash]
	restPath := destPathClean[firstDoubleSlash+2:] // Skip the //

	// Navigate to the collection from current data
	var collection []interface{}

	if collectionPath == sourcePathClean || collectionPath == "" {
		// Data IS the collection
		var ok bool
		collection, ok = data.([]interface{})
		if !ok {
			return nil
		}
	} else {
		// Need to navigate to collection
		if dataMap, ok := data.(map[string]interface{}); ok {
			collectionVal := navigateMap(dataMap, collectionPath)
			collection, ok = collectionVal.([]interface{})
			if !ok {
				return nil
			}
		} else {
			return nil
		}
	}

	// Check if there are more // levels in restPath
	if strings.Contains(restPath, "//") {
		// Recursive case: more collection levels to traverse
		result := make([]interface{}, len(collection))
		for i, item := range collection {
			// Recursively extract from each item
			subResult := extractFromNestedStructure(item, "", "/"+restPath)
			result[i] = subResult
		}
		return result
	}

	// Base case: extract final field from each item
	result := make([]interface{}, len(collection))
	for i, item := range collection {
		if itemMap, ok := item.(map[string]interface{}); ok {
			result[i] = navigateMap(itemMap, restPath)
		}
	}

	return result
}

// extractArrayIndices parses indices from a flat key path
// "/data[0]/assignments[1]/details/topics[0]/courseName[0]" -> [0, 1, 0]
func extractArrayIndices(path string) []int {
	var indices []int
	segments := strings.Split(path, "/")

	for _, segment := range segments {
		if segment == "" {
			continue
		}

		// Find array index in segment like "field[123]"
		bracketIdx := strings.IndexByte(segment, '[')
		if bracketIdx > 0 {
			closeBracket := strings.IndexByte(segment, ']')
			if closeBracket > bracketIdx {
				idxStr := segment[bracketIdx+1 : closeBracket]
				if idx, err := strconv.Atoi(idxStr); err == nil {
					indices = append(indices, idx)
				}
			}
		}
	}

	return indices
}

// parsePathSegments splits a path into segments, preserving empty strings for //
// "/data//assignments//details/topics//courseName" -> ["data", "", "assignments", "", "details", "topics", "", "courseName"]
func parsePathSegments(path string) []string {
	path = strings.TrimPrefix(path, "/")
	return strings.Split(path, "/")
}

// buildStructureFromFlatKeys constructs the data structure that setFieldAtPath expects
// For a destination like "/data//nameUpper", this should return an array ["ALEX", "JORDAN"]
// For a destination like "/data//assignments//details/topics//courseUpper", this needs more complex nesting
func buildStructureFromFlatKeys(matches []keyInfo, destParts []string) interface{} {
	// Count array levels in destination (number of empty strings marking //)
	arrayLevels := 0
	arrayPositions := []int{} // Track positions of array markers
	for i, part := range destParts {
		if part == "" {
			arrayLevels++
			arrayPositions = append(arrayPositions, i)
		}
	}

	if arrayLevels == 0 {
		// No arrays - return single value
		if len(matches) > 0 {
			return matches[0].value
		}
		return nil
	}

	// For a single array level like "/data//nameUpper"
	// destParts = ["data", "", "nameUpper"]
	// We want to return an array indexed by the first index in matches
	if arrayLevels == 1 {
		// Build array indexed by first index from matches
		maxIdx := -1
		for _, match := range matches {
			if len(match.indices) > 0 && match.indices[0] > maxIdx {
				maxIdx = match.indices[0]
			}
		}

		if maxIdx < 0 {
			return nil
		}

		result := make([]interface{}, maxIdx+1)
		for _, match := range matches {
			if len(match.indices) > 0 {
				idx := match.indices[0]
				result[idx] = match.value
			}
		}
		return result
	}

	// For multiple array levels like "/data//assignments//details/topics//courseUpper"
	// We need to build nested structure
	// Group by first index
	indexGroups := make(map[int][]keyInfo)
	maxIdx := -1
	for _, match := range matches {
		if len(match.indices) > 0 {
			firstIdx := match.indices[0]
			if firstIdx > maxIdx {
				maxIdx = firstIdx
			}
			// Strip the first index for recursive call
			strippedMatch := keyInfo{
				fullKey: match.fullKey,
				indices: match.indices[1:],
				value:   match.value,
			}
			indexGroups[firstIdx] = append(indexGroups[firstIdx], strippedMatch)
		}
	}

	if maxIdx < 0 {
		return nil
	}

	// Build array with nested structures
	result := make([]interface{}, maxIdx+1)

	// Remove first array level from destParts for recursive processing
	// Find position of first "" and skip it plus the part before it
	firstArrayPos := arrayPositions[0]
	remainingParts := destParts[firstArrayPos+1:]

	for idx, group := range indexGroups {
		if len(group) == 0 {
			continue
		}

		// Recursively build structure for remaining parts
		if len(remainingParts) == 1 && remainingParts[0] != "" {
			// Simple field at this level - just use first match value
			result[idx] = group[0].value
		} else {
			// More complex structure - recurse
			subResult := buildStructureFromFlatKeys(group, remainingParts)
			result[idx] = subResult
		}
	}

	return result
}

// setNestedValueWithIndices sets a value in nested structure using destination path and indices
func setNestedValueWithIndices(
	root map[string]interface{},
	destParts []string,
	indices []int,
	value interface{},
) {
	current := root
	indicesIdx := 0

	for i := 0; i < len(destParts); i++ {
		part := destParts[i]

		if part == "" {
			// Array boundary marker - skip
			continue
		}

		// Check if next part is empty (array boundary)
		isArray := (i+1 < len(destParts) && destParts[i+1] == "")

		if isArray {
			// This field should be an array
			if _, exists := current[part]; !exists {
				current[part] = make([]interface{}, 0)
			}

			arr, ok := current[part].([]interface{})
			if !ok {
				arr = make([]interface{}, 0)
				current[part] = arr
			}

			// Get target index from indices array
			if indicesIdx >= len(indices) {
				return
			}
			targetIdx := indices[indicesIdx]
			indicesIdx++

			// Ensure array is large enough
			for len(arr) <= targetIdx {
				arr = append(arr, make(map[string]interface{}))
			}
			current[part] = arr

			// Move into array item
			if arr[targetIdx] == nil {
				arr[targetIdx] = make(map[string]interface{})
			}
			itemMap, ok := arr[targetIdx].(map[string]interface{})
			if !ok {
				itemMap = make(map[string]interface{})
				arr[targetIdx] = itemMap
			}
			current = itemMap

			// Skip the empty string marker
			i++
		} else {
			// Regular object field
			if i == len(destParts)-1 {
				// Last part - set value
				current[part] = value
				return
			} else {
				// Intermediate object
				if _, exists := current[part]; !exists {
					current[part] = make(map[string]interface{})
				}
				nextMap, ok := current[part].(map[string]interface{})
				if !ok {
					nextMap = make(map[string]interface{})
					current[part] = nextMap
				}
				current = nextMap
			}
		}
	}
}

// unwrapSingleFieldObject extracts scalar values from single-key wrapper objects
// {"name": "ALEX"} -> "ALEX"
// {"isHigher18": true} -> true
func unwrapSingleFieldObject(v interface{}) interface{} {
	m, ok := v.(map[string]interface{})
	if !ok || len(m) != 1 {
		return v
	}

	// Single-key object - return the value
	for _, val := range m {
		return val
	}
	return v
}

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

	// Check if we have flat-key data available for direct extraction
	flatKeyData := hasFlatKeyData(params.SourceResults)

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

		// Try flat-key extraction first if available
		if flatKeyData != nil {
			result := extractFromFlatKeys(
				flatKeyData,
				mapping.SourceNodeID,
				mapping.SourceEndpoint,
				mapping.DestinationEndpoints[0],
				mapping.Iterate,
			)

			if result != nil {
				// Success - apply to all destination endpoints
				for _, destEndpoint := range mapping.DestinationEndpoints {
					setFieldAtPath(inputData, destEndpoint, result)
				}
				continue // Skip fallback
			}
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
					// Unwrap single-field objects before setting
					unwrappedValue := unwrapSingleFieldObject(valueArray[i])
					setFieldAtPath(itemMap, fieldPath, unwrappedValue)
					collection[i] = itemMap
				}
			}
		} else {
			for i, item := range collection {
				if itemMap, ok := item.(map[string]interface{}); ok {
					// Unwrap single-field objects before setting
					unwrappedValue := unwrapSingleFieldObject(valueArray[i])
					setFieldAtPath(itemMap, fieldPath, unwrappedValue)
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
