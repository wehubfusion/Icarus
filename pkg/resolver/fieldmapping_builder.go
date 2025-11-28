package resolver

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// BuildInputParams contains the data required to build unit input from field mappings.
type BuildInputParams struct {
	UnitNodeID    string
	FieldMappings []message.FieldMapping
	SourceResults map[string]*SourceResult
	TriggerData   []byte
}

func buildInputFromMappings(params BuildInputParams) ([]byte, error) {
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

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
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

		if sourceResult == nil || strings.ToLower(sourceResult.Status) != "success" {
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
					needsArray := false
					for _, destEndpoint := range mapping.DestinationEndpoints {
						if strings.Contains(destEndpoint, "//") {
							needsArray = true
							break
						}
					}

					sourceData = extractFromPath(nodeFields, mapping.SourceEndpoint)
					if sourceData == nil && needsArray {
						resultArray := extractFromPath(nodeFields, "/result")
						if arr, isArr := resultArray.([]interface{}); isArr && len(arr) > 0 {
							if strings.Contains(mapping.SourceEndpoint, "//") {
								sourceData = extractFromPath(nodeFields, mapping.SourceEndpoint)
							} else {
								sourcePath := strings.Trim(mapping.SourceEndpoint, "/")
								if sourcePath != "" {
									traversalPath := "/result//" + sourcePath
									sourceData = extractFromPath(nodeFields, traversalPath)
								}
								if sourceData == nil {
									sourceData = resultArray
								}
							}
						}
					}
				}
			}
		}

		if sourceData == nil {
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
				if sourceMap, ok := sourceData.(map[string]interface{}); ok {
					for k, v := range sourceMap {
						inputData[k] = v
					}
				} else {
					inputData["data"] = sourceData
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
		if len(params.TriggerData) > 0 {
			return params.TriggerData, nil
		}
		return []byte("{}"), nil
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
		if valueMap, ok := value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				data[k] = v
			}
		} else {
			data["data"] = value
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

	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return fields
	}

	if strings.Contains(path, "//") {
		return extractWithCollectionTraversal(fields, path)
	}

	if strings.HasPrefix(path, "_meta") ||
		strings.HasPrefix(path, "_events") ||
		strings.HasPrefix(path, "_error") {
		return navigateMap(fields, path)
	}

	if strings.HasPrefix(path, "result/") {
		return navigateMap(fields, path)
	}

	resultPath := "result/" + path
	value := navigateMap(fields, resultPath)
	if value != nil {
		return value
	}

	return navigateMap(fields, path)
}

func extractWithCollectionTraversal(fields map[string]interface{}, path string) interface{} {
	parts := strings.SplitN(path, "//", 2)
	if len(parts) != 2 {
		return extractFromPath(fields, path)
	}

	collectionPath := strings.Trim(parts[0], "/")
	fieldPath := strings.Trim(parts[1], "/")

	if collectionPath == "" || fieldPath == "" {
		return nil
	}

	var collectionPathFull string
	if strings.HasPrefix(collectionPath, "_meta") ||
		strings.HasPrefix(collectionPath, "_events") ||
		strings.HasPrefix(collectionPath, "_error") ||
		strings.HasPrefix(collectionPath, "result/") {
		collectionPathFull = collectionPath
	} else {
		collectionPathFull = "result/" + collectionPath
	}

	collection := navigateMap(fields, collectionPathFull)
	if collection == nil {
		collection = navigateMap(fields, collectionPath)
		if collection == nil {
			return nil
		}
	}

	collectionArray, ok := collection.([]interface{})
	if !ok {
		return nil
	}

	result := make([]interface{}, len(collectionArray))
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
		return nil
	}

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
