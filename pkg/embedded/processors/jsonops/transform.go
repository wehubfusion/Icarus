package jsonops

import (
	"encoding/json"
	"fmt"

	"github.com/tidwall/sjson"
)

// transform performs transformations on JSON data
// Supports set, delete, and merge operations
func transform(inputJSON []byte, params TransformParams) ([]byte, error) {
	// Validate input JSON
	if !isValidJSON(inputJSON) {
		return nil, &TransformError{
			Type:    params.Type,
			Message: "input is not valid JSON",
		}
	}

	switch params.Type {
	case "set":
		return transformSet(inputJSON, params)
	case "delete":
		return transformDelete(inputJSON, params)
	case "merge":
		return transformMerge(inputJSON, params)
	default:
		return nil, &TransformError{
			Type:    params.Type,
			Message: fmt.Sprintf("unknown transform type: %s", params.Type),
		}
	}
}

// transformSet sets a value at a specific path
func transformSet(inputJSON []byte, params TransformParams) ([]byte, error) {
	normalizedPath := normalizePath(params.Path)

	// Convert value to JSON-compatible format
	var valueToSet interface{}
	switch v := params.Value.(type) {
	case string, int, int64, float64, bool, nil:
		valueToSet = v
	case map[string]interface{}, []interface{}:
		valueToSet = v
	default:
		// Try to marshal and unmarshal to ensure JSON compatibility
		data, err := json.Marshal(v)
		if err != nil {
			return nil, &TransformError{
				Type:    "set",
				Path:    params.Path,
				Message: "value is not JSON-serializable",
				Cause:   err,
			}
		}
		if err := json.Unmarshal(data, &valueToSet); err != nil {
			return nil, &TransformError{
				Type:    "set",
				Path:    params.Path,
				Message: "value cannot be deserialized",
				Cause:   err,
			}
		}
	}

	// Use sjson to set the value
	result, err := sjson.SetBytes(inputJSON, normalizedPath, valueToSet)
	if err != nil {
		return nil, &TransformError{
			Type:    "set",
			Path:    params.Path,
			Message: "failed to set value",
			Cause:   err,
		}
	}

	return result, nil
}

// transformDelete deletes values at specified paths
func transformDelete(inputJSON []byte, params TransformParams) ([]byte, error) {
	result := inputJSON

	// If single path is provided
	if params.Path != "" {
		normalizedPath := normalizePath(params.Path)
		var err error
		result, err = sjson.DeleteBytes(result, normalizedPath)
		if err != nil {
			return nil, &TransformError{
				Type:    "delete",
				Path:    params.Path,
				Message: "failed to delete path",
				Cause:   err,
			}
		}
	}

	// If multiple paths are provided
	if len(params.Paths) > 0 {
		for _, path := range params.Paths {
			normalizedPath := normalizePath(path)
			var err error
			result, err = sjson.DeleteBytes(result, normalizedPath)
			if err != nil {
				return nil, &TransformError{
					Type:    "delete",
					Path:    path,
					Message: "failed to delete path",
					Cause:   err,
				}
			}
		}
	}

	return result, nil
}

// transformMerge merges two JSON objects
func transformMerge(inputJSON []byte, params TransformParams) ([]byte, error) {
	// Parse input JSON into map
	var inputMap map[string]interface{}
	if err := json.Unmarshal(inputJSON, &inputMap); err != nil {
		return nil, &TransformError{
			Type:    "merge",
			Message: "input is not a JSON object",
			Cause:   err,
		}
	}

	// Parse merge data into map
	var mergeMap map[string]interface{}
	if err := json.Unmarshal(params.MergeData, &mergeMap); err != nil {
		return nil, &TransformError{
			Type:    "merge",
			Message: "merge_data is not a valid JSON object",
			Cause:   err,
		}
	}

	// Determine merge depth
	deepMerge := true // default to deep merge
	if params.DeepMerge != nil {
		deepMerge = *params.DeepMerge
	}

	// Perform merge
	merged := mergeMaps(inputMap, mergeMap, deepMerge)

	// Marshal result
	result, err := json.Marshal(merged)
	if err != nil {
		return nil, &TransformError{
			Type:    "merge",
			Message: "failed to marshal merged result",
			Cause:   err,
		}
	}

	return result, nil
}
