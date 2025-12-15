package runtime

import (
	"fmt"
	"strings"
)

// FlattenMap flattens a nested map with nodeId prefix.
// Example: {"name": "david", "age": 19} with nodeId "abc" becomes:
// {"abc-/name": "david", "abc-/age": 19}
func FlattenMap(data map[string]interface{}, nodeId, basePath string) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range data {
		var fullPath string
		if basePath == "" {
			fullPath = "/" + key
		} else {
			fullPath = basePath + "/" + key
		}

		flatKey := nodeId + "-" + fullPath

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := FlattenMap(v, nodeId, fullPath)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = value
		}
	}

	return result
}

// FlattenWithArrayPath flattens a map that came from an array item.
// Uses // notation: {"name": "david"} from array "data" becomes:
// {"nodeId-/data//name": "david"}
func FlattenWithArrayPath(item map[string]interface{}, nodeId, arrayPath string) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range item {
		flatKey := fmt.Sprintf("%s-/%s//%s", nodeId, arrayPath, key)

		switch v := value.(type) {
		case map[string]interface{}:
			// Nested object within array item
			nested := flattenNestedInArrayItem(v, nodeId, arrayPath, key)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = value
		}
	}

	return result
}

// flattenNestedInArrayItem handles nested objects within array items.
func flattenNestedInArrayItem(data map[string]interface{}, nodeId, arrayPath, parentKey string) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range data {
		flatKey := fmt.Sprintf("%s-/%s//%s/%s", nodeId, arrayPath, parentKey, key)

		switch v := value.(type) {
		case map[string]interface{}:
			nested := flattenNestedInArrayItem(v, nodeId, arrayPath, parentKey+"/"+key)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = value
		}
	}

	return result
}

// GenerateOutputKey creates a standardized key for the flat output.
// Example: GenerateOutputKey("node-123", "name") -> "node-123-/name"
func GenerateOutputKey(nodeId, path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return nodeId + "-" + path
}

// GenerateArrayOutputKey creates a key for array item output.
// Example: GenerateArrayOutputKey("node-123", "data", "name") -> "node-123-/data//name"
func GenerateArrayOutputKey(nodeId, arrayPath, fieldPath string) string {
	return fmt.Sprintf("%s-/%s//%s", nodeId, arrayPath, fieldPath)
}

// ParseOutputKey parses a flattened key back to nodeId and path.
// Returns nodeId, path, isArrayItem, error
func ParseOutputKey(key string) (nodeId string, path string, isArrayItem bool, err error) {
	idx := strings.Index(key, "-/")
	if idx == -1 {
		return "", "", false, fmt.Errorf("%w: invalid key format: %s", ErrInvalidInput, key)
	}

	nodeId = key[:idx]
	path = key[idx+1:] // includes leading /
	isArrayItem = strings.Contains(path, "//")

	return nodeId, path, isArrayItem, nil
}

// GetNestedValue gets a value from a nested map using a path.
// Path format: "field" or "nested/field"
func GetNestedValue(data map[string]interface{}, path string) interface{} {
	if path == "" {
		return data
	}

	parts := strings.Split(path, "/")
	current := interface{}(data)

	for _, part := range parts {
		if part == "" {
			continue
		}
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current = m[part]
		if current == nil {
			return nil
		}
	}

	return current
}

// SetNestedValue sets a value at a nested path in the map.
// Creates intermediate maps as needed.
func SetNestedValue(data map[string]interface{}, path string, value interface{}) {
	path = strings.TrimPrefix(path, "/")

	// Handle // in path - use only the part after //
	if idx := strings.Index(path, "//"); idx >= 0 {
		path = path[idx+2:]
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
			if next, ok := current[part].(map[string]interface{}); ok {
				current = next
			} else {
				return // Can't navigate further
			}
		}
	}
}

// ExtractArrayPath extracts the array path from a source endpoint.
// Example: "/data//name" -> "data", "name"
func ExtractArrayPath(endpoint string) (arrayPath, fieldPath string, hasArray bool) {
	endpoint = strings.TrimPrefix(endpoint, "/")

	if idx := strings.Index(endpoint, "//"); idx >= 0 {
		return endpoint[:idx], endpoint[idx+2:], true
	}

	return "", endpoint, false
}

// ExtractFieldFromDestination extracts the final field name from a destination path.
// Example: "/data//deleted" -> "deleted"
// Example: "/field" -> "field"
func ExtractFieldFromDestination(dest string) string {
	dest = strings.TrimPrefix(dest, "/")

	if idx := strings.Index(dest, "//"); idx >= 0 {
		return dest[idx+2:]
	}

	parts := strings.Split(dest, "/")
	return parts[len(parts)-1]
}

// MergeMaps merges source into target, overwriting existing keys.
func MergeMaps(target, source map[string]interface{}) {
	for k, v := range source {
		target[k] = v
	}
}

// CloneMap creates a shallow copy of a map.
func CloneMap(source map[string]interface{}) map[string]interface{} {
	if source == nil {
		return nil
	}
	result := make(map[string]interface{}, len(source))
	for k, v := range source {
		result[k] = v
	}
	return result
}

// DeepCloneMap creates a deep copy of a map.
func DeepCloneMap(source map[string]interface{}) map[string]interface{} {
	if source == nil {
		return nil
	}
	result := make(map[string]interface{}, len(source))
	for k, v := range source {
		switch val := v.(type) {
		case map[string]interface{}:
			result[k] = DeepCloneMap(val)
		case []interface{}:
			result[k] = deepCloneSlice(val)
		default:
			result[k] = v
		}
	}
	return result
}

// deepCloneSlice creates a deep copy of a slice.
func deepCloneSlice(source []interface{}) []interface{} {
	if source == nil {
		return nil
	}
	result := make([]interface{}, len(source))
	for i, v := range source {
		switch val := v.(type) {
		case map[string]interface{}:
			result[i] = DeepCloneMap(val)
		case []interface{}:
			result[i] = deepCloneSlice(val)
		default:
			result[i] = v
		}
	}
	return result
}

// FlattenMapWithIndex flattens a nested map like FlattenMap but injects an index
// into the path before the field. This is used when flattening per-item outputs
// so shared output can reflect indexed items (e.g., "/0/name").
func FlattenMapWithIndex(data map[string]interface{}, nodeId, basePath string, index int) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range data {
		var fullPath string
		// insert index into path
		if basePath == "" {
			fullPath = fmt.Sprintf("/%d/%s", index, key)
		} else {
			fullPath = basePath + fmt.Sprintf("/%d/%s", index, key)
		}

		flatKey := nodeId + "-" + fullPath

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := FlattenMapWithIndex(v, nodeId, fullPath, index)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = value
		}
	}

	return result
}
