package jsonops

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

// normalizePath converts slash-separated paths to dot-separated gjson/sjson format
// Example: "/data/user/name" -> "data.user.name"
// Example: "data.user.name" -> "data.user.name"
// Also converts * wildcards to # for gjson compatibility
func normalizePath(path string) string {
	// If path starts with /, convert to dot notation
	if strings.HasPrefix(path, "/") {
		// Remove leading slash and convert remaining slashes to dots
		path = strings.TrimPrefix(path, "/")
		path = strings.ReplaceAll(path, "/", ".")
	}

	// Convert * wildcard to # for gjson (gjson uses # for array iteration)
	path = strings.ReplaceAll(path, "*", "#")

	return path
}

// getValue extracts a value from JSON at the given path
// Returns the raw gjson.Result for further processing
func getValue(jsonData []byte, path string) gjson.Result {
	normalizedPath := normalizePath(path)
	return gjson.GetBytes(jsonData, normalizedPath)
}

// isValidJSON checks if the provided data is valid JSON
func isValidJSON(data []byte) bool {
	var js interface{}
	return json.Unmarshal(data, &js) == nil
}

// mergeMaps performs a deep merge of two maps
// Values from source override values in destination
func mergeMaps(dest, source map[string]interface{}, deep bool) map[string]interface{} {
	if dest == nil {
		dest = make(map[string]interface{})
	}

	for key, sourceVal := range source {
		if destVal, exists := dest[key]; exists && deep {
			// If both values are maps, merge them recursively
			if destMap, destIsMap := destVal.(map[string]interface{}); destIsMap {
				if sourceMap, sourceIsMap := sourceVal.(map[string]interface{}); sourceIsMap {
					dest[key] = mergeMaps(destMap, sourceMap, deep)
					continue
				}
			}
		}
		// Otherwise, override with source value
		dest[key] = sourceVal
	}

	return dest
}

// convertToMap converts interface{} to map[string]interface{} if possible
func convertToMap(data interface{}) (map[string]interface{}, bool) {
	if m, ok := data.(map[string]interface{}); ok {
		return m, true
	}
	return nil, false
}

// isPathWildcard checks if a path contains wildcard characters
func isPathWildcard(path string) bool {
	return strings.Contains(path, "*") || strings.Contains(path, "#")
}
