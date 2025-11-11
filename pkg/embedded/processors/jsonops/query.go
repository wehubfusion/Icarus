package jsonops

import (
	"encoding/json"

	"github.com/tidwall/gjson"
)

// query queries JSON using path notation with support for wildcards
// Returns matching results as an array or single value
func query(inputJSON []byte, params QueryParams) (interface{}, error) {
	// Validate input JSON
	if !isValidJSON(inputJSON) {
		return nil, &QueryError{
			Query:   params.Path,
			Message: "input is not valid JSON",
		}
	}

	if params.Multiple {
		// Query multiple paths
		return queryMultiple(inputJSON, params.Paths)
	}

	// Query single path
	return querySingle(inputJSON, params.Path)
}

// querySingle queries a single path and returns the result
func querySingle(inputJSON []byte, path string) (interface{}, error) {
	normalizedPath := normalizePath(path)

	// Use gjson to query the path
	result := gjson.GetBytes(inputJSON, normalizedPath)

	// For wildcard queries, gjson returns an array (possibly empty)
	// We don't treat empty array as non-existent
	if isPathWildcard(normalizedPath) {
		// For wildcard queries, return as array
		if result.IsArray() {
			return result.Value(), nil
		}
		// If result exists but is not an array, wrap it
		if result.Exists() {
			return []interface{}{result.Value()}, nil
		}
		// No matches for wildcard - return empty array
		return []interface{}{}, nil
	}

	// For non-wildcard queries, check existence
	if !result.Exists() {
		return nil, &QueryError{
			Query:   path,
			Message: "path does not exist",
		}
	}

	// For non-wildcard queries, return the direct value
	return result.Value(), nil
}

// queryMultiple queries multiple paths and returns results as a map
func queryMultiple(inputJSON []byte, paths []string) (map[string]interface{}, error) {
	results := make(map[string]interface{})

	for _, path := range paths {
		normalizedPath := normalizePath(path)
		result := gjson.GetBytes(inputJSON, normalizedPath)

		if !result.Exists() {
			// For multiple queries, include null for non-existent paths
			results[path] = nil
			continue
		}

		// Handle wildcard results
		if isPathWildcard(normalizedPath) {
			if result.IsArray() {
				results[path] = result.Value()
			} else {
				results[path] = []interface{}{result.Value()}
			}
		} else {
			results[path] = result.Value()
		}
	}

	return results, nil
}

// queryToJSON queries JSON and returns results as JSON bytes
func queryToJSON(inputJSON []byte, params QueryParams) ([]byte, error) {
	result, err := query(inputJSON, params)
	if err != nil {
		return nil, err
	}

	// Wrap result in an object with "result" key for consistency
	output := map[string]interface{}{
		"result": result,
	}

	jsonBytes, err := json.Marshal(output)
	if err != nil {
		return nil, &QueryError{
			Query:   params.Path,
			Message: "failed to marshal result",
			Cause:   err,
		}
	}

	return jsonBytes, nil
}
