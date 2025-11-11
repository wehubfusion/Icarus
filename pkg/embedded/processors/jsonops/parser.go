package jsonops

import (
	"encoding/json"
)

// parse extracts values from JSON using path notation
// Returns a map of field names to extracted values
func parse(inputJSON []byte, params ParseParams) (map[string]interface{}, error) {
	// Validate input JSON
	if !isValidJSON(inputJSON) {
		return nil, &ParseError{
			Path:    "",
			Message: "input is not valid JSON",
		}
	}

	result := make(map[string]interface{})

	// Extract each path
	for fieldName, path := range params.Paths {
		normalizedPath := normalizePath(path)
		value := getValue(inputJSON, normalizedPath)

		// Check if path exists
		if !value.Exists() {
			if params.FailOnMissing {
				return nil, &ParseError{
					Path:    path,
					Message: "path does not exist",
				}
			}
			// Set to null if path doesn't exist and FailOnMissing is false
			result[fieldName] = nil
			continue
		}

		// Extract the value
		result[fieldName] = value.Value()
	}

	return result, nil
}

// parseToJSON extracts values and returns them as JSON bytes
func parseToJSON(inputJSON []byte, params ParseParams) ([]byte, error) {
	result, err := parse(inputJSON, params)
	if err != nil {
		return nil, err
	}

	output, err := json.Marshal(result)
	if err != nil {
		return nil, &ParseError{
			Path:    "",
			Message: "failed to marshal result",
			Cause:   err,
		}
	}

	return output, nil
}
