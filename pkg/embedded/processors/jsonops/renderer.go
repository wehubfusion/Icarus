package jsonops

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// render constructs JSON from a template and input data
// Template can contain placeholders like {{path}} or ${path}
// Returns the rendered JSON structure
func render(inputJSON []byte, params RenderParams) (map[string]interface{}, error) {
	// Validate input JSON
	if !isValidJSON(inputJSON) {
		return nil, &RenderError{
			Field:   "",
			Message: "input is not valid JSON",
		}
	}

	// Process the template recursively
	result, err := renderValue(params.Template, inputJSON, params.FailOnMissing)
	if err != nil {
		return nil, err
	}

	// Convert result to map
	resultMap, ok := convertToMap(result)
	if !ok {
		return nil, &RenderError{
			Field:   "",
			Message: "template must evaluate to a JSON object",
		}
	}

	return resultMap, nil
}

// renderValue processes a single value in the template
// Handles strings with placeholders, nested objects, arrays, and primitives
func renderValue(value interface{}, inputJSON []byte, failOnMissing bool) (interface{}, error) {
	switch v := value.(type) {
	case string:
		// Check if entire string is a single placeholder reference
		if isSinglePlaceholder(v) {
			path := extractPlaceholderPath(v)
			result := getValue(inputJSON, path)

			if !result.Exists() {
				if failOnMissing {
					return nil, &RenderError{
						Field:   path,
						Message: "referenced path does not exist",
					}
				}
				return nil, nil
			}

			// Return the actual value (not stringified)
			return result.Value(), nil
		}

		// Otherwise, expand template placeholders in string
		expanded, err := renderString(v, inputJSON, failOnMissing)
		if err != nil {
			return nil, err
		}
		return expanded, nil

	case map[string]interface{}:
		// Recursively process nested objects
		result := make(map[string]interface{})
		for key, val := range v {
			renderedValue, err := renderValue(val, inputJSON, failOnMissing)
			if err != nil {
				return nil, err
			}
			result[key] = renderedValue
		}
		return result, nil

	case []interface{}:
		// Recursively process arrays
		result := make([]interface{}, len(v))
		for i, val := range v {
			renderedValue, err := renderValue(val, inputJSON, failOnMissing)
			if err != nil {
				return nil, err
			}
			result[i] = renderedValue
		}
		return result, nil

	default:
		// Return primitives as-is (numbers, booleans, null)
		return v, nil
	}
}

// renderString expands placeholders in a string template
// Supports {{path}} and ${path} syntax
func renderString(template string, inputJSON []byte, failOnMissing bool) (string, error) {
	// Match {{path}} or ${path}
	re := regexp.MustCompile(`\{\{([^}]+)\}\}|\$\{([^}]+)\}`)

	var renderErr error
	result := re.ReplaceAllStringFunc(template, func(match string) string {
		// Don't process if we already have an error
		if renderErr != nil {
			return match
		}

		// Extract the path from the match
		var path string
		if strings.HasPrefix(match, "{{") {
			path = strings.TrimSpace(match[2 : len(match)-2])
		} else {
			path = strings.TrimSpace(match[2 : len(match)-1])
		}

		// Get value from input JSON
		value := getValue(inputJSON, path)
		if !value.Exists() {
			if failOnMissing {
				renderErr = &RenderError{
					Field:   path,
					Message: "referenced path does not exist",
				}
				return match
			}
			return ""
		}

		// Return the value as string
		// For complex types, return JSON representation
		if value.IsObject() || value.IsArray() {
			return value.Raw
		}
		return value.String()
	})

	if renderErr != nil {
		return "", renderErr
	}

	return result, nil
}

// isSinglePlaceholder checks if a string is exactly one placeholder reference
// Example: "{{user.name}}" returns true, "Hello {{user.name}}" returns false
func isSinglePlaceholder(s string) bool {
	s = strings.TrimSpace(s)

	// Check for {{...}} format
	if strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}") {
		inner := s[2 : len(s)-2]
		// Make sure there are no other braces inside
		return !strings.Contains(inner, "{{") && !strings.Contains(inner, "}}")
	}

	// Check for ${...} format
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		inner := s[2 : len(s)-1]
		// Make sure there are no other braces inside
		return !strings.Contains(inner, "${") && !strings.Contains(inner, "}")
	}

	return false
}

// extractPlaceholderPath extracts the path from a placeholder
// Example: "{{user.name}}" returns "user.name"
func extractPlaceholderPath(s string) string {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}") {
		return strings.TrimSpace(s[2 : len(s)-2])
	}

	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		return strings.TrimSpace(s[2 : len(s)-1])
	}

	return s
}

// renderToJSON constructs JSON from template and returns as JSON bytes
func renderToJSON(inputJSON []byte, params RenderParams) ([]byte, error) {
	result, err := render(inputJSON, params)
	if err != nil {
		return nil, err
	}

	output, err := json.Marshal(result)
	if err != nil {
		return nil, &RenderError{
			Field:   "",
			Message: fmt.Sprintf("failed to marshal result: %v", err),
			Cause:   err,
		}
	}

	return output, nil
}
