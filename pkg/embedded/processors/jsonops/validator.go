package jsonops

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// validate validates JSON data against a JSON Schema
func validate(inputJSON []byte, params ValidateParams) (bool, []string, error) {
	// Validate input JSON
	if !isValidJSON(inputJSON) {
		return false, nil, &ValidationError{
			Message: "input is not valid JSON",
		}
	}

	// Parse the schema
	var schemaData interface{}
	if err := json.Unmarshal(params.Schema, &schemaData); err != nil {
		return false, nil, &ValidationError{
			Message: "schema is not valid JSON",
		}
	}

	// Create compiler with appropriate draft
	compiler := jsonschema.NewCompiler()
	compiler.Draft = getDraftVersion(params.Draft)

	// Add the schema to the compiler
	// AddResource expects a reader, so we need to marshal and use bytes.NewReader
	schemaBytes, err := json.Marshal(schemaData)
	if err != nil {
		return false, nil, &ValidationError{
			Message: fmt.Sprintf("failed to marshal schema: %v", err),
		}
	}

	if err := compiler.AddResource("schema.json", strings.NewReader(string(schemaBytes))); err != nil {
		return false, nil, &ValidationError{
			Message: fmt.Sprintf("failed to add schema: %v", err),
		}
	}

	// Compile the schema
	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return false, nil, &ValidationError{
			Message: fmt.Sprintf("failed to compile schema: %v", err),
		}
	}

	// Parse input data
	var inputData interface{}
	if err := json.Unmarshal(inputJSON, &inputData); err != nil {
		return false, nil, &ValidationError{
			Message: fmt.Sprintf("failed to parse input: %v", err),
		}
	}

	// Validate the data
	if err := schema.Validate(inputData); err != nil {
		// Validation failed - extract error details
		validationErrors := extractValidationErrors(err)
		return false, validationErrors, nil
	}

	// Validation succeeded
	return true, nil, nil
}

// validateToJSON performs validation and returns result as JSON
func validateToJSON(inputJSON []byte, params ValidateParams) ([]byte, error) {
	valid, errors, err := validate(inputJSON, params)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{
		"valid": valid,
	}

	if !valid {
		result["errors"] = errors
	}

	output, err := json.Marshal(result)
	if err != nil {
		return nil, &ValidationError{
			Message: fmt.Sprintf("failed to marshal result: %v", err),
		}
	}

	return output, nil
}

// getDraftVersion returns the jsonschema draft version
func getDraftVersion(draft string) *jsonschema.Draft {
	switch draft {
	case "draft-04", "4":
		return jsonschema.Draft4
	case "draft-06", "6":
		return jsonschema.Draft6
	case "draft-07", "7":
		return jsonschema.Draft7
	case "2019-09":
		return jsonschema.Draft2019
	case "2020-12", "":
		// Default to latest
		return jsonschema.Draft2020
	default:
		// Default to latest if unknown
		return jsonschema.Draft2020
	}
}

// extractValidationErrors extracts detailed error information from validation error
func extractValidationErrors(err error) []string {
	if err == nil {
		return nil
	}

	// Check if it's a validation error from jsonschema library
	if validationErr, ok := err.(*jsonschema.ValidationError); ok {
		return flattenValidationErrors(validationErr)
	}

	// Otherwise return the error message as a single error
	return []string{err.Error()}
}

// flattenValidationErrors recursively flattens nested validation errors
func flattenValidationErrors(err *jsonschema.ValidationError) []string {
	var errors []string

	// Build error message for this error
	errorMsg := buildErrorMessage(err)
	if errorMsg != "" {
		errors = append(errors, errorMsg)
	}

	// Recursively process causes (nested errors)
	for _, cause := range err.Causes {
		errors = append(errors, flattenValidationErrors(cause)...)
	}

	return errors
}

// buildErrorMessage builds a human-readable error message from ValidationError
func buildErrorMessage(err *jsonschema.ValidationError) string {
	var parts []string

	// Add instance location (path in the data)
	if err.InstanceLocation != "" {
		parts = append(parts, fmt.Sprintf("at '%s'", err.InstanceLocation))
	}

	// Add the error message
	if err.Message != "" {
		parts = append(parts, err.Message)
	}

	// If we have parts, join them
	if len(parts) > 0 {
		return strings.Join(parts, ": ")
	}

	return ""
}
