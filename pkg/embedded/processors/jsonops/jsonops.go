package jsonops

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for JSON operations
type Executor struct{}

// NewExecutor creates a new JSON operations executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a JSON operation
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var jsonConfig Config
	if err := json.Unmarshal(config.Configuration, &jsonConfig); err != nil {
		return nil, fmt.Errorf("failed to parse jsonops configuration: %w", err)
	}

	// Validate configuration (operation is required)
	if err := jsonConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid jsonops configuration: %w", err)
	}

	// Route to appropriate operation handler
	switch jsonConfig.Operation {
	case "parse":
		return e.executeParse(config.Input, jsonConfig)

	case "produce":
		return e.executeProduce(config.Input, jsonConfig)

	default:
		return nil, fmt.Errorf("unknown operation: %s", jsonConfig.Operation)
	}
}

// shouldAutoWrapSchema determines if the schema should be auto-wrapped
// Returns true when:
// - Data is an array ([]interface{})
// - Schema root type is NOT "ARRAY" (i.e., schema describes a single item)
// This allows schemas to describe single item structure while handling array data
func shouldAutoWrapSchema(data []byte, schemaDefinition json.RawMessage) bool {
	// Parse data to check its type
	var dataValue interface{}
	if err := json.Unmarshal(data, &dataValue); err != nil {
		return false
	}

	// Check if data is an array
	_, isArray := dataValue.([]interface{})
	if !isArray {
		return false // Not an array, no wrapping needed
	}

	// Parse schema to check root type
	var schemaStruct struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(schemaDefinition, &schemaStruct); err != nil {
		return false
	}

	// Auto-wrap when data is array but schema describes a single item (not ARRAY)
	// Schema can be STRING, NUMBER, BOOLEAN, OBJECT, etc. - anything that describes one item
	schemaType := schemaStruct.Type
	return schemaType != "ARRAY" && schemaType != "array"
}

// isPrimitiveArray checks if data is an array of primitive values
func isPrimitiveArray(data []byte) bool {
	var arr []interface{}
	if err := json.Unmarshal(data, &arr); err != nil {
		return false
	}

	if len(arr) == 0 {
		return true // Empty array treated as primitive array
	}

	// Check first non-nil element
	for _, item := range arr {
		if item == nil {
			continue
		}
		switch item.(type) {
		case string, float64, bool:
			return true
		default:
			return false // Not a primitive (likely object or nested array)
		}
	}

	return true // All nil or empty
}

// getFirstPropertyName extracts the first property name from an OBJECT schema
func getFirstPropertyName(schemaDefinition json.RawMessage) string {
	var schema struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schemaDefinition, &schema); err != nil {
		return ""
	}

	if schema.Type != "OBJECT" && schema.Type != "object" {
		return ""
	}

	// Return the first property name
	for propName := range schema.Properties {
		return propName
	}

	return ""
}

// wrapPrimitivesIntoObjects transforms primitive array into object array
// Input: ["saml2", "manual"], propertyName: "auth"
// Output: [{"auth": "saml2"}, {"auth": "manual"}]
func wrapPrimitivesIntoObjects(data []byte, propertyName string) ([]byte, error) {
	var arr []interface{}
	if err := json.Unmarshal(data, &arr); err != nil {
		return nil, err
	}

	result := make([]map[string]interface{}, len(arr))
	for i, item := range arr {
		result[i] = map[string]interface{}{
			propertyName: item,
		}
	}

	return json.Marshal(result)
}

// wrapSchemaForIteration wraps schema for array data
// Creates {type: "ARRAY", items: <original_schema>}
func wrapSchemaForIteration(itemSchema json.RawMessage) json.RawMessage {
	var itemsSchema map[string]interface{}
	if err := json.Unmarshal(itemSchema, &itemsSchema); err != nil {
		return itemSchema // Return original on error
	}

	// Create wrapped array schema
	wrappedSchema := map[string]interface{}{
		"type":  "ARRAY",
		"items": itemsSchema,
	}

	wrapped, err := json.Marshal(wrappedSchema)
	if err != nil {
		return itemSchema
	}

	return wrapped
}

// executeParse parses and validates JSON against schema
func (e *Executor) executeParse(input []byte, config Config) ([]byte, error) {
	// Unmarshal input envelope
	var inputEnvelope map[string]interface{}
	if err := json.Unmarshal(input, &inputEnvelope); err != nil {
		return nil, fmt.Errorf("failed to parse input: %w", err)
	}

	dataField, hasData := inputEnvelope["data"]
	if !hasData {
		return nil, fmt.Errorf("input must contain a 'data' field")
	}

	var dataToValidate []byte

	// Check if data is a base64-encoded string (from JS runner)
	if encodedStr, isString := dataField.(string); isString {
		// Decode base64 string
		decoded, err := base64.StdEncoding.DecodeString(encodedStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 data: %w", err)
		}
		dataToValidate = decoded
	} else {
		// Data is already JSON object/array, marshal it
		marshaled, err := json.Marshal(dataField)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal data field: %w", err)
		}
		dataToValidate = marshaled
	}

	// Determine which schema to use and potentially transform data
	schemaToUse := config.Schema
	dataToProcess := dataToValidate

	if config.GetAutoWrapForIteration() {
		if shouldAutoWrapSchema(dataToValidate, config.Schema) {
			// Check if we have a primitive array with an OBJECT schema
			// In this case, wrap each primitive into an object using the first property name
			if isPrimitiveArray(dataToValidate) {
				propName := getFirstPropertyName(config.Schema)
				if propName != "" {
					// Transform: ["saml2", "manual"] -> [{"auth": "saml2"}, {"auth": "manual"}]
					transformed, err := wrapPrimitivesIntoObjects(dataToValidate, propName)
					if err == nil {
						dataToProcess = transformed
					}
				}
			}
			// Wrap schema to expect array of items
			schemaToUse = wrapSchemaForIteration(config.Schema)
		}
	}

	// Process with schema (potentially auto-wrapped for array data)
	validatedData, err := processWithSchema(dataToProcess, schemaToUse, config.SchemaID, SchemaOptions{
		ApplyDefaults:    config.GetApplyDefaults(),
		StructureData:    config.GetStructureData(),
		StrictValidation: config.GetStrictValidation(),
	})
	if err != nil {
		return nil, err
	}

	// Unmarshal the validated data to wrap it
	var validatedResult interface{}
	if err := json.Unmarshal(validatedData, &validatedResult); err != nil {
		return nil, fmt.Errorf("failed to unmarshal validated data: %w", err)
	}

	return json.Marshal(validatedResult)
}

// executeProduce validates, structures, and encodes JSON to base64
func (e *Executor) executeProduce(input []byte, config Config) ([]byte, error) {
	// Extract data from input envelope
	var inputEnvelope map[string]interface{}
	if err := json.Unmarshal(input, &inputEnvelope); err != nil {
		return nil, fmt.Errorf("failed to parse input envelope: %w", err)
	}

	// Data is regular JSON (not encoded), marshal it to bytes
	dataToEncode, err := json.Marshal(inputEnvelope)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal data field: %w", err)
	}

	// Process with schema (no defaults on produce)
	processedJSON, err := processWithSchema(dataToEncode, config.Schema, config.SchemaID, SchemaOptions{
		ApplyDefaults:    false, // Never apply defaults on produce
		StructureData:    config.GetStructureData(),
		StrictValidation: config.GetStrictValidation(),
	})
	if err != nil {
		return nil, err
	}

	// Pretty print if requested
	if config.Pretty {
		var prettyData interface{}
		if err := json.Unmarshal(processedJSON, &prettyData); err != nil {
			return nil, fmt.Errorf("failed to parse for pretty printing: %w", err)
		}
		processedJSON, err = json.MarshalIndent(prettyData, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to pretty print: %w", err)
		}
	}

	// Encode to base64
	encoded := base64.StdEncoding.EncodeToString(processedJSON)

	// Wrap result in data envelope following the standard convention
	wrappedResult := map[string]interface{}{
		"data": encoded,
	}

	return json.Marshal(wrappedResult)
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-json-operations"
}
