package schema

import "fmt"

// Transformer handles data transformation operations
type Transformer struct{}

// NewTransformer creates a new data transformer
func NewTransformer() *Transformer {
	return &Transformer{}
}

// ApplyDefaults applies default values from schema to data
// Only applies to fields that are missing (doesn't override existing values)
func (t *Transformer) ApplyDefaults(data interface{}, schema *Schema) (interface{}, error) {
	// Validate root data type matches schema type
	if schema.Type == TypeArray {
		if data == nil {
			data = []interface{}{}
		} else if _, ok := data.([]interface{}); !ok {
			return nil, fmt.Errorf("schema type is ARRAY but data is %T, expected []interface{}", data)
		}
	} else if schema.Type == TypeObject {
		if data == nil {
			data = make(map[string]interface{})
		} else if _, ok := data.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("schema type is OBJECT but data is %T, expected map[string]interface{}", data)
		}
	}
	// Primitive types (STRING, NUMBER, etc.) pass through without validation

	prop := &Property{
		Type:       schema.Type,
		Properties: schema.Properties,
		Items:      schema.Items,
	}
	return t.applyDefaultsToValue(data, prop)
}

// ApplyCSVDefaults applies defaults to each CSV row using the CSV schema
func (t *Transformer) ApplyCSVDefaults(rows []map[string]interface{}, schema *CSVSchema) ([]map[string]interface{}, error) {
	if schema == nil {
		return rows, nil
	}
	objSchema := schema.ToObjectSchema()

	for i, row := range rows {
		processed, err := t.ApplyDefaults(row, objSchema)
		if err != nil {
			return nil, err
		}
		if cast, ok := processed.(map[string]interface{}); ok {
			rows[i] = cast
		}
	}

	return rows, nil
}

// applyDefaultsToValue recursively applies defaults to a value
func (t *Transformer) applyDefaultsToValue(data interface{}, prop *Property) (interface{}, error) {
	switch prop.Type {
	case TypeObject:
		obj, ok := data.(map[string]interface{})
		if !ok {
			obj = make(map[string]interface{})
		}

		if prop.Properties == nil {
			return obj, nil
		}

		// Apply defaults for each property
		for propName, propDef := range prop.Properties {
			value, exists := obj[propName]

			if !exists && propDef.Default != nil {
				// Field doesn't exist but has default - apply it
				obj[propName] = propDef.Default
			} else if exists {
				// Field exists - recursively apply defaults to nested structures
				if propDef.Type == TypeObject && propDef.Properties != nil {
					obj[propName], _ = t.applyDefaultsToValue(value, propDef)
				} else if propDef.Type == TypeArray && propDef.Items != nil {
					obj[propName], _ = t.applyDefaultsToValue(value, propDef)
				}
			} else if !exists && propDef.Type == TypeObject && propDef.Properties != nil {
				// Field doesn't exist and is an object with nested properties
				// Recursively check if nested properties have defaults
				nestedObj, _ := t.applyDefaultsToValue(nil, propDef)
				if nestedMap, ok := nestedObj.(map[string]interface{}); ok && len(nestedMap) > 0 {
					// Nested defaults were applied - add the object
					obj[propName] = nestedObj
				}
			} else if !exists && propDef.Type == TypeArray && propDef.Items != nil && propDef.Default == nil {
				// Array field doesn't exist and has no default
				// Check if array items have defaults (for empty array with item defaults)
				// For now, we don't create empty arrays automatically
				// This is correct behavior - only create if there's an explicit default
			}
		}
		return obj, nil

	case TypeArray:
		arr, ok := data.([]interface{})
		if !ok {
			return data, nil
		}

		// Apply defaults to EACH item in the array
		if prop.Items != nil {
			for i, item := range arr {
				processedItem, _ := t.applyDefaultsToValue(item, prop.Items)
				arr[i] = processedItem
			}
		}
		return arr, nil
	}

	return data, nil
}

// StructureData ensures data conforms to schema structure
// Creates missing structures from schema and removes fields not defined in schema
func (t *Transformer) StructureData(data interface{}, schema *Schema) (interface{}, error) {
	// Validate root data type matches schema type
	if schema.Type == TypeArray {
		if data == nil {
			data = []interface{}{}
		} else if _, ok := data.([]interface{}); !ok {
			return nil, fmt.Errorf("schema type is ARRAY but data is %T, expected []interface{}", data)
		}
	} else if schema.Type == TypeObject {
		if data == nil {
			data = make(map[string]interface{})
		} else if _, ok := data.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("schema type is OBJECT but data is %T, expected map[string]interface{}", data)
		}
	}
	// Primitive types (STRING, NUMBER, etc.) pass through without validation

	prop := &Property{
		Type:       schema.Type,
		Properties: schema.Properties,
		Items:      schema.Items,
	}
	return t.structureValue(data, prop, true) // true = create missing structures
}

// StructureCSVRows removes fields not present in the CSV schema (when enabled)
func (t *Transformer) StructureCSVRows(rows []map[string]interface{}, schema *CSVSchema) ([]map[string]interface{}, error) {
	if schema == nil {
		return rows, nil
	}
	objSchema := schema.ToObjectSchema()

	for i, row := range rows {
		structured, err := t.StructureData(row, objSchema)
		if err != nil {
			return nil, err
		}
		if cast, ok := structured.(map[string]interface{}); ok {
			rows[i] = cast
		}
	}

	return rows, nil
}

// structureValue recursively structures a value according to property definition
// If createMissing is true, creates missing structures from schema
func (t *Transformer) structureValue(data interface{}, prop *Property, createMissing bool) (interface{}, error) {
	switch prop.Type {
	case TypeObject:
		obj, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected object, got %T", data)
		}

		if prop.Properties == nil {
			return obj, nil
		}

		structured := make(map[string]interface{})

		// Include fields defined in schema
		for propName, propDef := range prop.Properties {
			if value, exists := obj[propName]; exists {
				// Field exists in data
				if propDef.Type == TypeObject && propDef.Properties != nil {
					// Recursively structure nested objects
					structuredValue, err := t.structureValue(value, propDef, createMissing)
					if err != nil {
						continue // Skip invalid nested objects
					}
					structured[propName] = structuredValue
				} else if propDef.Type == TypeArray && propDef.Items != nil {
					// Recursively structure arrays
					structuredValue, err := t.structureValue(value, propDef, createMissing)
					if err != nil {
						continue // Skip invalid arrays
					}
					structured[propName] = structuredValue
				} else {
					// Copy value as-is
					structured[propName] = value
				}
			} else if createMissing {
				// Field doesn't exist but we should create missing structures
				if propDef.Type == TypeObject && propDef.Properties != nil {
					// Create empty object structure
					emptyObj := make(map[string]interface{})
					structuredValue, err := t.structureValue(emptyObj, propDef, createMissing)
					if err == nil && len(structuredValue.(map[string]interface{})) > 0 {
						structured[propName] = structuredValue
					}
				} else if propDef.Type == TypeArray && propDef.Items != nil {
					// Create empty array structure
					// Arrays are created as empty - items will be added via field mappings or defaults
					structured[propName] = []interface{}{}
				}
				// For primitive types, don't create - let defaults handle it
			}
		}

		return structured, nil

	case TypeArray:
		arr, ok := data.([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected array, got %T", data)
		}

		if prop.Items == nil {
			return arr, nil
		}

		structured := make([]interface{}, len(arr))

		// Structure each item in the array
		for i, item := range arr {
			structuredItem, err := t.structureValue(item, prop.Items, createMissing)
			if err != nil {
				// Keep original item if structuring fails
				structured[i] = item
			} else {
				structured[i] = structuredItem
			}
		}

		return structured, nil
	}

	// For primitive types, return as-is
	return data, nil
}

// TruncateFields truncates nested fields beyond specified level
func (t *Transformer) TruncateFields(data interface{}, level int) interface{} {
	if level <= 0 {
		return data
	}
	return t.truncateRecursive(data, level, 0)
}

// truncateRecursive recursively truncates nested structures
func (t *Transformer) truncateRecursive(data interface{}, maxLevel, currentLevel int) interface{} {
	if currentLevel >= maxLevel {
		// Reached max depth - return simple representation
		switch data.(type) {
		case map[string]interface{}:
			return map[string]interface{}{"__truncated": true}
		case []interface{}:
			return []interface{}{"__truncated"}
		default:
			return data
		}
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			result[key] = t.truncateRecursive(value, maxLevel, currentLevel+1)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = t.truncateRecursive(item, maxLevel, currentLevel+1)
		}
		return result
	}

	return data
}
