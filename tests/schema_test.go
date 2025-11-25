package tests

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/schema"
)

// TestSchemaParser tests schema parsing functionality
func TestSchemaParser(t *testing.T) {
	t.Run("Parse valid object schema", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"name": {
					"type": "STRING",
					"required": true
				},
				"age": {
					"type": "NUMBER",
					"validation": {
						"minimum": 0,
						"maximum": 150
					}
				}
			}
		}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if result == nil {
			t.Fatal("Expected schema, got nil")
		}
		if result.Type != schema.TypeObject {
			t.Errorf("Expected type OBJECT, got: %s", result.Type)
		}
		if len(result.Properties) != 2 {
			t.Errorf("Expected 2 properties, got: %d", len(result.Properties))
		}
	})

	t.Run("Parse array schema", func(t *testing.T) {
		schemaJSON := `{
			"type": "ARRAY",
			"items": {
				"type": "STRING"
			}
		}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if result.Type != schema.TypeArray {
			t.Errorf("Expected type ARRAY, got: %s", result.Type)
		}
		if result.Items == nil {
			t.Error("Expected items definition")
		}
	})

	t.Run("Parse invalid schema - empty", func(t *testing.T) {
		parser := schema.NewParser()
		result, err := parser.Parse([]byte(""))

		if err == nil {
			t.Error("Expected error for empty schema")
		}
		if result != nil {
			t.Error("Expected nil result for invalid schema")
		}
	})

	t.Run("Parse invalid schema - missing type", func(t *testing.T) {
		schemaJSON := `{"properties": {}}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err == nil {
			t.Error("Expected error for schema without type")
		}
		if result != nil {
			t.Error("Expected nil result")
		}
	})

	t.Run("Parse invalid schema - wrong validation rules", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"age": {
					"type": "NUMBER",
					"validation": {
						"minLength": 5
					}
				}
			}
		}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err == nil {
			t.Error("Expected error for string validation on number type")
		}
		if result != nil {
			t.Error("Expected nil result")
		}
	})
}

// TestSchemaValidator tests validation functionality
func TestSchemaValidator(t *testing.T) {
	t.Run("Validate required fields", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"name": {
					Type:     schema.TypeString,
					Required: true,
				},
			},
		}

		validator := schema.NewValidator()

		// Valid data
		validData := map[string]interface{}{
			"name": "John Doe",
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Invalid data - missing required field
		invalidData := map[string]interface{}{}
		result = validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for missing required field")
		}
		if len(result.Errors) == 0 {
			t.Error("Expected validation errors")
		}
	})

	t.Run("Validate string constraints", func(t *testing.T) {
		minLen := 3
		maxLen := 10
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"username": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						MinLength: &minLen,
						MaxLength: &maxLen,
						Pattern:   "^[a-z]+$",
					},
				},
			},
		}

		validator := schema.NewValidator()

		// Valid
		validData := map[string]interface{}{"username": "john"}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Too short
		shortData := map[string]interface{}{"username": "ab"}
		result = validator.Validate(shortData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for too short username")
		}

		// Too long
		longData := map[string]interface{}{"username": "verylongusername"}
		result = validator.Validate(longData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for too long username")
		}

		// Pattern mismatch
		patternData := map[string]interface{}{"username": "John123"}
		result = validator.Validate(patternData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for pattern mismatch")
		}
	})

	t.Run("Validate number constraints", func(t *testing.T) {
		min := 0.0
		max := 100.0
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"score": {
					Type: schema.TypeNumber,
					Validation: &schema.ValidationRules{
						Minimum: &min,
						Maximum: &max,
					},
				},
			},
		}

		validator := schema.NewValidator()

		// Valid
		validData := map[string]interface{}{"score": 50.0}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Below minimum
		belowData := map[string]interface{}{"score": -10.0}
		result = validator.Validate(belowData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for value below minimum")
		}

		// Above maximum
		aboveData := map[string]interface{}{"score": 150.0}
		result = validator.Validate(aboveData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for value above maximum")
		}
	})

	t.Run("Validate array constraints", func(t *testing.T) {
		minItems := 1
		maxItems := 5
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"tags": {
					Type: schema.TypeArray,
					Items: &schema.Property{
						Type: schema.TypeString,
					},
					Validation: &schema.ValidationRules{
						MinItems:    &minItems,
						MaxItems:    &maxItems,
						UniqueItems: true,
					},
				},
			},
		}

		validator := schema.NewValidator()

		// Valid
		validData := map[string]interface{}{
			"tags": []interface{}{"tag1", "tag2", "tag3"},
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Too few items
		fewData := map[string]interface{}{
			"tags": []interface{}{},
		}
		result = validator.Validate(fewData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for too few items")
		}

		// Too many items
		manyData := map[string]interface{}{
			"tags": []interface{}{"t1", "t2", "t3", "t4", "t5", "t6"},
		}
		result = validator.Validate(manyData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for too many items")
		}

		// Duplicate items
		dupData := map[string]interface{}{
			"tags": []interface{}{"tag1", "tag1"},
		}
		result = validator.Validate(dupData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for duplicate items")
		}
	})

	t.Run("Validate format constraints", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"email": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Format: "email",
					},
				},
				"website": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Format: "uri",
					},
				},
			},
		}

		validator := schema.NewValidator()

		// Valid data
		validData := map[string]interface{}{
			"email":   "user@example.com",
			"website": "https://example.com",
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Invalid email
		invalidData := map[string]interface{}{
			"email": "not-an-email",
		}
		result = validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for bad email format")
		}
	})

	t.Run("Validate enum constraints", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"status": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Enum: []string{"active", "inactive", "pending"},
					},
				},
			},
		}

		validator := schema.NewValidator()

		// Valid
		validData := map[string]interface{}{"status": "active"}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Invalid
		invalidData := map[string]interface{}{"status": "unknown"}
		result = validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for value not in enum")
		}
	})
}

// TestSchemaTransformer tests transformation functionality
func TestSchemaTransformer(t *testing.T) {
	t.Run("Apply defaults to missing fields", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"name": {
					Type:     schema.TypeString,
					Required: true,
				},
				"country": {
					Type:    schema.TypeString,
					Default: "USA",
				},
				"active": {
					Type:    schema.TypeBoolean,
					Default: true,
				},
			},
		}

		transformer := schema.NewTransformer()

		inputData := map[string]interface{}{
			"name": "John Doe",
		}

		result, err := transformer.ApplyDefaults(inputData, schemaObj)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected map result")
		}

		if resultMap["name"] != "John Doe" {
			t.Error("Expected name to remain unchanged")
		}
		if resultMap["country"] != "USA" {
			t.Error("Expected country default to be applied")
		}
		if resultMap["active"] != true {
			t.Error("Expected active default to be applied")
		}
	})

	t.Run("Apply defaults to nested objects", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"user": {
					Type: schema.TypeObject,
					Properties: map[string]*schema.Property{
						"name": {
							Type: schema.TypeString,
						},
						"city": {
							Type:    schema.TypeString,
							Default: "NY",
						},
					},
				},
			},
		}

		transformer := schema.NewTransformer()

		inputData := map[string]interface{}{}

		result, err := transformer.ApplyDefaults(inputData, schemaObj)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected map result")
		}

		userObj, hasUser := resultMap["user"]
		if !hasUser {
			t.Fatal("Expected user object to be created with defaults")
		}

		userMap, ok := userObj.(map[string]interface{})
		if !ok {
			t.Fatal("Expected user to be a map")
		}

		if userMap["city"] != "NY" {
			t.Error("Expected nested default to be applied")
		}
	})

	t.Run("Apply defaults to array items", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeArray,
			Items: &schema.Property{
				Type: schema.TypeObject,
				Properties: map[string]*schema.Property{
					"name": {
						Type: schema.TypeString,
					},
					"status": {
						Type:    schema.TypeString,
						Default: "active",
					},
				},
			},
		}

		transformer := schema.NewTransformer()

		inputData := []interface{}{
			map[string]interface{}{"name": "User 1"},
			map[string]interface{}{"name": "User 2"},
		}

		result, err := transformer.ApplyDefaults(inputData, schemaObj)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resultArray, ok := result.([]interface{})
		if !ok {
			t.Fatal("Expected array result")
		}

		if len(resultArray) != 2 {
			t.Errorf("Expected 2 items, got: %d", len(resultArray))
		}

		// Check defaults applied to each item
		for i, item := range resultArray {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				t.Errorf("Item %d is not a map", i)
				continue
			}
			if itemMap["status"] != "active" {
				t.Errorf("Item %d: expected default status to be applied", i)
			}
		}
	})

	t.Run("Structure data removes extra fields", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"name": {
					Type: schema.TypeString,
				},
				"email": {
					Type: schema.TypeString,
				},
			},
		}

		transformer := schema.NewTransformer()

		inputData := map[string]interface{}{
			"name":        "John Doe",
			"email":       "john@example.com",
			"internal_id": "12345",        // Extra field
			"metadata":    "should remove", // Extra field
		}

		result, err := transformer.StructureData(inputData, schemaObj)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected map result")
		}

		if len(resultMap) != 2 {
			t.Errorf("Expected 2 fields, got: %d", len(resultMap))
		}
		if _, hasInternalID := resultMap["internal_id"]; hasInternalID {
			t.Error("Expected internal_id to be removed")
		}
		if _, hasMetadata := resultMap["metadata"]; hasMetadata {
			t.Error("Expected metadata to be removed")
		}
	})

	t.Run("Structure data creates missing structures", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"users": {
					Type: schema.TypeArray,
					Items: &schema.Property{
						Type: schema.TypeObject,
						Properties: map[string]*schema.Property{
							"name": {Type: schema.TypeString},
						},
					},
				},
			},
		}

		transformer := schema.NewTransformer()

		inputData := map[string]interface{}{}

		result, err := transformer.StructureData(inputData, schemaObj)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected map result")
		}

		usersField, hasUsers := resultMap["users"]
		if !hasUsers {
			t.Fatal("Expected users array to be created")
		}

		usersArray, ok := usersField.([]interface{})
		if !ok {
			t.Fatal("Expected users to be an array")
		}

		if len(usersArray) != 0 {
			t.Errorf("Expected empty array, got length: %d", len(usersArray))
		}
	})

	t.Run("Truncate fields at specified level", func(t *testing.T) {
		transformer := schema.NewTransformer()

		inputData := map[string]interface{}{
			"level1": map[string]interface{}{
				"level2": map[string]interface{}{
					"level3": "deep value",
				},
			},
		}

		// Truncate at level 2
		result := transformer.TruncateFields(inputData, 2)

		resultMap, ok := result.(map[string]interface{})
		if !ok {
			t.Fatal("Expected map result")
		}

		level1, ok := resultMap["level1"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected level1 to be a map")
		}

		level2, ok := level1["level2"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected level2 to exist")
		}

		if truncated, ok := level2["__truncated"]; !ok || truncated != true {
			t.Error("Expected level3 to be truncated")
		}
	})
}

// TestSchemaEngine tests the complete schema processing engine
func TestSchemaEngine(t *testing.T) {
	t.Run("Process with defaults and validation", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"name": {
					"type": "STRING",
					"required": true,
					"validation": {
						"minLength": 1,
						"maxLength": 100
					}
				},
				"email": {
					"type": "STRING",
					"required": true,
					"validation": {
						"format": "email"
					}
				},
				"country": {
					"type": "STRING",
					"default": "USA"
				},
				"active": {
					"type": "BOOLEAN",
					"default": true
				}
			}
		}`

		inputData := `{
			"name": "John Doe",
			"email": "john@example.com"
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    true,
				StructureData:    false,
				StrictValidation: true,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !result.Valid {
			t.Errorf("Expected valid result, got errors: %v", result.Errors)
		}

		// Check output has defaults
		var outputData map[string]interface{}
		if err := schema.FromJSON(result.Data, &outputData); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		if outputData["country"] != "USA" {
			t.Error("Expected country default to be applied")
		}
		if outputData["active"] != true {
			t.Error("Expected active default to be applied")
		}
	})

	t.Run("Process array with defaults on each item", func(t *testing.T) {
		schemaJSON := `{
			"type": "ARRAY",
			"items": {
				"type": "OBJECT",
				"properties": {
					"name": {
						"type": "STRING",
						"required": true
					},
					"city": {
						"type": "STRING",
						"default": "NY"
					}
				}
			}
		}`

		inputData := `[
			{"name": "John"},
			{"name": "Jane"}
		]`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    true,
				StructureData:    false,
				StrictValidation: false,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var outputData []interface{}
		if err := schema.FromJSON(result.Data, &outputData); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		if len(outputData) != 2 {
			t.Errorf("Expected 2 items, got: %d", len(outputData))
		}

		// Check each item has default
		for i, item := range outputData {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				t.Errorf("Item %d is not a map", i)
				continue
			}
			if itemMap["city"] != "NY" {
				t.Errorf("Item %d: expected city default", i)
			}
		}
	})

	t.Run("Strict mode fails on validation errors", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"email": {
					"type": "STRING",
					"required": true,
					"validation": {
						"format": "email"
					}
				}
			}
		}`

		inputData := `{
			"email": "not-an-email"
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    false,
				StructureData:    false,
				StrictValidation: true,
			},
		)

		if err == nil {
			t.Error("Expected error in strict mode")
		}
		if result == nil {
			t.Fatal("Expected result even with error")
		}
		if result.Valid {
			t.Error("Expected invalid result")
		}
		if len(result.Errors) == 0 {
			t.Error("Expected validation errors")
		}
	})

	t.Run("Non-strict mode returns data with errors", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"age": {
					"type": "NUMBER",
					"validation": {
						"minimum": 0,
						"maximum": 100
					}
				}
			}
		}`

		inputData := `{
			"age": 150
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    false,
				StructureData:    false,
				StrictValidation: false,
			},
		)

		if err != nil {
			t.Errorf("Expected no error in non-strict mode, got: %v", err)
		}
		if result.Valid {
			t.Error("Expected invalid result")
		}
		if len(result.Errors) == 0 {
			t.Error("Expected validation errors")
		}
		if result.Data == nil {
			t.Error("Expected data even with validation errors")
		}
	})

	t.Run("Structure data with nested objects", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"user": {
					"type": "OBJECT",
					"properties": {
						"name": {"type": "STRING"},
						"email": {"type": "STRING"}
					}
				}
			}
		}`

		inputData := `{
			"user": {
				"name": "John",
				"email": "john@example.com",
				"internal_field": "remove_this"
			},
			"extra_field": "remove_this_too"
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    false,
				StructureData:    true,
				StrictValidation: false,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var outputData map[string]interface{}
		if err := schema.FromJSON(result.Data, &outputData); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		if _, hasExtra := outputData["extra_field"]; hasExtra {
			t.Error("Expected extra_field to be removed")
		}

		user, ok := outputData["user"].(map[string]interface{})
		if !ok {
			t.Fatal("Expected user to be a map")
		}

		if _, hasInternal := user["internal_field"]; hasInternal {
			t.Error("Expected internal_field to be removed from nested object")
		}
	})

	t.Run("Complete processing with all options", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"id": {
					"type": "STRING",
					"required": true
				},
				"name": {
					"type": "STRING",
					"required": true,
					"validation": {
						"minLength": 2,
						"maxLength": 50
					}
				},
				"email": {
					"type": "STRING",
					"required": true,
					"validation": {
						"format": "email"
					}
				},
				"city": {
					"type": "STRING",
					"default": "NY"
				},
				"country": {
					"type": "STRING",
					"default": "USA"
				},
				"active": {
					"type": "BOOLEAN",
					"default": true
				}
			}
		}`

		inputData := `{
			"id": "123",
			"name": "John Doe",
			"email": "john@example.com",
			"internal_metadata": "should be removed"
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    true,
				StructureData:    true,
				StrictValidation: true,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !result.Valid {
			t.Errorf("Expected valid result, got errors: %v", result.Errors)
		}

		var outputData map[string]interface{}
		if err := schema.FromJSON(result.Data, &outputData); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		// Check defaults applied
		if outputData["city"] != "NY" {
			t.Error("Expected city default")
		}
		if outputData["country"] != "USA" {
			t.Error("Expected country default")
		}
		if outputData["active"] != true {
			t.Error("Expected active default")
		}

		// Check extra field removed
		if _, hasMetadata := outputData["internal_metadata"]; hasMetadata {
			t.Error("Expected internal_metadata to be removed")
		}

		// Check required fields present
		if outputData["id"] != "123" {
			t.Error("Expected id to be present")
		}
		if outputData["name"] != "John Doe" {
			t.Error("Expected name to be present")
		}
		if outputData["email"] != "john@example.com" {
			t.Error("Expected email to be present")
		}
	})
}

// TestSchemaFormats tests format validators
func TestSchemaFormats(t *testing.T) {
	t.Run("Email format validation", func(t *testing.T) {
		validator := schema.NewValidator()

		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"email": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Format: "email",
					},
				},
			},
		}

		validEmails := []string{
			"user@example.com",
			"john.doe@company.co.uk",
			"test+tag@domain.com",
		}

		for _, email := range validEmails {
			data := map[string]interface{}{"email": email}
			result := validator.Validate(data, schemaObj)
			if !result.Valid {
				t.Errorf("Expected '%s' to be valid email", email)
			}
		}

		invalidEmails := []string{
			"not-an-email",
			"@example.com",
			"user@",
			"",
		}

		for _, email := range invalidEmails {
			data := map[string]interface{}{"email": email}
			result := validator.Validate(data, schemaObj)
			if result.Valid {
				t.Errorf("Expected '%s' to be invalid email", email)
			}
		}
	})

	t.Run("UUID format validation", func(t *testing.T) {
		validator := schema.NewValidator()

		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"id": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Format: "uuid",
					},
				},
			},
		}

		// Valid UUIDs (v4 format: lowercase with dashes, 4 in third group, 8/9/a/b in fourth group)
		validUUIDs := []string{
			"123e4567-e89b-41d4-a456-426614174000", // Changed to v4 format
			"550e8400-e29b-41d4-a716-446655440000", // v4 format
		}
		for _, uuid := range validUUIDs {
			data := map[string]interface{}{"id": uuid}
			result := validator.Validate(data, schemaObj)
			if !result.Valid {
				t.Errorf("Expected '%s' to be valid UUID, got errors: %v", uuid, result.Errors)
			}
		}

		// Invalid UUIDs
		invalidUUIDs := []string{
			"not-a-uuid",
			"123e4567e89b12d3a456426614174000", // No dashes
			"g23e4567-e89b-41d4-a456-426614174000", // Invalid hex char
		}
		for _, uuid := range invalidUUIDs {
			data := map[string]interface{}{"id": uuid}
			result := validator.Validate(data, schemaObj)
			if result.Valid {
				t.Errorf("Expected '%s' to be invalid UUID", uuid)
			}
		}
	})

	t.Run("Date format validation", func(t *testing.T) {
		validator := schema.NewValidator()

		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"birthdate": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Format: "date",
					},
				},
			},
		}

		// Valid dates (YYYY-MM-DD format with basic pattern check)
		validDates := []string{"2024-01-15", "2025-12-31", "2000-06-30"}
		for _, date := range validDates {
			data := map[string]interface{}{"birthdate": date}
			result := validator.Validate(data, schemaObj)
			if !result.Valid {
				t.Errorf("Expected '%s' to be valid date, got errors: %v", date, result.Errors)
			}
		}

		// Invalid dates (wrong format)
		invalidDates := []string{"not-a-date", "01/15/2024", "2024/01/15"}
		for _, date := range invalidDates {
			data := map[string]interface{}{"birthdate": date}
			result := validator.Validate(data, schemaObj)
			if result.Valid {
				t.Errorf("Expected '%s' to be invalid date", date)
			}
		}
	})

	t.Run("Custom format validator", func(t *testing.T) {
		validator := schema.NewValidator()

		// Register custom phone format
		validator.RegisterFormat("phone", func(value string) bool {
			return len(value) >= 10 && len(value) <= 15
		})

		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"phone": {
					Type: schema.TypeString,
					Validation: &schema.ValidationRules{
						Format: "phone",
					},
				},
			},
		}

		// Valid
		data := map[string]interface{}{"phone": "1234567890"}
		result := validator.Validate(data, schemaObj)
		if !result.Valid {
			t.Error("Expected valid phone")
		}

		// Invalid
		data = map[string]interface{}{"phone": "123"}
		result = validator.Validate(data, schemaObj)
		if result.Valid {
			t.Error("Expected invalid phone")
		}
	})
}

// TestSchemaEdgeCases tests edge cases and error conditions
func TestSchemaEdgeCases(t *testing.T) {
	t.Run("Empty object with required fields", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"name": {
					"type": "STRING",
					"required": true
				}
			}
		}`

		inputData := `{}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				StrictValidation: true,
			},
		)

		if err == nil {
			t.Error("Expected error for missing required field in strict mode")
		}
		if result.Valid {
			t.Error("Expected invalid result")
		}
	})

	t.Run("Type mismatch", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"age": {
					"type": "NUMBER"
				}
			}
		}`

		inputData := `{
			"age": "not a number"
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				StrictValidation: false,
			},
		)

		if err != nil {
			t.Errorf("Expected no error in non-strict mode, got: %v", err)
		}
		if result.Valid {
			t.Error("Expected invalid result for type mismatch")
		}
	})

	t.Run("Invalid input JSON", func(t *testing.T) {
		schemaJSON := `{"type": "OBJECT", "properties": {}}`
		inputData := `{invalid json`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{},
		)

		if err == nil {
			t.Error("Expected error for invalid JSON")
		}
		if result != nil {
			t.Error("Expected nil result for parse error")
		}
	})

	t.Run("Default value doesn't override existing", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"city": {
					"type": "STRING",
					"default": "NY"
				}
			}
		}`

		inputData := `{
			"city": "LA"
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults: true,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var outputData map[string]interface{}
		if err := schema.FromJSON(result.Data, &outputData); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		if outputData["city"] != "LA" {
			t.Errorf("Expected existing value 'LA', got: %v", outputData["city"])
		}
	})

	t.Run("Nested array with object items", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"users": {
					"type": "ARRAY",
					"items": {
						"type": "OBJECT",
						"properties": {
							"name": {"type": "STRING", "required": true},
							"status": {"type": "STRING", "default": "active"}
						}
					},
					"validation": {
						"minItems": 1
					}
				}
			}
		}`

		inputData := `{
			"users": [
				{"name": "John"},
				{"name": "Jane", "status": "inactive"}
			]
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    true,
				StrictValidation: true,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		var outputData map[string]interface{}
		if err := schema.FromJSON(result.Data, &outputData); err != nil {
			t.Fatalf("Failed to parse output: %v", err)
		}

		users, ok := outputData["users"].([]interface{})
		if !ok {
			t.Fatal("Expected users to be an array")
		}

		user1, ok := users[0].(map[string]interface{})
		if !ok {
			t.Fatal("Expected user1 to be a map")
		}
		if user1["status"] != "active" {
			t.Error("Expected default status on user1")
		}

		user2, ok := users[1].(map[string]interface{})
		if !ok {
			t.Fatal("Expected user2 to be a map")
		}
		if user2["status"] != "inactive" {
			t.Error("Expected existing status on user2 to be preserved")
		}
	})
}

// TestSchemaByteType tests BYTE type validation
func TestSchemaByteType(t *testing.T) {
	t.Run("Parse schema with BYTE type", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"attachment": {
					"type": "BYTE",
					"required": true
				}
			}
		}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if result == nil {
			t.Fatal("Expected schema, got nil")
		}
		if result.Properties["attachment"].Type != schema.TypeByte {
			t.Errorf("Expected type BYTE, got: %s", result.Properties["attachment"].Type)
		}
	})

	t.Run("Validate BYTE type with valid base64", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type:     schema.TypeByte,
					Required: true,
				},
			},
		}

		validator := schema.NewValidator()

		// "Hello World" in base64
		validData := map[string]interface{}{
			"result": "SGVsbG8gV29ybGQ=",
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}
	})

	t.Run("Validate BYTE type with invalid base64", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type:       schema.TypeByte,
					Validation: &schema.ValidationRules{},
				},
			},
		}

		validator := schema.NewValidator()

		invalidData := map[string]interface{}{
			"result": "not-valid-base64!!!",
		}
		result := validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for bad base64")
		}
		if len(result.Errors) == 0 {
			t.Error("Expected validation errors")
		}
	})

	t.Run("Validate BYTE minLength constraint", func(t *testing.T) {
		minLen := 10
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type: schema.TypeByte,
					Validation: &schema.ValidationRules{
						MinLength: &minLen,
					},
				},
			},
		}

		validator := schema.NewValidator()

		// "Hello World" = 11 bytes - should pass
		validData := map[string]interface{}{
			"result": "SGVsbG8gV29ybGQ=",
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid (11 bytes >= 10), got errors: %v", result.Errors)
		}

		// "Hi" = 2 bytes - should fail
		invalidData := map[string]interface{}{
			"result": "SGk=", // "Hi" in base64
		}
		result = validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for data shorter than minLength")
		}
	})

	t.Run("Validate BYTE maxLength constraint", func(t *testing.T) {
		maxLen := 5
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type: schema.TypeByte,
					Validation: &schema.ValidationRules{
						MaxLength: &maxLen,
					},
				},
			},
		}

		validator := schema.NewValidator()

		// "Hi" = 2 bytes - should pass
		validData := map[string]interface{}{
			"result": "SGk=",
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid (2 bytes <= 5), got errors: %v", result.Errors)
		}

		// "Hello World" = 11 bytes - should fail
		invalidData := map[string]interface{}{
			"result": "SGVsbG8gV29ybGQ=",
		}
		result = validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for data exceeding maxLength")
		}
	})

	t.Run("Validate BYTE with both min and max length", func(t *testing.T) {
		minLen := 5
		maxLen := 20
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"thumbnail": {
					Type: schema.TypeByte,
					Validation: &schema.ValidationRules{
						MinLength: &minLen,
						MaxLength: &maxLen,
					},
				},
			},
		}

		validator := schema.NewValidator()

		// Valid: 11 bytes
		validData := map[string]interface{}{
			"thumbnail": "SGVsbG8gV29ybGQ=",
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid, got errors: %v", result.Errors)
		}

		// Too short: 2 bytes
		shortData := map[string]interface{}{
			"thumbnail": "SGk=",
		}
		result = validator.Validate(shortData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for too short data")
		}

		// Too long: Create a string > 20 bytes
		longString := "This is a very long string that exceeds twenty bytes"
		longBase64 := "VGhpcyBpcyBhIHZlcnkgbG9uZyBzdHJpbmcgdGhhdCBleGNlZWRzIHR3ZW50eSBieXRlcw=="
		longData := map[string]interface{}{
			"thumbnail": longBase64,
		}
		result = validator.Validate(longData, schemaObj)
		if result.Valid {
			t.Errorf("Expected invalid for too long data (%d bytes)", len(longString))
		}
	})

	t.Run("BYTE type with URL-safe base64", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type: schema.TypeByte,
				},
			},
		}

		validator := schema.NewValidator()

		// URL-safe base64 (uses - and _ instead of + and /)
		validData := map[string]interface{}{
			"result": "SGVsbG8gV29ybGQ=", // Standard base64
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid for standard base64, got errors: %v", result.Errors)
		}
	})

	t.Run("Parse schema with BYTE validation rules", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"file": {
					"type": "BYTE",
					"required": true,
					"validation": {
						"minLength": 100,
						"maxLength": 1048576
					}
				}
			}
		}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err != nil {
			t.Errorf("Expected no error for BYTE with minLength/maxLength, got: %v", err)
		}
		if result == nil {
			t.Fatal("Expected valid schema")
		}

		fileProp := result.Properties["file"]
		if fileProp.Validation == nil {
			t.Fatal("Expected validation rules")
		}
		if *fileProp.Validation.MinLength != 100 {
			t.Errorf("Expected minLength 100, got: %d", *fileProp.Validation.MinLength)
		}
		if *fileProp.Validation.MaxLength != 1048576 {
			t.Errorf("Expected maxLength 1048576, got: %d", *fileProp.Validation.MaxLength)
		}
	})

	t.Run("Parse schema rejects string validation on BYTE", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"result": {
					"type": "BYTE",
					"validation": {
						"pattern": "^[a-z]+$"
					}
				}
			}
		}`

		parser := schema.NewParser()
		result, err := parser.Parse([]byte(schemaJSON))

		if err == nil {
			t.Error("Expected error for pattern validation on BYTE type")
		}
		if result != nil {
			t.Error("Expected nil result for invalid schema")
		}
	})

	t.Run("Complete BYTE processing with engine", func(t *testing.T) {
		schemaJSON := `{
			"type": "OBJECT",
			"properties": {
				"id": {
					"type": "STRING",
					"required": true
				},
				"certificate": {
					"type": "BYTE",
					"required": true,
					"validation": {
						"minLength": 10,
						"maxLength": 1000
					}
				}
			}
		}`

		// "Hello World" = 11 bytes in base64
		inputData := `{
			"id": "cert-123",
			"certificate": "SGVsbG8gV29ybGQ="
		}`

		engine := schema.NewEngine()
		result, err := engine.ProcessWithSchema(
			[]byte(inputData),
			[]byte(schemaJSON),
			schema.ProcessOptions{
				ApplyDefaults:    false,
				StructureData:    false,
				StrictValidation: true,
			},
		)

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !result.Valid {
			t.Errorf("Expected valid result, got errors: %v", result.Errors)
		}
	})

	t.Run("BYTE type accepts byte slice", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type: schema.TypeByte,
				},
			},
		}

		validator := schema.NewValidator()

		// Byte slice should be valid
		validData := map[string]interface{}{
			"result": []byte("Hello World"),
		}
		result := validator.Validate(validData, schemaObj)
		if !result.Valid {
			t.Errorf("Expected valid for byte slice, got errors: %v", result.Errors)
		}
	})

	t.Run("BYTE type rejects non-string/non-byte types", func(t *testing.T) {
		schemaObj := &schema.Schema{
			Type: schema.TypeObject,
			Properties: map[string]*schema.Property{
				"result": {
					Type: schema.TypeByte,
				},
			},
		}

		validator := schema.NewValidator()

		invalidData := map[string]interface{}{
			"result": 12345, // Number instead of string/bytes
		}
		result := validator.Validate(invalidData, schemaObj)
		if result.Valid {
			t.Error("Expected invalid for non-string/non-byte value")
		}
	})
}

