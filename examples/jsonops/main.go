package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
)

func main() {
	fmt.Println("=== JSON Operations Processor Examples ===")
	fmt.Println()

	executor := jsonops.NewExecutor()

	// Example 1: Parse Operation
	runParseExample(executor)

	// Example 2: Render Operation
	runRenderExample(executor)

	// Example 3: Query Operation
	runQueryExample(executor)

	// Example 4: Transform Operations
	runTransformExamples(executor)

	// Example 5: Validate Operation
	runValidateExample(executor)
}

func runParseExample(executor *jsonops.Executor) {
	fmt.Println("--- Parse Operation ---")
	fmt.Println("Extracts specific values from JSON using path notation")

	input := `{
		"user": {
			"name": "Alice Smith",
			"email": "alice@example.com",
			"age": 30
		},
		"orders": [
			{"id": 1, "total": 99.99},
			{"id": 2, "total": 149.99}
		]
	}`

	config := createConfig(map[string]interface{}{
		"operation": "parse",
		"params": map[string]interface{}{
			"paths": map[string]string{
				"userName":   "user.name",
				"userEmail":  "user.email",
				"firstOrder": "orders.0",
				"orderCount": "orders.#", // Use # for array length
			},
		},
	})

	result := execute(executor, config, input)
	fmt.Printf("Input: %s\n", compact(input))
	fmt.Printf("Output: %s\n\n", result)
}

func runRenderExample(executor *jsonops.Executor) {
	fmt.Println("--- Render Operation ---")
	fmt.Println("Constructs JSON from a template with placeholders")

	input := `{
		"firstName": "Bob",
		"lastName": "Johnson",
		"company": "Acme Corp",
		"role": "Engineer"
	}`

	config := createConfig(map[string]interface{}{
		"operation": "render",
		"params": map[string]interface{}{
			"template": map[string]interface{}{
				"fullName": "{{firstName}} {{lastName}}",
				"email":    "{{firstName}}.{{lastName}}@example.com",
				"profile": map[string]interface{}{
					"company": "{{company}}",
					"role":    "{{role}}",
				},
			},
		},
	})

	result := execute(executor, config, input)
	fmt.Printf("Input: %s\n", compact(input))
	fmt.Printf("Output: %s\n\n", result)
}

func runQueryExample(executor *jsonops.Executor) {
	fmt.Println("--- Query Operation ---")
	fmt.Println("Queries JSON with path notation and wildcards")

	input := `{
		"products": [
			{"name": "Laptop", "price": 999, "category": "Electronics"},
			{"name": "Mouse", "price": 29, "category": "Electronics"},
			{"name": "Desk", "price": 299, "category": "Furniture"}
		]
	}`

	// Single path query with wildcard
	fmt.Println("Query all product prices:")
	config1 := createConfig(map[string]interface{}{
		"operation": "query",
		"params": map[string]interface{}{
			"path": "products.*.price",
		},
	})
	result1 := execute(executor, config1, input)
	fmt.Printf("Output: %s\n", result1)

	// Multiple paths query
	fmt.Println("\nQuery multiple paths:")
	config2 := createConfig(map[string]interface{}{
		"operation": "query",
		"params": map[string]interface{}{
			"multiple": true,
			"paths": []string{
				"products.0.name",
				"products.*.category",
			},
		},
	})
	result2 := execute(executor, config2, input)
	fmt.Printf("Output: %s\n\n", result2)
}

func runTransformExamples(executor *jsonops.Executor) {
	fmt.Println("--- Transform Operations ---")

	baseInput := `{
		"user": {
			"name": "Charlie Brown",
			"age": 25
		},
		"status": "active"
	}`

	// Set operation
	fmt.Println("1. Set - Add/update a value:")
	config1 := createConfig(map[string]interface{}{
		"operation": "transform",
		"params": map[string]interface{}{
			"type":  "set",
			"path":  "user.email",
			"value": "charlie@example.com",
		},
	})
	result1 := execute(executor, config1, baseInput)
	fmt.Printf("Output: %s\n", result1)

	// Delete operation
	fmt.Println("\n2. Delete - Remove a field:")
	config2 := createConfig(map[string]interface{}{
		"operation": "transform",
		"params": map[string]interface{}{
			"type": "delete",
			"path": "user.age",
		},
	})
	result2 := execute(executor, config2, baseInput)
	fmt.Printf("Output: %s\n", result2)

	// Merge operation
	fmt.Println("\n3. Merge - Combine two objects:")
	mergeData := map[string]interface{}{
		"user": map[string]interface{}{
			"email":    "charlie@example.com",
			"verified": true,
		},
		"lastLogin": "2025-11-11",
	}
	mergeJSON, _ := json.Marshal(mergeData)

	config3 := createConfig(map[string]interface{}{
		"operation": "transform",
		"params": map[string]interface{}{
			"type":       "merge",
			"merge_data": json.RawMessage(mergeJSON),
		},
	})
	result3 := execute(executor, config3, baseInput)
	fmt.Printf("Output: %s\n\n", result3)
}

func runValidateExample(executor *jsonops.Executor) {
	fmt.Println("--- Validate Operation ---")
	fmt.Println("Validates JSON against a JSON Schema")

	schema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"name": map[string]interface{}{
				"type":      "string",
				"minLength": 1,
			},
			"age": map[string]interface{}{
				"type":    "integer",
				"minimum": 0,
				"maximum": 150,
			},
			"email": map[string]interface{}{
				"type":   "string",
				"format": "email",
			},
		},
		"required": []string{"name", "age"},
	}
	schemaJSON, _ := json.Marshal(schema)

	// Valid data
	fmt.Println("1. Valid data:")
	validInput := `{"name": "Alice", "age": 30, "email": "alice@example.com"}`
	config1 := createConfig(map[string]interface{}{
		"operation": "validate",
		"params": map[string]interface{}{
			"schema": json.RawMessage(schemaJSON),
		},
	})
	result1 := execute(executor, config1, validInput)
	fmt.Printf("Input: %s\n", validInput)
	fmt.Printf("Result: %s\n", result1)

	// Invalid data
	fmt.Println("\n2. Invalid data (missing required field):")
	invalidInput := `{"name": "Bob"}`
	result2 := execute(executor, config1, invalidInput)
	fmt.Printf("Input: %s\n", invalidInput)
	fmt.Printf("Result: %s\n", result2)

	// Invalid data (constraint violation)
	fmt.Println("\n3. Invalid data (age out of range):")
	invalidInput2 := `{"name": "Charlie", "age": 200}`
	result3 := execute(executor, config1, invalidInput2)
	fmt.Printf("Input: %s\n", invalidInput2)
	fmt.Printf("Result: %s\n\n", result3)
}

// Helper functions

func createConfig(config map[string]interface{}) []byte {
	data, err := json.Marshal(config)
	if err != nil {
		log.Fatalf("Failed to marshal config: %v", err)
	}
	return data
}

func execute(executor *jsonops.Executor, config []byte, input string) string {
	result, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: config,
		Input:         []byte(input),
	})
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}
	return prettyPrint(result)
}

func prettyPrint(data []byte) string {
	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return string(data)
	}
	pretty, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return string(data)
	}
	return string(pretty)
}

func compact(input string) string {
	var obj interface{}
	if err := json.Unmarshal([]byte(input), &obj); err != nil {
		return input
	}
	compact, err := json.Marshal(obj)
	if err != nil {
		return input
	}
	return string(compact)
}
