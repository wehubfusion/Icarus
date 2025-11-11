package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
)

func main() {
	fmt.Println("=== JavaScript Runner Examples ===")
	fmt.Println()

	// Example 1: Basic Execution
	example1()

	// Example 2: Working with Input
	example2()

	// Example 3: Using JSON Utility
	example3()

	// Example 4: Using Console Utility
	example4()

	// Example 5: Complex Calculation
	example5()

	// Example 6: Error Handling
	example6()

	// Example 7: Timeout Handling
	example7()

	// Example 8: Pool Statistics
	example8()
}

func example1() {
	fmt.Println("Example 1: Basic Execution")
	fmt.Println("--------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		// Simple calculation
		var result = 2 + 2;
		result
	`

	result, err := executor.ExecuteScript(context.Background(), script, nil)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Result: %v\n\n", result)
}

func example2() {
	fmt.Println("Example 2: Working with Input")
	fmt.Println("------------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		// Access input data and perform calculations
		var total = input.items.reduce(function(sum, item) {
			return sum + item.price * item.quantity;
		}, 0);
		
		({
			total: total,
			count: input.items.length,
			average: total / input.items.length
		})
	`

	input := map[string]interface{}{
		"items": []map[string]interface{}{
			{"name": "Apple", "price": 1.5, "quantity": 3},
			{"name": "Banana", "price": 0.8, "quantity": 5},
			{"name": "Orange", "price": 2.0, "quantity": 2},
		},
	}

	result, err := executor.ExecuteScript(context.Background(), script, input)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	resultJSON, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Result:\n%s\n\n", resultJSON)
}

func example3() {
	fmt.Println("Example 3: Using JSON Utility")
	fmt.Println("-----------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	config := jsrunner.Config{
		Script: `
			// Parse JSON string and manipulate data
			var data = JSON.parse(input.jsonString);
			data.processed = true;
			data.timestamp = Date.now();
			
			// Return stringified result
			({
				original: input.jsonString,
				processed: JSON.stringify(data)
			})
		`,
		EnabledUtilities: []string{"json"},
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputData := map[string]interface{}{
		"jsonString": `{"name":"Alice","age":30}`,
	}
	inputJSON, _ := json.Marshal(inputData)

	nodeConfig := embedded.NodeConfig{
		NodeID:        "example-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	var result map[string]interface{}
	json.Unmarshal(output, &result)
	resultJSON, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Result:\n%s\n\n", resultJSON)
}

func example4() {
	fmt.Println("Example 4: Using Console Utility")
	fmt.Println("--------------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		console.log('Starting calculation...');
		
		var numbers = [1, 2, 3, 4, 5];
		var sum = 0;
		
		for (var i = 0; i < numbers.length; i++) {
			console.log('Adding', numbers[i]);
			sum += numbers[i];
		}
		
		console.log('Total sum:', sum);
		
		sum
	`

	config := jsrunner.Config{
		Script:           script,
		EnabledUtilities: []string{"console"},
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "example-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	var result map[string]interface{}
	json.Unmarshal(output, &result)
	fmt.Printf("Result: %v\n\n", result["result"])
}

func example5() {
	fmt.Println("Example 5: Complex Calculation")
	fmt.Println("------------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		// Fibonacci sequence
		function fibonacci(n) {
			if (n <= 1) return n;
			return fibonacci(n - 1) + fibonacci(n - 2);
		}
		
		// Calculate statistics
		var numbers = input.numbers;
		var stats = {
			count: numbers.length,
			sum: numbers.reduce(function(a, b) { return a + b; }, 0),
			min: Math.min.apply(null, numbers),
			max: Math.max.apply(null, numbers),
			fibonacci_10: fibonacci(10)
		};
		
		stats.average = stats.sum / stats.count;
		
		stats
	`

	input := map[string]interface{}{
		"numbers": []int{15, 23, 8, 42, 16, 4, 31},
	}

	result, err := executor.ExecuteScript(context.Background(), script, input)
	if err != nil {
		log.Printf("Error: %v\n", err)
		return
	}

	resultJSON, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("Result:\n%s\n\n", resultJSON)
}

func example6() {
	fmt.Println("Example 6: Error Handling")
	fmt.Println("-------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		// This will cause a runtime error
		var obj = null;
		obj.property // Cannot read property of null
	`

	config := jsrunner.Config{
		Script: script,
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "example-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	output, err := executor.Execute(context.Background(), nodeConfig)

	fmt.Printf("Execution Error: %v\n", err)

	if output != nil {
		var result map[string]interface{}
		json.Unmarshal(output, &result)
		if errInfo, ok := result["error"].(map[string]interface{}); ok {
			fmt.Printf("Error Type: %v\n", errInfo["type"])
			fmt.Printf("Error Message: %v\n", errInfo["message"])
		}
	}
	fmt.Println()
}

func example7() {
	fmt.Println("Example 7: Timeout Handling")
	fmt.Println("---------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		// Simulate long-running operation
		var start = Date.now();
		var count = 0;
		while (Date.now() - start < 2000) {
			count++;
		}
		count
	`

	config := jsrunner.Config{
		Script:  script,
		Timeout: 500, // 500ms timeout
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "example-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	_, err := executor.Execute(context.Background(), nodeConfig)

	if err != nil {
		fmt.Printf("Expected timeout error: %v\n\n", err)
	}
}

func example8() {
	fmt.Println("Example 8: Pool Statistics")
	fmt.Println("--------------------------")

	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `42`

	// Execute multiple times to see pool in action
	for i := 0; i < 10; i++ {
		_, err := executor.ExecuteScript(context.Background(), script, nil)
		if err != nil {
			log.Printf("Error: %v\n", err)
		}
	}

	stats := executor.GetPoolStats()
	if stats != nil {
		fmt.Printf("Pool Statistics:\n")
		fmt.Printf("  Current Size: %d\n", stats.CurrentSize)
		fmt.Printf("  Min Size: %d\n", stats.MinSize)
		fmt.Printf("  Max Size: %d\n", stats.MaxSize)
		fmt.Printf("  Total Created: %d\n", stats.TotalCreated)
		fmt.Printf("  Total Acquired: %d\n", stats.TotalAcquired)
		fmt.Printf("  Total Released: %d\n", stats.TotalReleased)
		fmt.Printf("  Available: %d\n", stats.Available)
	}
	fmt.Println()
}
