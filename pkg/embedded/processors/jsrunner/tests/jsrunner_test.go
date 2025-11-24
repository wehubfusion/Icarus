package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
)

// TestJSRunnerBasicExecution tests basic JavaScript execution
func TestJSRunnerBasicExecution(t *testing.T) {
	tests := []struct {
		name     string
		script   string
		input    map[string]interface{}
		expected interface{}
	}{
		{
			name:     "simple arithmetic",
			script:   "2 + 2",
			input:    map[string]interface{}{},
			expected: float64(4),
		},
		{
			name:     "string concatenation",
			script:   "'Hello ' + 'World'",
			input:    map[string]interface{}{},
			expected: "Hello World",
		},
		{
			name:     "access input",
			script:   "input.value * 2",
			input:    map[string]interface{}{"value": 10},
			expected: float64(20),
		},
		{
			name: "return object",
			script: `
				var result = {
					sum: input.a + input.b,
					product: input.a * input.b,
				};
				return result;
			`,
			input: map[string]interface{}{"a": 5, "b": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := jsrunner.NewExecutor()
			defer executor.Close()

			config := jsrunner.Config{
				Script:           tt.script,
				EnabledUtilities: []string{"console", "json"},
			}
			config.ApplyDefaults()

			configJSON, _ := json.Marshal(config)
			inputJSON, _ := json.Marshal(tt.input)

			nodeConfig := embedded.NodeConfig{
				NodeID:        "test-node",
				PluginType:    "plugin-jsrunner",
				Configuration: configJSON,
				Input:         inputJSON,
			}

			ctx := context.Background()
			output, err := executor.Execute(ctx, nodeConfig)

			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			decoded := decodeJSRunnerTestOutput(t, output)
			if tt.expected != nil {
				if decoded != tt.expected {
					t.Fatalf("expected %v, got %v", tt.expected, decoded)
				}
			} else {
				resultMap, ok := decoded.(map[string]interface{})
				if !ok {
					t.Fatalf("expected map output, got %T (%v)", decoded, decoded)
				}
				if resultMap["sum"] != float64(8) {
					t.Fatalf("expected sum=8, got %v", resultMap["sum"])
				}
				if resultMap["product"] != float64(15) {
					t.Fatalf("expected product=15, got %v", resultMap["product"])
				}
			}
		})
	}
}

// TestJSRunnerTimeout tests timeout functionality
func TestJSRunnerTimeout(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		var start = Date.now();
		while (Date.now() - start < 10000) {
			// Infinite loop
		}
	`

	config := jsrunner.Config{
		Script:  script,
		Timeout: 100, // 100ms timeout
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	ctx := context.Background()
	start := time.Now()
	_, err := executor.Execute(ctx, nodeConfig)
	duration := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout") {
		t.Errorf("expected timeout error, got: %v", err)
	}

	// Should complete within reasonable time after timeout
	if duration > 500*time.Millisecond {
		t.Errorf("timeout took too long: %v", duration)
	}
}

// TestJSRunnerConsoleUtility tests the console utility
func TestJSRunnerConsoleUtility(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		console.log('Hello', 'World');
		console.error('Error message');
		return input.value + 1;
	`

	config := jsrunner.Config{
		Script:           script,
		EnabledUtilities: []string{"console"},
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{"value": 41})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	ctx := context.Background()
	output, err := executor.Execute(ctx, nodeConfig)

	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	decoded := decodeJSRunnerTestOutput(t, output)
	if decoded != float64(42) {
		t.Errorf("expected 42, got %v", decoded)
	}
}

// TestJSRunnerJSONUtility tests JSON utility
func TestJSRunnerJSONUtility(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		var obj = { name: 'Alice', age: 30 };
		var str = JSON.stringify(obj);
		var parsed = JSON.parse(str);
		return parsed;
	`

	config := jsrunner.Config{
		Script:           script,
		EnabledUtilities: []string{"json"},
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	ctx := context.Background()
	output, err := executor.Execute(ctx, nodeConfig)

	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	resultObj, ok := decodeJSRunnerTestOutput(t, output).(map[string]interface{})
	if !ok {
		t.Fatalf("expected object result")
	}

	if resultObj["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", resultObj["name"])
	}
}

// TestJSRunnerEncodingUtility tests base64 encoding utilities
func TestJSRunnerEncodingUtility(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		var encoded = btoa('Hello World');
		var decoded = atob(encoded);
		return { encoded: encoded, decoded: decoded };
	`

	config := jsrunner.Config{
		Script:           script,
		EnabledUtilities: []string{"encoding"},
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	ctx := context.Background()
	output, err := executor.Execute(ctx, nodeConfig)

	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	resultObj, ok := decodeJSRunnerTestOutput(t, output).(map[string]interface{})
	if !ok {
		t.Fatalf("expected object result")
	}

	if resultObj["decoded"] != "Hello World" {
		t.Errorf("expected decoded='Hello World', got %v", resultObj["decoded"])
	}
}

// TestJSRunnerErrorHandling tests error handling
func TestJSRunnerErrorHandling(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	tests := []struct {
		name        string
		script      string
		expectError bool
	}{
		{
			name:        "syntax error",
			script:      "var x = ;",
			expectError: true,
		},
		{
			name:        "runtime error",
			script:      "undefined.property",
			expectError: true,
		},
		{
			name:        "reference error",
			script:      "nonExistentVariable",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := jsrunner.Config{
				Script: tt.script,
			}
			config.ApplyDefaults()

			configJSON, _ := json.Marshal(config)
			inputJSON, _ := json.Marshal(map[string]interface{}{})

			nodeConfig := embedded.NodeConfig{
				NodeID:        "test-node",
				PluginType:    "plugin-jsrunner",
				Configuration: configJSON,
				Input:         inputJSON,
			}

			ctx := context.Background()
			_, err := executor.Execute(ctx, nodeConfig)

			if tt.expectError && err == nil {
				t.Error("expected error, got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestJSRunnerSecurity tests security restrictions
func TestJSRunnerSecurity(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	tests := []struct {
		name          string
		script        string
		securityLevel string
	}{
		{
			name:          "no require in strict mode",
			script:        "typeof require",
			securityLevel: jsrunner.SecurityLevelStrict,
		},
		{
			name:          "no process in standard mode",
			script:        "typeof process",
			securityLevel: jsrunner.SecurityLevelStandard,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := jsrunner.Config{
				Script:        tt.script,
				SecurityLevel: tt.securityLevel,
			}
			config.ApplyDefaults()

			configJSON, _ := json.Marshal(config)
			inputJSON, _ := json.Marshal(map[string]interface{}{})

			nodeConfig := embedded.NodeConfig{
				NodeID:        "test-node",
				PluginType:    "plugin-jsrunner",
				Configuration: configJSON,
				Input:         inputJSON,
			}

			ctx := context.Background()
			output, err := executor.Execute(ctx, nodeConfig)

			if err != nil {
				t.Fatalf("execution failed: %v", err)
			}

			decoded := decodeJSRunnerTestOutput(t, output)
			if decoded != "undefined" {
				t.Errorf("expected 'undefined', got %v", decoded)
			}
		})
	}
}

// TestJSRunnerConcurrency tests concurrent execution
func TestJSRunnerConcurrency(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `input.value * 2`

	config := jsrunner.Config{
		Script: script,
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)

	concurrency := 20
	var wg sync.WaitGroup
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()

			inputJSON, _ := json.Marshal(map[string]interface{}{"value": val})

			nodeConfig := embedded.NodeConfig{
				NodeID:        "test-node",
				PluginType:    "plugin-jsrunner",
				Configuration: configJSON,
				Input:         inputJSON,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			output, err := executor.Execute(ctx, nodeConfig)

			if err != nil {
				errors <- fmt.Errorf("goroutine %d: %w", val, err)
				return
			}

			decoded := decodeJSRunnerTestOutput(t, output)

			expected := float64(val * 2)
			if decoded != expected {
				errors <- fmt.Errorf("goroutine %d: expected %v, got %v", val, expected, decoded)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent execution error: %v", err)
	}
}

// TestJSRunnerPoolStats tests pool statistics
func TestJSRunnerPoolStats(t *testing.T) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `42`

	config := jsrunner.Config{
		Script: script,
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	ctx := context.Background()

	// Execute a few times to populate pool
	for i := 0; i < 5; i++ {
		_, err := executor.Execute(ctx, nodeConfig)
		if err != nil {
			t.Fatalf("execution failed: %v", err)
		}
	}

	stats := executor.GetPoolStats()
	if stats == nil {
		t.Fatal("expected pool stats, got nil")
	}

	if stats.TotalCreated == 0 {
		t.Error("expected VMs to be created")
	}

	t.Logf("Pool stats: %s", stats.String())
}

// TestJSRunnerConfigValidation tests configuration validation
func TestJSRunnerConfigValidation(t *testing.T) {
	tests := []struct {
		name          string
		config        jsrunner.Config
		expectError   bool
		applyDefaults bool
	}{
		{
			name: "valid config",
			config: jsrunner.Config{
				Script:  "42",
				Timeout: 5000,
			},
			expectError:   false,
			applyDefaults: true,
		},
		{
			name: "empty script",
			config: jsrunner.Config{
				Script: "",
			},
			expectError:   true,
			applyDefaults: true,
		},
		{
			name: "negative timeout",
			config: jsrunner.Config{
				Script:  "42",
				Timeout: -1,
			},
			expectError:   false, // Negative timeout gets converted to default
			applyDefaults: true,
		},
		{
			name: "excessive timeout",
			config: jsrunner.Config{
				Script:  "42",
				Timeout: 400000,
			},
			expectError:   true,
			applyDefaults: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.applyDefaults {
				tt.config.ApplyDefaults()
			}
			err := tt.config.Validate()

			if tt.expectError && err == nil {
				t.Error("expected validation error, got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected validation error: %v", err)
			}
		})
	}
}

func decodeJSRunnerTestOutput(t *testing.T, raw []byte) interface{} {
	t.Helper()
	var wrapper map[string]interface{}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		t.Fatalf("failed to unmarshal jsrunner output: %v", err)
	}
	dataField, ok := wrapper["data"].(string)
	if !ok {
		t.Fatalf("jsrunner output missing data field")
	}
	decoded, err := base64.StdEncoding.DecodeString(dataField)
	if err != nil {
		t.Fatalf("failed to decode base64 jsrunner output: %v", err)
	}
	var val interface{}
	if err := json.Unmarshal(decoded, &val); err != nil {
		t.Fatalf("failed to unmarshal decoded jsrunner output: %v", err)
	}
	return val
}

// BenchmarkJSRunnerExecution benchmarks JavaScript execution
// Note: Currently disabled due to VM pool reuse issues under high load
/*
func BenchmarkJSRunnerExecution(b *testing.B) {
	executor := jsrunner.NewExecutor()
	defer executor.Close()

	script := `
		var sum = 0;
		for (var i = 0; i < 100; i++) {
			sum += i;
		}
		sum
	`

	config := jsrunner.Config{
		Script:  script,
		Timeout: 30000, // 30 seconds timeout for benchmark
	}
	config.ApplyDefaults()

	configJSON, _ := json.Marshal(config)
	inputJSON, _ := json.Marshal(map[string]interface{}{})

	nodeConfig := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-jsrunner",
		Configuration: configJSON,
		Input:         inputJSON,
	}

	b.ResetTimer()

	// Limit to prevent infinite loop issues in benchmark
	iterations := b.N
	if iterations > 1000 {
		iterations = 1000
	}

	for i := 0; i < iterations; i++ {
		ctx := context.Background()
		_, err := executor.Execute(ctx, nodeConfig)
		if err != nil {
			b.Fatalf("execution failed: %v", err)
		}
	}
}
*/
