package jsrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/schema"
)

// JSRunnerNode implements JavaScript execution for embedded
type JSRunnerNode struct {
	runtime.BaseNode
}

// NewJSRunnerNode creates a new jsrunner node instance
func NewJSRunnerNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	// Validate plugin type
	if config.PluginType != "plugin-js" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-js', got '%s'", config.PluginType)
	}

	return &JSRunnerNode{
		BaseNode: runtime.NewBaseNode(config),
	}, nil
}

// Process executes the JavaScript code
func (n *JSRunnerNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	// Parse configuration
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "failed to parse configuration", err))
	}

	// Apply defaults
	cfg.ApplyDefaults()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "invalid configuration", err))
	}

	// If input schema is configured, validate the input before executing script
	if cfg.HasInputSchema() {
		validatedInput, err := n.validateInput(input, &cfg)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		// Update input.Data with validated data
		input.Data = validatedInput
	}

	// Execute JavaScript with timeout
	ctx, cancel := context.WithTimeout(input.Ctx, time.Duration(cfg.Timeout)*time.Millisecond)
	defer cancel()

	result, err := n.executeScript(ctx, input, &cfg)
	if err != nil {
		// Check for timeout
		if ctx.Err() == context.DeadlineExceeded {
			return runtime.ErrorOutput(NewTimeoutError(n.NodeId(), "script execution timed out", input.ItemIndex, cfg.Timeout))
		}
		return runtime.ErrorOutput(err)
	}

	// If output schema is configured, validate the result
	if cfg.HasOutputSchema() {
		validatedResult, err := n.validateOutput(result, &cfg, input.ItemIndex)
		if err != nil {
			return runtime.ErrorOutput(err)
		}
		return runtime.SuccessOutput(validatedResult)
	}

	// Return result as-is
	return runtime.SuccessOutput(result)
}

// executeScript executes the JavaScript code and returns the result
func (n *JSRunnerNode) executeScript(ctx context.Context, input runtime.ProcessInput, cfg *Config) (map[string]interface{}, error) {
	// Create a new VM
	vm := goja.New()

	// Create utility registry
	utilityRegistry := NewUtilityRegistry()

	// Apply security sandbox
	if err := CreateSecureContext(vm, cfg); err != nil {
		return nil, NewConfigError(n.NodeId(), "failed to create secure context", err)
	}

	// Register utilities
	if err := utilityRegistry.RegisterEnabled(vm, cfg); err != nil {
		return nil, NewConfigError(n.NodeId(), "failed to register utilities", err)
	}

	// Inject input data as 'input' global
	if err := vm.Set("input", input.Data); err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to set input variable", input.ItemIndex, 0, 0, err)
	}

	// Inject input schema if provided (for reference in JS)
	if inputSchema := cfg.GetInputSchema(); inputSchema != nil {
		vm.Set("inputSchema", inputSchema)
	}

	// Inject output schema if provided (for reference in JS)
	if outputSchema := cfg.GetOutputSchema(); outputSchema != nil {
		vm.Set("outputSchema", outputSchema)
	}

	// Inject manual inputs if provided
	if len(cfg.ManualInputs) > 0 {
		vm.Set("manualInputs", cfg.ManualInputs)
	}

	// Wrap script for execution
	wrappedScript := wrapScript(cfg.Script)

	// Execute script with timeout
	resultChan := make(chan goja.Value, 1)
	errChan := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				if gojaErr, ok := r.(*goja.Exception); ok {
					errChan <- NewExecutionError(n.NodeId(), gojaErr.Error(), input.ItemIndex, 0, 0, gojaErr)
				} else {
					errChan <- NewExecutionError(n.NodeId(), fmt.Sprintf("%v", r), input.ItemIndex, 0, 0, nil)
				}
			}
		}()

		val, err := vm.RunString(wrappedScript)
		if err != nil {
			if gojaErr, ok := err.(*goja.Exception); ok {
				errChan <- NewExecutionError(n.NodeId(), gojaErr.Error(), input.ItemIndex, 0, 0, gojaErr)
			} else {
				errChan <- NewExecutionError(n.NodeId(), err.Error(), input.ItemIndex, 0, 0, err)
			}
			return
		}
		resultChan <- val
	}()

	// Wait for completion or timeout
	var val goja.Value
	select {
	case val = <-resultChan:
		// Success
	case execErr := <-errChan:
		return nil, execErr
	case <-ctx.Done():
		return nil, NewTimeoutError(n.NodeId(), "script execution timed out", input.ItemIndex, cfg.Timeout)
	}

	// Cleanup utilities
	if err := utilityRegistry.CleanupEnabled(vm, cfg); err != nil {
		// Non-fatal, just log
	}

	// Export result
	if val == nil || val == goja.Undefined() || val == goja.Null() {
		return map[string]interface{}{}, nil
	}

	exported := val.Export()

	// Convert to map
	resultMap, ok := exported.(map[string]interface{})
	if !ok {
		// If result is not a map, wrap it
		resultMap = map[string]interface{}{
			"result": exported,
		}
	}

	return resultMap, nil
}

// validateInput validates the input data against the configured input schema
func (n *JSRunnerNode) validateInput(input runtime.ProcessInput, cfg *Config) (map[string]interface{}, error) {
	// Get input schema (enriched by Elysium)
	inputSchema := cfg.GetInputSchema()
	if inputSchema == nil {
		return nil, NewConfigError(
			n.NodeId(),
			fmt.Sprintf("inputSchemaID '%s' was not enriched - ensure Elysium enrichment is configured", cfg.InputSchemaID),
			nil,
		)
	}

	// Marshal input data to JSON
	inputJSON, err := json.Marshal(input.Data)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal input data", input.ItemIndex, 0, 0, err)
	}

	// Marshal schema to JSON for ProcessWithSchema
	schemaJSON, err := json.Marshal(inputSchema)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal input schema", input.ItemIndex, 0, 0, err)
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema (structure and validate)
	schemaResult, err := engine.ProcessWithSchema(
		inputJSON,
		schemaJSON,
		schema.ProcessOptions{
			ApplyDefaults:    cfg.ApplySchemaDefaults,
			StructureData:    cfg.StructureData,
			StrictValidation: cfg.StrictValidation,
		},
	)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "input schema processing failed", input.ItemIndex, 0, 0, err)
	}

	// Check validation result
	if !schemaResult.Valid {
		errorMessages := make([]string, len(schemaResult.Errors))
		for i, err := range schemaResult.Errors {
			errorMessages[i] = fmt.Sprintf("%s: %s", err.Path, err.Message)
		}
		return nil, NewExecutionError(
			n.NodeId(),
			fmt.Sprintf("input validation failed: %v", errorMessages),
			input.ItemIndex,
			0,
			0,
			nil,
		)
	}

	// Unmarshal validated data
	var validatedMap map[string]interface{}
	if err := json.Unmarshal(schemaResult.Data, &validatedMap); err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to unmarshal validated input data", input.ItemIndex, 0, 0, err)
	}

	return validatedMap, nil
}

// validateOutput validates the script result against the configured schema
func (n *JSRunnerNode) validateOutput(result map[string]interface{}, cfg *Config, itemIndex int) (map[string]interface{}, error) {
	// Get output schema (enriched by Elysium)
	outputSchema := cfg.GetOutputSchema()
	if outputSchema == nil {
		return nil, NewConfigError(
			n.NodeId(),
			fmt.Sprintf("outputSchemaID '%s' was not enriched - ensure Elysium enrichment is configured", cfg.OutputSchemaID),
			nil,
		)
	}

	// Marshal result to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal result", itemIndex, 0, 0, err)
	}

	// Marshal schema to JSON for ProcessWithSchema
	schemaJSON, err := json.Marshal(outputSchema)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to marshal output schema", itemIndex, 0, 0, err)
	}

	// Create schema engine
	engine := schema.NewEngine()

	// Process with schema (structure and validate)
	schemaResult, err := engine.ProcessWithSchema(
		resultJSON,
		schemaJSON,
		schema.ProcessOptions{
			ApplyDefaults:    cfg.ApplySchemaDefaults,
			StructureData:    cfg.StructureData,
			StrictValidation: cfg.StrictValidation,
		},
	)
	if err != nil {
		return nil, NewExecutionError(n.NodeId(), "schema processing failed", itemIndex, 0, 0, err)
	}

	// Check validation result
	if !schemaResult.Valid {
		errorMessages := make([]string, len(schemaResult.Errors))
		for i, err := range schemaResult.Errors {
			errorMessages[i] = fmt.Sprintf("%s: %s", err.Path, err.Message)
		}
		return nil, NewExecutionError(
			n.NodeId(),
			fmt.Sprintf("validation failed: %v", errorMessages),
			itemIndex,
			0,
			0,
			nil,
		)
	}

	// Unmarshal validated data
	var validatedMap map[string]interface{}
	if err := json.Unmarshal(schemaResult.Data, &validatedMap); err != nil {
		return nil, NewExecutionError(n.NodeId(), "failed to unmarshal validated data", itemIndex, 0, 0, err)
	}

	return validatedMap, nil
}

// wrapScript wraps the user script in an IIFE for safe execution
func wrapScript(script string) string {
	// Trim whitespace
	script = strings.TrimSpace(script)

	// Check if script has explicit return statement or is multi-line
	hasReturn := strings.Contains(script, "return")
	isMultiLine := strings.Contains(script, "\n") || strings.Contains(script, ";")

	if hasReturn || isMultiLine {
		// Multi-statement script - wrap as-is
		return fmt.Sprintf("(function() {\n%s\n})()", script)
	}

	// Single expression - auto-return
	return fmt.Sprintf("(function() { return %s; })()", script)
}
