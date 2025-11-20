package embedded

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
	"github.com/wehubfusion/Icarus/pkg/embedded/pathutil"
	"github.com/wehubfusion/Icarus/pkg/iteration"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// toPathutilStandardOutput converts embedded.StandardOutput to pathutil.StandardOutput
// This is needed to avoid circular dependency while keeping types compatible
func toPathutilStandardOutput(output *StandardOutput) *pathutil.StandardOutput {
	if output == nil {
		return nil
	}
	return &pathutil.StandardOutput{
		Meta:   output.Meta,
		Events: output.Events,
		Error:  output.Error,
		Result: output.Result,
	}
}

// Processor handles execution of embedded nodes
type Processor struct {
	registry    *ExecutorRegistry
	fieldMapper *FieldMapper
	concurrent  bool // Enable concurrent execution by depth
	limiter     *concurrency.Limiter
	logger      Logger
}

// NewProcessor creates a new embedded node processor with concurrent execution enabled by default
func NewProcessor(registry *ExecutorRegistry) *Processor {
	return &Processor{
		registry:    registry,
		fieldMapper: NewFieldMapper(nil),
		concurrent:  true, // Enable concurrent execution by default
		limiter:     nil,  // No limiter by default
		logger:      &NoOpLogger{},
	}
}

// NewProcessorWithConfig creates a new embedded node processor with custom configuration
func NewProcessorWithConfig(registry *ExecutorRegistry, concurrent bool) *Processor {
	return &Processor{
		registry:    registry,
		fieldMapper: NewFieldMapper(nil),
		concurrent:  concurrent,
		limiter:     nil, // No limiter by default
		logger:      &NoOpLogger{},
	}
}

// NewProcessorWithLimiter creates a new embedded node processor with concurrency limiter
func NewProcessorWithLimiter(registry *ExecutorRegistry, concurrent bool, limiter *concurrency.Limiter) *Processor {
	return &Processor{
		registry:    registry,
		fieldMapper: NewFieldMapper(nil),
		concurrent:  concurrent,
		limiter:     limiter,
		logger:      &NoOpLogger{},
	}
}

// NewProcessorWithLogger creates a new embedded node processor with logger support
func NewProcessorWithLogger(registry *ExecutorRegistry, concurrent bool, limiter *concurrency.Limiter, logger Logger) *Processor {
	if logger == nil {
		logger = &NoOpLogger{}
	}
	return &Processor{
		registry:    registry,
		fieldMapper: NewFieldMapper(logger),
		concurrent:  concurrent,
		limiter:     limiter,
		logger:      logger,
	}
}

// ProcessEmbeddedNodes processes embedded nodes either concurrently or sequentially
// based on the processor configuration. Dispatches to the appropriate implementation.
func (p *Processor) ProcessEmbeddedNodes(
	ctx context.Context,
	msg *message.Message,
	parentOutput *StandardOutput,
) ([]EmbeddedNodeResult, error) {
	if p.concurrent {
		return p.ProcessEmbeddedNodesConcurrent(ctx, msg, parentOutput)
	}
	return p.processEmbeddedNodesSequential(ctx, msg, parentOutput)
}

// processEmbeddedNodesSequential processes all embedded nodes in a message sequentially
// It chains the output of each node to the next, applying field mappings
func (p *Processor) processEmbeddedNodesSequential(
	ctx context.Context,
	msg *message.Message,
	parentOutput *StandardOutput,
) ([]EmbeddedNodeResult, error) {
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	if len(msg.EmbeddedNodes) == 0 {
		return []EmbeddedNodeResult{}, nil
	}

	// Sort embedded nodes by execution order
	nodes := make([]message.EmbeddedNode, len(msg.EmbeddedNodes))
	copy(nodes, msg.EmbeddedNodes)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ExecutionOrder < nodes[j].ExecutionOrder
	})

	// Create output registry and store parent output
	outputRegistry := NewOutputRegistry()
	outputRegistry.Set(msg.Node.NodeID, parentOutput)

	// Process each node sequentially
	results := make([]EmbeddedNodeResult, 0, len(nodes))

	for _, embNode := range nodes {
		result := p.processNode(ctx, embNode, outputRegistry)
		results = append(results, result)
	}

	return results, nil
}

// ProcessEmbeddedNodesConcurrent processes embedded nodes concurrently by depth level
// Nodes at the same depth level execute in parallel, while maintaining dependency order across levels
func (p *Processor) ProcessEmbeddedNodesConcurrent(
	ctx context.Context,
	msg *message.Message,
	parentOutput *StandardOutput,
) ([]EmbeddedNodeResult, error) {
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	if len(msg.EmbeddedNodes) == 0 {
		return []EmbeddedNodeResult{}, nil
	}

	// Group nodes by depth
	depthGroups := groupNodesByDepth(msg.EmbeddedNodes)
	maxDepth := getMaxDepth(depthGroups)

	// Create thread-safe output registry
	outputRegistry := NewOutputRegistry()
	outputRegistry.Set(msg.Node.NodeID, parentOutput)

	// Process each depth level sequentially
	// Within each level, process nodes concurrently
	var allResults []EmbeddedNodeResult
	var resultsMu sync.Mutex

	for depth := 0; depth <= maxDepth; depth++ {
		nodes := depthGroups[depth]
		if len(nodes) == 0 {
			continue
		}

		var wg sync.WaitGroup
		for _, node := range nodes {
			wg.Add(1)

			// Use limiter if available, otherwise spawn goroutine directly
			if p.limiter != nil {
				// Capture node in closure
				embNode := node
				err := p.limiter.Go(ctx, func() error {
					defer wg.Done()
					result := p.processNode(ctx, embNode, outputRegistry)

					resultsMu.Lock()
					allResults = append(allResults, result)
					resultsMu.Unlock()

					// Return error if processing failed (for circuit breaker)
					if result.Status == "failed" {
						return fmt.Errorf("node %s failed: %s", result.NodeID, result.Error)
					}
					return nil
				})

				// If limiter fails to acquire (circuit breaker open or context cancelled)
				if err != nil {
					wg.Done() // Balance the Add(1) above

					// Wrap limiter error
					wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, 0, fmt.Errorf("limiter error: %w", err))

					// Store error output in registry
					outputRegistry.Set(embNode.NodeID, wrappedOutput)

					result := EmbeddedNodeResult{
						NodeID:               embNode.NodeID,
						PluginType:           embNode.PluginType,
						Status:               "failed",
						Output:               wrappedOutput,
						Error:                fmt.Sprintf("limiter error: %v", err),
						ExecutionOrder:       embNode.ExecutionOrder,
						ProcessingDurationMs: 0,
					}
					resultsMu.Lock()
					allResults = append(allResults, result)
					resultsMu.Unlock()
				}
			} else {
				// No limiter, spawn goroutine directly
				go func(embNode message.EmbeddedNode) {
					defer wg.Done()
					result := p.processNode(ctx, embNode, outputRegistry)

					resultsMu.Lock()
					allResults = append(allResults, result)
					resultsMu.Unlock()
				}(node)
			}
		}
		wg.Wait() // Wait for all nodes at this depth to complete
	}

	return allResults, nil
}

// groupNodesByDepth groups embedded nodes by their depth value
// Returns a map where the key is the depth level and the value is a slice of nodes at that depth
func groupNodesByDepth(nodes []message.EmbeddedNode) map[int][]message.EmbeddedNode {
	groups := make(map[int][]message.EmbeddedNode)
	for _, node := range nodes {
		groups[node.Depth] = append(groups[node.Depth], node)
	}
	return groups
}

// getMaxDepth finds the maximum depth value in the depth groups
func getMaxDepth(depthGroups map[int][]message.EmbeddedNode) int {
	maxDepth := 0
	for depth := range depthGroups {
		if depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

// detectAutoIteration checks if the input is an array that should be auto-iterated
// Returns (isArray bool, items []interface{}, error)
// Detects three patterns:
// 1. Array of data envelopes: [{"data": "<base64>"}, ...]
// 2. Data field containing array: {"data": [item1, item2, ...]}
// 3. Root-level array: [item1, item2, ...]
func (p *Processor) detectAutoIteration(mappedInput []byte) (bool, []interface{}, error) {
	if len(mappedInput) == 0 {
		return false, nil, nil
	}

	// Try to parse as generic map first
	var inputMap map[string]interface{}
	if err := json.Unmarshal(mappedInput, &inputMap); err == nil {
		// Pattern 2: Check if "data" field contains an array
		if dataField, exists := inputMap["data"]; exists {
			if dataArray, ok := dataField.([]interface{}); ok {
				// Found array in data field (even if empty)
				return true, dataArray, nil
			}
		}
		// Not an array pattern
		return false, nil, nil
	}

	// Try to parse as array
	var inputArray []interface{}
	if err := json.Unmarshal(mappedInput, &inputArray); err != nil {
		// Not valid JSON or not an array - no auto-iteration
		return false, nil, nil
	}

	// Pattern 1 & 3: We have a root-level array (even if empty)
	// Check if first item is a data envelope to potentially unwrap
	if len(inputArray) > 0 {
		if itemMap, ok := inputArray[0].(map[string]interface{}); ok {
			if _, hasData := itemMap["data"]; hasData {
				// Pattern 1: Array of data envelopes [{"data": "..."}, ...]
				// Return as-is, items will be unwrapped during iteration
				return true, inputArray, nil
			}
		}
	}

	// Pattern 3: Plain array [item1, item2, ...]
	return true, inputArray, nil
}

// processWithAutoIteration handles auto-detected array iteration concurrently
// Processes each item through the plugin and aggregates results into single StandardOutput
func (p *Processor) processWithAutoIteration(
	ctx context.Context,
	embNode message.EmbeddedNode,
	arrayItems []interface{},
	startTime time.Time,
	outputRegistry *OutputRegistry,
) EmbeddedNodeResult {
	// Create iterator with concurrency limiter support
	var iterator *iteration.Iterator
	maxConcurrent := 0 // Default to runtime.NumCPU()

	// TODO: Read maxConcurrent from embNode.Configuration if available
	// For now, use default

	if p.limiter != nil {
		iterator = iteration.NewIteratorWithLimiter(iteration.Config{
			MaxConcurrent: maxConcurrent,
		}, p.limiter)
	} else {
		iterator = iteration.NewIterator(iteration.Config{
			MaxConcurrent: maxConcurrent,
		})
	}

	// Process each item through the plugin
	processFunc := func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		// Construct input envelope for this item
		var itemInput map[string]interface{}

		// If item is already a map, use it as base
		if itemMap, ok := item.(map[string]interface{}); ok {
			itemInput = itemMap
		} else {
			// Wrap primitive or other types in data field
			itemInput = map[string]interface{}{
				"data": item,
			}
		}

		// Marshal to JSON for plugin execution
		itemInputBytes, err := json.Marshal(itemInput)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal item %d input: %w", index, err)
		}

		// Create node configuration for this item
		nodeConfig := NodeConfig{
			NodeID:        embNode.NodeID,
			PluginType:    embNode.PluginType,
			Configuration: embNode.Configuration,
			Input:         itemInputBytes,
			Connection:    embNode.Connection,
			Schema:        embNode.Schema,
		}

		// Execute plugin for this item
		nodeOutput, err := p.registry.Execute(ctx, nodeConfig)
		if err != nil {
			return nil, fmt.Errorf("failed executing item %d: %w", index, err)
		}

		// Parse the StandardOutput from plugin
		var output StandardOutput
		if err := json.Unmarshal(nodeOutput, &output); err != nil {
			return nil, fmt.Errorf("failed parsing item %d output: %w", index, err)
		}

		// Extract the result field
		return output.Result, nil
	}

	// Execute iteration concurrently
	results, err := iterator.Process(ctx, arrayItems, processFunc)
	execTime := time.Since(startTime).Milliseconds()

	if err != nil {
		// Iteration failed - wrap error
		wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime, fmt.Errorf("auto-iteration failed: %w", err))
		outputRegistry.Set(embNode.NodeID, wrappedOutput)

		return EmbeddedNodeResult{
			NodeID:               embNode.NodeID,
			PluginType:           embNode.PluginType,
			Status:               "failed",
			Output:               wrappedOutput,
			Error:                fmt.Sprintf("auto-iteration failed: %v", err),
			ExecutionOrder:       embNode.ExecutionOrder,
			ProcessingDurationMs: execTime,
		}
	}

	// Wrap aggregated results in single StandardOutput
	// The result field contains the flat array of all item results
	wrappedOutput := &StandardOutput{
		Meta: MetaData{
			Status:          "success",
			NodeID:          embNode.NodeID,
			PluginType:      embNode.PluginType,
			ExecutionTimeMs: execTime,
			Timestamp:       time.Now(),
		},
		Events: EventEndpoints{
			Success: true,
			Error:   false,
		},
		Result: results, // Flat array: [result1, result2, result3]
	}

	outputRegistry.Set(embNode.NodeID, wrappedOutput)

	return EmbeddedNodeResult{
		NodeID:               embNode.NodeID,
		PluginType:           embNode.PluginType,
		Status:               "success",
		Output:               wrappedOutput,
		Error:                "",
		ExecutionOrder:       embNode.ExecutionOrder,
		ProcessingDurationMs: execTime,
	}
}

// processNode executes a single embedded node and returns the result
// This function is extracted to support both sequential and concurrent execution
func (p *Processor) processNode(
	ctx context.Context,
	embNode message.EmbeddedNode,
	outputRegistry *OutputRegistry,
) EmbeddedNodeResult {
	startTime := time.Now()

	// Check event trigger conditions before executing
	shouldExecute, skipReason := p.checkEventTriggers(embNode, outputRegistry)
	if !shouldExecute {
		// Event trigger conditions not met - skip this node
		wrappedOutput := WrapSkipped(embNode.NodeID, embNode.PluginType, skipReason)

		// Store skipped output in registry for downstream nodes to reference
		outputRegistry.Set(embNode.NodeID, wrappedOutput)

		return EmbeddedNodeResult{
			NodeID:               embNode.NodeID,
			PluginType:           embNode.PluginType,
			Status:               "skipped",
			Output:               wrappedOutput,
			Error:                skipReason,
			ExecutionOrder:       embNode.ExecutionOrder,
			ProcessingDurationMs: time.Since(startTime).Milliseconds(),
		}
	}

	// Apply field mappings to prepare input for this node
	mappedInput, err := p.fieldMapper.ApplyMappings(outputRegistry, embNode.FieldMappings, []byte("{}"))
	if err != nil {
		// Field mapping failed - wrap error output
		execTime := time.Since(startTime).Milliseconds()
		wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime, fmt.Errorf("field mapping failed: %w", err))

		// ALWAYS store in registry (even errors)
		outputRegistry.Set(embNode.NodeID, wrappedOutput)

		return EmbeddedNodeResult{
			NodeID:               embNode.NodeID,
			PluginType:           embNode.PluginType,
			Status:               "failed",
			Output:               wrappedOutput,
			Error:                fmt.Sprintf("field mapping failed: %v", err),
			ExecutionOrder:       embNode.ExecutionOrder,
			ProcessingDurationMs: execTime,
		}
	}

	// Check if input is an array requiring auto-iteration
	isArray, arrayItems, err := p.detectAutoIteration(mappedInput)
	if err != nil {
		// Detection failed - wrap error output
		execTime := time.Since(startTime).Milliseconds()
		wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime, fmt.Errorf("auto-iteration detection failed: %w", err))
		outputRegistry.Set(embNode.NodeID, wrappedOutput)

		return EmbeddedNodeResult{
			NodeID:               embNode.NodeID,
			PluginType:           embNode.PluginType,
			Status:               "failed",
			Output:               wrappedOutput,
			Error:                fmt.Sprintf("auto-iteration detection failed: %v", err),
			ExecutionOrder:       embNode.ExecutionOrder,
			ProcessingDurationMs: execTime,
		}
	}

	if isArray {
		// Route to auto-iteration handler (even for empty arrays)
		return p.processWithAutoIteration(ctx, embNode, arrayItems, startTime, outputRegistry)
	}

	// Single-item processing (normal flow)
	if embNode.PluginType == "plugin-js" || embNode.PluginType == "plugin-jsrunner" {
		p.logger.Info("JS Runner input mapped",
			Field{Key: "node_id", Value: embNode.NodeID},
			Field{Key: "plugin_type", Value: embNode.PluginType},
			Field{Key: "mapped_input", Value: string(mappedInput)},
		)
	}

	// Create node configuration
	nodeConfig := NodeConfig{
		NodeID:        embNode.NodeID,
		PluginType:    embNode.PluginType,
		Configuration: embNode.Configuration,
		Input:         mappedInput,
		Connection:    embNode.Connection,
		Schema:        embNode.Schema,
	}

	// Execute the node
	nodeOutput, err := p.registry.Execute(ctx, nodeConfig)
	execTime := time.Since(startTime).Milliseconds()

	var wrappedOutput *StandardOutput
	status := "success"
	errorMsg := ""

	if err != nil {
		// Execution failed - wrap error output
		wrappedOutput = WrapError(embNode.NodeID, embNode.PluginType, execTime, err)
		status = "failed"
		errorMsg = err.Error()
	} else {
		// Execution succeeded - wrap success output
		wrappedOutput = WrapSuccess(embNode.NodeID, embNode.PluginType, execTime, nodeOutput)
	}

	// ALWAYS store in registry (even errors)
	outputRegistry.Set(embNode.NodeID, wrappedOutput)

	return EmbeddedNodeResult{
		NodeID:               embNode.NodeID,
		PluginType:           embNode.PluginType,
		Status:               status,
		Output:               wrappedOutput,
		Error:                errorMsg,
		ExecutionOrder:       embNode.ExecutionOrder,
		ProcessingDurationMs: execTime,
	}
}

// checkEventTriggers checks if all event trigger conditions are met for a node
// Returns (shouldExecute bool, skipReason string)
func (p *Processor) checkEventTriggers(
	embNode message.EmbeddedNode,
	outputRegistry *OutputRegistry,
) (bool, string) {
	// Find all event trigger field mappings
	eventTriggers := []message.FieldMapping{}
	for _, mapping := range embNode.FieldMappings {
		if mapping.IsEventTrigger {
			eventTriggers = append(eventTriggers, mapping)
		}
	}

	// If no event triggers, always execute
	if len(eventTriggers) == 0 {
		return true, ""
	}

	// Check each event trigger - ALL must be satisfied for node to execute
	for _, trigger := range eventTriggers {
		// Get source output from registry
		sourceOutput, exists := outputRegistry.Get(trigger.SourceNodeID)
		if !exists {
			// Source node hasn't executed yet or failed
			return false, fmt.Sprintf("event trigger source node '%s' not found", trigger.SourceNodeID)
		}

		// Check if the trigger endpoint has data and is truthy using namespace-aware navigation
		sourceValue, valueExists := pathutil.NavigatePath(toPathutilStandardOutput(sourceOutput), trigger.SourceEndpoint)

		// Event is NOT fired if:
		// - Endpoint doesn't exist
		// - Value is null
		// - Value is false (boolean)
		// - Value is empty string
		if !valueExists {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' not found",
				trigger.SourceEndpoint, trigger.SourceNodeID)
		}

		if sourceValue == nil {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' is null",
				trigger.SourceEndpoint, trigger.SourceNodeID)
		}

		if boolVal, ok := sourceValue.(bool); ok && !boolVal {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' is false",
				trigger.SourceEndpoint, trigger.SourceNodeID)
		}

		if strVal, ok := sourceValue.(string); ok && strVal == "" {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' is empty",
				trigger.SourceEndpoint, trigger.SourceNodeID)
		}
	}

	// All event triggers satisfied
	return true, ""
}
