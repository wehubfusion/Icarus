package embedded

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/wehubfusion/Icarus/pkg/concurrency"
	"github.com/wehubfusion/Icarus/pkg/embedded/pathutil"
	"github.com/wehubfusion/Icarus/pkg/iteration"
	"github.com/wehubfusion/Icarus/pkg/message"
)

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
	parentOutput []byte,
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
	parentOutput []byte,
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
	parentOutput []byte,
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

	// Check if this node has iterate mappings
	hasIterateMapping := false
	var iterateArray []interface{}

	for _, mapping := range embNode.FieldMappings {
		if mapping.Iterate {
			// Get source output from registry for iterate check
			sourceOutput, exists := outputRegistry.Get(mapping.SourceNodeID)
			if exists {
				sourceValue := gjson.GetBytes(sourceOutput, mapping.SourceEndpoint)
				if sourceValue.IsArray() {
					hasIterateMapping = true
					iterateArray = sourceValue.Value().([]interface{})
					break
				}
			}
		}
	}

	if hasIterateMapping {
		// Process array items using iteration package
		itemResults, err := p.processArrayIteration(ctx, embNode, iterateArray, outputRegistry)
		execTime := time.Since(startTime).Milliseconds()

		if err != nil {
			// Record iteration failure - wrap error output
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime, fmt.Errorf("array iteration failed: %w", err))

			// ALWAYS store in registry (even errors)
			outputRegistry.Set(embNode.NodeID, wrappedOutput)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("array iteration failed: %v", err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		// Aggregate item results into single output and wrap
		aggregated, _ := json.Marshal(itemResults)
		wrappedOutput := WrapSuccess(embNode.NodeID, embNode.PluginType, execTime, aggregated)

		// Store wrapped result in registry
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

	// Normal processing (no iteration)
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

	// Debug: Log mapped input for JS runner nodes
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

	var wrappedOutput []byte
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
		sourceValue, valueExists := pathutil.NavigatePath(sourceOutput, trigger.SourceEndpoint)

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

// processArrayIteration processes an array of items for a single embedded node
// Each item is processed through the node individually, returns array of results
func (p *Processor) processArrayIteration(
	ctx context.Context,
	embNode message.EmbeddedNode,
	items []interface{},
	outputRegistry *OutputRegistry,
) ([]interface{}, error) {
	// Create iterator with sequential strategy by default
	// Could be made configurable via embNode.Configuration in the future
	var iterator *iteration.Iterator

	if p.limiter != nil {
		iterator = iteration.NewIteratorWithLimiter(iteration.Config{
			Strategy: iteration.StrategySequential,
		}, p.limiter)
	} else {
		iterator = iteration.NewIterator(iteration.Config{
			Strategy: iteration.StrategySequential,
		})
	}

	// Process each item through the node
	return iterator.Process(ctx, items, func(ctx context.Context, item interface{}, index int) (interface{}, error) {
		// Marshal item to JSON
		itemJSON, err := json.Marshal(item)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal item: %w", err)
		}

		// Create a local registry for this iteration that includes the item
		// This allows mappings to reference both the current item and other nodes
		localRegistry := NewOutputRegistry()

		// Copy all existing outputs from parent registry
		for nodeID, output := range outputRegistry.GetAll() {
			localRegistry.Set(nodeID, output)
		}

		// Add current item as a special source that can be referenced
		// Using a temporary ID for the iteration item
		itemNodeID := fmt.Sprintf("%s_item_%d", embNode.NodeID, index)
		localRegistry.Set(itemNodeID, itemJSON)

		// Apply field mappings with local registry
		mappedInput, err := p.fieldMapper.ApplyMappings(localRegistry, embNode.FieldMappings, []byte("{}"))
		if err != nil {
			return nil, fmt.Errorf("field mapping failed: %w", err)
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

		// Execute node for this item
		output, err := p.registry.Execute(ctx, nodeConfig)
		if err != nil {
			return nil, err
		}

		// Parse output back to interface for aggregation
		var result interface{}
		if err := json.Unmarshal(output, &result); err != nil {
			// If unmarshal fails, return raw bytes as string
			return string(output), nil
		}

		return result, nil
	})
}
