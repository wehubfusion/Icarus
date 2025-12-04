package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime/pathutil"
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
// consumerGraph tracks which downstream nodes consume each source node's output for auto-cleanup.
func (p *Processor) ProcessEmbeddedNodes(
	ctx context.Context,
	msg *message.Message,
	parentOutput *StandardOutput,
	consumerGraph map[string][]string,
) ([]EmbeddedNodeResult, error) {
	if p.concurrent {
		return p.ProcessEmbeddedNodesConcurrent(ctx, msg, parentOutput, consumerGraph)
	}
	return p.processEmbeddedNodesSequential(ctx, msg, parentOutput, consumerGraph)
}

// processEmbeddedNodesSequential processes all embedded nodes in a message sequentially
// It chains the output of each node to the next, applying field mappings
func (p *Processor) processEmbeddedNodesSequential(
	ctx context.Context,
	msg *message.Message,
	parentOutput *StandardOutput,
	consumerGraph map[string][]string,
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

	// Consumer graph is REQUIRED for array coordination - no backward compatibility
	if len(consumerGraph) == 0 {
		return nil, fmt.Errorf("consumer graph is required for embedded node execution (ensure Zeus computed it)")
	}

	storage := NewSmartStorageWithConsumers(consumerGraph, p.logger)
	p.logger.Debug("Initialized SmartStorage with consumer graph",
		Field{Key: "source_count", Value: len(consumerGraph)})

	// Store parent output in storage with array detection
	parentResult := map[string]interface{}{
		"_meta":   parentOutput.Meta,
		"_events": parentOutput.Events,
		"_error":  parentOutput.Error,
		"result":  parentOutput.Result,
	}

	// Detect if parent output is an array result
	var iterationCtx *IterationContext
	if parentOutput.Result != nil {
		if arrayResult, isArray := parentOutput.Result.([]interface{}); isArray {
			iterationCtx = &IterationContext{
				IsArray:     true,
				ArrayPath:   "/result",
				ArrayLength: len(arrayResult),
			}
			p.logger.Debug("Parent output is array",
				Field{Key: "node_id", Value: msg.Node.NodeID},
				Field{Key: "array_length", Value: len(arrayResult)})
		}
	}

	if err := storage.Set(msg.Node.NodeID, parentResult, iterationCtx); err != nil {
		return nil, fmt.Errorf("failed to store parent output: %w", err)
	}

	// Process each node sequentially
	results := make([]EmbeddedNodeResult, 0, len(nodes))

	for _, embNode := range nodes {
		result := p.processNode(ctx, embNode, storage)
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
	consumerGraph map[string][]string,
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

	// Consumer graph is REQUIRED for array coordination - no backward compatibility
	if len(consumerGraph) == 0 {
		return nil, fmt.Errorf("consumer graph is required for embedded node execution (ensure Zeus computed it)")
	}

	storage := NewSmartStorageWithConsumers(consumerGraph, p.logger)
	p.logger.Debug("Initialized SmartStorage with consumer graph",
		Field{Key: "source_count", Value: len(consumerGraph)})

	// Store parent output in storage with array detection
	parentResult := map[string]interface{}{
		"_meta":   parentOutput.Meta,
		"_events": parentOutput.Events,
		"_error":  parentOutput.Error,
		"result":  parentOutput.Result,
	}

	// Detect if parent output is an array result
	var iterationCtx *IterationContext
	if parentOutput.Result != nil {
		if arrayResult, isArray := parentOutput.Result.([]interface{}); isArray {
			iterationCtx = &IterationContext{
				IsArray:     true,
				ArrayPath:   "/result",
				ArrayLength: len(arrayResult),
			}
			p.logger.Debug("Parent output is array",
				Field{Key: "node_id", Value: msg.Node.NodeID},
				Field{Key: "array_length", Value: len(arrayResult)})
		}
	}

	if err := storage.Set(msg.Node.NodeID, parentResult, iterationCtx); err != nil {
		return nil, fmt.Errorf("failed to store parent output: %w", err)
	}

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
					result := p.processNode(ctx, embNode, storage)

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

					// Store error output in storage
					errorResult := map[string]interface{}{
						"_meta":   wrappedOutput.Meta,
						"_events": wrappedOutput.Events,
						"_error":  wrappedOutput.Error,
						"result":  wrappedOutput.Result,
					}
					storage.Set(embNode.NodeID, errorResult, nil)

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
					result := p.processNode(ctx, embNode, storage)

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

// checkCoordinatedIteration checks if this node needs coordinated array iteration
// Returns true if ANY field mapping has iterate:true and references an array source
// Also returns the maximum array length across all array sources for validation
func (p *Processor) checkCoordinatedIteration(embNode message.EmbeddedNode, storage *SmartStorage) (bool, int) {
	maxArrayLength := 0
	needsIteration := false

	for _, mapping := range embNode.FieldMappings {
		// Skip event triggers
		if mapping.IsEventTrigger {
			continue
		}

		// Check if this mapping has iterate flag
		if mapping.Iterate {
			// Check if the source is an array
			iterCtx, err := storage.GetIterationContext(mapping.SourceNodeID)
			if err == nil && iterCtx != nil && iterCtx.IsArray {
				needsIteration = true
				if iterCtx.ArrayLength > maxArrayLength {
					maxArrayLength = iterCtx.ArrayLength
				}
				p.logger.Debug("Found array source with iterate flag",
					Field{Key: "node_id", Value: embNode.NodeID},
					Field{Key: "source_node_id", Value: mapping.SourceNodeID},
					Field{Key: "array_length", Value: iterCtx.ArrayLength})
			}
		}
	}

	return needsIteration, maxArrayLength
}

// processWithCoordinatedIteration handles coordinated iteration across multiple array sources
// Each iteration index applies field mappings at that index, then executes the plugin
func (p *Processor) processWithCoordinatedIteration(
	ctx context.Context,
	embNode message.EmbeddedNode,
	arrayLength int,
	startTime time.Time,
	storage *SmartStorage,
) EmbeddedNodeResult {
	if arrayLength == 0 {
		// Empty array - return empty result
		execTime := time.Since(startTime).Milliseconds()
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
			Result: []interface{}{},
		}

		successResult := map[string]interface{}{
			"_meta":   wrappedOutput.Meta,
			"_events": wrappedOutput.Events,
			"result":  wrappedOutput.Result,
		}
		iterationCtx := &IterationContext{
			IsArray:     true,
			ArrayPath:   "/result",
			ArrayLength: 0,
		}
		storage.Set(embNode.NodeID, successResult, iterationCtx)

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

	// Process each array index
	results := make([]interface{}, arrayLength)

	for i := 0; i < arrayLength; i++ {
		// Apply field mappings at this specific iteration index
		mappedInput, err := p.fieldMapper.ApplyMappings(storage, embNode.FieldMappings, []byte("{}"), i)
		if err != nil {
			execTime := time.Since(startTime).Milliseconds()
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime,
				fmt.Errorf("field mapping failed at iteration %d: %w", i, err))

			errorResult := map[string]interface{}{
				"_meta":   wrappedOutput.Meta,
				"_events": wrappedOutput.Events,
				"_error":  wrappedOutput.Error,
				"result":  wrappedOutput.Result,
			}
			storage.Set(embNode.NodeID, errorResult, nil)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("field mapping failed at iteration %d: %v", i, err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		// Execute plugin with mapped input for this iteration
		nodeConfig := NodeConfig{
			NodeID:        embNode.NodeID,
			PluginType:    embNode.PluginType,
			Configuration: embNode.Configuration,
			Input:         mappedInput,
			Connection:    embNode.Connection,
			Schema:        embNode.Schema,
		}

		nodeOutput, err := p.registry.Execute(ctx, nodeConfig)
		if err != nil {
			execTime := time.Since(startTime).Milliseconds()
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime,
				fmt.Errorf("execution failed at iteration %d: %w", i, err))

			errorResult := map[string]interface{}{
				"_meta":   wrappedOutput.Meta,
				"_events": wrappedOutput.Events,
				"_error":  wrappedOutput.Error,
				"result":  wrappedOutput.Result,
			}
			storage.Set(embNode.NodeID, errorResult, nil)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("execution failed at iteration %d: %v", i, err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		// Parse output and extract result
		var output StandardOutput
		if err := json.Unmarshal(nodeOutput, &output); err != nil {
			execTime := time.Since(startTime).Milliseconds()
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime,
				fmt.Errorf("failed parsing output at iteration %d: %w", i, err))

			errorResult := map[string]interface{}{
				"_meta":   wrappedOutput.Meta,
				"_events": wrappedOutput.Events,
				"_error":  wrappedOutput.Error,
				"result":  wrappedOutput.Result,
			}
			storage.Set(embNode.NodeID, errorResult, nil)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("failed parsing output at iteration %d: %v", i, err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		results[i] = output.Result
	}

	// Mark all sources as consumed after successful coordinated iteration
	for _, mapping := range embNode.FieldMappings {
		if !mapping.IsEventTrigger {
			storage.MarkConsumed(mapping.SourceNodeID, embNode.NodeID)
		}
	}

	// Wrap results in StandardOutput with array
	execTime := time.Since(startTime).Milliseconds()
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
		Result: results,
	}

	// Store with iteration context
	successResult := map[string]interface{}{
		"_meta":   wrappedOutput.Meta,
		"_events": wrappedOutput.Events,
		"result":  wrappedOutput.Result,
	}
	iterationCtx := &IterationContext{
		IsArray:     true,
		ArrayPath:   "/result",
		ArrayLength: arrayLength,
	}
	storage.Set(embNode.NodeID, successResult, iterationCtx)

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

// processWithEventMaskedIteration handles iteration filtered by event mask (boolean array)
// Only processes items where the event mask is true, getting data from sources at those indices
func (p *Processor) processWithEventMaskedIteration(
	ctx context.Context,
	embNode message.EmbeddedNode,
	eventMask []bool,
	startTime time.Time,
	storage *SmartStorage,
) EmbeddedNodeResult {
	// Count how many items we'll process
	processCount := countTrueValues(eventMask)
	if processCount == 0 {
		// No items to process - return empty result
		execTime := time.Since(startTime).Milliseconds()
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
			Result: []interface{}{},
		}

		successResult := map[string]interface{}{
			"_meta":   wrappedOutput.Meta,
			"_events": wrappedOutput.Events,
			"result":  wrappedOutput.Result,
		}
		iterationCtx := &IterationContext{
			IsArray:     true,
			ArrayPath:   "/result",
			ArrayLength: 0,
		}
		storage.Set(embNode.NodeID, successResult, iterationCtx)

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

	// Collect indices where mask is true
	filteredIndices := make([]int, 0, processCount)
	for i, shouldProcess := range eventMask {
		if shouldProcess {
			filteredIndices = append(filteredIndices, i)
		}
	}

	// Process each filtered index
	results := make([]interface{}, len(filteredIndices))

	for resultIdx, sourceIdx := range filteredIndices {
		// Apply field mappings at this specific source index
		mappedInput, err := p.fieldMapper.ApplyMappings(storage, embNode.FieldMappings, []byte("{}"), sourceIdx)
		if err != nil {
			execTime := time.Since(startTime).Milliseconds()
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime,
				fmt.Errorf("field mapping failed at index %d (source idx %d): %w", resultIdx, sourceIdx, err))

			errorResult := map[string]interface{}{
				"_meta":   wrappedOutput.Meta,
				"_events": wrappedOutput.Events,
				"_error":  wrappedOutput.Error,
				"result":  wrappedOutput.Result,
			}
			storage.Set(embNode.NodeID, errorResult, nil)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("field mapping failed at index %d: %v", sourceIdx, err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		// Execute plugin with mapped input for this iteration
		nodeConfig := NodeConfig{
			NodeID:        embNode.NodeID,
			PluginType:    embNode.PluginType,
			Configuration: embNode.Configuration,
			Input:         mappedInput,
			Connection:    embNode.Connection,
			Schema:        embNode.Schema,
		}

		nodeOutput, err := p.registry.Execute(ctx, nodeConfig)
		if err != nil {
			execTime := time.Since(startTime).Milliseconds()
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime,
				fmt.Errorf("execution failed at index %d (source idx %d): %w", resultIdx, sourceIdx, err))

			errorResult := map[string]interface{}{
				"_meta":   wrappedOutput.Meta,
				"_events": wrappedOutput.Events,
				"_error":  wrappedOutput.Error,
				"result":  wrappedOutput.Result,
			}
			storage.Set(embNode.NodeID, errorResult, nil)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("execution failed at index %d: %v", sourceIdx, err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		// Parse output and extract result
		var output StandardOutput
		if err := json.Unmarshal(nodeOutput, &output); err != nil {
			execTime := time.Since(startTime).Milliseconds()
			wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime,
				fmt.Errorf("failed parsing output at index %d (source idx %d): %w", resultIdx, sourceIdx, err))

			errorResult := map[string]interface{}{
				"_meta":   wrappedOutput.Meta,
				"_events": wrappedOutput.Events,
				"_error":  wrappedOutput.Error,
				"result":  wrappedOutput.Result,
			}
			storage.Set(embNode.NodeID, errorResult, nil)

			return EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               wrappedOutput,
				Error:                fmt.Sprintf("failed parsing output at index %d: %v", sourceIdx, err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: execTime,
			}
		}

		results[resultIdx] = output.Result
	}

	// Mark all sources as consumed after successful event-masked iteration
	for _, mapping := range embNode.FieldMappings {
		if !mapping.IsEventTrigger {
			storage.MarkConsumed(mapping.SourceNodeID, embNode.NodeID)
		}
	}

	// Wrap results in StandardOutput with array
	execTime := time.Since(startTime).Milliseconds()
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
		Result: results,
	}

	// Store with iteration context
	successResult := map[string]interface{}{
		"_meta":   wrappedOutput.Meta,
		"_events": wrappedOutput.Events,
		"result":  wrappedOutput.Result,
	}
	iterationCtx := &IterationContext{
		IsArray:     true,
		ArrayPath:   "/result",
		ArrayLength: len(results),
	}
	storage.Set(embNode.NodeID, successResult, iterationCtx)

	p.logger.Debug("Event-masked iteration complete",
		Field{Key: "node_id", Value: embNode.NodeID},
		Field{Key: "total_items", Value: len(eventMask)},
		Field{Key: "processed_items", Value: len(results)})

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

// processWithAutoIteration handles auto-detected array iteration concurrently
// Processes each item through the plugin and aggregates results into single StandardOutput
func (p *Processor) processWithAutoIteration(
	ctx context.Context,
	embNode message.EmbeddedNode,
	arrayItems []interface{},
	startTime time.Time,
	storage *SmartStorage,
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

		// Store error in storage
		errorResult := map[string]interface{}{
			"_meta":   wrappedOutput.Meta,
			"_events": wrappedOutput.Events,
			"_error":  wrappedOutput.Error,
			"result":  wrappedOutput.Result,
		}
		storage.Set(embNode.NodeID, errorResult, nil)

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

	// Create iteration context for array output
	iterationContext := &IterationContext{
		IsArray:     true,
		ArrayPath:   "/result",
		ArrayLength: len(arrayItems),
	}

	// Store in storage with iteration context
	successResult := map[string]interface{}{
		"_meta":   wrappedOutput.Meta,
		"_events": wrappedOutput.Events,
		"result":  wrappedOutput.Result,
	}
	storage.Set(embNode.NodeID, successResult, iterationContext)

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
	storage *SmartStorage,
) EmbeddedNodeResult {
	startTime := time.Now()

	// Check event trigger conditions before executing
	shouldExecute, skipReason, eventMask := p.checkEventTriggers(embNode, storage)
	if !shouldExecute {
		// Event trigger conditions not met - skip this node
		wrappedOutput := WrapSkipped(embNode.NodeID, embNode.PluginType, skipReason)

		// Store skipped output in storage for downstream nodes to reference
		skippedResult := map[string]interface{}{
			"_meta":   wrappedOutput.Meta,
			"_events": wrappedOutput.Events,
			"result":  wrappedOutput.Result,
		}
		storage.Set(embNode.NodeID, skippedResult, nil)

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

	// ARRAY HANDLING: Four possible paths (checked in priority order):
	// 0. EVENT-MASKED ITERATION: Event trigger is a boolean array (filter items)
	//    → Process only items where event mask is true
	// 1. COORDINATED ITERATION: Field mappings have iterate:true from array sources
	//    → Process N times (once per array index), extracting item[i] from each source
	// 2. AUTO-ITERATION: Mapped input result is an array (detected via patterns)
	//    → Process N times (once per array item) through the plugin
	// 3. NORMAL: Single item processing (no arrays involved)

	// Path 0: Check for event-masked iteration (boolean array event)
	if len(eventMask) > 0 {
		p.logger.Debug("Using EVENT-MASKED ITERATION path",
			Field{Key: "node_id", Value: embNode.NodeID},
			Field{Key: "mask_length", Value: len(eventMask)},
			Field{Key: "filtered_count", Value: countTrueValues(eventMask)})
		return p.processWithEventMaskedIteration(ctx, embNode, eventMask, startTime, storage)
	}

	// Path 1: Check for coordinated iteration (takes precedence)
	needsCoordinatedIteration, maxArrayLength := p.checkCoordinatedIteration(embNode, storage)

	if needsCoordinatedIteration {
		p.logger.Debug("Using COORDINATED ITERATION path",
			Field{Key: "node_id", Value: embNode.NodeID},
			Field{Key: "array_length", Value: maxArrayLength})
		// Process this node for each array index with coordinated field mapping
		// This path handles MarkConsumed internally and returns immediately
		return p.processWithCoordinatedIteration(ctx, embNode, maxArrayLength, startTime, storage)
	}

	// Path 2 & 3: Apply field mappings (will determine if auto-iteration is needed)
	// Use iteration index -1 (no coordinated iteration)
	mappedInput, err := p.fieldMapper.ApplyMappings(storage, embNode.FieldMappings, []byte("{}"), -1)
	if err != nil {
		// Field mapping failed - wrap error output
		execTime := time.Since(startTime).Milliseconds()
		wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime, fmt.Errorf("field mapping failed: %w", err))

		// Store error in storage
		errorResult := map[string]interface{}{
			"_meta":   wrappedOutput.Meta,
			"_events": wrappedOutput.Events,
			"_error":  wrappedOutput.Error,
			"result":  wrappedOutput.Result,
		}
		storage.Set(embNode.NodeID, errorResult, nil)

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

	// Path 2: Check if mapped input is an array requiring auto-iteration
	isArray, arrayItems, err := p.detectAutoIteration(mappedInput)
	if err != nil {
		// Detection failed - wrap error output
		execTime := time.Since(startTime).Milliseconds()
		wrappedOutput := WrapError(embNode.NodeID, embNode.PluginType, execTime, fmt.Errorf("auto-iteration detection failed: %w", err))

		errorResult := map[string]interface{}{
			"_meta":   wrappedOutput.Meta,
			"_events": wrappedOutput.Events,
			"_error":  wrappedOutput.Error,
			"result":  wrappedOutput.Result,
		}
		storage.Set(embNode.NodeID, errorResult, nil)

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
		p.logger.Debug("Using AUTO-ITERATION path",
			Field{Key: "node_id", Value: embNode.NodeID},
			Field{Key: "array_items", Value: len(arrayItems)})
		// Route to auto-iteration handler (even for empty arrays)
		result := p.processWithAutoIteration(ctx, embNode, arrayItems, startTime, storage)

		// Mark sources consumed after successful auto-iteration
		if result.Status == "success" {
			for _, mapping := range embNode.FieldMappings {
				if !mapping.IsEventTrigger {
					storage.MarkConsumed(mapping.SourceNodeID, embNode.NodeID)
				}
			}
		}
		return result
	}

	// Path 3: Single-item processing (normal flow)
	p.logger.Debug("Using NORMAL processing path",
		Field{Key: "node_id", Value: embNode.NodeID})
	if embNode.PluginType == "plugin-js" {
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

	// Store in storage (even errors)
	finalResult := map[string]interface{}{
		"_meta":   wrappedOutput.Meta,
		"_events": wrappedOutput.Events,
		"result":  wrappedOutput.Result,
	}
	if wrappedOutput.Error != nil {
		finalResult["_error"] = wrappedOutput.Error
	}
	storage.Set(embNode.NodeID, finalResult, nil)

	// Mark sources consumed after successful storage
	if status == "success" {
		for _, mapping := range embNode.FieldMappings {
			if !mapping.IsEventTrigger {
				storage.MarkConsumed(mapping.SourceNodeID, embNode.NodeID)
			}
		}
	}

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
// Returns (shouldExecute bool, skipReason string, eventMask []bool)
// eventMask is non-nil when the event source is a boolean array (for filtered iteration)
func (p *Processor) checkEventTriggers(
	embNode message.EmbeddedNode,
	storage *SmartStorage,
) (bool, string, []bool) {
	// Find all event trigger field mappings
	eventTriggers := []message.FieldMapping{}
	for _, mapping := range embNode.FieldMappings {
		if mapping.IsEventTrigger {
			eventTriggers = append(eventTriggers, mapping)
		}
	}

	// If no event triggers, always execute
	if len(eventTriggers) == 0 {
		return true, "", nil
	}

	// Track event masks from array sources
	var eventMask []bool

	// Check each event trigger - ALL must be satisfied for node to execute
	for _, trigger := range eventTriggers {
		// Get source output from storage
		sourceOutput, err := storage.GetResultAsStandardOutput(trigger.SourceNodeID)
		if err != nil {
			// Source node hasn't executed yet or failed
			return false, fmt.Sprintf("event trigger source node '%s' not found", trigger.SourceNodeID), nil
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
				trigger.SourceEndpoint, trigger.SourceNodeID), nil
		}

		if sourceValue == nil {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' is null",
				trigger.SourceEndpoint, trigger.SourceNodeID), nil
		}

		// Check for boolean array event (from simple-condition)
		if boolArray, ok := sourceValue.([]interface{}); ok {
			// Convert to []bool for event mask
			mask := make([]bool, len(boolArray))
			hasAnyTrue := false
			for i, v := range boolArray {
				if b, isBool := v.(bool); isBool && b {
					mask[i] = true
					hasAnyTrue = true
				}
			}
			if !hasAnyTrue {
				return false, fmt.Sprintf("event not fired: array event '%s' from node '%s' has no true values",
					trigger.SourceEndpoint, trigger.SourceNodeID), nil
			}
			// Combine with existing event mask (AND logic for multiple triggers)
			if eventMask == nil {
				eventMask = mask
			} else {
				// AND the masks together
				for i := range eventMask {
					if i < len(mask) {
						eventMask[i] = eventMask[i] && mask[i]
					} else {
						eventMask[i] = false
					}
				}
			}
			p.logger.Debug("Event trigger is boolean array",
				Field{Key: "node_id", Value: embNode.NodeID},
				Field{Key: "source_node", Value: trigger.SourceNodeID},
				Field{Key: "endpoint", Value: trigger.SourceEndpoint},
				Field{Key: "array_length", Value: len(mask)},
				Field{Key: "true_count", Value: countTrueValues(mask)})
			continue
		}

		if boolVal, ok := sourceValue.(bool); ok && !boolVal {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' is false",
				trigger.SourceEndpoint, trigger.SourceNodeID), nil
		}

		if strVal, ok := sourceValue.(string); ok && strVal == "" {
			return false, fmt.Sprintf("event not fired: endpoint '%s' from node '%s' is empty",
				trigger.SourceEndpoint, trigger.SourceNodeID), nil
		}
	}

	// All event triggers satisfied
	return true, "", eventMask
}

// countTrueValues counts the number of true values in a boolean slice
func countTrueValues(mask []bool) int {
	count := 0
	for _, v := range mask {
		if v {
			count++
		}
	}
	return count
}
