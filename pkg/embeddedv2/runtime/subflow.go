package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
)

// SubflowProcessor processes embedded nodes for a single parent item.
// It supports mid-flow iteration by detecting mappings with iterate:true
// and running nodes per-item when necessary.
// Mid-flow iteration uses concurrent processing when a limiter is provided.
type SubflowProcessor struct {
	parentNodeId     string
	nodes            []EmbeddedNode
	nodeConfigs      []EmbeddedNodeConfig
	arrayPath        string
	logger           Logger
	metrics          MetricsCollector
	limiter          *concurrency.Limiter
	workerPoolConfig WorkerPoolConfig
}

// NewSubflowProcessor creates a new subflow processor.
func NewSubflowProcessor(config SubflowConfig) (*SubflowProcessor, error) {
	if config.Factory == nil {
		return nil, fmt.Errorf("factory is required")
	}

	// Sort by execution order
	sortedConfigs := make([]EmbeddedNodeConfig, len(config.NodeConfigs))
	copy(sortedConfigs, config.NodeConfigs)
	sort.Slice(sortedConfigs, func(i, j int) bool {
		return sortedConfigs[i].ExecutionOrder < sortedConfigs[j].ExecutionOrder
	})

	// Create node instances
	nodes := make([]EmbeddedNode, 0, len(sortedConfigs))
	for _, nodeConfig := range sortedConfigs {
		n, err := config.Factory.Create(nodeConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create node %s: %w", nodeConfig.Label, err)
		}
		nodes = append(nodes, n)
	}

	logger := config.Logger
	if logger == nil {
		logger = &NoOpLogger{}
	}

	metrics := config.Metrics
	if metrics == nil {
		metrics = &NoOpMetricsCollector{}
	}

	workerPoolCfg := config.WorkerPoolConfig
	workerPoolCfg.Validate()

	return &SubflowProcessor{
		parentNodeId:     config.ParentNodeId,
		nodes:            nodes,
		nodeConfigs:      sortedConfigs,
		arrayPath:        config.ArrayPath,
		logger:           logger,
		metrics:          metrics,
		limiter:          config.Limiter,
		workerPoolConfig: workerPoolCfg,
	}, nil
}

// SubflowConfig holds configuration for creating a SubflowProcessor
type SubflowConfig struct {
	ParentNodeId     string
	NodeConfigs      []EmbeddedNodeConfig
	Factory          EmbeddedNodeFactory
	ArrayPath        string
	Logger           Logger
	Metrics          MetricsCollector
	Limiter          *concurrency.Limiter // Optional: enables concurrent mid-flow iteration
	WorkerPoolConfig WorkerPoolConfig     // Config for mid-flow worker pool
}

// ProcessItem processes a single parent item through all embedded nodes.
// It handles mid-flow iteration by:
// Phase 1: Process all nodes before iteration starts (pre-iteration)
// Phase 2: For each array item, process ALL remaining subflow nodes
func (sp *SubflowProcessor) ProcessItem(ctx context.Context, item BatchItem) BatchResult {
	start := time.Now()
	res := BatchResult{
		Index:  item.Index,
		Output: make(map[string]interface{}),
		Items:  []map[string]interface{}{},
	}

	select {
	case <-ctx.Done():
		res.Error = ctx.Err()
		return res
	default:
	}

	// Pre-iteration store holds outputs from nodes before iteration starts
	preIterStore := NewNodeOutputStore()
	preIterStore.SetSingleOutput(sp.parentNodeId, item.Data)

	// Flatten parent data into shared output
	if sp.arrayPath != "" {
		parentFlat := FlattenWithArrayPath(item.Data, sp.parentNodeId, sp.arrayPath)
		MergeMaps(res.Output, parentFlat)
	} else {
		parentFlat := FlattenMap(item.Data, sp.parentNodeId, "")
		MergeMaps(res.Output, parentFlat)
	}

	// Phase 1: Process nodes until we find one that starts iteration
	var iterStartIndex int = -1
	var iterState IterationState

	for i, node := range sp.nodes {
		select {
		case <-ctx.Done():
			res.Error = ctx.Err()
			return res
		default:
		}

		config := sp.nodeConfigs[i]

		// Should we skip due to event mapping?
		if sp.shouldSkipNode(config, preIterStore) {
			sp.logger.Debug("skipping node due to event trigger",
				Field{Key: "node_id", Value: config.NodeId},
				Field{Key: "node_label", Value: config.Label},
				Field{Key: "item_index", Value: item.Index},
			)
			continue
		}

		// Check if this node starts iteration
		needsIter, state := sp.analyzeNodeIteration(config, preIterStore, nil)
		if needsIter {
			iterStartIndex = i
			iterState = state
			sp.logger.Debug("iteration detected, starting per-item subflow processing",
				Field{Key: "node_id", Value: config.NodeId},
				Field{Key: "node_label", Value: config.Label},
				Field{Key: "total_items", Value: state.TotalItems},
				Field{Key: "start_index", Value: i},
			)
			break
		}

		// Normal single execution (pre-iteration)
		err := sp.processNodeSingle(ctx, node, config, preIterStore, res.Output)
		if err != nil {
			res.Error = err
			return res
		}
	}

	// Phase 2: If iteration found, process entire subflow for each item concurrently
	if iterStartIndex >= 0 {
		// Prepare res.Items with empty maps for each iteration
		res.Items = make([]map[string]interface{}, iterState.TotalItems)
		for i := range res.Items {
			res.Items[i] = make(map[string]interface{})
		}

		// Process items concurrently using worker pool pattern
		err := sp.processMidFlowConcurrently(ctx, iterStartIndex, preIterStore, iterState, &res)
		if err != nil {
			res.Error = err
			return res
		}
	}

	sp.metrics.RecordProcessed(time.Since(start).Nanoseconds())
	return res
}

// midFlowItemJob represents a job for processing one mid-flow item
type midFlowItemJob struct {
	itemIndex int
	itemData  map[string]interface{}
}

// midFlowItemResult represents the result of processing one mid-flow item
type midFlowItemResult struct {
	itemIndex   int
	itemOutput  map[string]interface{} // flattened output for this item
	sharedMerge map[string]interface{} // output to merge into shared (with index notation)
	err         error
}

// processMidFlowConcurrently processes mid-flow iteration items concurrently.
// It uses a worker pool pattern similar to parent-level iteration.
func (sp *SubflowProcessor) processMidFlowConcurrently(
	ctx context.Context,
	startIndex int,
	preIterStore *NodeOutputStore,
	iter IterationState,
	res *BatchResult,
) error {
	totalItems := iter.TotalItems

	// Determine number of workers
	numWorkers := sp.workerPoolConfig.NumWorkers
	if numWorkers <= 0 {
		numWorkers = 4 // Default for mid-flow
	}
	// Don't use more workers than items
	if numWorkers > totalItems {
		numWorkers = totalItems
	}

	sp.logger.Debug("starting concurrent mid-flow processing",
		Field{Key: "total_items", Value: totalItems},
		Field{Key: "workers", Value: numWorkers},
		Field{Key: "start_index", Value: startIndex},
	)

	// Create channels
	jobChan := make(chan midFlowItemJob, totalItems)
	resultChan := make(chan midFlowItemResult, totalItems)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go sp.midFlowWorker(ctx, &wg, startIndex, preIterStore, iter, jobChan, resultChan)
	}

	// Submit all jobs
	go func() {
		for itemIdx := 0; itemIdx < totalItems; itemIdx++ {
			currentItem := iter.Items[itemIdx]
			currentItemMap, ok := currentItem.(map[string]interface{})
			if !ok {
				currentItemMap = map[string]interface{}{"value": currentItem}
			}

			select {
			case <-ctx.Done():
				return
			case jobChan <- midFlowItemJob{itemIndex: itemIdx, itemData: currentItemMap}:
			}
		}
		close(jobChan)
	}()

	// Wait for workers in separate goroutine and close results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var firstError error
	received := 0
	for result := range resultChan {
		if result.err != nil && firstError == nil {
			firstError = result.err
		}
		if result.itemIndex >= 0 && result.itemIndex < totalItems {
			// Merge item output into res.Items[itemIndex]
			if result.itemOutput != nil {
				MergeMaps(res.Items[result.itemIndex], result.itemOutput)
			}
			// Merge shared output (with index notation) into res.Output
			if result.sharedMerge != nil {
				MergeMaps(res.Output, result.sharedMerge)
			}
		}
		received++
		if received >= totalItems {
			break
		}
	}

	return firstError
}

// midFlowWorker is a worker goroutine for processing mid-flow items.
func (sp *SubflowProcessor) midFlowWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	startIndex int,
	preIterStore *NodeOutputStore,
	iter IterationState,
	jobs <-chan midFlowItemJob,
	results chan<- midFlowItemResult,
) {
	defer wg.Done()

	for job := range jobs {
		select {
		case <-ctx.Done():
			results <- midFlowItemResult{
				itemIndex: job.itemIndex,
				err:       ctx.Err(),
			}
			return
		default:
		}

		// Acquire from limiter if available
		if sp.workerPoolConfig.UseLimiter && sp.limiter != nil {
			if err := sp.limiter.Acquire(ctx); err != nil {
				results <- midFlowItemResult{
					itemIndex: job.itemIndex,
					err:       err,
				}
				continue
			}
		}

		// Process this item
		result := sp.processOneMidFlowItem(ctx, startIndex, preIterStore, iter, job.itemIndex, job.itemData)

		// Release limiter
		if sp.workerPoolConfig.UseLimiter && sp.limiter != nil {
			sp.limiter.Release()
		}

		results <- result
	}
}

// processOneMidFlowItem processes a single mid-flow item through the subflow.
// Returns the item's output and any shared output to merge.
func (sp *SubflowProcessor) processOneMidFlowItem(
	ctx context.Context,
	startIndex int,
	preIterStore *NodeOutputStore,
	iter IterationState,
	itemIndex int,
	itemData map[string]interface{},
) midFlowItemResult {
	result := midFlowItemResult{
		itemIndex:   itemIndex,
		itemOutput:  make(map[string]interface{}),
		sharedMerge: make(map[string]interface{}),
	}

	// Create isolated store for this item
	itemStore := sp.createItemStore(preIterStore, itemData, iter, itemIndex)

	sp.logger.Debug("processing mid-flow item",
		Field{Key: "item_index", Value: itemIndex},
		Field{Key: "start_node_index", Value: startIndex},
		Field{Key: "total_nodes", Value: len(sp.nodes) - startIndex},
	)

	// Process all subflow nodes for this item
	for i := startIndex; i < len(sp.nodes); i++ {
		select {
		case <-ctx.Done():
			result.err = ctx.Err()
			return result
		default:
		}

		node := sp.nodes[i]
		config := sp.nodeConfigs[i]

		// Check event triggers for this item
		if sp.shouldSkipNodeForItem(config, itemStore, iter, itemIndex) {
			sp.logger.Debug("skipping node for item due to event trigger",
				Field{Key: "node_id", Value: config.NodeId},
				Field{Key: "item_index", Value: itemIndex},
			)
			continue
		}

		// Build input for this node
		input := sp.buildItemInput(config, itemStore, iter, itemIndex)

		procInput := ProcessInput{
			Ctx:           ctx,
			Data:          input,
			Config:        nil,
			RawConfig:     config.NodeConfig.Config,
			NodeId:        config.NodeId,
			PluginType:    config.PluginType,
			Label:         config.Label,
			ItemIndex:     itemIndex,
			TotalItems:    iter.TotalItems,
			IsIteration:   true,
			IterationPath: iter.ArrayPath,
		}

		start := time.Now()
		out := node.Process(procInput)
		dur := time.Since(start).Nanoseconds()

		if out.Error != nil {
			sp.metrics.RecordError()
			result.err = fmt.Errorf("node %s failed at item %d: %w", config.Label, itemIndex, out.Error)
			return result
		}

		if out.Skipped {
			sp.metrics.RecordSkipped()
			sp.logger.Debug("node skipped for item",
				Field{Key: "node_id", Value: config.NodeId},
				Field{Key: "item_index", Value: itemIndex},
				Field{Key: "reason", Value: out.SkipReason},
			)
			continue
		}

		sp.metrics.RecordProcessed(dur)

		// Store output in item store for downstream nodes in this item's subflow
		itemStore.SetSingleOutput(config.NodeId, out.Data)

		// Flatten with index notation for shared output
		flatWithIndex := FlattenMapWithIndex(out.Data, config.NodeId, "", itemIndex)
		MergeMaps(result.sharedMerge, flatWithIndex)

		// Flatten without index for this item's output
		flatItem := FlattenMap(out.Data, config.NodeId, "")
		MergeMaps(result.itemOutput, flatItem)
	}

	return result
}

// analyzeNodeIteration determines if a node should start iterating.
func (sp *SubflowProcessor) analyzeNodeIteration(
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	active *IterationState,
) (bool, IterationState) {
	mappings := config.GetIterateMappings()
	if len(mappings) == 0 {
		return false, IterationState{}
	}

	for _, m := range mappings {
		// if active and source matches, skip
		if active != nil && active.SourceNodeId == m.SourceNodeId {
			continue
		}

		// get source output (single)
		sourceOutput, ok := store.GetOutput(m.SourceNodeId, -1)
		if !ok {
			continue
		}

		arrayPath, _, hasArray := ExtractArrayPath(m.SourceEndpoint)
		if !hasArray {
			// check if any field in sourceOutput is an array
			for _, v := range sourceOutput {
				if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
					return true, IterationState{
						IsActive:          true,
						ArrayPath:         "",
						SourceNodeId:      m.SourceNodeId,
						InitiatedByNodeId: config.NodeId,
						TotalItems:        len(arr),
						Items:             arr,
					}
				}
			}
			continue
		}

		// get value at arrayPath
		val := GetNestedValue(sourceOutput, arrayPath)
		if arr, ok := val.([]interface{}); ok && len(arr) > 0 {
			return true, IterationState{
				IsActive:          true,
				ArrayPath:         arrayPath,
				SourceNodeId:      m.SourceNodeId,
				InitiatedByNodeId: config.NodeId,
				TotalItems:        len(arr),
				Items:             arr,
			}
		}
	}

	return false, IterationState{}
}

// createItemStore creates an isolated NodeOutputStore for processing one item.
// It inherits all pre-iteration outputs and stores the current iteration item.
func (sp *SubflowProcessor) createItemStore(
	preIterStore *NodeOutputStore,
	currentItem map[string]interface{},
	iter IterationState,
	itemIndex int,
) *NodeOutputStore {
	itemStore := NewNodeOutputStore()

	// Copy all pre-iteration single outputs
	for nodeId, output := range preIterStore.GetAllSingleOutputs() {
		itemStore.SetSingleOutput(nodeId, output)
	}

	// Store the current item as a special entry for the iteration source
	// This allows nodes to reference the current item via the source node ID
	itemStore.SetCurrentIterationItem(iter.SourceNodeId, currentItem, itemIndex)

	return itemStore
}

// buildItemInput builds input for a node processing a specific item.
// It handles four cases for each mapping:
// 1. Source is the iteration source -> extract from current item
// 2. Source was processed in this item's subflow -> get from itemStore
// 3. Source is pre-iteration with array notation (//) -> extract at index
// 4. Source is pre-iteration without array notation -> pass full value (shared)
func (sp *SubflowProcessor) buildItemInput(
	config EmbeddedNodeConfig,
	itemStore *NodeOutputStore,
	iter IterationState,
	itemIndex int,
) map[string]interface{} {
	input := make(map[string]interface{})

	for _, m := range config.GetFieldMappings() {
		var val interface{}

		// Check if this is from the iteration source's current item
		if m.SourceNodeId == iter.SourceNodeId {
			// Get current item from store
			currentItem, idx := itemStore.GetCurrentIterationItem(m.SourceNodeId)
			if currentItem != nil && idx == itemIndex {
				_, fieldPath, hasArray := ExtractArrayPath(m.SourceEndpoint)
				if hasArray && fieldPath != "" {
					val = GetNestedValue(currentItem, fieldPath)
				} else {
					val = currentItem
				}
			}
		} else if sourceOut, ok := itemStore.GetOutput(m.SourceNodeId, -1); ok {
			// Source exists in itemStore (either pre-iteration or processed in this subflow)
			// Use extractValueAtIndex to handle array notation vs direct path
			val = sp.extractValueAtIndex(sourceOut, m.SourceEndpoint, itemIndex)
		}

		if val == nil {
			continue
		}

		for _, dest := range m.DestinationEndpoints {
			SetNestedValue(input, dest, val)
		}
	}

	return input
}

// shouldSkipNodeForItem checks event triggers for a specific item.
// Handles three cases:
// 1. Event source is the iteration source -> check current item
// 2. Event source in itemStore -> check value
// 3. Event source not found -> skip
func (sp *SubflowProcessor) shouldSkipNodeForItem(
	config EmbeddedNodeConfig,
	itemStore *NodeOutputStore,
	iter IterationState,
	itemIndex int,
) bool {
	for _, m := range config.GetEventMappings() {
		var eventVal interface{}
		var found bool

		if m.SourceNodeId == iter.SourceNodeId {
			// Event from iteration source - check current item
			currentItem, idx := itemStore.GetCurrentIterationItem(m.SourceNodeId)
			if currentItem != nil && idx == itemIndex {
				eventField := strings.TrimPrefix(m.SourceEndpoint, "/")
				eventVal, found = currentItem[eventField]
			}
		} else if sourceOut, ok := itemStore.GetOutput(m.SourceNodeId, -1); ok {
			// Event from itemStore
			eventField := strings.TrimPrefix(m.SourceEndpoint, "/")
			eventVal, found = sourceOut[eventField]
		}

		if !found {
			return true
		}

		switch v := eventVal.(type) {
		case bool:
			if !v {
				return true
			}
		default:
			if v == nil {
				return true
			}
		}
	}

	return false
}

// processNodeSingle processes a node without iteration.
func (sp *SubflowProcessor) processNodeSingle(
	ctx context.Context,
	node EmbeddedNode,
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	shared map[string]interface{},
) error {
	// build input
	input := sp.buildSingleNodeInput(config, store)

	procInput := ProcessInput{
		Ctx:         ctx,
		Data:        input,
		Config:      nil,
		RawConfig:   config.NodeConfig.Config,
		NodeId:      config.NodeId,
		PluginType:  config.PluginType,
		Label:       config.Label,
		ItemIndex:   -1,
		IsIteration: false,
	}

	start := time.Now()
	out := node.Process(procInput)
	dur := time.Since(start).Nanoseconds()
	if out.Error != nil {
		sp.metrics.RecordError()
		return fmt.Errorf("node %s failed: %w", config.Label, out.Error)
	}

	if out.Skipped {
		sp.metrics.RecordSkipped()
		sp.logger.Debug("node skipped", Field{Key: "node_id", Value: config.NodeId}, Field{Key: "reason", Value: out.SkipReason})
		return nil
	}

	sp.metrics.RecordProcessed(dur)

	// store single output
	store.SetSingleOutput(config.NodeId, out.Data)

	// flatten and merge into shared
	flat := FlattenMap(out.Data, config.NodeId, "")
	MergeMaps(shared, flat)

	return nil
}

// buildSingleNodeInput builds input for a non-iterated node.
func (sp *SubflowProcessor) buildSingleNodeInput(
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
) map[string]interface{} {
	input := make(map[string]interface{})

	for _, m := range config.GetFieldMappings() {
		sourceOut, ok := store.GetOutput(m.SourceNodeId, -1)
		if !ok {
			continue
		}

		val := sp.extractValue(sourceOut, m.SourceEndpoint)
		if val == nil {
			continue
		}

		for _, dest := range m.DestinationEndpoints {
			SetNestedValue(input, dest, val)
		}
	}

	return input
}

// shouldSkipNode checks if a node should be skipped due to event triggers.
func (sp *SubflowProcessor) shouldSkipNode(
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
) bool {
	for _, m := range config.GetEventMappings() {
		sourceOut, ok := store.GetOutput(m.SourceNodeId, -1)
		if !ok {
			return true
		}

		eventField := strings.TrimPrefix(m.SourceEndpoint, "/")
		eventVal, exists := sourceOut[eventField]
		if !exists {
			return true
		}

		switch v := eventVal.(type) {
		case bool:
			if !v {
				return true
			}
		default:
			if v == nil {
				return true
			}
		}
	}

	return false
}

// extractValue extracts a value from a source output based on endpoint.
func (sp *SubflowProcessor) extractValue(source map[string]interface{}, endpoint string) interface{} {
	endpoint = strings.TrimPrefix(endpoint, "/")

	if idx := strings.Index(endpoint, "//"); idx >= 0 {
		fieldPath := endpoint[idx+2:]
		return GetNestedValue(source, fieldPath)
	}

	return GetNestedValue(source, endpoint)
}

// extractValueAtIndex extracts a value from a source at a specific array index.
// Used when inheriting iteration from a non-iterated source that contains array data.
// For endpoint "/data//Hire_Date" at index 2, this navigates to data[2].Hire_Date
func (sp *SubflowProcessor) extractValueAtIndex(source map[string]interface{}, endpoint string, index int) interface{} {
	endpoint = strings.TrimPrefix(endpoint, "/")

	// Check for array notation
	if idx := strings.Index(endpoint, "//"); idx >= 0 {
		arrayPath := endpoint[:idx]
		fieldPath := endpoint[idx+2:]

		// Get the array at arrayPath
		arrVal := GetNestedValue(source, arrayPath)
		if arr, ok := arrVal.([]interface{}); ok && index < len(arr) {
			// Get the item at index
			item := arr[index]
			if itemMap, ok := item.(map[string]interface{}); ok {
				// Extract field from item
				if fieldPath != "" {
					return GetNestedValue(itemMap, fieldPath)
				}
				return itemMap
			}
			// If item is not a map, return it directly
			return item
		}
		return nil
	}

	// No array notation - just extract the value directly
	return GetNestedValue(source, endpoint)
}

// Ensure SubflowProcessor implements ItemProcessor
var _ ItemProcessor = (*SubflowProcessor)(nil)
