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
// Nodes are processed by depth level, with parallel execution within each depth.
// Mid-flow iteration uses concurrent processing when a limiter is provided.
type SubflowProcessor struct {
	parentNodeId     string
	nodes            []EmbeddedNode
	nodeConfigs      []EmbeddedNodeConfig
	depthGroups      [][]int // indices grouped by depth
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

	// Sort by depth (primary), then execution order (secondary for determinism)
	sortedConfigs := make([]EmbeddedNodeConfig, len(config.NodeConfigs))
	copy(sortedConfigs, config.NodeConfigs)
	sort.Slice(sortedConfigs, func(i, j int) bool {
		if sortedConfigs[i].Depth != sortedConfigs[j].Depth {
			return sortedConfigs[i].Depth < sortedConfigs[j].Depth
		}
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

	// Group nodes by depth for parallel processing
	depthGroups := groupNodesByDepth(sortedConfigs)

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
		depthGroups:      depthGroups,
		arrayPath:        config.ArrayPath,
		logger:           logger,
		metrics:          metrics,
		limiter:          config.Limiter,
		workerPoolConfig: workerPoolCfg,
	}, nil
}

// groupNodesByDepth groups node indices by their depth level.
// Returns slice where index is depth level, value is slice of node indices at that depth.
func groupNodesByDepth(configs []EmbeddedNodeConfig) [][]int {
	if len(configs) == 0 {
		return nil
	}

	// Find max depth
	maxDepth := 0
	for _, c := range configs {
		if c.Depth > maxDepth {
			maxDepth = c.Depth
		}
	}

	// Group indices by depth
	groups := make([][]int, maxDepth+1)
	for i := range groups {
		groups[i] = []int{}
	}
	for i, c := range configs {
		groups[c.Depth] = append(groups[c.Depth], i)
	}

	return groups
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
// Nodes are processed by depth level with parallel execution within each depth.
// It handles mid-flow iteration by:
// Phase 1: Process depth levels until we find one that starts iteration (parallel within depth)
// Phase 2: For each array item, process ALL remaining depth levels (parallel within depth)
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

	// Phase 1: Process depth levels until we find one that starts iteration
	var iterStartDepth int = -1
	var iterState IterationState

	for depth, nodeIndices := range sp.depthGroups {
		select {
		case <-ctx.Done():
			res.Error = ctx.Err()
			return res
		default:
		}

		if len(nodeIndices) == 0 {
			continue
		}

		// Check if any node at this depth starts iteration
		for _, idx := range nodeIndices {
			config := sp.nodeConfigs[idx]
			needsIter, state := sp.analyzeNodeIteration(config, preIterStore, nil)
			if needsIter {
				iterStartDepth = depth
				iterState = state
				sp.logger.Debug("iteration detected at depth, starting per-item subflow processing",
					Field{Key: "node_id", Value: config.NodeId},
					Field{Key: "depth", Value: depth},
					Field{Key: "total_items", Value: state.TotalItems},
				)
				break
			}
		}

		if iterStartDepth >= 0 {
			break
		}

		// Process all nodes at this depth in parallel
		err := sp.processDepthLevelParallel(ctx, depth, nodeIndices, preIterStore, res.Output, nil, -1)
		if err != nil {
			res.Error = err
			return res
		}
	}

	// Phase 2: If iteration found, process entire subflow for each item concurrently
	if iterStartDepth >= 0 {
		// Prepare res.Items with empty maps for each iteration
		res.Items = make([]map[string]interface{}, iterState.TotalItems)
		for i := range res.Items {
			res.Items[i] = make(map[string]interface{})
		}

		// Process items concurrently using worker pool pattern
		err := sp.processMidFlowConcurrently(ctx, iterStartDepth, preIterStore, iterState, &res)
		if err != nil {
			res.Error = err
			return res
		}
	}

	sp.metrics.RecordProcessed(time.Since(start).Nanoseconds())
	return res
}

// depthNodeResult holds the result of processing a single node at a depth level
type depthNodeResult struct {
	nodeIndex int
	output    map[string]interface{}
	err       error
	skipped   bool
}

// processDepthLevelParallel processes all nodes at a depth level in parallel.
// For iteration context, pass iter and itemIndex; otherwise pass nil and -1.
func (sp *SubflowProcessor) processDepthLevelParallel(
	ctx context.Context,
	depth int,
	nodeIndices []int,
	store *NodeOutputStore,
	shared map[string]interface{},
	iter *IterationState,
	itemIndex int,
) error {
	if len(nodeIndices) == 0 {
		return nil
	}

	// Single node - process directly without goroutine overhead
	if len(nodeIndices) == 1 {
		idx := nodeIndices[0]
		return sp.processSingleNodeAtDepth(ctx, idx, store, shared, iter, itemIndex)
	}

	sp.logger.Debug("processing depth level in parallel",
		Field{Key: "depth", Value: depth},
		Field{Key: "node_count", Value: len(nodeIndices)},
		Field{Key: "item_index", Value: itemIndex},
	)

	// Multiple nodes - process in parallel
	var wg sync.WaitGroup
	resultChan := make(chan depthNodeResult, len(nodeIndices))

	for _, idx := range nodeIndices {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()

			result := depthNodeResult{nodeIndex: nodeIdx}

			// Acquire limiter if available
			if sp.workerPoolConfig.UseLimiter && sp.limiter != nil {
				if err := sp.limiter.Acquire(ctx); err != nil {
					result.err = err
					resultChan <- result
					return
				}
				defer sp.limiter.Release()
			}

			node := sp.nodes[nodeIdx]
			config := sp.nodeConfigs[nodeIdx]

			// Check if should skip
			var shouldSkip bool
			if iter != nil && itemIndex >= 0 {
				shouldSkip = sp.shouldSkipNodeForItem(config, store, *iter, itemIndex)
			} else {
				shouldSkip = sp.shouldSkipNode(config, store)
			}

			if shouldSkip {
				sp.logger.Debug("skipping node at depth",
					Field{Key: "node_id", Value: config.NodeId},
					Field{Key: "depth", Value: depth},
				)
				result.skipped = true
				resultChan <- result
				return
			}

			// Build input
			var input map[string]interface{}
			if iter != nil && itemIndex >= 0 {
				input = sp.buildItemInput(config, store, *iter, itemIndex)
			} else {
				input = sp.buildSingleNodeInput(config, store)
			}

			procInput := ProcessInput{
				Ctx:           ctx,
				Data:          input,
				Config:        nil,
				RawConfig:     config.NodeConfig.Config,
				NodeId:        config.NodeId,
				PluginType:    config.PluginType,
				Label:         config.Label,
				ItemIndex:     itemIndex,
				TotalItems:    0,
				IsIteration:   iter != nil,
				IterationPath: "",
			}
			if iter != nil {
				procInput.TotalItems = iter.TotalItems
				procInput.IterationPath = iter.ArrayPath
			}

			startTime := time.Now()
			out := node.Process(procInput)
			dur := time.Since(startTime).Nanoseconds()

			if out.Error != nil {
				sp.metrics.RecordError()
				result.err = fmt.Errorf("node %s failed: %w", config.Label, out.Error)
				resultChan <- result
				return
			}

			if out.Skipped {
				sp.metrics.RecordSkipped()
				result.skipped = true
				resultChan <- result
				return
			}

			sp.metrics.RecordProcessed(dur)
			result.output = out.Data
			resultChan <- result
		}(idx)
	}

	// Wait and close
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and update store/shared
	var firstErr error
	for result := range resultChan {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
			continue
		}
		if result.skipped || result.output == nil {
			continue
		}

		config := sp.nodeConfigs[result.nodeIndex]

		// Store output for downstream nodes
		store.SetSingleOutput(config.NodeId, result.output)

		// Flatten and merge to shared
		if iter != nil && itemIndex >= 0 {
			flat := FlattenMapWithIndex(result.output, config.NodeId, "", itemIndex)
			MergeMaps(shared, flat)
		} else {
			flat := FlattenMap(result.output, config.NodeId, "")
			MergeMaps(shared, flat)
		}
	}

	return firstErr
}

// processSingleNodeAtDepth processes a single node (optimization for depth with one node)
func (sp *SubflowProcessor) processSingleNodeAtDepth(
	ctx context.Context,
	nodeIdx int,
	store *NodeOutputStore,
	shared map[string]interface{},
	iter *IterationState,
	itemIndex int,
) error {
	node := sp.nodes[nodeIdx]
	config := sp.nodeConfigs[nodeIdx]

	// Check if should skip
	var shouldSkip bool
	if iter != nil && itemIndex >= 0 {
		shouldSkip = sp.shouldSkipNodeForItem(config, store, *iter, itemIndex)
	} else {
		shouldSkip = sp.shouldSkipNode(config, store)
	}

	if shouldSkip {
		sp.logger.Debug("skipping single node at depth",
			Field{Key: "node_id", Value: config.NodeId},
		)
		return nil
	}

	// Build input
	var input map[string]interface{}
	if iter != nil && itemIndex >= 0 {
		input = sp.buildItemInput(config, store, *iter, itemIndex)
	} else {
		input = sp.buildSingleNodeInput(config, store)
	}

	procInput := ProcessInput{
		Ctx:           ctx,
		Data:          input,
		Config:        nil,
		RawConfig:     config.NodeConfig.Config,
		NodeId:        config.NodeId,
		PluginType:    config.PluginType,
		Label:         config.Label,
		ItemIndex:     itemIndex,
		TotalItems:    0,
		IsIteration:   iter != nil,
		IterationPath: "",
	}
	if iter != nil {
		procInput.TotalItems = iter.TotalItems
		procInput.IterationPath = iter.ArrayPath
	}

	startTime := time.Now()
	out := node.Process(procInput)
	dur := time.Since(startTime).Nanoseconds()

	if out.Error != nil {
		sp.metrics.RecordError()
		return fmt.Errorf("node %s failed: %w", config.Label, out.Error)
	}

	if out.Skipped {
		sp.metrics.RecordSkipped()
		return nil
	}

	sp.metrics.RecordProcessed(dur)

	// Store output for downstream nodes
	store.SetSingleOutput(config.NodeId, out.Data)

	// Flatten and merge to shared
	if iter != nil && itemIndex >= 0 {
		flat := FlattenMapWithIndex(out.Data, config.NodeId, "", itemIndex)
		MergeMaps(shared, flat)
	} else {
		flat := FlattenMap(out.Data, config.NodeId, "")
		MergeMaps(shared, flat)
	}

	return nil
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
	startDepth int,
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
		Field{Key: "start_depth", Value: startDepth},
	)

	// Create channels
	jobChan := make(chan midFlowItemJob, totalItems)
	resultChan := make(chan midFlowItemResult, totalItems)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go sp.midFlowWorker(ctx, &wg, startDepth, preIterStore, iter, jobChan, resultChan)
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
	startDepth int,
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

		// Acquire from limiter if available (for overall item processing)
		if sp.workerPoolConfig.UseLimiter && sp.limiter != nil {
			if err := sp.limiter.Acquire(ctx); err != nil {
				results <- midFlowItemResult{
					itemIndex: job.itemIndex,
					err:       err,
				}
				continue
			}
		}

		// Process this item through depth levels
		result := sp.processOneMidFlowItem(ctx, startDepth, preIterStore, iter, job.itemIndex, job.itemData)

		// Release limiter
		if sp.workerPoolConfig.UseLimiter && sp.limiter != nil {
			sp.limiter.Release()
		}

		results <- result
	}
}

// processOneMidFlowItem processes a single mid-flow item through remaining depth levels.
// Uses parallel processing within each depth level.
func (sp *SubflowProcessor) processOneMidFlowItem(
	ctx context.Context,
	startDepth int,
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

	sp.logger.Debug("processing mid-flow item by depth",
		Field{Key: "item_index", Value: itemIndex},
		Field{Key: "start_depth", Value: startDepth},
		Field{Key: "total_depths", Value: len(sp.depthGroups) - startDepth},
	)

	// Process remaining depth levels (from startDepth to end)
	for depth := startDepth; depth < len(sp.depthGroups); depth++ {
		select {
		case <-ctx.Done():
			result.err = ctx.Err()
			return result
		default:
		}

		nodeIndices := sp.depthGroups[depth]
		if len(nodeIndices) == 0 {
			continue
		}

		// Process all nodes at this depth in parallel
		// Note: We don't use the limiter here since we already acquired it for this item
		err := sp.processDepthLevelForItem(ctx, depth, nodeIndices, itemStore, &iter, itemIndex, &result)
		if err != nil {
			result.err = err
			return result
		}
	}

	return result
}

// processDepthLevelForItem processes all nodes at a depth level for a specific item.
// Outputs are merged into the result's itemOutput and sharedMerge.
func (sp *SubflowProcessor) processDepthLevelForItem(
	ctx context.Context,
	depth int,
	nodeIndices []int,
	itemStore *NodeOutputStore,
	iter *IterationState,
	itemIndex int,
	result *midFlowItemResult,
) error {
	if len(nodeIndices) == 0 {
		return nil
	}

	// Single node - process directly
	if len(nodeIndices) == 1 {
		idx := nodeIndices[0]
		return sp.processSingleNodeForItem(ctx, idx, itemStore, iter, itemIndex, result)
	}

	sp.logger.Debug("processing depth level in parallel for item",
		Field{Key: "depth", Value: depth},
		Field{Key: "node_count", Value: len(nodeIndices)},
		Field{Key: "item_index", Value: itemIndex},
	)

	// Multiple nodes - process in parallel
	var wg sync.WaitGroup
	resultChan := make(chan depthNodeResult, len(nodeIndices))

	for _, idx := range nodeIndices {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()

			nodeResult := depthNodeResult{nodeIndex: nodeIdx}

			node := sp.nodes[nodeIdx]
			config := sp.nodeConfigs[nodeIdx]

			// Check if should skip
			if sp.shouldSkipNodeForItem(config, itemStore, *iter, itemIndex) {
				sp.logger.Debug("skipping node at depth for item",
					Field{Key: "node_id", Value: config.NodeId},
					Field{Key: "depth", Value: depth},
					Field{Key: "item_index", Value: itemIndex},
				)
				nodeResult.skipped = true
				resultChan <- nodeResult
				return
			}

			// Build input
			input := sp.buildItemInput(config, itemStore, *iter, itemIndex)

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

			startTime := time.Now()
			out := node.Process(procInput)
			dur := time.Since(startTime).Nanoseconds()

			if out.Error != nil {
				sp.metrics.RecordError()
				nodeResult.err = fmt.Errorf("node %s failed at item %d: %w", config.Label, itemIndex, out.Error)
				resultChan <- nodeResult
				return
			}

			if out.Skipped {
				sp.metrics.RecordSkipped()
				nodeResult.skipped = true
				resultChan <- nodeResult
				return
			}

			sp.metrics.RecordProcessed(dur)
			nodeResult.output = out.Data
			resultChan <- nodeResult
		}(idx)
	}

	// Wait and close
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var firstErr error
	for nodeResult := range resultChan {
		if nodeResult.err != nil && firstErr == nil {
			firstErr = nodeResult.err
			continue
		}
		if nodeResult.skipped || nodeResult.output == nil {
			continue
		}

		config := sp.nodeConfigs[nodeResult.nodeIndex]

		// Store output for downstream nodes
		itemStore.SetSingleOutput(config.NodeId, nodeResult.output)

		// Flatten with index notation for shared output
		flatWithIndex := FlattenMapWithIndex(nodeResult.output, config.NodeId, "", itemIndex)
		MergeMaps(result.sharedMerge, flatWithIndex)

		// Flatten without index for this item's output
		flatItem := FlattenMap(nodeResult.output, config.NodeId, "")
		MergeMaps(result.itemOutput, flatItem)
	}

	return firstErr
}

// processSingleNodeForItem processes a single node for a specific item (optimization)
func (sp *SubflowProcessor) processSingleNodeForItem(
	ctx context.Context,
	nodeIdx int,
	itemStore *NodeOutputStore,
	iter *IterationState,
	itemIndex int,
	result *midFlowItemResult,
) error {
	node := sp.nodes[nodeIdx]
	config := sp.nodeConfigs[nodeIdx]

	// Check if should skip
	if sp.shouldSkipNodeForItem(config, itemStore, *iter, itemIndex) {
		sp.logger.Debug("skipping single node for item",
			Field{Key: "node_id", Value: config.NodeId},
			Field{Key: "item_index", Value: itemIndex},
		)
		return nil
	}

	// Build input
	input := sp.buildItemInput(config, itemStore, *iter, itemIndex)

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

	startTime := time.Now()
	out := node.Process(procInput)
	dur := time.Since(startTime).Nanoseconds()

	if out.Error != nil {
		sp.metrics.RecordError()
		return fmt.Errorf("node %s failed at item %d: %w", config.Label, itemIndex, out.Error)
	}

	if out.Skipped {
		sp.metrics.RecordSkipped()
		return nil
	}

	sp.metrics.RecordProcessed(dur)

	// Store output for downstream nodes
	itemStore.SetSingleOutput(config.NodeId, out.Data)

	// Flatten with index notation for shared output
	flatWithIndex := FlattenMapWithIndex(out.Data, config.NodeId, "", itemIndex)
	MergeMaps(result.sharedMerge, flatWithIndex)

	// Flatten without index for this item's output
	flatItem := FlattenMap(out.Data, config.NodeId, "")
	MergeMaps(result.itemOutput, flatItem)

	return nil
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
