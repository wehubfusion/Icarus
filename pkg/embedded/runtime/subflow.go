package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// SubflowProcessor processes embedded nodes for a single parent item.
// It supports mid-flow iteration by detecting mappings with iterate:true
// and running nodes per-item when necessary.
// Nodes are processed by depth level, with parallel execution within each depth.
// Mid-flow iteration uses concurrent processing when configured workers are available.
type SubflowProcessor struct {
	parentNodeId     string
	nodes            []EmbeddedNode
	nodeConfigs      []EmbeddedNodeConfig
	depthGroups      [][]int // indices grouped by depth
	arrayPath        string
	logger           Logger
	metrics          MetricsCollector
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
	WorkerPoolConfig WorkerPoolConfig // Config for mid-flow worker pool
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

			// Check context cancellation before starting
			select {
			case <-ctx.Done():
				result.err = ctx.Err()
				resultChan <- result
				return
			default:
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

			// Process node in a goroutine with timeout to prevent indefinite blocking
			processDone := make(chan struct{})
			var out ProcessOutput
			var dur int64
			go func() {
				defer close(processDone)
				startTime := time.Now()
				out = node.Process(procInput)
				dur = time.Since(startTime).Nanoseconds()
			}()

			// Wait for processing with context cancellation
			select {
			case <-ctx.Done():
				result.err = fmt.Errorf("context cancelled while processing node %s: %w", config.Label, ctx.Err())
				resultChan <- result
				return
			case <-processDone:
				// Processing completed
			}

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

	// Wait for all goroutines to complete, then close channel
	// The channel buffer size equals the number of goroutines, so all sends
	// will succeed before the channel is closed
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

// midFlowItemProcessor implements ItemProcessor for mid-flow iteration.
// It captures the context needed to process mid-flow items.
type midFlowItemProcessor struct {
	subflow      *SubflowProcessor
	startDepth   int
	preIterStore *NodeOutputStore
	iter         IterationState
}

// ProcessItem processes a single mid-flow item.
func (p *midFlowItemProcessor) ProcessItem(ctx context.Context, item BatchItem) BatchResult {
	return p.subflow.processOneMidFlowItem(ctx, item, p.startDepth, p.preIterStore, p.iter)
}

// newMidFlowItemProcessor creates an ItemProcessor for mid-flow iteration.
func (sp *SubflowProcessor) newMidFlowItemProcessor(
	startDepth int,
	preIterStore *NodeOutputStore,
	iter IterationState,
) ItemProcessor {
	return &midFlowItemProcessor{
		subflow:      sp,
		startDepth:   startDepth,
		preIterStore: preIterStore,
		iter:         iter,
	}
}

// processMidFlowConcurrently processes mid-flow iteration items concurrently.
// It uses WorkerPool for consistent concurrency handling.
func (sp *SubflowProcessor) processMidFlowConcurrently(
	ctx context.Context,
	startDepth int,
	preIterStore *NodeOutputStore,
	iter IterationState,
	res *BatchResult,
) error {
	totalItems := iter.TotalItems

	sp.logger.Debug("starting concurrent mid-flow processing",
		Field{Key: "total_items", Value: totalItems},
		Field{Key: "workers", Value: sp.workerPoolConfig.NumWorkers},
		Field{Key: "start_depth", Value: startDepth},
	)

	// Create ItemProcessor for mid-flow items
	processor := sp.newMidFlowItemProcessor(startDepth, preIterStore, iter)

	// Create worker pool
	pool := NewWorkerPool(sp.workerPoolConfig, processor, sp.logger)
	sp.logger.Debug("created worker pool for mid-flow", Field{Key: "num_workers", Value: sp.workerPoolConfig.NumWorkers})

	// Start workers
	pool.Start(ctx)
	sp.logger.Debug("started worker pool for mid-flow")

	// Convert iter.Items to BatchItems
	batchItems := make([]BatchItem, 0, totalItems)
	for itemIdx := 0; itemIdx < totalItems; itemIdx++ {
		currentItem := iter.Items[itemIdx]
		currentItemMap, ok := currentItem.(map[string]interface{})
		if !ok {
			currentItemMap = map[string]interface{}{"value": currentItem}
		}
		batchItems = append(batchItems, BatchItem{
			Index: itemIdx,
			Data:  currentItemMap,
		})
	}

	// Submit items and wait for completion in a goroutine
	submitDone := make(chan struct{})
	go func() {
		defer close(submitDone)
		sp.logger.Debug("submitting batch items to worker pool")
		pool.SubmitAll(ctx, batchItems)
		sp.logger.Debug("submitted all batch items, waiting for workers to complete")
		pool.Wait()
	}()

	// Collect results with context cancellation support
	collectedResults, collectedItems, collectErr := collectResultsWithContext(ctx, pool.Results(), totalItems)

	// Wait for submission to complete
	select {
	case <-submitDone:
	case <-ctx.Done():
		pool.Close()
		return ctx.Err()
	case <-time.After(10 * time.Second):
		// Submission taking too long, but continue with results we have
	}

	if collectErr != nil {
		return collectErr
	}

	// Merge results into res.Items and res.Output
	// collectedResults[i] contains BatchResult.Output (sharedMerge with index notation)
	// collectedItems[i] contains BatchResult.Items (where [0] is itemOutput)
	for i := 0; i < totalItems && i < len(collectedResults); i++ {
		// Merge item-specific output (Items[0] from BatchResult) into res.Items[i]
		if i < len(res.Items) && len(collectedItems) > i && len(collectedItems[i]) > 0 {
			// collectedItems[i] is []map[string]interface{} from BatchResult.Items
			// Merge each item in the slice (typically just [0]) into res.Items[i]
			for _, item := range collectedItems[i] {
				MergeMaps(res.Items[i], item)
			}
		}
		// Merge shared output (Output from BatchResult) into res.Output
		if collectedResults[i] != nil {
			// The Output contains sharedMerge with index notation - merge into res.Output
			MergeMaps(res.Output, collectedResults[i])
		}
	}

	return nil
}

// processOneMidFlowItem processes a single mid-flow item through remaining depth levels.
// Uses parallel processing within each depth level.
// Returns BatchResult where:
//   - Output contains sharedMerge (output with index notation to merge into shared)
//   - Items[0] contains itemOutput (flattened output for this specific item)
func (sp *SubflowProcessor) processOneMidFlowItem(
	ctx context.Context,
	item BatchItem,
	startDepth int,
	preIterStore *NodeOutputStore,
	iter IterationState,
) BatchResult {
	result := BatchResult{
		Index:  item.Index,
		Output: make(map[string]interface{}),                           // sharedMerge with index notation
		Items:  []map[string]interface{}{make(map[string]interface{})}, // itemOutput
	}

	// Create isolated store for this item
	itemStore := sp.createItemStore(preIterStore, item.Data, iter, item.Index)

	sp.logger.Debug("processing mid-flow item by depth",
		Field{Key: "item_index", Value: item.Index},
		Field{Key: "start_depth", Value: startDepth},
		Field{Key: "total_depths", Value: len(sp.depthGroups) - startDepth},
	)

	// Process remaining depth levels (from startDepth to end)
	for depth := startDepth; depth < len(sp.depthGroups); depth++ {
		select {
		case <-ctx.Done():
			result.Error = ctx.Err()
			return result
		default:
		}

		nodeIndices := sp.depthGroups[depth]
		if len(nodeIndices) == 0 {
			continue
		}

		// Process all nodes at this depth in parallel
		err := sp.processDepthLevelForItem(ctx, depth, nodeIndices, itemStore, &iter, item.Index, &result)
		if err != nil {
			result.Error = err
			return result
		}
	}

	return result
}

// processDepthLevelForItem processes all nodes at a depth level for a specific item.
// Outputs are merged into the result's Items[0] (itemOutput) and Output (sharedMerge with index notation).
func (sp *SubflowProcessor) processDepthLevelForItem(
	ctx context.Context,
	depth int,
	nodeIndices []int,
	itemStore *NodeOutputStore,
	iter *IterationState,
	itemIndex int,
	result *BatchResult,
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

			// Check context cancellation before starting
			select {
			case <-ctx.Done():
				nodeResult.err = ctx.Err()
				resultChan <- nodeResult
				return
			default:
			}

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

			// Process node in a goroutine with timeout to prevent indefinite blocking
			processDone := make(chan struct{})
			var out ProcessOutput
			var dur int64
			go func() {
				defer close(processDone)
				startTime := time.Now()
				out = node.Process(procInput)
				dur = time.Since(startTime).Nanoseconds()
			}()

			// Wait for processing with context cancellation
			select {
			case <-ctx.Done():
				nodeResult.err = fmt.Errorf("context cancelled while processing node %s at item %d: %w", config.Label, itemIndex, ctx.Err())
				resultChan <- nodeResult
				return
			case <-processDone:
				// Processing completed
			}

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

	// Wait for all goroutines to complete, then close channel
	// The channel buffer size equals the number of goroutines, so all sends
	// will succeed before the channel is closed
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
		MergeMaps(result.Output, flatWithIndex)

		// Flatten without index for this item's output
		flatItem := FlattenMap(nodeResult.output, config.NodeId, "")
		if len(result.Items) == 0 {
			result.Items = []map[string]interface{}{make(map[string]interface{})}
		}
		MergeMaps(result.Items[0], flatItem)
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
	result *BatchResult,
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
	MergeMaps(result.Output, flatWithIndex)

	// Flatten without index for this item's output
	flatItem := FlattenMap(out.Data, config.NodeId, "")
	if len(result.Items) == 0 {
		result.Items = []map[string]interface{}{make(map[string]interface{})}
	}
	MergeMaps(result.Items[0], flatItem)

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

		// Check for wrapped array with empty sourceEndpoint (pass-through case)
		if m.SourceEndpoint == "" {
			// Check if source node has isWrappedArray metadata
			if wrapped, ok := sourceOut["isWrappedArray"]; ok {
				if isWrapped, ok := wrapped.(bool); ok && isWrapped {
					// Auto-unwrap: get the items array
					if items, ok := sourceOut["items"]; ok {
						// Pass the raw array directly to destination endpoints
						for _, dest := range m.DestinationEndpoints {
							// Handle empty destination - means pass as "input" or root
							if dest == "" {
								// For empty destination, use "input" as the key
								input["input"] = items
							} else {
								SetNestedValue(input, dest, items)
							}
						}
						continue
					}
				}
			}
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
