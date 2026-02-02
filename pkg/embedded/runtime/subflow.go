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
	config           NestedIterationConfig // Nested iteration config
	priorUnitOutputs map[string]map[string]interface{}
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

	// Set default values for nested iteration config if not provided
	nestedIterCfg := config.Config
	if nestedIterCfg.MaxIterationDepth == 0 {
		nestedIterCfg.MaxIterationDepth = 10
	}
	if nestedIterCfg.MaxActiveCombinations == 0 {
		nestedIterCfg.MaxActiveCombinations = 100000
	}

	return &SubflowProcessor{
		parentNodeId:     config.ParentNodeId,
		nodes:            nodes,
		nodeConfigs:      sortedConfigs,
		depthGroups:      depthGroups,
		arrayPath:        config.ArrayPath,
		logger:           logger,
		metrics:          metrics,
		workerPoolConfig: workerPoolCfg,
		config:           nestedIterCfg,
		priorUnitOutputs: config.PriorUnitOutputs,
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

// hasDownstreamPluginErrorListener returns true if any embedded node has an event mapping
// from sourceNodeId's pluginError section (so error output should be stored and flow continued).
func (sp *SubflowProcessor) hasDownstreamPluginErrorListener(sourceNodeId string) bool {
	for _, cfg := range sp.nodeConfigs {
		for _, m := range cfg.GetEventMappings() {
			if m.SourceNodeId == sourceNodeId && m.SourceSectionId == SectionPluginError {
				return true
			}
		}
	}
	return false
}

// SubflowConfig holds configuration for creating a SubflowProcessor
type SubflowConfig struct {
	ParentNodeId     string
	NodeConfigs      []EmbeddedNodeConfig
	Factory          EmbeddedNodeFactory
	ArrayPath        string
	Logger           Logger
	Metrics          MetricsCollector
	WorkerPoolConfig WorkerPoolConfig      // Config for mid-flow worker pool
	Config           NestedIterationConfig // Nested iteration config
	// PriorUnitOutputs seeds the store with per-node outputs from prior units so embedded nodes
	// can reference those node IDs in field mappings (e.g. Unit A node IDs when Unit B runs).
	PriorUnitOutputs map[string]map[string]interface{}
}

// ProcessItem processes a single parent item through all embedded nodes.
// Uses DFS (depth-first search) for nested array iteration.
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

	// Initialize iteration stack (empty = root level)
	iterStack := NewIterationStack()

	// Initialize store with parent data
	store := NewNodeOutputStore()
	store.SetSingleOutput(sp.parentNodeId, item.Data)

	// Seed store with prior unit outputs so embedded nodes can reference prior unit node IDs
	if len(sp.priorUnitOutputs) > 0 {
		for nodeId, output := range sp.priorUnitOutputs {
			if output != nil {
				store.SetSingleOutput(nodeId, output)
			}
		}
	}

	// Flatten parent data into shared output
	var parentFlat map[string]interface{}
	if sp.arrayPath != "" {
		// Parent-level array iteration: include array path and index in keys
		// Creates keys like "nodeId-/data[0]/FieldName" for array items
		basePath := "/" + sp.arrayPath + fmt.Sprintf("[%d]", item.Index)
		parentFlat = FlattenMap(item.Data, sp.parentNodeId, basePath)
	} else {
		// Single parent object (no array iteration): no index or array path
		// Creates keys like "nodeId-/FieldName"
		parentFlat = FlattenMap(item.Data, sp.parentNodeId, "")
	}
	MergeMaps(res.Output, parentFlat)

	// Process all depth levels recursively with DFS
	err := sp.processDepthLevelsRecursive(ctx, 0, store, iterStack, &res)
	if err != nil {
		res.Error = err
		return res
	}

	// Remove root keys from output (they're only needed internally for "" → "" mappings)
	FilterRootKeys(res.Output)

	sp.metrics.RecordProcessed(time.Since(start).Nanoseconds())
	return res
}

// filterNodesByIterationDepth filters nodes to only process those whose mapping depth
// matches the current iteration stack depth. This ensures nodes only run at their
// appropriate nesting level.
func (sp *SubflowProcessor) filterNodesByIterationDepth(nodeIndices []int, currentStackDepth int) []int {
	var filtered []int

	for _, idx := range nodeIndices {
		config := sp.nodeConfigs[idx]

		// Get the expected iteration depth for this node by counting array markers in mappings
		expectedDepth := sp.getNodeIterationDepth(config)

		// Node should only process when iteration depth matches its expected depth
		// Depth 0 means no iteration (process at root level)
		if expectedDepth == currentStackDepth {
			filtered = append(filtered, idx)
		}
	}

	return filtered
}

// getNodeIterationDepth determines the iteration depth for a node based on its mappings
// by counting the number of array markers (//) in source endpoints
func (sp *SubflowProcessor) getNodeIterationDepth(config EmbeddedNodeConfig) int {
	maxDepth := 0

	// Check all field mappings for array notation
	for _, m := range config.GetFieldMappings() {
		// Count array markers (//) in the source endpoint
		depth := strings.Count(m.SourceEndpoint, "//")

		if depth > maxDepth {
			maxDepth = depth
		}
	}

	return maxDepth
}

// processDepthLevelsRecursive processes depth levels starting from startDepth using DFS.
func (sp *SubflowProcessor) processDepthLevelsRecursive(
	ctx context.Context,
	startDepth int,
	store *NodeOutputStore,
	iterStack *IterationStack,
	res *BatchResult,
) error {
	// DFS: Process each depth level
	for depth := startDepth; depth < len(sp.depthGroups); depth++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		nodeIndices := sp.depthGroups[depth]
		if len(nodeIndices) == 0 {
			continue
		}

		// Check if any node at this depth starts new iteration
		newIterCtx := sp.detectNewIteration(nodeIndices, store, iterStack, sp.config)

		if newIterCtx != nil {
			// DFS: Found new iteration - expand and recurse for each item
			sp.logger.Debug("detected new iteration, expanding DFS",
				Field{Key: "depth", Value: depth},
				Field{Key: "nesting_level", Value: iterStack.Depth()},
				Field{Key: "total_items", Value: newIterCtx.TotalItems},
				Field{Key: "source_node", Value: newIterCtx.SourceNodeId},
			)

			// Process the iteration, which will recursively handle nested levels
			err := sp.expandAndProcessIteration(ctx, depth, store, iterStack, newIterCtx, res)
			if err != nil {
				return err
			}

			// DON'T continue - fall through to process nodes at this depth too
			// This ensures nodes get processed even after iterations are expanded
		}

		// No iteration at this depth - process nodes normally
		var shared map[string]interface{}
		if iterStack.IsActive() {
			// Inside iteration - prepare indexed output
			shared = make(map[string]interface{})
		} else {
			// Root level - use res.Output directly
			shared = res.Output
		}

		// Convert IterationStack to old API for processDepthLevelParallel
		var iter *IterationState
		var itemIndex int = -1
		currentStackDepth := iterStack.Depth()
		if iterStack.IsActive() {
			current := iterStack.Current()
			iter = &IterationState{
				IsActive:          true,
				ArrayPath:         current.ArrayPath,
				SourceNodeId:      current.SourceNodeId,
				InitiatedByNodeId: current.InitiatedByNodeId,
				TotalItems:        current.TotalItems,
				Items:             current.Items,
			}
			itemIndex = current.CurrentIndex
		}

		// Filter nodes by iteration depth - only process nodes whose iteration depth matches current stack depth
		filteredNodeIndices := sp.filterNodesByIterationDepth(nodeIndices, currentStackDepth)

		if len(filteredNodeIndices) == 0 {
			continue // No nodes match this iteration depth
		}

		var pathPrefix string
		if iterStack.IsActive() {
			pathPrefix = iterStack.BuildIterationPathPrefix()
		}
		err := sp.processDepthLevelParallel(ctx, depth, filteredNodeIndices, store, shared, iter, itemIndex, pathPrefix)
		if err != nil {
			return err
		}

		// If we're in iteration context, merge indexed output to result
		if iterStack.IsActive() {
			indices := iterStack.GetCurrentIndices()
			sp.mergeIndexedOutputToResult(shared, indices, res)
		}
	}

	return nil
}

// detectNewIteration checks if any node at this depth starts a new iteration.
// Returns nil if no new iteration needed.
func (sp *SubflowProcessor) detectNewIteration(
	nodeIndices []int,
	store *NodeOutputStore,
	iterStack *IterationStack,
	config NestedIterationConfig,
) *NestedIterationContext {
	for _, idx := range nodeIndices {
		nodeConfig := sp.nodeConfigs[idx]

		mappings := nodeConfig.GetIterateMappings()
		if len(mappings) == 0 {
			continue
		}

		for _, m := range mappings {
			sp.logger.Debug("checking iterate mapping",
				Field{Key: "node", Value: nodeConfig.NodeId},
				Field{Key: "source_node", Value: m.SourceNodeId},
				Field{Key: "endpoint", Value: m.SourceEndpoint},
				Field{Key: "stack_depth", Value: iterStack.Depth()},
			)

			// Get source output from store (context-aware)
			sourceOut := store.GetOutputAtContext(m.SourceNodeId, iterStack)
			if sourceOut == nil {
				sp.logger.Debug("source output not found",
					Field{Key: "source_node", Value: m.SourceNodeId},
				)
				continue
			}

			// Parse nested array path
			segments := ParseNestedArrayPath(m.SourceEndpoint)
			sp.logger.Debug("parsed segments",
				Field{Key: "endpoint", Value: m.SourceEndpoint},
				Field{Key: "segment_count", Value: len(segments)},
			)

			// Navigate to the array, respecting current iteration context
			arr, arrayPath := sp.navigateToArrayWithContext(sourceOut, segments, iterStack)

			sp.logger.Debug("navigation result",
				Field{Key: "found_array", Value: arr != nil},
				Field{Key: "array_path", Value: arrayPath},
				Field{Key: "array_len", Value: len(arr)},
			)

			// NOW check if we're already iterating from this source and array path
			// This prevents creating duplicate iteration contexts when multiple field mappings
			// target the same array (e.g., /data//name and /data//age both iterate "data")
			if iterStack.IsIteratingFrom(m.SourceNodeId, arrayPath) {
				continue
			}

			if len(arr) > 0 {
				// Check depth limit
				if iterStack.Depth() >= config.MaxIterationDepth {
					sp.logger.Warn("max iteration depth exceeded",
						Field{Key: "current_depth", Value: iterStack.Depth()},
						Field{Key: "max_depth", Value: config.MaxIterationDepth},
						Field{Key: "source_node", Value: m.SourceNodeId},
					)
					continue
				}

				sp.logger.Debug("found new iteration opportunity",
					Field{Key: "source_node", Value: m.SourceNodeId},
					Field{Key: "array_path", Value: arrayPath},
					Field{Key: "items_count", Value: len(arr)},
					Field{Key: "current_nesting", Value: iterStack.Depth()},
				)

				return &NestedIterationContext{
					SourceNodeId:      m.SourceNodeId,
					InitiatedByNodeId: nodeConfig.NodeId,
					ArrayPath:         arrayPath,
					TotalItems:        len(arr),
					Items:             arr,
				}
			}
		}
	}

	return nil
}

// navigateToArrayWithContext navigates to an array respecting current iteration context
func (sp *SubflowProcessor) navigateToArrayWithContext(
	data map[string]interface{},
	segments []ArrayPathSegment,
	iterStack *IterationStack,
) ([]interface{}, string) {
	current := interface{}(data)

	// Track which segment we're at vs current iteration depth
	stackDepth := iterStack.Depth()

	sp.logger.Debug("navigating to array",
		Field{Key: "stack_depth", Value: stackDepth},
		Field{Key: "segment_count", Value: len(segments)},
	)

	for segIdx, seg := range segments {
		sp.logger.Debug("processing segment",
			Field{Key: "seg_idx", Value: segIdx},
			Field{Key: "seg_path", Value: seg.Path},
			Field{Key: "seg_is_array", Value: seg.IsArray},
		)
		// If we're already iterating at this depth level, skip the field navigation
		// because 'data' already represents the current item (e.g., data[0])
		if segIdx < stackDepth {
			iterCtx := iterStack.GetContextForDepth(segIdx)
			if iterCtx != nil && iterCtx.ArrayPath == seg.Path {
				// We're already inside this array level
				// 'current' already represents the current item
				// Just continue to next segment
				if seg.IsArray {
					continue
				}
			}
		}

		// Navigate to the field
		if m, ok := current.(map[string]interface{}); ok && seg.Path != "" {
			current = GetNestedValue(m, seg.Path)
		}

		if seg.IsArray {
			// Check if it's actually an array
			if arr, ok := current.([]interface{}); ok {
				// This is a NEW array to iterate (not already in the stack)
				return arr, seg.Path
			}
			// Not an array - continue
			continue
		}
	}

	return nil, ""
}

// expandAndProcessIteration expands an iteration and processes each item recursively
func (sp *SubflowProcessor) expandAndProcessIteration(
	ctx context.Context,
	currentDepth int,
	parentStore *NodeOutputStore,
	iterStack *IterationStack,
	newIterCtx *NestedIterationContext,
	res *BatchResult,
) error {
	// Check for combinatorial explosion
	estimatedCombinations := newIterCtx.TotalItems
	for i := 0; i < iterStack.Depth(); i++ {
		ctx := iterStack.GetContextForDepth(i)
		if ctx != nil {
			estimatedCombinations *= ctx.TotalItems
		}
	}

	if estimatedCombinations > sp.config.MaxActiveCombinations {
		return fmt.Errorf("estimated %d combinations exceeds limit %d",
			estimatedCombinations,
			sp.config.MaxActiveCombinations,
		)
	}

	// DFS: Push iteration context onto stack
	iterStack.Push(newIterCtx)
	defer iterStack.Pop() // DFS: Backtrack when done

	sp.logger.Debug("expanding iteration (DFS forward)",
		Field{Key: "depth", Value: currentDepth},
		Field{Key: "nesting_level", Value: iterStack.Depth()},
		Field{Key: "total_items", Value: newIterCtx.TotalItems},
	)

	// Initialize result structure for first-level iteration
	if iterStack.Depth() == 1 && len(res.Items) == 0 {
		res.Items = make([]map[string]interface{}, newIterCtx.TotalItems)
		for i := range res.Items {
			res.Items[i] = make(map[string]interface{})
		}
	}

	// Sequential DFS traversal of items
	return sp.processIterationSequential(ctx, currentDepth, parentStore, iterStack, newIterCtx, res)
}

// processIterationSequential processes iteration items sequentially (DFS in order)
func (sp *SubflowProcessor) processIterationSequential(
	ctx context.Context,
	currentDepth int,
	parentStore *NodeOutputStore,
	iterStack *IterationStack,
	iterCtx *NestedIterationContext,
	res *BatchResult,
) error {
	for i := 0; i < iterCtx.TotalItems; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// DFS: Update current index and item data
		iterCtx.CurrentIndex = i
		iterCtx.ItemData = sp.extractItemDataAsMap(iterCtx.Items[i])

		sp.logger.Debug("processing item (DFS)",
			Field{Key: "item_index", Value: i},
			Field{Key: "total_items", Value: iterCtx.TotalItems},
			Field{Key: "nesting_level", Value: iterStack.Depth()},
			Field{Key: "indices", Value: iterStack.GetCurrentIndices()},
		)

		// Create isolated store for this item
		itemStore := sp.createNestedItemStore(parentStore, iterStack)

		// DFS: Recursive call to process remaining depths
		// Start from currentDepth (not currentDepth+1) because:
		// - The IsIteratingFrom check prevents duplicate iteration contexts
		// - We need to process the nodes at this depth within the iteration context
		err := sp.processDepthLevelsRecursive(ctx, currentDepth, itemStore, iterStack, res)
		if err != nil {
			return err
		}
	}

	sp.logger.Debug("completed DFS iteration (backtrack)",
		Field{Key: "nesting_level", Value: iterStack.Depth()},
		Field{Key: "total_items", Value: iterCtx.TotalItems},
	)

	return nil
}

// extractItemDataAsMap converts an item to map[string]interface{}
func (sp *SubflowProcessor) extractItemDataAsMap(item interface{}) map[string]interface{} {
	if m, ok := item.(map[string]interface{}); ok {
		return m
	}
	// Wrap non-map items
	return map[string]interface{}{"value": item}
}

// createNestedItemStore creates an isolated store for an iteration item
func (sp *SubflowProcessor) createNestedItemStore(
	parentStore *NodeOutputStore,
	iterStack *IterationStack,
) *NodeOutputStore {
	itemStore := NewNodeOutputStore()

	// Copy all outputs from parent store
	for nodeId, output := range parentStore.GetAllSingleOutputs() {
		itemStore.SetSingleOutput(nodeId, output)
	}

	// Store current iteration items for all active contexts
	if iterStack != nil && iterStack.Depth() > 0 {
		// Get the deepest (most recent) context's item
		// This represents the "current view" of the data at this nesting level
		deepestCtx := iterStack.Current()
		if deepestCtx != nil && deepestCtx.CurrentIndex >= 0 && deepestCtx.CurrentIndex < len(deepestCtx.Items) {
			itemData := sp.extractItemDataAsMap(deepestCtx.Items[deepestCtx.CurrentIndex])

			// Store as the source node's output for this context
			// This makes the current item available for value extraction
			itemStore.SetSingleOutput(deepestCtx.SourceNodeId, itemData)
			itemStore.SetCurrentIterationItem(deepestCtx.SourceNodeId, itemData, deepestCtx.CurrentIndex)
		}

		// Also store all context items for potential use
		for i := 0; i < iterStack.Depth(); i++ {
			ctx := iterStack.GetContextForDepth(i)
			if ctx != nil && ctx.CurrentIndex >= 0 && ctx.CurrentIndex < len(ctx.Items) {
				itemData := sp.extractItemDataAsMap(ctx.Items[ctx.CurrentIndex])
				itemStore.SetCurrentIterationItem(ctx.SourceNodeId, itemData, ctx.CurrentIndex)
			}
		}
	}

	return itemStore
}

// depthNodeResult holds the result of processing a single node at a depth level
type depthNodeResult struct {
	nodeIndex int
	output    map[string]interface{}
	err       error
	skipped   bool
}

// processDepthLevelParallel processes all nodes at a depth level in parallel.
// For iteration context, pass iter and itemIndex; pathPrefix is the iteration path for flat keys (e.g. /data[0]/assignments[0]).
func (sp *SubflowProcessor) processDepthLevelParallel(
	ctx context.Context,
	depth int,
	nodeIndices []int,
	store *NodeOutputStore,
	shared map[string]interface{},
	iter *IterationState,
	itemIndex int,
	pathPrefix string,
) error {
	if len(nodeIndices) == 0 {
		return nil
	}

	// Single node - process directly without goroutine overhead
	if len(nodeIndices) == 1 {
		idx := nodeIndices[0]
		return sp.processSingleNodeAtDepth(ctx, idx, store, shared, iter, itemIndex, pathPrefix)
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
				if sp.hasDownstreamPluginErrorListener(config.NodeId) {
					result.output = map[string]interface{}{
						ErrorOutputKeyError:       true,
						ErrorOutputKeyDescription: out.Error.Error(),
					}
					result.err = nil
					result.skipped = false
					resultChan <- result
					return
				}
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

		// Flatten and merge to shared. When in iteration, pathPrefix gives path-style keys (e.g. /data[0]/assignments[0]/...).
		flat := FlattenMap(result.output, config.NodeId, pathPrefix)
		MergeMaps(shared, flat)
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
	pathPrefix string,
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
		if sp.hasDownstreamPluginErrorListener(config.NodeId) {
			errorOutput := map[string]interface{}{
				ErrorOutputKeyError:       true,
				ErrorOutputKeyDescription: out.Error.Error(),
			}
			store.SetSingleOutput(config.NodeId, errorOutput)
			flat := FlattenMap(errorOutput, config.NodeId, pathPrefix)
			MergeMaps(shared, flat)
			return nil
		}
		return fmt.Errorf("node %s failed: %w", config.Label, out.Error)
	}

	if out.Skipped {
		sp.metrics.RecordSkipped()
		return nil
	}

	sp.metrics.RecordProcessed(dur)

	// Store output for downstream nodes
	store.SetSingleOutput(config.NodeId, out.Data)

	// Flatten and merge to shared. When in iteration, pathPrefix gives path-style keys.
	flat := FlattenMap(out.Data, config.NodeId, pathPrefix)
	MergeMaps(shared, flat)

	return nil
}

// mergeIndexedOutputToResult merges indexed output into result structure.
// When in iteration, keys are built with path-style indices (e.g. nodeId-/data[0]/assignments[0]/.../chapter[0])
// so we only append the current level's index to the key (key already has path prefix from flatten).
func (sp *SubflowProcessor) mergeIndexedOutputToResult(
	indexed map[string]interface{},
	indices []int,
	res *BatchResult,
) {
	for key, val := range indexed {
		if len(indices) == 0 {
			res.Output[key] = val
		} else {
			// Path-style: key is already nodeId-/path[0]/path[1]/.../field; append current index only.
			indexedKey := key + fmt.Sprintf("[%d]", indices[len(indices)-1])
			res.Output[indexedKey] = val
		}
	}
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

	sp.logger.Debug("building item input",
		Field{Key: "node", Value: config.NodeId},
		Field{Key: "item_index", Value: itemIndex},
		Field{Key: "iter_source", Value: iter.SourceNodeId},
	)

	for _, m := range config.GetFieldMappings() {
		var val interface{}

		sp.logger.Debug("processing field mapping",
			Field{Key: "source_node", Value: m.SourceNodeId},
			Field{Key: "endpoint", Value: m.SourceEndpoint},
		)

		// Check if this is from the iteration source's current item
		if m.SourceNodeId == iter.SourceNodeId {
			// Get current item from store
			currentItem, idx := itemStore.GetCurrentIterationItem(m.SourceNodeId)
			if currentItem != nil && idx == itemIndex {
				// For nested paths, extract the field relevant to THIS iteration level
				if strings.Contains(m.SourceEndpoint, "//") {
					// Parse the full nested path to find which segment we're at
					segments := ParseNestedArrayPath(m.SourceEndpoint)

					// The last segment (non-array) is the field to extract from current item
					if len(segments) > 0 {
						lastSeg := segments[len(segments)-1]
						if !lastSeg.IsArray && lastSeg.Path != "" {
							// Extract this field from the current item
							val = GetNestedValue(currentItem, lastSeg.Path)
						} else {
							// No final field, return the whole item
							val = currentItem
						}
					}
				} else {
					// Simple path, no nesting
					_, fieldPath, _ := ExtractArrayPath(m.SourceEndpoint)
					if fieldPath != "" {
						val = GetNestedValue(currentItem, fieldPath)
					} else {
						val = currentItem
					}
				}
			}
		} else if sourceOut, ok := itemStore.GetOutput(m.SourceNodeId, -1); ok {
			// Source exists in itemStore
			if m.SourceSectionId == SectionDefault && IsErrorOnlyOutput(sourceOut) {
				continue
			}
			if strings.Contains(m.SourceEndpoint, "//") {
				// Parse the nested path and extract value considering current context
				val = sp.extractNestedValueFromSource(sourceOut, m.SourceEndpoint, itemStore, itemIndex)
			} else {
				// Simple path extraction
				val = sp.extractValue(sourceOut, m.SourceEndpoint)
			}
		}

		if val == nil {
			sp.logger.Debug("extracted value is nil",
				Field{Key: "source_node", Value: m.SourceNodeId},
				Field{Key: "endpoint", Value: m.SourceEndpoint},
			)
			continue
		}

		sp.logger.Debug("extracted value successfully",
			Field{Key: "source_node", Value: m.SourceNodeId},
			Field{Key: "endpoint", Value: m.SourceEndpoint},
			Field{Key: "value_type", Value: fmt.Sprintf("%T", val)},
		)

		for _, dest := range m.DestinationEndpoints {
			SetNestedValue(input, dest, val)
		}
	}

	return input
}

// extractNestedValueFromSource extracts value from source considering nested context
// This handles paths like /data//assignments//topics//name when we're in nested iteration
func (sp *SubflowProcessor) extractNestedValueFromSource(
	source map[string]interface{},
	endpoint string,
	_ *NodeOutputStore,
	_ int,
) interface{} {
	endpoint = strings.TrimPrefix(endpoint, "/")

	// Parse into segments: data, assignments/details/topics, name
	parts := strings.Split(endpoint, "//")
	if len(parts) == 1 {
		// No array notation, simple extraction
		return GetNestedValue(source, endpoint)
	}

	current := interface{}(source)

	// Navigate through each // segment
	for i, part := range parts {
		if part == "" {
			continue
		}

		// Navigate to this part
		if m, ok := current.(map[string]interface{}); ok {
			current = GetNestedValue(m, part)
			if current == nil {
				return nil
			}
		}

		// If this is not the last part, it should be an array
		if i < len(parts)-1 {
			if arr, ok := current.([]interface{}); ok {
				// Check if we have context for this array level
				// Look for current item in itemStore based on the path
				// For now, use the first item as a fallback
				if len(arr) > 0 {
					current = arr[0]
				} else {
					return nil
				}
			}
		}
	}

	return current
}

// shouldSkipNodeForItem checks event triggers for a specific item, and skips when
// all default-section sources have error-only output.
// Handles three cases for events:
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

	// Skip if this node only consumes default (success) section and all such sources have error-only output
	defaultMappings := config.GetDefaultSectionFieldMappings()
	if len(defaultMappings) == 0 || len(defaultMappings) != len(config.GetFieldMappings()) {
		return false
	}
	seen := make(map[string]bool)
	for _, m := range defaultMappings {
		if seen[m.SourceNodeId] {
			continue
		}
		seen[m.SourceNodeId] = true
		sourceOut, ok := itemStore.GetOutput(m.SourceNodeId, -1)
		if !ok {
			return false
		}
		if !IsErrorOnlyOutput(sourceOut) {
			return false
		}
	}
	return true
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
		if m.SourceSectionId == SectionDefault && IsErrorOnlyOutput(sourceOut) {
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

// shouldSkipNode checks if a node should be skipped due to event triggers
// or because all its default-section sources emitted error-only output.
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

	// Skip if this node only consumes default (success) section and all such sources have error-only output
	defaultMappings := config.GetDefaultSectionFieldMappings()
	if len(defaultMappings) == 0 || len(defaultMappings) != len(config.GetFieldMappings()) {
		return false
	}
	seen := make(map[string]bool)
	for _, m := range defaultMappings {
		if seen[m.SourceNodeId] {
			continue
		}
		seen[m.SourceNodeId] = true
		sourceOut, ok := store.GetOutput(m.SourceNodeId, -1)
		if !ok {
			return false
		}
		if !IsErrorOnlyOutput(sourceOut) {
			return false
		}
	}
	return true
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

// Ensure SubflowProcessor implements ItemProcessor
var _ ItemProcessor = (*SubflowProcessor)(nil)
