package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SubflowProcessor processes embedded nodes for a single parent item.
// It supports mid-flow iteration by detecting mappings with iterate:true
// and running nodes per-item when necessary.
type SubflowProcessor struct {
	parentNodeId string
	nodes        []EmbeddedNode
	nodeConfigs  []EmbeddedNodeConfig
	arrayPath    string
	logger       Logger
	metrics      MetricsCollector
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

	return &SubflowProcessor{
		parentNodeId: config.ParentNodeId,
		nodes:        nodes,
		nodeConfigs:  sortedConfigs,
		arrayPath:    config.ArrayPath,
		logger:       logger,
		metrics:      metrics,
	}, nil
}

// SubflowConfig holds configuration for creating a SubflowProcessor
type SubflowConfig struct {
	ParentNodeId string
	NodeConfigs  []EmbeddedNodeConfig
	Factory      EmbeddedNodeFactory
	ArrayPath    string
	Logger       Logger
	Metrics      MetricsCollector
}

// ProcessItem processes a single parent item through all embedded nodes.
// It handles mid-flow iteration when nodes output arrays.
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

	store := NewNodeOutputStore()
	store.SetSingleOutput(sp.parentNodeId, item.Data)

	// Flatten parent data into shared output
	if sp.arrayPath != "" {
		parentFlat := FlattenWithArrayPath(item.Data, sp.parentNodeId, sp.arrayPath)
		MergeMaps(res.Output, parentFlat)
	} else {
		parentFlat := FlattenMap(item.Data, sp.parentNodeId, "")
		MergeMaps(res.Output, parentFlat)
	}

	// Track active iteration
	var activeIter *IterationState

	for i, node := range sp.nodes {
		select {
		case <-ctx.Done():
			res.Error = ctx.Err()
			return res
		default:
		}

		config := sp.nodeConfigs[i]

		// Should we skip due to event mapping?
		if sp.shouldSkipNode(config, store) {
			sp.logger.Debug("skipping node due to event trigger",
				Field{Key: "node_id", Value: config.NodeId},
				Field{Key: "node_label", Value: config.Label},
				Field{Key: "item_index", Value: item.Index},
			)
			continue
		}

		// Analyze if this node starts a new iteration
		needsIter, iterState := sp.analyzeNodeIteration(config, store, activeIter)
		if needsIter {
			// Run node for each item
			err := sp.processNodeWithIteration(ctx, node, config, store, iterState, &res)
			if err != nil {
				res.Error = err
				return res
			}
			// update active iteration
			activeIter = &iterState
			continue
		}

		// If inside an active iteration and this node should inherit it
		if activeIter != nil && sp.nodeInheritsIteration(config, activeIter, store) {
			err := sp.processNodeInActiveIteration(ctx, node, config, store, activeIter, &res)
			if err != nil {
				res.Error = err
				return res
			}
			continue
		}

		// Normal single execution
		err := sp.processNodeSingle(ctx, node, config, store, res.Output)
		if err != nil {
			res.Error = err
			return res
		}
	}

	sp.metrics.RecordProcessed(time.Since(start).Nanoseconds())
	return res
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
						IsActive:     true,
						ArrayPath:    "",
						SourceNodeId: m.SourceNodeId,
						TotalItems:   len(arr),
						Items:        arr,
					}
				}
			}
			continue
		}

		// get value at arrayPath
		val := GetNestedValue(sourceOutput, arrayPath)
		if arr, ok := val.([]interface{}); ok && len(arr) > 0 {
			return true, IterationState{
				IsActive:     true,
				ArrayPath:    arrayPath,
				SourceNodeId: m.SourceNodeId,
				TotalItems:   len(arr),
				Items:        arr,
			}
		}
	}

	return false, IterationState{}
}

// nodeInheritsIteration checks if a node should run within an existing iteration.
func (sp *SubflowProcessor) nodeInheritsIteration(
	config EmbeddedNodeConfig,
	active *IterationState,
	store *NodeOutputStore,
) bool {
	if active == nil || !active.IsActive {
		return false
	}

	for _, m := range config.GetFieldMappings() {
		if m.SourceNodeId == active.SourceNodeId {
			return true
		}
		if store.HasIteratedOutput(m.SourceNodeId) {
			return true
		}
	}

	return false
}

// processNodeWithIteration processes a node that starts a new iteration.
func (sp *SubflowProcessor) processNodeWithIteration(
	ctx context.Context,
	node EmbeddedNode,
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	iter IterationState,
	res *BatchResult,
) error {
	sp.logger.Debug("processing node with iteration",
		Field{Key: "node_id", Value: config.NodeId},
		Field{Key: "array_path", Value: iter.ArrayPath},
		Field{Key: "total_items", Value: iter.TotalItems},
	)

	outputs := make([]map[string]interface{}, iter.TotalItems)

	for i := 0; i < iter.TotalItems; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// current item
		item := iter.Items[i]
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			itemMap = map[string]interface{}{"value": item}
		}

		// build input for this iteration
		input := sp.buildIteratedNodeInput(config, store, itemMap, iter, i)

		// event triggers per iteration
		if sp.shouldSkipNodeForIteration(config, store, i) {
			outputs[i] = make(map[string]interface{})
			continue
		}

		procInput := ProcessInput{
			Ctx:           ctx,
			Data:          input,
			Config:        nil,
			RawConfig:     config.NodeConfig.Config,
			NodeId:        config.NodeId,
			PluginType:    config.PluginType,
			Label:         config.Label,
			ItemIndex:     i,
			TotalItems:    iter.TotalItems,
			IsIteration:   true,
			IterationPath: iter.ArrayPath,
		}

		start := time.Now()
		out := node.Process(procInput)
		dur := time.Since(start).Nanoseconds()
		if out.Error != nil {
			sp.metrics.RecordError()
			return fmt.Errorf("node %s failed at iteration %d: %w", config.Label, i, out.Error)
		}

		if out.Skipped {
			sp.metrics.RecordSkipped()
			outputs[i] = make(map[string]interface{})
			continue
		}

		sp.metrics.RecordProcessed(dur)
		outputs[i] = out.Data

		// flatten per item and merge to shared result with index notation
		flat := FlattenMapWithIndex(out.Data, config.NodeId, "", i)
		MergeMaps(res.Output, flat)
	}

	// store iterated outputs
	store.SetIteratedOutputs(config.NodeId, outputs)
	store.SetIterationInfo(config.NodeId, IterationState{
		IsActive:     true,
		ArrayPath:    iter.ArrayPath,
		SourceNodeId: iter.SourceNodeId,
		TotalItems:   iter.TotalItems,
		Items:        iter.Items,
	})

	// append outputs to BatchResult.Items (ensure length)
	if len(res.Items) < iter.TotalItems {
		for len(res.Items) < iter.TotalItems {
			res.Items = append(res.Items, make(map[string]interface{}))
		}
	}
	for i := 0; i < iter.TotalItems; i++ {
		if outputs[i] != nil {
			itemFlat := FlattenMap(outputs[i], config.NodeId, "")
			MergeMaps(res.Items[i], itemFlat)
		}
	}

	return nil
}

// processNodeInActiveIteration processes a node within an existing iteration.
func (sp *SubflowProcessor) processNodeInActiveIteration(
	ctx context.Context,
	node EmbeddedNode,
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	active *IterationState,
	res *BatchResult,
) error {
	sp.logger.Debug("processing node in active iteration",
		Field{Key: "node_id", Value: config.NodeId},
		Field{Key: "iterations", Value: active.TotalItems},
	)

	outputs := make([]map[string]interface{}, active.TotalItems)

	for i := 0; i < active.TotalItems; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		input := sp.buildInheritedIterationInput(config, store, i)

		if sp.shouldSkipNodeForIteration(config, store, i) {
			outputs[i] = make(map[string]interface{})
			continue
		}

		procInput := ProcessInput{
			Ctx:           ctx,
			Data:          input,
			Config:        nil,
			RawConfig:     config.NodeConfig.Config,
			NodeId:        config.NodeId,
			PluginType:    config.PluginType,
			Label:         config.Label,
			ItemIndex:     i,
			TotalItems:    active.TotalItems,
			IsIteration:   true,
			IterationPath: active.ArrayPath,
		}

		start := time.Now()
		out := node.Process(procInput)
		dur := time.Since(start).Nanoseconds()
		if out.Error != nil {
			sp.metrics.RecordError()
			return fmt.Errorf("node %s failed at iteration %d: %w", config.Label, i, out.Error)
		}

		if out.Skipped {
			sp.metrics.RecordSkipped()
			outputs[i] = make(map[string]interface{})
			continue
		}

		sp.metrics.RecordProcessed(dur)
		outputs[i] = out.Data

		flat := FlattenMapWithIndex(out.Data, config.NodeId, "", i)
		MergeMaps(res.Output, flat)
	}

	store.SetIteratedOutputs(config.NodeId, outputs)

	if len(res.Items) < active.TotalItems {
		for len(res.Items) < active.TotalItems {
			res.Items = append(res.Items, make(map[string]interface{}))
		}
	}

	for i := 0; i < active.TotalItems; i++ {
		if outputs[i] != nil {
			itemFlat := FlattenMap(outputs[i], config.NodeId, "")
			MergeMaps(res.Items[i], itemFlat)
		}
	}

	return nil
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

// buildIteratedNodeInput builds input for a node starting iteration.
func (sp *SubflowProcessor) buildIteratedNodeInput(
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	currentItem map[string]interface{},
	iter IterationState,
	index int,
) map[string]interface{} {
	input := make(map[string]interface{})

	for _, m := range config.GetFieldMappings() {
		var val interface{}

		if m.Iterate && m.SourceNodeId == iter.SourceNodeId {
			// use current item
			_, fieldPath, hasArray := ExtractArrayPath(m.SourceEndpoint)
			if hasArray && fieldPath != "" {
				val = GetNestedValue(currentItem, fieldPath)
			} else {
				val = currentItem
			}
		} else if store.HasIteratedOutput(m.SourceNodeId) {
			if outputs, ok := store.GetAllIteratedOutputs(m.SourceNodeId); ok && index < len(outputs) {
				val = sp.extractValue(outputs[index], m.SourceEndpoint)
			}
		} else {
			if sourceOut, ok := store.GetOutput(m.SourceNodeId, -1); ok {
				// For non-iterated sources with array notation, extract at index
				val = sp.extractValueAtIndex(sourceOut, m.SourceEndpoint, index)
			}
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

// buildInheritedIterationInput builds input for a node inheriting iteration.
func (sp *SubflowProcessor) buildInheritedIterationInput(
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	index int,
) map[string]interface{} {
	input := make(map[string]interface{})

	for _, m := range config.GetFieldMappings() {
		var val interface{}
		if store.HasIteratedOutput(m.SourceNodeId) {
			if outputs, ok := store.GetAllIteratedOutputs(m.SourceNodeId); ok && index < len(outputs) {
				val = sp.extractValue(outputs[index], m.SourceEndpoint)
			}
		} else {
			if sourceOut, ok := store.GetOutput(m.SourceNodeId, -1); ok {
				// Check if endpoint has array notation and source has array data
				// In this case, extract the i-th item from the array
				val = sp.extractValueAtIndex(sourceOut, m.SourceEndpoint, index)
			}
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

// shouldSkipNodeForIteration checks event triggers for a specific iteration index.
func (sp *SubflowProcessor) shouldSkipNodeForIteration(
	config EmbeddedNodeConfig,
	store *NodeOutputStore,
	index int,
) bool {
	for _, m := range config.GetEventMappings() {
		var sourceOut map[string]interface{}
		var ok bool

		if store.HasIteratedOutput(m.SourceNodeId) {
			outputs, exists := store.GetAllIteratedOutputs(m.SourceNodeId)
			if !exists || index >= len(outputs) {
				return true
			}
			sourceOut = outputs[index]
			ok = true
		} else {
			sourceOut, ok = store.GetOutput(m.SourceNodeId, -1)
		}

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
