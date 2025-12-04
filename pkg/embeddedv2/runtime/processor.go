package runtime

import (
	"context"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/concurrency"
)

// EmbeddedProcessor handles processing of embedded nodes with concurrency support.
// It integrates with the concurrency package's Limiter for rate limiting.
type EmbeddedProcessor struct {
	factory EmbeddedNodeFactory
	config  ProcessorConfig
	limiter *concurrency.Limiter
	logger  Logger
}

// NewEmbeddedProcessor creates a new embedded processor with full configuration.
func NewEmbeddedProcessor(
	factory EmbeddedNodeFactory,
	config ProcessorConfig,
	limiter *concurrency.Limiter,
) *EmbeddedProcessor {
	config.Validate()

	logger := config.Logger
	if logger == nil {
		logger = &NoOpLogger{}
	}

	return &EmbeddedProcessor{
		factory: factory,
		config:  config,
		limiter: limiter,
		logger:  logger,
	}
}

// NewEmbeddedProcessorWithDefaults creates a processor with default configuration.
func NewEmbeddedProcessorWithDefaults(factory EmbeddedNodeFactory) *EmbeddedProcessor {
	return NewEmbeddedProcessor(factory, DefaultProcessorConfig(), nil)
}

// NewEmbeddedProcessorWithLimiter creates a processor with a limiter.
func NewEmbeddedProcessorWithLimiter(
	factory EmbeddedNodeFactory,
	limiter *concurrency.Limiter,
) *EmbeddedProcessor {
	return NewEmbeddedProcessor(factory, DefaultProcessorConfig(), limiter)
}

// ProcessEmbeddedNodes processes all embedded nodes for a unit.
// It handles both single object and array iteration cases.
func (p *EmbeddedProcessor) ProcessEmbeddedNodes(
	ctx context.Context,
	parentOutput map[string]interface{},
	unit ExecutionUnit,
) (*StandardUnitOutput, error) {
	p.logger.Debug("processing embedded nodes",
		Field{Key: "unit_id", Value: unit.NodeId},
		Field{Key: "unit_label", Value: unit.Label},
		Field{Key: "embedded_count", Value: len(unit.EmbeddedNodes)},
	)

	// If no embedded nodes, just flatten parent output
	if len(unit.EmbeddedNodes) == 0 {
		return p.flattenParentOnly(parentOutput, unit.NodeId)
	}

	// Analyze iteration context from field mappings
	iterCtx := p.analyzeIterationContext(unit)

	if iterCtx.IsArrayIteration {
		p.logger.Debug("processing with array iteration",
			Field{Key: "array_path", Value: iterCtx.ArrayPath},
		)
		return p.processWithConcurrency(ctx, parentOutput, unit, iterCtx)
	}

	p.logger.Debug("processing single object")
	return p.processSingleObject(ctx, parentOutput, unit)
}

// analyzeIterationContext determines if array iteration is needed.
func (p *EmbeddedProcessor) analyzeIterationContext(unit ExecutionUnit) IterationContext {
	ctx := IterationContext{}

	for _, node := range unit.EmbeddedNodes {
		for _, mapping := range node.FieldMappings {
			// Look for patterns like "/data//field" with iterate=true from parent
			if mapping.Iterate && mapping.SourceNodeId == unit.NodeId {
				endpoint := mapping.SourceEndpoint
				if idx := strings.Index(endpoint, "//"); idx > 0 {
					arrayPath := strings.TrimPrefix(endpoint[:idx], "/")
					if arrayPath != "" {
						ctx.IsArrayIteration = true
						ctx.ArrayPath = arrayPath
						return ctx
					}
				}
			}
		}
	}

	return ctx
}

// processSingleObject handles non-array parent output.
func (p *EmbeddedProcessor) processSingleObject(
	ctx context.Context,
	parentOutput map[string]interface{},
	unit ExecutionUnit,
) (*StandardUnitOutput, error) {
	// Create subflow processor
	subflowCfg := SubflowConfig{
		ParentNodeId: unit.NodeId,
		NodeConfigs:  unit.EmbeddedNodes,
		Factory:      p.factory,
		ArrayPath:    "",
		Logger:       p.logger,
	}
	subflow, err := NewSubflowProcessor(subflowCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create subflow processor: %w", err)
	}

	// Process as single item
	result := subflow.ProcessItem(ctx, BatchItem{
		Index: 0,
		Data:  parentOutput,
	})

	if result.Error != nil {
		return nil, result.Error
	}

	return &StandardUnitOutput{
		Data:  result.Output,
		Items: result.Items,
	}, nil
}

// processWithConcurrency handles array iteration with worker pool.
func (p *EmbeddedProcessor) processWithConcurrency(
	ctx context.Context,
	parentOutput map[string]interface{},
	unit ExecutionUnit,
	iterCtx IterationContext,
) (*StandardUnitOutput, error) {
	// Extract array from parent output
	arrayData, ok := parentOutput[iterCtx.ArrayPath]
	if !ok {
		return nil, fmt.Errorf("%w: path '%s' not found in parent output", ErrArrayNotFound, iterCtx.ArrayPath)
	}

	rawItems, ok := arrayData.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: path '%s' is not an array", ErrNotAnArray, iterCtx.ArrayPath)
	}

	// Convert to map items
	items := make([]map[string]interface{}, 0, len(rawItems))
	for _, item := range rawItems {
		if m, ok := item.(map[string]interface{}); ok {
			items = append(items, m)
		}
	}

	if len(items) == 0 {
		p.logger.Debug("empty array, returning empty result")
		return &StandardUnitOutput{
			Data:  map[string]interface{}{},
			Items: []map[string]interface{}{},
		}, nil
	}

	p.logger.Debug("processing array items",
		Field{Key: "item_count", Value: len(items)},
		Field{Key: "workers", Value: p.config.WorkerPool.NumWorkers},
	)

	// Create subflow processor (implements ItemProcessor)
	subflowCfg := SubflowConfig{
		ParentNodeId: unit.NodeId,
		NodeConfigs:  unit.EmbeddedNodes,
		Factory:      p.factory,
		ArrayPath:    iterCtx.ArrayPath,
		Logger:       p.logger,
	}
	subflow, err := NewSubflowProcessor(subflowCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create subflow processor: %w", err)
	}

	// Create worker pool
	pool := NewWorkerPool(p.config.WorkerPool, subflow, p.limiter, p.logger)

	// Start workers
	pool.Start(ctx)

	// Create batch items
	batchItems := CreateBatchItems(items)

	// Submit items in a goroutine
	go pool.SubmitAll(ctx, batchItems)

	// Collect results
	results, resultItems, err := CollectResults(pool.Results(), len(items))

	// Log stats
	processed, errors := pool.Stats()
	p.logger.Debug("processing complete",
		Field{Key: "processed", Value: processed},
		Field{Key: "errors", Value: errors},
	)

	if err != nil {
		return nil, err
	}

	// Build final items: if subflow produced per-item Items, prefer them; otherwise use results (per-item Output)
	finalItems := make([]map[string]interface{}, len(results))
	for i := range results {
		// merge resultItems[i] (which is []map[string]interface{}) into a single map if present
		if len(resultItems[i]) > 0 {
			// merge all maps in resultItems[i]
			merged := make(map[string]interface{})
			for _, m := range resultItems[i] {
				MergeMaps(merged, m)
			}
			// also merge results[i] on top
			MergeMaps(merged, results[i])
			finalItems[i] = merged
			continue
		}
		// fallback to results[i]
		finalItems[i] = results[i]
	}

	return &StandardUnitOutput{
		Data:  map[string]interface{}{},
		Items: finalItems,
	}, nil
}

// flattenParentOnly handles case with no embedded nodes.
func (p *EmbeddedProcessor) flattenParentOnly(
	parentOutput map[string]interface{},
	parentNodeId string,
) (*StandardUnitOutput, error) {
	// Check for array at top level
	for key, value := range parentOutput {
		if arr, ok := value.([]interface{}); ok {
			results := make([]map[string]interface{}, 0, len(arr))
			for _, item := range arr {
				if itemMap, ok := item.(map[string]interface{}); ok {
					flat := FlattenWithArrayPath(itemMap, parentNodeId, key)
					results = append(results, flat)
				}
			}
			return &StandardUnitOutput{Data: map[string]interface{}{}, Items: results}, nil
		}
	}

	// Single object
	flat := FlattenMap(parentOutput, parentNodeId, "")
	return &StandardUnitOutput{Data: flat, Items: []map[string]interface{}{}}, nil
}

// Config returns the processor configuration.
func (p *EmbeddedProcessor) Config() ProcessorConfig {
	return p.config
}

// Factory returns the node factory.
func (p *EmbeddedProcessor) Factory() EmbeddedNodeFactory {
	return p.factory
}
