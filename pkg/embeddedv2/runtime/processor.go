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
// It handles two iteration scenarios:
//
// 1. Parent-level iteration:
//   - Parent node outputs an array (e.g., {data: [...]})
//   - First embedded node has iterate:true from parent's array
//   - processWithConcurrency() handles parallel processing of array items
//   - Single contains parent's non-array fields (metadata, status, etc.)
//   - Array contains per-item results from all embedded nodes
//
// 2. Mid-flow iteration:
//   - Parent node outputs a single object
//   - Some embedded node mid-flow produces an array
//   - Later embedded node has iterate:true from that array
//   - processSingleObject() handles this via SubflowProcessor
//   - SubflowProcessor internally handles the per-item subflow execution
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

// analyzeIterationContext determines if parent-level array iteration is needed.
// This only detects iteration that starts from the parent node's output.
// Mid-flow iteration (where an embedded node produces an array) is handled
// separately by SubflowProcessor during processSingleObject().
//
// Returns IterationContext with:
// - IsArrayIteration: true if any embedded node has iterate:true from parent's array
// - ArrayPath: the path to the array in parent output (e.g., "data" from "/data//field")
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
	// Create subflow processor with concurrency support for mid-flow iteration
	subflowCfg := SubflowConfig{
		ParentNodeId:     unit.NodeId,
		NodeConfigs:      unit.EmbeddedNodes,
		Factory:          p.factory,
		ArrayPath:        "",
		Logger:           p.logger,
		Limiter:          p.limiter,
		WorkerPoolConfig: p.config.WorkerPool,
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
		Single: result.Output,
		Array:  result.Items,
	}, nil
}

// processWithConcurrency handles parent-level array iteration with worker pool.
// This is used when the parent node's output contains an array and embedded nodes
// have iterate:true mappings from that array.
//
// The function:
// 1. Extracts the array from parentOutput at iterCtx.ArrayPath
// 2. Preserves non-array fields from parent for the Single output
// 3. Creates a SubflowProcessor to run all embedded nodes for each item
// 4. Uses a worker pool for concurrent processing
// 5. Returns StandardUnitOutput where:
//   - Single contains parent's non-array fields (shared across all items)
//   - Array contains per-item outputs from embedded nodes
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
		// Extract and flatten parent's non-array fields for Single output
		nonArrayFields := p.extractNonArrayFields(parentOutput, iterCtx.ArrayPath)
		flatSingle := FlattenMap(nonArrayFields, unit.NodeId, "")
		return &StandardUnitOutput{
			Single: flatSingle,
			Array:  []map[string]interface{}{},
		}, nil
	}

	p.logger.Debug("processing array items",
		Field{Key: "item_count", Value: len(items)},
		Field{Key: "workers", Value: p.config.WorkerPool.NumWorkers},
	)

	// Create subflow processor (implements ItemProcessor) with concurrency support
	subflowCfg := SubflowConfig{
		ParentNodeId:     unit.NodeId,
		NodeConfigs:      unit.EmbeddedNodes,
		Factory:          p.factory,
		ArrayPath:        iterCtx.ArrayPath,
		Logger:           p.logger,
		Limiter:          p.limiter,
		WorkerPoolConfig: p.config.WorkerPool,
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

	// Extract and flatten parent's non-array fields for Single output
	nonArrayFields := p.extractNonArrayFields(parentOutput, iterCtx.ArrayPath)
	flatSingle := FlattenMap(nonArrayFields, unit.NodeId, "")

	return &StandardUnitOutput{
		Single: flatSingle,
		Array:  finalItems,
	}, nil
}

// flattenParentOnly handles case with no embedded nodes.
// If parent has an array, it flattens the array items into Array and non-array fields into Single.
// If parent is a single object, it flattens everything into Single.
func (p *EmbeddedProcessor) flattenParentOnly(
	parentOutput map[string]interface{},
	parentNodeId string,
) (*StandardUnitOutput, error) {
	// Check for array at top level
	for key, value := range parentOutput {
		if arr, ok := value.([]interface{}); ok {
			// Flatten array items into Array
			results := make([]map[string]interface{}, 0, len(arr))
			for _, item := range arr {
				if itemMap, ok := item.(map[string]interface{}); ok {
					flat := FlattenWithArrayPath(itemMap, parentNodeId, key)
					results = append(results, flat)
				}
			}
			// Extract and flatten non-array fields into Single
			nonArrayFields := p.extractNonArrayFields(parentOutput, key)
			flatSingle := FlattenMap(nonArrayFields, parentNodeId, "")
			return &StandardUnitOutput{Single: flatSingle, Array: results}, nil
		}
	}

	// Single object - all goes to Single
	flat := FlattenMap(parentOutput, parentNodeId, "")
	return &StandardUnitOutput{Single: flat, Array: []map[string]interface{}{}}, nil
}

// extractNonArrayFields returns a copy of parentOutput without the specified array field.
// This preserves metadata and other shared fields when the parent outputs an array.
func (p *EmbeddedProcessor) extractNonArrayFields(parentOutput map[string]interface{}, arrayPath string) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range parentOutput {
		if key == arrayPath {
			continue // Skip the array being iterated
		}
		result[key] = value
	}
	return result
}

// Config returns the processor configuration.
func (p *EmbeddedProcessor) Config() ProcessorConfig {
	return p.config
}

// Factory returns the node factory.
func (p *EmbeddedProcessor) Factory() EmbeddedNodeFactory {
	return p.factory
}
