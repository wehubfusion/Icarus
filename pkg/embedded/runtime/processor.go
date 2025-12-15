package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// collectResult holds the results from CollectResults for channel communication
type collectResult struct {
	results     []map[string]interface{}
	resultItems [][]map[string]interface{}
	err         error
}

// collectResultsWithContext collects results with context cancellation support
// It will return when all results are received, the channel is closed, or context is cancelled
func collectResultsWithContext(ctx context.Context, resultChan <-chan BatchResult, count int) ([]map[string]interface{}, [][]map[string]interface{}, error) {
	results := make([]map[string]interface{}, count)
	items := make([][]map[string]interface{}, count)
	var firstError error

	received := 0
	// Track which indices we've received to detect if we're stuck
	receivedIndices := make(map[int]bool)

	// Add a "no progress" timeout - adaptive based on batch size and concurrent load
	// For large batches or concurrent scenarios, allow more time before first result
	// Base timeout: 1 minute, but scale up significantly for large batches
	baseTimeout := 1 * time.Minute
	if count > 10000 {
		// For very large batches (10k+), allow up to 15 minutes before first result
		// This accounts for worker startup delays and heavy concurrent load
		baseTimeout = 15 * time.Minute
	} else if count > 1000 {
		// For medium batches (1k-10k), allow 10 minutes
		baseTimeout = 10 * time.Minute
	} else if count > 100 {
		// For small-medium batches (100-1k), allow 5 minutes
		baseTimeout = 5 * time.Minute
	}
	// After first result, use longer timeout (2 minutes) to detect stalls
	// Increased from 30s to handle slower processing under concurrent load
	initialTimeout := baseTimeout
	progressTimeout := 2 * time.Minute

	noProgressTimeout := time.NewTimer(initialTimeout)
	defer noProgressTimeout.Stop()
	lastResultTime := time.Now()
	firstResultReceived := false

	// Overall timeout - maximum time to wait for all results
	// Scale up significantly for large batches to account for processing time and concurrent load
	overallTimeoutDuration := 10 * time.Minute
	if count > 10000 {
		// For very large batches, allow up to 60 minutes total
		// This accounts for processing 10k+ items under heavy concurrent load
		overallTimeoutDuration = 60 * time.Minute
	} else if count > 1000 {
		// For medium batches, allow 30 minutes
		overallTimeoutDuration = 30 * time.Minute
	} else if count > 100 {
		// For small-medium batches, allow 20 minutes
		overallTimeoutDuration = 20 * time.Minute
	}
	overallTimeout := time.NewTimer(overallTimeoutDuration)
	defer overallTimeout.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled - return partial results
			return results, items, fmt.Errorf("context cancelled during result collection: %w", ctx.Err())
		case <-overallTimeout.C:
			// Overall timeout - we've been waiting too long total
			return results, items, fmt.Errorf("overall timeout waiting for results: received %d/%d results", received, count)
		case <-noProgressTimeout.C:
			// No progress timeout - no results received within timeout period
			timeSinceLastResult := time.Since(lastResultTime)
			if received == 0 {
				return results, items, fmt.Errorf("no progress timeout: received 0/%d results, no results received within %v (workers may be resource constrained or blocked)", count, initialTimeout)
			}
			return results, items, fmt.Errorf("no progress timeout: received %d/%d results, last result %v ago", received, count, timeSinceLastResult)
		case result, ok := <-resultChan:
			if !ok {
				// Channel closed - return what we have
				if received < count {
					// We didn't get all results, but channel is closed
					return results, items, fmt.Errorf("result channel closed prematurely: received %d/%d results", received, count)
				}
				return results, items, firstError
			}
			if result.Error != nil && firstError == nil {
				firstError = result.Error
			}
			if result.Index >= 0 && result.Index < count {
				if result.Output != nil {
					results[result.Index] = result.Output
				}
				if len(result.Items) > 0 {
					items[result.Index] = result.Items
				}
				receivedIndices[result.Index] = true
			}
			received++
			lastResultTime = time.Now()
			// Reset no-progress timeout on each result
			// Use shorter timeout after first result is received
			if !noProgressTimeout.Stop() {
				<-noProgressTimeout.C
			}
			if !firstResultReceived {
				firstResultReceived = true
				// After first result, switch to shorter progress timeout
			}
			noProgressTimeout.Reset(progressTimeout)

			if received >= count {
				return results, items, firstError
			}
		}
	}
}

// EmbeddedProcessor handles processing of embedded nodes with concurrency support.
// Concurrency is controlled via an internal worker pool (no external limiter).
type EmbeddedProcessor struct {
	factory EmbeddedNodeFactory
	config  ProcessorConfig
	logger  Logger
}

// NewEmbeddedProcessor creates a new embedded processor with full configuration.
func NewEmbeddedProcessor(
	factory EmbeddedNodeFactory,
	config ProcessorConfig,
) *EmbeddedProcessor {
	config.Validate()

	logger := config.Logger
	if logger == nil {
		logger = &NoOpLogger{}
	}

	return &EmbeddedProcessor{
		factory: factory,
		config:  config,
		logger:  logger,
	}
}

// NewEmbeddedProcessorWithDefaults creates a processor with default configuration.
func NewEmbeddedProcessorWithDefaults(factory EmbeddedNodeFactory) *EmbeddedProcessor {
	return NewEmbeddedProcessor(factory, DefaultProcessorConfig())
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

	p.logger.Info("processing array items",
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
		WorkerPoolConfig: p.config.WorkerPool,
	}
	subflow, err := NewSubflowProcessor(subflowCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create subflow processor: %w", err)
	}

	// Create worker pool
	pool := NewWorkerPool(p.config.WorkerPool, subflow, p.logger)
	p.logger.Info("created worker pool", Field{Key: "num_workers", Value: p.config.WorkerPool.NumWorkers})

	// Start workers
	pool.Start(ctx)
	p.logger.Info("started worker pool")

	// Create batch items
	batchItems := CreateBatchItems(items)
	p.logger.Info("created batch items", Field{Key: "batch_count", Value: len(batchItems)})

	// Submit items and wait for completion in a goroutine
	// Use a separate context for the submission goroutine to ensure it completes
	submitDone := make(chan struct{})
	resultChanClosed := make(chan struct{}) // Track if result channel is closed
	go func() {
		defer close(submitDone)
		p.logger.Info("submitting batch items to worker pool")
		pool.SubmitAll(ctx, batchItems)
		p.logger.Info("submitted all batch items, waiting for workers to complete")
		// Wait for all workers to finish and close the result channel
		// This must happen after SubmitAll closes the job channel
		// Use a timeout to prevent indefinite blocking
		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.Wait()
			close(resultChanClosed) // Signal that result channel is now closed
		}()
		// Wait with timeout - if context is cancelled or workers are stuck, don't wait forever
		waitTimeout := 5 * time.Minute // Match the result collection timeout
		select {
		case <-waitDone:
			// Pool wait completed normally
			p.logger.Info("pool.Wait() completed normally")
			p.logger.Info("pool.Wait() completed normally")
		case <-ctx.Done():
			// Context cancelled - workers should exit when they check context
			// Give them a short time to finish, then continue
			select {
			case <-waitDone:
			case <-time.After(5 * time.Second):
				// Workers didn't finish in time, but we continue anyway
				// Force close result channel if pool.Wait() is stuck
				select {
				case <-resultChanClosed:
					// Already closed
				default:
					// Force close by calling pool.Close() in a goroutine with timeout
					go func() {
						pool.Close()
					}()
				}
			}
		case <-time.After(waitTimeout):
			// Workers are taking too long - something is stuck
			p.logger.Warn("pool.Wait() timeout - workers may be stuck", Field{Key: "timeout", Value: waitTimeout})
			// Force close result channel if not already closed
			select {
			case <-resultChanClosed:
				// Already closed
			default:
				// Force close by calling pool.Close() in a goroutine
				// This will close the job channel and wait for workers, then close result channel
				go func() {
					pool.Close()
				}()
			}
		}
	}()

	// Collect results with context cancellation support
	// Use a select to handle both context cancellation and result collection
	p.logger.Info("starting result collection")
	resultsChan := make(chan collectResult, 1)
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		p.logger.Info("collecting results from worker pool", Field{Key: "expected_count", Value: len(items)})
		collectedResults, collectedItems, collectErr := collectResultsWithContext(ctx, pool.Results(), len(items))
		p.logger.Info("finished collecting results",
			Field{Key: "results_count", Value: len(collectedResults)},
			Field{Key: "error", Value: collectErr != nil},
		)
		resultsChan <- collectResult{results: collectedResults, resultItems: collectedItems, err: collectErr}
	}()

	var results []map[string]interface{}
	var resultItems [][]map[string]interface{}
	var processErr error

	// Add overall timeout to prevent indefinite hanging
	overallTimeout := 2 * time.Minute
	overallTimer := time.NewTimer(overallTimeout)
	defer overallTimer.Stop()
	p.logger.Info("waiting for results or timeout", Field{Key: "overall_timeout", Value: overallTimeout})

	select {
	case <-ctx.Done():
		// Context cancelled - close the pool and return error
		pool.Close()
		// Wait for both goroutines to finish with timeout
		select {
		case <-submitDone:
		case <-time.After(5 * time.Second):
		}
		select {
		case <-collectDone:
		case <-time.After(5 * time.Second):
		}
		return nil, fmt.Errorf("context cancelled during embedded node processing: %w", ctx.Err())
	case <-overallTimer.C:
		// Overall timeout - something is stuck, return error with partial results if available
		p.logger.Error("overall timeout reached - workflow is hanging",
			Field{Key: "timeout", Value: overallTimeout},
		)
		pool.Close()
		// Try to get partial results if available
		select {
		case result := <-resultsChan:
			results = result.results
			resultItems = result.resultItems
			if result.err != nil {
				processErr = fmt.Errorf("overall timeout after %v: %w", overallTimeout, result.err)
			} else {
				processErr = fmt.Errorf("overall timeout after %v: processing incomplete", overallTimeout)
			}
		default:
			// No results available yet
			processErr = fmt.Errorf("overall timeout after %v: no results received", overallTimeout)
		}
		// Wait for goroutines with timeout
		select {
		case <-submitDone:
		case <-time.After(2 * time.Second):
		}
		select {
		case <-collectDone:
		case <-time.After(2 * time.Second):
		}
		// Return partial results even on timeout so caller can see what was processed
		if processErr != nil {
			return nil, processErr
		}
	case result := <-resultsChan:
		p.logger.Info("received results from collection goroutine",
			Field{Key: "results_count", Value: len(result.results)},
			Field{Key: "has_error", Value: result.err != nil},
		)
		results = result.results
		resultItems = result.resultItems
		processErr = result.err
		// Ensure both goroutines complete with timeout
		select {
		case <-submitDone:
		case <-time.After(10 * time.Second):
			// Submit goroutine is taking too long, but continue anyway
		}
		select {
		case <-collectDone:
		case <-time.After(10 * time.Second):
			// Collect goroutine is taking too long, but continue anyway
		}
	}

	// Log stats
	processed, errors := pool.Stats()
	p.logger.Debug("processing complete",
		Field{Key: "processed", Value: processed},
		Field{Key: "errors", Value: errors},
	)

	if processErr != nil {
		return nil, processErr
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
