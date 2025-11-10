package embedded

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/tidwall/gjson"
	"github.com/wehubfusion/Icarus/pkg/iteration"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// Processor handles execution of embedded nodes
type Processor struct {
	registry    *ExecutorRegistry
	fieldMapper *FieldMapper
}

// NewProcessor creates a new embedded node processor
func NewProcessor(registry *ExecutorRegistry) *Processor {
	return &Processor{
		registry:    registry,
		fieldMapper: NewFieldMapper(),
	}
}

// ProcessEmbeddedNodes processes all embedded nodes in a message sequentially
// It chains the output of each node to the next, applying field mappings
func (p *Processor) ProcessEmbeddedNodes(
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
		startTime := time.Now()

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
			if err != nil {
				// Record iteration failure
				results = append(results, EmbeddedNodeResult{
					NodeID:               embNode.NodeID,
					PluginType:           embNode.PluginType,
					Status:               "failed",
					Output:               []byte("{}"),
					Error:                fmt.Sprintf("array iteration failed: %v", err),
					ExecutionOrder:       embNode.ExecutionOrder,
					ProcessingDurationMs: time.Since(startTime).Milliseconds(),
				})
				continue
			}

			// Aggregate item results into single output
			aggregated, _ := json.Marshal(itemResults)
			results = append(results, EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "success",
				Output:               aggregated,
				Error:                "",
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: time.Since(startTime).Milliseconds(),
			})
			// Store result in registry
			outputRegistry.Set(embNode.NodeID, aggregated)
			continue
		}

		// Normal processing (no iteration)
		// Apply field mappings to prepare input for this node
		mappedInput, err := p.fieldMapper.ApplyMappings(outputRegistry, embNode.FieldMappings, []byte("{}"))
		if err != nil {
			// Field mapping failed - record as failed result
			results = append(results, EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               []byte("{}"),
				Error:                fmt.Sprintf("field mapping failed: %v", err),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: time.Since(startTime).Milliseconds(),
			})
			continue
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
		if err != nil {
			// Execution failed - record as failed result
			results = append(results, EmbeddedNodeResult{
				NodeID:               embNode.NodeID,
				PluginType:           embNode.PluginType,
				Status:               "failed",
				Output:               []byte("{}"),
				Error:                err.Error(),
				ExecutionOrder:       embNode.ExecutionOrder,
				ProcessingDurationMs: time.Since(startTime).Milliseconds(),
			})
			continue
		}

		// Store result in registry and results list
		outputRegistry.Set(embNode.NodeID, nodeOutput)
		results = append(results, EmbeddedNodeResult{
			NodeID:               embNode.NodeID,
			PluginType:           embNode.PluginType,
			Status:               "success",
			Output:               nodeOutput,
			Error:                "",
			ExecutionOrder:       embNode.ExecutionOrder,
			ProcessingDurationMs: time.Since(startTime).Milliseconds(),
		})
	}

	return results, nil
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
	iterator := iteration.NewIterator(iteration.Config{
		Strategy: iteration.StrategySequential,
	})

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
