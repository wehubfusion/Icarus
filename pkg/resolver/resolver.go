package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// DefaultMaxInlineBytes defines the default threshold (500KB) for inline payloads.
// This is set below NATS 1MB limit to leave room for message metadata and headers.
const DefaultMaxInlineBytes = 500 * 1024 // 500KB

// ResultMeta contains metadata required to build deterministic blob paths.
type ResultMeta struct {
	WorkflowID  string
	RunID       string
	NodeID      string
	ExecutionID string
}

// Result describes the outcome of CreateResult.
type Result struct {
	InlineData    []byte
	BlobReference *message.BlobReference
	UsedBlob      bool
}

// Service wraps blob-related helpers so plugins stay blob-agnostic.
type Service struct {
	blobClient     storage.BlobStorageClient
	maxInlineBytes int
}

// NewService builds a resolver service. If blobClient is nil, only inline resolution works.
func NewService(blobClient storage.BlobStorageClient, maxInlineBytes int) *Service {
	if maxInlineBytes <= 0 {
		maxInlineBytes = DefaultMaxInlineBytes
	}
	return &Service{
		blobClient:     blobClient,
		maxInlineBytes: maxInlineBytes,
	}
}

// ResolveInput returns inline data or downloads it from blob storage when a reference is provided.
func (s *Service) ResolveInput(ctx context.Context, inline []byte, blobRef *message.BlobReference) ([]byte, error) {
	if len(inline) > 0 {
		return inline, nil
	}

	if blobRef == nil || blobRef.URL == "" {
		return nil, fmt.Errorf("resolver: no inline data or blob reference provided")
	}

	if s.blobClient == nil {
		return nil, fmt.Errorf("resolver: blob client not configured for blob reference resolution")
	}

	data, err := s.blobClient.DownloadResult(ctx, blobRef.URL)
	if err != nil {
		return nil, fmt.Errorf("resolver: failed to download input from blob: %w", err)
	}
	return data, nil
}

// FieldMappingParams describes how resolver should apply field mappings.
type FieldMappingParams struct {
	FieldMappings    []message.FieldMapping
	SourceResults    map[string]*SourceResult
	TriggerData      []byte
	BlobSourceNodeID string
	ConsumerGraph    *ConsumerGraph // Optional: consumer graph for multi-blob downloads
}

// SourceResult contains the minimal data required to evaluate field mappings.
type SourceResult struct {
	NodeID            string
	Status            string
	ProjectedFields   map[string]map[string]interface{}
	IterationMetadata map[string]*IterationContext
}

// IterationContext mirrors array metadata used for coordinated iteration.
type IterationContext struct {
	IsArray     bool
	ArrayPath   string
	ArrayLength int
}

// ResolveMappedInput resolves the payload (inline or blob) and applies field mappings when provided.
// Priority: BlobRef > Inline
func (s *Service) ResolveMappedInput(
	ctx context.Context,
	inline []byte,
	blobRef *message.BlobReference,
	params *FieldMappingParams,
) ([]byte, error) {
	var base []byte
	var err error

	// Resolve input from blob reference or inline data
	if len(inline) > 0 || (blobRef != nil && blobRef.URL != "") {
		base, err = s.ResolveInput(ctx, inline, blobRef)
		if err != nil {
			return nil, err
		}
	}

	return s.buildInputFromFieldMappings(base, params)
}

// ResolveMappedInputWithConsumerGraph resolves input using consumer graph to download multiple blob files.
// This method handles cases where source nodes are in different blob files (e.g., Unit N-2, embedded nodes).
// If consumerGraph is nil, it falls back to ResolveMappedInput behavior.
func (s *Service) ResolveMappedInputWithConsumerGraph(
	ctx context.Context,
	inline []byte,
	blobRef *message.BlobReference,
	params *FieldMappingParams,
	consumerGraph *ConsumerGraph,
) ([]byte, error) {
	// If no consumer graph provided, fall back to standard behavior
	if consumerGraph == nil {
		return s.ResolveMappedInput(ctx, inline, blobRef, params)
	}

	// If we have inline data or blob ref, and no field mappings, use standard resolution
	if (len(inline) > 0 || (blobRef != nil && blobRef.URL != "")) && (params == nil || len(params.FieldMappings) == 0) {
		return s.ResolveMappedInput(ctx, inline, blobRef, params)
	}

	// If no field mappings but we have a consumer graph, try to use it
	if params == nil || len(params.FieldMappings) == 0 {
		// No field mappings - check if consumer graph has files we can use
		if len(consumerGraph.RequiredFiles) > 0 {
			// If there's exactly one file, use it directly
			if len(consumerGraph.RequiredFiles) == 1 {
				var requiredFile *RequiredBlobFile
				for _, file := range consumerGraph.RequiredFiles {
					requiredFile = file
					break
				}

				if requiredFile != nil && requiredFile.BlobURL != "" {
					if s.blobClient == nil {
						return nil, fmt.Errorf("resolver: blob client not configured")
					}

					data, err := s.blobClient.DownloadResult(ctx, requiredFile.BlobURL)
					if err != nil {
						return nil, fmt.Errorf("resolver: failed to download blob file from consumer graph: %w", err)
					}
					return data, nil
				}
			}

			// Multiple files but no field mappings - can't determine which to use
			return nil, fmt.Errorf("resolver: consumer graph has %d files but no field mappings to determine which to use", len(consumerGraph.RequiredFiles))
		}

		// No files in consumer graph - fall back to standard resolution
		return s.ResolveMappedInput(ctx, inline, blobRef, params)
	}

	// Determine which blob files are needed
	requiredFiles := consumerGraph.DetermineRequiredFiles(params.FieldMappings)

	// Download and parse required blob files (if any)
	sourceResults := make(map[string]*SourceResult)
	if len(requiredFiles) > 0 {
		var err error
		sourceResults, err = s.downloadAndParseBlobFiles(ctx, requiredFiles, params.FieldMappings)
		if err != nil {
			return nil, fmt.Errorf("resolver: failed to download and parse blob files: %w", err)
		}
	}

	// Merge with any existing source results
	if params.SourceResults == nil {
		params.SourceResults = make(map[string]*SourceResult)
	}
	for nodeID, result := range sourceResults {
		params.SourceResults[nodeID] = result
	}

	// Check ResultLocations for nodes with inline data that aren't in source results yet
	if consumerGraph.ResultLocations != nil {
		// Extract source node IDs from field mappings
		sourceNodeIDs := make(map[string]bool)
		for _, mapping := range params.FieldMappings {
			if !mapping.IsEventTrigger && mapping.SourceNodeID != "" {
				sourceNodeIDs[mapping.SourceNodeID] = true
			}
		}

		for nodeID := range sourceNodeIDs {
			// Skip if we already have this node in source results
			if _, exists := params.SourceResults[nodeID]; exists {
				continue
			}

			// Check if this node has inline data in ResultLocations
			if location, exists := consumerGraph.ResultLocations[nodeID]; exists && location != nil {
				if location.HasInlineData && len(location.InlineData) > 0 {
					// Parse inline data
					var inlineData map[string]interface{}
					if err := json.Unmarshal(location.InlineData, &inlineData); err == nil {
						var nodeFields map[string]interface{}

						// Check if this is StandardUnitOutput format (has "single" and "array" fields)
						singleMap, hasSingle := inlineData["single"].(map[string]interface{})
						arraySlice, hasArray := inlineData["array"].([]interface{})
						isStandardUnitOutput := hasSingle && hasArray

						if isStandardUnitOutput {
							// Extract and restructure data from StandardUnitOutput format
							nodeFields = extractNodeDataFromStandardOutput(singleMap, arraySlice, nodeID)
							// Handle empty extraction case (same logic as sourceResultsFromBlob)
							if len(nodeFields) == 0 {
								if len(singleMap) == 0 && len(arraySlice) == 0 {
									// Empty StandardUnitOutput - use inlineData as fallback
									nodeFields = inlineData
								} else {
									// Single/array has data but no keys for this nodeID
									nodeFields = make(map[string]interface{})
								}
							}
						} else {
							// Not StandardUnitOutput - use as-is
							nodeFields = inlineData
						}

						// Create SourceResult with extracted/restructured data
						params.SourceResults[nodeID] = &SourceResult{
							NodeID:            nodeID,
							Status:            "success", // Assume success for inline data
							ProjectedFields:   map[string]map[string]interface{}{nodeID: nodeFields},
							IterationMetadata: nil,
						}
					}
				}
			}
		}
	}

	// Build input using field mappings with all source results
	buildParams := BuildInputParams{
		UnitNodeID:    params.BlobSourceNodeID,
		FieldMappings: params.FieldMappings,
		SourceResults: params.SourceResults,
		TriggerData:   params.TriggerData,
	}

	return buildInputFromMappings(buildParams)
}

// downloadAndParseBlobFiles downloads multiple blob files in parallel and extracts SourceResults.
func (s *Service) downloadAndParseBlobFiles(
	ctx context.Context,
	requiredFiles []*RequiredBlobFile,
	fieldMappings []message.FieldMapping,
) (map[string]*SourceResult, error) {
	if s.blobClient == nil {
		return nil, fmt.Errorf("resolver: blob client not configured for multi-blob download")
	}

	// Extract all source node IDs we need
	sourceNodeIDs := make(map[string]bool)
	for _, mapping := range fieldMappings {
		if !mapping.IsEventTrigger && mapping.SourceNodeID != "" {
			sourceNodeIDs[mapping.SourceNodeID] = true
		}
	}

	// Download all files in parallel
	type downloadResult struct {
		file *RequiredBlobFile
		data []byte
		err  error
	}

	var wg sync.WaitGroup
	results := make([]downloadResult, len(requiredFiles))

	for i, file := range requiredFiles {
		wg.Add(1)
		go func(idx int, f *RequiredBlobFile) {
			defer wg.Done()
			data, err := s.blobClient.DownloadResult(ctx, f.BlobURL)
			results[idx] = downloadResult{
				file: f,
				data: data,
				err:  err,
			}
		}(i, file)
	}

	// Wait for all downloads to complete
	wg.Wait()

	allSourceResults := make(map[string]*SourceResult)
	for _, result := range results {
		if result.err != nil {
			return nil, fmt.Errorf("resolver: failed to download blob file %s: %w", result.file.BlobURL, result.err)
		}

		// Parse blob to extract source results
		parsedResults := sourceResultsFromBlob(result.data, sourceNodeIDs, result.file.ContainsNodes)
		for nodeID, sourceResult := range parsedResults {
			allSourceResults[nodeID] = sourceResult
		}
	}

	return allSourceResults, nil
}

// buildInputFromFieldMappings centralizes the logic for constructing unit inputs using field mappings.
func (s *Service) buildInputFromFieldMappings(
	base []byte,
	params *FieldMappingParams,
) ([]byte, error) {
	// If no field mappings, try to extract data from NodeExecutionResult format
	if params == nil || len(params.FieldMappings) == 0 {
		if len(base) > 0 {
			// Return blob data as-is when no field mappings
			return base, nil
		}
		// No field mappings - return empty object (don't fall back to trigger data)
		return []byte("{}"), nil
	}

	sourceResults := params.SourceResults
	// Note: When using consumer graph, sourceResults should already be populated
	// from downloadAndParseBlobFiles. If not, we can't extract from base blob
	// without knowing which nodes are in the file (requires ContainsNodes).

	buildParams := BuildInputParams{
		UnitNodeID:    params.BlobSourceNodeID,
		FieldMappings: params.FieldMappings,
		SourceResults: sourceResults,
		TriggerData:   params.TriggerData,
	}

	return buildInputFromMappings(buildParams)
}

// sourceResultsFromBlob parses blob data and creates SourceResults for nodes in the file.
// The blob file contains JSON data for one or more nodes (identified by containsNodes).
// For each node ID in containsNodes that matches sourceNodeIDs, creates a SourceResult
// with the blob data in ProjectedFields.
func sourceResultsFromBlob(blobData []byte, sourceNodeIDs map[string]bool, containsNodes []string) map[string]*SourceResult {
	if len(blobData) == 0 || len(sourceNodeIDs) == 0 || len(containsNodes) == 0 {
		return nil
	}

	// Parse blob as generic JSON object
	var blobContent map[string]interface{}
	if err := json.Unmarshal(blobData, &blobContent); err != nil {
		return nil
	}

	// Extract status if available (default to "success" if not found)
	status := "success"
	if s, ok := blobContent["status"].(string); ok && s != "" {
		status = s
	}

	// Check if blob is in StandardUnitOutput format (has "single" and "array" fields)
	singleMap, hasSingle := blobContent["single"].(map[string]interface{})
	arraySlice, hasArray := blobContent["array"].([]interface{})
	isStandardUnitOutput := hasSingle && hasArray

	// Build source results map for each node ID in containsNodes that matches sourceNodeIDs
	result := make(map[string]*SourceResult)
	for _, nodeID := range containsNodes {
		// Only create SourceResult for nodes that are in sourceNodeIDs
		if !sourceNodeIDs[nodeID] {
			continue
		}

		var nodeFields map[string]interface{}

		if isStandardUnitOutput {
			// Extract and restructure data from StandardUnitOutput format
			nodeFields = extractNodeDataFromStandardOutput(singleMap, arraySlice, nodeID)
			// If extraction returned empty (no matching keys), check if single map has any keys
			// If single map is empty or has no matching keys, this blob doesn't contain data for this node
			// In that case, return empty map so field mapping can handle it appropriately
			if len(nodeFields) == 0 {
				// Check if single map has any keys at all (might be empty StandardUnitOutput)
				if len(singleMap) == 0 && len(arraySlice) == 0 {
					// Empty StandardUnitOutput - use blob content as fallback
					nodeFields = blobContent
				} else {
					// Single/array has data but no keys for this nodeID
					// Return empty map - this blob doesn't contain data for this node
					nodeFields = make(map[string]interface{})
				}
			}
		} else {
			// Use blob content as-is (for other formats)
			nodeFields = blobContent
		}

		// Create SourceResult with the extracted/restructured data
		result[nodeID] = &SourceResult{
			NodeID:            nodeID,
			Status:            status,
			ProjectedFields:   map[string]map[string]interface{}{nodeID: nodeFields},
			IterationMetadata: nil,
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// extractNodeDataFromStandardOutput extracts and restructures data from StandardUnitOutput format.
// Converts flattened keys like "nodeId-/path" to nested structure like {"path": value}.
// Handles both single map and array slice data.
func extractNodeDataFromStandardOutput(single map[string]interface{}, array []interface{}, nodeID string) map[string]interface{} {
	result := make(map[string]interface{})
	prefix := nodeID + "-/"

	// Extract from single map - keys are formatted as "nodeId-/path"
	for key, value := range single {
		if strings.HasPrefix(key, prefix) {
			// Extract path (everything after "nodeId-/")
			path := key[len(prefix):]
			if path != "" {
				// Build nested structure from path
				setNestedValue(result, path, value)
			}
		}
	}

	// Extract from array slice if present
	// Array items contain keys formatted as "nodeId-/arrayPath//field" or "nodeId-/field"
	if len(array) > 0 {
		// First, collect simple paths (no "//") from array items
		// For embedded node outputs like "9f250ca5.../result", we collect all simple fields per item
		simpleArrayItems := make([]map[string]interface{}, 0)
		for _, item := range array {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			// For each array item, collect all simple fields (no "//") with this node prefix
			simpleFields := make(map[string]interface{})
			for key, value := range itemMap {
				if strings.HasPrefix(key, prefix) {
					path := key[len(prefix):]
					if path != "" && !strings.Contains(path, "//") {
						// Simple field - add directly to this item's object
						simpleFields[path] = value
					}
				}
			}
			// If this item has any simple fields for this node, add to array
			if len(simpleFields) > 0 {
				simpleArrayItems = append(simpleArrayItems, simpleFields)
			}
		}

		// If we have simple array items, determine the array path
		// For embedded nodes with simple fields, we typically want a root-level array
		if len(simpleArrayItems) > 0 {
			// Check if all items have the same field(s) - if so, this is a simple array
			// For now, add as root-level array if there's only one field, otherwise merge into result
			if len(simpleArrayItems[0]) == 1 {
				// Single field per item - create array with that field name
				for fieldName := range simpleArrayItems[0] {
					values := make([]interface{}, len(simpleArrayItems))
					for i, item := range simpleArrayItems {
						values[i] = item[fieldName]
					}
					result[fieldName] = values
				}
			} else {
				// Multiple fields per item - keep as array of objects
				// Use empty string as key to indicate root-level array
				result[""] = convertToInterfaceSlice(simpleArrayItems)
			}
		}

		// Group array items by array path
		arrayStructures := buildArrayStructure(array, nodeID, prefix)
		// Merge array structures into result
		for arrayPath, arrayData := range arrayStructures {
			// Check for path conflicts - if arrayPath already exists as a single field
			if existing, exists := result[arrayPath]; exists {
				// If existing is not an array, we have a conflict
				// Prefer array structure over single field
				if _, isArray := existing.([]interface{}); !isArray {
					// Remove single field and use array instead
					delete(result, arrayPath)
				}
			}
			result[arrayPath] = arrayData
		}
	}

	return result
}

// buildArrayStructure builds array structure from array items.
// Groups keys by array path and builds arrays of objects.
func buildArrayStructure(array []interface{}, nodeID, prefix string) map[string][]interface{} {
	arrayStructures := make(map[string][]interface{})

	// Process each array item
	for itemIndex, item := range array {
		if itemMap, ok := item.(map[string]interface{}); ok {
			// Group keys by array path
			itemsByArrayPath := make(map[string]map[string]interface{})

			for key, value := range itemMap {
				if strings.HasPrefix(key, prefix) {
					path := key[len(prefix):]
					if path != "" {
						// Check if it's an array item path (contains "//")
						if strings.Contains(path, "//") {
							// Array item path: "arrayPath//field" or "arrayPath//parent/field"
							parts := strings.SplitN(path, "//", 2)
							if len(parts) == 2 {
								arrayPath := strings.Trim(parts[0], "/")
								fieldPath := parts[1]

								// Initialize array path structure if needed
								if itemsByArrayPath[arrayPath] == nil {
									itemsByArrayPath[arrayPath] = make(map[string]interface{})
								}

								// Set nested value in the item map
								setNestedValue(itemsByArrayPath[arrayPath], fieldPath, value)
							}
						} else {
							// Regular path (not array item) - this shouldn't happen in array items
							// but handle it gracefully by treating as root-level field
							// Skip it to avoid conflicts
						}
					}
				}
			}

			// Add items to their respective arrays
			for arrayPath, itemData := range itemsByArrayPath {
				if arrayStructures[arrayPath] == nil {
					arrayStructures[arrayPath] = make([]interface{}, len(array))
					// Initialize all items as empty maps
					for i := range arrayStructures[arrayPath] {
						arrayStructures[arrayPath][i] = make(map[string]interface{})
					}
				}
				// Merge item data into the appropriate index
				if itemIndex < len(arrayStructures[arrayPath]) {
					if existingItem, ok := arrayStructures[arrayPath][itemIndex].(map[string]interface{}); ok {
						// Merge maps
						for k, v := range itemData {
							existingItem[k] = v
						}
					}
				}
			}
		}
	}

	return arrayStructures
}

// setNestedValue sets a value in a nested map structure using a path like "/payload" or "/data/field".
// Creates intermediate maps as needed.
func setNestedValue(m map[string]interface{}, path string, value interface{}) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return
	}

	parts := strings.Split(path, "/")
	current := m

	for i, part := range parts {
		if part == "" {
			continue
		}

		if i == len(parts)-1 {
			// Last part - set the value
			current[part] = value
		} else {
			// Intermediate part - create nested map if needed
			if current[part] == nil {
				current[part] = make(map[string]interface{})
			}
			if nextMap, ok := current[part].(map[string]interface{}); ok {
				current = nextMap
			} else {
				// Path conflict - existing value is not a map, can't create nested structure
				// Overwrite with new map structure
				current[part] = make(map[string]interface{})
				current = current[part].(map[string]interface{})
			}
		}
	}
}

// convertToInterfaceSlice converts a slice of maps to a slice of interfaces.
func convertToInterfaceSlice(items []map[string]interface{}) []interface{} {
	result := make([]interface{}, len(items))
	for i, item := range items {
		result[i] = item
	}
	return result
}

// CreateResult decides whether to return inline data or upload it to blob storage.
func (s *Service) CreateResult(ctx context.Context, data []byte, meta ResultMeta) (*Result, error) {
	if len(data) == 0 {
		return &Result{InlineData: data}, nil
	}

	if len(data) <= s.maxInlineBytes || s.blobClient == nil {
		return &Result{
			InlineData: data,
		}, nil
	}

	if meta.WorkflowID == "" || meta.RunID == "" {
		return nil, fmt.Errorf("resolver: workflow metadata required for blob result")
	}

	nodeID := meta.NodeID
	if nodeID == "" {
		nodeID = "unknown-node"
	}

	execID := meta.ExecutionID
	if execID == "" {
		execID = nodeID
	}

	blobPath := fmt.Sprintf("results/%s/%s/%s.json",
		sanitizeBlobPathPart(meta.WorkflowID, "workflow"),
		sanitizeBlobPathPart(meta.RunID, "run"),
		sanitizeBlobPathPart(execID, "execution"),
	)

	blobURL, err := s.blobClient.UploadResult(ctx, blobPath, data, map[string]string{
		"workflow_id":  meta.WorkflowID,
		"run_id":       meta.RunID,
		"execution_id": execID,
		"node_id":      nodeID,
	})
	if err != nil {
		return nil, fmt.Errorf("resolver: failed to upload result to blob: %w", err)
	}

	return &Result{
		BlobReference: &message.BlobReference{
			URL:       blobURL,
			SizeBytes: len(data),
		},
		UsedBlob: true,
	}, nil
}

func sanitizeBlobPathPart(part string, fallback string) string {
	part = strings.TrimSpace(part)
	if part == "" {
		return fallback
	}
	part = strings.ReplaceAll(part, "/", "-")
	part = strings.ReplaceAll(part, "\\", "-")
	return part
}
