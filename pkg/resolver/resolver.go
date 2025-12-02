package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// DefaultMaxInlineBytes defines the default threshold (1.5MB) for inline payloads.
const DefaultMaxInlineBytes = 1_500_000

// ResultMeta contains metadata required to build deterministic blob paths.
type ResultMeta struct {
	WorkflowID  string
	RunID       string
	NodeID      string
	ExecutionID string
}

// ResultFilePath returns the standard blob path for a workflow's result file
func ResultFilePath(workflowID, runID string) string {
	return storage.ResultFilePath(workflowID, runID)
}

// Result describes the outcome of CreateResult.
type Result struct {
	InlineData    []byte
	BlobReference *message.BlobReference
	UsedBlob      bool
}

// Service wraps blob-related helpers so plugins stay blob-agnostic.
type Service struct {
	blobClient       storage.BlobStorageClient
	resultFileClient *storage.ResultFileClient
	maxInlineBytes   int
}

// NewService builds a resolver service. If blobClient is nil, only inline resolution works.
func NewService(blobClient storage.BlobStorageClient, maxInlineBytes int) *Service {
	if maxInlineBytes <= 0 {
		maxInlineBytes = DefaultMaxInlineBytes
	}
	var resultFileClient *storage.ResultFileClient
	if blobClient != nil {
		resultFileClient = storage.NewResultFileClient(blobClient, nil)
	}
	return &Service{
		blobClient:       blobClient,
		resultFileClient: resultFileClient,
		maxInlineBytes:   maxInlineBytes,
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
	ResultFilePath   string // Path to the unified result file (when available)
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
// Priority: ResultFilePath > BlobRef > Inline
func (s *Service) ResolveMappedInput(
	ctx context.Context,
	inline []byte,
	blobRef *message.BlobReference,
	params *FieldMappingParams,
) ([]byte, error) {
	var base []byte
	var err error

	// Priority 1: If result file path is provided, load from unified result file
	if params != nil && params.ResultFilePath != "" && s.resultFileClient != nil {
		base, err = s.resultFileClient.GetResultFileRaw(ctx, params.ResultFilePath)
		if err != nil {
			return nil, fmt.Errorf("resolver: failed to load from result file: %w", err)
		}
	} else if len(inline) > 0 || (blobRef != nil && blobRef.URL != "") {
		// Priority 2/3: Inline data or blob reference
		base, err = s.ResolveInput(ctx, inline, blobRef)
		if err != nil {
			return nil, err
		}
	}

	return s.buildInputFromFieldMappings(base, params)
}

// buildInputFromFieldMappings centralizes the logic for constructing unit inputs using field mappings.
func (s *Service) buildInputFromFieldMappings(
	base []byte,
	params *FieldMappingParams,
) ([]byte, error) {
	// If no field mappings, try to extract data from NodeExecutionResult format
	if params == nil || len(params.FieldMappings) == 0 {
		if len(base) > 0 {
			// Check if base is a NodeExecutionResult with projected_fields
			// If so, try to extract the actual data for downstream units
			if params != nil && params.BlobSourceNodeID != "" {
				// For backward compatibility, try to extract using BlobSourceNodeID
				sourceNodeIDs := map[string]bool{params.BlobSourceNodeID: true}
				if sr := sourceResultsFromBlob(base, sourceNodeIDs); sr != nil {
					// Found NodeExecutionResult format - extract data from projected_fields
					// Return the first node's projected fields as the data
					for _, sourceResult := range sr {
						if len(sourceResult.ProjectedFields) > 0 {
							// Get the first embedded node's data (most common case)
							for _, fields := range sourceResult.ProjectedFields {
								if fields != nil {
									// Check if fields contain a data array
									if dataArray, ok := fields["data"].([]interface{}); ok {
										return json.Marshal(dataArray)
									}
									// Otherwise return the fields as-is
									return json.Marshal(fields)
								}
							}
						}
					}
				}
			}
			// Not a NodeExecutionResult or couldn't extract - return as-is
			return base, nil
		}
		// No field mappings - return empty object (don't fall back to trigger data)
		return []byte("{}"), nil
	}

	sourceResults := params.SourceResults
	if len(sourceResults) == 0 && len(base) > 0 && len(params.FieldMappings) > 0 {
		// Extract source node IDs from field mappings
		sourceNodeIDs := make(map[string]bool)
		for _, mapping := range params.FieldMappings {
			if mapping.SourceNodeID != "" {
				sourceNodeIDs[mapping.SourceNodeID] = true
			}
		}
		// Try to extract source results from blob for each source node ID
		if sr := sourceResultsFromBlob(base, sourceNodeIDs); len(sr) > 0 {
			sourceResults = sr
		}
	}

	buildParams := BuildInputParams{
		UnitNodeID:    params.BlobSourceNodeID,
		FieldMappings: params.FieldMappings,
		SourceResults: sourceResults,
		TriggerData:   params.TriggerData,
	}

	return buildInputFromMappings(buildParams)
}

// sourceResultsFromBlob attempts to reconstruct SourceResults by parsing the blob payload.
// It first tries to parse as the new result file format (map of node_id -> NodeResult),
// then falls back to the legacy nodeExecutionPayload format.
func sourceResultsFromBlob(blobData []byte, sourceNodeIDs map[string]bool) map[string]*SourceResult {
	if len(blobData) == 0 || len(sourceNodeIDs) == 0 {
		return nil
	}

	// First, try to parse as new result file format
	if IsResultFileFormat(blobData) {
		resultFile, err := ParseResultFile(blobData)
		if err == nil {
			// Convert field mappings slice from source node IDs
			fieldMappings := make([]message.FieldMapping, 0, len(sourceNodeIDs))
			for nodeID := range sourceNodeIDs {
				fieldMappings = append(fieldMappings, message.FieldMapping{SourceNodeID: nodeID})
			}
			return sourceResultsFromResultFile(resultFile, fieldMappings)
		}
	}

	// Fall back to legacy nodeExecutionPayload format
	var payload nodeExecutionPayload
	if err := json.Unmarshal(blobData, &payload); err != nil {
		return nil
	}

	if len(payload.ProjectedFields) == 0 {
		return nil
	}

	// Build source results map for each source node ID that exists in projected_fields
	result := make(map[string]*SourceResult)
	for sourceNodeID := range sourceNodeIDs {
		// Check if this source node's projected fields exist
		if nodeFields, exists := payload.ProjectedFields[sourceNodeID]; exists && nodeFields != nil {
			result[sourceNodeID] = &SourceResult{
				NodeID:            sourceNodeID,
				Status:            payload.Status,
				ProjectedFields:   map[string]map[string]interface{}{sourceNodeID: nodeFields},
				IterationMetadata: convertRawIterationMetadata(payload.IterationMetadata),
			}
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

func convertRawIterationMetadata(input map[string]*rawIterationContext) map[string]*IterationContext {
	if len(input) == 0 {
		return nil
	}
	result := make(map[string]*IterationContext, len(input))
	for nodeID, ctx := range input {
		if ctx == nil {
			continue
		}
		result[nodeID] = &IterationContext{
			IsArray:     ctx.IsArray,
			ArrayPath:   ctx.ArrayPath,
			ArrayLength: ctx.ArrayLength,
		}
	}
	return result
}

type nodeExecutionPayload struct {
	NodeID            string                            `json:"node_id"`
	Status            string                            `json:"status"`
	ProjectedFields   map[string]map[string]interface{} `json:"projected_fields"`
	IterationMetadata map[string]*rawIterationContext   `json:"iteration_metadata"`
}

type rawIterationContext struct {
	IsArray     bool   `json:"is_array"`
	ArrayPath   string `json:"array_path"`
	ArrayLength int    `json:"array_length"`
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

// AppendNodeResult adds a node's result to the shared result file for a workflow
// Returns the blob URL and whether it used blob storage
func (s *Service) AppendNodeResult(
	ctx context.Context,
	workflowID string,
	runID string,
	nodeID string,
	result *storage.NodeResult,
) (string, bool, error) {
	if s.resultFileClient == nil {
		return "", false, fmt.Errorf("resolver: result file client not initialized")
	}

	blobURL, err := s.resultFileClient.AppendNodeResult(ctx, workflowID, runID, nodeID, result)
	if err != nil {
		return "", false, fmt.Errorf("resolver: failed to append node result: %w", err)
	}

	return blobURL, true, nil
}

// GetResultFile retrieves the entire result file for a workflow
func (s *Service) GetResultFile(
	ctx context.Context,
	workflowID string,
	runID string,
) (storage.ResultFile, error) {
	if s.resultFileClient == nil {
		return nil, fmt.Errorf("resolver: result file client not initialized")
	}

	return s.resultFileClient.GetResultFile(ctx, workflowID, runID)
}

// GetNodeResultFromFile retrieves a specific node's result from the result file
func (s *Service) GetNodeResultFromFile(
	ctx context.Context,
	workflowID string,
	runID string,
	nodeID string,
) (*storage.NodeResult, error) {
	if s.resultFileClient == nil {
		return nil, fmt.Errorf("resolver: result file client not initialized")
	}

	return s.resultFileClient.GetNodeResult(ctx, workflowID, runID, nodeID)
}

// ResolveInputFromResultFile resolves input data by downloading the result file
// and extracting source node data based on field mappings
func (s *Service) ResolveInputFromResultFile(
	ctx context.Context,
	workflowID string,
	runID string,
	params *FieldMappingParams,
) ([]byte, error) {
	if s.resultFileClient == nil {
		return nil, fmt.Errorf("resolver: result file client not initialized")
	}

	// Download the result file
	resultFile, err := s.resultFileClient.GetResultFile(ctx, workflowID, runID)
	if err != nil {
		return nil, fmt.Errorf("resolver: failed to download result file: %w", err)
	}

	// Convert result file to source results for field mapping
	sourceResults := sourceResultsFromResultFile(resultFile, params.FieldMappings)

	// Build input using field mappings
	buildParams := BuildInputParams{
		UnitNodeID:    params.BlobSourceNodeID,
		FieldMappings: params.FieldMappings,
		SourceResults: sourceResults,
		TriggerData:   params.TriggerData,
	}

	return buildInputFromMappings(buildParams)
}

// sourceResultsFromResultFile converts the new result file format to SourceResult format
// for compatibility with existing field mapping logic
func sourceResultsFromResultFile(resultFile storage.ResultFile, fieldMappings []message.FieldMapping) map[string]*SourceResult {
	if len(resultFile) == 0 {
		return nil
	}

	// Collect all source node IDs from field mappings
	sourceNodeIDs := make(map[string]bool)
	for _, mapping := range fieldMappings {
		if mapping.SourceNodeID != "" {
			sourceNodeIDs[mapping.SourceNodeID] = true
		}
	}

	result := make(map[string]*SourceResult)
	for nodeID := range sourceNodeIDs {
		nodeResult, exists := resultFile[nodeID]
		if !exists || nodeResult == nil {
			continue
		}

		// Extract the actual result data and wrap it in projected_fields format
		// The result data is in nodeResult.Result field
		// We spread the result content at root level so field mappings can access directly:
		// - /data//hire_date finds "data" at root (not "result/data")
		// - /_meta/status finds metadata
		// - /_events/success finds event flags
		projectedFields := make(map[string]map[string]interface{})

		// Convert the result to map format
		resultData := nodeResult.Result
		if resultData != nil {
			// Create fields map - this is what field mappings will extract from
			// Field mappings like /data//hire_date expect the result content at root level
			fields := make(map[string]interface{})

			// If result is already a map, spread it at the root level
			// This way /data//hire_date will find "data" directly (if result contains {"data": [...]})
			if resultMap, ok := resultData.(map[string]interface{}); ok {
				for k, v := range resultMap {
					fields[k] = v
				}
			} else {
				// For non-map results (arrays, primitives), store as-is
				// The field mapping will need to reference it appropriately
				fields["_raw"] = resultData
			}

			// Also include metadata for direct access if needed via _meta, _events, _error paths
			fields["_meta"] = nodeResult.Meta
			fields["_events"] = nodeResult.Events
			if nodeResult.Error != nil {
				fields["_error"] = nodeResult.Error
			}

			projectedFields[nodeID] = fields
		}

		result[nodeID] = &SourceResult{
			NodeID:          nodeID,
			Status:          nodeResult.Meta.Status,
			ProjectedFields: projectedFields,
		}
	}

	return result
}

// IsResultFileFormat checks if the given data is in the new result file format
// (map of node_id -> NodeResult)
func IsResultFileFormat(data []byte) bool {
	if len(data) == 0 {
		return false
	}

	var resultFile storage.ResultFile
	if err := json.Unmarshal(data, &resultFile); err != nil {
		return false
	}

	// Check if any entry has the expected _meta structure
	for _, nodeResult := range resultFile {
		if nodeResult != nil && nodeResult.Meta.NodeID != "" {
			return true
		}
	}

	return false
}

// ParseResultFile parses blob data as the new result file format
func ParseResultFile(data []byte) (storage.ResultFile, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	var resultFile storage.ResultFile
	if err := json.Unmarshal(data, &resultFile); err != nil {
		return nil, fmt.Errorf("failed to parse result file: %w", err)
	}

	return resultFile, nil
}
