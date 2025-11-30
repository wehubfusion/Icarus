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
func (s *Service) ResolveMappedInput(
	ctx context.Context,
	inline []byte,
	blobRef *message.BlobReference,
	params *FieldMappingParams,
) ([]byte, error) {
	var base []byte
	var err error

	if len(inline) > 0 || (blobRef != nil && blobRef.URL != "") {
		base, err = s.ResolveInput(ctx, inline, blobRef)
		if err != nil {
			return nil, err
		}
	}

	return buildInputFromFieldMappings(base, params)
}

// buildInputFromFieldMappings centralizes the logic for constructing unit inputs using field mappings.
func buildInputFromFieldMappings(
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

// sourceResultsFromBlob attempts to reconstruct SourceResults by parsing the blob payload as a node execution result.
// It extracts source results for the specified source node IDs from the blob's projected_fields.
func sourceResultsFromBlob(blobData []byte, sourceNodeIDs map[string]bool) map[string]*SourceResult {
	if len(blobData) == 0 || len(sourceNodeIDs) == 0 {
		return nil
	}

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
