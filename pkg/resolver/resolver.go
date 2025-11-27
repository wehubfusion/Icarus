package resolver

import (
	"context"
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
