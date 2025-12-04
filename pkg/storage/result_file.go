package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// NodeResultMeta contains metadata about a node execution
type NodeResultMeta struct {
	Status          string `json:"status"`            // "success" or "failed"
	NodeID          string `json:"node_id"`           // Node identifier
	PluginType      string `json:"plugin_type"`       // Plugin type that processed the node
	ExecutionTimeMs int64  `json:"execution_time_ms"` // Execution duration in milliseconds
}

// NodeResultEvents contains event endpoint flags
type NodeResultEvents struct {
	Success *bool `json:"success,omitempty"` // true if success event triggered, nil otherwise
	Error   *bool `json:"error,omitempty"`   // true if error event triggered, nil otherwise
}

// NodeResultError contains error information when a node fails
type NodeResultError struct {
	Code      string `json:"code"`      // Error code
	Message   string `json:"message"`   // Human-readable error message
	Retryable bool   `json:"retryable"` // Whether the error is retryable
}

// NodeResult represents the standardized result format for a node execution
type NodeResult struct {
	Meta   NodeResultMeta   `json:"_meta"`
	Events NodeResultEvents `json:"_events"`
	Error  *NodeResultError `json:"_error,omitempty"`
	Result interface{}      `json:"result"` // Actual output from the node
}

// ResultFile represents the shared result file containing all node results
// Format: { "<node_id>": NodeResult, "<node_id>": NodeResult, ... }
type ResultFile map[string]*NodeResult

// ResultFileMetadata contains metadata about the result file
type ResultFileMetadata struct {
	WorkflowID   string    `json:"workflow_id"`
	RunID        string    `json:"run_id"`
	LastModified time.Time `json:"last_modified"`
	NodeCount    int       `json:"node_count"`
}

// ResultFileClient provides operations for managing the shared result file
type ResultFileClient struct {
	blobClient BlobStorageClient
	logger     *zap.Logger
	mu         sync.Mutex // Protects concurrent access to the result file
}

// NewResultFileClient creates a new result file client
func NewResultFileClient(blobClient BlobStorageClient, logger *zap.Logger) *ResultFileClient {
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	return &ResultFileClient{
		blobClient: blobClient,
		logger:     logger,
	}
}

// ResultFilePath returns the standard blob path for a workflow's result file
func ResultFilePath(workflowID, runID string) string {
	return fmt.Sprintf("results/%s/%s/results.json", workflowID, runID)
}

// AppendNodeResult adds or updates a node's result in the shared result file
// This operation is atomic - it reads the current file, adds the result, and writes back
func (c *ResultFileClient) AppendNodeResult(
	ctx context.Context,
	workflowID string,
	runID string,
	nodeID string,
	result *NodeResult,
) (string, error) {
	if c.blobClient == nil {
		return "", fmt.Errorf("blob client not initialized")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	blobPath := ResultFilePath(workflowID, runID)

	c.logger.Debug("Appending node result to result file",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("node_id", nodeID),
		zap.String("blob_path", blobPath))

	// Try to download existing result file
	var resultFile ResultFile
	existingData, err := c.blobClient.DownloadResult(ctx, blobPath)
	if err != nil {
		// File doesn't exist yet - create new
		c.logger.Debug("Result file doesn't exist yet, creating new",
			zap.String("blob_path", blobPath))
		resultFile = make(ResultFile)
	} else {
		// Parse existing file
		if err := json.Unmarshal(existingData, &resultFile); err != nil {
			c.logger.Error("Failed to parse existing result file, starting fresh",
				zap.String("blob_path", blobPath),
				zap.Error(err))
			resultFile = make(ResultFile)
		}
	}

	// Add/update the node result
	resultFile[nodeID] = result

	// Serialize updated file
	updatedData, err := json.Marshal(resultFile)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result file: %w", err)
	}

	// Upload updated file
	blobURL, err := c.blobClient.UploadResult(ctx, blobPath, updatedData, map[string]string{
		"workflow_id":   workflowID,
		"run_id":        runID,
		"last_node_id":  nodeID,
		"node_count":    fmt.Sprintf("%d", len(resultFile)),
		"last_modified": time.Now().Format(time.RFC3339),
	})
	if err != nil {
		return "", fmt.Errorf("failed to upload result file: %w", err)
	}

	c.logger.Info("Successfully appended node result to result file",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("node_id", nodeID),
		zap.Int("total_nodes", len(resultFile)),
		zap.Int("result_size_bytes", len(updatedData)))

	return blobURL, nil
}

// GetResultFile downloads and parses the entire result file
func (c *ResultFileClient) GetResultFile(
	ctx context.Context,
	workflowID string,
	runID string,
) (ResultFile, error) {
	if c.blobClient == nil {
		return nil, fmt.Errorf("blob client not initialized")
	}

	blobPath := ResultFilePath(workflowID, runID)

	data, err := c.blobClient.DownloadResult(ctx, blobPath)
	if err != nil {
		return nil, fmt.Errorf("failed to download result file: %w", err)
	}

	var resultFile ResultFile
	if err := json.Unmarshal(data, &resultFile); err != nil {
		return nil, fmt.Errorf("failed to parse result file: %w", err)
	}

	return resultFile, nil
}

// GetResultFileRaw downloads the result file and returns the raw bytes
// This is useful when the caller needs to parse or process the data themselves
func (c *ResultFileClient) GetResultFileRaw(
	ctx context.Context,
	blobPath string,
) ([]byte, error) {
	if c.blobClient == nil {
		return nil, fmt.Errorf("blob client not initialized")
	}

	data, err := c.blobClient.DownloadResult(ctx, blobPath)
	if err != nil {
		return nil, fmt.Errorf("failed to download result file: %w", err)
	}

	return data, nil
}

// GetNodeResult retrieves a specific node's result from the result file
func (c *ResultFileClient) GetNodeResult(
	ctx context.Context,
	workflowID string,
	runID string,
	nodeID string,
) (*NodeResult, error) {
	resultFile, err := c.GetResultFile(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}

	result, exists := resultFile[nodeID]
	if !exists {
		return nil, fmt.Errorf("node result not found: %s", nodeID)
	}

	return result, nil
}

// GetResultFileSize returns the size of the result file in bytes
// Returns 0 if the file doesn't exist
func (c *ResultFileClient) GetResultFileSize(
	ctx context.Context,
	workflowID string,
	runID string,
) (int, error) {
	if c.blobClient == nil {
		return 0, fmt.Errorf("blob client not initialized")
	}

	blobPath := ResultFilePath(workflowID, runID)

	data, err := c.blobClient.DownloadResult(ctx, blobPath)
	if err != nil {
		// File doesn't exist - return 0
		return 0, nil
	}

	return len(data), nil
}

// CreateNodeResult is a helper to build a NodeResult with standard fields
func CreateNodeResult(
	nodeID string,
	pluginType string,
	status string,
	executionTimeMs int64,
	result interface{},
	errorInfo *NodeResultError,
) *NodeResult {
	nodeResult := &NodeResult{
		Meta: NodeResultMeta{
			Status:          status,
			NodeID:          nodeID,
			PluginType:      pluginType,
			ExecutionTimeMs: executionTimeMs,
		},
		Events: NodeResultEvents{},
		Result: result,
	}

	// Set events based on status
	if status == "success" {
		success := true
		nodeResult.Events.Success = &success
	} else if status == "failed" {
		errorFlag := true
		nodeResult.Events.Error = &errorFlag
		nodeResult.Error = errorInfo
	}

	return nodeResult
}

// ExtractResultData extracts the actual result data from a NodeResult
// This is used by field mapping to get the data for downstream nodes
func ExtractResultData(nodeResult *NodeResult) interface{} {
	if nodeResult == nil {
		return nil
	}
	return nodeResult.Result
}
