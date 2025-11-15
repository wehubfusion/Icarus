package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"go.uber.org/zap"
)

// BlobStorageClient interface for storing large workflow results
type BlobStorageClient interface {
	UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
	GenerateSASURL(ctx context.Context, blobURL string, expiryHours int) (string, error)
	DownloadResult(ctx context.Context, sasURL string) ([]byte, error)
}

// AzureBlobClient implements BlobStorageClient for Azure Blob Storage
type AzureBlobClient struct {
	client        *azblob.Client
	containerName string
	logger        *zap.Logger
}

// NewAzureBlobClient creates a new Azure Blob storage client
func NewAzureBlobClient(connectionString, containerName string, logger *zap.Logger) (*AzureBlobClient, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if connectionString == "" {
		return nil, fmt.Errorf("connection string is required")
	}

	if containerName == "" {
		return nil, fmt.Errorf("container name is required")
	}

	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		logger.Error("Failed to create Azure Blob client",
			zap.String("container", containerName),
			zap.Error(err))
		return nil, fmt.Errorf("failed to create blob client: %w", err)
	}

	logger.Info("Created Azure Blob storage client",
		zap.String("container", containerName))

	return &AzureBlobClient{
		client:        client,
		containerName: containerName,
		logger:        logger,
	}, nil
}

// UploadResult uploads a result to Azure Blob Storage
func (a *AzureBlobClient) UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error) {
	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	blobClient := containerClient.NewBlockBlobClient(blobPath)

	a.logger.Debug("Uploading result to Azure Blob",
		zap.String("blob_path", blobPath),
		zap.Int("size_bytes", len(data)))

	// Convert metadata to *string map
	metadataPtr := make(map[string]*string)
	for k, v := range metadata {
		metadataPtr[k] = to.Ptr(v)
	}

	// Upload with metadata and content type
	uploadOptions := &azblob.UploadBufferOptions{
		Metadata: metadataPtr,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/json"),
		},
	}

	_, err := blobClient.UploadBuffer(ctx, data, uploadOptions)
	if err != nil {
		a.logger.Error("Failed to upload to blob storage",
			zap.String("blob_path", blobPath),
			zap.Int("size", len(data)),
			zap.Error(err))
		return "", fmt.Errorf("blob upload failed: %w", err)
	}

	blobURL := blobClient.URL()

	a.logger.Info("Successfully uploaded result to blob storage",
		zap.String("blob_path", blobPath),
		zap.String("blob_url", blobURL),
		zap.Int("size_bytes", len(data)))

	return blobURL, nil
}

// GenerateSASURL creates a time-limited SAS URL for blob access
func (a *AzureBlobClient) GenerateSASURL(ctx context.Context, blobURL string, expiryHours int) (string, error) {
	a.logger.Debug("Generating SAS URL",
		zap.String("blob_url", blobURL),
		zap.Int("expiry_hours", expiryHours))

	// Parse the blob URL to get container and blob name
	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)

	// Extract blob name from URL (last part after container name)
	// Format: https://{account}.blob.core.windows.net/{container}/{blobPath}
	blobName := blobURL[len(containerClient.URL())+1:] // +1 for trailing slash

	blobClient := containerClient.NewBlobClient(blobName)

	// Set expiry time
	expiryTime := time.Now().Add(time.Duration(expiryHours) * time.Hour)

	// Create SAS permissions (read only)
	permissions := sas.BlobPermissions{
		Read: true,
	}

	// Generate SAS URL
	sasURL, err := blobClient.GetSASURL(permissions, expiryTime, nil)
	if err != nil {
		a.logger.Error("Failed to generate SAS URL",
			zap.String("blob_url", blobURL),
			zap.Error(err))
		return "", fmt.Errorf("failed to generate SAS URL: %w", err)
	}

	a.logger.Debug("Generated SAS URL",
		zap.String("blob_url", blobURL),
		zap.Time("expiry", expiryTime))

	return sasURL, nil
}

// DownloadResult downloads a result from Azure Blob using SAS URL
func (a *AzureBlobClient) DownloadResult(ctx context.Context, sasURL string) ([]byte, error) {
	a.logger.Debug("Downloading result from blob",
		zap.String("sas_url_prefix", sasURL[:min(50, len(sasURL))]))

	// Create block blob client from SAS URL (pre-authenticated, no credentials needed)
	blobClient, err := blockblob.NewClientWithNoCredential(sasURL, nil)
	if err != nil {
		a.logger.Error("Failed to create blob client from SAS URL", zap.Error(err))
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	// Download the blob
	downloadResponse, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		a.logger.Error("Failed to download from blob", zap.Error(err))
		return nil, fmt.Errorf("failed to download: %w", err)
	}
	defer downloadResponse.Body.Close()

	// Read all data
	data, err := io.ReadAll(downloadResponse.Body)
	if err != nil {
		a.logger.Error("Failed to read blob data", zap.Error(err))
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	a.logger.Debug("Successfully downloaded result",
		zap.Int("size_bytes", len(data)))

	return data, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

