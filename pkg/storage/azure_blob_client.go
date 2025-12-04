package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"go.uber.org/zap"
)

// BlobStorageClient interface for storing large workflow results
type BlobStorageClient interface {
	UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
	DownloadResult(ctx context.Context, blobURL string) ([]byte, error)
}

// AzureBlobClient implements BlobStorageClient for Azure Blob Storage using shared keys
// This implementation is intentionally close to the lightweight blob client used by the
// plugin backend so we can seamlessly target local Azurite instances over HTTP.
type AzureBlobClient struct {
	client        *azblob.Client
	serviceURL    string
	containerName string
	credential    *azblob.SharedKeyCredential
	logger        *zap.Logger
	containerInit bool
}

// NewAzureBlobClient creates a new Azure Blob storage client from a standard connection string.
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

	params := parseConnectionString(connectionString)
	accountName := params["AccountName"]
	accountKey := params["AccountKey"]
	serviceURL := params["BlobEndpoint"]
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("account name and key are required in the connection string")
	}
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
		return nil, fmt.Errorf("failed to create shared key credential: %w", err)
		}

	var clientOpts *azblob.ClientOptions
	if strings.HasPrefix(strings.ToLower(serviceURL), "http://") {
		clientOpts = &azblob.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				InsecureAllowCredentialWithHTTP: true,
			},
	}
	}

	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, credential, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob client: %w", err)
	}

	return &AzureBlobClient{
		client:        client,
		serviceURL:    strings.TrimRight(serviceURL, "/"),
		containerName: containerName,
		credential:    credential,
		logger:        logger,
	}, nil
}

// UploadResult uploads data to the configured container.
func (a *AzureBlobClient) UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error) {
	if err := a.ensureContainer(ctx); err != nil {
		return "", err
	}

	metadataPtr := make(map[string]*string, len(metadata))
	for k, v := range metadata {
		metadataPtr[k] = to.Ptr(v)
	}

		containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
		blobClient := containerClient.NewBlockBlobClient(blobPath)

	_, err := blobClient.UploadBuffer(ctx, data, &azblob.UploadBufferOptions{
			Metadata: metadataPtr,
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentType: to.Ptr("application/json"),
			},
	})
		if err != nil {
			a.logger.Error("Failed to upload to blob storage",
				zap.String("blob_path", blobPath),
				zap.Int("size", len(data)),
				zap.Error(err))
			return "", fmt.Errorf("blob upload failed: %w", err)
		}

	a.logger.Info("Successfully uploaded blob",
		zap.String("blob_path", blobPath),
		zap.Int("size_bytes", len(data)))

	return blobClient.URL(), nil
}

// DownloadResult downloads blob contents using the shared-key client.
func (a *AzureBlobClient) DownloadResult(ctx context.Context, reference string) ([]byte, error) {
	blobPath, err := a.extractBlobPath(reference)
		if err != nil {
		return nil, err
	}

	containerClient := a.client.ServiceClient().NewContainerClient(a.containerName)
	blobClient := containerClient.NewBlobClient(blobPath)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to download blob: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read blob data: %w", err)
	}

	return data, nil
}

func (a *AzureBlobClient) ensureContainer(ctx context.Context) error {
	if a.containerInit {
		return nil
	}

	_, err := a.client.CreateContainer(ctx, a.containerName, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if strings.Contains(strings.ToLower(err.Error()), "containeralreadyexists") {
			a.containerInit = true
			return nil
		}
		if errors.As(err, &respErr) {
			if respErr.ErrorCode == "ContainerAlreadyExists" {
				a.containerInit = true
				return nil
			}
		}
		return fmt.Errorf("failed to ensure container: %w", err)
	}

	a.containerInit = true
	return nil
}

func parseConnectionString(connectionString string) map[string]string {
	parts := strings.Split(connectionString, ";")
	params := make(map[string]string, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.Index(part, "=")
		if idx <= 0 {
			continue
		}
		key := part[:idx]
		value := part[idx+1:]
		params[key] = value
	}
	return params
}

func (a *AzureBlobClient) extractBlobPath(reference string) (string, error) {
	ref := strings.TrimSpace(reference)
	if ref == "" {
		return "", fmt.Errorf("blob reference is required")
	}

	lowerSvc := strings.ToLower(a.serviceURL)
	lowerRef := strings.ToLower(ref)
	if strings.HasPrefix(lowerRef, lowerSvc) {
		ref = ref[len(a.serviceURL):]
	}

	if idx := strings.Index(ref, "?"); idx != -1 {
		ref = ref[:idx]
	}

	ref = strings.TrimSpace(ref)
	decodedRef, err := url.PathUnescape(ref)
	if err == nil && decodedRef != "" {
		ref = decodedRef
	}

	u, err := url.Parse(ref)
	if err == nil && u.Host != "" {
		ref = u.Path
	}

	ref = strings.TrimPrefix(ref, "/")
	ref = strings.TrimPrefix(ref, a.containerName+"/")

	if ref == "" {
		return "", fmt.Errorf("blob path is empty")
	}

	return ref, nil
}
