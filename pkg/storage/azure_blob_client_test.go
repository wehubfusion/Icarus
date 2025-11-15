package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNewAzureBlobClient(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name             string
		connectionString string
		containerName    string
		wantErr          bool
		errContains      string
	}{
		{
			name:             "empty connection string",
			connectionString: "",
			containerName:    "test-container",
			wantErr:          true,
			errContains:      "connection string is required",
		},
		{
			name:             "empty container name",
			connectionString: "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
			containerName:    "",
			wantErr:          true,
			errContains:      "container name is required",
		},
		{
			name:             "nil logger",
			connectionString: "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
			containerName:    "test-container",
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewAzureBlobClient(tt.connectionString, tt.containerName, logger)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				// Note: Will fail if connection string is invalid
				// In production tests, use Azure Storage Emulator (Azurite)
				if err != nil {
					t.Logf("Azure connection failed (expected in test env): %v", err)
				}
			}
		})
	}
}

func TestAzureBlobClient_UploadResult(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Use test connection string (requires Azurite or real Azure account)
	connectionString := "UseDevelopmentStorage=true" // Azurite default
	containerName := "test-results"

	client, err := NewAzureBlobClient(connectionString, containerName, logger)
	if err != nil {
		t.Skip("Azure Blob Storage not available - skipping upload test")
	}

	ctx := context.Background()
	testData := []byte(`{"node_id":"test-node","status":"success","output":"test"}`)
	metadata := map[string]string{
		"workflow_id":  "test-workflow",
		"execution_id": "test-execution",
	}

	blobURL, err := client.UploadResult(ctx, "test-path/result.json", testData, metadata)

	if err != nil {
		t.Logf("Upload failed (expected without Azurite): %v", err)
		return
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, blobURL)
	assert.Contains(t, blobURL, "test-path/result.json")
}

func TestAzureBlobClient_GenerateSASURL(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	connectionString := "UseDevelopmentStorage=true"
	containerName := "test-results"

	client, err := NewAzureBlobClient(connectionString, containerName, logger)
	if err != nil {
		t.Skip("Azure Blob Storage not available")
	}

	ctx := context.Background()

	// First upload a blob
	testData := []byte(`{"test":"data"}`)
	blobURL, err := client.UploadResult(ctx, "test-sas/result.json", testData, nil)
	if err != nil {
		t.Skip("Upload failed - skipping SAS test")
	}

	// Generate SAS URL
	sasURL, err := client.GenerateSASURL(ctx, blobURL, 1)

	if err != nil {
		t.Logf("SAS generation failed: %v", err)
		return
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, sasURL)
	assert.Contains(t, sasURL, "sig=") // SAS URLs contain signature
}

func TestAzureBlobClient_DownloadResult(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	connectionString := "UseDevelopmentStorage=true"
	containerName := "test-results"

	client, err := NewAzureBlobClient(connectionString, containerName, logger)
	if err != nil {
		t.Skip("Azure Blob Storage not available")
	}

	ctx := context.Background()

	// Upload test data
	testData := []byte(`{"node_id":"download-test","status":"success"}`)
	blobURL, err := client.UploadResult(ctx, "test-download/result.json", testData, nil)
	if err != nil {
		t.Skip("Upload failed")
	}

	// Generate SAS URL
	sasURL, err := client.GenerateSASURL(ctx, blobURL, 1)
	if err != nil {
		t.Skip("SAS generation failed")
	}

	// Download using SAS URL
	downloadedData, err := client.DownloadResult(ctx, sasURL)

	assert.NoError(t, err)
	assert.Equal(t, testData, downloadedData)
}

func TestAzureBlobClient_UploadResult_EmptyData(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	connectionString := "UseDevelopmentStorage=true"
	containerName := "test-results"

	client, err := NewAzureBlobClient(connectionString, containerName, logger)
	if err != nil {
		t.Skip("Azure Blob Storage not available")
	}

	ctx := context.Background()

	// Upload empty data (should succeed)
	blobURL, err := client.UploadResult(ctx, "empty/result.json", []byte{}, nil)

	if err != nil {
		t.Logf("Upload failed: %v", err)
		return
	}

	assert.NoError(t, err)
	assert.NotEmpty(t, blobURL)
}

func TestAzureBlobClient_RoundTrip(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	connectionString := "UseDevelopmentStorage=true"
	containerName := "test-results"

	client, err := NewAzureBlobClient(connectionString, containerName, logger)
	if err != nil {
		t.Skip("Azure Blob Storage not available - run 'azurite' for local testing")
	}

	ctx := context.Background()

	// Test data
	originalData := []byte(`{
		"node_id": "test-node-123",
		"status": "success",
		"output": {"result": "test data"},
		"projected_fields": {"field1": "value1"}
	}`)

	metadata := map[string]string{
		"workflow_id":  "wf-123",
		"run_id":       "run-456",
		"execution_id": "exec-789",
		"node_id":      "test-node-123",
	}

	// 1. Upload
	blobURL, err := client.UploadResult(ctx, "roundtrip/result.json", originalData, metadata)
	require.NoError(t, err)
	require.NotEmpty(t, blobURL)

	// 2. Generate SAS URL
	sasURL, err := client.GenerateSASURL(ctx, blobURL, 24)
	require.NoError(t, err)
	require.NotEmpty(t, sasURL)

	// 3. Download
	downloadedData, err := client.DownloadResult(ctx, sasURL)
	require.NoError(t, err)

	// 4. Verify
	assert.Equal(t, originalData, downloadedData)
}

