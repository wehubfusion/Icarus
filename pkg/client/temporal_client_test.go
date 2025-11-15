package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewTemporalClient(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	tests := []struct {
		name      string
		hostPort  string
		namespace string
		wantErr   bool
	}{
		{
			name:      "valid config with default namespace",
			hostPort:  "localhost:7233",
			namespace: "",
			wantErr:   false,
		},
		{
			name:      "valid config with custom namespace",
			hostPort:  "localhost:7233",
			namespace: "custom",
			wantErr:   false,
		},
		{
			name:      "empty hostPort",
			hostPort:  "",
			namespace: "default",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewTemporalClient(tt.hostPort, tt.namespace, logger)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, client)
			} else {
				// Note: This will fail if Temporal is not running
				// In a real test, we'd use mocks
				if err != nil {
					t.Skip("Temporal server not available - skipping connection test")
				}
				assert.NoError(t, err)
				assert.NotNil(t, client)
				if client != nil {
					client.Close()
				}
			}
		})
	}
}

func TestNewTemporalClient_NilLogger(t *testing.T) {
	client, err := NewTemporalClient("localhost:7233", "default", nil)
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "logger is required")
}

func TestTemporalClient_SignalWorkflow(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	// Skip if Temporal is not available
	client, err := NewTemporalClient("localhost:7233", "default", logger)
	if err != nil {
		t.Skip("Temporal server not available - skipping signal test")
	}
	defer client.Close()

	ctx := context.Background()

	// Test signaling a non-existent workflow (should fail gracefully)
	err = client.SignalWorkflow(ctx, "non-existent-workflow", "non-existent-run", "test-signal", map[string]string{"test": "data"})

	// Should return an error (workflow not found)
	assert.Error(t, err)
}

func TestTemporalClient_SignalWorkflow_ContextCancellation(t *testing.T) {
	logger, _ := zap.NewDevelopment()

	client, err := NewTemporalClient("localhost:7233", "default", logger)
	if err != nil {
		t.Skip("Temporal server not available")
	}
	defer client.Close()

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = client.SignalWorkflow(ctx, "workflow-id", "run-id", "signal-name", "data")
	assert.Error(t, err)
}
