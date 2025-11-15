package client

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
)

// TemporalClient wraps Temporal client for signaling Zeus workflows
type TemporalClient struct {
	client client.Client
	logger *zap.Logger
}

// NewTemporalClient creates a new Temporal client for signaling
func NewTemporalClient(hostPort string, namespace string, logger *zap.Logger) (*TemporalClient, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if hostPort == "" {
		return nil, fmt.Errorf("hostPort is required")
	}

	if namespace == "" {
		namespace = "default"
	}

	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
	if err != nil {
		logger.Error("Failed to create Temporal client for signaling",
			zap.String("host_port", hostPort),
			zap.String("namespace", namespace),
			zap.Error(err))
		return nil, fmt.Errorf("failed to dial Temporal: %w", err)
	}

	logger.Info("Created Temporal signaling client",
		zap.String("host_port", hostPort),
		zap.String("namespace", namespace))

	return &TemporalClient{
		client: c,
		logger: logger,
	}, nil
}

// SignalWorkflow sends a signal to a Temporal workflow
func (t *TemporalClient) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, data interface{}) error {
	err := t.client.SignalWorkflow(ctx, workflowID, runID, signalName, data)
	if err != nil {
		t.logger.Error("Failed to signal workflow",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("signal_name", signalName),
			zap.Error(err))
		return fmt.Errorf("failed to signal workflow: %w", err)
	}

	t.logger.Debug("Successfully signaled workflow",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("signal_name", signalName))

	return nil
}

// Close closes the Temporal client
func (t *TemporalClient) Close() {
	if t.client != nil {
		t.client.Close()
		t.logger.Info("Closed Temporal signaling client")
	}
}

