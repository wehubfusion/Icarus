package callback

import (
	"context"
	"fmt"
	"time"

	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.uber.org/zap"
)

// ResultType represents different types of results that can be published
type ResultType string

const (
	ResultTypeSuccess ResultType = "success"
	ResultTypeError   ResultType = "error"
	ResultTypeWarning ResultType = "warning"
	ResultTypeInfo    ResultType = "info"
)

// Config holds configuration for the callback handler
type Config struct {
	Subject       string        // Subject to publish results to (default: "result")
	MaxRetries    int           // Maximum number of retry attempts (default: 3)
	RetryDelay    time.Duration // Delay between retries (default: 1s)
	EnableLogging bool          // Enable logging of operations (default: true)
	Logger        *zap.Logger   // Custom logger instance (optional, uses default if nil)
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	logger, _ := zap.NewProduction()
	return &Config{
		Subject:       "result",
		MaxRetries:    3,
		RetryDelay:    time.Second,
		EnableLogging: true,
		Logger:        logger,
	}
}

// CallbackHandler handles publishing messages to the result subject.
// It uses the client's message service for publishing with enhanced features.
type CallbackHandler struct {
	client *client.Client
	config *Config
	logger *zap.Logger
}

// NewCallbackHandler creates a new callback handler using the client's message service with default config.
func NewCallbackHandler(c *client.Client) *CallbackHandler {
	return NewCallbackHandlerWithConfig(c, DefaultConfig())
}

// NewCallbackHandlerWithConfig creates a new callback handler with custom configuration.
// You can pass your own zap logger via config.Logger to integrate with your plugin's logging.
func NewCallbackHandlerWithConfig(c *client.Client, config *Config) *CallbackHandler {
	logger := config.Logger
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	return &CallbackHandler{
		client: c,
		config: config,
		logger: logger,
	}
}

// validateMessage performs strict validation on the message
func (c *CallbackHandler) validateMessage(msg *message.Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	if msg.CreatedAt == "" {
		return fmt.Errorf("message CreatedAt is required")
	}

	if msg.UpdatedAt == "" {
		return fmt.Errorf("message UpdatedAt is required")
	}

	if msg.Workflow == nil {
		return fmt.Errorf("message Workflow is required")
	}

	// Node is optional when Workflow is present (for workflow-level callbacks)
	// But if Node is present, Workflow must also be present
	if msg.Node != nil && msg.Workflow == nil {
		return fmt.Errorf("message Workflow is required when Node is present")
	}

	if msg.Payload == nil {
		return fmt.Errorf("message Payload is required")
	}

	return nil
}

// logOperation logs the operation if logging is enabled
func (c *CallbackHandler) logOperation(operation string, msg *message.Message, err error) {
	if !c.config.EnableLogging {
		return
	}

	fields := []zap.Field{
		zap.String("operation", operation),
		zap.String("subject", c.config.Subject),
	}

	var msgID string
	if msg != nil {
		if msg.Workflow != nil {
			msgID = fmt.Sprintf("workflow:%s/run:%s", msg.Workflow.WorkflowID, msg.Workflow.RunID)
			fields = append(fields,
				zap.String("workflow_id", msg.Workflow.WorkflowID),
				zap.String("run_id", msg.Workflow.RunID),
			)
		} else if msg.Node != nil {
			msgID = fmt.Sprintf("node:%s", msg.Node.NodeID)
			fields = append(fields, zap.String("node_id", msg.Node.NodeID))
		} else {
			msgID = "unknown"
		}
	}
	fields = append(fields, zap.String("message_id", msgID))

	if err != nil {
		fields = append(fields, zap.Error(err))
		c.logger.Error(fmt.Sprintf("Failed to %s message", operation), fields...)
	} else {
		c.logger.Info(fmt.Sprintf("Successfully %s message", operation), fields...)
	}
}

// publishWithRetry attempts to publish a message with retry logic
func (c *CallbackHandler) publishWithRetry(ctx context.Context, subject string, msg *message.Message) error {
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			if c.config.EnableLogging {
				c.logger.Info("Retrying publish",
					zap.Int("attempt", attempt),
					zap.Int("max_attempts", c.config.MaxRetries+1),
					zap.String("subject", subject),
					zap.Duration("retry_delay", c.config.RetryDelay),
				)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("publish cancelled during retry: %w", ctx.Err())
			case <-time.After(c.config.RetryDelay):
				// Continue with retry
			}
		}

		err := c.client.Messages.Publish(ctx, subject, msg)
		if err == nil {
			return nil // Success
		}

		lastErr = err
		if c.config.EnableLogging {
			c.logger.Warn("Publish attempt failed",
				zap.Int("attempt", attempt+1),
				zap.Int("max_attempts", c.config.MaxRetries+1),
				zap.String("subject", subject),
				zap.Error(err),
			)
		}
	}

	return fmt.Errorf("publish failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// Callback publishes a message to the configured result subject using the client's message service.
// It includes validation, logging, and retry logic based on configuration.
// Returns an error if the publish operation fails after all retry attempts.
func (c *CallbackHandler) Callback(ctx context.Context, msg *message.Message) error {
	// Validate message
	if err := c.validateMessage(msg); err != nil {
		c.logOperation("validate", msg, err)
		return fmt.Errorf("validation failed: %w", err)
	}

	// Attempt to publish with retries
	err := c.publishWithRetry(ctx, c.config.Subject, msg)
	if err != nil {
		c.logOperation("publish", msg, err)
		return err
	}

	c.logOperation("publish", msg, nil)
	return nil
}

// ReportSuccess publishes a success result with the given message
func (c *CallbackHandler) ReportSuccess(ctx context.Context, workflowID, runID string, data string) error {
	msg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", data, fmt.Sprintf("success-%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeSuccess)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID))

	return c.Callback(ctx, msg)
}

// ReportError publishes an error result with the given message
func (c *CallbackHandler) ReportError(ctx context.Context, workflowID, runID string, errorMsg string) error {
	msg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", errorMsg, fmt.Sprintf("error-%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeError)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID))

	return c.Callback(ctx, msg)
}

// ReportWarning publishes a warning result with the given message
func (c *CallbackHandler) ReportWarning(ctx context.Context, workflowID, runID string, warningMsg string) error {
	msg := message.NewWorkflowMessage(workflowID, runID).
		WithPayload("callback-service", warningMsg, fmt.Sprintf("warning-%s-%s", workflowID, runID)).
		WithMetadata("result_type", string(ResultTypeWarning)).
		WithMetadata("correlation_id", fmt.Sprintf("%s-%s", workflowID, runID))

	return c.Callback(ctx, msg)
}

// GetConfig returns the current configuration (read-only)
func (c *CallbackHandler) GetConfig() *Config {
	return c.config
}

// Close gracefully closes the logger and cleans up resources.
// This should be called when the handler is no longer needed.
func (c *CallbackHandler) Close() error {
	if c.logger != nil {
		return c.logger.Sync()
	}
	return nil
}
