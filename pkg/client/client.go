package client

import (
	"context"
	"fmt"

	natsclient "github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/internal/nats"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.uber.org/zap"
)

// Client is the central JetStream client that manages the connection and provides access to services.
// It serves as the entry point for all JetStream operations and automatically initializes
// the JetStream context and service interfaces.
//
// Note: This SDK uses JetStream exclusively for all messaging operations.
// Standard NATS publish/subscribe is not supported.
//
// Example usage:
//
//	client := client.NewClient("nats://localhost:4222")
//	if err := client.Connect(ctx); err != nil {
//	    logger.Fatal("Failed to connect", zap.Error(err))
//	}
//	defer client.Close()
//
//	// Use the Messages service (JetStream-based)
//	msg := message.NewMessage("id-123", "Hello World")
//	client.Messages.Publish(ctx, "events.test", msg)
type Client struct {
	conn   *natsclient.Conn
	js     natsclient.JetStreamContext
	config *nats.ConnectionConfig
	logger *zap.Logger

	// Messages provides access to all JetStream messaging operations including
	// publish, subscribe, request-reply, and pull-based consumers
	Messages *message.MessageService

	// Processes will provide process management functionality (reserved for future use)
	// Processes *process.ProcessService
}

// NewClient creates a new JetStream SDK client with default configuration.
// The URL parameter specifies the NATS server address (e.g., "nats://localhost:4222").
//
// Note: JetStream must be enabled on the NATS server for this SDK to function.
// The client must be connected using Connect() before use.
//
// Example:
//
//	client := client.NewClient("nats://localhost:4222")
func NewClient(url string) *Client {
	logger, _ := zap.NewProduction()
	return &Client{
		config: nats.DefaultConnectionConfig(url),
		logger: logger,
	}
}

// NewClientWithConfig creates a new JetStream SDK client with custom configuration.
// This allows full control over connection parameters such as reconnection settings,
// timeouts, and authentication.
//
// Note: JetStream must be enabled on the NATS server for this SDK to function.
//
// Example:
//
//	config := &nats.ConnectionConfig{
//	    URL:           "nats://localhost:4222",
//	    Name:          "my-service",
//	    MaxReconnects: 10,
//	    ReconnectWait: 2 * time.Second,
//	}
//	client := client.NewClientWithConfig(config)
func NewClientWithConfig(config *nats.ConnectionConfig) *Client {
	logger, _ := zap.NewProduction()
	return &Client{
		config: config,
		logger: logger,
	}
}

// Connect establishes a connection to the NATS server and initializes JetStream context.
// This method must be called before using any service methods.
//
// The method performs the following operations:
//   - Establishes TCP connection to NATS server
//   - Initializes JetStream context (REQUIRED - fails if JetStream is not enabled)
//   - Creates and initializes the MessageService with JetStream
//
// Returns an error if connection fails or if JetStream is not enabled on the server.
//
// Example:
//
//	ctx := context.Background()
//	if err := client.Connect(ctx); err != nil {
//	    client.logger.Fatal("Failed to connect", zap.Error(err))
//	}
func (c *Client) Connect(ctx context.Context) error {
	if c.conn != nil && c.conn.IsConnected() {
		return nil // Already connected
	}

	// Establish NATS connection
	conn, err := nats.Connect(ctx, c.config)
	if err != nil {
		return sdkerrors.NewInternalError("", "failed to connect to NATS", "CONNECTION_FAILED", err)
	}

	c.conn = conn

	// Initialize JetStream context - REQUIRED for this SDK
	js, err := conn.JetStream()
	if err != nil {
		_ = nats.Close(c.conn)
		c.conn = nil
		return sdkerrors.NewInternalError("", "JetStream is not enabled on the NATS server", "JETSTREAM_NOT_ENABLED", err)
	}
	c.js = js

	// Initialize message service with JetStream (wrapped to interface for testability)
	msgService, err := message.NewMessageService(
		message.WrapNATSJetStream(c.js),
		c.config.MaxDeliver,
		c.config.PublishMaxRetries,
		c.config.ResultStream,
		c.config.ResultSubject,
	)
	if err != nil {
		// Clean up connection on service initialization failure
		_ = nats.Close(c.conn)
		c.conn = nil
		c.js = nil
		return sdkerrors.NewInternalError("", "failed to initialize message service", "SERVICE_INIT_FAILED", err)
	}
	c.Messages = msgService

	return nil
}

// NewClientWithJSContext creates a client wired to a provided JSContext implementation.
// Useful for tests to avoid connecting to a real NATS server.
func NewClientWithJSContext(js message.JSContext) *Client {
	logger, _ := zap.NewProduction()
	// Use defaults: MaxDeliver=5, PublishMaxRetries=3, ResultStream=RESULTS, ResultSubject=result
	svc, _ := message.NewMessageService(js, 5, 3, "RESULTS", "result")
	return &Client{
		Messages: svc,
		logger:   logger,
	}
}

// SetLogger sets a custom zap logger for the client
func (c *Client) SetLogger(logger *zap.Logger) {
	if logger != nil {
		c.logger = logger
	}
}

// Close gracefully closes the NATS connection and cleans up all resources.
// It drains in-flight messages before closing to ensure no message loss.
//
// This method should always be called when done with the client, typically
// using defer immediately after Connect().
//
// Example:
//
//	client := client.NewClient("nats://localhost:4222")
//	if err := client.Connect(ctx); err != nil {
//	    client.logger.Fatal("Failed to connect", zap.Error(err))
//	}
//	defer client.Close()
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}

	if err := nats.Close(c.conn); err != nil {
		return sdkerrors.NewInternalError("", "failed to close connection", "CLOSE_FAILED", err)
	}

	// Clean up resources
	c.conn = nil
	c.js = nil
	c.Messages = nil

	return nil
}

// IsConnected returns true if the client is currently connected to the NATS server.
// This can be used to check connection health before performing operations.
//
// Example:
//
//	if !client.IsConnected() {
//	    client.logger.Info("Not connected, attempting reconnection...")
//	    client.Connect(ctx)
//	}
func (c *Client) IsConnected() bool {
	return nats.IsConnected(c.conn)
}

// Connection returns the underlying NATS connection.
// This is exposed for advanced use cases where direct access to the connection is needed.
//
// Warning: Direct manipulation of the connection can interfere with the SDK's
// connection management. Use with caution.
func (c *Client) Connection() *natsclient.Conn {
	return c.conn
}

// JetStream returns the JetStream context for advanced JetStream operations.
// Returns nil if JetStream is not enabled on the NATS server.
//
// This provides direct access to JetStream features like stream and consumer management.
//
// Example:
//
//	js := client.JetStream()
//	if js != nil {
//	    // Create a stream
//	    js.AddStream(&nats.StreamConfig{
//	        Name:     "EVENTS",
//	        Subjects: []string{"events.>"},
//	    })
//	}
func (c *Client) JetStream() natsclient.JetStreamContext {
	return c.js
}

// Stats returns current connection statistics including message counts and reconnection attempts.
//
// Example:
//
//	stats := client.Stats()
//	client.logger.Info("Connection stats",
//		zap.Uint64("messages_sent", stats.OutMsgs),
//		zap.Uint64("messages_received", stats.InMsgs))
func (c *Client) Stats() ConnectionStats {
	if c.conn == nil {
		return ConnectionStats{}
	}

	stats := c.conn.Stats()
	return ConnectionStats{
		InMsgs:     stats.InMsgs,
		OutMsgs:    stats.OutMsgs,
		InBytes:    stats.InBytes,
		OutBytes:   stats.OutBytes,
		Reconnects: stats.Reconnects,
	}
}

// ConnectionStats holds connection statistics for monitoring and debugging.
type ConnectionStats struct {
	InMsgs     uint64 // Number of messages received
	OutMsgs    uint64 // Number of messages sent
	InBytes    uint64 // Number of bytes received
	OutBytes   uint64 // Number of bytes sent
	Reconnects uint64 // Number of reconnections performed
}

// ensureConnected checks if the client is connected and returns an error if not.
func (c *Client) ensureConnected() error {
	if !c.IsConnected() {
		return sdkerrors.NewInternalError("", "not connected to NATS", "NOT_CONNECTED", nil)
	}
	return nil
}

// Ping sends a ping to the NATS server to verify connectivity.
// This can be used as a health check to ensure the connection is alive and responsive.
//
// The operation respects the context deadline and can be cancelled via context.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	if err := client.Ping(ctx); err != nil {
//	    client.logger.Error("Connection unhealthy", zap.Error(err))
//	}
func (c *Client) Ping(ctx context.Context) error {
	if err := c.ensureConnected(); err != nil {
		return err
	}

	// Create a channel to handle ping result
	resultCh := make(chan error, 1)

	go func() {
		err := c.conn.FlushTimeout(c.config.Timeout)
		resultCh <- err
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("ping cancelled: %w", ctx.Err())
	case err := <-resultCh:
		if err != nil {
			return sdkerrors.NewInternalError("", "ping failed", "PING_FAILED", err)
		}
		return nil
	}
}
