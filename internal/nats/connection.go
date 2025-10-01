package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// ConnectionConfig holds configuration for NATS connection
type ConnectionConfig struct {
	// URL is the NATS server URL (e.g., "nats://localhost:4222")
	URL string

	// Name is the client name for identifying this connection
	Name string

	// MaxReconnects is the maximum number of reconnection attempts
	// Use -1 for unlimited reconnects
	MaxReconnects int

	// ReconnectWait is the time to wait between reconnection attempts
	ReconnectWait time.Duration

	// Timeout is the connection timeout
	Timeout time.Duration

	// Token is an optional authentication token
	Token string

	// Username is an optional username for authentication
	Username string

	// Password is an optional password for authentication
	Password string
}

// DefaultConnectionConfig returns a configuration with sensible defaults
func DefaultConnectionConfig(url string) *ConnectionConfig {
	return &ConnectionConfig{
		URL:           url,
		Name:          "nats-sdk-client",
		MaxReconnects: 10,
		ReconnectWait: 2 * time.Second,
		Timeout:       5 * time.Second,
	}
}

// Connect establishes a connection to NATS with the provided configuration
func Connect(ctx context.Context, config *ConnectionConfig) (*nats.Conn, error) {
	if config == nil {
		return nil, fmt.Errorf("connection config cannot be nil")
	}

	if config.URL == "" {
		return nil, fmt.Errorf("NATS URL cannot be empty")
	}

	opts := []nats.Option{
		nats.Name(config.Name),
		nats.MaxReconnects(config.MaxReconnects),
		nats.ReconnectWait(config.ReconnectWait),
		nats.Timeout(config.Timeout),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				// In production, use structured logging
				fmt.Printf("NATS disconnected: %v\n", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Printf("NATS reconnected to %s\n", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			fmt.Println("NATS connection closed")
		}),
	}

	// Add authentication options if provided
	if config.Token != "" {
		opts = append(opts, nats.Token(config.Token))
	} else if config.Username != "" && config.Password != "" {
		opts = append(opts, nats.UserInfo(config.Username, config.Password))
	}

	// Create a channel to handle connection result
	type result struct {
		conn *nats.Conn
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		conn, err := nats.Connect(config.URL, opts...)
		resultCh <- result{conn: conn, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("connection cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", res.err)
		}
		return res.conn, nil
	}
}

// Close safely closes a NATS connection
func Close(conn *nats.Conn) error {
	if conn == nil {
		return nil
	}

	// Drain the connection to allow in-flight messages to complete
	if err := conn.Drain(); err != nil {
		// If drain fails, force close
		conn.Close()
		return fmt.Errorf("error draining connection: %w", err)
	}

	return nil
}

// IsConnected checks if the connection is active
func IsConnected(conn *nats.Conn) bool {
	return conn != nil && conn.IsConnected()
}

// WaitForConnection waits for the connection to be established or context to expire
func WaitForConnection(ctx context.Context, conn *nats.Conn, checkInterval time.Duration) error {
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		if conn.IsConnected() {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("connection wait cancelled: %w", ctx.Err())
		case <-ticker.C:
			// Continue waiting
		}
	}
}

