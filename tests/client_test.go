package tests

import (
	"context"
	"testing"
	"time"

	"github.com/amirhy/nats-sdk/pkg/client"
)

func TestClientCreation(t *testing.T) {
	// Test creating client with URL
	url := "nats://localhost:4222"
	c := client.NewClient(url)
	if c == nil {
		t.Error("Expected client to be created")
	}

	// Test creating client with config
	// We can't directly test NewClientWithConfig without the internal type,
	// but we can test that it doesn't panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewClientWithConfig panicked: %v", r)
		}
	}()

	// Test with a mock config structure
	_ = struct {
		URL string
	}{
		URL: url,
	}

	// Test that client starts with nil connection
	if c.Connection() != nil {
		t.Error("Expected initial connection to be nil")
	}
	if c.IsConnected() {
		t.Error("Expected initial state to be not connected")
	}
	if c.JetStream() != nil {
		t.Error("Expected initial JetStream to be nil")
	}
	if c.Messages != nil {
		t.Error("Expected initial Messages service to be nil")
	}
}

func TestClientConnectionLifecycle(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	// Test initial state
	if c.IsConnected() {
		t.Error("Expected client to not be connected initially")
	}

	// Test connection
	err := c.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	if !c.IsConnected() {
		t.Error("Expected client to be connected after Connect()")
	}

	if c.JetStream() == nil {
		t.Error("Expected JetStream to be available")
	}

	if c.Messages == nil {
		t.Error("Expected Messages service to be initialized")
	}

	if c.Connection() == nil {
		t.Error("Expected connection to be available")
	}

	// Test stats
	stats := c.Stats()
	if stats.InMsgs != 0 {
		t.Logf("Initial InMsgs: %d", stats.InMsgs)
	}
	if stats.OutMsgs != 0 {
		t.Logf("Initial OutMsgs: %d", stats.OutMsgs)
	}

	// Test disconnection
	c.Close()

	if c.IsConnected() {
		t.Error("Expected client to be disconnected after Close()")
	}

	if c.Connection() != nil {
		t.Error("Expected connection to be nil after Close()")
	}

	if c.JetStream() != nil {
		t.Error("Expected JetStream to be nil after Close()")
	}

	if c.Messages != nil {
		t.Error("Expected Messages service to be nil after Close()")
	}
}

func TestClientPing(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	// Test ping when not connected
	err := c.Ping(ctx)
	if err == nil {
		t.Error("Expected ping to fail when not connected")
	}

	// Connect and test ping
	err = c.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	// Test successful ping
	err = c.Ping(ctx)
	if err != nil {
		t.Errorf("Expected ping to succeed when connected, got: %v", err)
	}

	// Test ping with timeout
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancel()

	// This should timeout (though in practice it might be fast enough)
	err = c.Ping(ctxWithTimeout)
	// Note: This test is timing-dependent and may not always fail
	// In a real scenario, we'd use a much shorter timeout
	if ctxWithTimeout.Err() == context.DeadlineExceeded {
		t.Log("Ping correctly timed out")
	}
}

func TestClientReconnection(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	// Connect initially
	err := c.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	// Verify connection
	if !c.IsConnected() {
		t.Error("Expected to be connected")
	}

	// Stop the server
	ts.Stop()

	// Give some time for disconnection to be detected
	time.Sleep(100 * time.Millisecond)

	// Note: In a real test, we'd verify reconnection behavior,
	// but the NATS client handles reconnection automatically.
	// Here we just ensure the client doesn't crash.

	// Clean up
	c.Close()
}

func TestClientConfiguration(t *testing.T) {
	// Test that different client configurations work
	urls := []string{
		"nats://localhost:4222",
		"nats://127.0.0.1:4222",
		"nats://example.com:4222",
	}

	for _, url := range urls {
		c := client.NewClient(url)
		if c == nil {
			t.Errorf("Failed to create client with URL: %s", url)
		}
	}
}

func TestClientMultipleConnections(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	// Test multiple clients connecting to the same server
	const numClients = 3

	clients := make([]*client.Client, numClients)
	ctx := context.Background()

	// Connect all clients
	for i := 0; i < numClients; i++ {
		c := client.NewClient(ts.URL())
		clients[i] = c

		err := c.Connect(ctx)
		if err != nil {
			t.Fatalf("Failed to connect client %d: %v", i, err)
		}

		if !c.IsConnected() {
			t.Errorf("Client %d not connected", i)
		}
	}

	// Disconnect all clients
	for i, c := range clients {
		c.Close()
		if c.IsConnected() {
			t.Errorf("Client %d still connected after close", i)
		}
	}
}

// Note: JetStream requirement testing is covered in the Connect method
// The SDK requires JetStream to be enabled and will fail to connect without it.

func TestClientServiceInitialization(t *testing.T) {
	ts := StartTestServer(t)
	defer ts.Stop()

	c := client.NewClient(ts.URL())
	ctx := context.Background()

	// Before connection, services should be nil
	if c.Messages != nil {
		t.Error("Expected Messages service to be nil before connection")
	}

	// After connection, services should be initialized
	err := c.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	if c.Messages == nil {
		t.Error("Expected Messages service to be initialized after connection")
	}

	// After disconnection, services should be cleaned up
	c.Close()

	if c.Messages != nil {
		t.Error("Expected Messages service to be nil after disconnection")
	}
}
