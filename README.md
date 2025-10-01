# NATS JetStream SDK for Go

A clean, future-proof Go SDK for messaging over NATS JetStream with idiomatic patterns, comprehensive error handling, and built-in support for advanced JetStream messaging patterns.

## Features

- **JetStream-Only**: Exclusively uses NATS JetStream for all messaging operations with persistence and advanced features
- **Clean Architecture**: Well-organized package structure with clear separation of concerns
- **Idiomatic Go**: Follows Go best practices and conventions
- **Context Support**: All operations support `context.Context` for cancellation and timeout control
- **Structured Messages**: Rich message format with workflow, node, payload, and output information
- **Central Client**: Single client provides access to all JetStream services with automatic initialization
- **JetStream Messaging Patterns**:
  - Publish/Subscribe (JetStream-backed)
  - Queue subscriptions (load balancing with JetStream consumers)
  - Pull-based consumers (batch message processing)
- **Middleware Support**: Composable middleware for cross-cutting concerns
- **Robust Error Handling**: SDK-specific errors with proper error wrapping
- **Connection Management**: Automatic reconnection and connection health monitoring
- **Full JetStream Integration**: Automatic JetStream context initialization and advanced operations
- **Future-Proof**: Designed for easy extension (process management coming soon)

## Installation

```bash
go get github.com/amirhy/nats-sdk
```

## Quick Start

### 1. Connect to NATS

```go
import (
    "context"
    "github.com/amirhy/nats-sdk/pkg/client"
    "github.com/amirhy/nats-sdk/pkg/messaging"
)

// Create and connect client
c := client.NewClient("nats://localhost:4222")
ctx := context.Background()

if err := c.Connect(ctx); err != nil {
    log.Fatalf("Failed to connect: %v", err)
}
defer c.Close()
```

### 2. Publish Messages

```go
// Create a message
msg := message.NewWorkflowMessage("user-events", "run-123").
    WithPayload("api-server", "Hello, NATS!", "msg-123").
    WithMetadata("priority", "high")

// Publish using the client's Messages service (automatically initialized)
if err := c.Messages.Publish(ctx, "events.user.created", msg); err != nil {
    log.Printf("Failed to publish: %v", err)
}
```

### 3. Subscribe to Messages

```go
// Define a message handler
handler := func(ctx context.Context, msg *message.NATSMsg) error {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Received: %s - %s\n", msg.Workflow.WorkflowID, content)
    msg.Ack() // Acknowledge message
    return nil
}

// Subscribe using the client's Messages service
sub, err := c.Messages.Subscribe(ctx, "events.user.created", handler)
if err != nil {
    log.Fatalf("Failed to subscribe: %v", err)
}
defer sub.Unsubscribe()
```

### 4. Queue Subscriptions (Load Balancing)

```go
// Create multiple workers in the same queue group
handler := func(ctx context.Context, msg *message.NATSMsg) error {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Processing: %s\n", content)
    msg.Ack() // Acknowledge message processing
    return nil
}

// Worker 1
sub1, _ := c.Messages.QueueSubscribe(ctx, "tasks.process", "workers", handler)
defer sub1.Unsubscribe()

// Worker 2
sub2, _ := c.Messages.QueueSubscribe(ctx, "tasks.process", "workers", handler)
defer sub2.Unsubscribe()

// Messages published to "tasks.process" will be distributed between workers
```

## Client Architecture

The `Client` struct provides a central hub for all NATS operations with automatic service initialization:

```go
type Client struct {
    conn   *natsclient.Conn
    js     natsclient.JetStreamContext
    config *nats.ConnectionConfig

    // Public service interface
    Messages *message.MessageService

    // Reserved for future expansion
    // Processes *process.ProcessService
}
```

When `Connect()` is called, it:

- Establishes NATS connection
- Initializes JetStream context (if available)
- Automatically creates and assigns the `MessageService`

### Direct Service Access

Users can now access messaging operations directly through the client:

```go
c := client.NewClient("nats://localhost:4222")
c.Connect(ctx)
// c.Messages is ready to use immediately
c.Messages.Publish(ctx, "subject", msg)
```

### JetStream Context Access

Direct access to JetStream for advanced operations:

```go
js := c.JetStream()
if js != nil {
    // Create streams, consumers, etc.
    js.AddStream(&nats.StreamConfig{
        Name:     "EVENTS",
        Subjects: []string{"events.>"},
    })
}
```

## Advanced Usage

### Custom Connection Configuration

```go
import "github.com/amirhy/nats-sdk/internal/nats"

config := &nats.ConnectionConfig{
    URL:           "nats://localhost:4222",
    Name:          "my-service",
    MaxReconnects: 10,
    ReconnectWait: 2 * time.Second,
    Timeout:       5 * time.Second,
    Username:      "user",
    Password:      "pass",
}

c := client.NewClientWithConfig(config)
```

### Custom Middleware

```go
// Create custom middleware
customMiddleware := func(next message.Handler) message.Handler {
    return func(ctx context.Context, msg *message.NATSMsg) error {
        // Pre-processing
        start := time.Now()

        // Call next handler
        err := next(ctx, msg)

        // Post-processing
        duration := time.Since(start)
        fmt.Printf("Processed in %v\n", duration)

        return err
    }
}

// Apply middleware to the client's Messages service
c.Messages = c.Messages.WithMiddleware(customMiddleware)
```

### Pull-Based Consumers (JetStream)

```go
// Pull messages from a JetStream consumer
messages, err := c.Messages.PullMessages(ctx, "mystream", "myconsumer", 10)
if err != nil {
    log.Printf("Failed to pull messages: %v", err)
}

for _, msg := range messages {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Pulled message: %s\n", content)
}
```

This method:

- Connects to a JetStream pull consumer
- Fetches messages in batches
- Automatically acknowledges successfully deserialized messages
- Respects context cancellation/timeout

### Context-Based Cancellation

```go
// Create a context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

// All operations respect the context
if err := c.Messages.Publish(ctx, "events.test", msg); err != nil {
    log.Printf("Publish timed out or was cancelled: %v", err)
}
```

## Project Structure

```text
my-nats-sdk/
├── go.mod
├── README.md
├── internal/                 # Private helpers
│   └── nats/                 # NATS connection utilities
│       └── connection.go     # Connection management, reconnection logic
├── pkg/                      # Public API
│   ├── client/               # Central NATS client
│   │   └── client.go         # Client with connection management
│   ├── message/              # Messaging functionality
│   │   ├── message.go        # Message struct and serialization
│   │   ├── handler.go        # Handler types and middleware
│   │   └── service.go        # Message service with pub/sub operations
│   ├── errors/               # SDK-specific errors
│   │   └── errors.go         # Error types and utilities
│   └── process/              # Process management (coming soon)
└── examples/                 # Usage examples
    ├── message/              # JetStream messaging patterns
    │   └── main.go           # All messaging patterns with the client
    └── process/              # Process management (coming soon)
        └── main.go           # Process management examples (placeholder)
```

## Error Handling

The SDK provides structured error handling:

```go
import sdkerrors "github.com/amirhy/nats-sdk/pkg/errors"

err := c.Messages.Publish(ctx, "test", msg)
if err != nil {
    // Check for specific error types
    if sdkerrors.IsTimeout(err) {
        fmt.Println("Operation timed out")
    } else if sdkerrors.IsNotConnected(err) {
        fmt.Println("Not connected to NATS")
    }

    // Or check with errors.Is
    if errors.Is(err, sdkerrors.ErrTimeout) {
        fmt.Println("Timeout error")
    }
}
```

## Message Format

Messages are strongly typed with a rich structure designed for workflow orchestration and complex messaging scenarios:

```go
type Message struct {
    // Workflow contains workflow execution information (optional)
    Workflow *Workflow `json:"workflow,omitempty"`

    // Node contains node information within a workflow (optional)
    Node *Node `json:"node,omitempty"`

    // Payload contains the message data (optional)
    Payload *Payload `json:"payload,omitempty"`

    // Output contains output destination information (optional)
    Output *Output `json:"output,omitempty"`

    // Metadata holds additional key-value pairs
    Metadata map[string]string `json:"metadata,omitempty"`

    // CreatedAt is the timestamp when the message was created
    CreatedAt string `json:"createdAt"`

    // UpdatedAt is the timestamp when the message was last updated
    UpdatedAt string `json:"updatedAt"`
}
```

### Message Components

- **Workflow**: Contains `WorkflowID` and `RunID` for tracking workflow executions
- **Node**: Contains `NodeID` and `Configuration` for workflow node information
- **Payload**: Contains `Source`, `Data`, and `Reference` for the message content
- **Output**: Contains `DestinationType` for routing information
- **Metadata**: Flexible key-value pairs for additional information
- **Timestamps**: RFC3339 formatted creation and update timestamps

### Creating Messages

```go
// Simple workflow message
msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithPayload("user-service", "User created", "user-789").
    WithMetadata("priority", "high")

// Message with node information
msg = message.NewMessage().
    WithNode("process-payment", map[string]interface{}{"amount": 100}).
    WithOutput("payment-gateway")
```

## Complete Example

Here's a comprehensive example showing the client usage pattern:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/amirhy/nats-sdk/pkg/client"
    "github.com/amirhy/nats-sdk/pkg/messaging"
    "github.com/google/uuid"
)

func main() {
    // Create and connect
    c := client.NewClient("nats://localhost:4222")
    ctx := context.Background()

    if err := c.Connect(ctx); err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    // Apply middleware
    c.Messages = c.Messages.WithMiddleware(
        message.Chain(
            message.RecoveryMiddleware(),
            message.LoggingMiddleware(),
        ),
    )

    // Subscribe
    handler := func(ctx context.Context, msg *message.NATSMsg) error {
        var content string
        if msg.Payload != nil {
            content = msg.Payload.Data
        }
        fmt.Printf("Received: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
        msg.Ack() // Acknowledge message
        return nil
    }

    sub, err := c.Messages.Subscribe(ctx, "events.test", handler)
    if err != nil {
        log.Fatal(err)
    }
    defer sub.Unsubscribe()

    time.Sleep(100 * time.Millisecond) // Wait for subscription

    // Publish
    msg := message.NewWorkflowMessage("test-workflow", uuid.New().String()).
        WithPayload("example-service", "Hello, World!", "msg-123").
        WithMetadata("test", "true")
    if err := c.Messages.Publish(ctx, "events.test", msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }

    time.Sleep(500 * time.Millisecond) // Wait for processing
}
```

### Pull-Based Consumers Example

```go
// Pull messages from JetStream
messages, err := c.Messages.PullMessages(ctx, "EVENTS", "pull-consumer", 10)
if err != nil {
    log.Printf("Failed to pull messages: %v", err)
    return
}

for _, msg := range messages {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
    // Messages are automatically acknowledged after successful deserialization
}
```


## Examples

See the `examples/` directory for complete working examples:

- **examples/message/main.go**: Demonstrates JetStream messaging patterns including publish/subscribe, queue subscriptions, and pull-based consumers
- **examples/process/main.go**: Placeholder for future process management examples

To run examples:

```bash
# Start NATS server
docker run -p 4222:4222 nats:latest

# Run message example
go run examples/message/main.go
```

## Migration Guide

If you have existing code using the old pattern:

**Before:**

```go
c := client.NewClient(url)
c.Connect(ctx)
msgService, _ := message.NewMessageService(c.Connection())
msgService.Publish(ctx, subject, msg)
```

**After:**

```go
c := client.NewClient(url)
c.Connect(ctx)
c.Messages.Publish(ctx, subject, msg)
```

The old pattern still works but the new pattern is more concise.

## Best Practices

1. **Always use context**: Pass context to all operations for proper cancellation
2. **JetStream is required**: This SDK requires JetStream to be enabled on the NATS server
3. **Handle errors properly**: Check and handle all errors returned by SDK methods
4. **Close resources**: Always defer `c.Close()` and `sub.Unsubscribe()`
5. **Use middleware**: Leverage middleware for cross-cutting concerns like logging and validation
6. **Set timeouts**: Use context timeouts for operations that shouldn't block indefinitely
7. **Unique message IDs**: Generate unique IDs (e.g., using UUID) for message tracing
8. **Service access**: Use `c.Messages` directly instead of creating MessageService manually

## Public API

### Constructor Functions

```go
// Create with default config
NewClient(url string) *Client

// Create with custom config
NewClientWithConfig(config *nats.ConnectionConfig) *Client
```

### Connection Methods

```go
Connect(ctx context.Context) error
Close() error
IsConnected() bool
Ping(ctx context.Context) error
```

### Service Access

```go
Messages *message.MessageService  // Access to all messaging operations
JetStream() natsclient.JetStreamContext  // Direct JetStream access
Connection() *natsclient.Conn  // Low-level connection access
```

### Monitoring

```go
Stats() ConnectionStats  // Get connection statistics
```

### Usage Pattern

```go
// 1. Create and connect
c := client.NewClient("nats://localhost:4222")
ctx := context.Background()
c.Connect(ctx)
defer c.Close()

// 2. Use services directly
msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithPayload("service", "Hello World", "msg-789")
c.Messages.Publish(ctx, "subject", msg)
```

## Future Roadmap

- **Process Management**: Service registration, discovery, and health monitoring
- **Metrics and Tracing**: Built-in observability support
- **Advanced JetStream Features**: Streams, consumers, and persistence
- **Message Encryption**: End-to-end encryption support
- **Schema Validation**: Message schema validation

## Requirements

- Go 1.25.1 or higher
- NATS Server 2.x or higher with JetStream enabled (required for all operations)
