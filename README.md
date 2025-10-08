# Icarus - NATS JetStream SDK for Go

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
  - Concurrent message processing with worker pools (Runner)
- **Middleware Support**: Composable middleware for cross-cutting concerns
- **Robust Error Handling**: SDK-specific errors with proper error wrapping
- **Connection Management**: Automatic reconnection and connection health monitoring
- **Full JetStream Integration**: Automatic JetStream context initialization and advanced operations
- **Future-Proof**: Designed for easy extension with growing process management capabilities

## Installation

```bash
go get github.com/wehubfusion/Icarus
```

## Quick Start

### 1. Connect to NATS

```go
import (
    "context"
    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/message"
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
msg := message.NewWorkflowMessage("workflow-123", "run-456").
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
sub1, _ := c.Messages.Subscribe(ctx, "tasks.process", handler)
defer sub1.Unsubscribe()

// Worker 2
sub2, _ := c.Messages.Subscribe(ctx, "tasks.process", handler)
defer sub2.Unsubscribe()

// Messages published to "tasks.process" will be received by both workers
```

### 5. Concurrent Message Processing (Runner)

```go
// Define a message processor
processor := func(ctx context.Context, msg *message.Message) error {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
    // Process message...
    return nil
}

// Create and configure runner
runner := client.NewRunner(c, "mystream", "myconsumer", 10, 3) // batchSize=10, numWorkers=3
runner.RegisterProcessor(processor)

// Start processing (blocks until context cancelled)
if err := runner.Run(ctx); err != nil {
    log.Printf("Runner error: %v", err)
}
```

The Runner provides:
- Concurrent message processing with configurable worker pools
- Batch message pulling from JetStream consumers
- Automatic success/error reporting to the "result" subject
- Graceful shutdown on context cancellation

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
import "github.com/wehubfusion/Icarus/internal/nats"

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
Icarus/
├── go.mod
├── go.sum
├── README.md
├── internal/                 # Private helpers
│   └── nats/                 # NATS connection utilities
│       └── connection.go     # Connection management, reconnection logic
├── pkg/                      # Public API
│   ├── client/               # Central NATS client
│   │   └── client.go         # Client with connection management
│   ├── message/              # Message handling and JetStream operations
│   │   ├── message.go        # Message struct and serialization
│   │   ├── handler.go        # Handler types and middleware
│   │   └── service.go        # Message service with pub/sub operations
│   ├── runner/               # Concurrent message processing
│   │   └── runner.go         # Worker pool-based message processing
│   ├── errors/               # SDK-specific errors
│   │   └── errors.go         # Error types and utilities
│   └── process/              # Process management utilities
│       └── strings/          # String processing utilities
│           └── strings.go    # String manipulation functions
├── examples/                 # Usage examples
│   ├── message/              # JetStream messaging patterns
│   │   └── main.go           # All messaging patterns with the client
│   ├── runner/               # Concurrent processing examples
│   │   └── main.go           # Runner usage examples
│   └── process/              # Process management examples
│       └── strings/          # String processing utilities
│           └── main.go       # String processing demo
└── tests/                    # Test suite
    ├── client_test.go        # Client functionality tests
    ├── message_test.go       # Message and service tests
    ├── runner_test.go        # Runner functionality tests
    ├── nats_mock_test.go     # Mock implementations for testing
    └── error_test.go         # Error handling tests
```

## Error Handling

The SDK provides structured error handling with typed errors:

```go
import (
    sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
    "errors"
)

err := c.Messages.Publish(ctx, "test", msg)
if err != nil {
    // Check for specific error types using errors.As
    var appErr *sdkerrors.AppError
    if errors.As(err, &appErr) {
        switch appErr.Type {
        case sdkerrors.ValidationFailed:
            fmt.Println("Validation error:", appErr.Message)
        case sdkerrors.NotFound:
            fmt.Println("Resource not found:", appErr.Message)
        case sdkerrors.Internal:
            fmt.Println("Internal error:", appErr.Message)
        }
    }

    // Or check specific error codes
    if errors.As(err, &appErr) && appErr.Code == "NOT_CONNECTED" {
        fmt.Println("Not connected to NATS")
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

## String Processing Utilities

The SDK includes comprehensive string processing utilities for workflow orchestration and data manipulation:

```go
import "github.com/wehubfusion/Icarus/pkg/process/strings"

// Basic operations
result := strings.Concatenate("-", "hello", "world", "test") // "hello-world-test"
parts := strings.Split("a,b,c", ",")                         // ["a", "b", "c"]
joined := strings.Join([]string{"x", "y", "z"}, " ")         // "x y z"

// Case manipulation
upper := strings.ToUpper("hello")        // "HELLO"
lower := strings.ToLower("WORLD")        // "world"
title := strings.TitleCase("hello world") // "Hello World"
cap := strings.Capitalize("hello")        // "Hello"

// Search and replace
contains, _ := strings.Contains("hello world", "world", false) // true
replaced, _ := strings.Replace("hello 123", "\\d+", "456", -1, true) // "hello 456"

// Encoding/Decoding
encoded := strings.Base64Encode("hello")     // "aGVsbG8="
decoded, _ := strings.Base64Decode(encoded)   // "hello"
urlSafe := strings.URIEncode("hello world")  // "hello%20world"

// Template formatting
formatted := strings.Format("Hello {name}!", map[string]string{"name": "Alice"}) // "Hello Alice!"

// Text normalization (removes diacritics)
normalized := strings.Normalize("café") // "cafe"
```

### Available Functions

- **Basic Operations**: `Concatenate`, `Split`, `Join`, `Trim`, `Substring`
- **Search & Replace**: `Replace` (with regex support), `Contains`
- **Case Operations**: `ToUpper`, `ToLower`, `TitleCase`, `Capitalize`
- **Encoding**: `Base64Encode`, `Base64Decode`, `URIEncode`, `URIDecode`
- **Text Processing**: `Format` (template replacement), `Normalize` (diacritic removal)
- **Regex Operations**: `RegexExtract`
- **Utilities**: `Length` (rune-aware character count)

All functions are Unicode-aware and handle multi-byte characters correctly.

## Complete Example

Here's a comprehensive example showing the client usage pattern:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/message"
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
            RecoveryMiddleware(),
            LoggingMiddleware(),
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

### Runner Example

```go
// Define a message processor for concurrent processing
processor := func(ctx context.Context, msg *message.Message) error {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)
    // Simulate processing work
    time.Sleep(100 * time.Millisecond)
    return nil
}

// Create and configure runner for concurrent processing
runner := client.NewRunner(c, "EVENTS", "worker-consumer", 5, 3) // batchSize=5, numWorkers=3
runner.RegisterProcessor(processor)

// Start processing in background (blocks until context cancelled)
go func() {
    if err := runner.Run(ctx); err != nil {
        log.Printf("Runner error: %v", err)
    }
}()

// Publish messages for processing
for i := 0; i < 10; i++ {
    msg := message.NewWorkflowMessage("batch-workflow", fmt.Sprintf("run-%d", i)).
        WithPayload("batch-service", fmt.Sprintf("Message %d", i), fmt.Sprintf("msg-%d", i))
    if err := c.Messages.Publish(ctx, "events.process", msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}

time.Sleep(2 * time.Second) // Wait for processing
```

## Examples

See the `examples/` directory for complete working examples:

- **examples/message/main.go**: Demonstrates JetStream messaging patterns including publish/subscribe, queue subscriptions, and pull-based consumers
- **examples/runner/main.go**: Shows concurrent message processing using the Runner with worker pools
- **examples/process/strings/main.go**: Comprehensive string processing utilities demo

To run examples:

```bash
# Start NATS server
docker run -p 4222:4222 nats:latest

# Run message example
go run examples/message/main.go

# Run runner example
go run examples/runner/main.go

# Run string processing example
go run examples/process/strings/main.go
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

- **Process Management**: Service registration, discovery, and health monitoring (string processing utilities implemented)
- **Metrics and Tracing**: Built-in observability support
- **Advanced JetStream Features**: Streams, consumers, and persistence
- **Message Encryption**: End-to-end encryption support
- **Schema Validation**: Message schema validation

## Requirements

- Go 1.25.1 or higher
- NATS Server 2.x or higher with JetStream enabled (required for all operations)
