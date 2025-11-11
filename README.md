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
  - Message Publishing (JetStream-backed persistence)
  - Pull-based consumers (batch message processing)
  - Concurrent message processing with worker pools (Runner)
- **Runner Framework**: Built-in concurrent message processing with configurable worker pools, batch processing, and automatic success/error callback reporting
- **Distributed Tracing**: Integrated OpenTelemetry tracing support with Jaeger and OTLP exporters for observability
- **Callback Reporting**: Automatic success and error reporting to result streams with proper message acknowledgment
- **Robust Error Handling**: SDK-specific errors with proper error wrapping
- **Connection Management**: Automatic reconnection and connection health monitoring
- **Full JetStream Integration**: Automatic JetStream context initialization and advanced operations
- **Structured Logging**: Built-in zap logger integration with configurable log levels
- **Kubernetes-Aware Concurrency Control**: Production-ready goroutine limiting with automatic CPU quota detection, circuit breaker protection, and comprehensive observability
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
    "time"

    "github.com/wehubfusion/Icarus/pkg/client"
    "github.com/wehubfusion/Icarus/pkg/message"
    "github.com/wehubfusion/Icarus/pkg/runner"
    "github.com/wehubfusion/Icarus/pkg/tracing"
    "go.uber.org/zap"
)

// Create and connect client
c := client.NewClient("nats://localhost:4222")
ctx := context.Background()

// Create logger (optional, defaults will be used if not set)
logger, _ := zap.NewProduction()
c.SetLogger(logger)

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

### 3. Pull-Based Message Consumption

```go
// Pull messages from a JetStream consumer
messages, err := c.Messages.PullMessages(ctx, "EVENTS", "my-consumer", 10)
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
    msg.Ack() // Acknowledge message processing
}
```

### 4. Concurrent Message Processing (Runner)

```go
// Define a message processor that implements the Processor interface
type MyProcessor struct{}

func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    var content string
    if msg.Payload != nil {
        content = msg.Payload.Data
    }
    fmt.Printf("Processing: %s (Workflow: %s)\n", content, msg.Workflow.WorkflowID)

    // Create result message with processing outcome
    resultMessage := message.NewMessage().
        WithPayload("processor", "Successfully processed", "result-ref")

    // Copy workflow information for callback reporting
    if msg.Workflow != nil {
        resultMessage.Workflow = msg.Workflow
    }

    return *resultMessage, nil
}

// Create and configure runner with tracing
processor := &MyProcessor{}
tracingConfig := tracing.JaegerConfig("my-service")

runner, err := runner.NewRunner(
    c,                // client
    processor,        // processor implementation
    "TASKS",          // stream name
    "task-consumer",  // consumer name
    10,               // batch size
    3,                // number of workers
    5*time.Minute,    // process timeout
    logger,           // zap logger
    &tracingConfig,   // tracing configuration (optional)
)
if err != nil {
    log.Fatal(err)
}
defer runner.Close() // Clean up tracing resources

// Start processing (blocks until context cancelled)
if err := runner.Run(ctx); err != nil {
    log.Printf("Runner error: %v", err)
}
```

The Runner provides:
- Concurrent message processing with configurable worker pools
- Batch message pulling from JetStream consumers
- Automatic success/error reporting to the "result" subject
- Built-in OpenTelemetry tracing support
- Graceful shutdown on context cancellation

## Client Architecture

The `Client` struct provides a central hub for all NATS operations with automatic service initialization:

```go
type Client struct {
    conn   *natsclient.Conn
    js     natsclient.JetStreamContext
    config *nats.ConnectionConfig
    logger *zap.Logger

    // Messages provides access to all JetStream messaging operations including
    // publish, pull-based consumers, and callback reporting
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

// Configure logger (optional - defaults will be used if not set)
logger, _ := zap.NewProduction()
c.SetLogger(logger)

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

## Runner Framework

The Runner provides a powerful concurrent message processing framework that automatically handles batch processing, worker pools, success/error callbacks, and distributed tracing.

### Runner Architecture

The Runner consists of the following components:

- **Processor Interface**: Defines how individual messages are processed
- **Worker Pool**: Configurable number of goroutines for concurrent processing
- **Batch Puller**: Pulls messages in configurable batches from JetStream consumers
- **Callback Reporter**: Automatically reports success/error results to the "result" subject
- **Tracing Integration**: Built-in OpenTelemetry tracing with span propagation
- **Graceful Shutdown**: Handles context cancellation and proper cleanup

### Processor Interface

Implement the `Processor` interface to define your message processing logic:

```go
type Processor interface {
    Process(ctx context.Context, msg *message.Message) (message.Message, error)
}
```

The `Process` method should:
- Return a result `Message` containing processing outcomes
- Copy workflow information from input to result message for callback reporting
- Handle errors appropriately (they'll be automatically reported)

### Creating a Runner

```go
// Implement the Processor interface
type MyProcessor struct{}

func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Your processing logic here

    // Create result message
    result := message.NewMessage().
        WithPayload("processor", "Processing complete", "result-ref")

    // Copy workflow info for callback reporting
    if msg.Workflow != nil {
        result.Workflow = msg.Workflow
    }

    return *result, nil
}

// Configure tracing (optional)
tracingConfig := tracing.JaegerConfig("my-service")

// Create runner
runner, err := runner.NewRunner(
    client,           // Connected Icarus client
    &MyProcessor{},   // Processor implementation
    "TASKS",          // Stream name
    "task-consumer",  // Consumer name
    10,               // Batch size (messages per pull)
    5,                // Number of worker goroutines
    5*time.Minute,    // Processing timeout per message
    logger,           // Zap logger
    &tracingConfig,   // Tracing configuration (nil to disable)
)
if err != nil {
    log.Fatal(err)
}
defer runner.Close() // Cleanup tracing resources

// Start processing (blocks until context cancelled)
if err := runner.Run(ctx); err != nil {
    log.Printf("Runner stopped: %v", err)
}
```

### Runner Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `*client.Client` | Connected Icarus client |
| `processor` | `Processor` | Message processing implementation |
| `stream` | `string` | JetStream stream name to consume from |
| `consumer` | `string` | Durable consumer name |
| `batchSize` | `int` | Messages to pull per batch (1-1000) |
| `numWorkers` | `int` | Concurrent worker goroutines (1-100) |
| `processTimeout` | `time.Duration` | Max processing time per message |
| `logger` | `*zap.Logger` | Structured logger (cannot be nil) |
| `tracingConfig` | `*tracing.TracingConfig` | Tracing setup (nil disables tracing) |

### Configuring MaxDeliver (Retry Limit)

Control how many times messages are retried before being considered failed. This prevents infinite retry loops for permanent failures while allowing transient failures to recover.

**Default**: 5 retries (2.5 minutes with 30s AckWait)

```go
// Configure MaxDeliver in ConnectionConfig before Connect()
config := nats.DefaultConnectionConfig("nats://localhost:4222")
config.MaxDeliver = 20  // 20 retries = 10 minutes total (with 30s AckWait)

c := client.NewClientWithConfig(config)
if err := c.Connect(ctx); err != nil {
    logger.Fatal("Failed to connect", zap.Error(err))
}

// Or use one of these common configurations:
config.MaxDeliver = 5    // Default: 2.5 minutes (good for fast-moving systems)
config.MaxDeliver = 20   // 10 minutes (handles most incidents)
config.MaxDeliver = 100  // 50 minutes (very conservative)
config.MaxDeliver = -1   // Unlimited retries (not recommended)
```

**Calculation**: `Total Retry Time = MaxDeliver × AckWait`

- With default AckWait of 30 seconds:
  - MaxDeliver: 5 → 2.5 minutes
  - MaxDeliver: 20 → 10 minutes
  - MaxDeliver: 100 → 50 minutes

**When to adjust**:

- **Increase** if your services have longer MTTR (Mean Time To Recovery)
- **Decrease** for fast-moving systems where old messages become stale
- Consider your typical incident response time

### Automatic Callback Reporting

The Runner automatically reports processing results:

**Success Callbacks**: Published to the "result" subject when processing succeeds
**Error Callbacks**: Published to the "result" subject when processing fails
**Message Acknowledgment**: Source messages are properly ack'd/nack'd based on processing outcome

### Stream and Consumer Setup

Ensure your streams and consumers are configured for the Runner:

```go
js := client.JetStream()

// Create stream
_, err := js.AddStream(&nats.StreamConfig{
    Name:        "TASKS",
    Description: "Task messages for processing",
    Subjects:    []string{"tasks.>"},
    Storage:     nats.FileStorage,
    Replicas:    1,
    MaxAge:      24 * time.Hour,
})

// Create consumer
_, err = js.AddConsumer("TASKS", &nats.ConsumerConfig{
    Durable:       "task-consumer",
    Description:   "Consumer for task processing",
    AckPolicy:     nats.AckExplicitPolicy,
    MaxDeliver:    3,               // Retry failed messages
    AckWait:       5 * time.Minute, // Wait for acknowledgment
    MaxAckPending: 100,             // Allow pending messages
})
```

### Tracing Integration

The Runner includes comprehensive tracing:

- **Automatic Span Creation**: Spans for message pulling, processing, and reporting
- **Context Propagation**: Trace context flows through the entire processing pipeline
- **Rich Attributes**: Workflow IDs, run IDs, worker information, timing data
- **Error Recording**: Failed processing is recorded in spans
- **Cleanup**: Proper shutdown of tracing resources

## Distributed Tracing

Icarus includes built-in OpenTelemetry tracing support for observability and debugging of message processing pipelines.

### Tracing Setup

Configure tracing when creating a Runner or set it up manually:

```go
// Option 1: Jaeger configuration (recommended for development)
tracingConfig := tracing.JaegerConfig("my-service-name")
tracingConfig.SampleRatio = 1.0 // Sample all traces in development

// Option 2: Generic OTLP configuration
tracingConfig := tracing.TracingConfig{
    ServiceName:    "my-service",
    ServiceVersion: "1.0.0",
    Environment:    "production",
    OTLPEndpoint:   "otel-collector.company.com:4318", // Host:port only
    SampleRatio:    0.1, // Sample 10% of traces in production
}

// Option 3: Manual setup
shutdown, err := tracing.SetupTracing(ctx, tracingConfig, logger)
if err != nil {
    logger.Error("Failed to setup tracing", zap.Error(err))
}
defer shutdown(ctx) // Cleanup when done
```

### Tracing with Runner

The Runner automatically integrates tracing:

```go
runner, err := runner.NewRunner(
    client,
    processor,
    "TASKS",
    "task-consumer",
    10,             // batch size
    5,              // workers
    5*time.Minute,  // timeout
    logger,
    &tracingConfig, // Enable tracing
)
if err != nil {
    log.Fatal(err)
}
defer runner.Close() // Automatically shuts down tracing
```

### Tracing Configuration

| Field | Description | Default |
|-------|-------------|---------|
| `ServiceName` | Service identifier for traces | Required |
| `ServiceVersion` | Service version | "1.0.0" |
| `Environment` | Deployment environment | "development" |
| `OTLPEndpoint` | OTLP exporter endpoint (host:port) | "127.0.0.1:4318" |
| `SampleRatio` | Trace sampling ratio (0.0-1.0) | 1.0 (development), 0.1 (production) |

### What Gets Traced

The Runner creates spans for:

- **Message Pulling**: Batch message retrieval from JetStream
- **Message Processing**: Individual message processing by workers
- **Success Reporting**: Callback publication for successful processing
- **Error Reporting**: Callback publication for failed processing

### Span Attributes

Traces include rich metadata:

```go
// Workflow information
workflow.id = "workflow-123"
workflow.run_id = "run-456"

// Worker information
worker.id = 2

// Stream/consumer details
stream = "TASKS"
consumer = "task-consumer"

// Timing information
processing.duration_ms = 1500

// Message details
message.node.id = "process-payment"
message.payload.source = "api-server"
message.created_at = "2024-01-01T12:00:00Z"
```

### Jaeger Integration

For local development with Jaeger:

```bash
# Start Jaeger
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest

# Configure Icarus to send traces to Jaeger
tracingConfig := tracing.JaegerConfig("my-service")
tracingConfig.OTLPEndpoint = "localhost:14268" // Jaeger OTLP endpoint
```

Then visit `http://localhost:16686` to view traces.

### Production Setup

For production environments:

```go
tracingConfig := tracing.TracingConfig{
    ServiceName:    "order-processor",
    ServiceVersion: "2.1.0",
    Environment:    "production",
    OTLPEndpoint:   "otel-collector.company.com:4318",
    SampleRatio:    0.1, // Sample 10% of traces
}
```

## Callback Reporting

Icarus provides automatic callback reporting for workflow orchestration, allowing downstream systems to be notified of processing results.

### Callback Architecture

The callback system consists of:

- **Result Stream**: A dedicated "result" subject for all callback messages
- **Success Callbacks**: Published when message processing succeeds
- **Error Callbacks**: Published when message processing fails
- **Message Acknowledgment**: Proper ack/nak handling based on processing outcome

### Automatic Callbacks with Runner

The Runner automatically handles callback reporting:

```go
// When processing succeeds, a success callback is published to "result"
func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Processing logic...

    result := message.NewMessage().
        WithPayload("processor", "Success", "result-ref")

    // Copy workflow info for callback correlation
    if msg.Workflow != nil {
        result.Workflow = msg.Workflow
    }

    return *result, nil // Triggers success callback
}

// When processing fails, an error callback is published to "result"
func (p *MyProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
    // Processing logic...

    if someErrorCondition {
        return message.Message{}, fmt.Errorf("processing failed") // Triggers error callback
    }

    return *result, nil
}
```

### Manual Callback Reporting

You can also report callbacks manually using the MessageService:

```go
// Report success
successResult := message.NewWorkflowMessage(workflowID, runID).
    WithPayload("processor", "Task completed successfully", "success-ref")

err := client.Messages.ReportSuccess(ctx, *successResult, originalNATSMsg)
if err != nil {
    logger.Error("Failed to report success", zap.Error(err))
}

// Report error
err := client.Messages.ReportError(ctx, workflowID, runID, "Processing failed: timeout", originalNATSMsg)
if err != nil {
    logger.Error("Failed to report error", zap.Error(err))
}
```

### Callback Message Format

Success callbacks contain:

```json
{
  "workflow": {
    "workflowID": "workflow-123",
    "runID": "run-456"
  },
  "payload": {
    "source": "processor",
    "data": "Processing completed successfully",
    "reference": "result-ref"
  },
  "metadata": {
    "result_type": "success"
  },
  "createdAt": "2024-01-01T12:00:00Z",
  "updatedAt": "2024-01-01T12:00:00Z"
}
```

Error callbacks contain:

```json
{
  "workflow": {
    "workflowID": "workflow-123",
    "runID": "run-456"
  },
  "payload": {
    "source": "callback-service",
    "data": "Processing failed: connection timeout",
    "reference": "error-workflow-123-run-456"
  },
  "metadata": {
    "result_type": "error",
    "correlation_id": "workflow-123-run-456"
  },
  "createdAt": "2024-01-01T12:00:00Z",
  "updatedAt": "2024-01-01T12:00:00Z"
}
```

### Setting Up Result Streams

Create a stream to capture callback messages:

```go
js := client.JetStream()

// Create RESULTS stream for callbacks
_, err := js.AddStream(&nats.StreamConfig{
    Name:        "RESULTS",
    Description: "Stream for success/error callback reporting",
    Subjects:    []string{"result"},
    Storage:     nats.FileStorage,
    Replicas:    1,
    MaxAge:      7 * 24 * time.Hour, // Keep results for 7 days
})
```

### Consuming Callbacks

Pull callbacks for monitoring or further processing:

```go
// Pull callback messages from the RESULTS stream
messages, err := client.Messages.PullMessages(ctx, "RESULTS", "callback-consumer", 10)
if err != nil {
    logger.Error("Failed to pull callback messages", zap.Error(err))
    return
}

for _, msg := range messages {
    resultType := msg.Metadata["result_type"]

    if resultType == "success" {
        logger.Info("Processing succeeded",
            zap.String("workflow_id", msg.Workflow.WorkflowID),
            zap.String("run_id", msg.Workflow.RunID))
    } else if resultType == "error" {
        logger.Error("Processing failed",
            zap.String("workflow_id", msg.Workflow.WorkflowID),
            zap.String("run_id", msg.Workflow.RunID),
            zap.String("error", msg.Payload.Data))
    }

    msg.Ack()
}
```

### Message Acknowledgment

The callback system handles message acknowledgment automatically:

- **Success Callbacks**: Original message is acknowledged after successful callback publication
- **Error Callbacks**: Original message is negatively acknowledged after callback publication
- **Retry Logic**: Callbacks are published with retry logic for reliability

## Concurrency Control

Icarus provides production-ready, Kubernetes-aware concurrency control that prevents unlimited goroutine creation, provides observability, and scales properly in containerized environments.

### Features

- **Semaphore-Based Limiting**: Prevents unlimited goroutine creation with bounded concurrency
- **Circuit Breaker**: Automatic failure detection and graceful degradation under load
- **Kubernetes-Aware**: Automatically detects and respects container CPU limits using `automaxprocs`
- **Configurable**: Environment variables for fine-tuning based on workload characteristics
- **Observable**: Comprehensive metrics logged every 30 seconds
- **Thread-Safe**: All operations use atomic counters and proper synchronization

### Quick Start

```go
import (
    "github.com/wehubfusion/Icarus/pkg/concurrency"
    "github.com/wehubfusion/Icarus/pkg/runner"
)

func main() {
    // Initialize Kubernetes-aware concurrency (respects cgroup CPU limits)
    undoMaxprocs := concurrency.InitializeForKubernetes()
    defer undoMaxprocs()
    
    // Load configuration (auto-detects environment)
    config := concurrency.LoadConfig()
    
    // Create limiter with optimal settings
    limiter := concurrency.NewLimiter(config.MaxConcurrent)
    
    // Log configuration for observability
    logger.Info("Concurrency initialized",
        zap.Int("max_concurrent", config.MaxConcurrent),
        zap.Int("runner_workers", config.RunnerWorkers),
        zap.Int("effective_cpus", config.EffectiveCPUs),
        zap.Bool("is_kubernetes", config.IsKubernetes),
    )
    
    // Pass limiter to runner and processor
    runner, err := runner.NewRunner(
        client,
        processor,
        "TASKS",
        "consumer",
        10,                   // batch size
        config.RunnerWorkers, // workers from config
        5*time.Minute,
        logger,
        nil,    // tracing
        limiter, // concurrency limiter
    )
}
```

### Configuration

Concurrency is configured through environment variables with intelligent defaults:

| Variable | Description | Default |
|----------|-------------|---------|
| `ICARUS_MAX_CONCURRENT` | Explicit max concurrent goroutines | Auto-detected |
| `ICARUS_CONCURRENCY_MULTIPLIER` | CPU × multiplier for max concurrent | K8s: 2, Bare: 4 |
| `ICARUS_RUNNER_WORKERS` | Number of NATS consumer workers | max(CPU, 4) |
| `ICARUS_PROCESSOR_MODE` | `concurrent` or `sequential` processing | `concurrent` |
| `ICARUS_ITERATOR_MODE` | `parallel` or `sequential` array iteration | `sequential` |

**Configuration Priority**: Explicit env vars > auto-detection > defaults

**Environment Detection**:
- **Kubernetes**: Detected via `KUBERNETES_SERVICE_HOST` environment variable
- **CPU Limits**: Automatically detected using `runtime.GOMAXPROCS(0)` which respects cgroup limits

### Usage with Processor

The limiter automatically controls goroutine creation in embedded node processing:

```go
import (
    "github.com/wehubfusion/Icarus/pkg/embedded"
    "github.com/wehubfusion/Icarus/pkg/concurrency"
)

// Create processor with limiter
processor := embedded.NewProcessorWithLimiter(
    registry,
    true,    // concurrent mode
    limiter, // concurrency limiter
)

// Process embedded nodes - goroutines are automatically limited
results, err := processor.ProcessEmbeddedNodes(ctx, msg, parentOutput)
```

### Usage with Iterator

Control array iteration concurrency:

```go
import (
    "github.com/wehubfusion/Icarus/pkg/iteration"
    "github.com/wehubfusion/Icarus/pkg/concurrency"
)

// Create iterator with limiter
iterator := iteration.NewIteratorWithLimiter(
    iteration.Config{
        Strategy: iteration.StrategyParallel,
    },
    limiter,
)

// Process items - worker goroutines are automatically limited
results, err := iterator.Process(ctx, items, processFn)
```

### Observability

The runner logs concurrency metrics every 30 seconds:

```json
{
  "level": "info",
  "msg": "Concurrency metrics",
  "active_goroutines": 45,
  "peak_concurrent": 78,
  "total_acquired": 1523,
  "total_released": 1478,
  "avg_wait_time": "15ms",
  "circuit_breaker": "closed"
}
```

**Key Metrics**:
- **active_goroutines**: Current concurrent operations
- **peak_concurrent**: Maximum concurrency reached since start
- **total_acquired/released**: Cumulative slot acquisitions
- **avg_wait_time**: Average time waiting for a concurrency slot
- **circuit_breaker**: Protection state (closed/open/half-open)

### Circuit Breaker

Protects against cascade failures during overload:

- **Threshold**: Opens after 100 consecutive failures
- **Reset Timeout**: 30 seconds before attempting recovery
- **Half-Open State**: Tests recovery with limited traffic
- **Automatic Recovery**: Closes after 5 consecutive successes

When the circuit breaker is open, new operations fail immediately instead of overwhelming the system.

### Tuning Guidelines

#### CPU-Bound Workloads

Increase concurrency and enable parallelism:

```bash
export ICARUS_CONCURRENCY_MULTIPLIER=4
export ICARUS_PROCESSOR_MODE=concurrent
export ICARUS_ITERATOR_MODE=parallel
```

Monitor CPU throttling and adjust as needed.

#### I/O-Bound Workloads

Use higher concurrency with conservative CPU allocation:

```bash
export ICARUS_CONCURRENCY_MULTIPLIER=8
export ICARUS_RUNNER_WORKERS=12
```

Higher concurrency allows better I/O overlap.

#### Memory-Constrained Environments

Reduce concurrency and use sequential processing:

```bash
export ICARUS_CONCURRENCY_MULTIPLIER=1
export ICARUS_PROCESSOR_MODE=sequential
export ICARUS_ITERATOR_MODE=sequential
export ICARUS_RUNNER_WORKERS=2
```

### Kubernetes Deployment

See the `k8s/` directory for complete Kubernetes manifests including:
- ConfigMap with concurrency settings
- Deployment with proper resource limits
- HorizontalPodAutoscaler for automatic scaling
- Service for health and metrics endpoints

```bash
# Deploy with default settings
kubectl apply -f k8s/

# Monitor concurrency metrics
kubectl logs -f -l app=icarus-processor | grep "Concurrency metrics"

# Check autoscaling
kubectl get hpa icarus-processor-hpa
```

**Critical for Kubernetes**: Set CPU limits in your deployment:

```yaml
resources:
  limits:
    cpu: "2000m"  # automaxprocs uses this to set GOMAXPROCS
    memory: "4Gi"
```

### Best Practices

1. **Always Initialize**: Call `concurrency.InitializeForKubernetes()` at the start of `main()`
2. **Pass Limiter Through**: Pass the limiter to Runner, Processor, and Iterator
3. **Monitor Metrics**: Watch for high `avg_wait_time` or `circuit_breaker: open`
4. **Start Conservative**: Begin with default settings, scale up based on metrics
5. **Set Resource Limits**: In Kubernetes, always set both requests and limits
6. **Test Scaling**: Verify behavior under load before production deployment

### Troubleshooting

**High Wait Times**: Increase `ICARUS_CONCURRENCY_MULTIPLIER` or scale horizontally

**Circuit Breaker Opens**: Check downstream services and reduce load temporarily

**CPU Throttling**: Increase CPU limits in Kubernetes or reduce concurrency multiplier

**Memory Pressure**: Reduce max concurrent operations and use sequential modes

See `k8s/README.md` for detailed troubleshooting guides.

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
│   │   ├── handler.go        # Handler types and utilities
│   │   └── service.go        # Message service with pub/sub operations
│   ├── runner/               # Concurrent message processing
│   │   └── runner.go         # Worker pool-based message processing
│   ├── tracing/              # Distributed tracing utilities
│   │   └── tracing.go        # OpenTelemetry tracing setup
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
│   ├── runner-with-tracing/  # Runner with tracing examples
│   │   └── main.go           # Runner with OpenTelemetry tracing
│   └── process/              # Process management examples
│       └── strings/          # String processing utilities
│           └── main.go       # String processing demo
└── tests/                    # Test suite
    ├── client_test.go        # Client functionality tests
    ├── message_test.go       # Message and service tests
    ├── message_service_test.go # Message service tests
    ├── runner_test.go        # Runner functionality tests
    ├── tracing_test.go       # Tracing functionality tests
    ├── nats_mock_test.go     # Mock implementations for testing
    ├── integration_test.go   # Integration tests
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

    // EmbeddedNodes contains child nodes to be executed within the parent node (optional)
    EmbeddedNodes []EmbeddedNode `json:"embeddedNodes,omitempty"`

    // Connection contains connection details for the parent node (optional)
    Connection *ConnectionDetails `json:"connection,omitempty"`

    // Schema contains schema details for the parent node (optional)
    Schema *SchemaDetails `json:"schema,omitempty"`

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
- **EmbeddedNodes**: Array of child nodes to execute within the parent node (for unit processing)
- **Connection**: Connection details (ConnectionID, Type, Config) for database/API connections
- **Schema**: Schema details (SchemaID, Name, Fields) for data validation
- **Timestamps**: RFC3339 formatted creation and update timestamps

#### Unit Processing

Messages now support **unit processing** where a parent node can contain multiple embedded child nodes to be executed together. This enables atomic execution of related operations with proper field mapping between nodes.

```go
// Create a message with embedded nodes (unit processing)
embeddedNodes := []message.EmbeddedNode{
    {
        NodeID:         "child-node-1",
        PluginType:     "transform",
        Configuration:  []byte(`{"operation": "map"}`),
        ExecutionOrder: 1,
        FieldMappings: []message.FieldMapping{
            {
                SourceNodeID:         "parent",
                SourceEndpoint:       "/data/input",
                DestinationEndpoints: []string{"/input"},
                DataType:             "json",
            },
        },
    },
    {
        NodeID:         "child-node-2",
        PluginType:     "output",
        Configuration:  []byte(`{"destination": "database"}`),
        ExecutionOrder: 2,
    },
}

msg := message.NewWorkflowMessage("workflow-123", "run-456").
    WithNode("parent-node", nil).
    WithEmbeddedNodes(embeddedNodes).
    WithConnection(&message.ConnectionDetails{
        ConnectionID: "conn-789",
        Type:         "postgres",
        Config:       []byte(`{"host": "localhost", "port": 5432}`),
    }).
    WithSchema(&message.SchemaDetails{
        SchemaID: "schema-456",
        Name:     "users",
        Fields:   []byte(`[{"name": "id", "type": "int"}]`),
    })
```

**Key Features:**
- **ExecutionOrder**: Controls the sequence of embedded node execution
- **FieldMappings**: Maps data between parent and child nodes or between child nodes
- **Connection/Schema**: Shared across parent and embedded nodes for consistency
- **Backward Compatible**: Messages without embedded nodes work as before

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

    // Publish a test message
    msg := message.NewWorkflowMessage("test-workflow", uuid.New().String()).
        WithPayload("example-service", "Hello, World!", "msg-123").
        WithMetadata("test", "true")
    if err := c.Messages.Publish(ctx, "events.test", msg); err != nil {
        log.Printf("Failed to publish: %v", err)
    }

    // Simulate consuming the message using pull-based approach
    // In a real application, you would pull from a consumer
    messages, err := c.Messages.PullMessages(ctx, "TEST_EVENTS", "test-consumer", 5)
    if err != nil {
        log.Printf("Failed to pull messages: %v", err)
    } else {
        for _, pulledMsg := range messages {
            var content string
            if pulledMsg.Payload != nil {
                content = pulledMsg.Payload.Data
            }
            fmt.Printf("Pulled: %s (Workflow: %s)\n", content, pulledMsg.Workflow.WorkflowID)
            pulledMsg.Ack() // Acknowledge message
        }
    }
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

- **examples/message/main.go**: Demonstrates JetStream messaging patterns including message publishing, pull-based consumers, and callback reporting
- **examples/runner/main.go**: Shows concurrent message processing using the Runner with worker pools and callback reporting
- **examples/runner-with-tracing/main.go**: Demonstrates the Runner with OpenTelemetry tracing integration using Jaeger
- **examples/process/strings/main.go**: Comprehensive string processing utilities demo

To run examples:

```bash
# Start NATS server
docker run -p 4222:4222 nats:latest

# For tracing examples, also start Jaeger
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest

# Run message example
go run examples/message/main.go

# Run runner example
go run examples/runner/main.go

# Run runner with tracing example
go run examples/runner-with-tracing/main.go

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
4. **Close resources**: Always defer `c.Close()` after connecting
5. **Use structured logging**: Configure logging for better observability and debugging
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
- **Metrics**: Built-in metrics collection and export
- **Advanced JetStream Features**: Enhanced streams, consumers, and persistence management
- **Message Encryption**: End-to-end encryption support
- **Schema Validation**: Message schema validation
- **Advanced Routing**: Content-based routing and message transformation
- **Health Monitoring**: Service health checks and automatic recovery

## Requirements

- Go 1.25.1 or higher
- NATS Server 2.x or higher with JetStream enabled (required for all operations)
