// Package runtime provides the core types and interfaces for embedded node processing.
package runtime

import "context"

// EmbeddedNode is the interface that all embedded node processors must implement.
// Each processor handles a specific plugin type and implements its own logic.
type EmbeddedNode interface {
	// Process executes the node's logic and returns the result.
	// This is the single entry point for all node processing.
	Process(input ProcessInput) ProcessOutput

	// NodeId returns the unique identifier of this node instance.
	NodeId() string

	// PluginType returns the type of plugin this node represents.
	PluginType() string
}

// EmbeddedNodeFactory creates embedded nodes from configuration.
// It acts as a registry for node creators.
type EmbeddedNodeFactory interface {
	// Create creates an embedded node from its configuration.
	// Returns an error if the plugin type is not registered or creation fails.
	Create(config EmbeddedNodeConfig) (EmbeddedNode, error)

	// Register registers a creator function for a plugin type.
	Register(pluginType string, creator NodeCreator)

	// HasCreator checks if a creator exists for a plugin type.
	HasCreator(pluginType string) bool

	// RegisteredTypes returns all registered plugin types.
	RegisteredTypes() []string
}

// NodeCreator is a function that creates an embedded node from configuration.
type NodeCreator func(config EmbeddedNodeConfig) (EmbeddedNode, error)

// ItemProcessor processes a single batch item.
// Used by the worker pool for concurrent processing.
type ItemProcessor interface {
	// ProcessItem processes a single item and returns the result.
	ProcessItem(ctx context.Context, item BatchItem) BatchResult
}

// Logger defines the logging interface for the runtime.
// Compatible with the existing logging.Logger interface.
type Logger interface {
	// Debug logs a debug message with optional fields.
	Debug(msg string, fields ...Field)
	// Info logs an info message with optional fields.
	Info(msg string, fields ...Field)
	// Warn logs a warning message with optional fields.
	Warn(msg string, fields ...Field)
	// Error logs an error message with optional fields.
	Error(msg string, fields ...Field)
}

// Field represents a key-value pair for structured logging.
type Field struct {
	Key   string
	Value interface{}
}

// NoOpLogger is a logger that does nothing.
// Useful for testing or when logging is not needed.
type NoOpLogger struct{}

func (l *NoOpLogger) Debug(msg string, fields ...Field) {}
func (l *NoOpLogger) Info(msg string, fields ...Field)  {}
func (l *NoOpLogger) Warn(msg string, fields ...Field)  {}
func (l *NoOpLogger) Error(msg string, fields ...Field) {}

// Ensure NoOpLogger implements Logger
var _ Logger = (*NoOpLogger)(nil)

// OutputResolver resolves field mappings from StandardUnitOutput.
// Used to build input for subsequent units.
type OutputResolver interface {
	// ResolveValue finds a value in the output based on source mapping.
	ResolveValue(output *StandardUnitOutput, sourceNodeId, sourceEndpoint string) (interface{}, error)

	// BuildInputForUnit builds the complete input for a unit based on its field mappings.
	BuildInputForUnit(previousOutput *StandardUnitOutput, unit ExecutionUnit) (map[string]interface{}, error)
}

// Metrics holds processing metrics for observability.
type Metrics struct {
	// TotalItemsProcessed is the count of items processed
	TotalItemsProcessed int64
	// TotalErrors is the count of processing errors
	TotalErrors int64
	// TotalSkipped is the count of skipped nodes
	TotalSkipped int64
	// ProcessingTimeNs is the total processing time in nanoseconds
	ProcessingTimeNs int64
	// ConcurrentWorkers is the number of workers used
	ConcurrentWorkers int
}

// MetricsCollector collects processing metrics.
type MetricsCollector interface {
	// RecordProcessed records a successfully processed item
	RecordProcessed(durationNs int64)
	// RecordError records a processing error
	RecordError()
	// RecordSkipped records a skipped node
	RecordSkipped()
	// GetMetrics returns the current metrics
	GetMetrics() Metrics
	// Reset resets all metrics
	Reset()
}
