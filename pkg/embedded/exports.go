// Package embeddedv2 provides the embedded node processing system for Icarus.
//
// This package exports the runtime components for processing embedded nodes
// within execution units. It supports concurrent processing with worker pools
// and integrates with the concurrency package for rate limiting.
//
// See the runtime subpackage for the core implementation.
package embeddedv2

import (
	"github.com/wehubfusion/Icarus/pkg/embedded/processors"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// Re-export core types for convenience
type (
	// EmbeddedNode is the interface all embedded nodes must implement.
	EmbeddedNode = runtime.EmbeddedNode

	// EmbeddedNodeFactory creates embedded nodes from configuration.
	EmbeddedNodeFactory = runtime.EmbeddedNodeFactory

	// NodeCreator is a function that creates an embedded node.
	NodeCreator = runtime.NodeCreator

	// ProcessInput contains all data needed for an embedded node to process.
	ProcessInput = runtime.ProcessInput

	// ProcessOutput contains the result of embedded node processing.
	ProcessOutput = runtime.ProcessOutput

	// StandardUnitOutput represents the flattened output of a unit.
	StandardUnitOutput = runtime.StandardUnitOutput

	// ExecutionUnit represents a group of nodes to be executed together.
	ExecutionUnit = runtime.ExecutionUnit

	// EmbeddedNodeConfig represents the configuration for an embedded node.
	EmbeddedNodeConfig = runtime.EmbeddedNodeConfig

	// FieldMapping represents the mapping configuration between nodes.
	FieldMapping = runtime.FieldMapping

	// NodeConfig contains the detailed configuration for a node.
	NodeConfig = runtime.NodeConfig

	// BaseNode provides common functionality for embedded nodes.
	BaseNode = runtime.BaseNode

	// Logger defines the logging interface for the runtime.
	Logger = runtime.Logger

	// Field represents a key-value pair for structured logging.
	Field = runtime.Field

	// ProcessorConfig configures the embedded processor.
	ProcessorConfig = runtime.ProcessorConfig

	// WorkerPoolConfig configures the worker pool.
	WorkerPoolConfig = runtime.WorkerPoolConfig

	// BatchItem represents a single item to be processed.
	BatchItem = runtime.BatchItem

	// BatchResult represents the result of processing a batch item.
	BatchResult = runtime.BatchResult

	// OutputResolver resolves field mappings from StandardUnitOutput.
	OutputResolver = runtime.OutputResolver

	// Metrics holds processing metrics.
	Metrics = runtime.Metrics

	// MetricsCollector collects processing metrics.
	MetricsCollector = runtime.MetricsCollector

	// ProcessingError wraps an error with additional context.
	ProcessingError = runtime.ProcessingError
)

// Re-export constructors and factory functions
var (
	// NewDefaultNodeFactory creates a new default node factory.
	NewDefaultNodeFactory = runtime.NewDefaultNodeFactory

	// NewProcessorRegistry creates and configures a processor registry with all available processors.
	NewProcessorRegistry = processors.NewProcessorRegistry

	// NewEmbeddedProcessor creates a new embedded processor.
	NewEmbeddedProcessor = runtime.NewEmbeddedProcessor

	// NewEmbeddedProcessorWithDefaults creates a processor with default configuration.
	NewEmbeddedProcessorWithDefaults = runtime.NewEmbeddedProcessorWithDefaults

	// NewSubflowProcessor creates a new subflow processor.
	NewSubflowProcessor = runtime.NewSubflowProcessor

	// NewWorkerPool creates a new worker pool.
	NewWorkerPool = runtime.NewWorkerPool

	// NewOutputResolver creates a new output resolver.
	NewOutputResolver = runtime.NewOutputResolver

	// NewBaseNode creates a new base node from configuration.
	NewBaseNode = runtime.NewBaseNode

	// NewMetricsCollector creates a new metrics collector.
	NewMetricsCollector = runtime.NewMetricsCollector

	// NewProcessingError creates a new processing error.
	NewProcessingError = runtime.NewProcessingError

	// DefaultProcessorConfig returns sensible defaults for the processor.
	DefaultProcessorConfig = runtime.DefaultProcessorConfig

	// DefaultWorkerPoolConfig returns sensible defaults for the worker pool.
	DefaultWorkerPoolConfig = runtime.DefaultWorkerPoolConfig
)

// Re-export utility functions
var (
	// FlattenMap flattens a nested map with nodeId prefix.
	FlattenMap = runtime.FlattenMap

	// FlattenWithIndex flattens a map with index notation for array items.
	FlattenWithIndex = runtime.FlattenWithIndex

	// GenerateOutputKey creates a standardized key for the flat output.
	GenerateOutputKey = runtime.GenerateOutputKey

	// BuildIndexedKey creates a key with index notation.
	BuildIndexedKey = runtime.BuildIndexedKey

	// ParseIndexedKey parses a key to extract nodeId, path, and indices.
	ParseIndexedKey = runtime.ParseIndexedKey

	// GetNestedValue gets a value from a nested map.
	GetNestedValue = runtime.GetNestedValue

	// SetNestedValue sets a value at a nested path.
	SetNestedValue = runtime.SetNestedValue

	// ExtractArrayPath extracts the array path from a source endpoint.
	ExtractArrayPath = runtime.ExtractArrayPath

	// ExtractFieldFromDestination extracts the field name from destination.
	ExtractFieldFromDestination = runtime.ExtractFieldFromDestination

	// MergeMaps merges source into target.
	MergeMaps = runtime.MergeMaps

	// CloneMap creates a shallow copy of a map.
	CloneMap = runtime.CloneMap

	// DeepCloneMap creates a deep copy of a map.
	DeepCloneMap = runtime.DeepCloneMap

	// CreateBatchItems converts raw map items to BatchItems.
	CreateBatchItems = runtime.CreateBatchItems

	// CreateBatches groups items into batches.
	CreateBatches = runtime.CreateBatches

	// CollectResults collects results from a channel.
	CollectResults = runtime.CollectResults

	// CollectResultsAll collects all results including errors.
	CollectResultsAll = runtime.CollectResultsAll
)

// Re-export output helpers
var (
	// SuccessOutput creates a successful ProcessOutput.
	SuccessOutput = runtime.SuccessOutput

	// ErrorOutput creates a failed ProcessOutput.
	ErrorOutput = runtime.ErrorOutput

	// SkippedOutput creates a skipped ProcessOutput.
	SkippedOutput = runtime.SkippedOutput
)

// Re-export error utilities
var (
	// IsRetryableError determines if an error is retryable.
	IsRetryableError = runtime.IsRetryableError

	// IsPermanentError determines if an error is permanent.
	IsPermanentError = runtime.IsPermanentError
)

// Re-export common errors
var (
	ErrNoExecutor          = runtime.ErrNoExecutor
	ErrInvalidInput        = runtime.ErrInvalidInput
	ErrInvalidConfig       = runtime.ErrInvalidConfig
	ErrProcessingFailed    = runtime.ErrProcessingFailed
	ErrArrayNotFound       = runtime.ErrArrayNotFound
	ErrNotAnArray          = runtime.ErrNotAnArray
	ErrKeyNotFound         = runtime.ErrKeyNotFound
	ErrContextCancelled    = runtime.ErrContextCancelled
	ErrCircuitOpen         = runtime.ErrCircuitOpen
	ErrSourceNodeNotFound  = runtime.ErrSourceNodeNotFound
	ErrInvalidFieldMapping = runtime.ErrInvalidFieldMapping
)
