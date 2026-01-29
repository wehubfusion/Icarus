package runtime

import (
	"os"
	"runtime"
	"strconv"
)

// WorkerPoolConfig configures the worker pool for concurrent processing.
type WorkerPoolConfig struct {
	// NumWorkers is the number of concurrent workers.
	// If 0, it will be determined from env overrides or defaults to GOMAXPROCS.
	NumWorkers int

	// BatchSize is the number of items to group together.
	// Larger batches reduce channel overhead but increase latency.
	// Default: 10
	BatchSize int

	// BufferSize is the channel buffer size.
	// Larger buffers allow more items to be queued but use more memory.
	// Default: 100
	BufferSize int
}

// DefaultWorkerPoolConfig returns sensible defaults for the worker pool.
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		NumWorkers: 0,    // Will be determined at runtime
		BatchSize:  10,   // Good balance for most workloads
		BufferSize: 1000, // Large buffer for high-volume processing (10k+ items)
	}
}

// ProcessorConfig configures the embedded processor.
type ProcessorConfig struct {
	// WorkerPool configures concurrent processing
	WorkerPool WorkerPoolConfig

	// EnableMetrics enables metrics collection
	EnableMetrics bool

	// Logger for structured logging (nil for no logging)
	Logger Logger

	// StopOnFirstError stops processing on first error
	// When false, continues processing remaining items and collects all errors
	StopOnFirstError bool

	// MaxRetries is the maximum number of retries for retryable errors
	MaxRetries int

	// SkipOnEventTriggerFalse determines behavior when event trigger is false
	// When true, the node is skipped; when false, an error is returned
	SkipOnEventTriggerFalse bool
}

// DefaultProcessorConfig returns sensible defaults for the processor.
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		WorkerPool:              DefaultWorkerPoolConfig(),
		EnableMetrics:           true,
		Logger:                  nil, // No logging by default
		StopOnFirstError:        false,
		MaxRetries:              0, // No retries by default
		SkipOnEventTriggerFalse: true,
	}
}

// Validate validates the configuration and applies defaults.
func (c *WorkerPoolConfig) Validate() {
	if c.BatchSize <= 0 {
		c.BatchSize = 10
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 100
	}
	c.NumWorkers = resolveWorkerCount(c.NumWorkers)
}

// Validate validates the configuration and applies defaults.
func (c *ProcessorConfig) Validate() {
	c.WorkerPool.Validate()
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
}

// WithNumWorkers sets the number of workers.
func (c WorkerPoolConfig) WithNumWorkers(n int) WorkerPoolConfig {
	c.NumWorkers = n
	return c
}

// WithBatchSize sets the batch size.
func (c WorkerPoolConfig) WithBatchSize(n int) WorkerPoolConfig {
	c.BatchSize = n
	return c
}

// WithBufferSize sets the buffer size.
func (c WorkerPoolConfig) WithBufferSize(n int) WorkerPoolConfig {
	c.BufferSize = n
	return c
}

// WithWorkerPool sets the worker pool config.
func (c ProcessorConfig) WithWorkerPool(wp WorkerPoolConfig) ProcessorConfig {
	c.WorkerPool = wp
	return c
}

// resolveWorkerCount chooses the effective worker count using:
// 1) Explicit configuration
// 2) Environment overrides
// 3) Runtime CPU count fallback
func resolveWorkerCount(configured int) int {
	if configured > 0 {
		return configured
	}

	// Environment overrides (direct or multiplier)
	if v := getEnvInt("ICARUS_EMBEDDED_WORKERS", 0); v > 0 {
		return v
	}
	if mult := getEnvInt("ICARUS_EMBEDDED_WORKER_MULTIPLIER", 0); mult > 0 {
		workers := runtime.GOMAXPROCS(0) * mult
		if workers > 0 {
			return workers
		}
	}

	// Fallback to CPU count (respecting cgroups)
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}
	return workers
}

// getEnvInt retrieves an integer from environment variable with default fallback.
func getEnvInt(key string, defaultValue int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue
	}
	return value
}

// WithMetrics sets whether to enable metrics.
func (c ProcessorConfig) WithMetrics(enable bool) ProcessorConfig {
	c.EnableMetrics = enable
	return c
}

// WithLogger sets the logger.
func (c ProcessorConfig) WithLogger(logger Logger) ProcessorConfig {
	c.Logger = logger
	return c
}

// WithStopOnFirstError sets whether to stop on first error.
func (c ProcessorConfig) WithStopOnFirstError(stop bool) ProcessorConfig {
	c.StopOnFirstError = stop
	return c
}

// WithMaxRetries sets the maximum number of retries.
func (c ProcessorConfig) WithMaxRetries(n int) ProcessorConfig {
	c.MaxRetries = n
	return c
}
