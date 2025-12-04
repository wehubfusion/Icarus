package runtime

// WorkerPoolConfig configures the worker pool for concurrent processing.
type WorkerPoolConfig struct {
	// NumWorkers is the number of concurrent workers.
	// If 0, it will be determined from the limiter or defaults to runtime.NumCPU().
	NumWorkers int

	// BatchSize is the number of items to group together.
	// Larger batches reduce channel overhead but increase latency.
	// Default: 10
	BatchSize int

	// BufferSize is the channel buffer size.
	// Larger buffers allow more items to be queued but use more memory.
	// Default: 100
	BufferSize int

	// UseLimiter determines if the global limiter should be used for rate limiting.
	// When true, workers will acquire/release from the limiter before processing.
	// Default: true
	UseLimiter bool

	// UseCircuitBreaker determines if circuit breaker should be used.
	// When true, processing failures are recorded to the circuit breaker.
	// Default: true
	UseCircuitBreaker bool
}

// DefaultWorkerPoolConfig returns sensible defaults for the worker pool.
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		NumWorkers:        0,    // Will be determined at runtime
		BatchSize:         10,   // Good balance for most workloads
		BufferSize:        100,  // Allow queuing without blocking
		UseLimiter:        true, // Use global limiter by default
		UseCircuitBreaker: true, // Enable circuit breaker by default
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

// WithLimiter sets whether to use the limiter.
func (c WorkerPoolConfig) WithLimiter(use bool) WorkerPoolConfig {
	c.UseLimiter = use
	return c
}

// WithCircuitBreaker sets whether to use the circuit breaker.
func (c WorkerPoolConfig) WithCircuitBreaker(use bool) WorkerPoolConfig {
	c.UseCircuitBreaker = use
	return c
}

// WithWorkerPool sets the worker pool config.
func (c ProcessorConfig) WithWorkerPool(wp WorkerPoolConfig) ProcessorConfig {
	c.WorkerPool = wp
	return c
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
