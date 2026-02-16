package runtime

import (
	"os"
	"runtime"
	"strconv"
)

// WorkerPoolConfig configures the worker pool for concurrent embedded node processing.
type WorkerPoolConfig struct {
	// NumWorkers is the number of concurrent workers. Defaults from ICARUS_EMBEDDED_WORKERS
	// or ICARUS_EMBEDDED_WORKER_MULTIPLIER × NumCPU, otherwise NumCPU.
	NumWorkers int
	// BufferSize is the channel buffer size for jobs and results.
	BufferSize int
}

// Validate checks that the worker pool configuration is valid and fills defaults.
func (c *WorkerPoolConfig) Validate() {
	if c.NumWorkers <= 0 {
		c.NumWorkers = resolveWorkerCount()
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 256
	}
}

// ProcessorConfig configures the embedded processor.
type ProcessorConfig struct {
	// WorkerPool configures the worker pool for concurrent processing.
	WorkerPool WorkerPoolConfig
	// Logger is the logger to use. If nil, a no-op logger is used.
	Logger Logger
	// LifecycleEmitter emits lifecycle events (parent.ended, embedded.started). Optional.
	LifecycleEmitter EmbeddedNodeLifecycleEmitter
}

// Validate checks that the processor configuration is valid and fills defaults.
func (c *ProcessorConfig) Validate() {
	c.WorkerPool.Validate()
}

// WithLifecycleEmitter returns a copy of the config with the lifecycle emitter set.
func (c ProcessorConfig) WithLifecycleEmitter(emitter EmbeddedNodeLifecycleEmitter) ProcessorConfig {
	c.LifecycleEmitter = emitter
	return c
}

// DefaultWorkerPoolConfig returns sensible defaults for the worker pool.
// Reads ICARUS_EMBEDDED_WORKERS or ICARUS_EMBEDDED_WORKER_MULTIPLIER from env when applicable.
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		NumWorkers: resolveWorkerCount(),
		BufferSize: 256,
	}
}

// DefaultProcessorConfig returns sensible defaults for the processor.
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		WorkerPool:       DefaultWorkerPoolConfig(),
		Logger:           nil,
		LifecycleEmitter: nil,
	}
}

func resolveWorkerCount() int {
	if v := os.Getenv("ICARUS_EMBEDDED_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	if v := os.Getenv("ICARUS_EMBEDDED_WORKER_MULTIPLIER"); v != "" {
		if m, err := strconv.ParseFloat(v, 64); err == nil && m > 0 {
			n := int(float64(runtime.NumCPU()) * m)
			if n < 1 {
				n = 1
			}
			return n
		}
	}
	n := runtime.NumCPU()
	if n < 1 {
		n = 1
	}
	return n
}
