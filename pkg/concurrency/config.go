package concurrency

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// ProcessorMode defines how embedded nodes are processed
type ProcessorMode string

const (
	ProcessorModeConcurrent ProcessorMode = "concurrent"
	ProcessorModeSequential ProcessorMode = "sequential"
)

// IteratorMode defines how array iterations are processed
type IteratorMode string

const (
	IteratorModeParallel   IteratorMode = "parallel"
	IteratorModeSequential IteratorMode = "sequential"
)

// ConfigSource indicates where the configuration came from
type ConfigSource string

const (
	ConfigSourceEnvVar     ConfigSource = "environment_variable"
	ConfigSourceAutoDetect ConfigSource = "auto_detect"
	ConfigSourceDefault    ConfigSource = "default"
)

// Config holds concurrency configuration parameters
type Config struct {
	MaxConcurrent int
	RunnerWorkers int
	ProcessorMode ProcessorMode
	IteratorMode  IteratorMode
	Source        ConfigSource
	IsKubernetes  bool
	EffectiveCPUs int
}

// LoadConfig loads concurrency configuration with priority: env vars > auto-detection > defaults
func LoadConfig() *Config {
	config := &Config{}

	// Detect if running in Kubernetes
	config.IsKubernetes = isKubernetes()

	// Get effective CPUs (respects cgroup limits)
	config.EffectiveCPUs = runtime.GOMAXPROCS(0)

	// Load MaxConcurrent with priority
	if maxConcurrent := getEnvInt("ICARUS_MAX_CONCURRENT", 0); maxConcurrent > 0 {
		config.MaxConcurrent = maxConcurrent
		config.Source = ConfigSourceEnvVar
	} else if multiplier := getEnvInt("ICARUS_CONCURRENCY_MULTIPLIER", 0); multiplier > 0 {
		config.MaxConcurrent = config.EffectiveCPUs * multiplier
		config.Source = ConfigSourceEnvVar
	} else {
		// Auto-detect based on environment
		config.MaxConcurrent = getDefaultMaxConcurrent(config.IsKubernetes, config.EffectiveCPUs)
		config.Source = ConfigSourceAutoDetect
	}

	// Ensure minimum value
	if config.MaxConcurrent < 1 {
		config.MaxConcurrent = 1
	}

	// Load RunnerWorkers
	if workers := getEnvInt("ICARUS_RUNNER_WORKERS", 0); workers > 0 {
		config.RunnerWorkers = workers
	} else {
		// Default to a reasonable worker pool size
		config.RunnerWorkers = getDefaultRunnerWorkers(config.IsKubernetes, config.EffectiveCPUs)
	}

	// Load ProcessorMode
	if mode := getEnv("ICARUS_PROCESSOR_MODE", ""); mode != "" {
		config.ProcessorMode = ProcessorMode(strings.ToLower(mode))
	} else {
		config.ProcessorMode = ProcessorModeConcurrent // Default to concurrent
	}

	// Validate ProcessorMode
	if config.ProcessorMode != ProcessorModeConcurrent && config.ProcessorMode != ProcessorModeSequential {
		config.ProcessorMode = ProcessorModeConcurrent
	}

	// Load IteratorMode
	if mode := getEnv("ICARUS_ITERATOR_MODE", ""); mode != "" {
		config.IteratorMode = IteratorMode(strings.ToLower(mode))
	} else {
		// Default to sequential for more predictable behavior
		config.IteratorMode = IteratorModeSequential
	}

	// Validate IteratorMode
	if config.IteratorMode != IteratorModeParallel && config.IteratorMode != IteratorModeSequential {
		config.IteratorMode = IteratorModeSequential
	}

	return config
}

// isKubernetes detects if the application is running in Kubernetes
func isKubernetes() bool {
	// Kubernetes sets this environment variable in all containers
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

// getDefaultMaxConcurrent returns sensible defaults based on environment
func getDefaultMaxConcurrent(isK8s bool, cpus int) int {
	if isK8s {
		// Conservative for Kubernetes to prevent resource exhaustion
		return cpus * 2
	}
	// More aggressive for bare metal
	return cpus * 4
}

// getDefaultRunnerWorkers returns sensible defaults for runner worker pool
func getDefaultRunnerWorkers(isK8s bool, cpus int) int {
	if isK8s {
		// Conservative for Kubernetes
		return max(cpus, 4)
	}
	// More workers for bare metal
	return max(cpus*2, 8)
}

// getEnvInt retrieves an integer from environment variable with default fallback
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// getEnv retrieves a string from environment variable with default fallback
func getEnv(key string, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// String returns a formatted string representation of the config
func (c *Config) String() string {
	return fmt.Sprintf(
		"Config{MaxConcurrent: %d, RunnerWorkers: %d, ProcessorMode: %s, IteratorMode: %s, IsK8s: %t, CPUs: %d, Source: %s}",
		c.MaxConcurrent,
		c.RunnerWorkers,
		c.ProcessorMode,
		c.IteratorMode,
		c.IsKubernetes,
		c.EffectiveCPUs,
		c.Source,
	)
}

// GetOptimalConcurrency calculates optimal concurrency for a given multiplier
func GetOptimalConcurrency(multiplier int) int {
	cpus := runtime.GOMAXPROCS(0)
	if multiplier <= 0 {
		multiplier = 2
	}
	return cpus * multiplier
}
