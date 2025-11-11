package concurrency

import (
	"log"
	"runtime"

	"go.uber.org/automaxprocs/maxprocs"
)

// InitializeForKubernetes sets up the application for optimal Kubernetes performance
// This should be called at the very start of main() before any other initialization
// Returns an undo function that can be called to restore the original GOMAXPROCS value
func InitializeForKubernetes() func() {
	// Set GOMAXPROCS to match Linux container CPU quota
	// This respects cgroup CPU limits which is crucial for Kubernetes
	undo, err := maxprocs.Set(maxprocs.Logger(log.Printf))
	if err != nil {
		log.Printf("Failed to set maxprocs: %v", err)
		return func() {} // Return no-op cleanup function
	}

	// Log the effective GOMAXPROCS for visibility
	log.Printf("Concurrency initialized: GOMAXPROCS=%d", runtime.GOMAXPROCS(0))

	return undo
}

// GetEffectiveCPUs returns the effective number of CPUs available
// This respects cgroup limits in containerized environments
func GetEffectiveCPUs() int {
	return runtime.GOMAXPROCS(0)
}

// GetOptimalConcurrency calculates optimal concurrency based on CPU availability and multiplier
func GetOptimalConcurrencyForK8s(multiplier int) int {
	cpus := GetEffectiveCPUs()
	if multiplier <= 0 {
		// Conservative default for Kubernetes
		multiplier = 2
	}
	return cpus * multiplier
}
