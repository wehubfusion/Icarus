package iteration

import "context"

// Strategy defines how array items are processed
type Strategy string

const (
	StrategySequential Strategy = "sequential" // Process items one by one
	StrategyParallel   Strategy = "parallel"   // Process items concurrently
)

// Config holds configuration for array iteration
type Config struct {
	Strategy      Strategy // sequential or parallel
	MaxConcurrent int      // Max concurrent workers (0 = runtime.NumCPU())
}

// ProcessFunc is the function called for each array item
type ProcessFunc func(ctx context.Context, item interface{}, index int) (interface{}, error)
