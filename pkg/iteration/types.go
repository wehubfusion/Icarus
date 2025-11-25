package iteration

import "context"

// Config holds configuration for array iteration
type Config struct {
	MaxConcurrent int // Max concurrent workers (0 = runtime.NumCPU())
}

// ProcessFunc is the function called for each array item
type ProcessFunc func(ctx context.Context, item interface{}, index int) (interface{}, error)
