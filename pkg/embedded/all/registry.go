package all

import (
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
)

// NewRegistry creates a new executor registry with all built-in executors registered
func NewRegistry() *embedded.ExecutorRegistry {
	registry := embedded.NewExecutorRegistry()

	// Register strings executor
	registry.Register(strings.NewExecutor())

	return registry
}

// NewProcessor creates a new processor with all built-in executors registered
func NewProcessor() *embedded.Processor {
	return embedded.NewProcessor(NewRegistry())
}
