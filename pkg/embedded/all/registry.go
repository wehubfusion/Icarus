package all

import (
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/constantvalue"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/dateformatter"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
)

// NewRegistry creates a new executor registry with all built-in executors registered
func NewRegistry() *embedded.ExecutorRegistry {
	registry := embedded.NewExecutorRegistry()

	// Register strings executor
	registry.Register(strings.NewExecutor())

	// Register JavaScript runner executor
	registry.Register(jsrunner.NewExecutor())

	// Register JSON operations executor
	registry.Register(jsonops.NewExecutor())

	// Register date formatter executor
	registry.Register(dateformatter.NewExecutor())

	// Register simple condition executor
	registry.Register(simplecondition.NewExecutor())

	// Register constant value executor
	registry.Register(constantvalue.NewExecutor())

	return registry
}

// NewProcessor creates a new processor with all built-in executors registered
func NewProcessor() *embedded.Processor {
	return embedded.NewProcessor(NewRegistry())
}
