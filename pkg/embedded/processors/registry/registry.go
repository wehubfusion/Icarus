package registry

import (
	"github.com/wehubfusion/Icarus/pkg/embedded"
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
	jsRunnerExecutor := jsrunner.NewExecutor()
	registry.Register(jsRunnerExecutor)

	// Register JSON operations executor
	jsonOpsExecutor := jsonops.NewExecutor()
	registry.Register(jsonOpsExecutor)

	// Register date formatter executor
	dateFormatterExecutor := dateformatter.NewExecutor()
	registry.Register(dateFormatterExecutor)

	// Register simple condition executor
	registry.Register(simplecondition.NewExecutor())

	return registry
}

// NewProcessor creates a new processor with all built-in executors registered
func NewProcessor() *embedded.Processor {
	return embedded.NewProcessor(NewRegistry())
}
