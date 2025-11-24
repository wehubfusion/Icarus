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

	// Register JavaScript runner executor (with both new and legacy names)
	jsRunnerExecutor := jsrunner.NewExecutor()
	registry.Register(jsRunnerExecutor)
	// Also register with legacy name for backward compatibility
	registry.RegisterWithName(jsRunnerExecutor, "plugin-js")

	// Register JSON operations executor (with both new and legacy names)
	jsonOpsExecutor := jsonops.NewExecutor()
	registry.Register(jsonOpsExecutor)
	// Also register with legacy name for backward compatibility
	registry.RegisterWithName(jsonOpsExecutor, "plugin-json-operations")

	// Register date formatter executor (with both new and legacy names)
	dateFormatterExecutor := dateformatter.NewExecutor()
	registry.Register(dateFormatterExecutor)
	// Also register with legacy name for backward compatibility
	registry.RegisterWithName(dateFormatterExecutor, "plugin-dateformatter")

	// Register simple condition executor
	registry.Register(simplecondition.NewExecutor())

	return registry
}

// NewProcessor creates a new processor with all built-in executors registered
func NewProcessor() *embedded.Processor {
	return embedded.NewProcessor(NewRegistry())
}
