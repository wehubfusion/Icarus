package processors

import (
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/dateformatter"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// NewProcessorRegistry creates and configures a new processor registry
// with all available processors registered.
func NewProcessorRegistry() runtime.EmbeddedNodeFactory {
	factory := runtime.NewDefaultNodeFactory()

	// Register jsonops processor
	factory.Register("plugin-json-operations", jsonops.NewJsonOpsNode)

	// Register jsrunner processor
	factory.Register("plugin-js", jsrunner.NewJSRunnerNode)

	// Register simplecondition processor
	factory.Register("plugin-simple-condition", simplecondition.NewSimpleConditionNode)

	// Register dateformatter processor
	factory.Register("plugin-date-formatter", dateformatter.NewDateFormatterNode)

	// Register strings processor
	factory.Register("plugin-strings", strings.NewStringsNode)

	// Future processors can be registered here...

	return factory
}
