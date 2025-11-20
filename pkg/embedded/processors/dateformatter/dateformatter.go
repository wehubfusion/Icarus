package dateformatter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for date formatting operations
type Executor struct{}

// NewExecutor creates a new date formatter executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a date formatting operation
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var dateConfig Config
	if err := json.Unmarshal(config.Configuration, &dateConfig); err != nil {
		return nil, fmt.Errorf("failed to parse dateformatter configuration: %w", err)
	}

	// Validate configuration
	if err := dateConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid dateformatter configuration: %w", err)
	}

	// Route to appropriate operation handler
	switch dateConfig.Operation {
	case "format":
		return e.executeFormatOperation(config.Input, dateConfig.Params)

	default:
		return nil, fmt.Errorf("unknown operation: %s", dateConfig.Operation)
	}
}

// executeFormatOperation executes the format operation
func (e *Executor) executeFormatOperation(input []byte, paramsRaw json.RawMessage) ([]byte, error) {
	var params FormatParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return nil, fmt.Errorf("failed to parse format params: %w", err)
	}

	// Validate params
	if err := params.Validate(); err != nil {
		return nil, err
	}

	// Execute format operation
	result, err := executeFormat(input, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-date-formatter"
}
