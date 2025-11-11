package jsonops

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/embedded"
)

// Executor implements NodeExecutor for JSON operations
type Executor struct{}

// NewExecutor creates a new JSON operations executor
func NewExecutor() *Executor {
	return &Executor{}
}

// Execute executes a JSON operation
func (e *Executor) Execute(ctx context.Context, config embedded.NodeConfig) ([]byte, error) {
	// Parse configuration
	var jsonConfig Config
	if err := json.Unmarshal(config.Configuration, &jsonConfig); err != nil {
		return nil, fmt.Errorf("failed to parse jsonops configuration: %w", err)
	}

	// Validate configuration
	if err := jsonConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid jsonops configuration: %w", err)
	}

	// Route to appropriate operation handler
	switch jsonConfig.Operation {
	case "parse":
		return e.executeParse(config.Input, jsonConfig.Params)

	case "render":
		return e.executeRender(config.Input, jsonConfig.Params)

	case "query":
		return e.executeQuery(config.Input, jsonConfig.Params)

	case "transform":
		return e.executeTransform(config.Input, jsonConfig.Params)

	case "validate":
		return e.executeValidate(config.Input, jsonConfig.Params)

	default:
		return nil, fmt.Errorf("unknown operation: %s", jsonConfig.Operation)
	}
}

// executeParse executes parse operation
func (e *Executor) executeParse(input []byte, paramsRaw json.RawMessage) ([]byte, error) {
	var params ParseParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return nil, fmt.Errorf("failed to parse params: %w", err)
	}

	result, err := parseToJSON(input, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// executeRender executes render operation
func (e *Executor) executeRender(input []byte, paramsRaw json.RawMessage) ([]byte, error) {
	var params RenderParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return nil, fmt.Errorf("failed to parse params: %w", err)
	}

	result, err := renderToJSON(input, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// executeQuery executes query operation
func (e *Executor) executeQuery(input []byte, paramsRaw json.RawMessage) ([]byte, error) {
	var params QueryParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return nil, fmt.Errorf("failed to parse params: %w", err)
	}

	result, err := queryToJSON(input, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// executeTransform executes transform operation
func (e *Executor) executeTransform(input []byte, paramsRaw json.RawMessage) ([]byte, error) {
	var params TransformParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return nil, fmt.Errorf("failed to parse params: %w", err)
	}

	result, err := transform(input, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// executeValidate executes validate operation
func (e *Executor) executeValidate(input []byte, paramsRaw json.RawMessage) ([]byte, error) {
	var params ValidateParams
	if err := json.Unmarshal(paramsRaw, &params); err != nil {
		return nil, fmt.Errorf("failed to parse params: %w", err)
	}

	result, err := validateToJSON(input, params)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// PluginType returns the plugin type this executor handles
func (e *Executor) PluginType() string {
	return "plugin-jsonops"
}
