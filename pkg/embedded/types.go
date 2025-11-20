package embedded

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// Logger defines a simple logging interface to avoid external dependencies
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// Field represents a key-value pair for structured logging
type Field struct {
	Key   string
	Value interface{}
}

// NoOpLogger is a logger that does nothing (used when no logger is provided)
type NoOpLogger struct{}

func (n *NoOpLogger) Debug(msg string, fields ...Field) {}
func (n *NoOpLogger) Info(msg string, fields ...Field)  {}
func (n *NoOpLogger) Warn(msg string, fields ...Field)  {}
func (n *NoOpLogger) Error(msg string, fields ...Field) {}

// NodeConfig contains configuration for executing a single embedded node
type NodeConfig struct {
	NodeID        string
	PluginType    string
	Configuration json.RawMessage
	Input         []byte
	Connection    *message.ConnectionDetails
	Schema        *message.SchemaDetails
}

// EmbeddedNodeResult represents the result of an embedded node execution
type EmbeddedNodeResult struct {
	NodeID               string          `json:"node_id"`
	PluginType           string          `json:"plugin_type"`
	Status               string          `json:"status"` // "success", "failed"
	Output               *StandardOutput `json:"output,omitempty"`
	Error                string          `json:"error,omitempty"`
	ExecutionOrder       int             `json:"execution_order"`
	ProcessingDurationMs int64           `json:"processing_duration_ms"`
}

// NodeExecutor defines the interface for executing embedded nodes
type NodeExecutor interface {
	// Execute executes a node with the given configuration
	Execute(ctx context.Context, config NodeConfig) ([]byte, error)

	// PluginType returns the plugin type this executor handles
	PluginType() string
}

// ExecutorRegistry manages executors for different plugin types
type ExecutorRegistry struct {
	executors map[string]NodeExecutor
}

// NewExecutorRegistry creates a new executor registry
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{
		executors: make(map[string]NodeExecutor),
	}
}

// Register registers a node executor for a specific plugin type
func (r *ExecutorRegistry) Register(executor NodeExecutor) {
	pluginType := executor.PluginType()
	r.executors[pluginType] = executor
}

// RegisterWithName registers a node executor with a specific plugin type name
// This is useful for backward compatibility when plugin type names change
func (r *ExecutorRegistry) RegisterWithName(executor NodeExecutor, pluginType string) {
	r.executors[pluginType] = executor
}

// Execute executes a node using the appropriate executor for its plugin type
func (r *ExecutorRegistry) Execute(ctx context.Context, config NodeConfig) ([]byte, error) {
	executor, ok := r.executors[config.PluginType]
	if !ok {
		return nil, fmt.Errorf("no executor registered for plugin type: %s", config.PluginType)
	}

	return executor.Execute(ctx, config)
}

// HasExecutor checks if an executor exists for a plugin type
func (r *ExecutorRegistry) HasExecutor(pluginType string) bool {
	_, ok := r.executors[pluginType]
	return ok
}

// RegisteredTypes returns all registered plugin types
func (r *ExecutorRegistry) RegisteredTypes() []string {
	types := make([]string, 0, len(r.executors))
	for pluginType := range r.executors {
		types = append(types, pluginType)
	}
	return types
}
