package runtime

import (
	"encoding/json"
)

// BaseNode provides common functionality for embedded nodes.
// Embed this in your custom node implementations.
type BaseNode struct {
	nodeId     string
	pluginType string
	label      string
	config     map[string]interface{}
	rawConfig  json.RawMessage
}

// NewBaseNode creates a new base node from configuration.
func NewBaseNode(config EmbeddedNodeConfig) BaseNode {
	var parsedConfig map[string]interface{}
	if len(config.NodeConfig.Config) > 0 {
		_ = json.Unmarshal(config.NodeConfig.Config, &parsedConfig)
	}
	if parsedConfig == nil {
		parsedConfig = make(map[string]interface{})
	}

	return BaseNode{
		nodeId:     config.NodeId,
		pluginType: config.PluginType,
		label:      config.Label,
		config:     parsedConfig,
		rawConfig:  config.NodeConfig.Config,
	}
}

// NodeId returns the node ID.
func (n *BaseNode) NodeId() string {
	return n.nodeId
}

// PluginType returns the plugin type.
func (n *BaseNode) PluginType() string {
	return n.pluginType
}

// Label returns the node label.
func (n *BaseNode) Label() string {
	return n.label
}

// Config returns the parsed configuration map.
func (n *BaseNode) Config() map[string]interface{} {
	return n.config
}

// RawConfig returns the raw JSON configuration.
func (n *BaseNode) RawConfig() json.RawMessage {
	return n.rawConfig
}

// GetConfig returns a config value by key.
func (n *BaseNode) GetConfig(key string) interface{} {
	return n.config[key]
}

// GetConfigString returns a config value as string.
func (n *BaseNode) GetConfigString(key string) string {
	if v, ok := n.config[key].(string); ok {
		return v
	}
	return ""
}

// GetConfigStringWithDefault returns a config value as string with default.
func (n *BaseNode) GetConfigStringWithDefault(key, defaultVal string) string {
	if v, ok := n.config[key].(string); ok && v != "" {
		return v
	}
	return defaultVal
}

// GetConfigBool returns a config value as bool.
func (n *BaseNode) GetConfigBool(key string) bool {
	if v, ok := n.config[key].(bool); ok {
		return v
	}
	return false
}

// GetConfigBoolWithDefault returns a config value as bool with default.
func (n *BaseNode) GetConfigBoolWithDefault(key string, defaultVal bool) bool {
	if v, ok := n.config[key].(bool); ok {
		return v
	}
	return defaultVal
}

// GetConfigInt returns a config value as int.
func (n *BaseNode) GetConfigInt(key string) int {
	switch v := n.config[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	case int32:
		return int(v)
	}
	return 0
}

// GetConfigIntWithDefault returns a config value as int with default.
func (n *BaseNode) GetConfigIntWithDefault(key string, defaultVal int) int {
	switch v := n.config[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	case int32:
		return int(v)
	}
	return defaultVal
}

// GetConfigFloat returns a config value as float64.
func (n *BaseNode) GetConfigFloat(key string) float64 {
	switch v := n.config[key].(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return 0
}

// GetConfigFloatWithDefault returns a config value as float64 with default.
func (n *BaseNode) GetConfigFloatWithDefault(key string, defaultVal float64) float64 {
	switch v := n.config[key].(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	}
	return defaultVal
}

// GetConfigMap returns a config value as map.
func (n *BaseNode) GetConfigMap(key string) map[string]interface{} {
	if v, ok := n.config[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

// GetConfigSlice returns a config value as slice.
func (n *BaseNode) GetConfigSlice(key string) []interface{} {
	if v, ok := n.config[key].([]interface{}); ok {
		return v
	}
	return nil
}

// GetConfigStringSlice returns a config value as string slice.
func (n *BaseNode) GetConfigStringSlice(key string) []string {
	slice := n.GetConfigSlice(key)
	if slice == nil {
		return nil
	}
	result := make([]string, 0, len(slice))
	for _, v := range slice {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

// HasConfig checks if a config key exists.
func (n *BaseNode) HasConfig(key string) bool {
	_, ok := n.config[key]
	return ok
}

// SuccessOutput creates a successful ProcessOutput with the given data.
func SuccessOutput(data map[string]interface{}) ProcessOutput {
	return ProcessOutput{
		Data:    data,
		Error:   nil,
		Skipped: false,
	}
}

// ErrorOutput creates a failed ProcessOutput with the given error.
func ErrorOutput(err error) ProcessOutput {
	return ProcessOutput{
		Data:    nil,
		Error:   err,
		Skipped: false,
	}
}

// SkippedOutput creates a skipped ProcessOutput with the given reason.
func SkippedOutput(reason string) ProcessOutput {
	return ProcessOutput{
		Data:       nil,
		Error:      nil,
		Skipped:    true,
		SkipReason: reason,
	}
}
