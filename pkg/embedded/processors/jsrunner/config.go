package jsrunner

import (
	"encoding/json"
	"time"
)

// Config defines the configuration for JavaScript execution
type Config struct {
	// Script is the JavaScript code to execute
	Script string `json:"script"`

	// SchemaDefinition is the input schema (injected by Elysium enrichment)
	// When provided, the schema is available in JS as a global variable
	SchemaDefinition json.RawMessage `json:"schema,omitempty"`

	// SchemaID is a reference to a schema in Morpheus (enriched by Elysium)
	// This is only used as a reference - Elysium enriches it before execution
	SchemaID string `json:"schema_id,omitempty"`

	// Timeout is the maximum execution time in milliseconds
	// Default: 5000ms (5 seconds)
	Timeout int `json:"timeout,omitempty"`

	// EnabledUtilities specifies which utilities are available in the script
	// Available: "console", "json", "timers", "encoding", "fetch"
	// Default: ["console", "json"]
	EnabledUtilities []string `json:"enabled_utilities,omitempty"`

	// SecurityLevel determines the level of sandboxing
	// "strict": Maximum restrictions, minimal utilities
	// "standard": Balanced security and functionality (default)
	// "permissive": More utilities, less restrictions
	SecurityLevel string `json:"security_level,omitempty"`

	// MemoryLimitMB sets the memory limit for the VM (not enforced by goja itself)
	// This is informational and can be used for monitoring
	// Default: 50MB
	MemoryLimitMB int `json:"memory_limit_mb,omitempty"`

	// MaxStackDepth limits the call stack depth
	// Default: 100
	MaxStackDepth int `json:"max_stack_depth,omitempty"`

	// AllowNetworkAccess enables network utilities (fetch)
	// Only applies in "permissive" security level
	// Default: false
	AllowNetworkAccess bool `json:"allow_network_access,omitempty"`
}

// SecurityLevel constants
const (
	SecurityLevelStrict     = "strict"
	SecurityLevelStandard   = "standard"
	SecurityLevelPermissive = "permissive"
)

// Default configuration values
const (
	DefaultTimeout       = 5000 // 5 seconds
	DefaultMemoryLimitMB = 50   // 50 MB
	DefaultMaxStackDepth = 100  // 100 calls
	DefaultSecurityLevel = SecurityLevelStandard
)

// Default utilities per security level
var DefaultUtilitiesByLevel = map[string][]string{
	SecurityLevelStrict:     {"json"},
	SecurityLevelStandard:   {"console", "json", "encoding"},
	SecurityLevelPermissive: {"console", "json", "encoding", "timers"},
}

// ApplyDefaults applies default values to the configuration
func (c *Config) ApplyDefaults() {
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}

	if c.MemoryLimitMB <= 0 {
		c.MemoryLimitMB = DefaultMemoryLimitMB
	}

	if c.MaxStackDepth <= 0 {
		c.MaxStackDepth = DefaultMaxStackDepth
	}

	if c.SecurityLevel == "" {
		c.SecurityLevel = DefaultSecurityLevel
	}

	if c.EnabledUtilities == nil {
		c.EnabledUtilities = DefaultUtilitiesByLevel[c.SecurityLevel]
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Script == "" {
		return &ConfigError{Field: "script", Message: "script cannot be empty"}
	}

	if c.Timeout < 0 {
		return &ConfigError{Field: "timeout", Message: "timeout must be non-negative"}
	}

	if c.Timeout > 300000 { // 5 minutes
		return &ConfigError{Field: "timeout", Message: "timeout cannot exceed 300000ms (5 minutes)"}
	}

	if c.MemoryLimitMB < 0 {
		return &ConfigError{Field: "memory_limit_mb", Message: "memory limit must be non-negative"}
	}

	if c.MaxStackDepth < 0 {
		return &ConfigError{Field: "max_stack_depth", Message: "max stack depth must be non-negative"}
	}

	validSecurityLevels := map[string]bool{
		SecurityLevelStrict:     true,
		SecurityLevelStandard:   true,
		SecurityLevelPermissive: true,
	}

	if !validSecurityLevels[c.SecurityLevel] {
		return &ConfigError{
			Field:   "security_level",
			Message: "security level must be 'strict', 'standard', or 'permissive'",
		}
	}

	return nil
}

// GetTimeoutDuration returns the timeout as a time.Duration
func (c *Config) GetTimeoutDuration() time.Duration {
	return time.Duration(c.Timeout) * time.Millisecond
}

// IsUtilityEnabled checks if a utility is enabled in the configuration
func (c *Config) IsUtilityEnabled(utilityName string) bool {
	for _, name := range c.EnabledUtilities {
		if name == utilityName {
			return true
		}
	}
	return false
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "config error [" + e.Field + "]: " + e.Message
}
