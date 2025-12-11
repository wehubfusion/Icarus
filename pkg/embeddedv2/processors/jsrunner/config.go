package jsrunner

import (
	"encoding/json"
	"fmt"
	"time"
)

// SecurityLevel defines the security restrictions for JavaScript execution
const (
	SecurityLevelStrict     = "strict"
	SecurityLevelStandard   = "standard"
	SecurityLevelPermissive = "permissive"
)

// Config represents the configuration for a JavaScript execution node
type Config struct {
	// Script is the JavaScript code to execute
	Script string `json:"script"`

	// SchemaID is the ID of the output schema from the schema engine
	SchemaID string `json:"schema_id,omitempty"`

	// Schema is the inline JSON schema for output validation
	Schema map[string]interface{} `json:"schema,omitempty"`

	// ManualInputs allows manually specifying inputs instead of using ProcessInput.Data
	ManualInputs map[string]interface{} `json:"manual_inputs,omitempty"`

	// Timeout is the maximum execution time for the script
	Timeout time.Duration `json:"timeout,omitempty"`

	// SecurityLevel defines security restrictions (strict, standard, permissive)
	SecurityLevel string `json:"security_level,omitempty"`

	// EnabledUtilities is a list of utility modules to enable (console, json, encoding, timers)
	EnabledUtilities []string `json:"enabled_utilities,omitempty"`

	// ApplySchemaDefaults specifies whether to apply schema defaults
	ApplySchemaDefaults bool `json:"apply_defaults,omitempty"`

	// StructureData specifies whether to structure data according to schema
	StructureData bool `json:"structure_data,omitempty"`

	// StrictValidation specifies whether to use strict schema validation
	StrictValidation bool `json:"strict_validation,omitempty"`

	// MaxStackDepth is the maximum call stack depth
	MaxStackDepth int `json:"max_stack_depth,omitempty"`
}

// ApplyDefaults sets default values for configuration fields
func (c *Config) ApplyDefaults() {
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.SecurityLevel == "" {
		c.SecurityLevel = SecurityLevelStandard
	}
	if c.EnabledUtilities == nil {
		c.EnabledUtilities = DefaultUtilitiesByLevel[c.SecurityLevel]
	}
	if c.MaxStackDepth == 0 {
		c.MaxStackDepth = 100
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Script == "" {
		return fmt.Errorf("script is required")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if c.SecurityLevel != SecurityLevelStrict &&
		c.SecurityLevel != SecurityLevelStandard &&
		c.SecurityLevel != SecurityLevelPermissive {
		return fmt.Errorf("invalid security level: %s", c.SecurityLevel)
	}
	if c.MaxStackDepth <= 0 {
		return fmt.Errorf("max_stack_depth must be positive")
	}
	return nil
}

// HasOutputSchema returns true if an output schema is configured
func (c *Config) HasOutputSchema() bool {
	return c.SchemaID != "" || len(c.Schema) > 0
}

// DefaultUtilitiesByLevel defines default utilities for each security level
var DefaultUtilitiesByLevel = map[string][]string{
	SecurityLevelStrict:     {"console", "json"},
	SecurityLevelStandard:   {"console", "json", "encoding"},
	SecurityLevelPermissive: {"console", "json", "encoding", "timers"},
}

// UnmarshalJSON implements custom JSON unmarshaling for Config
func (c *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	aux := &struct {
		Timeout string `json:"timeout,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse timeout if provided as string
	if aux.Timeout != "" {
		duration, err := time.ParseDuration(aux.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout format: %w", err)
		}
		c.Timeout = duration
	}

	return nil
}
