package jsonops

import (
	"encoding/json"
	"fmt"
)

// Config defines the configuration for JSON operations
type Config struct {
	// Operation specifies which JSON operation to perform
	// Valid values: "parse", "render", "query", "transform", "validate"
	Operation string `json:"operation"`

	// Params contains operation-specific parameters
	Params json.RawMessage `json:"params"`
}

// ParseParams defines parameters for parse operation
type ParseParams struct {
	// Paths is a map of output field names to JSON paths to extract
	// Example: {"userName": "data.user.name", "userId": "data.user.id"}
	Paths map[string]string `json:"paths"`

	// FailOnMissing determines whether to fail if a path doesn't exist
	// Default: false (returns null for missing paths)
	FailOnMissing bool `json:"fail_on_missing,omitempty"`
}

// RenderParams defines parameters for render operation
type RenderParams struct {
	// Template defines the structure to build
	// Each field can reference input values using {{path}} syntax
	Template map[string]interface{} `json:"template"`

	// FailOnMissing determines whether to fail if a referenced path doesn't exist
	// Default: false (uses null for missing values)
	FailOnMissing bool `json:"fail_on_missing,omitempty"`
}

// QueryParams defines parameters for query operation
type QueryParams struct {
	// Path is the JSON path to query (supports wildcards)
	// Example: "items.*.price", "data.users[0].name"
	Path string `json:"path"`

	// Multiple allows querying multiple paths at once
	Multiple bool `json:"multiple,omitempty"`

	// Paths is used when Multiple is true
	Paths []string `json:"paths,omitempty"`
}

// TransformParams defines parameters for transform operations
type TransformParams struct {
	// Type specifies the transformation type: "set", "delete", "merge"
	Type string `json:"type"`

	// Path is the JSON path to modify (for set/delete operations)
	Path string `json:"path,omitempty"`

	// Value is the value to set (for set operation)
	Value interface{} `json:"value,omitempty"`

	// Paths is a list of paths to delete (for delete operation with multiple paths)
	Paths []string `json:"paths,omitempty"`

	// MergeData is the JSON object to merge (for merge operation)
	MergeData json.RawMessage `json:"merge_data,omitempty"`

	// DeepMerge specifies whether to perform deep merge (default: true)
	DeepMerge *bool `json:"deep_merge,omitempty"`
}

// ValidateParams defines parameters for JSON Schema validation
type ValidateParams struct {
	// Schema is the JSON Schema to validate against
	Schema json.RawMessage `json:"schema"`

	// Draft specifies the JSON Schema draft version
	// Default: "2020-12" (latest)
	// Supported: "draft-04", "draft-06", "draft-07", "2019-09", "2020-12"
	Draft string `json:"draft,omitempty"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Operation == "" {
		return &ConfigError{Field: "operation", Message: "operation cannot be empty"}
	}

	validOperations := map[string]bool{
		"parse":     true,
		"render":    true,
		"query":     true,
		"transform": true,
		"validate":  true,
	}

	if !validOperations[c.Operation] {
		return &ConfigError{
			Field:   "operation",
			Message: fmt.Sprintf("invalid operation '%s', must be one of: parse, render, query, transform, validate", c.Operation),
		}
	}

	// Validate operation-specific params
	switch c.Operation {
	case "parse":
		var params ParseParams
		if err := json.Unmarshal(c.Params, &params); err != nil {
			return &ConfigError{Field: "params", Message: fmt.Sprintf("invalid parse params: %v", err)}
		}
		return validateParseParams(&params)

	case "render":
		var params RenderParams
		if err := json.Unmarshal(c.Params, &params); err != nil {
			return &ConfigError{Field: "params", Message: fmt.Sprintf("invalid render params: %v", err)}
		}
		return validateRenderParams(&params)

	case "query":
		var params QueryParams
		if err := json.Unmarshal(c.Params, &params); err != nil {
			return &ConfigError{Field: "params", Message: fmt.Sprintf("invalid query params: %v", err)}
		}
		return validateQueryParams(&params)

	case "transform":
		var params TransformParams
		if err := json.Unmarshal(c.Params, &params); err != nil {
			return &ConfigError{Field: "params", Message: fmt.Sprintf("invalid transform params: %v", err)}
		}
		return validateTransformParams(&params)

	case "validate":
		var params ValidateParams
		if err := json.Unmarshal(c.Params, &params); err != nil {
			return &ConfigError{Field: "params", Message: fmt.Sprintf("invalid validate params: %v", err)}
		}
		return validateValidateParams(&params)
	}

	return nil
}

// validateParseParams validates parse operation parameters
func validateParseParams(params *ParseParams) error {
	if len(params.Paths) == 0 {
		return &ConfigError{Field: "params.paths", Message: "at least one path must be specified"}
	}

	for key, path := range params.Paths {
		if path == "" {
			return &ConfigError{Field: "params.paths", Message: fmt.Sprintf("path for key '%s' cannot be empty", key)}
		}
	}

	return nil
}

// validateRenderParams validates render operation parameters
func validateRenderParams(params *RenderParams) error {
	if len(params.Template) == 0 {
		return &ConfigError{Field: "params.template", Message: "template cannot be empty"}
	}
	return nil
}

// validateQueryParams validates query operation parameters
func validateQueryParams(params *QueryParams) error {
	if params.Multiple {
		if len(params.Paths) == 0 {
			return &ConfigError{Field: "params.paths", Message: "paths must be specified when multiple is true"}
		}
		for i, path := range params.Paths {
			if path == "" {
				return &ConfigError{Field: "params.paths", Message: fmt.Sprintf("path at index %d cannot be empty", i)}
			}
		}
	} else {
		if params.Path == "" {
			return &ConfigError{Field: "params.path", Message: "path cannot be empty"}
		}
	}
	return nil
}

// validateTransformParams validates transform operation parameters
func validateTransformParams(params *TransformParams) error {
	validTypes := map[string]bool{
		"set":    true,
		"delete": true,
		"merge":  true,
	}

	if !validTypes[params.Type] {
		return &ConfigError{
			Field:   "params.type",
			Message: fmt.Sprintf("invalid transform type '%s', must be one of: set, delete, merge", params.Type),
		}
	}

	switch params.Type {
	case "set":
		if params.Path == "" {
			return &ConfigError{Field: "params.path", Message: "path is required for set operation"}
		}
		if params.Value == nil {
			return &ConfigError{Field: "params.value", Message: "value is required for set operation"}
		}

	case "delete":
		if params.Path == "" && len(params.Paths) == 0 {
			return &ConfigError{Field: "params.path", Message: "path or paths is required for delete operation"}
		}

	case "merge":
		if len(params.MergeData) == 0 {
			return &ConfigError{Field: "params.merge_data", Message: "merge_data is required for merge operation"}
		}
	}

	return nil
}

// validateValidateParams validates JSON Schema validation parameters
func validateValidateParams(params *ValidateParams) error {
	if len(params.Schema) == 0 {
		return &ConfigError{Field: "params.schema", Message: "schema is required"}
	}

	// Validate that schema is valid JSON
	var schemaObj interface{}
	if err := json.Unmarshal(params.Schema, &schemaObj); err != nil {
		return &ConfigError{Field: "params.schema", Message: fmt.Sprintf("schema must be valid JSON: %v", err)}
	}

	if params.Draft != "" {
		validDrafts := map[string]bool{
			"draft-04": true,
			"draft-06": true,
			"draft-07": true,
			"2019-09":  true,
			"2020-12":  true,
		}
		if !validDrafts[params.Draft] {
			return &ConfigError{
				Field:   "params.draft",
				Message: fmt.Sprintf("invalid draft version '%s'", params.Draft),
			}
		}
	}

	return nil
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config error [%s]: %s", e.Field, e.Message)
}
