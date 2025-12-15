package schema

import "encoding/json"

// Schema represents a complete schema definition
type Schema struct {
	Type        SchemaType           `json:"type"`
	Properties  map[string]*Property `json:"properties,omitempty"`
	Items       *Property            `json:"items,omitempty"`
	Description string               `json:"description,omitempty"`
}

// CSVSchema represents a typed CSV schema definition
type CSVSchema struct {
	Name          string                `json:"name,omitempty"`
	Delimiter     string                `json:"delimiter,omitempty"`
	ColumnHeaders map[string]*CSVColumn `json:"columnHeaders"`
	ColumnOrder   []string              `json:"-"` // preserves declared column order
}

// CSVColumn describes a single CSV column
type CSVColumn struct {
	Type        SchemaType       `json:"type"`
	Required    bool             `json:"required,omitempty"`
	Default     interface{}      `json:"default,omitempty"`
	Description string           `json:"description,omitempty"`
	Validation  *ValidationRules `json:"validation,omitempty"`
}

// Property represents a field property in a schema
type Property struct {
	Type        SchemaType           `json:"type"`
	Required    bool                 `json:"required,omitempty"`
	Default     interface{}          `json:"default,omitempty"`
	Description string               `json:"description,omitempty"`
	Validation  *ValidationRules     `json:"validation,omitempty"`
	Properties  map[string]*Property `json:"properties,omitempty"` // For OBJECT type
	Items       *Property            `json:"items,omitempty"`      // For ARRAY type
}

// SchemaType represents the data type of a field
type SchemaType string

// Supported schema types
const (
	TypeString   SchemaType = "STRING"
	TypeNumber   SchemaType = "NUMBER"
	TypeBoolean  SchemaType = "BOOLEAN"
	TypeObject   SchemaType = "OBJECT"
	TypeArray    SchemaType = "ARRAY"
	TypeDate     SchemaType = "DATE"
	TypeDateTime SchemaType = "DATETIME"
	TypeByte     SchemaType = "BYTE"
	TypeAny      SchemaType = "ANY"
)

// ValidationRules contains validation rules for a field
type ValidationRules struct {
	// String validations
	MinLength *int     `json:"minLength,omitempty"`
	MaxLength *int     `json:"maxLength,omitempty"`
	Pattern   string   `json:"pattern,omitempty"`
	Format    string   `json:"format,omitempty"`
	Enum      []string `json:"enum,omitempty"`

	// Number validations
	Minimum *float64 `json:"minimum,omitempty"`
	Maximum *float64 `json:"maximum,omitempty"`

	// Array validations
	MinItems    *int `json:"minItems,omitempty"`
	MaxItems    *int `json:"maxItems,omitempty"`
	UniqueItems bool `json:"uniqueItems,omitempty"`
}

// ValidationError represents a single validation error
type ValidationError struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// ValidationResult holds the result of validation
type ValidationResult struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors,omitempty"`
}

// ProcessOptions controls schema processing behavior
type ProcessOptions struct {
	ApplyDefaults    bool // Apply default values from schema
	StructureData    bool // Remove fields not in schema
	StrictValidation bool // Fail on validation errors
}

// ProcessResult contains the result of schema processing
type ProcessResult struct {
	Valid  bool              `json:"valid"`
	Data   []byte            `json:"data"`
	Errors []ValidationError `json:"errors,omitempty"`
}

// IsValidType checks if a schema type is valid
func IsValidType(t SchemaType) bool {
	validTypes := map[SchemaType]bool{
		TypeString: true, TypeNumber: true, TypeBoolean: true,
		TypeObject: true, TypeArray: true, TypeDate: true,
		TypeDateTime: true, TypeByte: true, TypeAny: true,
	}
	return validTypes[t]
}

// ToJSON converts a value to JSON bytes
func ToJSON(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// FromJSON parses JSON bytes into a value
func FromJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// EffectiveDelimiter returns the configured delimiter or a default comma
func (c *CSVSchema) EffectiveDelimiter() string {
	if c == nil || c.Delimiter == "" {
		return ","
	}
	return c.Delimiter
}

// ToObjectSchema converts a CSV schema into an object schema for reuse in validation/transformers
func (c *CSVSchema) ToObjectSchema() *Schema {
	props := make(map[string]*Property)
	for name, col := range c.ColumnHeaders {
		props[name] = &Property{
			Type:        col.Type,
			Required:    col.Required,
			Default:     col.Default,
			Description: col.Description,
			Validation:  col.Validation,
		}
	}

	return &Schema{
		Type:       TypeObject,
		Properties: props,
	}
}
