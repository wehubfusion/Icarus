package json

import "encoding/json"

// Schema represents a complete schema definition
type Schema struct {
	Type        SchemaType           `json:"type"`
	Properties  map[string]*Property `json:"properties,omitempty"`
	Items       *Property            `json:"items,omitempty"`
	Description string               `json:"description,omitempty"`
}

// Property represents a field property in a schema.
type Property struct {
	Type        SchemaType           `json:"type"`
	Required    *bool                `json:"required,omitempty"`
	Default     interface{}          `json:"default,omitempty"`
	Prefix      *string              `json:"prefix,omitempty"`  // Optional prefix for UUID type (e.g. "urn:uuid:")
	Postfix     *string              `json:"postfix,omitempty"` // Optional postfix for UUID type
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
	TypeUUID     SchemaType = "UUID"
)

// ValidationRules contains validation rules for a field (Morpheus-aligned: *float64 for lengths/counts, *string for pattern/format, *bool for uniqueItems).
type ValidationRules struct {
	// String validations
	MinLength *float64 `json:"minLength,omitempty"`
	MaxLength *float64 `json:"maxLength,omitempty"`
	Pattern   *string  `json:"pattern,omitempty"`
	Format    *string  `json:"format,omitempty"`
	Enum      []string `json:"enum,omitempty"`

	// Number validations
	Minimum *float64 `json:"minimum,omitempty"`
	Maximum *float64 `json:"maximum,omitempty"`

	// Array validations
	MinItems    *float64 `json:"minItems,omitempty"`
	MaxItems    *float64 `json:"maxItems,omitempty"`
	UniqueItems *bool    `json:"uniqueItems,omitempty"`

	// Date validations (Morpheus-aligned)
	MinDate *string `json:"minDate,omitempty"`
	MaxDate *string `json:"maxDate,omitempty"`
}

// IsValidType checks if a schema type is valid
func IsValidType(t SchemaType) bool {
	validTypes := map[SchemaType]bool{
		TypeString: true, TypeNumber: true, TypeBoolean: true,
		TypeObject: true, TypeArray: true, TypeDate: true,
		TypeDateTime: true, TypeByte: true, TypeAny: true,
		TypeUUID: true,
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
