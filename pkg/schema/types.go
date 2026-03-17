package schema

import (
	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
	"github.com/wehubfusion/Icarus/pkg/schema/csv"
	"github.com/wehubfusion/Icarus/pkg/schema/json"
)

// Re-exports from json sub-package for backward compatibility.
type (
	Schema          = json.Schema
	Property        = json.Property
	SchemaType      = json.SchemaType
	ValidationRules = json.ValidationRules
	Parser          = json.Parser
	Validator       = json.Validator
)

// Constructor re-exports so callers can use schema.NewParser(), etc.
var (
	NewParser    = json.NewParser
	NewValidator = json.NewValidator
)

// Re-exports from csv sub-package for backward compatibility.
type (
	CSVSchema = csv.CSVSchema
	CSVColumn = csv.CSVColumn
)

// Schema type constants (re-exported from json).
const (
	TypeString   = json.TypeString
	TypeNumber   = json.TypeNumber
	TypeBoolean  = json.TypeBoolean
	TypeObject   = json.TypeObject
	TypeArray    = json.TypeArray
	TypeDate     = json.TypeDate
	TypeDateTime = json.TypeDateTime
	TypeByte     = json.TypeByte
	TypeAny      = json.TypeAny
	TypeUUID     = json.TypeUUID
)

// Re-exports from contracts for backward compatibility.
type (
	SchemaFormat     = contracts.SchemaFormat
	Severity         = contracts.Severity
	ValidationMode   = contracts.ValidationMode
	ValidationIssue  = contracts.ValidationIssue
	ValidationError  = contracts.ValidationError
	ValidationResult = contracts.ValidationResult
	ProcessOptions   = contracts.ProcessOptions
	ProcessResult    = contracts.ProcessResult
)

const (
	FormatJSON = contracts.FormatJSON
	FormatCSV  = contracts.FormatCSV
	FormatHL7  = contracts.FormatHL7
)

const (
	ValidationModeStrict  = contracts.ValidationModeStrict
	ValidationModeNormal  = contracts.ValidationModeNormal
	ValidationModeLenient = contracts.ValidationModeLenient
)

const (
	SeverityError   = contracts.SeverityError
	SeverityWarning = contracts.SeverityWarning
	SeverityInfo    = contracts.SeverityInfo
)

// IsValidType checks if a schema type is valid (re-exported from json).
func IsValidType(t SchemaType) bool {
	return json.IsValidType(t)
}

// ToJSON converts a value to JSON bytes (re-exported from json).
func ToJSON(v interface{}) ([]byte, error) {
	return json.ToJSON(v)
}

// FromJSON parses JSON bytes into a value (re-exported from json).
func FromJSON(data []byte, v interface{}) error {
	return json.FromJSON(data, v)
}
