package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Parser handles parsing of schema definitions
type Parser struct{}

// NewParser creates a new schema parser
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a schema from JSON bytes
func (p *Parser) Parse(schemaBytes []byte) (*Schema, error) {
	if len(schemaBytes) == 0 {
		return nil, fmt.Errorf("schema bytes cannot be empty")
	}

	var schema Schema
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	// Validate schema structure
	if err := p.validateSchema(&schema); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	return &schema, nil
}

// ParseCSV parses a typed CSV schema definition (see Olympus/csv.md)
func (p *Parser) ParseCSV(schemaBytes []byte) (*CSVSchema, error) {
	if len(schemaBytes) == 0 {
		return nil, fmt.Errorf("schema bytes cannot be empty")
	}

	var raw struct {
		Name          string          `json:"name,omitempty"`
		Delimiter     string          `json:"delimiter,omitempty"`
		ColumnHeaders json.RawMessage `json:"columnHeaders"`
	}

	if err := json.Unmarshal(schemaBytes, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse csv schema: %w", err)
	}

	if len(raw.ColumnHeaders) == 0 {
		return nil, fmt.Errorf("columnHeaders is required for CSV schema")
	}

	columnHeaders := make(map[string]*CSVColumn)
	columnOrder, err := p.decodeOrderedColumns(raw.ColumnHeaders, columnHeaders)
	if err != nil {
		return nil, err
	}

	schema := &CSVSchema{
		Name:          raw.Name,
		Delimiter:     raw.Delimiter,
		ColumnHeaders: columnHeaders,
		ColumnOrder:   columnOrder,
	}

	if schema.Delimiter == "" {
		schema.Delimiter = ","
	}

	return schema, nil
}

// decodeOrderedColumns preserves column declaration order while decoding column headers
func (p *Parser) decodeOrderedColumns(raw json.RawMessage, dest map[string]*CSVColumn) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))

	token, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid columnHeaders: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("columnHeaders must be an object")
	}

	var order []string
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid columnHeaders key: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("columnHeaders keys must be strings")
		}

		var col CSVColumn
		if err := dec.Decode(&col); err != nil {
			return nil, fmt.Errorf("invalid column definition for '%s': %w", key, err)
		}
		if err := p.validateCSVColumn(&col, key); err != nil {
			return nil, err
		}

		dest[key] = &col
		order = append(order, key)
	}

	if _, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("invalid columnHeaders closing token: %w", err)
	}

	if len(order) == 0 {
		return nil, fmt.Errorf("columnHeaders cannot be empty")
	}

	return order, nil
}

// validateSchema ensures the schema structure is valid
func (p *Parser) validateSchema(schema *Schema) error {
	if schema.Type == "" {
		return fmt.Errorf("schema type is required")
	}

	if !IsValidType(schema.Type) {
		return fmt.Errorf("invalid schema type: %s", schema.Type)
	}

	// Validate OBJECT type has properties
	if schema.Type == TypeObject && schema.Properties != nil {
		for propName, prop := range schema.Properties {
			if err := p.validateProperty(prop, propName); err != nil {
				return err
			}
		}
	}

	// Validate ARRAY type has items definition
	if schema.Type == TypeArray && schema.Items != nil {
		if err := p.validateProperty(schema.Items, "items"); err != nil {
			return fmt.Errorf("invalid array items: %w", err)
		}
	}

	return nil
}

// validateProperty validates a property definition
func (p *Parser) validateProperty(prop *Property, name string) error {
	if prop.Type == "" {
		return fmt.Errorf("property '%s' must have a type", name)
	}

	if !IsValidType(prop.Type) {
		return fmt.Errorf("property '%s' has invalid type: %s", name, prop.Type)
	}

	// Validate nested OBJECT properties
	if prop.Type == TypeObject && prop.Properties != nil {
		for nestedName, nestedProp := range prop.Properties {
			if err := p.validateProperty(nestedProp, name+"."+nestedName); err != nil {
				return err
			}
		}
	}

	// Validate ARRAY items
	if prop.Type == TypeArray && prop.Items != nil {
		if err := p.validateProperty(prop.Items, name+"[]"); err != nil {
			return err
		}
	}

	// Validate validation rules if present
	if prop.Validation != nil {
		if err := p.validateValidationRules(prop.Validation, prop.Type, name); err != nil {
			return err
		}
	}

	return nil
}

// validateValidationRules ensures validation rules are appropriate for the field type
func (p *Parser) validateValidationRules(rules *ValidationRules, fieldType SchemaType, name string) error {
	// String-specific validations (pattern, format, enum are string-only)
	if fieldType != TypeString && fieldType != TypeByte && fieldType != TypeDate && fieldType != TypeDateTime {
		if rules.MinLength != nil || rules.MaxLength != nil {
			return fmt.Errorf("property '%s': minLength/maxLength validation rules used on non-string/non-byte type %s", name, fieldType)
		}
	}

	// String-only validations (not applicable to BYTE)
	if fieldType != TypeString && fieldType != TypeDate && fieldType != TypeDateTime {
		if rules.Pattern != "" || rules.Format != "" || len(rules.Enum) > 0 {
			return fmt.Errorf("property '%s': string validation rules (pattern/format/enum) used on non-string type %s", name, fieldType)
		}
	}

	// Number-specific validations
	if fieldType != TypeNumber {
		if rules.Minimum != nil || rules.Maximum != nil {
			return fmt.Errorf("property '%s': number validation rules used on non-number type %s", name, fieldType)
		}
	}

	// Array-specific validations
	if fieldType != TypeArray {
		if rules.MinItems != nil || rules.MaxItems != nil || rules.UniqueItems {
			return fmt.Errorf("property '%s': array validation rules used on non-array type %s", name, fieldType)
		}
	}

	return nil
}

// validateCSVColumn enforces CSV-specific constraints
func (p *Parser) validateCSVColumn(col *CSVColumn, name string) error {
	if col == nil {
		return fmt.Errorf("column '%s' definition cannot be nil", name)
	}

	switch col.Type {
	case TypeString, TypeNumber, TypeDate:
	default:
		return fmt.Errorf("column '%s' has invalid type %s (allowed: STRING, NUMBER, DATE)", name, col.Type)
	}

	if col.Validation != nil {
		if err := p.validateValidationRules(col.Validation, col.Type, name); err != nil {
			return err
		}
	}

	return nil
}
