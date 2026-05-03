package csv

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"sort"

	"github.com/wehubfusion/Icarus/pkg/schema/json"
)

// Parser handles parsing of CSV schema definitions
type Parser struct{}

// NewParser creates a new CSV schema parser
func NewParser() *Parser {
	return &Parser{}
}

// ParseCSV parses a typed CSV schema definition (see Olympus/csv.md)
func (p *Parser) ParseCSV(schemaBytes []byte) (*CSVSchema, error) {
	if len(schemaBytes) == 0 {
		return nil, fmt.Errorf("schema bytes cannot be empty")
	}

	var raw struct {
		Name          string              `json:"name,omitempty"`
		Delimiter     string              `json:"delimiter,omitempty"`
		ColumnHeaders stdjson.RawMessage `json:"columnHeaders"`
	}

	if err := stdjson.Unmarshal(schemaBytes, &raw); err != nil {
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
func (p *Parser) decodeOrderedColumns(raw stdjson.RawMessage, dest map[string]*CSVColumn) ([]string, error) {
	dec := stdjson.NewDecoder(bytes.NewReader(raw))

	token, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid columnHeaders: %w", err)
	}
	if delim, ok := token.(stdjson.Delim); !ok || delim != '{' {
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

	order, err = applyPositionColumnOrder(order, dest)
	if err != nil {
		return nil, err
	}

	return order, nil
}

// applyPositionColumnOrder sorts columns by explicit position when every column declares one.
// If any column omits position, declaration order (JSON key stream order) is preserved.
func applyPositionColumnOrder(order []string, dest map[string]*CSVColumn) ([]string, error) {
	allHavePosition := true
	for _, name := range order {
		if dest[name] == nil || dest[name].Position == nil {
			allHavePosition = false
			break
		}
	}
	if !allHavePosition {
		return order, nil
	}

	seen := make(map[int]string, len(order))
	for _, name := range order {
		pos := *dest[name].Position
		if pos < 0 {
			return nil, fmt.Errorf("column '%s' has invalid negative position %d", name, pos)
		}
		if conflict, ok := seen[pos]; ok {
			return nil, fmt.Errorf("duplicate position %d on columns %q and %q", pos, conflict, name)
		}
		seen[pos] = name
	}

	sort.SliceStable(order, func(i, j int) bool {
		pi := *dest[order[i]].Position
		pj := *dest[order[j]].Position
		if pi != pj {
			return pi < pj
		}
		return order[i] < order[j]
	})

	return order, nil
}

// validateCSVColumn enforces CSV-specific constraints
func (p *Parser) validateCSVColumn(col *CSVColumn, name string) error {
	if col == nil {
		return fmt.Errorf("column '%s' definition cannot be nil", name)
	}

	switch col.Type {
	case json.TypeString, json.TypeNumber, json.TypeDate:
	default:
		return fmt.Errorf("column '%s' has invalid type %s (allowed: STRING, NUMBER, DATE)", name, col.Type)
	}

	if col.Validation != nil {
		if err := json.ValidateValidationRulesForType(col.Validation, col.Type, name); err != nil {
			return err
		}
	}

	return nil
}
