# CSV Schema

The `csv` package provides schema-based parsing, validation, and transformation for CSV data represented as a JSON array of row objects in the Icarus schema engine.

## Overview

- **Parser**: Reads CSV schema definitions (JSON) and preserves column order from `columnHeaders`.
- **Validator**: Validates each row against the schema using the same rules as the JSON validator (types, formats, lengths); error paths are row-scoped (e.g. `rows[0].columnName`).
- **Transformer**: Applies default values and structures rows (drops columns not in the schema).
- **Processor**: Implements `contracts.SchemaProcessor`; input is a JSON array of objects, one per row.

## Schema Definition Format

Schemas are JSON objects with a `columnHeaders` object (and optional `name`, `delimiter`). Column order is taken from the order keys appear in `columnHeaders`.

### Structure

```json
{
  "name": "optional schema name",
  "delimiter": ",",
  "columnHeaders": {
    "columnName1": { "type": "STRING", "required": true },
    "columnName2": { "type": "NUMBER", "default": 0 }
  }
}
```

- **delimiter** – Optional; default `,`. Used when parsing raw CSV text (e.g. in other tools); validation works on row objects.
- **columnHeaders** – Required. Keys are column names; values use the same types and validation rules as the JSON schema package (`type`, `required`, `default`, `validation`, etc.).

### Column types

Same as JSON schema: STRING, NUMBER, BOOLEAN, OBJECT, ARRAY, DATE, DATETIME, BYTE, UUID, ANY. Each column can have `validation` (minLength, maxLength, format, enum, minimum, maximum, etc.).

## Input and output

- **Process input**: Bytes of a JSON array of objects, e.g. `[{"a": "1", "b": 2}, {"a": "3", "b": 4}]`.
- **Validation**: Each object is validated against the schema; errors use paths like `rows[0].fieldName` (no dependency on a fixed `"root"` prefix).
- **Transformation**: Defaults are applied and columns not in the schema are removed per row.

## Usage

Use the schema engine for the unified API:

```go
engine := schema.NewEngine()
result, err := engine.ProcessCSVWithSchema(inputJSON, schemaDef, schema.ProcessOptions{
    ApplyDefaults:    true,
    StructureData:    true,
    CollectAllErrors: true,
})
```

Or use this package directly for validation/transformation of in-memory rows:

```go
parser := csv.NewParser()
s, _ := parser.ParseCSV(schemaBytes)

rows := []map[string]interface{}{ ... }
res := csv.ValidateCSVRowsWithOptions(rows, s, true)
rows, _ = csv.ApplyCSVDefaults(rows, s)
rows, _ = csv.StructureCSVRows(rows, s)
```

## Files

| File           | Purpose                                    |
|----------------|--------------------------------------------|
| `schema.go`    | CSVSchema, CSVColumn, ToObjectSchema        |
| `parser.go`    | Parse CSV schema JSON, preserve column order |
| `validator.go` | Validate rows; uses json.ValidateWithRootPath |
| `transformer.go` | Apply defaults and structure rows        |
| `processor.go` | SchemaProcessor implementation             |
