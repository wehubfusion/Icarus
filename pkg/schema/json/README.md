# JSON Schema

The `json` package provides schema-based parsing, validation, and transformation for JSON data in the Icarus schema engine.

## Overview

- **Parser**: Reads JSON schema definitions and validates their structure.
- **Validator**: Validates JSON data against a schema (types, formats, lengths, required fields).
- **Transformer**: Applies default values and structures data (removes extra fields) according to the schema.
- **Processor**: Implements `contracts.SchemaProcessor` and wires parse → validate → transform for the engine.

## Schema Definition Format

Schemas are JSON objects with a root `type` and optional `properties` or `items`.

### Root types

- **OBJECT** – `properties` map of field names to property definitions.
- **ARRAY** – `items` defines the type of each element.

### Supported field types

| Type       | Description                    |
|-----------|---------------------------------|
| STRING    | Text; optional format/pattern  |
| NUMBER    | Numeric; optional min/max      |
| BOOLEAN   | true/false                     |
| OBJECT    | Nested object with properties  |
| ARRAY     | Array with typed items         |
| DATE      | Date string (YYYY-MM-DD)       |
| DATETIME  | ISO 8601 datetime              |
| BYTE      | Base64-encoded binary          |
| UUID      | UUID v1–v5; optional prefix/postfix |
| ANY       | No type check                  |

### Validation rules (per property)

- **Strings**: `minLength`, `maxLength`, `pattern`, `format`, `enum`
- **Numbers**: `minimum`, `maximum`
- **Arrays**: `minItems`, `maxItems`, `uniqueItems`
- **Formats** (built-in): `email`, `uri`, `uuid`, `date`, `datetime` (custom formats via `Validator.RegisterFormat`)

### Example schema

```json
{
  "type": "OBJECT",
  "properties": {
    "id": { "type": "STRING", "required": true },
    "email": { "type": "STRING", "validation": { "format": "email" } },
    "count": { "type": "NUMBER", "validation": { "minimum": 0, "maximum": 100 } },
    "tags": { "type": "ARRAY", "items": { "type": "STRING" } }
  }
}
```

## Usage

Use the schema engine for the unified API; or use this package directly:

```go
parser := json.NewParser()
schema, err := parser.Parse(schemaBytes)

validator := json.NewValidator()
result := validator.ValidateWithOptions(data, schema, true)

transformer := json.NewTransformer()
data, _ = transformer.ApplyDefaults(data, schema)
data, _ = transformer.StructureData(data, schema)
```

For row-level validation with a custom root path (e.g. CSV rows), use `ValidateWithRootPath(data, schema, "rows[0]", collectAllErrors)`.

## Severity overrides

All validation issues emitted by this package use codes from `KnownErrorCodes` in `codes.go` (e.g. `REQUIRED`, `TYPE_MISMATCH`, `FORMAT_MISMATCH`). Pass `ProcessOptions.CodeSeverityOverrides` to the engine to remap any code to WARNING, INFO, or DROP:

```go
engine.ProcessWithSchema(data, schemaDef, schema.ProcessOptions{
    CodeSeverityOverrides: map[string]schema.Severity{
        "REQUIRED": schema.SeverityWarning, // treat missing required fields as warnings
    },
})
```

## Files

| File          | Purpose                                  |
|---------------|------------------------------------------|
| `schema.go`   | Schema and property types, validation rules |
| `parser.go`   | Parse and validate schema definitions   |
| `validator.go`| Validate data against schema             |
| `formats.go`  | Format validators (email, URI, UUID, etc.) |
| `transformer.go` | Apply defaults and structure data     |
| `processor.go`  | SchemaProcessor implementation          |
| `codes.go`      | `KnownErrorCodes` — the complete set of validation codes this package can emit |
