# Schema Engine

The schema engine provides format-aware parsing, validation, and (where applicable) transformation of data against schema definitions. It supports **JSON**, **CSV** (row objects), and **HL7 v2.x** messages through a single `Process` API.

## Quick start

```go
engine := schema.NewEngine()

// JSON
result, err := engine.ProcessWithSchema(jsonBytes, jsonSchemaDef, schema.ProcessOptions{
    ApplyDefaults: true, StructureData: true, CollectAllErrors: true,
})

// CSV (input = JSON array of row objects)
result, err := engine.ProcessCSVWithSchema(csvRowsJSON, csvSchemaDef, options)

// HL7 (validation only; result.Data = original bytes)
result, err := engine.ProcessHL7WithSchema(hl7MessageBytes, hl7SchemaDef, options)
```

## Schema types

| Format | Package | README | Description |
|--------|---------|--------|-------------|
| **JSON** | [json](./json/) | [json/README.md](./json/README.md) | Object/array schemas; validation + defaults + structuring |
| **CSV**  | [csv](./csv/)  | [csv/README.md](./csv/README.md)  | Typed column headers; input = JSON array of rows |
| **HL7**  | [hl7](./hl7/)  | [hl7/README.md](./hl7/README.md)  | HL7 v2.x message validation; no transformation |

## Engine API

- **Process(input, schemaDef, format, opts)** – Unified entry point; format is `FormatJSON`, `FormatCSV`, or `FormatHL7`.
- **ProcessWithSchema**, **ProcessCSVWithSchema**, **ProcessHL7WithSchema** – Convenience methods that call `Process` with the right format.
- **ValidateOnly**, **ValidateCSVOnly**, **ValidateHL7Only** – Validation only (no defaults/structure); delegate through `Process`.
- **TransformOnly(input, schemaDef)** – JSON only: apply defaults and structure; delegates through `Process`.
- **ParseJSONSchema(schemaDef)** – Parse a JSON schema to inspect it (e.g. root type) without processing data.

## ProcessOptions

| Option | JSON | CSV | HL7 |
|--------|------|-----|-----|
| ApplyDefaults | ✓ | ✓ | — |
| StructureData | ✓ | ✓ | — |
| StrictValidation | ✓ | ✓ | ✓ |
| CollectAllErrors | ✓ | ✓ | ✓ |
| Mode | — | — | ✓ (STRICT/NORMAL/LENIENT; controls severity) |

## ProcessResult

- **Valid** – Whether validation passed.
- **Data** – Processed output (JSON/CSV: transformed bytes; HL7: original input bytes unchanged).
- **Errors** – List of validation errors (path, message, code).

## Package layout

- **contracts/** – Shared interfaces (`SchemaProcessor`, `CompiledSchema`, `ProcessOptions`, `ProcessResult`, etc.) to avoid import cycles.
- **json/** – JSON schema parser, validator, transformer, processor.
- **csv/** – CSV schema parser, validator, transformer, processor (reuses JSON validation rules for columns).
- **hl7/** – HL7 message parser, schema parser, matcher, validator, VARIES resolution, processor.
- **engine.go** – Engine and registry; `Process` dispatches to the correct processor by format.
- **types.go** – Backward-compatible type aliases and re-exports from contracts, json, and csv.
- **transformer.go** – Root transformer wrapper (JSON + CSV helpers).
- **registry.go** – Processor registry by format name.
- **errors.go** – `SchemaError` and `NewSchemaError`.

For details on each schema type, see the README in the corresponding subpackage.
