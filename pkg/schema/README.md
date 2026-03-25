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
result, err := engine.ProcessHL7WithSchema(hl7MessageBytes, hl7SchemaDef, schema.ProcessOptions{
    CollectAllErrors: true,
    Mode:             schema.ValidationModeNormal,
})
```

HL7 results are severity-bucketed into three slices:

- `ProcessResult.Errors` — ERROR severity issues
- `ProcessResult.Warnings` — WARNING severity issues
- `ProcessResult.Infos` — INFO severity issues

The bucket a code lands in depends on `ProcessOptions.Mode` (STRICT / NORMAL / LENIENT). For HL7, see `pkg/schema/hl7/README.md` for the full mapping, including **`HL7_CEL_EVAL_ERROR`** (always WARNING, even in STRICT).

## Schema formats

| Format | Package | README | Description |
|--------|---------|--------|-------------|
| **JSON** | [json](./json/) | [json/README.md](./json/README.md) | Object/array schemas; validation + defaults + structuring |
| **CSV**  | [csv](./csv/)  | [csv/README.md](./csv/README.md)  | Typed column headers; input = JSON array of row objects |
| **HL7**  | [hl7](./hl7/)  | [hl7/README.md](./hl7/README.md)  | HL7 v2.x message validation + optional CEL custom rules; no transformation |

## Engine API

| Method | Description |
|--------|-------------|
| `Process(input, schemaDef, format, opts)` | Unified entry point; `format` is `FormatJSON`, `FormatCSV`, or `FormatHL7` |
| `ProcessWithSchema(input, schemaDef, opts)` | JSON shortcut |
| `ProcessCSVWithSchema(input, schemaDef, opts)` | CSV shortcut |
| `ProcessHL7WithSchema(input, schemaDef, opts)` | HL7 shortcut |
| `ValidateOnly`, `ValidateCSVOnly`, `ValidateHL7Only` | Validation without defaults/structuring |
| `TransformOnly(input, schemaDef)` | JSON only: apply defaults and structuring without re-validation |
| `ParseJSONSchema(schemaDef)` | Parse a JSON schema for inspection without processing data |

## ProcessOptions

| Option | JSON | CSV | HL7 | Description |
|--------|------|-----|-----|-------------|
| `ApplyDefaults` | ✓ | ✓ | — | Fill in default values from the schema |
| `StructureData` | ✓ | ✓ | — | Reshape data to match schema structure; remove undeclared keys |
| `StrictValidation` | ✓ | ✓ | ✓ | Deprecated; prefer `Mode`. When set, behaves like `Mode=STRICT` where supported. |
| `CollectAllErrors` | ✓ | ✓ | ✓ | Collect every issue (true) or stop at the first error-severity issue (false) |
| `Mode` | — | — | ✓ | `STRICT` / `NORMAL` / `LENIENT` — controls HL7 severity buckets (see `hl7/README.md`) |

## ProcessResult

| Field | Description |
|-------|-------------|
| `Valid` | `true` when there are no ERROR-severity issues (warnings alone, including HL7 CEL eval warnings, do not set `Valid` to false) |
| `Data` | Processed output — JSON/CSV: transformed bytes; HL7: original input bytes unchanged |
| `Errors` | ERROR-severity validation issues (path, message, code) |
| `Warnings` | WARNING-severity issues (HL7 only) |
| `Infos` | INFO-severity issues (HL7 only) |

## Package layout

| Path | Purpose |
|------|---------|
| `contracts/` | Shared interfaces (`SchemaProcessor`, `CompiledSchema`, `ProcessOptions`, `ProcessResult`, severity constants) — avoids import cycles |
| `json/` | JSON schema parser, validator, transformer, processor |
| `csv/` | CSV schema parser, validator, transformer, processor (reuses JSON validation rules for columns) |
| `hl7/` | HL7 message parser, schema parser, matcher, field/component validator, CEL integration, processor |
| `engine.go` | `Engine` struct and `Process` dispatcher |
| `types.go` | Backward-compatible type aliases re-exported from `contracts`, `json`, and `csv` |
| `transformer.go` | Root transformer wrapper (JSON + CSV helpers) |
| `registry.go` | Processor registry keyed by format name |
| `errors.go` | `SchemaError` and `NewSchemaError` |

For details on each format, see the README in the corresponding sub-package.
