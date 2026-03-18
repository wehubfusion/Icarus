# HL7 Schema

The `hl7` package provides parsing and validation of HL7 v2.x messages against a JSON schema definition. It is validation-only: no transformation is applied, and `ProcessResult.Data` is always the original raw message bytes.

## Overview

- **Parser**: Tokenizes raw message bytes (BOM strip, line endings), extracts delimiters from MSH, and parses segments/fields/components.
- **Schema**: JSON definition of message type, version, and segment/field/component structure (Morpheus-compatible).
- **Matcher**: Matches parsed message segments to the schema (order, required segments, repetition).
- **Validator**: Field- and component-level checks: usage (R/X/W), repetitions, length, datatypes, and optional MSH-9/MSH-12.
- **VARIES**: Resolution is intentionally disabled for now; `VARIES` falls back to validating as `ST`.
- **Processor**: Implements `contracts.SchemaProcessor`; returns validation result and original input as `Data`.

## Schema Definition Format

Schema is JSON with `messageType`, `version`, and `segments`. Segments can be groups (`isGroup: true`) with nested `segments` and `fields`.

### Segment and field structure

- **Segment**: `name` (e.g. MSH, PID), `usage` (R, RE, O, C, B, X, W), `rpt` (repetition, e.g. 1, *, 5).
- **Field**: `position` (e.g. "MSH.9" or "PID-3"), `dataType`, `usage`, `rpt`, `length`.
- **Component**: `position`, `dataType`, `usage`, `length`, optional `subComponents`.

### Usage codes

| Code | Meaning |
|------|--------|
| R    | Required; must be present and non-empty |
| RE   | Required but may be empty (not enforced currently) |
| O    | Optional |
| C    | Conditional |
| B    | Backward compatible |
| X    | Not used; must be absent or ignored |
| W    | Withdrawn (v2.7+) |

### Datatypes

Primitive types validated include: ST, TX, FT, NM, SI, SN, DT, TM, DTM/TS, IS, and others. `VARIES` falls back to `ST`. TableID is not validated.

## Validation Checks

- **Structure**: Message starts with MSH; delimiters and segment parsing.
- **Match**: Segment order and presence; required segments; repetition limits; unexpected segments.
- **Message type / version**: If schema sets `messageType` or `version`, MSH-9 and MSH-12 are checked.
- **Fields**: Required/missing, R non-empty, X/W not present, repetitions within `rpt` (when specified), length, datatype (including VARIES fallback).
- **Components**: Length, datatype; extra fields/components/subcomponents are detected only when the extra slot is non-empty; severity depends on `Mode`.

## Error codes

| Code | Meaning |
|------|--------|
| HL7_INVALID_MSH | Message invalid or missing MSH |
| HL7_MESSAGE_TYPE_MISMATCH | MSH-9 does not match schema messageType |
| HL7_VERSION_MISMATCH | MSH-12 does not match schema version |
| HL7_UNEXPECTED_SEGMENT | Segment not allowed in schema at this position |
| HL7_MISSING_REQUIRED | Required segment missing |
| HL7_MISSING_REQUIRED | Required field missing (path = segment-field) |
| HL7_REQUIRED | Required field/component empty or missing |
| HL7_NOT_USED | Field present but usage is X |
| HL7_REPETITION_VIOLATION | Repetitions exceed rpt |
| HL7_LENGTH | Value length exceeds schema length |
| HL7_EXTRA_FIELD | Segment has more fields than schema |
| HL7_EXTRA_COMPONENT | Field has more components than datatype definition allows |
| HL7_EXTRA_SUBCOMPONENT | Component has more subcomponents than datatype definition allows |

## Severity by mode

The same validation codes are bucketed into `Errors`, `Warnings`, and `Infos` depending on `ProcessOptions.Mode`.
This is the authoritative mapping implemented in `processor.go`.

| Code | STRICT | NORMAL | LENIENT |
|------|--------|--------|---------|
| HL7_EMPTY_MESSAGE | ERROR | ERROR | ERROR |
| HL7_INVALID_MSH | ERROR | ERROR | ERROR |
| HL7_INVALID_SCHEMA | ERROR | ERROR | ERROR |
| HL7_MISSING_REQUIRED | ERROR | ERROR | WARNING |
| HL7_MESSAGE_TYPE_MISMATCH | ERROR | ERROR | WARNING |
| HL7_REPETITION_VIOLATION | ERROR | ERROR | WARNING |
| HL7_DATATYPE | ERROR | ERROR | WARNING |
| HL7_VERSION_MISMATCH | ERROR | WARNING | INFO |
| HL7_REQUIRED | ERROR | WARNING | INFO |
| HL7_NOT_USED | ERROR | WARNING | WARNING |
| HL7_LENGTH | ERROR | WARNING | INFO |
| HL7_UNEXPECTED_SEGMENT | ERROR | WARNING | INFO |
| HL7_EXTRA_FIELD | ERROR | INFO | INFO |
| HL7_EXTRA_COMPONENT | ERROR | INFO | INFO |
| HL7_EXTRA_SUBCOMPONENT | ERROR | INFO | INFO |

## ProcessOptions

- **CollectAllErrors**: If true, collect all validation errors; otherwise stop after the first.
- **StrictValidation**: Deprecated alias for `Mode=STRICT`.
- **Mode**: `STRICT`, `NORMAL`, or `LENIENT` (controls severity bucketing).

## Unspecified schema constraints

- If `usage` is empty/omitted, it is treated as **unspecified** (no required/not-used enforcement).
- If `rpt` is empty/omitted, it is treated as **unspecified** (no repetition enforcement).

## Usage

Use the schema engine for the unified API:

```go
engine := schema.NewEngine()
result, err := engine.ProcessHL7WithSchema(rawMessage, schemaDef, schema.ProcessOptions{
    CollectAllErrors: true,
    Mode: schema.ValidationModeNormal,
})
// result.Data is the original rawMessage
```

Or use this package directly:

```go
msg, err := hl7.ParseMessage(raw)
compiled, _ := hl7.ParseHL7Schema(schemaDef)
match := hl7.MatchMessage(msg, compiled)
errs := hl7.ValidateMatchResult(match, msg, true, nil)
```

## Files

| File           | Purpose                                      |
|----------------|----------------------------------------------|
| `schema.go`    | HL7Schema, segment/field/component defs, Validate() |
| `parser.go`    | Tokenize, ParseMessage, ParseHL7Schema       |
| `types.go`     | Delimiters, Segment, Field, Component, Message |
| `matcher.go`   | MatchMessage, segment order and repetition   |
| `validator.go` | ValidateMessageTypeAndVersion, ValidateMatchResult, datatype checks |
| `varies.go`    | VariesResolver, OBX-5 resolver registration   |
| `processor.go` | SchemaProcessor implementation               |
