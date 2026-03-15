# HL7 Schema

The `hl7` package provides parsing and validation of HL7 v2.x messages against a JSON schema definition. It is validation-only: no transformation is applied, and `ProcessResult.Data` is always the original raw message bytes.

## Overview

- **Parser**: Tokenizes raw message bytes (BOM strip, line endings), extracts delimiters from MSH, and parses segments/fields/components.
- **Schema**: JSON definition of message type, version, and segment/field/component structure (Morpheus-compatible).
- **Matcher**: Matches parsed message segments to the schema (order, required segments, repetition).
- **Validator**: Field- and component-level checks: usage (R/RE/O/C/X), repetitions, length, datatypes, and optional MSH-9/MSH-12.
- **VARIES**: Resolves VARIES datatypes from context (e.g. OBX-5 from OBX-2 in the same segment).
- **Processor**: Implements `contracts.SchemaProcessor`; returns validation result and original input as `Data`.

## Schema Definition Format

Schema is JSON with `messageType`, `version`, and `segments`. Segments can be groups (`isGroup: true`) with nested `segments` and `fields`.

### Segment and field structure

- **Segment**: `name` (e.g. MSH, PID), `usage` (R, RE, O, C, B, X, W), `rpt` (repetition, e.g. 1, *, 5).
- **Field**: `position` (e.g. "1", "2"), `dataType`, `usage`, `rpt`, `length`, optional `components`.
- **Component**: `position`, `dataType`, `usage`, `length`, optional `subComponents`.

### Usage codes

| Code | Meaning |
|------|--------|
| R    | Required; must be present and non-empty |
| RE   | Required but may be empty |
| O    | Optional |
| C    | Conditional |
| B    | Backward compatible |
| X    | Not used; must be absent or ignored |
| W    | Withdrawn (v2.7+) |

### Datatypes

Primitive types validated include: ST, TX, FT, NM, SI, SN, DT, TM, DTM/TS, IS, and others. **VARIES** is resolved at runtime (e.g. OBX-5 from OBX-2 via `VariesResolver`). TableID is not validated.

## Validation Checks

- **Structure**: Message starts with MSH; delimiters and segment parsing.
- **Match**: Segment order and presence; required segments; repetition limits; unexpected segments.
- **Message type / version**: If schema sets `messageType` or `version`, MSH-9 and MSH-12 are checked.
- **Fields**: Required/missing, R non-empty, X not present, repetitions within `rpt`, length, datatype (including VARIES).
- **Components**: Required/missing, length, datatype; extra components reported unless `AllowExtraFields` is set.

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
| HL7_EXTRA_FIELD | Segment has more fields than schema (suppress with AllowExtraFields) |
| HL7_EXTRA_COMPONENT | Field has more components than schema (suppress with AllowExtraFields) |

## ProcessOptions

- **CollectAllErrors**: If true, collect all validation errors; otherwise stop after the first.
- **StrictValidation**: If true, return a non-nil error when validation fails.
- **AllowExtraFields**: If true, do not report HL7_EXTRA_FIELD or HL7_EXTRA_COMPONENT.

## VARIES and extensibility

VARIES fields (e.g. OBX-5) get their effective type from a `VariesResolver`. OBX-5 is registered to use OBX-2 of the same segment. Custom resolvers can be registered with `RegisterVariesResolver(segment, field, resolver)`.

## Usage

Use the schema engine for the unified API:

```go
engine := schema.NewEngine()
result, err := engine.ProcessHL7WithSchema(rawMessage, schemaDef, schema.ProcessOptions{
    CollectAllErrors: true,
    AllowExtraFields: true,
})
// result.Data is the original rawMessage
```

Or use this package directly:

```go
msg, err := hl7.ParseMessage(raw)
compiled, _ := hl7.ParseHL7Schema(schemaDef)
match := hl7.MatchMessage(msg, compiled)
errs := hl7.ValidateMatchResult(match, msg, true, false)
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
