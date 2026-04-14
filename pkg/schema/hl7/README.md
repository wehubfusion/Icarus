# HL7 Schema

The `hl7` package provides parsing and validation of HL7 v2.x messages against a JSON schema definition. It is **validation-only**: no transformation is applied, and `ProcessResult.Data` is always the original raw message bytes.

## Overview

| Component | Responsibility |
|-----------|---------------|
| **Parser** | Tokenizes raw message bytes (BOM strip, CRLF/LF normalisation, size + segment limits), extracts delimiters from MSH, parses segments → fields → components → subcomponents, handles HL7 escape sequences |
| **Schema** | JSON definition of message type, version, segment/field/component structure, and optional CEL custom rules (Morpheus-compatible) |
| **Matcher** | Matches parsed segments to the schema: order, required/optional/repeated segments, group semantics |
| **Validator** | Field-level checks: usage (R/X/W), repetition limits, **field** length (truncation-aware, whole wire value including `^`/`&`), datatype format, extra fields; component/subcomponent **counts** (extra component/subcomponent) but not per-component max length from the datatype tables |
| **Datatype engine** | Registry-driven composite decomposition + primitive scalar validation; truncation delimiter stripped before format checks where values are read |
| **CEL rules** | Optional declarative validation rules evaluated on top of structural validation |
| **Processor** | Implements `contracts.SchemaProcessor`; orchestrates the full pipeline and returns bucketed `Errors`/`Warnings`/`Infos` |

---

## Schema Definition Format

```json
{
  "messageType": "ADT^A01",
  "version": "2.5",
  "segments": [...],
  "rules": [...]
}
```

### Segment / Group

```json
{
  "name": "PID",
  "usage": "R",
  "rpt": "1",
  "fields": [...]
}
```

Groups have `"isGroup": true` and a nested `"segments"` array instead of `"fields"`.

### Field

```json
{
  "position": "PID-3",
  "dataType": "CX",
  "usage": "R",
  "rpt": "*",
  "length": 250,
  "tableId": "0061"
}
```

### Usage codes

| Code | Meaning |
|------|---------|
| R  | Required — must be present and non-empty |
| RE | Required but may be empty — absence is allowed; treated as optional in the current engine |
| O  | Optional |
| C  | Conditional — treated as optional; use CEL `when`/`assert` for the condition logic |
| B  | Backward compatible |
| X  | Not used — any non-empty value produces `HL7_NOT_USED` |
| W  | Withdrawn (v2.7+) — same treatment as X |

### Repetition (`rpt`)

| Value | Meaning |
|-------|---------|
| (empty) | Unspecified — no max enforcement |
| `1` | At most one repetition |
| `*` or `unbounded` | Unlimited |
| `N` | At most N repetitions |

---

## Datatypes

### Primitive types

Validated by regex + range checks in `pkg/schema/hl7/primitive`:

| Type | Rule |
|------|------|
| NM | `[-+]?\d*\.?\d+` |
| SI | `\d+` (non-negative integer) |
| DT | `YYYY[MM[DD]]` with month-specific day count and leap-year awareness |
| TM | `HH[MM[SS[.S+]]][±HHMM]` with range and sign-aware TZ offset (±12h/±14h) |
| DTM / TS | Combined date + time, same checks as DT + TM |
| ST, TX, FT | Free text — no format constraint |
| ID, IS, GTS | Table-driven or free-form — accepted without format check |

### Composite types

Resolved via the datatype registry (`pkg/schema/hl7/datatypes`). Each component is recursively validated:

- Field-level: top-level components of the field's type are iterated.
- Nested composites (e.g. HD inside CX) are further decomposed via `flattenLeaves`.
- Primitive components are validated against only their **first subcomponent value** — extra subcomponents produce `HL7_EXTRA_SUBCOMPONENT` but do not cause double type errors.
- **Length:** `HL7_LENGTH` is raised only when the **whole field** (re-serialized with the message’s delimiters) exceeds the schema field `length`. Per-component / per-subcomponent max lengths from the embedded datatype JSON are **not** enforced (those limits are not user-configurable per deployment).

> **`SN`, `MO`, `NR`** — These are composite types. Their components are split by the parser and each component is validated by its own scalar type. They are not validated as primitives at the field level.

### VARIES

Fields typed `VARIES` (e.g. OBX-5) are validated as `ST` (free-text) because the runtime type is declared in a sibling field (e.g. OBX-2) which is not available at static validation time.  
For correct per-message type enforcement, use a CEL rule. If the sibling type code is unknown, `validateAs` returns false (failed assertion / `HL7_CUSTOM_RULE_VIOLATION`), not a runtime error.

```json
{ "assert": "validateAs('OBX-5', msg('OBX-2'))" }
```

### TableID

The `tableId` field is parsed and stored but not validated — terminology/value-set validation requires an external service (marked with `TODO(terminology)` in the code).

---

## Parser features

| Feature | Detail |
|---------|--------|
| Max message size | 10 MB — `ErrMessageTooLarge` returned for larger input |
| Max segment count | 5,000 — `ErrTooManySegments` returned |
| BOM strip | UTF-8 BOM stripped before parsing |
| Line endings | `\r\n` and `\n` normalised to `\r` |
| Delimiter collision | All 5–6 delimiters must be distinct; `ErrInvalidDelimiters` on clash |
| Escape sequences | `\F\ \S\ \T\ \R\ \E\`, hex (`\X...\`), formatting hints, `\C...\` / `\M...\` (charset — preserved verbatim), `\Z...\` (vendor — preserved verbatim) |
| Truncation delimiter | 5th encoding character (HL7 v2.7+, commonly `#`) — stored as `Delimiters.Truncation`; content at and after that character is ignored for **field length** and for **primitive datatype format** checks (so a value like `25#.7389` is measured/validated as `25`). If MSH-2 has only four encoding characters (typical v2.5), `Truncation` is unset and `#` in payload data is literal. |

---

## Validation checks

### Structure
- `HL7_INVALID_MSH` — missing or malformed MSH
- `HL7_INVALID_MESSAGE` — nil message passed to `MatchMessage`

### Segment matching
- `HL7_MISSING_REQUIRED` — required segment absent
- `HL7_UNEXPECTED_SEGMENT` — segment present but not allowed at this position
- `HL7_REPETITION_VIOLATION` — segment repeats more than `rpt` allows

### Message header
- `HL7_MESSAGE_TYPE_MISMATCH` — MSH-9 doesn't match schema `messageType`
- `HL7_VERSION_MISMATCH` — MSH-12 doesn't match schema `version`. Comparison uses `versionsMatch` in `header_validator.go`: trailing `.0` segments stripped; a leading `v`/`V` is removed **only when followed by a digit** (so `"v2.5.1"` matches `"2.5.1"`, but bare `"v"` does not match a real version). Both sides must contain at least one digit after normalisation, except the reflexive empty case `""` vs `""`.

### Field / component
- `HL7_MISSING_REQUIRED` — required field absent from segment
- `HL7_REQUIRED` — required field is present but empty
- `HL7_NOT_USED` — field with usage X or W contains a value
- `HL7_REPETITION_VIOLATION` — field exceeds `rpt`
- `HL7_LENGTH` — field value length exceeds the `length` defined in the node schema (content after the truncation delimiter is excluded; component/subcomponent lengths from the embedded HL7 datatype tables are not enforced)
- `HL7_DATATYPE` — value does not conform to its HL7 datatype (format, range, calendar)
- `HL7_EXTRA_FIELD` — segment has more fields than the schema defines
- `HL7_EXTRA_COMPONENT` — field has more components than the datatype allows
- `HL7_EXTRA_SUBCOMPONENT` — component has more subcomponents than the datatype allows

### CEL custom rules
- `HL7_CUSTOM_RULE_VIOLATION` — a CEL `assert`/`require`/`forbid` rule evaluated to false
- `HL7_CUSTOM_RULE_RUNTIME_ERROR` — a CEL rule could not finish evaluating (e.g. invalid regex in `matchesPattern`, non-DTM text passed to `toDTM`, invalid `validateAs` location). Severity follows the rule’s `severity` field, defaulting to **WARNING** when omitted. This is **not** the same as a failed assertion; see [CEL runtime errors vs rule violations](#cel-runtime-errors-vs-rule-violations).

---

## Severity by mode

`STRICT` promotes several structural / field HL7 codes from WARNING to ERROR so unexpected segments, length, version mismatch, etc. surface as hard failures. **`HL7_CUSTOM_RULE_RUNTIME_ERROR`** uses the rule’s configured severity (default WARNING when omitted); it is not part of that structural promotion table.

| Code | STRICT | NORMAL | LENIENT |
|------|--------|--------|---------|
| HL7_EMPTY_MESSAGE | ERROR | ERROR | ERROR |
| HL7_INVALID_MSH | ERROR | ERROR | ERROR |
| HL7_INVALID_SCHEMA | ERROR | ERROR | ERROR |
| HL7_INVALID_MESSAGE | ERROR | ERROR | ERROR |
| HL7_MISSING_REQUIRED | ERROR | ERROR | WARNING |
| HL7_MESSAGE_TYPE_MISMATCH | ERROR | ERROR | WARNING |
| HL7_REPETITION_VIOLATION | ERROR | ERROR | WARNING |
| HL7_DATATYPE | ERROR | ERROR | WARNING |
| HL7_VERSION_MISMATCH | ERROR | WARNING | INFO |
| HL7_REQUIRED | ERROR | WARNING | INFO |
| HL7_NOT_USED | ERROR | WARNING | WARNING |
| HL7_LENGTH | ERROR | WARNING | INFO |
| HL7_UNEXPECTED_SEGMENT | ERROR | WARNING | INFO |
| HL7_EXTRA_FIELD | WARNING | INFO | INFO |
| HL7_EXTRA_COMPONENT | WARNING | INFO | INFO |
| HL7_EXTRA_SUBCOMPONENT | WARNING | INFO | INFO |
| HL7_CUSTOM_RULE_VIOLATION | per rule severity | per rule severity | per rule severity |
| HL7_CUSTOM_RULE_RUNTIME_ERROR | per rule severity (default WARNING) | per rule severity (default WARNING) | per rule severity (default WARNING) |

---

## CEL runtime errors vs rule violations

| | `HL7_CUSTOM_RULE_VIOLATION` | `HL7_CUSTOM_RULE_RUNTIME_ERROR` |
|---|------------------------------|----------------------------------|
| **Meaning** | The rule **ran**; the check failed (assert false, require missing, forbid present). | The rule **did not complete** (helper threw / could not parse input). |
| **Typical path** | HL7 location from `errorPath`, e.g. `OBX[2]-5`, `PID[1]-8`. | HL7 path when known, else `rule[<id>].assert` / `rule[<id>].when`. |
| **Severity** | From the rule’s `severity` field (ERROR / WARNING / INFO). | Same — from the rule’s `severity` field; **WARNING** if omitted. |

`validateAs` with an **unknown** datatype code evaluates to **false** (assertion failure → `HL7_CUSTOM_RULE_VIOLATION`), not a runtime error. Invalid **location** (e.g. first argument is a field value) still produces `HL7_CUSTOM_RULE_RUNTIME_ERROR`.

If bad data must **hard-fail** validation, prefer schema/datatype checks or guarded rules (`valued('X') && validateAs('X', 'DT')`) before `toDTM`, so failures become `HL7_DATATYPE` or `HL7_CUSTOM_RULE_VIOLATION`, not runtime errors.

---

## CEL Custom Validation Rules

Rules are declared in the schema's `"rules"` array and compiled once at schema load time. They are evaluated after structural and field-level checks.

### Rule fields

| Field | Required | Description |
|-------|----------|-------------|
| `id` | yes | Unique identifier (used in error paths like `rule[id].assert`) |
| `name` | yes | Human-readable label (shown in diagnostic messages) |
| `assert` | one of three | CEL boolean expression |
| `require` | one of three | Shorthand: `valued('LOC')` — field must be non-empty |
| `forbid` | one of three | Shorthand: `!valued('LOC')` — field must be absent/empty |
| `when` | no | CEL boolean guard — rule is skipped when this evaluates to false |
| `message` | no | Human-readable violation message |
| `errorPath` | no | HL7 location path for the violation (e.g. `OBX-5`) |
| `severity` | no | `ERROR` (default), `WARNING`, or `INFO` |

### Scope inference

The iterator infers iteration scope from quoted HL7 location literals in `when` and `assert` (assert-first when a single segment dominates, frequency-based tie-breaking, then message instance counts). See `pkg/cel/hl7/iterator.go` for the full algorithm.

### CEL helper functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `msg(loc)` | `string → string` | Returns the field value at the given HL7 location for the current scope instance |
| `valued(loc)` | `string → bool` | True if the field at `loc` is non-empty and not `"\"\""` |
| `validateAs(loc, typeCode)` | `string, string → bool` | Validates the field at `loc` against the given HL7 primitive type code (e.g. `"NM"`, `"DT"`); `typeCode` can be a literal or `msg('OBX-2')`. Unknown codes return **false** (failed assertion), not a runtime error. |
| `matchesPattern(val, pattern)` | `string, string → bool` | RE2/Perl-compatible regex match with 50 ms ReDoS timeout; warns on empty input or timeout |
| `toDTM(loc)` | `string → timestamp` | Parses the HL7 DTM/TS value at `loc` into a CEL `timestamp`; warns on empty or invalid input |
| `toNumber(loc)` | `string → double` | Parses the value at `loc` as a float64; warns on empty or non-numeric input |

When a helper cannot complete (invalid regex, unparseable `toDTM`/`toNumber` input, etc.), the engine emits **`HL7_CUSTOM_RULE_RUNTIME_ERROR`** with severity from the rule (default **WARNING**). That keeps “rule engine could not run” separate from structural HL7 errors and from **`HL7_CUSTOM_RULE_VIOLATION`** (rule ran and failed).

### Example rules

```json
"rules": [
  {
    "id": "obx-5-required-when-nm",
    "name": "OBX-5 required when type is NM",
    "when": "msg('OBX-2') == 'NM'",
    "assert": "valued('OBX-5')",
    "message": "OBX-5 must be populated when OBX-2 is NM",
    "errorPath": "OBX-5",
    "severity": "ERROR"
  },
  {
    "id": "obx-5-valid-nm",
    "name": "OBX-5 must be numeric when OBX-2 is NM",
    "when": "msg('OBX-2') == 'NM' && valued('OBX-5')",
    "assert": "validateAs('OBX-5', msg('OBX-2'))",
    "message": "OBX-5 must be a valid numeric value when OBX-2 is NM",
    "errorPath": "OBX-5",
    "severity": "ERROR"
  },
  {
    "id": "pid-7-valid-date",
    "name": "PID-7 must be a valid date",
    "when": "valued('PID-7')",
    "assert": "validateAs('PID-7', 'DT')",
    "message": "PID-7 (date of birth) must be a valid HL7 DT",
    "errorPath": "PID-7",
    "severity": "WARNING"
  },
  {
    "id": "msh-9-present",
    "name": "MSH-9 must be populated",
    "require": "MSH-9",
    "message": "MSH-9 (message type) must be present",
    "errorPath": "MSH-9",
    "severity": "ERROR"
  }
]
```

---

## ProcessOptions

| Option | Effect on HL7 |
|--------|---------------|
| `CollectAllErrors` | `true` = collect all issues; `false` = stop after the first error-severity issue |
| `Mode` | `STRICT`, `NORMAL` (default), or `LENIENT` — controls severity bucketing |

**STRICT and `Process` errors:** When `Mode` is `STRICT` and `ProcessResult.Valid` is `false` (any ERROR-severity issue), `Process` also returns a non-nil Go error (`StrictProcessError`) while still populating `Errors` / `Warnings` / `Infos`. Warnings alone (including `HL7_CUSTOM_RULE_RUNTIME_ERROR` at WARNING) do not set `Valid` to false.

---

## Usage

### Via the schema engine (recommended)

```go
engine := schema.NewEngine()
result, err := engine.ProcessHL7WithSchema(
    rawHL7Bytes,
    schemaDefBytes,
    schema.ProcessOptions{
        CollectAllErrors: true,
        Mode:             schema.ValidationModeNormal,
    },
)
// result.Data is always the original rawHL7Bytes
// result.Errors / Warnings / Infos are bucketed by mode
```

### Direct package use

```go
msg, err := hl7.ParseMessage(rawHL7Bytes)

compiled, err := hl7.CompileHL7Schema(schemaDefBytes)
// compiled.Schema     — structural definition
// compiled.Registry   — datatype registry
// compiled.CELValidation — compiled CEL rules (nil if none declared)

match := hl7.MatchMessage(msg, compiled)
fieldErrs := hl7.ValidateMatchResult(match, msg, true, compiled.Registry)
```

### Message model (package `message`)

Nil-safe `*Message` methods: `Get`, `GetAtSegmentInstance`, `SegmentByName`, `NthSegmentByName`, and `SegmentInstanceCount` return empty / nil / zero when the receiver is nil (no panic).

**Locations:** paths like `MSH-12`, `PID-3(2).1` are parsed by `LocationParts` / `parseLocation`. **Segment names are matched case-insensitively** (e.g. `pid-5` and `PID-5` both resolve PID).

**Joining composite values**

| API | Delimiters used |
|-----|-----------------|
| `Component.String()`, `Repetition.String()`, `Field.String()` | `DefaultDelimiters()` only |
| `FormatWithDelimiters(d Delimiters)` on `Component` / `Repetition` / `Field` | The supplied `d` (use `msg.Delimiters` from a parsed message for wire-accurate `^` / `&`) |
| `msg.Get(loc)` / `msg.GetAtSegmentInstance(...)` | `msg.Delimiters` when building multi-part field values |
| `FieldValueOnSegment(seg, loc)` | No `Message` available — uses **default** delimiters via `effectiveDelimiters` (same idea as `String()` for isolated segment helpers) |

---

## Package layout

| File / package | Purpose |
|----------------|---------|
| `schema.go` | `HL7Schema`, `HL7SegmentDef`, `HL7FieldDef`, `CompiledHL7Schema`, `CompiledCELValidation` |
| `parser.go` | `Tokenize`, `ParseMessage`, `ParseMessageWithDelimiters`, escape decoding, size guards |
| `types.go` | Re-exports from `message` sub-package (type aliases for `Message`, `Segment`, `Field`, etc.) |
| `matcher.go` | `MatchMessage`, `matchSegments`, group start/reserve computation, depth guard |
| `validator.go` | `ValidateMatchResult`, `ValidateMessageTypeAndVersion`, field/length/truncation helpers |
| `datatype_validator.go` | `validateDataType`, composite decomposition, `flattenLeaves`, leaf validation |
| `header_validator.go` | MSH-9 message type and MSH-12 version comparison |
| `processor.go` | `HL7SchemaProcessor` — full pipeline, severity bucketing, early-stop logic |
| `message/` | `Message`, `Segment`, `Field`, `Component`, `Subcomponent`, `Delimiters`, `Get` / `FormatWithDelimiters`, location parsing (see [Message model](#message-model-package-message)) |
| `primitive/` | `ValidatePrimitive` — scalar type checks (NM, SI, DT, TM, DTM, TZ offset) |
| `datatypes/` | `Registry` — composite datatype definitions loaded from JSON; `Lookup` by version |
| `pkg/cel/` | Generic CEL engine (`Engine`, `InputRule`, `CompiledRule`, `ScopeIterator`) |
| `pkg/cel/hl7/` | HL7-specific CEL: `Engine()`, `CELRule`, `CompileHL7Rules`, `HL7ScopeIterator`, helper functions |
