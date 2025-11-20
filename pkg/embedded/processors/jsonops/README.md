# JSON Operations Processor

A schema-driven embedded processor for JSON data validation and transformation using Icarus schemas.

## Overview

The jsonops processor provides two core operations for JSON data processing in Icarus pipelines:

- **parse**: Validate and structure incoming JSON data against a schema
- **produce**: Validate and encode JSON data to base64 output

Both operations leverage the Icarus schema engine for comprehensive validation, default value application, and data structuring.

## Operations

### Parse

Parse validates incoming JSON data against an Icarus schema, applies defaults, and returns structured JSON.

**Use Cases:**
- Validate input data from external sources
- Apply default values to incomplete data
- Normalize data structure before processing

**Configuration:**
```json
{
  "operation": "parse",
  "schema_id": "user-input-schema-uuid-123"
}
```

**With Options:**
```json
{
  "operation": "parse",
  "schema_id": "user-input-schema-uuid-123",
  "apply_defaults": true,
  "structure_data": false,
  "strict_validation": false
}
```

**Input:**
```json
{
  "email": "user@example.com",
  "age": 30
}
```

**Output:**
```json
{
  "email": "user@example.com",
  "age": 30,
  "status": "active"
}
```
*Note: "status" was added from schema defaults*

---

### Produce

Produce validates JSON data against a schema, removes extra fields, encodes to base64, and returns the encoded output.

**Use Cases:**
- Prepare data for external API calls
- Ensure output conforms to schema specification
- Clean and encode data for transmission

**Configuration:**
```json
{
  "operation": "produce",
  "schema_id": "api-output-schema-uuid-456"
}
```

**With Options:**
```json
{
  "operation": "produce",
  "schema_id": "api-output-schema-uuid-456",
  "structure_data": true,
  "strict_validation": true,
  "pretty": false
}
```

**Input:**
```json
{
  "email": "user@example.com",
  "age": 30,
  "status": "active",
  "internal_field": "not in schema"
}
```

**Output:**
```json
{
  "result": "eyJlbWFpbCI6InVzZXJAZXhhbXBsZS5jb20iLCJhZ2UiOjMwLCJzdGF0dXMiOiJhY3RpdmUifQ==",
  "encoding": "base64"
}
```

**Decoded Data:**
```json
{
  "email": "user@example.com",
  "age": 30,
  "status": "active"
}
```
*Note: "internal_field" was removed by structure_data*

---

## Configuration Reference

### Flat Configuration Structure

```json
{
  "operation": "parse|produce",
  "schema_id": "uuid",
  "schema": { },
  "apply_defaults": true|false,
  "structure_data": true|false,
  "strict_validation": true|false,
  "pretty": true|false
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `operation` | string | Yes | Operation type: "parse" or "produce" |
| `schema_id` | string | Yes* | UUID reference to schema in Morpheus (enriched by Elysium) |
| `schema` | object | Yes* | Embedded Icarus schema definition (for testing/standalone) |
| `apply_defaults` | boolean | No | Apply default values from schema (see defaults below) |
| `structure_data` | boolean | No | Remove fields not defined in schema (see defaults below) |
| `strict_validation` | boolean | No | Fail immediately on validation errors (see defaults below) |
| `pretty` | boolean | No | Pretty-format JSON before encoding (produce only, default: false) |

*Either `schema_id` or `schema` must be provided. When using embedded nodes in Elysium, prefer `schema_id` as it will be automatically enriched.

### Operation-Specific Defaults

| Option | Parse Default | Produce Default | Rationale |
|--------|--------------|-----------------|-----------|
| `apply_defaults` | `true` | `false` | Parse fills missing fields; Produce expects complete data |
| `structure_data` | `false` | `true` | Parse allows extra fields; Produce cleans output |
| `strict_validation` | `false` | `true` | Parse is lenient; Produce ensures valid output |

---

## Schema Format

The processor uses the Icarus schema format. See [Olympus SCHEMA.md](../../../../../Olympus/SCHEMA.md) for full documentation.

**Example Schema:**
```json
{
  "type": "OBJECT",
  "properties": {
    "email": {
      "type": "STRING",
      "required": true,
      "validation": {
        "format": "email",
        "maxLength": 255
      }
    },
    "age": {
      "type": "NUMBER",
      "validation": {
        "minimum": 0,
        "maximum": 150
      }
    },
    "status": {
      "type": "STRING",
      "default": "active",
      "validation": {
        "enum": ["active", "inactive", "suspended"]
      }
    }
  }
}
```

**Supported Types:**
- STRING, NUMBER, BOOLEAN, DATE, DATETIME, BYTE
- OBJECT (nested objects)
- ARRAY (with item schemas)

**Validation Rules:**
- `required`: Field is mandatory
- `default`: Default value if not provided
- `format`: String format (email, uri, uuid, etc.)
- `pattern`: Regex pattern
- `minLength`, `maxLength`: String length constraints
- `minimum`, `maximum`: Numeric range constraints
- `enum`: Allowed values
- And many more...

---

## Usage Examples

### Example 1: Parse User Input

**Embedded Node Configuration:**
```json
{
  "nodeId": "parse-user-input",
  "pluginType": "plugin-jsonops",
  "order": 1,
  "configuration": {
    "operation": "parse",
    "schema_id": "user-registration-schema"
  }
}
```

**Input from HTTP request:**
```json
{
  "email": "john@example.com",
  "name": "John Doe"
}
```

**Output (with defaults applied):**
```json
{
  "email": "john@example.com",
  "name": "John Doe",
  "status": "pending",
  "created_at": "2025-11-13T10:00:00Z"
}
```

---

### Example 2: Produce API Output

**Embedded Node Configuration:**
```json
{
  "nodeId": "produce-api-response",
  "pluginType": "plugin-jsonops",
  "order": 5,
  "configuration": {
    "operation": "produce",
    "schema_id": "external-api-schema",
    "strict_validation": true
  }
}
```

**Input (from pipeline):**
```json
{
  "userId": 123,
  "email": "john@example.com",
  "internal_tracking_id": "xyz-123",
  "debug_info": "..."
}
```

**Output (structured and encoded):**
```json
{
  "result": "eyJ1c2VySWQiOjEyMywiZW1haWwiOiJqb2huQGV4YW1wbGUuY29tIn0=",
  "encoding": "base64"
}
```

**Decoded output sent to external API:**
```json
{
  "userId": 123,
  "email": "john@example.com"
}
```

---

### Example 3: Chaining Operations

**Pipeline with multiple jsonops nodes:**
```json
{
  "embeddedNodes": [
    {
      "nodeId": "parse-input",
      "pluginType": "plugin-jsonops",
      "order": 1,
      "configuration": {
        "operation": "parse",
        "schema_id": "input-schema",
        "apply_defaults": true,
        "strict_validation": true
      }
    },
    {
      "nodeId": "transform-data",
      "pluginType": "plugin-jsrunner",
      "order": 2,
      "configuration": {
        "script": "return { ...input, processed: true };"
      }
    },
    {
      "nodeId": "produce-output",
      "pluginType": "plugin-jsonops",
      "order": 3,
      "configuration": {
        "operation": "produce",
        "schema_id": "output-schema",
        "structure_data": true
      }
    }
  ]
}
```

---

## Error Handling

### Configuration Errors

**Missing operation:**
```json
{
  "error": "operation cannot be empty"
}
```

**Invalid operation:**
```json
{
  "error": "invalid operation 'transform', must be one of: parse, produce"
}
```

**Missing schema:**
```json
{
  "error": "either 'schema_id' or 'schema' must be provided"
}
```

**Unenriched schema_id:**
```json
{
  "error": "schema_id 'abc-123' was not enriched - ensure Elysium enrichment is configured"
}
```

### Validation Errors

**Strict validation (fails immediately):**
```
Error: schema processing failed: validation failed with 2 errors
```

**Non-strict validation (collects errors):**
The operation completes but the schema engine may return validation errors in the result.

### Input Errors

**Invalid JSON:**
```json
{
  "error": "input is not valid JSON"
}
```

---

## Integration with Elysium

When using jsonops in Elysium workflows, the `schema_id` field is automatically enriched:

**Before Enrichment (what you configure):**
```json
{
  "operation": "parse",
  "schema_id": "user-schema-uuid-123"
}
```

**After Enrichment (what Icarus receives):**
```json
{
  "operation": "parse",
  "schema_id": "user-schema-uuid-123",
  "schema": {
    "type": "OBJECT",
    "properties": { ... }
  }
}
```

Elysium's schema enricher fetches the full schema from Morpheus and injects it before execution.

---

## Best Practices

### 1. Use Schema References in Workflows

**Good (in Elysium):**
```json
{
  "operation": "parse",
  "schema_id": "user-schema"
}
```

**Acceptable (testing/standalone):**
```json
{
  "operation": "parse",
  "schema": { ... full schema ... }
}
```

### 2. Parse at Input, Produce at Output

```
External Input → [parse] → Processing → [produce] → External Output
```

- Parse validates and normalizes incoming data
- Produce validates and formats outgoing data

### 3. Leverage Operation Defaults

Don't override defaults unless necessary:

```json
{
  "operation": "parse",
  "schema_id": "user-schema"
}
```

This automatically gets:
- `apply_defaults: true` (fill missing fields)
- `structure_data: false` (allow extra fields)
- `strict_validation: false` (lenient)

### 4. Enable Strict Validation for Critical Operations

```json
{
  "operation": "produce",
  "schema_id": "payment-api-schema",
  "strict_validation": true
}
```

Ensures no invalid data is sent to external systems.

### 5. Use Pretty Format for Debugging

```json
{
  "operation": "produce",
  "schema_id": "debug-schema",
  "pretty": true
}
```

Makes it easier to inspect base64-encoded output during development.

---

## Migration from Previous Version

### Breaking Changes

The jsonops processor has been simplified to only two operations:

**Removed Operations:**
- `render` - Use jsrunner for template rendering
- `query` - Use jsrunner for JSON queries
- `transform` - Use jsrunner for transformations
- `validate` - Use `parse` or `produce` with schema validation

**Configuration Changes:**
- Flat configuration (no nested `params` object)
- Only `schema_id` or `schema` (no `schema_definition`)
- Operation-specific smart defaults

### Migration Guide

**Old (render):**
```json
{
  "operation": "render",
  "params": {
    "template": { ... }
  }
}
```

**New (use jsrunner):**
```json
{
  "script": "return { ...buildTemplate(input) };"
}
```

**Old (query):**
```json
{
  "operation": "query",
  "params": {
    "path": "user.email"
  }
}
```

**New (use jsrunner):**
```json
{
  "script": "return { email: input.user.email };"
}
```

**Old (validate):**
```json
{
  "operation": "validate",
  "params": {
    "schema": { ... }
  }
}
```

**New (use parse):**
```json
{
  "operation": "parse",
  "schema_id": "validation-schema",
  "strict_validation": true
}
```

**Old (schema with nested params):**
```json
{
  "operation": "schema",
  "params": {
    "schema_id": "user-schema",
    "apply_defaults": true
  }
}
```

**New (flat config):**
```json
{
  "operation": "parse",
  "schema_id": "user-schema",
  "apply_defaults": true
}
```

---

## Testing

Run tests:
```bash
cd pkg/embedded/processors/jsonops
go test -v
```

Run specific test:
```bash
go test -v -run TestParseOperation_ValidInput
```

---

## See Also

- [Icarus Schema Format](../../../../../Olympus/SCHEMA.md)
- [Embedded Node Enrichment](../../../../../Olympus/docs/ARCHITECTURE.md)
- [jsrunner Processor](../jsrunner/README.md)
- [Schema Engine](../../../schema/README.md)

