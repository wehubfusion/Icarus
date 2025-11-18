# JSRunner - JavaScript Processor for Embedded Nodes

The JSRunner processor enables execution of JavaScript code within embedded nodes, providing a flexible way to transform, validate, and process data using JavaScript.

## Features

- **Schema-Aware Processing**: Access input schemas directly in JavaScript code for meta-programming and validation
- **Raw Byte Output**: Always returns raw bytes (no JSON wrapping) for efficient processing
- **VM Pooling**: Reuses JavaScript VMs for performance
- **Configurable Security**: Multiple security levels (strict, standard, permissive)
- **Timeout Protection**: Configurable execution timeouts
- **Rich Utilities**: Optional utilities for JSON, console, timers, encoding, and fetch

## Configuration

```json
{
  "script": "// Your JavaScript code here",
  "schema_id": "user-schema",
  "inputs": [
    {"key": "name", "type": "STRING", "required": true},
    {"key": "age", "type": "NUMBER", "required": false}
  ],
  "timeout": 20000,
  "enabled_utilities": ["json"],
  "security_level": "strict"
}
```

### Configuration Options

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `script` | string | Yes | - | JavaScript code to execute |
| `schema_id` | string | No | - | Reference to schema in Morpheus (enriched by Elysium) |
| `schema` | object | No | - | Full schema definition (injected by enrichment) |
| `inputs` | array | No | - | Manual input field definitions. Each field should have `key`, `type`, and optionally `required`. Alternative to schema. |
| `timeout` | number | No | 20000 | Maximum execution time in milliseconds |
| `enabled_utilities` | array | No | `["json"]` | Available utilities in JavaScript |
| `security_level` | string | No | `"strict"` | Security level: strict, standard, or permissive |
| `memory_limit_mb` | number | No | 80 | Memory limit (informational) |
| `max_stack_depth` | number | No | 100 | Maximum call stack depth |
| `allow_network_access` | boolean | No | false | Enable network utilities (requires permissive mode) |

## JavaScript Context

### Available Global Variables

Your JavaScript code has access to the following global variables:

#### `input` (always available)

The input data passed to the node. Always present, may be an empty object if no input is provided.

```javascript
// Access input fields
var userName = input.name;
var userAge = input.age;

// Return transformed data
({
  fullName: input.firstName + " " + input.lastName,
  age: input.age
})
```

#### `schema` (when provided)

The input schema definition, enriched from Morpheus by Elysium. Available when `schema_id` is specified in the configuration.

**Note:** `schema` and `inputs` are mutually exclusive. If `inputs` is provided, `schema` will not be available.

```javascript
// Access schema properties
var fields = Object.keys(schema.properties);

// Check required fields
var requiredFields = fields.filter(function(key) {
  return schema.properties[key].required === true;
});

// Use schema for validation
var errors = [];
for (var key in schema.properties) {
  var field = schema.properties[key];
  var value = input[key];
  
  if (field.required && value === undefined) {
    errors.push("Missing required field: " + key);
  }
}
```

#### `inputSchema` (when manual inputs are provided)

When `inputs` are defined in the configuration, they are available as `inputSchema` in JavaScript. This provides a schema-like structure based on your manual input definitions.

**Note:** `inputSchema` and `schema` are mutually exclusive. If `inputs` is provided, `schema` will not be available.

```javascript
// Access manual input definitions
if (typeof inputSchema !== 'undefined') {
  var fields = Object.keys(inputSchema.properties);
  
  // Check field types
  for (var key in inputSchema.properties) {
    var fieldType = inputSchema.properties[key].type;
    console.log("Field " + key + " has type " + fieldType);
  }
  
  // Use inputSchema for validation
  var errors = [];
  for (var key in inputSchema.properties) {
    var field = inputSchema.properties[key];
    var value = input[key];
    
    // Check required fields
    if (field.required && value === undefined) {
      errors.push("Missing required field: " + key);
    }
    
    // Basic type checking
    if (value !== undefined) {
      if (field.type === "NUMBER" && typeof value !== "number") {
        errors.push(key + " must be a number");
      }
      if (field.type === "STRING" && typeof value !== "string") {
        errors.push(key + " must be a string");
      }
    }
  }
}
```

## Output Format

**JSRunner always returns raw bytes**, not wrapped JSON. This provides consistency and efficiency.

### Output Behavior

| JavaScript Return Type | Output |
|----------------------|--------|
| String | Raw string as bytes |
| Number | JSON-marshaled number (`42` → `"42"`) |
| Boolean | JSON-marshaled boolean (`true` → `"true"`) |
| Object | JSON-marshaled object |
| Array | JSON-marshaled array |
| null | `"null"` as bytes |

### Examples

```javascript
// String output
"Hello World"
// Output: Hello World (raw bytes)

// Number output
42
// Output: 42 (marshaled to JSON)

// Object output
({name: "John", age: 30})
// Output: {"age":30,"name":"John"} (marshaled to JSON)

// Array output
[1, 2, 3]
// Output: [1,2,3] (marshaled to JSON)
```

## Schema-Driven Processing

### Example: Field Validation

```javascript
var errors = [];

for (var key in schema.properties) {
  var field = schema.properties[key];
  var value = input[key];
  
  // Check required fields
  if (field.required && value === undefined) {
    errors.push("Missing required field: " + key);
  }
  
  // Type validation
  if (value !== undefined) {
    if (field.type === "NUMBER" && typeof value !== "number") {
      errors.push(key + " must be a number");
    }
    if (field.type === "STRING" && typeof value !== "string") {
      errors.push(key + " must be a string");
    }
  }
  
  // Range validation for numbers
  if (field.type === "NUMBER" && value !== undefined) {
    if (field.min !== undefined && value < field.min) {
      errors.push(key + " is below minimum: " + value + " < " + field.min);
    }
    if (field.max !== undefined && value > field.max) {
      errors.push(key + " is above maximum: " + value + " > " + field.max);
    }
  }
}

// Return result
if (errors.length > 0) {
  JSON.stringify({valid: false, errors: errors});
} else {
  JSON.stringify({valid: true, data: input});
}
```

### Example: Apply Defaults

```javascript
var output = {};

for (var key in schema.properties) {
  var field = schema.properties[key];
  var value = input[key];
  
  // Apply default if value is missing
  if (value === undefined && field.default !== undefined) {
    output[key] = field.default;
  } else {
    output[key] = value;
  }
}

JSON.stringify(output);
```

### Example: Extract Required Fields

```javascript
var requiredFields = [];

for (var key in schema.properties) {
  if (schema.properties[key].required === true) {
    requiredFields.push(key);
  }
}

// Extract only required fields from input
var output = {};
for (var i = 0; i < requiredFields.length; i++) {
  var field = requiredFields[i];
  if (input[field] !== undefined) {
    output[field] = input[field];
  }
}

JSON.stringify(output);
```

## Integration with Elysium

When used in Elysium workflows, the `schema_id` is automatically enriched:

### Before Enrichment (Workflow Definition)

```json
{
  "embedded_nodes": [
    {
      "plugin_type": "plugin-jsrunner",
      "configuration": {
        "script": "// Use schema here",
        "schema_id": "user-schema"
      }
    }
  ]
}
```

### After Enrichment (What JSRunner Receives)

```json
{
  "script": "// Use schema here",
  "schema": {
    "type": "OBJECT",
    "properties": {
      "name": {"type": "STRING", "required": true},
      "age": {"type": "NUMBER", "min": 0, "max": 150}
    }
  }
}
```

The enrichment is handled transparently by Elysium's enrichment framework - you don't need to implement anything!

## Use Cases

### 1. Custom Validation Logic

Implement complex validation rules that go beyond schema validation:

```javascript
// Check password strength
if (input.password && input.password.length < 8) {
  JSON.stringify({error: "Password too short"});
} else if (!/[A-Z]/.test(input.password)) {
  JSON.stringify({error: "Password must contain uppercase letter"});
} else {
  JSON.stringify({valid: true, data: input});
}
```

### 2. Data Transformation

Transform data structures on the fly:

```javascript
// Flatten nested structure
var flat = {
  user_name: input.user.name,
  user_email: input.user.email,
  order_id: input.order.id,
  order_total: input.order.total
};

JSON.stringify(flat);
```

### 3. Conditional Processing

Apply different logic based on input data:

```javascript
if (input.user_type === "premium") {
  // Premium users get full data
  JSON.stringify(input);
} else {
  // Free users get limited data
  JSON.stringify({
    id: input.id,
    name: input.name
  });
}
```

### 4. Schema-Driven Field Mapping

Use schema to dynamically map fields:

```javascript
var mapped = {};

for (var key in schema.properties) {
  var field = schema.properties[key];
  
  // Check for field alias
  var sourceKey = field.alias || key;
  mapped[key] = input[sourceKey];
}

JSON.stringify(mapped);
```

### 5. Data Enrichment

Add computed fields based on input:

```javascript
var enriched = JSON.parse(JSON.stringify(input)); // Clone

// Add computed fields
enriched.full_name = input.first_name + " " + input.last_name;
enriched.is_adult = input.age >= 18;
enriched.processed_at = Date.now();

JSON.stringify(enriched);
```

## Error Handling

Errors in JavaScript code are returned as structured error objects:

```json
{
  "error": {
    "type": "ReferenceError",
    "message": "undefined is not defined",
    "stack": "at line 5:10"
  }
}
```

### Error Types

- **SyntaxError**: Invalid JavaScript syntax
- **ReferenceError**: Accessing undefined variables
- **TypeError**: Type-related errors
- **RangeError**: Value out of range
- **TimeoutError**: Execution exceeded timeout
- **InternalError**: VM or runtime errors

## Performance Considerations

### VM Pooling

JSRunner uses VM pooling to reuse JavaScript execution contexts:

- Default pool size: 5 VMs
- Maximum idle time: 5 minutes
- Warm-up: Pre-creates VMs on initialization

### Best Practices

1. **Keep scripts small**: Large scripts increase execution time
2. **Avoid loops on large arrays**: Use native JavaScript methods when possible
3. **Reuse executors**: Create one executor per configuration and reuse it
4. **Set appropriate timeouts**: Balance between safety and performance
5. **Use schema efficiently**: Cache schema-derived data when possible

## Security

### Security Levels

- **strict** (default): Minimal utilities, maximum restrictions
- **standard**: Balanced security and functionality
- **permissive**: More utilities, including network access

### Sandboxing

All JavaScript code runs in a sandboxed environment:

- No file system access
- No process spawning
- No native module loading
- Configurable network access (permissive mode only)

## Testing

See `schema_test.go` for comprehensive examples of:

- Schema access patterns
- Raw byte output handling
- Validation logic
- Transformation logic
- Error handling

## Migration Guide

### Migrating from Old Output Format

**Old format (wrapped):**

```json
{"result": "Hello World"}
```

**New format (raw bytes):**

```
Hello World
```

**What to change:**

1. Remove any code that expects `{"result": ...}` wrapper
2. Parse output directly as raw bytes
3. For objects, parse bytes as JSON: `JSON.parse(outputBytes)`
4. For strings, use bytes as-is: `string(outputBytes)`

## Advanced Examples

### Dynamic Schema Validation

```javascript
function validateField(key, value, fieldSchema) {
  if (fieldSchema.required && value === undefined) {
    return "Missing required field: " + key;
  }
  
  if (value !== undefined) {
    if (fieldSchema.type === "STRING" && typeof value !== "string") {
      return key + " must be a string";
    }
    if (fieldSchema.type === "NUMBER" && typeof value !== "number") {
      return key + " must be a number";
    }
    if (fieldSchema.type === "BOOLEAN" && typeof value !== "boolean") {
      return key + " must be a boolean";
    }
    
    // String validations
    if (fieldSchema.type === "STRING" && value) {
      if (fieldSchema.min_length && value.length < fieldSchema.min_length) {
        return key + " is too short";
      }
      if (fieldSchema.max_length && value.length > fieldSchema.max_length) {
        return key + " is too long";
      }
      if (fieldSchema.pattern) {
        var regex = new RegExp(fieldSchema.pattern);
        if (!regex.test(value)) {
          return key + " does not match pattern";
        }
      }
    }
    
    // Number validations
    if (fieldSchema.type === "NUMBER" && typeof value === "number") {
      if (fieldSchema.min !== undefined && value < fieldSchema.min) {
        return key + " is below minimum";
      }
      if (fieldSchema.max !== undefined && value > fieldSchema.max) {
        return key + " is above maximum";
      }
    }
  }
  
  return null;
}

var errors = [];
for (var key in schema.properties) {
  var error = validateField(key, input[key], schema.properties[key]);
  if (error) {
    errors.push(error);
  }
}

if (errors.length > 0) {
  JSON.stringify({valid: false, errors: errors});
} else {
  JSON.stringify({valid: true, data: input});
}
```

## See Also

- [Icarus Embedded Nodes](../../README.md)
- [Elysium Enrichment Framework](../../../../../Olympus/elysium/README.md)
- [Morpheus Schema Documentation](../../../../../Olympus/docs/morpheus/SCHEMA.md)

