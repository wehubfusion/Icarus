# Simple Condition Processor

A powerful embedded processor for evaluating conditional logic and enabling event-based routing in Icarus pipelines.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Input/Output Structure](#inputoutput-structure)
- [Configuration Reference](#configuration-reference)
- [Comparison Operators](#comparison-operators)
- [Logic Operators](#logic-operators)
- [Field Path Syntax](#field-path-syntax)
- [Data Types](#data-types)
- [Usage Examples](#usage-examples)
- [Event-Based Routing](#event-based-routing)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [Common Use Cases](#common-use-cases)
- [Testing](#testing)

---

## Overview

The simplecondition processor evaluates one or more conditions against JSON input data and provides detailed results for each condition. It supports complex conditional logic with AND/OR operators and enables event-based routing through special `true` and `false` output fields.

## Features

- **15 Comparison Operators**: Equality, numeric, string, collection, and existence checks
- **Flexible JSON Path Support**: Access nested fields, arrays, and complex structures using gjson syntax
- **AND/OR Logic**: Combine multiple conditions with logical operators
- **Case-Insensitive Comparisons**: Optional case-insensitive string matching
- **Detailed Results**: Individual condition results with actual vs expected values
- **Event-Based Routing**: Built-in `true`/`false` fields for conditional workflow routing
- **Type-Aware Comparisons**: Automatic or explicit data type handling
- **Comprehensive Error Reporting**: Detailed error messages for each condition

---

## Quick Start

### Simple Equality Check

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "check_status",
      "field_path": "user.status",
      "operator": "equals",
      "expected_value": "active"
    }
  ]
}
```

### Multiple Conditions with OR

```json
{
  "logic_operator": "OR",
  "conditions": [
    {
      "name": "is_admin",
      "field_path": "user.role",
      "operator": "equals",
      "expected_value": "admin"
    },
    {
      "name": "is_premium",
      "field_path": "user.subscription",
      "operator": "equals",
      "expected_value": "premium"
    }
  ]
}
```

### Numeric Comparison

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "check_age",
      "field_path": "user.age",
      "operator": "greater_than_or_equal",
      "expected_value": 18
    }
  ]
}
```

---

## Input/Output Structure

### Input Format

The processor accepts any valid JSON as input:

```json
{
  "user": {
    "name": "John Doe",
    "age": 25,
    "status": "active",
    "email": "john@example.com"
  }
}
```

### Output Format

The processor returns a comprehensive result object:

```json
{
  "result": true,
  "conditions": {
    "check_status": {
      "name": "check_status",
      "met": true,
      "actual_value": "active",
      "expected_value": "active",
      "operator": "equals"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

### Output Fields

| Field | Type | Description |
|-------|------|-------------|
| `result` | boolean | Overall result based on logic operator |
| `conditions` | object | Map of condition names to their individual results |
| `summary` | object | Aggregate statistics about condition evaluation |
| `true` | boolean/null | Event endpoint that fires when result is true |
| `false` | boolean/null | Event endpoint that fires when result is false |

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Input JSON: {"user": {"age": 25, "status": "active"}}          │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   Extract field values        │
         │   using field_path (gjson)    │
         └───────────┬───────────────────┘
                     │
                     ▼
         ┌───────────────────────────────┐
         │   Evaluate each condition     │
         │   with specified operator     │
         └───────────┬───────────────────┘
                     │
                     ▼
         ┌───────────────────────────────┐
         │   Combine results using       │
         │   logic_operator (AND/OR)     │
         └───────────┬───────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────┐
│ Output: {result, conditions, summary, true, false}             │
└────────────────────────────────────────────────────────────────┘
```

---

## Configuration Reference

### Configuration Structure

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "string",
      "field_path": "string",
      "operator": "string",
      "expected_value": "any",
      "data_type": "auto",
      "case_insensitive": false
    }
  ]
}
```

### Configuration Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `logic_operator` | string | No | `"AND"` | How to combine multiple conditions: `"AND"` or `"OR"` |
| `conditions` | array | Yes | - | Array of condition objects to evaluate |

### Condition Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Unique identifier for this condition |
| `field_path` | string | Yes | - | JSON path to the field (gjson syntax, no leading slash) |
| `operator` | string | Yes | - | Comparison operator to use |
| `expected_value` | any | No* | - | Value to compare against (*required except for existence operators) |
| `data_type` | string | No | `"auto"` | How to interpret values: `auto`, `string`, `number`, `boolean`, `null` |
| `case_insensitive` | boolean | No | `false` | Make string comparisons case-insensitive |

---

## Comparison Operators

### Equality Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `equals` | Values are equal | `"active" == "active"` → true |
| `not_equals` | Values are not equal | `"active" != "inactive"` → true |

**Configuration Example:**

```json
{
  "name": "check_status",
  "field_path": "status",
  "operator": "equals",
  "expected_value": "active"
}
```

---

### Numeric Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `greater_than` | Actual > Expected | `25 > 18` → true |
| `less_than` | Actual < Expected | `15 < 18` → true |
| `greater_than_or_equal` | Actual >= Expected | `18 >= 18` → true |
| `less_than_or_equal` | Actual <= Expected | `18 <= 18` → true |

**Configuration Example:**

```json
{
  "name": "check_age",
  "field_path": "user.age",
  "operator": "greater_than_or_equal",
  "expected_value": 18
}
```

**Note:** String numbers are automatically converted: `"25"` is treated as `25` for numeric comparisons.

---

### String Operators

| Operator | Description | Supports case_insensitive |
|----------|-------------|---------------------------|
| `contains` | String contains substring | ✅ Yes |
| `not_contains` | String does not contain substring | ✅ Yes |
| `starts_with` | String starts with prefix | ✅ Yes |
| `ends_with` | String ends with suffix | ✅ Yes |
| `regex` | String matches regex pattern | ❌ No |

**Configuration Example:**

```json
{
  "name": "check_email",
  "field_path": "user.email",
  "operator": "contains",
  "expected_value": "@example.com",
  "case_insensitive": true
}
```

**Regex Example:**

```json
{
  "name": "validate_phone",
  "field_path": "phone",
  "operator": "regex",
  "expected_value": "^\\+?[1-9]\\d{1,14}$"
}
```

---

### Collection Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `in` | Value exists in collection | `"apple" in ["apple", "banana"]` → true |
| `not_in` | Value does not exist in collection | `"grape" not in ["apple", "banana"]` → true |

**Configuration Example:**

```json
{
  "name": "check_role",
  "field_path": "user.role",
  "operator": "in",
  "expected_value": ["admin", "moderator", "editor"]
}
```

**Note:** The `expected_value` must be an array for `in` and `not_in` operators.

---

### Existence Operators

| Operator | Description | Requires expected_value |
|----------|-------------|-------------------------|
| `is_empty` | Value is empty | ❌ No |
| `is_not_empty` | Value is not empty | ❌ No |

**What is considered empty:**
- `null`
- `""` (empty string)
- `[]` (empty array)
- `{}` (empty object)
- `false` (boolean false)
- `0` (numeric zero)

**Configuration Example:**

```json
{
  "name": "has_email",
  "field_path": "user.email",
  "operator": "is_not_empty"
}
```

**Note:** `expected_value` is not required and will be ignored for existence operators.

---

## Logic Operators

### AND Operator

**All conditions must be met** for the overall result to be `true`.

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "is_adult",
      "field_path": "age",
      "operator": "greater_than_or_equal",
      "expected_value": 18
    },
    {
      "name": "has_email",
      "field_path": "email",
      "operator": "is_not_empty"
    }
  ]
}
```

**Truth Table:**

| Condition 1 | Condition 2 | Result |
|-------------|-------------|--------|
| ✅ Met | ✅ Met | ✅ **true** |
| ✅ Met | ❌ Not Met | ❌ **false** |
| ❌ Not Met | ✅ Met | ❌ **false** |
| ❌ Not Met | ❌ Not Met | ❌ **false** |

---

### OR Operator

**At least one condition must be met** for the overall result to be `true`.

```json
{
  "logic_operator": "OR",
  "conditions": [
    {
      "name": "is_admin",
      "field_path": "role",
      "operator": "equals",
      "expected_value": "admin"
    },
    {
      "name": "is_owner",
      "field_path": "role",
      "operator": "equals",
      "expected_value": "owner"
    }
  ]
}
```

**Truth Table:**

| Condition 1 | Condition 2 | Result |
|-------------|-------------|--------|
| ✅ Met | ✅ Met | ✅ **true** |
| ✅ Met | ❌ Not Met | ✅ **true** |
| ❌ Not Met | ✅ Met | ✅ **true** |
| ❌ Not Met | ❌ Not Met | ❌ **false** |

---

## Field Path Syntax

The processor uses **gjson** for path extraction. Paths use **dot notation** (not JSON path with slashes).

### Basic Paths

```json
// Input:
{
  "name": "John",
  "user": {
    "email": "john@example.com",
    "profile": {
      "age": 25
    }
  }
}

// Field paths:
"name"                  → "John"
"user.email"            → "john@example.com"
"user.profile.age"      → 25
```

### Array Access

```json
// Input:
{
  "users": [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
  ],
  "tags": ["important", "urgent"]
}

// Field paths:
"users.0.name"          → "Alice"
"users.1.age"           → 25
"tags.0"                → "important"
"users.#"               → 2 (array length)
"users.#.name"          → ["Alice", "Bob"] (all names)
```

### Advanced Queries

```json
// Conditional queries
"users.#(age>25).name"    // Names of users with age > 25
"items.#(price<100)#"     // Count of items with price < 100

// Nested array queries
"orders.#.items.#.price"  // All prices from all items in all orders
```

### Important: No Leading Slash

❌ **Incorrect:** `"/status"`, `"/user/email"`  
✅ **Correct:** `"status"`, `"user.email"`

### Escaping Special Characters

```json
// If field names contain dots or special characters
"user\\.name"             // Access field literally named "user.name"
"data\\.2024-11-13"       // Access field with dots and dashes
```

---

## Data Types

### Auto Type Detection (Default)

```json
{
  "name": "check_value",
  "field_path": "value",
  "operator": "equals",
  "expected_value": 42,
  "data_type": "auto"  // or omit this field
}
```

Auto mode attempts to match types intelligently:
- Numbers are compared numerically: `42 == "42"` → true
- Strings are compared as strings
- Booleans are compared as booleans

### Explicit Type Coercion

Force values to be compared as specific types:

```json
{
  "name": "check_age_as_string",
  "field_path": "age",
  "operator": "equals",
  "expected_value": "25",
  "data_type": "string"
}
```

**Available data types:**
- `auto` - Automatic type detection (default)
- `string` - Compare as strings
- `number` - Compare as numbers
- `boolean` - Compare as booleans
- `null` - Compare as null values

---

## Usage Examples

### Example 1: Simple Status Check

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "check_status",
      "field_path": "user.status",
      "operator": "equals",
      "expected_value": "active"
    }
  ]
}
```

**Input:**

```json
{
  "user": {
    "id": 123,
    "status": "active"
  }
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "check_status": {
      "name": "check_status",
      "met": true,
      "actual_value": "active",
      "expected_value": "active",
      "operator": "equals"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

---

### Example 2: Age Verification

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "is_adult",
      "field_path": "age",
      "operator": "greater_than_or_equal",
      "expected_value": 18
    }
  ]
}
```

**Input (Pass):**

```json
{
  "name": "John",
  "age": 25
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "is_adult": {
      "name": "is_adult",
      "met": true,
      "actual_value": 25,
      "expected_value": 18,
      "operator": "greater_than_or_equal"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

**Input (Fail):**

```json
{
  "name": "Jane",
  "age": 15
}
```

**Output:**

```json
{
  "result": false,
  "conditions": {
    "is_adult": {
      "name": "is_adult",
      "met": false,
      "actual_value": 15,
      "expected_value": 18,
      "operator": "greater_than_or_equal"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 0,
    "unmet_conditions": 1,
    "logic_operator": "AND"
  },
  "true": null,
  "false": true
}
```

---

### Example 3: Multiple Conditions with AND

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "is_adult",
      "field_path": "age",
      "operator": "greater_than_or_equal",
      "expected_value": 18
    },
    {
      "name": "has_email",
      "field_path": "email",
      "operator": "is_not_empty"
    }
  ]
}
```

**Input (Both conditions met):**

```json
{
  "age": 25,
  "email": "john@example.com"
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "is_adult": {
      "name": "is_adult",
      "met": true,
      "actual_value": 25,
      "expected_value": 18,
      "operator": "greater_than_or_equal"
    },
    "has_email": {
      "name": "has_email",
      "met": true,
      "actual_value": "john@example.com",
      "operator": "is_not_empty"
    }
  },
  "summary": {
    "total_conditions": 2,
    "met_conditions": 2,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

---

### Example 4: Role-Based Access with OR

**Configuration:**

```json
{
  "logic_operator": "OR",
  "conditions": [
    {
      "name": "is_admin",
      "field_path": "user.role",
      "operator": "equals",
      "expected_value": "admin"
    },
    {
      "name": "is_owner",
      "field_path": "user.role",
      "operator": "equals",
      "expected_value": "owner"
    }
  ]
}
```

**Input:**

```json
{
  "user": {
    "id": 123,
    "role": "admin"
  }
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "is_admin": {
      "name": "is_admin",
      "met": true,
      "actual_value": "admin",
      "expected_value": "admin",
      "operator": "equals"
    },
    "is_owner": {
      "name": "is_owner",
      "met": false,
      "actual_value": "admin",
      "expected_value": "owner",
      "operator": "equals"
    }
  },
  "summary": {
    "total_conditions": 2,
    "met_conditions": 1,
    "unmet_conditions": 1,
    "logic_operator": "OR"
  },
  "true": true,
  "false": null
}
```

---

### Example 5: Case-Insensitive String Matching

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "check_domain",
      "field_path": "email",
      "operator": "ends_with",
      "expected_value": "@EXAMPLE.COM",
      "case_insensitive": true
    }
  ]
}
```

**Input:**

```json
{
  "email": "user@example.com"
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "check_domain": {
      "name": "check_domain",
      "met": true,
      "actual_value": "user@example.com",
      "expected_value": "@EXAMPLE.COM",
      "operator": "ends_with"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

---

### Example 6: Collection Membership

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "is_authorized",
      "field_path": "user.role",
      "operator": "in",
      "expected_value": ["admin", "moderator", "editor"]
    }
  ]
}
```

**Input:**

```json
{
  "user": {
    "name": "Jane",
    "role": "moderator"
  }
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "is_authorized": {
      "name": "is_authorized",
      "met": true,
      "actual_value": "moderator",
      "expected_value": ["admin", "moderator", "editor"],
      "operator": "in"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

---

### Example 7: Regex Pattern Matching

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "valid_phone",
      "field_path": "phone",
      "operator": "regex",
      "expected_value": "^\\+?[1-9]\\d{1,14}$"
    }
  ]
}
```

**Input:**

```json
{
  "phone": "+14155552671"
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "valid_phone": {
      "name": "valid_phone",
      "met": true,
      "actual_value": "+14155552671",
      "expected_value": "^\\+?[1-9]\\d{1,14}$",
      "operator": "regex"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

---

### Example 8: Array Element Access

**Configuration:**

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "first_item_expensive",
      "field_path": "cart.items.0.price",
      "operator": "greater_than",
      "expected_value": 100
    }
  ]
}
```

**Input:**

```json
{
  "cart": {
    "items": [
      {"name": "Laptop", "price": 1200},
      {"name": "Mouse", "price": 25}
    ]
  }
}
```

**Output:**

```json
{
  "result": true,
  "conditions": {
    "first_item_expensive": {
      "name": "first_item_expensive",
      "met": true,
      "actual_value": 1200,
      "expected_value": 100,
      "operator": "greater_than"
    }
  },
  "summary": {
    "total_conditions": 1,
    "met_conditions": 1,
    "unmet_conditions": 0,
    "logic_operator": "AND"
  },
  "true": true,
  "false": null
}
```

---

## Event-Based Routing

The simplecondition processor includes special `true` and `false` fields in its output to enable **event-based routing** in Elysium workflows.

### How It Works

**When result is `true`:**

```json
{
  "result": true,
  "true": true,   // ✅ TRUE event fires
  "false": null   // ❌ FALSE event does NOT fire
}
```

**When result is `false`:**

```json
{
  "result": false,
  "true": null,   // ❌ TRUE event does NOT fire
  "false": true   // ✅ FALSE event fires
}
```

### Workflow Routing Example

```json
{
  "embeddedNodes": [
    {
      "nodeId": "check-age",
      "pluginType": "plugin-simple-condition",
      "order": 1,
      "configuration": {
        "logic_operator": "AND",
        "conditions": [
          {
            "name": "is_adult",
            "field_path": "age",
            "operator": "greater_than_or_equal",
            "expected_value": 18
          }
        ]
      }
    },
    {
      "nodeId": "adult-workflow",
      "order": 2,
      "triggerCondition": "check-age.true"
    },
    {
      "nodeId": "minor-workflow",
      "order": 2,
      "triggerCondition": "check-age.false"
    }
  ]
}
```

**Flow:**
- If age >= 18: Routes to `adult-workflow` node
- If age < 18: Routes to `minor-workflow` node

### Event Properties

- Both `true` and `false` fields are **always present** in output
- Only one event fires (has value `true`)
- The other event does not fire (has value `null`)
- Event fields are typed as `boolean | null`
- Use these fields with event-based routing in Elysium workflows

---

## Error Handling

### Configuration Errors

**Missing conditions:**

```
config error [conditions]: at least one condition must be specified
```

**Invalid logic operator:**

```
config error [logic_operator]: invalid logic operator 'XOR', must be 'AND' or 'OR'
```

**Duplicate condition names:**

```
config error [conditions]: duplicate condition name 'check_status'
```

**Empty condition name:**

```
config error [name]: condition name cannot be empty
```

**Empty field path:**

```
config error [field_path]: field_path cannot be empty for condition 'check_status'
```

**Invalid operator:**

```
config error [operator]: invalid operator 'matches' for condition 'check_email'
```

**Missing expected value:**

```
config error [expected_value]: expected_value is required for operator 'equals' in condition 'check_status'
```

**Invalid data type:**

```
config error [data_type]: invalid data_type 'integer' for condition 'check_age'
```

---

### Evaluation Errors

Evaluation errors are returned in the condition result's `error` field:

**Field not found:**

```json
{
  "name": "check_status",
  "met": false,
  "actual_value": null,
  "expected_value": "active",
  "operator": "equals",
  "error": "field 'user.status' not found in input"
}
```

**Type conversion error:**

```json
{
  "name": "check_age",
  "met": false,
  "actual_value": "invalid",
  "expected_value": 18,
  "operator": "greater_than",
  "error": "comparison error [greater_than]: actual value conversion failed: cannot convert string 'invalid' to number"
}
```

**Invalid regex pattern:**

```json
{
  "name": "validate_format",
  "met": false,
  "actual_value": "test",
  "expected_value": "[invalid(",
  "operator": "regex",
  "error": "comparison error [regex]: invalid regex pattern '[invalid(': error parsing regexp..."
}
```

**Collection type error:**

```json
{
  "name": "check_membership",
  "met": false,
  "actual_value": "value",
  "expected_value": "not_an_array",
  "operator": "in",
  "error": "comparison error [in]: expected value must be a collection: cannot convert type string to slice"
}
```

---

## Best Practices

### 1. Use Descriptive Condition Names

**Good:**

```json
{
  "name": "user_is_adult",
  "name": "email_domain_is_valid",
  "name": "payment_amount_exceeds_limit"
}
```

**Bad:**

```json
{
  "name": "condition1",
  "name": "check",
  "name": "test"
}
```

### 2. Group Related Conditions

For complex logic, use multiple simplecondition nodes rather than one complex node:

```json
// Instead of one node with 10 conditions
// Use multiple nodes:
{
  "nodeId": "check-user-eligibility",
  "conditions": [/* user-related checks */]
},
{
  "nodeId": "check-payment-validity",
  "conditions": [/* payment-related checks */]
}
```

### 3. Use Case-Insensitive for User Input

When comparing user-provided strings:

```json
{
  "name": "check_command",
  "field_path": "user_input",
  "operator": "equals",
  "expected_value": "start",
  "case_insensitive": true
}
```

### 4. Validate Before Complex Operations

Use simplecondition as a guard before expensive operations:

```json
{
  "embeddedNodes": [
    {
      "nodeId": "validate-input",
      "pluginType": "plugin-simple-condition",
      "configuration": {
        "logic_operator": "AND",
        "conditions": [
          {"name": "has_required_fields", "field_path": "data", "operator": "is_not_empty"}
        ]
      }
    },
    {
      "nodeId": "expensive-processing",
      "triggerCondition": "validate-input.true"
    }
  ]
}
```

### 5. Use Appropriate Operators

**For optional fields:**

```json
// Check existence first
{
  "logic_operator": "AND",
  "conditions": [
    {"name": "email_exists", "field_path": "email", "operator": "is_not_empty"},
    {"name": "email_valid", "field_path": "email", "operator": "contains", "expected_value": "@"}
  ]
}
```

**For enums/allowed values:**

```json
// Use 'in' operator
{
  "name": "valid_status",
  "field_path": "status",
  "operator": "in",
  "expected_value": ["pending", "approved", "rejected"]
}
```

### 6. Handle Missing Fields Gracefully

For existence operators, missing fields are handled:
- `is_empty`: Missing field → condition **met**
- `is_not_empty`: Missing field → condition **not met**

For other operators, missing fields cause an error in the condition result.

---

## Common Use Cases

### 1. User Authorization

Check if user has required permissions:

```json
{
  "logic_operator": "OR",
  "conditions": [
    {
      "name": "is_admin",
      "field_path": "user.role",
      "operator": "equals",
      "expected_value": "admin"
    },
    {
      "name": "is_resource_owner",
      "field_path": "user.id",
      "operator": "equals",
      "expected_value": "{{resource.owner_id}}"
    }
  ]
}
```

### 2. Data Validation

Validate input before processing:

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "has_email",
      "field_path": "email",
      "operator": "is_not_empty"
    },
    {
      "name": "valid_email_format",
      "field_path": "email",
      "operator": "regex",
      "expected_value": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    },
    {
      "name": "age_is_valid",
      "field_path": "age",
      "operator": "greater_than_or_equal",
      "expected_value": 0
    }
  ]
}
```

### 3. Feature Flags

Enable features based on user properties:

```json
{
  "logic_operator": "OR",
  "conditions": [
    {
      "name": "is_beta_tester",
      "field_path": "user.beta_tester",
      "operator": "equals",
      "expected_value": true
    },
    {
      "name": "in_beta_region",
      "field_path": "user.region",
      "operator": "in",
      "expected_value": ["US-WEST", "EU-CENTRAL"]
    }
  ]
}
```

### 4. Rate Limiting

Check if user has exceeded limits:

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "under_daily_limit",
      "field_path": "user.requests_today",
      "operator": "less_than",
      "expected_value": 1000
    },
    {
      "name": "under_burst_limit",
      "field_path": "user.requests_last_minute",
      "operator": "less_than",
      "expected_value": 50
    }
  ]
}
```

### 5. Business Logic Routing

Route orders based on value and type:

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "is_high_value",
      "field_path": "order.total",
      "operator": "greater_than",
      "expected_value": 1000
    },
    {
      "name": "requires_approval",
      "field_path": "order.type",
      "operator": "in",
      "expected_value": ["custom", "enterprise"]
    }
  ]
}
```

### 6. A/B Testing

Route users to test variants:

```json
{
  "logic_operator": "OR",
  "conditions": [
    {
      "name": "variant_a",
      "field_path": "user.id",
      "operator": "regex",
      "expected_value": "[02468]$"
    }
  ]
}
```

### 7. Content Filtering

Filter content based on age rating:

```json
{
  "logic_operator": "AND",
  "conditions": [
    {
      "name": "user_age_sufficient",
      "field_path": "user.age",
      "operator": "greater_than_or_equal",
      "expected_value": "{{content.min_age}}"
    },
    {
      "name": "not_restricted_region",
      "field_path": "user.region",
      "operator": "not_in",
      "expected_value": ["{{content.restricted_regions}}"]
    }
  ]
}
```

---

## Testing

Run tests:

```bash
cd pkg/embedded/processors/simplecondition
go test -v
```

Run specific test:

```bash
go test -v -run TestSimpleCondition_EventOutputs
```

Run tests with coverage:

```bash
go test -v -cover
```

---

## See Also

- [Icarus Embedded Processors](../../README.md)
- [jsonops Processor](../jsonops/README.md)
- [jsrunner Processor](../jsrunner/README.md)
- [dateformatter Processor](../dateformatter/README.md)
- [gjson Syntax Documentation](https://github.com/tidwall/gjson#path-syntax)

---

## Contributing

When adding new operators:

1. Add the operator constant to `config.go`
2. Implement the comparison function in `compare.go`
3. Add the operator to `compareValues()` dispatcher
4. Update `isValidOperator()` validation
5. Add tests for the new operator
6. Update this documentation

When modifying logic operators:

1. Update `LogicOperator` constants in `config.go`
2. Modify `calculateOverallResult()` in `simplecondition.go`
3. Update validation in `Config.Validate()`
4. Add tests for the new logic
5. Update this documentation

---

## License

Part of the Icarus project. See main repository for license information.

