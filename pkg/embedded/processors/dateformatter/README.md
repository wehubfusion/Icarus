# Date Formatter Processor

A powerful embedded processor for date and time format conversion, timezone transformation, and date styling in Icarus pipelines.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Input/Output Structure](#inputoutput-structure)
- [Operations](#operations)
- [Configuration Reference](#configuration-reference)
- [Supported Formats](#supported-formats)
- [Date Styles](#date-styles)
- [Time Styles](#time-styles)
- [Usage Examples](#usage-examples)
- [Smart Input Normalization](#smart-input-normalization)
- [Timezone Reference](#timezone-reference)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [Common Use Cases](#common-use-cases)
- [Frontend Integration](#frontend-integration)
- [Testing](#testing)
- [See Also](#see-also)
- [Contributing](#contributing)

---

## Overview

The dateformatter processor provides flexible date/time format conversion with comprehensive timezone support. It can parse dates in various formats, convert between timezones, and output dates with customizable styling for both date and time components.

## Features

- **19+ Standard Formats**: RFC3339, RFC1123, ISO 8601, Unix timestamps, and more
- **Timezone Conversion**: Convert between any IANA timezones (e.g., UTC, America/New_York)
- **Flexible Date Styles**: Choose from 6 date formatting styles (YYYY-MM-DD, DD/MM/YYYY, etc.)
- **Flexible Time Styles**: Choose from 4 time formatting styles (24-hour, 12-hour, with/without seconds)
- **Smart Input Normalization**: Handles partial DateTime inputs automatically
- **Format Validation**: Comprehensive validation of formats, timezones, and styles

---

## Quick Start

### Minimal Configuration

Convert RFC3339 to human-readable format:

```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateTime"
  }
}
```

### With Timezone Conversion

```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "in_timezone": "UTC",
    "out_format": "DateTime",
    "out_timezone": "America/New_York"
  }
}
```

### With Custom Styles (US Format)

```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateTime",
    "out_timezone": "America/New_York",
    "date_style": "MM_DD_YYYY_SLASH",
    "time_style": "12_HOUR_HM"
  }
}
```

**Result:** `"2024-11-13T14:30:00Z"` → `"11/13/2024 09:30 AM"`

---

## Input/Output Structure

### Input Format

The dateformatter expects JSON input with a `data` field containing the date string:

```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

### Output Format

The dateformatter returns JSON output with a `data` field containing the formatted date string:

```json
{
  "result": "2024-11-13 14:30:00"
}
```

**Important:** The input must always be valid JSON with a `data` field. The processor does not accept raw date strings.

### Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Input JSON: {"result": "2024-11-13T14:30:00Z"}                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   Parse with in_format        │
         │   (RFC3339)                   │
         └───────────┬───────────────────┘
                     │
                     ▼
         ┌───────────────────────────────┐
         │   Apply in_timezone           │
         │   (if specified)              │
         └───────────┬───────────────────┘
                     │
                     ▼
         ┌───────────────────────────────┐
         │   Convert to out_timezone     │
         │   (if specified)              │
         └───────────┬───────────────────┘
                     │
                     ▼
         ┌───────────────────────────────┐
         │   Format with out_format      │
         │   (DateTime + styles)         │
         └───────────┬───────────────────┘
                     │
                     ▼
┌────────────────────────────────────────────────────────────────┐
│ Output JSON: {"result": "11-13-2024 09:30:00 AM"}                │
└────────────────────────────────────────────────────────────────┘
```

---

## Operations

The dateformatter currently supports one operation:

### Format

Convert dates between formats with optional timezone transformation and style customization.

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateTime"
  }
}
```

**With All Options:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "in_timezone": "UTC",
    "out_format": "DateTime",
    "out_timezone": "America/New_York",
    "date_style": "MM_DD_YYYY",
    "time_style": "12_HOUR"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output:**
```json
{
  "result": "11-13-2024 09:30:00 AM"
}
```

---

## Configuration Reference

### Configuration Structure

```json
{
  "operation": "format",
  "params": {
    "in_format": "string",
    "in_timezone": "string (optional)",
    "out_format": "string",
    "out_timezone": "string (optional)",
    "date_style": "string (optional)",
    "time_style": "string (optional)"
  }
}
```

### Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `operation` | string | Yes | Must be "format" |
| `params` | object | Yes | Format operation parameters |

### Format Parameters

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `in_format` | string | Yes | Input date format (see formats below) |
| `in_timezone` | string | No | Input timezone (IANA format, e.g., "UTC", "America/New_York") |
| `out_format` | string | Yes | Output date format (see formats below) |
| `out_timezone` | string | No | Output timezone (IANA format) |
| `date_style` | string | No | Date formatting style (only for DateOnly/DateTime) |
| `time_style` | string | No | Time formatting style (only for TimeOnly/DateTime) |

---

## Supported Formats

### Standard Go Time Formats

| Format | Layout | Example |
|--------|--------|---------|
| `Layout` | 01/02 03:04:05PM '06 -0700 | 11/13 02:30:00PM '24 -0500 |
| `ANSIC` | Mon Jan _2 15:04:05 2006 | Wed Nov 13 14:30:00 2024 |
| `UnixDate` | Mon Jan _2 15:04:05 MST 2006 | Wed Nov 13 14:30:00 UTC 2024 |
| `RubyDate` | Mon Jan 02 15:04:05 -0700 2006 | Wed Nov 13 14:30:00 -0500 2024 |
| `RFC822` | 02 Jan 06 15:04 MST | 13 Nov 24 14:30 UTC |
| `RFC822Z` | 02 Jan 06 15:04 -0700 | 13 Nov 24 14:30 -0500 |
| `RFC850` | Monday, 02-Jan-06 15:04:05 MST | Wednesday, 13-Nov-24 14:30:00 UTC |
| `RFC1123` | Mon, 02 Jan 2006 15:04:05 MST | Wed, 13 Nov 2024 14:30:00 UTC |
| `RFC1123Z` | Mon, 02 Jan 2006 15:04:05 -0700 | Wed, 13 Nov 2024 14:30:00 -0500 |
| `RFC3339` ⭐ | 2006-01-02T15:04:05Z07:00 | 2024-11-13T14:30:00Z |
| `RFC3339Nano` | 2006-01-02T15:04:05.999999999Z07:00 | 2024-11-13T14:30:00.000000000Z |
| `Kitchen` | 3:04PM | 2:30PM |
| `Stamp` | Jan _2 15:04:05 | Nov 13 14:30:00 |
| `StampMilli` | Jan _2 15:04:05.000 | Nov 13 14:30:00.000 |
| `StampMicro` | Jan _2 15:04:05.000000 | Nov 13 14:30:00.000000 |
| `StampNano` | Jan _2 15:04:05.000000000 | Nov 13 14:30:00.000000000 |

### Custom Formats with Style Support

| Format | Default Layout | Example | Customizable With |
|--------|---------------|---------|-------------------|
| `DateTime` ⭐ | 2006-01-02 15:04:05 | 2024-11-13 14:30:00 | `date_style`, `time_style` |
| `DateOnly` ⭐ | 2006-01-02 | 2024-11-13 | `date_style` |
| `TimeOnly` ⭐ | 15:04:05 | 14:30:00 | `time_style` |

⭐ Most commonly used formats

---

## Date Styles

Available when `out_format` is `DateOnly` or `DateTime`:

| Style | Layout | Example |
|-------|--------|---------|
| `YYYY_MM_DD` ⭐ | 2006-01-02 | 2024-11-13 |
| `DD_MM_YYYY` | 02-01-2006 | 13-11-2024 |
| `MM_DD_YYYY` | 01-02-2006 | 11-13-2024 |
| `YYYY_MM_DD_SLASH` | 2006/01/02 | 2024/11/13 |
| `DD_MM_YYYY_SLASH` | 02/01/2006 | 13/11/2024 |
| `MM_DD_YYYY_SLASH` | 01/02/2006 | 11/13/2024 |

⭐ Default style

---

## Time Styles

Available when `out_format` is `TimeOnly` or `DateTime`:

| Style | Layout | Example |
|-------|--------|---------|
| `24_HOUR` ⭐ | 15:04:05 | 14:30:00 |
| `12_HOUR` | 03:04:05 PM | 02:30:00 PM |
| `24_HOUR_HM` | 15:04 | 14:30 |
| `12_HOUR_HM` | 03:04 PM | 02:30 PM |

⭐ Default style

---

## Usage Examples

### Example 1: Simple Format Conversion

Convert RFC3339 to human-readable DateTime format:

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateTime"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output:**
```json
{
  "result": "2024-11-13 14:30:00"
}
```

---

### Example 2: Timezone Conversion

Convert UTC time to New York time:

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "in_timezone": "UTC",
    "out_format": "RFC3339",
    "out_timezone": "America/New_York"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output:**
```json
{
  "result": "2024-11-13T09:30:00-05:00"
}
```

---

### Example 3: Custom Date and Time Styles

Format with US-style date (MM-DD-YYYY) and 12-hour time:

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateTime",
    "date_style": "MM_DD_YYYY",
    "time_style": "12_HOUR"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output:**
```json
{
  "result": "11-13-2024 02:30:00 PM"
}
```

---

### Example 4: Date Only Extraction

Extract just the date from a full timestamp:

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateOnly",
    "date_style": "DD_MM_YYYY_SLASH"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output:**
```json
{
  "result": "13/11/2024"
}
```

---

### Example 5: Time Only Extraction

Extract just the time in 12-hour format:

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "TimeOnly",
    "time_style": "12_HOUR_HM"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output:**
```json
{
  "result": "02:30 PM"
}
```

---

### Example 6: Multi-Timezone Workflow

Convert from Tokyo time to London time:

**Configuration:**
```json
{
  "operation": "format",
  "params": {
    "in_format": "DateTime",
    "in_timezone": "Asia/Tokyo",
    "out_format": "DateTime",
    "out_timezone": "Europe/London",
    "date_style": "DD_MM_YYYY",
    "time_style": "24_HOUR"
  }
}
```

**Input:**
```json
{
  "result": "2024-11-13 23:30:00"
}
```

**Output:**
```json
{
  "result": "13-11-2024 14:30:00"
}
```

---

### Example 7: Embedded Node in Elysium

**Workflow Configuration:**
```json
{
  "embeddedNodes": [
    {
      "nodeId": "format-timestamp",
      "pluginType": "plugin-dateformatter",
      "order": 1,
      "configuration": {
        "operation": "format",
        "params": {
          "in_format": "RFC3339",
          "in_timezone": "UTC",
          "out_format": "DateTime",
          "out_timezone": "America/Los_Angeles",
          "date_style": "MM_DD_YYYY_SLASH",
          "time_style": "12_HOUR_HM"
        }
      }
    }
  ]
}
```

**Input from HTTP request:**
```json
{
  "result": "2024-11-13T14:30:00Z"
}
```

**Output (formatted for Pacific time):**
```json
{
  "result": "11/13/2024 06:30 AM"
}
```

---

## Smart Input Normalization

The dateformatter automatically handles partial DateTime inputs:

### Partial Date Inputs

**Input:** `"2024-11-13"` (date only)  
**Normalized:** `"2024-11-13 00:00:00"`

**Input:** `"2024-11-13 14:30"` (missing seconds)  
**Normalized:** `"2024-11-13 14:30:00"`

### Compact Date Formats

**Input:** `"20241113"` (YYYYMMDD compact)  
**Normalized:** `"2024-11-13"`  
*(Only for DateOnly format)*

This normalization happens transparently and allows flexible input handling.

---

## Timezone Reference

### IANA Timezone Format

Use IANA timezone identifiers for `in_timezone` and `out_timezone`:

**Americas:**
- `America/New_York` (Eastern Time)
- `America/Chicago` (Central Time)
- `America/Denver` (Mountain Time)
- `America/Los_Angeles` (Pacific Time)
- `America/Sao_Paulo` (Brazil)

**Europe:**
- `Europe/London` (UK)
- `Europe/Paris` (Central European Time)
- `Europe/Berlin` (Germany)
- `Europe/Moscow` (Russia)

**Asia:**
- `Asia/Tokyo` (Japan)
- `Asia/Shanghai` (China)
- `Asia/Dubai` (UAE)
- `Asia/Kolkata` (India)
- `Asia/Singapore` (Singapore)

**Pacific:**
- `Australia/Sydney`
- `Pacific/Auckland` (New Zealand)

**UTC:**
- `UTC` (Coordinated Universal Time)

Full list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

---

## Error Handling

The dateformatter provides detailed error messages for different error types:

### Configuration Errors

**Missing operation:**
```
config error [operation]: operation cannot be empty
```

**Invalid operation:**
```
config error [operation]: invalid operation 'convert', only 'format' is supported
```

**Missing required params:**
```
config error [params.in_format]: input format cannot be empty
config error [params.out_format]: output format cannot be empty
```

**Invalid format:**
```
config error [params.in_format]: invalid input format 'ISO8601'
config error [params.out_format]: invalid output format 'CustomFormat'
```

**Invalid date style:**
```
config error [params.date_style]: invalid date style 'INVALID_STYLE'
config error [params.date_style]: date_style can only be used with DateOnly or DateTime output formats
```

**Invalid time style:**
```
config error [params.time_style]: invalid time style 'INVALID_STYLE'
config error [params.time_style]: time_style can only be used with TimeOnly or DateTime output formats
```

### Timezone Errors

**Invalid input timezone:**
```
timezone error [America/New_Yor]: invalid input timezone (unknown time zone America/New_Yor)
```

**Invalid output timezone:**
```
timezone error [Europe/Londin]: invalid output timezone (unknown time zone Europe/Londin)
```

### Input Errors

**Invalid JSON:**
```
input error: failed to parse input JSON: invalid character...
```

**Missing data field:**
```
input error [data]: input must contain a 'data' field with a string value
```

**Empty date string:**
```
input error [data]: date string cannot be empty
```

**Non-string data field:**
```
input error [data]: input must contain a 'data' field with a string value
```

### Parse Errors

**Date parsing failure:**
```
parse error: failed to parse '2024-13-45' with format 'RFC3339': invalid date format (parsing time "2024-13-45" as "2006-01-02T15:04:05Z07:00": cannot parse...)
```

**Timezone parsing issues:**
```
parse error: failed to parse '2024-11-13 14:30:00' with format 'DateTime': timezone required but not provided in input
```

### Error Response Format

Errors are returned as plain error messages (not JSON). When used in Elysium workflows, these errors are captured by the workflow execution engine and can be handled appropriately.

---

## Best Practices

### 1. Use Standard Formats

**Preferred formats:**
- `RFC3339` - For APIs and data interchange
- `DateTime` - For human-readable output with style flexibility
- `DateOnly` - For date-only fields
- `TimeOnly` - For time-only fields

### 2. Always Specify Timezones for Conversion

**Good:**
```json
{
  "in_format": "DateTime",
  "in_timezone": "UTC",
  "out_format": "DateTime",
  "out_timezone": "America/New_York"
}
```

**Risky (ambiguous):**
```json
{
  "in_format": "DateTime",
  "out_format": "DateTime"
}
```

### 3. Use Appropriate Styles for Locales

**US Format:**
```json
{
  "date_style": "MM_DD_YYYY_SLASH",
  "time_style": "12_HOUR"
}
```

**European Format:**
```json
{
  "date_style": "DD_MM_YYYY_SLASH",
  "time_style": "24_HOUR"
}
```

**ISO Format:**
```json
{
  "date_style": "YYYY_MM_DD",
  "time_style": "24_HOUR"
}
```

### 4. Chain with Other Processors

Combine dateformatter with other processors for complex workflows:

```json
{
  "embeddedNodes": [
    {
      "nodeId": "parse-input",
      "pluginType": "plugin-jsonops",
      "order": 1,
      "configuration": {
        "operation": "parse",
        "schema_id": "event-schema"
      }
    },
    {
      "nodeId": "format-timestamp",
      "pluginType": "plugin-dateformatter",
      "order": 2,
      "configuration": {
        "operation": "format",
        "params": {
          "in_format": "RFC3339",
          "out_format": "DateTime",
          "out_timezone": "America/New_York",
          "date_style": "MM_DD_YYYY",
          "time_style": "12_HOUR"
        }
      }
    },
    {
      "nodeId": "produce-output",
      "pluginType": "plugin-jsonops",
      "order": 3,
      "configuration": {
        "operation": "produce",
        "schema_id": "output-schema"
      }
    }
  ]
}
```

### 5. Handle Partial Inputs

The processor handles partial DateTime inputs automatically:

```json
{
  "in_format": "DateTime",
  "out_format": "RFC3339"
}
```

Input `"2024-11-13"` → Automatically becomes `"2024-11-13 00:00:00"`

---

## Common Use Cases

### 1. API Timestamp Normalization

Convert various timestamp formats from external APIs to a standard format:

```json
{
  "operation": "format",
  "params": {
    "in_format": "UnixDate",
    "out_format": "RFC3339",
    "out_timezone": "UTC"
  }
}
```

### 2. User-Friendly Display

Convert RFC3339 timestamps to readable format for UI:

```json
{
  "operation": "format",
  "params": {
    "in_format": "RFC3339",
    "out_format": "DateTime",
    "out_timezone": "user_timezone",
    "date_style": "MM_DD_YYYY_SLASH",
    "time_style": "12_HOUR_HM"
  }
}
```

### 3. Database Timestamp Conversion

Convert between database timestamp formats:

```json
{
  "operation": "format",
  "params": {
    "in_format": "DateTime",
    "in_timezone": "UTC",
    "out_format": "RFC3339"
  }
}
```

### 4. Log Timestamp Standardization

Standardize log timestamps from different sources:

```json
{
  "operation": "format",
  "params": {
    "in_format": "Stamp",
    "in_timezone": "America/Los_Angeles",
    "out_format": "RFC3339Nano",
    "out_timezone": "UTC"
  }
}
```

### 5. Scheduled Job Timezone Handling

Convert scheduled times to local execution timezone:

```json
{
  "operation": "format",
  "params": {
    "in_format": "DateTime",
    "in_timezone": "UTC",
    "out_format": "DateTime",
    "out_timezone": "Asia/Tokyo",
    "time_style": "24_HOUR"
  }
}
```

---

## Frontend Integration

### Recommended UI Configuration

#### Input Section:
1. **Input Format** (required dropdown)
   - Show all 19 formats with examples
   - Default: `RFC3339`

2. **Input Country** (optional dropdown)
   - Populate timezone dropdown based on selection

3. **Input Timezone** (optional dropdown)
   - Enabled when country selected
   - Show all IANA timezones for selected country

#### Output Section:
1. **Output Format** (required dropdown)
   - Show all 19 formats with examples
   - Default: `DateTime`

2. **Output Country** (optional dropdown)
   - Populate timezone dropdown based on selection

3. **Output Timezone** (optional dropdown)
   - Enabled when country selected

4. **Date Style** (conditional dropdown)
   - Show only when `out_format` is `DateOnly` or `DateTime`
   - 6 options
   - Default: `YYYY_MM_DD`

5. **Time Style** (conditional dropdown)
   - Show only when `out_format` is `TimeOnly` or `DateTime`
   - 4 options
   - Default: `24_HOUR`

#### Live Preview:
```
Input:  "2024-11-13T14:30:00Z" (RFC3339, UTC)
Output: "11-13-2024 09:30 AM" (DateTime, America/New_York, MM_DD_YYYY, 12_HOUR_HM)
```

---

## Testing

Run tests:
```bash
cd pkg/embedded/processors/dateformatter
go test -v
```

Run specific test:
```bash
go test -v -run TestFormatOperation_BasicConversion
```

---

## See Also

- [Icarus Embedded Processors](../../README.md)
- [jsonops Processor](../jsonops/README.md)
- [jsrunner Processor](../jsrunner/README.md)
- [Go Time Package](https://pkg.go.dev/time)
- [IANA Time Zone Database](https://www.iana.org/time-zones)

---

## Contributing

When adding new formats:

1. Add the format constant to `types.go`
2. Add the layout mapping to `GetTimeFormatLayout()`
3. Update this documentation with examples
4. Add test cases in `dateformatter_test.go`

When adding new styles:

1. Add the style constant to `types.go` (DateStyle or TimeStyle)
2. Add the layout mapping to the appropriate getter function
3. Update the configuration validation logic
4. Update this documentation
5. Add test cases

---

## License

Part of the Icarus project. See main repository for license information.

