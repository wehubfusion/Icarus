package jsrunner

import (
	"fmt"
	"strings"

	"github.com/dop251/goja"
)

// ErrorType categorizes different types of errors
type ErrorType string

const (
	ErrorTypeSyntax   ErrorType = "syntax_error"
	ErrorTypeRuntime  ErrorType = "runtime_error"
	ErrorTypeTimeout  ErrorType = "timeout_error"
	ErrorTypeSecurity ErrorType = "security_error"
	ErrorTypeConfig   ErrorType = "config_error"
	ErrorTypeInternal ErrorType = "internal_error"
)

// JSError represents a structured JavaScript execution error
type JSError struct {
	Type       ErrorType    `json:"type"`
	Message    string       `json:"message"`
	StackTrace []StackFrame `json:"stack_trace,omitempty"`
	Line       int          `json:"line,omitempty"`
	Column     int          `json:"column,omitempty"`
	Source     string       `json:"source,omitempty"`
}

// StackFrame represents a single frame in the stack trace
type StackFrame struct {
	FunctionName string `json:"function_name,omitempty"`
	FileName     string `json:"file_name,omitempty"`
	Line         int    `json:"line"`
	Column       int    `json:"column"`
}

// Error implements the error interface
func (e *JSError) Error() string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("[%s] %s", e.Type, e.Message))

	if e.Line > 0 {
		b.WriteString(fmt.Sprintf(" at line %d", e.Line))
		if e.Column > 0 {
			b.WriteString(fmt.Sprintf(", column %d", e.Column))
		}
	}

	if len(e.StackTrace) > 0 {
		b.WriteString("\nStack trace:")
		for i, frame := range e.StackTrace {
			if i >= 10 { // Limit stack trace output
				b.WriteString(fmt.Sprintf("\n  ... %d more frames", len(e.StackTrace)-i))
				break
			}
			b.WriteString("\n  at ")
			if frame.FunctionName != "" {
				b.WriteString(frame.FunctionName)
			} else {
				b.WriteString("<anonymous>")
			}
			if frame.FileName != "" {
				b.WriteString(fmt.Sprintf(" (%s:%d:%d)", frame.FileName, frame.Line, frame.Column))
			} else {
				b.WriteString(fmt.Sprintf(" (line %d:%d)", frame.Line, frame.Column))
			}
		}
	}

	return b.String()
}

// ParseGojaException converts a goja exception into a structured JSError
func ParseGojaException(exc *goja.Exception) *JSError {
	if exc == nil {
		return &JSError{
			Type:    ErrorTypeInternal,
			Message: "unknown error",
		}
	}

	jsErr := &JSError{
		Type:    ErrorTypeRuntime,
		Message: exc.Error(),
	}

	// Extract stack trace from the exception
	if exc.Value() != nil {
		vm := goja.New()
		obj := exc.Value().ToObject(vm)

		// Try to get the stack property
		if stack := obj.Get("stack"); stack != nil && stack != goja.Undefined() {
			jsErr.StackTrace = parseStackTrace(stack.String())
		}

		// Try to get line and column from Error object
		if line := obj.Get("line"); line != nil && line != goja.Undefined() {
			if lineNum, ok := line.Export().(int64); ok {
				jsErr.Line = int(lineNum)
			}
		}

		if col := obj.Get("column"); col != nil && col != goja.Undefined() {
			if colNum, ok := col.Export().(int64); ok {
				jsErr.Column = int(colNum)
			}
		}
	}

	// Categorize error type based on message
	errMsg := strings.ToLower(jsErr.Message)
	if strings.Contains(errMsg, "syntax") {
		jsErr.Type = ErrorTypeSyntax
	} else if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "interrupted") {
		jsErr.Type = ErrorTypeTimeout
	} else if strings.Contains(errMsg, "forbidden") || strings.Contains(errMsg, "not allowed") {
		jsErr.Type = ErrorTypeSecurity
	}

	return jsErr
}

// parseStackTrace parses a JavaScript stack trace string into structured frames
func parseStackTrace(stackStr string) []StackFrame {
	if stackStr == "" {
		return nil
	}

	lines := strings.Split(stackStr, "\n")
	frames := make([]StackFrame, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		frame := parseStackFrame(line)
		if frame != nil {
			frames = append(frames, *frame)
		}
	}

	return frames
}

// parseStackFrame parses a single stack frame line
// Handles various formats:
// - "at functionName (file:line:column)"
// - "at file:line:column"
// - "functionName@file:line:column"
func parseStackFrame(line string) *StackFrame {
	frame := &StackFrame{}

	// Remove "at " prefix if present
	line = strings.TrimPrefix(line, "at ")
	line = strings.TrimSpace(line)

	// Try to extract function name and location
	// Format: "functionName (location)" or "functionName@location"
	var location string

	if idx := strings.Index(line, "("); idx != -1 {
		// Format: "functionName (location)"
		if idx > 0 {
			frame.FunctionName = strings.TrimSpace(line[:idx])
		}
		closeParen := strings.Index(line[idx:], ")")
		if closeParen != -1 {
			location = line[idx+1 : idx+closeParen]
		}
	} else if idx := strings.Index(line, "@"); idx != -1 {
		// Format: "functionName@location"
		if idx > 0 {
			frame.FunctionName = strings.TrimSpace(line[:idx])
		}
		location = line[idx+1:]
	} else {
		// No function name, entire line is location
		location = line
	}

	// Parse location: "file:line:column" or just "line:column"
	if location != "" {
		parseLocation(location, frame)
	}

	return frame
}

// parseLocation extracts file, line, and column from a location string
func parseLocation(location string, frame *StackFrame) {
	parts := strings.Split(location, ":")

	switch len(parts) {
	case 3:
		// file:line:column
		frame.FileName = parts[0]
		fmt.Sscanf(parts[1], "%d", &frame.Line)
		fmt.Sscanf(parts[2], "%d", &frame.Column)
	case 2:
		// line:column or file:line
		var first, second int
		fmt.Sscanf(parts[0], "%d", &first)
		fmt.Sscanf(parts[1], "%d", &second)

		if first > 0 && second > 0 {
			// Assume line:column
			frame.Line = first
			frame.Column = second
		} else {
			// Might be file:line
			frame.FileName = parts[0]
			frame.Line = second
		}
	case 1:
		// Just line number
		fmt.Sscanf(parts[0], "%d", &frame.Line)
	}
}

// NewSyntaxError creates a new syntax error
func NewSyntaxError(message string, line, column int) *JSError {
	return &JSError{
		Type:    ErrorTypeSyntax,
		Message: message,
		Line:    line,
		Column:  column,
	}
}

// NewTimeoutError creates a new timeout error
func NewTimeoutError(timeout int) *JSError {
	return &JSError{
		Type:    ErrorTypeTimeout,
		Message: fmt.Sprintf("execution timeout after %dms", timeout),
	}
}

// NewSecurityError creates a new security error
func NewSecurityError(message string) *JSError {
	return &JSError{
		Type:    ErrorTypeSecurity,
		Message: message,
	}
}

// NewInternalError creates a new internal error
func NewInternalError(message string) *JSError {
	return &JSError{
		Type:    ErrorTypeInternal,
		Message: message,
	}
}

// WrapError wraps a regular error as an internal error
func WrapError(err error) *JSError {
	if err == nil {
		return nil
	}

	// Check if it's already a JSError
	if jsErr, ok := err.(*JSError); ok {
		return jsErr
	}

	return &JSError{
		Type:    ErrorTypeInternal,
		Message: err.Error(),
	}
}
