package httpclient

import "fmt"

// ConfigError represents a configuration validation error.
type ConfigError struct {
	NodeID  string
	Field   string
	Message string
	Err     error
}

func (e *ConfigError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("node %s: config error [%s]: %s", e.NodeID, e.Field, e.Message)
	}
	return fmt.Sprintf("node %s: config error: %s", e.NodeID, e.Message)
}

func (e *ConfigError) Unwrap() error { return e.Err }

func NewConfigError(nodeID, field, message string, err error) *ConfigError {
	return &ConfigError{NodeID: nodeID, Field: field, Message: message, Err: err}
}

// HTTPError represents an HTTP request or response failure.
type HTTPError struct {
	NodeID   string
	Message  string
	Err      error
	Status   int
	Response []byte
}

func (e *HTTPError) Error() string {
	if e.Status > 0 {
		return fmt.Sprintf("node %s: HTTP error (status %d): %s", e.NodeID, e.Status, e.Message)
	}
	return fmt.Sprintf("node %s: HTTP error: %s", e.NodeID, e.Message)
}

func (e *HTTPError) Unwrap() error { return e.Err }

// NewHTTPError creates an HTTPError.
func NewHTTPError(nodeID, message string, err error, status int, response []byte) *HTTPError {
	return &HTTPError{NodeID: nodeID, Message: message, Err: err, Status: status, Response: response}
}
