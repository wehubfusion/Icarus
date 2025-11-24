package output

import "time"

// StandardOutput represents the standardized output structure for all nodes
// All nodes (parent and embedded, success or failure) output this consistent format
type StandardOutput struct {
	Meta   MetaData       `json:"_meta"`
	Events EventEndpoints `json:"_events"`
	Error  *ErrorInfo     `json:"_error,omitempty"`
	Result interface{}    `json:"result"`
}

// MetaData contains metadata about the node execution
type MetaData struct {
	Status          string    `json:"status"` // "success", "failed", "skipped"
	NodeID          string    `json:"node_id"`
	PluginType      string    `json:"plugin_type"`
	ExecutionTimeMs int64     `json:"execution_time_ms"`
	Timestamp       time.Time `json:"timestamp"`
}

// EventEndpoints contains event trigger endpoints for routing
// Only one endpoint fires (has truthy value) based on execution result
type EventEndpoints struct {
	Success interface{} `json:"success"` // true on success, null on failure
	Error   interface{} `json:"error"`   // true on failure, null on success
}

// ErrorInfo contains detailed error information when execution fails
type ErrorInfo struct {
	Code      string                 `json:"code"`      // Error code for categorization (e.g., "HTTP_TIMEOUT")
	Message   string                 `json:"message"`   // Human-readable error message
	Retryable bool                   `json:"retryable"` // Whether the error is transient and retryable
	Details   map[string]interface{} `json:"details,omitempty"`
}
