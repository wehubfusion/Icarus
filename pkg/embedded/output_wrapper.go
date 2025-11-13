package embedded

import (
	"encoding/json"
	"time"
)

// WrapSuccess wraps a successful node execution output in the StandardOutput format
func WrapSuccess(nodeID, pluginType string, execTime int64, output []byte) []byte {
	var data interface{}
	if err := json.Unmarshal(output, &data); err != nil {
		// If unmarshaling fails, store as raw string
		data = string(output)
	}

	wrapped := StandardOutput{
		Meta: MetaData{
			Status:          "success",
			NodeID:          nodeID,
			PluginType:      pluginType,
			ExecutionTimeMs: execTime,
			Timestamp:       time.Now(),
		},
		Events: EventEndpoints{
			Success: true,
			Error:   nil,
		},
		Error: nil,
		Data:  data,
	}

	result, _ := json.Marshal(wrapped)
	return result
}

// WrapError wraps a failed node execution in the StandardOutput format
func WrapError(nodeID, pluginType string, execTime int64, err error) []byte {
	wrapped := StandardOutput{
		Meta: MetaData{
			Status:          "failed",
			NodeID:          nodeID,
			PluginType:      pluginType,
			ExecutionTimeMs: execTime,
			Timestamp:       time.Now(),
		},
		Events: EventEndpoints{
			Success: nil,
			Error:   true,
		},
		Error: &ErrorInfo{
			Code:      CategorizeError(err),
			Message:   err.Error(),
			Retryable: IsRetryable(err),
			Details:   ExtractErrorDetails(err),
		},
		Data: nil,
	}

	result, _ := json.Marshal(wrapped)
	return result
}

// WrapSkipped wraps a skipped node execution in the StandardOutput format
func WrapSkipped(nodeID, pluginType string, reason string) []byte {
	wrapped := StandardOutput{
		Meta: MetaData{
			Status:          "skipped",
			NodeID:          nodeID,
			PluginType:      pluginType,
			ExecutionTimeMs: 0,
			Timestamp:       time.Now(),
		},
		Events: EventEndpoints{
			Success: nil,
			Error:   nil,
		},
		Error: &ErrorInfo{
			Code:      "SKIPPED",
			Message:   reason,
			Retryable: false,
		},
		Data: nil,
	}

	result, _ := json.Marshal(wrapped)
	return result
}

