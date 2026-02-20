package httpclient

import "encoding/json"

// HeaderItem represents a key-value header pair.
type HeaderItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Config defines the configuration for the HTTP client processor.
type Config struct {
	Label        string       `json:"label"`
	ConnectionID string       `json:"connection_id"`
	ManualInputs []HeaderItem `json:"manual_inputs"`
	// Connection is populated by Elysium enrichment (connection_id -> connection details from Nyx).
	// Contains: url, method, auth_type, bearer_token, basic_username, basic_password,
	// api_key_header, api_key_value, custom_headers, etc.
	Connection map[string]interface{} `json:"connection"`
}

// UnmarshalJSON supports both raw config (with connection_id) and enriched config (with connection).
func (c *Config) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if v, ok := raw["label"]; ok {
		_ = json.Unmarshal(v, &c.Label)
	}
	if v, ok := raw["connection_id"]; ok {
		_ = json.Unmarshal(v, &c.ConnectionID)
	}
	if v, ok := raw["manual_inputs"]; ok {
		_ = json.Unmarshal(v, &c.ManualInputs)
	}
	if v, ok := raw["connection"]; ok {
		var conn map[string]interface{}
		if err := json.Unmarshal(v, &conn); err == nil {
			c.Connection = conn
		}
	}

	return nil
}

// Validate checks if the configuration is valid.
func (c *Config) Validate(nodeID string) error {
	if c.Connection == nil {
		if c.ConnectionID == "" {
			return NewConfigError(nodeID, "connection_id", "connection_id is required", nil)
		}
		return NewConfigError(nodeID, "connection", "connection was not enriched (connection_id present but connection missing); ensure Elysium enrichment runs before processing", nil)
	}

	urlVal, ok := c.Connection["url"]
	if !ok || urlVal == nil {
		return NewConfigError(nodeID, "connection.url", "connection must contain url", nil)
	}
	urlStr, ok := urlVal.(string)
	if !ok || urlStr == "" {
		return NewConfigError(nodeID, "connection.url", "connection url must be a non-empty string", nil)
	}

	return nil
}
