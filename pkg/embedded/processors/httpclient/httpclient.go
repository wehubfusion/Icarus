package httpclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

const (
	defaultRequestTimeout = 30 * time.Second
)

// HTTPClientNode implements HTTP client requests for embedded processing.
type HTTPClientNode struct {
	runtime.BaseNode
}

// NewHTTPClientNode creates a new HTTP client node.
func NewHTTPClientNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-http-client" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-http-client', got '%s'", config.PluginType)
	}
	return &HTTPClientNode{BaseNode: runtime.NewBaseNode(config)}, nil
}

// Process executes the HTTP request.
func (n *HTTPClientNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	var cfg Config
	if err := json.Unmarshal(input.RawConfig, &cfg); err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err), err))
	}

	if err := cfg.Validate(n.NodeId()); err != nil {
		return runtime.ErrorOutput(err)
	}

	// Build request
	req, err := n.buildRequest(input.Ctx, &cfg, input.Data)
	if err != nil {
		return runtime.ErrorOutput(err)
	}

	// Execute request
	client := &http.Client{Timeout: defaultRequestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return runtime.ErrorOutput(NewHTTPError(n.NodeId(), "request failed: "+err.Error(), err, 0, nil))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return runtime.ErrorOutput(NewHTTPError(n.NodeId(), "failed to read response body: "+err.Error(), err, resp.StatusCode, nil))
	}

	// Output: status (number), body (byte - base64 encoded for JSON serialization)
	output := map[string]interface{}{
		"status": resp.StatusCode,
		"body":   base64.StdEncoding.EncodeToString(body),
	}
	return runtime.SuccessOutput(output)
}

func (n *HTTPClientNode) buildRequest(ctx context.Context, cfg *Config, data map[string]interface{}) (*http.Request, error) {
	conn := cfg.Connection
	urlStr, _ := conn["url"].(string)

	method := getString(conn, "method", "POST")
	payloadBytes := n.extractPayload(data)
	if len(payloadBytes) == 0 && (method == "GET" || method == "HEAD") {
		method = "GET"
	} else if len(payloadBytes) > 0 && method == "" {
		method = "POST"
	}
	if method == "" {
		method = "POST"
	}

	var body io.Reader
	if len(payloadBytes) > 0 && method != "GET" && method != "HEAD" {
		body = bytes.NewReader(payloadBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, NewConfigError(n.NodeId(), "url", "invalid URL: "+err.Error(), err)
	}

	// Apply headers from manual_inputs (node config)
	for _, h := range cfg.ManualInputs {
		if h.Key != "" {
			req.Header.Set(h.Key, h.Value)
		}
	}

	// Apply auth
	n.applyAuth(req, conn)

	return req, nil
}

func (n *HTTPClientNode) applyAuth(req *http.Request, conn map[string]interface{}) {
	// Only Bearer auth supported (matches Nyx HTTP schema: url, method, auth_type, bearer_token)
	if strings.ToLower(getString(conn, "auth_type", "")) != "bearer" {
		return
	}
	if token := getString(conn, "bearer_token", ""); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func (n *HTTPClientNode) extractPayload(data map[string]interface{}) []byte {
	if data == nil {
		return nil
	}
	v, ok := data["payload"]
	if !ok || v == nil {
		return nil
	}
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		decoded, err := base64.StdEncoding.DecodeString(val)
		if err == nil {
			return decoded
		}
		return []byte(val)
	default:
		// Try JSON marshal for other types
		b, _ := json.Marshal(v)
		return b
	}
}

func getString(m map[string]interface{}, key, def string) string {
	if m == nil {
		return def
	}
	v, ok := m[key]
	if !ok || v == nil {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	return def
}
