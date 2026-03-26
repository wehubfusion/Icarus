package httpclient_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/httpclient"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func createTestNode(t *testing.T, nodeID string) *httpclient.HTTPClientNode {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-http-client",
		PluginType: "plugin-http-client",
		Embeddable: true,
		Depth:      0,
	}
	node, err := httpclient.NewHTTPClientNode(config)
	if err != nil {
		t.Fatalf("failed to create test node: %v", err)
	}
	return node.(*httpclient.HTTPClientNode)
}

func TestNewHTTPClientNode_InvalidPluginType(t *testing.T) {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     "node1",
		PluginType: "plugin-other",
		Embeddable: true,
	}
	_, err := httpclient.NewHTTPClientNode(config)
	if err == nil {
		t.Fatal("expected error for invalid plugin type")
	}
}

func TestProcess_InvalidJSONConfig(t *testing.T) {
	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"payload": "hello"},
		RawConfig: json.RawMessage(`{invalid json`),
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error for invalid JSON config")
	}
	if output.Data != nil {
		t.Fatal("expected nil data on error")
	}
}

func TestProcess_MissingConnection(t *testing.T) {
	node := createTestNode(t, "node1")
	rawCfg := `{"label":"test","connection_id":"conn-1","manual_inputs":[]}`
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"payload": "{}"},
		RawConfig: json.RawMessage(rawCfg),
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error when connection not enriched")
	}
}

func TestProcess_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s", r.Method)
		}
		if v := r.Header.Get("X-Custom"); v != "custom-value" {
			t.Errorf("X-Custom header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"connection": map[string]interface{}{
			"url":    server.URL,
			"method": "POST",
		},
		"manual_inputs": []map[string]string{{"key": "X-Custom", "value": "custom-value"}},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"payload": base64.StdEncoding.EncodeToString([]byte(`{"data":"test"}`))},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatal("expected non-nil data")
	}
	if status, ok := output.Data["status"].(int); !ok || status != 200 {
		t.Errorf("status: got %v", output.Data["status"])
	}
	bodyB64, ok := output.Data["body"].(string)
	if !ok {
		t.Fatalf("body type: got %T", output.Data["body"])
	}
	body, err := base64.StdEncoding.DecodeString(bodyB64)
	if err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("body: got %q", string(body))
	}
}

func TestProcess_GETWithNoPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method: got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"connection": map[string]interface{}{
			"url":    server.URL,
			"method": "GET",
		},
		"manual_inputs": []map[string]string{},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
	if status, ok := output.Data["status"].(int); !ok || status != 204 {
		t.Errorf("status: got %v", output.Data["status"])
	}
}

func TestProcess_BearerAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer secret-token-123" {
			t.Errorf("Authorization: got %q", auth)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"connection": map[string]interface{}{
			"url":         server.URL,
			"method":      "GET",
			"auth_type":   "bearer",
			"bearer_token": "secret-token-123",
		},
		"manual_inputs": []map[string]string{},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_URLFromInput_OverridesConnection(t *testing.T) {
	inputServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/" {
			// sanity: still hitting our server
		}
		if r.Method != http.MethodGet {
			t.Errorf("method: got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer inputServer.Close()

	otherServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("should not have used connection url")
	}))
	defer otherServer.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"url":   map[string]interface{}{"source": "input", "inputKey": "url"},
		"method": "GET",
		"connection": map[string]interface{}{
			"url":    otherServer.URL,
			"method": "POST",
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"url": inputServer.URL},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_URLFromInput_Missing_Fails(t *testing.T) {
	cfg := map[string]interface{}{
		"label": "test",
		"url":   map[string]interface{}{"source": "input", "inputKey": "url"},
		"method": "GET",
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error when url.source=input but input url missing")
	}
}

func TestProcess_URLFromConfig_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"url":   map[string]interface{}{"source": "config", "value": server.URL},
		"method": "POST",
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_URLFromConfig_MissingMethod_Fails(t *testing.T) {
	cfg := map[string]interface{}{
		"label": "test",
		"url":   map[string]interface{}{"source": "config", "value": "https://example.com"},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error when url.source=config but method missing")
	}
}

func TestProcess_HeadersFromInput_DynamicKeys(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Test"); v != "abc" {
			t.Errorf("X-Test header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":   "test",
		"url":     map[string]interface{}{"source": "config", "value": server.URL},
		"method":  "GET",
		"headers": map[string]interface{}{"source": "input", "inputKey": "manual_inputs"},
		"manual_inputs": []map[string]string{
			{"name": "X-Test"},
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"X-Test": "abc"},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_HeadersMerge_InputOverridesConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Test"); v != "from-input" {
			t.Errorf("X-Test header: got %q", v)
		}
		if v := r.Header.Get("X-Only"); v != "cfg" {
			t.Errorf("X-Only header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "config", "value": server.URL},
		"method": "GET",
		"headers": map[string]interface{}{
			"source": "merge",
			"value": map[string]interface{}{
				"X-Test": "from-config",
				"X-Only": "cfg",
			},
		},
		"manual_inputs": []map[string]string{
			{"name": "X-Test"},
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"X-Test": "from-input"},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_ConfigHeaders_AsArray(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Arr"); v != "1" {
			t.Errorf("X-Arr header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "config", "value": server.URL},
		"method": "GET",
		"headers": map[string]interface{}{
			"source": "config",
			"value": []map[string]string{
				{"key": "X-Arr", "value": "1"},
			},
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}
