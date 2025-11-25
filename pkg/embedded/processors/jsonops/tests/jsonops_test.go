package tests

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsonops"
)

func wrapJSONOpsInput(t *testing.T, data interface{}) []byte {
	t.Helper()
	envelope := map[string]interface{}{"data": data}
	bytes, err := json.Marshal(envelope)
	require.NoError(t, err)
	return bytes
}

func decodeJSONOpsEnvelope(t *testing.T, raw []byte) map[string]interface{} {
	t.Helper()
	var wrapper map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &wrapper))

	dataField, ok := wrapper["data"].(string)
	require.True(t, ok, "output missing data field")

	decoded, err := base64.StdEncoding.DecodeString(dataField)
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded, &result))
	return result
}

func sampleSchema() map[string]interface{} {
	return map[string]interface{}{
		"type": "OBJECT",
		"properties": map[string]interface{}{
			"email": map[string]interface{}{
				"type":     "STRING",
				"required": true,
			},
			"age": map[string]interface{}{
				"type": "NUMBER",
			},
			"status": map[string]interface{}{
				"type":    "STRING",
				"default": "active",
			},
		},
	}
}

func TestJSONOpsParseValidEnvelope(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())

	cfg := map[string]interface{}{
		"operation": "parse",
		"schema":    json.RawMessage(schemaBytes),
	}
	cfgBytes, _ := json.Marshal(cfg)

	input := wrapJSONOpsInput(t, map[string]interface{}{
		"email": "user@example.com",
		"age":   30,
	})

	out, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &result))
	require.Equal(t, "user@example.com", result["email"])
	require.Equal(t, float64(30), result["age"])
	require.Equal(t, "active", result["status"], "default applied")
}

func TestJSONOpsParseMissingSchema(t *testing.T) {
	executor := jsonops.NewExecutor()
	cfg := map[string]interface{}{"operation": "parse"}
	cfgBytes, _ := json.Marshal(cfg)

	input := wrapJSONOpsInput(t, map[string]interface{}{"email": "user@example.com"})

	_, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.Error(t, err)
}

func TestJSONOpsParseRequiresDataField(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())
	cfg := map[string]interface{}{"operation": "parse", "schema": json.RawMessage(schemaBytes)}
	cfgBytes, _ := json.Marshal(cfg)

	_, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         []byte(`{"email":"user@example.com"}`),
	})
	require.Error(t, err)
}

func TestJSONOpsProduceWrapsBase64(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())
	cfg := map[string]interface{}{"operation": "produce", "schema": json.RawMessage(schemaBytes)}
	cfgBytes, _ := json.Marshal(cfg)

	input := []byte(`{"email":"user@example.com","age":40,"extra":"ignore"}`)

	out, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.NoError(t, err)

	decoded := decodeJSONOpsEnvelope(t, out)
	require.Equal(t, "user@example.com", decoded["email"])
	require.Nil(t, decoded["extra"], "structure_data should drop unknown fields")
}

func TestJSONOpsProducePretty(t *testing.T) {
	executor := jsonops.NewExecutor()
	schemaBytes, _ := json.Marshal(sampleSchema())
	cfg := map[string]interface{}{
		"operation": "produce",
		"schema":    json.RawMessage(schemaBytes),
		"pretty":    true,
	}
	cfgBytes, _ := json.Marshal(cfg)

	input := []byte(`{"email":"user@example.com"}`)

	out, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.NoError(t, err)

	var wrapper map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &wrapper))
	data := wrapper["data"].(string)
	decoded, err := base64.StdEncoding.DecodeString(data)
	require.NoError(t, err)
	require.Contains(t, string(decoded), "\n", "pretty output should contain newlines")
}

func TestJSONOpsProduceStrictValidation(t *testing.T) {
	executor := jsonops.NewExecutor()
	schema := map[string]interface{}{
		"type": "OBJECT",
		"properties": map[string]interface{}{
			"email": map[string]interface{}{
				"type":     "STRING",
				"required": true,
				"validation": map[string]interface{}{
					"format": "email",
				},
			},
		},
	}
	schemaBytes, _ := json.Marshal(schema)

	cfg := map[string]interface{}{
		"operation":         "produce",
		"schema":            json.RawMessage(schemaBytes),
		"strict_validation": true,
	}
	cfgBytes, _ := json.Marshal(cfg)

	input := []byte(`{"email":"not-an-email"}`)

	_, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: cfgBytes,
		Input:         input,
	})
	require.Error(t, err)
}
