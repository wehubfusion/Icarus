package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/constantvalue"
)

func TestConstantValueExecutor_String(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": [
				{
					"name": "nationalinsurance",
					"dataType": "STRING",
					"string": "235346"
				}
			]
		}`),
	}

	result, err := executor.Execute(context.Background(), config)
	require.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	assert.Equal(t, "235346", output["nationalinsurance"])
}

func TestConstantValueExecutor_EmptyString(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": [
				{
					"name": "Date",
					"dataType": "STRING",
					"string": ""
				}
			]
		}`),
	}

	result, err := executor.Execute(context.Background(), config)
	require.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	assert.Equal(t, "", output["Date"])
}

func TestConstantValueExecutor_ZeroString(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": [
				{
					"name": "0",
					"dataType": "STRING",
					"string": "0"
				}
			]
		}`),
	}

	result, err := executor.Execute(context.Background(), config)
	require.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	assert.Equal(t, "0", output["0"])
}

func TestConstantValueExecutor_Number(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": [
				{
					"name": "count",
					"dataType": "NUMBER",
					"number": "42"
				}
			]
		}`),
	}

	result, err := executor.Execute(context.Background(), config)
	require.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	// JSON numbers are unmarshaled as float64
	assert.Equal(t, float64(42), output["count"])
}

func TestConstantValueExecutor_Boolean(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": [
				{
					"name": "enabled",
					"dataType": "BOOLEAN",
					"boolean": true
				}
			]
		}`),
	}

	result, err := executor.Execute(context.Background(), config)
	require.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	assert.Equal(t, true, output["enabled"])
}

func TestConstantValueExecutor_MultipleFields(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": [
				{
					"name": "name",
					"dataType": "STRING",
					"string": "test"
				},
				{
					"name": "enabled",
					"dataType": "BOOLEAN",
					"boolean": true
				},
				{
					"name": "count",
					"dataType": "NUMBER",
					"number": "99"
				}
			]
		}`),
	}

	result, err := executor.Execute(context.Background(), config)
	require.NoError(t, err)

	var output map[string]interface{}
	err = json.Unmarshal(result, &output)
	require.NoError(t, err)

	assert.Equal(t, "test", output["name"])
	assert.Equal(t, true, output["enabled"])
	// JSON numbers are unmarshaled as float64
	assert.Equal(t, float64(99), output["count"])
}

func TestConstantValueExecutor_NoFields(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:     "test-node",
		PluginType: "plugin-constant-value",
		Configuration: []byte(`{
			"fields": []
		}`),
	}

	_, err := executor.Execute(context.Background(), config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no fields configured")
}

func TestConstantValueExecutor_InvalidJSON(t *testing.T) {
	executor := constantvalue.NewExecutor()

	config := embedded.NodeConfig{
		NodeID:        "test-node",
		PluginType:    "plugin-constant-value",
		Configuration: []byte(`invalid json`),
	}

	_, err := executor.Execute(context.Background(), config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse configuration")
}

func TestConstantValueExecutor_PluginType(t *testing.T) {
	executor := constantvalue.NewExecutor()
	assert.Equal(t, "plugin-constant-value", executor.PluginType())
}

