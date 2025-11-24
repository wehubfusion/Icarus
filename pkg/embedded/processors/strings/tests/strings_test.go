package tests

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/embedded"
	stringsproc "github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
)

func executeStringsOp(t *testing.T, executor *stringsproc.Executor, config map[string]interface{}, input map[string]interface{}) map[string]interface{} {
	t.Helper()

	configBytes, err := json.Marshal(config)
	require.NoError(t, err)

	inputBytes, err := json.Marshal(input)
	require.NoError(t, err)

	output, err := executor.Execute(context.Background(), embedded.NodeConfig{
		Configuration: configBytes,
		Input:         inputBytes,
	})
	require.NoError(t, err)

	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(output, &result))
	return result
}

func TestStringsConcatenateWithSeparator(t *testing.T) {
	executor := stringsproc.NewExecutor()

	result := executeStringsOp(t, executor, map[string]interface{}{
		"operation": "concatenate",
		"params": map[string]interface{}{
			"separator": "-",
			"parts":     []string{"alpha", "beta", "gamma"},
		},
	}, map[string]interface{}{})

	assert.Equal(t, "alpha-beta-gamma", result["value"])
}

func TestStringsReplaceRegex(t *testing.T) {
	executor := stringsproc.NewExecutor()

	result := executeStringsOp(t, executor, map[string]interface{}{
		"operation": "replace",
		"params": map[string]interface{}{
			"string":    "order-12345",
			"old":       "\\d+",
			"new":       "#####",
			"use_regex": true,
		},
	}, map[string]interface{}{})

	assert.Equal(t, "order-#####", result["value"])
}

func TestStringsBase64EncodeDecode(t *testing.T) {
	executor := stringsproc.NewExecutor()

	encoded := executeStringsOp(t, executor, map[string]interface{}{
		"operation": "base64_encode",
		"params": map[string]interface{}{
			"string": "hello world",
		},
	}, map[string]interface{}{})

	decoded := executeStringsOp(t, executor, map[string]interface{}{
		"operation": "base64_decode",
		"params": map[string]interface{}{
			"string": encoded["value"],
		},
	}, map[string]interface{}{})

	assert.Equal(t, "hello world", decoded["value"])
}
