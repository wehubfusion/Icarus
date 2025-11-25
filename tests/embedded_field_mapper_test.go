package tests

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	embedded "github.com/wehubfusion/Icarus/pkg/embedded"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func TestEmbeddedFieldMapperApplyMappingsSimplePath(t *testing.T) {
	fm := embedded.NewFieldMapper(nil)
	storage := embedded.NewSmartStorage(nil)

	sourceData := map[string]interface{}{"name": "John Doe", "age": 30}
	sourceJSON, _ := json.Marshal(sourceData)
	sourceOutput := embedded.WrapSuccess("source-node-1", "test", 0, sourceJSON)
	storage.Set("source-node-1", map[string]interface{}{
		"_meta":   sourceOutput.Meta,
		"_events": sourceOutput.Events,
		"result":  sourceOutput.Result,
	}, nil)

	mappings := []message.FieldMapping{{
		SourceNodeID:         "source-node-1",
		SourceEndpoint:       "/name",
		DestinationEndpoints: []string{"/name"},
	}}

	result, err := fm.ApplyMappings(storage, mappings, []byte("{}"), -1)
	require.NoError(t, err)

	var resultMap map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &resultMap))
	assert.Equal(t, "John Doe", resultMap["name"])
	assert.Nil(t, resultMap["/name"])
}

func TestEmbeddedFieldMapperApplyMappingsNestedPath(t *testing.T) {
	fm := embedded.NewFieldMapper(nil)
	storage := embedded.NewSmartStorage(nil)

	sourceData := map[string]interface{}{
		"user": map[string]interface{}{"email": "john@example.com"},
	}
	sourceJSON, _ := json.Marshal(sourceData)
	sourceOutput := embedded.WrapSuccess("source-node-1", "test", 0, sourceJSON)
	storage.Set("source-node-1", map[string]interface{}{
		"_meta":   sourceOutput.Meta,
		"_events": sourceOutput.Events,
		"result":  sourceOutput.Result,
	}, nil)

	mappings := []message.FieldMapping{{
		SourceNodeID:         "source-node-1",
		SourceEndpoint:       "/user/email",
		DestinationEndpoints: []string{"/user/email"},
	}}

	result, err := fm.ApplyMappings(storage, mappings, []byte("{}"), -1)
	require.NoError(t, err)

	var resultMap map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &resultMap))
	userMap, ok := resultMap["user"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "john@example.com", userMap["email"])
}

func TestEmbeddedFieldMapperRootLevelMerge(t *testing.T) {
	fm := embedded.NewFieldMapper(nil)
	storage := embedded.NewSmartStorage(nil)

	sourceData := map[string]interface{}{
		"Assignment": []interface{}{map[string]interface{}{"id": 1, "category": "Bank"}},
		"Person":     []interface{}{map[string]interface{}{"name": "John", "email": "john@example.com"}},
	}
	sourceJSON, _ := json.Marshal(sourceData)
	sourceOutput := embedded.WrapSuccess("source-node-1", "test", 0, sourceJSON)
	storage.Set("source-node-1", map[string]interface{}{
		"_meta":   sourceOutput.Meta,
		"_events": sourceOutput.Events,
		"result":  sourceOutput.Result,
	}, nil)

	mappings := []message.FieldMapping{{
		SourceNodeID:         "source-node-1",
		SourceEndpoint:       "",
		DestinationEndpoints: []string{""},
	}}

	result, err := fm.ApplyMappings(storage, mappings, []byte("{}"), -1)
	require.NoError(t, err)

	var resultMap map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &resultMap))

	assert.Contains(t, resultMap, "Assignment")
	assert.Contains(t, resultMap, "Person")
}
