package embedded

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func TestFieldMapper_SetFieldAtPath(t *testing.T) {
	fm := NewFieldMapper(nil)

	tests := []struct {
		name     string
		path     string
		value    interface{}
		expected map[string]interface{}
	}{
		{
			name:  "set simple field /name",
			path:  "/name",
			value: "John Doe",
			expected: map[string]interface{}{
				"name": "John Doe",
			},
		},
		{
			name:  "set nested field /user/email",
			path:  "/user/email",
			value: "john@example.com",
			expected: map[string]interface{}{
				"user": map[string]interface{}{
					"email": "john@example.com",
				},
			},
		},
		{
			name:  "set field without leading slash",
			path:  "age",
			value: 30,
			expected: map[string]interface{}{
				"age": 30,
			},
		},
		{
			name:  "set multiple fields",
			path:  "/name",
			value: "Jane",
			expected: map[string]interface{}{
				"name": "Jane",
				"age":  30, // from previous test
			},
		},
	}

	data := make(map[string]interface{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm.setFieldAtPath(data, tt.path, tt.value)

			// Check that the specific field was set correctly
			if tt.path == "/name" || tt.path == "name" {
				assert.Equal(t, tt.value, data["name"])
			} else if tt.path == "/user/email" {
				if userMap, ok := data["user"].(map[string]interface{}); ok {
					assert.Equal(t, tt.value, userMap["email"])
				} else {
					t.Errorf("Expected user to be a map")
				}
			}
		})
	}
}

func TestFieldMapper_ApplyMappings_SimplePath(t *testing.T) {
	fm := NewFieldMapper(nil)
	registry := NewOutputRegistry()

	// Create a source output with StandardOutput format
	sourceOutput := `{
		"_meta": {"status": "success"},
		"_events": {"success": true},
		"result": {
			"name": "John Doe",
			"age": 30
		}
	}`
	registry.Set("source-node-1", []byte(sourceOutput))

	// Create field mapping: /name from source -> /name to destination
	mappings := []message.FieldMapping{
		{
			SourceNodeID:         "source-node-1",
			SourceEndpoint:       "/name",
			DestinationEndpoints: []string{"/name"},
			Iterate:              false,
		},
	}

	// Apply mappings
	result, err := fm.ApplyMappings(registry, mappings, []byte("{}"))
	require.NoError(t, err)

	// Parse result
	var resultMap map[string]interface{}
	err = json.Unmarshal(result, &resultMap)
	require.NoError(t, err)

	// Verify structure - should be {"name": "John Doe"} not {"/name": "John Doe"}
	assert.Equal(t, "John Doe", resultMap["name"])
	assert.Nil(t, resultMap["/name"], "Should not have /name key")
}

func TestFieldMapper_ApplyMappings_NestedPath(t *testing.T) {
	fm := NewFieldMapper(nil)
	registry := NewOutputRegistry()

	// Create a source output
	sourceOutput := `{
		"_meta": {"status": "success"},
		"result": {
			"user": {
				"email": "john@example.com"
			}
		}
	}`
	registry.Set("source-node-1", []byte(sourceOutput))

	// Create field mapping: /user/email from source -> /user/email to destination
	mappings := []message.FieldMapping{
		{
			SourceNodeID:         "source-node-1",
			SourceEndpoint:       "/user/email",
			DestinationEndpoints: []string{"/user/email"},
			Iterate:              false,
		},
	}

	// Apply mappings
	result, err := fm.ApplyMappings(registry, mappings, []byte("{}"))
	require.NoError(t, err)

	// Parse result
	var resultMap map[string]interface{}
	err = json.Unmarshal(result, &resultMap)
	require.NoError(t, err)

	// Verify nested structure
	userMap, ok := resultMap["user"].(map[string]interface{})
	require.True(t, ok, "user should be a map")
	assert.Equal(t, "john@example.com", userMap["email"])
}

func TestFieldMapper_ApplyMappings_EmptyEndpoints_RootLevelMerge(t *testing.T) {
	fm := NewFieldMapper(nil)
	registry := NewOutputRegistry()

	// Create a source output with StandardOutput format containing ESR-like data
	sourceOutput := `{
		"_meta": {"status": "success", "node_id": "source-node-1"},
		"_events": {"success": true},
		"result": {
			"Assignment": [{"id": 1, "category": "Bank"}, {"id": 2, "category": "Permanent"}],
			"Person": [{"name": "John", "email": "john@example.com"}],
			"Location": [{"id": 100, "name": "Hospital"}],
			"Position": [{"id": 200, "title": "Nurse"}]
		}
	}`
	registry.Set("source-node-1", []byte(sourceOutput))

	// Create field mapping: empty sourceEndpoint and empty destinationEndpoint
	// This should extract entire data field and merge at root level
	mappings := []message.FieldMapping{
		{
			SourceNodeID:         "source-node-1",
			SourceEndpoint:       "",
			DestinationEndpoints: []string{""},
			Iterate:              false,
		},
	}

	// Apply mappings with empty destination input
	result, err := fm.ApplyMappings(registry, mappings, []byte("{}"))
	require.NoError(t, err)

	// Parse result
	var resultMap map[string]interface{}
	err = json.Unmarshal(result, &resultMap)
	require.NoError(t, err)

	// Verify entire data was merged at root level
	assert.Contains(t, resultMap, "Assignment", "Assignment array should be at root level")
	assert.Contains(t, resultMap, "Person", "Person array should be at root level")
	assert.Contains(t, resultMap, "Location", "Location array should be at root level")
	assert.Contains(t, resultMap, "Position", "Position array should be at root level")

	// Verify Assignment array structure
	assignments, ok := resultMap["Assignment"].([]interface{})
	require.True(t, ok, "Assignment should be an array")
	assert.Len(t, assignments, 2)

	assignment1, ok := assignments[0].(map[string]interface{})
	require.True(t, ok, "Assignment item should be a map")
	assert.Equal(t, float64(1), assignment1["id"])
	assert.Equal(t, "Bank", assignment1["category"])

	// Verify Person array structure
	persons, ok := resultMap["Person"].([]interface{})
	require.True(t, ok, "Person should be an array")
	assert.Len(t, persons, 1)

	person1, ok := persons[0].(map[string]interface{})
	require.True(t, ok, "Person item should be a map")
	assert.Equal(t, "John", person1["name"])
	assert.Equal(t, "john@example.com", person1["email"])

	// Verify Location array
	locations, ok := resultMap["Location"].([]interface{})
	require.True(t, ok, "Location should be an array")
	assert.Len(t, locations, 1)

	// Verify Position array
	positions, ok := resultMap["Position"].([]interface{})
	require.True(t, ok, "Position should be an array")
	assert.Len(t, positions, 1)
}
