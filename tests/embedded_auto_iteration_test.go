package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	embedded "github.com/wehubfusion/Icarus/pkg/embedded"
)

// Mirrors pkg/embedded/auto_iteration_test.go but exercises through exported helpers.
func TestEmbeddedDetectAutoIteration(t *testing.T) {
	processor := embedded.NewProcessor(embedded.NewExecutorRegistry())

	t.Run("Detects data envelope with array", func(t *testing.T) {
		input := []byte(`{"data": [1, 2, 3]}`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.True(t, isArray)
		assert.Len(t, items, 3)
	})

	t.Run("Detects root-level array", func(t *testing.T) {
		input := []byte(`[{"name": "a"}, {"name": "b"}]`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.True(t, isArray)
		assert.Len(t, items, 2)
	})

	t.Run("Detects array of data envelopes", func(t *testing.T) {
		input := []byte(`[{"data": "value1"}, {"data": "value2"}]`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.True(t, isArray)
		assert.Len(t, items, 2)
	})

	t.Run("Detects empty array in data field", func(t *testing.T) {
		input := []byte(`{"data": []}`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.True(t, isArray)
		assert.Len(t, items, 0)
	})

	t.Run("Detects empty root-level array", func(t *testing.T) {
		input := []byte(`[]`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.True(t, isArray)
		assert.Len(t, items, 0)
	})

	t.Run("Does not detect single object", func(t *testing.T) {
		input := []byte(`{"name": "test"}`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.False(t, isArray)
		assert.Nil(t, items)
	})

	t.Run("Does not detect data field with non-array", func(t *testing.T) {
		input := []byte(`{"data": "string value"}`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.False(t, isArray)
		assert.Nil(t, items)
	})

	t.Run("Handles invalid JSON", func(t *testing.T) {
		input := []byte(`{invalid json}`)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.False(t, isArray)
		assert.Nil(t, items)
	})

	t.Run("Handles empty input", func(t *testing.T) {
		input := []byte(``)
		isArray, items, err := embedded.DetectAutoIterationForTests(processor, input)
		require.NoError(t, err)
		assert.False(t, isArray)
		assert.Nil(t, items)
	})
}
