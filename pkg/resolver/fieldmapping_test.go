package resolver

import (
	"encoding/json"
	"testing"
)

// TestFlatKeyExtraction tests the flat-key extraction functionality
func TestFlatKeyExtraction(t *testing.T) {
	// Simulate flat keys from StandardUnitOutput format
	flatKeys := map[string]interface{}{
		"16a4f8ef-541f-4a1f-8ff2-82b8fbcc0fde-/data[0]/name[0]":                                        "ALEX",
		"16a4f8ef-541f-4a1f-8ff2-82b8fbcc0fde-/data[1]/name[1]":                                        "JORDAN",
		"2e0aa499-92ff-462b-807e-581c4a861307-/data[0]/assignments[0]/details/topics[0]/courseName[0]": "ALGEBRA",
		"2e0aa499-92ff-462b-807e-581c4a861307-/data[0]/assignments[0]/details/topics[1]/courseName[1]": "GEOMETRY",
		"2e0aa499-92ff-462b-807e-581c4a861307-/data[0]/assignments[1]/details/topics[0]/courseName[0]": "BIOLOGY",
		"2e0aa499-92ff-462b-807e-581c4a861307-/data[1]/assignments[0]/details/topics[0]/courseName[0]": "WORLD WAR II",
	}

	t.Run("Simple scalar extraction", func(t *testing.T) {
		result := extractFromFlatKeys(
			flatKeys,
			"16a4f8ef-541f-4a1f-8ff2-82b8fbcc0fde",
			"/data",
			"/data//nameUpper",
			false,
		)

		if result == nil {
			t.Fatal("Expected result, got nil")
		}

		// extractFromFlatKeys returns an array of values for setFieldAtPath to use
		resultArray, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected array for path with //, got %T", result)
		}

		if len(resultArray) != 2 {
			t.Fatalf("Expected 2 items in result array, got %d", len(resultArray))
		}

		if resultArray[0] != "ALEX" {
			t.Errorf("Expected resultArray[0]='ALEX', got %v", resultArray[0])
		}

		if resultArray[1] != "JORDAN" {
			t.Errorf("Expected resultArray[1]='JORDAN', got %v", resultArray[1])
		}
	})

	t.Run("Multi-level nested array extraction", func(t *testing.T) {
		result := extractFromFlatKeys(
			flatKeys,
			"2e0aa499-92ff-462b-807e-581c4a861307",
			"/data",
			"/data//assignments//details/topics//courseUpper",
			false,
		)

		if result == nil {
			t.Fatal("Expected result, got nil")
		}

		// For multi-level arrays, should return array of arrays
		// Like [ [ [val, val], [val] ], [ [val] ] ]
		dataArray, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected array for first level, got %T", result)
		}

		if len(dataArray) != 2 {
			t.Fatalf("Expected 2 items in data array, got %d", len(dataArray))
		}

		// First data item should have 2 assignments
		assignments0, ok := dataArray[0].([]interface{})
		if !ok {
			t.Fatalf("Expected array for assignments in data[0], got %T", dataArray[0])
		}

		if len(assignments0) != 2 {
			t.Fatalf("Expected 2 assignments in data[0], got %d", len(assignments0))
		}

		// First assignment should have 2 topics
		topics0, ok := assignments0[0].([]interface{})
		if !ok {
			t.Fatalf("Expected array for topics in assignments[0], got %T", assignments0[0])
		}

		if len(topics0) != 2 {
			t.Fatalf("Expected 2 topics in assignments[0], got %d", len(topics0))
		}

		if topics0[0] != "ALGEBRA" {
			t.Errorf("Expected 'ALGEBRA', got %v", topics0[0])
		}

		if topics0[1] != "GEOMETRY" {
			t.Errorf("Expected 'GEOMETRY', got %v", topics0[1])
		}

		// Second assignment should have 1 topic
		topics1, ok := assignments0[1].([]interface{})
		if !ok {
			t.Fatalf("Expected array for topics in assignments[1], got %T", assignments0[1])
		}

		if len(topics1) != 1 {
			t.Fatalf("Expected 1 topic in assignments[1], got %d", len(topics1))
		}

		if topics1[0] != "BIOLOGY" {
			t.Errorf("Expected 'BIOLOGY', got %v", topics1[0])
		}

		// Second data item should have 1 assignment
		assignments1, ok := dataArray[1].([]interface{})
		if !ok {
			t.Fatalf("Expected array for assignments in data[1], got %T", dataArray[1])
		}

		if len(assignments1) != 1 {
			t.Fatalf("Expected 1 assignment in data[1], got %d", len(assignments1))
		}

		topics2, ok := assignments1[0].([]interface{})
		if !ok {
			t.Fatalf("Expected array for topics, got %T", assignments1[0])
		}

		if len(topics2) != 1 {
			t.Fatalf("Expected 1 topic, got %d", len(topics2))
		}

		if topics2[0] != "WORLD WAR II" {
			t.Errorf("Expected 'WORLD WAR II', got %v", topics2[0])
		}
	})

	t.Run("Iterate flag returns flat array", func(t *testing.T) {
		result := extractFromFlatKeys(
			flatKeys,
			"16a4f8ef-541f-4a1f-8ff2-82b8fbcc0fde",
			"/data",
			"/data//name",
			true, // iterate = true
		)

		if result == nil {
			t.Fatal("Expected result, got nil")
		}

		resultArray, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected array with iterate=true, got %T", result)
		}

		if len(resultArray) != 2 {
			t.Fatalf("Expected 2 items in result array, got %d", len(resultArray))
		}

		if resultArray[0] != "ALEX" {
			t.Errorf("Expected 'ALEX', got %v", resultArray[0])
		}

		if resultArray[1] != "JORDAN" {
			t.Errorf("Expected 'JORDAN', got %v", resultArray[1])
		}
	})
}

// TestEmptySourceEndpoint tests extraction with empty source endpoints
func TestEmptySourceEndpoint(t *testing.T) {
	// Simulate flat keys with root data
	flatKeys := map[string]interface{}{
		"fa849706-87f0-4409-88c0-4b67e65ebcd4-/": map[string]interface{}{
			"data": []interface{}{
				map[string]interface{}{"name": "test"},
			},
		},
	}

	result := extractFromFlatKeys(
		flatKeys,
		"fa849706-87f0-4409-88c0-4b67e65ebcd4",
		"", // Empty source endpoint
		"",
		false,
	)

	if result == nil {
		t.Fatal("Expected result for empty endpoint, got nil")
	}

	resultMap, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map, got %T", result)
	}

	data, ok := resultMap["data"].([]interface{})
	if !ok {
		t.Fatalf("Expected data array, got %T", resultMap["data"])
	}

	if len(data) != 1 {
		t.Fatalf("Expected 1 item in data array, got %d", len(data))
	}
}

// TestCompleteNestedStructure tests extraction from complete nested structures
func TestCompleteNestedStructure(t *testing.T) {
	// Simulate flat keys with complete nested structure like node 71bf0d05
	flatKeys := map[string]interface{}{
		"71bf0d05-55fb-4896-bb1a-c8e53d606670-/data": []interface{}{
			map[string]interface{}{
				"name": "alex",
				"assignments": []interface{}{
					map[string]interface{}{
						"title": "Math Homework",
						"details": map[string]interface{}{
							"topics": []interface{}{
								map[string]interface{}{
									"name":     "algebra",
									"chapters": []interface{}{1, 2, 3},
								},
							},
						},
					},
				},
			},
			map[string]interface{}{
				"name": "jordan",
				"assignments": []interface{}{
					map[string]interface{}{
						"title": "History Essay",
					},
				},
			},
		},
	}

	t.Run("Extract simple field from nested structure", func(t *testing.T) {
		result := extractFromFlatKeys(
			flatKeys,
			"71bf0d05-55fb-4896-bb1a-c8e53d606670",
			"/data",
			"/data//name",
			false,
		)

		if result == nil {
			t.Fatal("Expected result, got nil")
		}

		resultArray, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected array, got %T", result)
		}

		if len(resultArray) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(resultArray))
		}

		if resultArray[0] != "alex" {
			t.Errorf("Expected 'alex', got %v", resultArray[0])
		}

		if resultArray[1] != "jordan" {
			t.Errorf("Expected 'jordan', got %v", resultArray[1])
		}
	})

	t.Run("Extract nested array field", func(t *testing.T) {
		result := extractFromFlatKeys(
			flatKeys,
			"71bf0d05-55fb-4896-bb1a-c8e53d606670",
			"/data",
			"/data//assignments//title",
			false,
		)

		if result == nil {
			t.Fatal("Expected result, got nil")
		}

		resultArray, ok := result.([]interface{})
		if !ok {
			t.Fatalf("Expected array, got %T", result)
		}

		if len(resultArray) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(resultArray))
		}

		// First person has 1 assignment
		assignments0, ok := resultArray[0].([]interface{})
		if !ok {
			t.Fatalf("Expected array for assignments[0], got %T", resultArray[0])
		}

		if len(assignments0) != 1 {
			t.Fatalf("Expected 1 assignment, got %d", len(assignments0))
		}

		if assignments0[0] != "Math Homework" {
			t.Errorf("Expected 'Math Homework', got %v", assignments0[0])
		}

		// Second person has 1 assignment
		assignments1, ok := resultArray[1].([]interface{})
		if !ok {
			t.Fatalf("Expected array for assignments[1], got %T", resultArray[1])
		}

		if assignments1[0] != "History Essay" {
			t.Errorf("Expected 'History Essay', got %v", assignments1[0])
		}
	})
}

// TestUnwrapSingleFieldObject tests the unwrap functionality
func TestUnwrapSingleFieldObject(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name:     "Single-key map with string",
			input:    map[string]interface{}{"name": "ALEX"},
			expected: "ALEX",
		},
		{
			name:     "Single-key map with bool",
			input:    map[string]interface{}{"isHigher18": true},
			expected: true,
		},
		{
			name:     "Multi-key map returns as-is",
			input:    map[string]interface{}{"name": "ALEX", "age": 30},
			expected: map[string]interface{}{"name": "ALEX", "age": 30},
		},
		{
			name:     "Empty map returns as-is",
			input:    map[string]interface{}{},
			expected: map[string]interface{}{},
		},
		{
			name:     "Non-map returns as-is",
			input:    "ALEX",
			expected: "ALEX",
		},
		{
			name:     "Array returns as-is",
			input:    []interface{}{"ALEX", "JORDAN"},
			expected: []interface{}{"ALEX", "JORDAN"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := unwrapSingleFieldObject(tt.input)
			resultJSON, _ := json.Marshal(result)
			expectedJSON, _ := json.Marshal(tt.expected)
			if string(resultJSON) != string(expectedJSON) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}
