package schema

import (
	"testing"
)

// TestTransformer_ApplyDefaults verifies the root Transformer wrapper delegates ApplyDefaults to the json transformer.
func TestTransformer_ApplyDefaults(t *testing.T) {
	tr := NewTransformer()
	s := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"status": {Type: TypeString, Default: "draft"},
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, s)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	if obj["status"] != "draft" {
		t.Errorf("status: got %v, want \"draft\"", obj["status"])
	}
}

// TestTransformer_ApplyCSVDefaults verifies the root Transformer wrapper delegates ApplyCSVDefaults to the csv package.
func TestTransformer_ApplyCSVDefaults(t *testing.T) {
	tr := NewTransformer()
	s := &CSVSchema{
		ColumnHeaders: map[string]*CSVColumn{
			"name": {Type: TypeString, Default: "N/A"},
		},
	}
	rows := []map[string]interface{}{{}}

	out, err := tr.ApplyCSVDefaults(rows, s)
	if err != nil {
		t.Fatalf("ApplyCSVDefaults: %v", err)
	}
	if len(out) != 1 || out[0]["name"] != "N/A" {
		t.Errorf("got %v, want [{\"name\": \"N/A\"}]", out)
	}
}
