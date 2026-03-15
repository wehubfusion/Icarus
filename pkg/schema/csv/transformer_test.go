package csv

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/schema/json"
)

func TestApplyCSVDefaults_AppliesColumnDefaults(t *testing.T) {
	s := &CSVSchema{
		ColumnHeaders: map[string]*CSVColumn{
			"name":  {Type: json.TypeString, Default: "N/A"},
			"score": {Type: json.TypeNumber, Default: 0.0},
		},
	}
	rows := []map[string]interface{}{
		{"name": "alice"},
		{},
	}

	out, err := ApplyCSVDefaults(rows, s)
	if err != nil {
		t.Fatalf("ApplyCSVDefaults: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("rows: got %d, want 2", len(out))
	}
	if out[0]["name"] != "alice" {
		t.Errorf("row0.name: got %v, want \"alice\"", out[0]["name"])
	}
	if out[0]["score"].(float64) != 0 {
		t.Errorf("row0.score: got %v, want 0", out[0]["score"])
	}
	if out[1]["name"] != "N/A" {
		t.Errorf("row1.name: got %v, want \"N/A\"", out[1]["name"])
	}
	if out[1]["score"].(float64) != 0 {
		t.Errorf("row1.score: got %v, want 0", out[1]["score"])
	}
}

func TestApplyCSVDefaults_NilSchema_ReturnsRowsUnchanged(t *testing.T) {
	rows := []map[string]interface{}{{"a": "b"}}

	out, err := ApplyCSVDefaults(rows, nil)
	if err != nil {
		t.Fatalf("ApplyCSVDefaults: %v", err)
	}
	if len(out) != 1 || out[0]["a"] != "b" {
		t.Errorf("rows unchanged: got %v", out)
	}
}
