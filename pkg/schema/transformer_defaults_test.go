package schema

import (
	"encoding/json"
	"testing"
)

func TestApplyDefaults_EmptyObject_SetsAllDefaults(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"name":   {Type: TypeString, Default: "unknown"},
			"count":  {Type: TypeNumber, Default: 0.0},
			"active": {Type: TypeBoolean, Default: true},
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})

	if v, ok := obj["name"].(string); !ok || v != "unknown" {
		t.Errorf("name: got %v (type %T), want \"unknown\"", obj["name"], obj["name"])
	}
	if v, ok := obj["count"].(float64); !ok || v != 0 {
		t.Errorf("count: got %v (type %T), want 0", obj["count"], obj["count"])
	}
	if v, ok := obj["active"].(bool); !ok || !v {
		t.Errorf("active: got %v (type %T), want true", obj["active"], obj["active"])
	}
}

func TestApplyDefaults_NilDataObject_CreatesObjectAndSetsDefaults(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"status": {Type: TypeString, Default: "draft"},
		},
	}

	out, err := tr.ApplyDefaults(nil, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	if obj["status"] != "draft" {
		t.Errorf("status: got %v, want \"draft\"", obj["status"])
	}
}

func TestApplyDefaults_ExistingValue_NotOverwritten(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"name": {Type: TypeString, Default: "default-name"},
			"id":   {Type: TypeNumber, Default: 42.0},
		},
	}
	data := map[string]interface{}{
		"name": "custom-name",
		"id":   float64(100),
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})

	if obj["name"] != "custom-name" {
		t.Errorf("name: got %v, want \"custom-name\"", obj["name"])
	}
	if v, ok := obj["id"].(float64); !ok || v != 100 {
		t.Errorf("id: got %v, want 100", obj["id"])
	}
}

func TestApplyDefaults_PartialObject_OnlyMissingFieldsGetDefaults(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"a": {Type: TypeString, Default: "default-a"},
			"b": {Type: TypeString, Default: "default-b"},
			"c": {Type: TypeString, Default: "default-c"},
		},
	}
	data := map[string]interface{}{
		"b": "provided-b",
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})

	if obj["a"] != "default-a" {
		t.Errorf("a: got %v, want \"default-a\"", obj["a"])
	}
	if obj["b"] != "provided-b" {
		t.Errorf("b: got %v, want \"provided-b\"", obj["b"])
	}
	if obj["c"] != "default-c" {
		t.Errorf("c: got %v, want \"default-c\"", obj["c"])
	}
}

func TestApplyDefaults_NoDefault_FieldNotAdded(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"withDefault":    {Type: TypeString, Default: "yes"},
			"withoutDefault": {Type: TypeString}, // Default is nil
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})

	if obj["withDefault"] != "yes" {
		t.Errorf("withDefault: got %v, want \"yes\"", obj["withDefault"])
	}
	if _, exists := obj["withoutDefault"]; exists {
		t.Errorf("withoutDefault should not be present when no default defined")
	}
}

func TestApplyDefaults_NestedObject_AppliesNestedDefaults(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"inner": {
				Type: TypeObject,
				Properties: map[string]*Property{
					"nested": {Type: TypeString, Default: "nested-default"},
				},
			},
		},
	}
	data := map[string]interface{}{
		"inner": map[string]interface{}{},
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	inner, ok := obj["inner"].(map[string]interface{})
	if !ok {
		t.Fatalf("inner: got %T, want map[string]interface{}", obj["inner"])
	}
	if inner["nested"] != "nested-default" {
		t.Errorf("inner.nested: got %v, want \"nested-default\"", inner["nested"])
	}
}

func TestApplyDefaults_MissingNestedObject_WithNestedDefaults_CreatesObjectWithDefaults(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"config": {
				Type: TypeObject,
				Properties: map[string]*Property{
					"level": {Type: TypeString, Default: "info"},
				},
			},
		},
	}
	data := map[string]interface{}{} // config key missing

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	config, ok := obj["config"].(map[string]interface{})
	if !ok {
		t.Fatalf("config: got %T, want map[string]interface{}", obj["config"])
	}
	if config["level"] != "info" {
		t.Errorf("config.level: got %v, want \"info\"", config["level"])
	}
}

func TestApplyDefaults_ArrayOfObjects_AppliesItemDefaults(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeArray,
		Items: &Property{
			Type: TypeObject,
			Properties: map[string]*Property{
				"label": {Type: TypeString, Default: "item"},
				"value": {Type: TypeNumber, Default: 1.0},
			},
		},
	}
	data := []interface{}{
		map[string]interface{}{},
		map[string]interface{}{"label": "custom"},
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	arr := out.([]interface{})
	if len(arr) != 2 {
		t.Fatalf("len(arr): got %d, want 2", len(arr))
	}

	item0 := arr[0].(map[string]interface{})
	if item0["label"] != "item" || item0["value"].(float64) != 1 {
		t.Errorf("item0: got label=%v value=%v, want label=\"item\" value=1", item0["label"], item0["value"])
	}
	item1 := arr[1].(map[string]interface{})
	if item1["label"] != "custom" {
		t.Errorf("item1.label: got %v, want \"custom\"", item1["label"])
	}
	if item1["value"].(float64) != 1 {
		t.Errorf("item1.value: got %v, want 1", item1["value"])
	}
}

func TestApplyDefaults_NilDataArray_ReturnsEmptySlice(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type:  TypeArray,
		Items: &Property{Type: TypeString},
	}

	out, err := tr.ApplyDefaults(nil, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	arr, ok := out.([]interface{})
	if !ok {
		t.Fatalf("got %T, want []interface{}", out)
	}
	if len(arr) != 0 {
		t.Errorf("len(arr): got %d, want 0", len(arr))
	}
}

func TestApplyDefaults_WrongRootType_ReturnsError(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{Type: TypeObject, Properties: map[string]*Property{}}
	data := []interface{}{"not", "object"}

	_, err := tr.ApplyDefaults(data, schema)
	if err == nil {
		t.Error("expected error for array data with object schema")
	}

	schema2 := &Schema{Type: TypeArray, Items: &Property{Type: TypeString}}
	data2 := map[string]interface{}{"x": "y"}
	_, err = tr.ApplyDefaults(data2, schema2)
	if err == nil {
		t.Error("expected error for object data with array schema")
	}
}

func TestApplyDefaults_DefaultZeroValues_StringEmptyAndNumberZero(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"emptyStr": {Type: TypeString, Default: ""},
			"zero":     {Type: TypeNumber, Default: 0.0},
			"falseVal": {Type: TypeBoolean, Default: false},
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})

	if obj["emptyStr"] != "" {
		t.Errorf("emptyStr: got %q, want \"\"", obj["emptyStr"])
	}
	if v, ok := obj["zero"].(float64); !ok || v != 0 {
		t.Errorf("zero: got %v, want 0", obj["zero"])
	}
	if v, ok := obj["falseVal"].(bool); !ok || v {
		t.Errorf("falseVal: got %v, want false", obj["falseVal"])
	}
}

func TestApplyDefaults_UUID_MissingField_GeneratesUUID(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"id": {Type: TypeUUID},
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	id, ok := obj["id"].(string)
	if !ok || id == "" {
		t.Errorf("id: got %v, want non-empty string UUID", obj["id"])
	}
	// Basic UUID format check (8-4-4-4-12 hex)
	if len(id) != 36 {
		t.Errorf("id length: got %d, want 36", len(id))
	}
}

func TestApplyDefaults_UUID_WithPrefixAndPostfix(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"urn": {Type: TypeUUID, Prefix: "urn:uuid:", Postfix: ""},
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	urn, ok := obj["urn"].(string)
	if !ok {
		t.Fatalf("urn: got %T", obj["urn"])
	}
	if len(urn) < 45 || urn[:9] != "urn:uuid:" {
		t.Errorf("urn: got %q, want prefix \"urn:uuid:\"", urn)
	}
}

func TestApplyCSVDefaults_AppliesColumnDefaults(t *testing.T) {
	tr := NewTransformer()
	csvSchema := &CSVSchema{
		ColumnHeaders: map[string]*CSVColumn{
			"name":  {Type: TypeString, Default: "N/A"},
			"score": {Type: TypeNumber, Default: 0.0},
		},
	}
	rows := []map[string]interface{}{
		{"name": "alice"},
		{},
	}

	out, err := tr.ApplyCSVDefaults(rows, csvSchema)
	if err != nil {
		t.Fatalf("ApplyCSVDefaults: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("rows: got %d, want 2", len(out))
	}
	// Row 0: name provided, score should get default
	if out[0]["name"] != "alice" {
		t.Errorf("row0.name: got %v, want \"alice\"", out[0]["name"])
	}
	if out[0]["score"].(float64) != 0 {
		t.Errorf("row0.score: got %v, want 0", out[0]["score"])
	}
	// Row 1: both get defaults
	if out[1]["name"] != "N/A" {
		t.Errorf("row1.name: got %v, want \"N/A\"", out[1]["name"])
	}
	if out[1]["score"].(float64) != 0 {
		t.Errorf("row1.score: got %v, want 0", out[1]["score"])
	}
}

func TestApplyCSVDefaults_NilSchema_ReturnsRowsUnchanged(t *testing.T) {
	tr := NewTransformer()
	rows := []map[string]interface{}{{"a": "b"}}

	out, err := tr.ApplyCSVDefaults(rows, nil)
	if err != nil {
		t.Fatalf("ApplyCSVDefaults: %v", err)
	}
	if len(out) != 1 || out[0]["a"] != "b" {
		t.Errorf("rows unchanged: got %v", out)
	}
}

func TestApplyDefaults_DefaultFromJSONNumber(t *testing.T) {
	// JSON unmarshaling often gives numbers as float64
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"count": {Type: TypeNumber, Default: 42.0},
		},
	}
	data := map[string]interface{}{}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	switch v := obj["count"].(type) {
	case float64:
		if v != 42 {
			t.Errorf("count: got %v, want 42", v)
		}
	default:
		t.Errorf("count: got %T (%v), want float64", obj["count"], obj["count"])
	}
}

func TestApplyDefaults_DeeplyNested_DefaultsAppliedAtEachLevel(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"level1": {
				Type: TypeObject,
				Properties: map[string]*Property{
					"level2": {
						Type: TypeObject,
						Properties: map[string]*Property{
							"leaf": {Type: TypeString, Default: "deep"},
						},
					},
				},
			},
		},
	}
	data := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{},
		},
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	l1, _ := obj["level1"].(map[string]interface{})
	l2, _ := l1["level2"].(map[string]interface{})
	if l2["leaf"] != "deep" {
		t.Errorf("level1.level2.leaf: got %v, want \"deep\"", l2["leaf"])
	}
}

func TestApplyDefaults_ObjectWithNoProperties_ReturnsDataUnchanged(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type:       TypeObject,
		Properties: nil,
	}
	data := map[string]interface{}{"extra": "kept"}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	if obj["extra"] != "kept" {
		t.Errorf("extra: got %v, want \"kept\"", obj["extra"])
	}
}

func TestApplyDefaults_ArraySchemaWithItemsDefault_ItemDefaultsApplied(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeArray,
		Items: &Property{
			Type: TypeObject,
			Properties: map[string]*Property{
				"id":   {Type: TypeNumber, Default: 0.0},
				"name": {Type: TypeString, Default: "unnamed"},
			},
		},
	}
	data := []interface{}{
		map[string]interface{}{"id": float64(1)},
		map[string]interface{}{"name": "only-name"},
		map[string]interface{}{},
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	arr := out.([]interface{})

	// First item: id provided, name should get default
	item0 := arr[0].(map[string]interface{})
	if item0["id"].(float64) != 1 || item0["name"] != "unnamed" {
		t.Errorf("item0: got id=%v name=%v", item0["id"], item0["name"])
	}
	// Second item: name provided, id should get default
	item1 := arr[1].(map[string]interface{})
	if item1["id"].(float64) != 0 || item1["name"] != "only-name" {
		t.Errorf("item1: got id=%v name=%v", item1["id"], item1["name"])
	}
	// Third item: both defaults
	item2 := arr[2].(map[string]interface{})
	if item2["id"].(float64) != 0 || item2["name"] != "unnamed" {
		t.Errorf("item2: got id=%v name=%v", item2["id"], item2["name"])
	}
}

// TestApplyDefaults_JSONRoundTrip ensures defaults applied to map[string]interface{}
// (e.g. from json.Unmarshal) behave correctly when asserted back.
func TestApplyDefaults_JSONRoundTrip(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"title": {Type: TypeString, Default: "Untitled"},
		},
	}
	var data map[string]interface{}
	_ = json.Unmarshal([]byte("{}"), &data)

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	if obj["title"] != "Untitled" {
		t.Errorf("title: got %v, want \"Untitled\"", obj["title"])
	}
}

// TestApplyDefaults_FromJSON_NestedObject verifies defaults work when data is
// unmarshaled from JSON with nested objects (same types as json.Unmarshal produces).
func TestApplyDefaults_FromJSON_NestedObject(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"user": {
				Type: TypeObject,
				Properties: map[string]*Property{
					"name":  {Type: TypeString, Default: "guest"},
					"role":  {Type: TypeString, Default: "viewer"},
					"meta":  {Type: TypeObject, Properties: map[string]*Property{"env": {Type: TypeString, Default: "prod"}}},
				},
			},
		},
	}
	// Real JSON: nested object with one field set, others missing
	jsonStr := `{"user": {"name": "alice"}}`
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	user, ok := obj["user"].(map[string]interface{})
	if !ok {
		t.Fatalf("user: got %T", obj["user"])
	}
	if user["name"] != "alice" {
		t.Errorf("user.name: got %v, want \"alice\"", user["name"])
	}
	if user["role"] != "viewer" {
		t.Errorf("user.role: got %v, want \"viewer\"", user["role"])
	}
	meta, ok := user["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("user.meta: got %T", user["meta"])
	}
	if meta["env"] != "prod" {
		t.Errorf("user.meta.env: got %v, want \"prod\"", meta["env"])
	}
}

// TestApplyDefaults_FromJSON_ArrayOfObjects verifies defaults work when data is
// unmarshaled from JSON with an array of objects.
func TestApplyDefaults_FromJSON_ArrayOfObjects(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"items": {
				Type: TypeArray,
				Items: &Property{
					Type: TypeObject,
					Properties: map[string]*Property{
						"id":   {Type: TypeNumber, Default: 0.0},
						"name": {Type: TypeString, Default: "item"},
					},
				},
			},
		},
	}
	jsonStr := `{"items": [{"id": 1}, {}, {"name": "custom"}]}`
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	items, ok := obj["items"].([]interface{})
	if !ok {
		t.Fatalf("items: got %T", obj["items"])
	}
	if len(items) != 3 {
		t.Fatalf("len(items): got %d, want 3", len(items))
	}
	// First: id from JSON (float64), name gets default
	item0 := items[0].(map[string]interface{})
	if item0["id"].(float64) != 1 || item0["name"] != "item" {
		t.Errorf("items[0]: got id=%v name=%v", item0["id"], item0["name"])
	}
	// Second: both defaults
	item1 := items[1].(map[string]interface{})
	if item1["id"].(float64) != 0 || item1["name"] != "item" {
		t.Errorf("items[1]: got id=%v name=%v", item1["id"], item1["name"])
	}
	// Third: name from JSON, id gets default
	item2 := items[2].(map[string]interface{})
	if item2["id"].(float64) != 0 || item2["name"] != "custom" {
		t.Errorf("items[2]: got id=%v name=%v", item2["id"], item2["name"])
	}
}

// TestApplyDefaults_FromJSON_RootArray verifies defaults when root is an array
// unmarshaled from JSON.
func TestApplyDefaults_FromJSON_RootArray(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeArray,
		Items: &Property{
			Type: TypeObject,
			Properties: map[string]*Property{
				"value": {Type: TypeString, Default: "default"},
			},
		},
	}
	jsonStr := `[{"value": "a"}, {}]`
	var data []interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	arr := out.([]interface{})
	if len(arr) != 2 {
		t.Fatalf("len: got %d, want 2", len(arr))
	}
	if arr[0].(map[string]interface{})["value"] != "a" {
		t.Errorf("arr[0].value: got %v, want \"a\"", arr[0].(map[string]interface{})["value"])
	}
	if arr[1].(map[string]interface{})["value"] != "default" {
		t.Errorf("arr[1].value: got %v, want \"default\"", arr[1].(map[string]interface{})["value"])
	}
}

// TestApplyDefaults_FromJSON_NullNestedObject creates object and applies nested
// defaults when JSON has explicit null for an object field.
func TestApplyDefaults_FromJSON_NullNestedObject(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"config": {
				Type: TypeObject,
				Properties: map[string]*Property{
					"level": {Type: TypeString, Default: "info"},
				},
			},
		},
	}
	jsonStr := `{"config": null}`
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	config, ok := obj["config"].(map[string]interface{})
	if !ok {
		t.Fatalf("config: got %T (null is treated as missing object, so we build {} with defaults)", obj["config"])
	}
	if config["level"] != "info" {
		t.Errorf("config.level: got %v, want \"info\"", config["level"])
	}
}

// TestApplyDefaults_FromJSON_ExplicitNullPrimitive documents current behavior:
// when a key exists in JSON with value null, the default is NOT applied (key exists).
func TestApplyDefaults_FromJSON_ExplicitNullPrimitive(t *testing.T) {
	tr := NewTransformer()
	schema := &Schema{
		Type: TypeObject,
		Properties: map[string]*Property{
			"name": {Type: TypeString, Default: "default-name"},
		},
	}
	jsonStr := `{"name": null}`
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	out, err := tr.ApplyDefaults(data, schema)
	if err != nil {
		t.Fatalf("ApplyDefaults: %v", err)
	}
	obj := out.(map[string]interface{})
	// Current behavior: key "name" exists, so we do not apply default; value stays nil.
	if obj["name"] != nil {
		t.Errorf("name: current behavior is to not override explicit null; got %v", obj["name"])
	}
}
