package datatypes

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"sync"
)

//go:embed v*.json
var datatypeFS embed.FS

type dtFile struct {
	VersionID string     `json:"version_id"`
	DataTypes []dtRecord `json:"data_types"`
}

type dtRecord struct {
	TypeID      string       `json:"type_id"`
	IsComposite bool         `json:"is_composite"`
	MaxLength   int          `json:"max_length"`
	Definition  dtDefinition `json:"definition"`
}

type dtDefinition struct {
	Fields []DTComponentDef `json:"fields"`
}

// DTComponentDef represents a component definition inside a composite datatype.
type DTComponentDef struct {
	Position string `json:"position"` // e.g. "CWE.1"
	Name     string `json:"name"`
	DataType string `json:"dataType"` // e.g. "ST", "HD", "TS"
	Length   int    `json:"length"`
	Usage    string `json:"usage"` // HL7 usage: R/RE/O/C/X/W/B
}

// DataTypeDef is the version-specific definition of a datatype.
// For primitives, Components is empty.
type DataTypeDef struct {
	TypeID      string
	IsComposite bool
	MaxLength   int
	Components  []DTComponentDef
}

// Registry provides O(1) datatype definition lookup by version and type.
// Keys are normalized: version "2.5", type "CWE".
type Registry struct {
	index map[string]map[string]*DataTypeDef
}

var (
	registryOnce sync.Once
	registryInst *Registry
	registryErr  error
)

// GetRegistry returns a singleton registry loaded from embedded JSON files.
func GetRegistry() (*Registry, error) {
	registryOnce.Do(func() {
		registryInst, registryErr = loadRegistry()
	})
	return registryInst, registryErr
}

func loadRegistry() (*Registry, error) {
	files, err := fs.Glob(datatypeFS, "v*.json")
	if err != nil {
		return nil, fmt.Errorf("datatypes: glob embedded files: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("datatypes: no embedded datatype JSON files found")
	}

	r := &Registry{index: make(map[string]map[string]*DataTypeDef, len(files))}
	for _, name := range files {
		b, err := datatypeFS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("datatypes: read %s: %w", name, err)
		}
		var f dtFile
		if err := json.Unmarshal(b, &f); err != nil {
			return nil, fmt.Errorf("datatypes: unmarshal %s: %w", name, err)
		}
		ver := NormalizeVersion(f.VersionID)
		if ver == "" {
			return nil, fmt.Errorf("datatypes: invalid version_id in %s", name)
		}
		if _, ok := r.index[ver]; !ok {
			r.index[ver] = make(map[string]*DataTypeDef, len(f.DataTypes))
		}
		for _, rec := range f.DataTypes {
			tid := strings.ToUpper(strings.TrimSpace(rec.TypeID))
			if tid == "" {
				continue
			}
			def := &DataTypeDef{
				TypeID:      tid,
				IsComposite: rec.IsComposite,
				MaxLength:   rec.MaxLength,
				Components:  rec.Definition.Fields,
			}
			r.index[ver][tid] = def
		}
	}
	return r, nil
}

// NormalizeVersion converts "v2.5" or "2.5" into "2.5".
func NormalizeVersion(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(strings.ToLower(v), "v")
	return v
}

// Lookup returns a datatype definition using exact or minor-version fallback.
// Example: version "2.3.1" falls back to "2.3" if not present.
func (r *Registry) Lookup(version, typeID string) (*DataTypeDef, bool) {
	if r == nil {
		return nil, false
	}
	ver := NormalizeVersion(version)
	tid := strings.ToUpper(strings.TrimSpace(typeID))
	if ver == "" || tid == "" {
		return nil, false
	}

	for {
		if m, ok := r.index[ver]; ok {
			if def, ok := m[tid]; ok {
				return def, true
			}
		}
		// minor-version fallback: "2.3.1" -> "2.3"
		if i := strings.LastIndexByte(ver, '.'); i > 0 {
			ver = ver[:i]
			continue
		}
		return nil, false
	}
}
