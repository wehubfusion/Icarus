package hl7

import "sync"

// VariesResolver resolves the effective datatype for a VARIES field (e.g. OBX-5 from OBX-2).
type VariesResolver interface {
	ResolveDataType(segmentName string, fieldPosition string, seg *Segment, msg *Message) string
}

// VariesResolverFunc adapts a function to VariesResolver.
type VariesResolverFunc func(segmentName, fieldPosition string, seg *Segment, msg *Message) string

func (f VariesResolverFunc) ResolveDataType(segName, field string, seg *Segment, msg *Message) string {
	return f(segName, field, seg, msg)
}

var (
	variesRegistry   = make(map[string]VariesResolver)
	variesRegistryMu sync.RWMutex
)

func RegisterVariesResolver(segment, field string, r VariesResolver) {
	key := segment + "-" + field
	variesRegistryMu.Lock()
	defer variesRegistryMu.Unlock()
	variesRegistry[key] = r
}

func ResolveVariesDataType(segmentName, fieldPosition string, seg *Segment, msg *Message) string {
	key := segmentName + "-" + fieldPosition
	variesRegistryMu.RLock()
	r, ok := variesRegistry[key]
	variesRegistryMu.RUnlock()
	if !ok {
		return ""
	}
	return r.ResolveDataType(segmentName, fieldPosition, seg, msg)
}

func init() {
	RegisterVariesResolver("OBX", "5", VariesResolverFunc(func(segmentName, fieldPosition string, seg *Segment, msg *Message) string {
		if seg == nil {
			return ""
		}
		f, ok := seg.FieldAt(2)
		if !ok {
			return ""
		}
		return f.String()
	}))
}
