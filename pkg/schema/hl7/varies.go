package hl7

import "sync"

// VariesResolver resolves the effective datatype for a VARIES field (e.g. OBX-5 from OBX-2).
// The registry allows adding resolvers for other segment/field combinations in the future.
type VariesResolver interface {
	// ResolveDataType returns the effective datatype for the given segment/field using message context.
	// Returns empty string if unknown (validation may then skip or treat as string).
	ResolveDataType(segmentName string, fieldPosition string, msg *Message) string
}

// VariesResolverFunc adapts a function to VariesResolver.
type VariesResolverFunc func(segmentName, fieldPosition string, msg *Message) string

func (f VariesResolverFunc) ResolveDataType(seg, field string, msg *Message) string {
	return f(seg, field, msg)
}

// VariesRegistry holds resolvers keyed by "segment-field" (e.g. "OBX-5").
var (
	variesRegistry   = make(map[string]VariesResolver)
	variesRegistryMu sync.RWMutex
)

// RegisterVariesResolver registers a resolver for the given segment and field (e.g. "OBX", "5" for OBX-5).
func RegisterVariesResolver(segment, field string, r VariesResolver) {
	key := segment + "-" + field
	variesRegistryMu.Lock()
	defer variesRegistryMu.Unlock()
	variesRegistry[key] = r
}

// ResolveVariesDataType returns the effective datatype for a VARIES field, or "" if no resolver.
func ResolveVariesDataType(segmentName, fieldPosition string, msg *Message) string {
	key := segmentName + "-" + fieldPosition
	variesRegistryMu.RLock()
	r, ok := variesRegistry[key]
	variesRegistryMu.RUnlock()
	if !ok {
		return ""
	}
	return r.ResolveDataType(segmentName, fieldPosition, msg)
}

func init() {
	// OBX-5: value type is given in OBX-2 (Observation Value Type).
	RegisterVariesResolver("OBX", "5", VariesResolverFunc(func(segmentName, fieldPosition string, msg *Message) string {
		seg, _ := msg.SegmentByName(segmentName)
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
