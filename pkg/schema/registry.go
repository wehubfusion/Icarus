package schema

import "fmt"

// ProcessorRegistry holds schema processors by format type.
type ProcessorRegistry struct {
	processors map[string]SchemaProcessor
}

// NewProcessorRegistry creates an empty registry.
func NewProcessorRegistry() *ProcessorRegistry {
	return &ProcessorRegistry{
		processors: make(map[string]SchemaProcessor),
	}
}

// Register adds a processor for its Type().
func (r *ProcessorRegistry) Register(p SchemaProcessor) {
	r.processors[p.Type()] = p
}

// Get returns the processor for the given format, or an error if not registered.
func (r *ProcessorRegistry) Get(format string) (SchemaProcessor, error) {
	p, ok := r.processors[format]
	if !ok {
		return nil, fmt.Errorf("no processor registered for schema type %q", format)
	}
	return p, nil
}
