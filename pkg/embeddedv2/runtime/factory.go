package runtime

import (
	"fmt"
	"sync"
)

// DefaultNodeFactory is the default implementation of EmbeddedNodeFactory.
// It maintains a thread-safe registry of node creators.
type DefaultNodeFactory struct {
	creators map[string]NodeCreator
	mu       sync.RWMutex
}

// NewDefaultNodeFactory creates a new default node factory.
func NewDefaultNodeFactory() *DefaultNodeFactory {
	return &DefaultNodeFactory{
		creators: make(map[string]NodeCreator),
	}
}

// Register registers a node creator for a plugin type.
// If a creator already exists for the plugin type, it will be overwritten.
func (f *DefaultNodeFactory) Register(pluginType string, creator NodeCreator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creators[pluginType] = creator
}

// Create creates an embedded node from configuration.
// Returns ErrNoExecutor if no creator is registered for the plugin type.
func (f *DefaultNodeFactory) Create(config EmbeddedNodeConfig) (EmbeddedNode, error) {
	f.mu.RLock()
	creator, exists := f.creators[config.PluginType]
	f.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNoExecutor, config.PluginType)
	}

	node, err := creator(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create node %s (%s): %w", config.Label, config.PluginType, err)
	}

	return node, nil
}

// HasCreator checks if a creator exists for a plugin type.
func (f *DefaultNodeFactory) HasCreator(pluginType string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, exists := f.creators[pluginType]
	return exists
}

// RegisteredTypes returns all registered plugin types.
func (f *DefaultNodeFactory) RegisteredTypes() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]string, 0, len(f.creators))
	for t := range f.creators {
		types = append(types, t)
	}
	return types
}

// Unregister removes a creator for a plugin type.
// Returns true if a creator was removed, false if none existed.
func (f *DefaultNodeFactory) Unregister(pluginType string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.creators[pluginType]; exists {
		delete(f.creators, pluginType)
		return true
	}
	return false
}

// Clear removes all registered creators.
func (f *DefaultNodeFactory) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creators = make(map[string]NodeCreator)
}

// Count returns the number of registered creators.
func (f *DefaultNodeFactory) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.creators)
}

// Ensure DefaultNodeFactory implements EmbeddedNodeFactory
var _ EmbeddedNodeFactory = (*DefaultNodeFactory)(nil)
