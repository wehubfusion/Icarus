package embedded

// NewDefaultRegistry creates a new executor registry with all built-in executors registered
// Note: Executors must be registered manually to avoid import cycles.
// See pkg/process/embedded/all package for a pre-configured registry.
func NewDefaultRegistry() *ExecutorRegistry {
	return NewExecutorRegistry()
}
