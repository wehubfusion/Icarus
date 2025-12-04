package runtime

import (
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMetricsCollector is a thread-safe implementation of MetricsCollector.
type DefaultMetricsCollector struct {
	processed        atomic.Int64
	errors           atomic.Int64
	skipped          atomic.Int64
	totalProcessTime atomic.Int64
	workers          int
	mu               sync.RWMutex
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector(workers int) *DefaultMetricsCollector {
	return &DefaultMetricsCollector{
		workers: workers,
	}
}

// RecordProcessed records a successfully processed item.
func (m *DefaultMetricsCollector) RecordProcessed(durationNs int64) {
	m.processed.Add(1)
	m.totalProcessTime.Add(durationNs)
}

// RecordError records a processing error.
func (m *DefaultMetricsCollector) RecordError() {
	m.errors.Add(1)
}

// RecordSkipped records a skipped node.
func (m *DefaultMetricsCollector) RecordSkipped() {
	m.skipped.Add(1)
}

// GetMetrics returns the current metrics.
func (m *DefaultMetricsCollector) GetMetrics() Metrics {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return Metrics{
		TotalItemsProcessed: m.processed.Load(),
		TotalErrors:         m.errors.Load(),
		TotalSkipped:        m.skipped.Load(),
		ProcessingTimeNs:    m.totalProcessTime.Load(),
		ConcurrentWorkers:   m.workers,
	}
}

// Reset resets all metrics.
func (m *DefaultMetricsCollector) Reset() {
	m.processed.Store(0)
	m.errors.Store(0)
	m.skipped.Store(0)
	m.totalProcessTime.Store(0)
}

// SetWorkers sets the number of workers.
func (m *DefaultMetricsCollector) SetWorkers(workers int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers = workers
}

// AverageProcessingTime returns the average processing time per item.
func (m *DefaultMetricsCollector) AverageProcessingTime() time.Duration {
	processed := m.processed.Load()
	if processed == 0 {
		return 0
	}
	return time.Duration(m.totalProcessTime.Load() / processed)
}

// ErrorRate returns the error rate as a percentage.
func (m *DefaultMetricsCollector) ErrorRate() float64 {
	processed := m.processed.Load()
	errors := m.errors.Load()
	total := processed + errors
	if total == 0 {
		return 0
	}
	return float64(errors) / float64(total) * 100
}

// Ensure DefaultMetricsCollector implements MetricsCollector
var _ MetricsCollector = (*DefaultMetricsCollector)(nil)

// NoOpMetricsCollector is a metrics collector that does nothing.
type NoOpMetricsCollector struct{}

func (m *NoOpMetricsCollector) RecordProcessed(durationNs int64) {}
func (m *NoOpMetricsCollector) RecordError()                     {}
func (m *NoOpMetricsCollector) RecordSkipped()                   {}
func (m *NoOpMetricsCollector) GetMetrics() Metrics              { return Metrics{} }
func (m *NoOpMetricsCollector) Reset()                           {}

// Ensure NoOpMetricsCollector implements MetricsCollector
var _ MetricsCollector = (*NoOpMetricsCollector)(nil)
