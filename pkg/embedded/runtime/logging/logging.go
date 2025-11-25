package logging

// Logger defines a simple logging interface to avoid external dependencies.
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
}

// Field represents a key-value pair for structured logging.
type Field struct {
	Key   string
	Value interface{}
}

// NoOpLogger is a logger that does nothing (used when no logger is provided).
type NoOpLogger struct{}

func (n *NoOpLogger) Debug(msg string, fields ...Field) {}
func (n *NoOpLogger) Info(msg string, fields ...Field)  {}
func (n *NoOpLogger) Warn(msg string, fields ...Field)  {}
func (n *NoOpLogger) Error(msg string, fields ...Field) {}
