package runner

import internaltracing "github.com/wehubfusion/Icarus/internal/tracing"

// TracingConfig is the public tracing configuration used by Runner clients.
// It mirrors the internal tracing configuration but keeps the implementation private.
type TracingConfig struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	OTLPEndpoint   string
	SampleRatio    float64
}

// DefaultTracingConfig returns a development-friendly tracing configuration.
func DefaultTracingConfig(serviceName string) TracingConfig {
	cfg := internaltracing.DefaultConfig(serviceName)
	return fromInternalConfig(cfg)
}

// JaegerTracingConfig returns a tracing configuration tailored for Jaeger.
func JaegerTracingConfig(serviceName string) TracingConfig {
	cfg := internaltracing.JaegerConfig(serviceName)
	return fromInternalConfig(cfg)
}

func (c TracingConfig) toInternalConfig() internaltracing.TracingConfig {
	return internaltracing.TracingConfig{
		ServiceName:    c.ServiceName,
		ServiceVersion: c.ServiceVersion,
		Environment:    c.Environment,
		OTLPEndpoint:   c.OTLPEndpoint,
		SampleRatio:    c.SampleRatio,
	}
}

func fromInternalConfig(cfg internaltracing.TracingConfig) TracingConfig {
	return TracingConfig{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		Environment:    cfg.Environment,
		OTLPEndpoint:   cfg.OTLPEndpoint,
		SampleRatio:    cfg.SampleRatio,
	}
}
