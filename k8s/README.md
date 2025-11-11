# Icarus Kubernetes Deployment Guide

This directory contains Kubernetes manifests for deploying Icarus with production-ready concurrency control and autoscaling.

## Architecture Overview

The deployment consists of:
- **ConfigMap**: Centralized concurrency configuration
- **Deployment**: Icarus processor pods with resource limits and health checks
- **HorizontalPodAutoscaler**: Automatic scaling based on CPU/memory metrics
- **Service**: Internal ClusterIP service for health/metrics endpoints

## Quick Start

### 1. Apply the ConfigMap

```bash
kubectl apply -f k8s/configmap.yaml
```

### 2. Deploy the Application

```bash
kubectl apply -f k8s/deployment.yaml
```

### 3. Set Up Autoscaling

```bash
kubectl apply -f k8s/hpa.yaml
```

### 4. Expose the Service (Optional)

```bash
kubectl apply -f k8s/service.yaml
```

### 5. Verify Deployment

```bash
# Check pod status
kubectl get pods -l app=icarus-processor

# Check HPA status
kubectl get hpa icarus-processor-hpa

# View pod logs
kubectl logs -f -l app=icarus-processor

# Check concurrency metrics in logs
kubectl logs -l app=icarus-processor | grep "Concurrency metrics"
```

## Configuration Options

### Concurrency Settings (ConfigMap)

| Variable | Default | Description |
|----------|---------|-------------|
| `ICARUS_CONCURRENCY_MULTIPLIER` | `2` | CPU × multiplier = max concurrent goroutines |
| `ICARUS_RUNNER_WORKERS` | `4` | Number of NATS message consumer workers |
| `ICARUS_PROCESSOR_MODE` | `concurrent` | `concurrent` or `sequential` for embedded nodes |
| `ICARUS_ITERATOR_MODE` | `sequential` | `parallel` or `sequential` for array processing |

### Resource Configuration (Deployment)

**Default Settings:**
- **CPU Request**: 1000m (1 core)
- **CPU Limit**: 2000m (2 cores)
- **Memory Request**: 2Gi
- **Memory Limit**: 4Gi

**Scaling Configuration:**
- **Min Replicas**: 3 (high availability)
- **Max Replicas**: 20 (burst capacity)
- **CPU Target**: 70% average utilization
- **Memory Target**: 80% average utilization

## Tuning Guidelines

### For CPU-Bound Workloads

Increase concurrency multiplier and processor parallelism:

```yaml
# configmap.yaml
ICARUS_CONCURRENCY_MULTIPLIER: "4"
ICARUS_PROCESSOR_MODE: "concurrent"
ICARUS_ITERATOR_MODE: "parallel"
```

**Note**: Monitor CPU throttling with `kubectl top pods`

### For I/O-Bound Workloads

Use higher concurrency with conservative CPU limits:

```yaml
# configmap.yaml
ICARUS_CONCURRENCY_MULTIPLIER: "8"
ICARUS_RUNNER_WORKERS: "8"

# deployment.yaml resources
resources:
  limits:
    cpu: "2000m"
    memory: "4Gi"
```

### For Memory-Constrained Environments

Reduce concurrency and use sequential processing:

```yaml
# configmap.yaml
ICARUS_CONCURRENCY_MULTIPLIER: "1"
ICARUS_PROCESSOR_MODE: "sequential"
ICARUS_ITERATOR_MODE: "sequential"
ICARUS_RUNNER_WORKERS: "2"

# deployment.yaml resources
resources:
  limits:
    cpu: "1000m"
    memory: "2Gi"
```

## Monitoring and Observability

### Concurrency Metrics

Icarus logs concurrency metrics every 30 seconds:

```json
{
  "level": "info",
  "msg": "Concurrency metrics",
  "active_goroutines": 45,
  "peak_concurrent": 78,
  "total_acquired": 1523,
  "total_released": 1478,
  "avg_wait_time": "15ms",
  "circuit_breaker": "closed"
}
```

### Key Metrics to Monitor

1. **active_goroutines**: Current concurrent operations
2. **peak_concurrent**: Maximum concurrent operations reached
3. **avg_wait_time**: Average time waiting to acquire concurrency slot
4. **circuit_breaker**: Circuit breaker state (closed/open/half-open)

### Prometheus Integration

The deployment includes Prometheus scraping annotations:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8080"
  prometheus.io/path: "/metrics"
```

## Troubleshooting

### CPU Throttling

**Symptom**: High `avg_wait_time`, pods hitting CPU limits

**Solutions**:
1. Increase CPU limits in deployment
2. Reduce `ICARUS_CONCURRENCY_MULTIPLIER`
3. Scale horizontally (increase replicas)

```bash
# Check CPU throttling
kubectl top pods -l app=icarus-processor

# Quick fix: scale up manually
kubectl scale deployment icarus-processor --replicas=5
```

### Memory Pressure

**Symptom**: OOMKilled pods, frequent restarts

**Solutions**:
1. Increase memory limits
2. Reduce concurrent operations
3. Use sequential processing modes

```bash
# Check memory usage
kubectl top pods -l app=icarus-processor

# View OOMKill events
kubectl get events --field-selector reason=OOMKilling
```

### Circuit Breaker Open

**Symptom**: `circuit_breaker: "open"` in logs, processing stopped

**Cause**: Too many failures (>100 in 30 seconds)

**Solutions**:
1. Check downstream services
2. Review error logs for root cause
3. Temporarily reduce load

```bash
# Check recent errors
kubectl logs -l app=icarus-processor --tail=100 | grep "error"

# Circuit breaker resets automatically after 30 seconds
```

### High Wait Times

**Symptom**: `avg_wait_time` > 100ms

**Solutions**:
1. Increase `ICARUS_CONCURRENCY_MULTIPLIER`
2. Add more CPU resources
3. Scale horizontally

```bash
# Update ConfigMap
kubectl edit configmap icarus-concurrency-config

# Restart pods to apply changes
kubectl rollout restart deployment icarus-processor
```

## Best Practices

### 1. Start Conservative

Begin with default settings and scale up based on observed metrics:
- Monitor for 24-48 hours
- Identify bottlenecks (CPU vs memory vs I/O)
- Adjust configuration incrementally

### 2. Use Pod Anti-Affinity

The deployment includes anti-affinity rules to distribute pods across nodes. This ensures:
- High availability during node failures
- Better resource utilization across the cluster

### 3. Set Proper Resource Limits

Always set both requests and limits:
- **Requests**: Guaranteed resources (for scheduling)
- **Limits**: Maximum resources (prevents runaway consumption)

### 4. Enable Metrics Collection

Integrate with Prometheus/Grafana for long-term trends:
- CPU and memory usage over time
- Concurrency patterns and peak times
- Error rates and circuit breaker activations

### 5. Test Scaling Behavior

Simulate load and verify:
- HPA scales up appropriately
- Scale-down is gradual (5-minute stabilization)
- Circuit breaker activates under extreme load

```bash
# Simulate load (requires appropriate tool)
kubectl run load-generator --rm -it --image=busybox -- /bin/sh

# Watch scaling in real-time
watch kubectl get hpa icarus-processor-hpa
```

## Configuration Updates

To update concurrency configuration without downtime:

```bash
# Edit ConfigMap
kubectl edit configmap icarus-concurrency-config

# Rolling restart to apply changes
kubectl rollout restart deployment icarus-processor

# Monitor rollout
kubectl rollout status deployment icarus-processor
```

## Health Checks

The deployment includes:

**Liveness Probe**: Restarts unhealthy pods
- Endpoint: `/health`
- Initial delay: 30s
- Check interval: 10s

**Readiness Probe**: Removes unhealthy pods from service
- Endpoint: `/ready`
- Initial delay: 5s
- Check interval: 5s

## Security

The deployment follows security best practices:
- Non-root user (UID 1000)
- Read-only root filesystem (when possible)
- Dropped all capabilities
- Seccomp profile enabled
- Resource limits enforced

## Cleanup

To remove the deployment:

```bash
kubectl delete -f k8s/service.yaml
kubectl delete -f k8s/hpa.yaml
kubectl delete -f k8s/deployment.yaml
kubectl delete -f k8s/configmap.yaml
```

## Support

For issues or questions:
1. Check pod logs: `kubectl logs -l app=icarus-processor`
2. Review metrics in concurrency logs
3. Consult the main README.md for architecture details
