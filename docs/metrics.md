# Metrics Reference

This document describes all Prometheus metrics exposed by the FreeBSD CSI driver components.

## Table of Contents

- [Enabling Metrics](#enabling-metrics)
- [CSI Driver Metrics](#csi-driver-metrics)
- [ctld-agent Metrics](#ctld-agent-metrics)
- [Grafana Dashboards](#grafana-dashboards)
- [Alerting Rules](#alerting-rules)

---

## Enabling Metrics

### CSI Driver (Controller)

Enable metrics in the Helm chart:

```yaml
# values.yaml
metrics:
  enabled: true
  port: 9090
  serviceMonitor:
    enabled: true  # Creates ServiceMonitor for Prometheus Operator
    interval: 30s
```

Or via command line:

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --set metrics.enabled=true \
  --set metrics.port=9090
```

### ctld-agent

Pass the metrics address flag:

```bash
ctld-agent --metrics-addr 0.0.0.0:9091
```

Or via rc.conf:

```bash
sysrc ctld_agent_flags="--zfs-parent tank/csi --metrics-addr 0.0.0.0:9091"
```

---

## CSI Driver Metrics

The CSI driver exposes metrics on the configured port (default: 9090).

### csi_operations_total

**Type:** Counter

**Description:** Total number of CSI operations, labeled by operation type and status.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `operation` | `CreateVolume`, `DeleteVolume`, `ExpandVolume`, `CreateSnapshot`, `DeleteSnapshot`, `NodeStageVolume`, `NodePublishVolume`, etc. | CSI RPC name |
| `status` | `success`, `invalid_argument`, `not_found`, `already_exists`, `internal`, `unavailable` | Operation result |

**Example queries:**

```promql
# Total successful volume creates
sum(csi_operations_total{operation="CreateVolume", status="success"})

# Error rate for all operations
sum(rate(csi_operations_total{status!="success"}[5m]))

# Operations by status
sum by (status) (rate(csi_operations_total[5m]))
```

### csi_operation_duration_seconds

**Type:** Histogram

**Description:** Duration of CSI operations in seconds.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `operation` | CSI RPC names | The operation being measured |

**Buckets:** 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60

**Example queries:**

```promql
# 99th percentile latency for CreateVolume
histogram_quantile(0.99, rate(csi_operation_duration_seconds_bucket{operation="CreateVolume"}[5m]))

# Average operation duration
rate(csi_operation_duration_seconds_sum[5m]) / rate(csi_operation_duration_seconds_count[5m])

# Slow operations (>10s)
sum(rate(csi_operation_duration_seconds_bucket{le="10"}[5m])) by (operation)
```

### csi_agent_connected

**Type:** Gauge

**Description:** Agent connection status. `1` if connected, `0` if disconnected.

**Example queries:**

```promql
# Alert if agent is disconnected
csi_agent_connected == 0

# Uptime percentage
avg_over_time(csi_agent_connected[1h]) * 100
```

### csi_agent_connection_attempts

**Type:** Counter

**Description:** Number of attempts to connect to the ctld-agent.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `success` | `true`, `false` | Whether the connection succeeded |

**Example queries:**

```promql
# Connection failure rate
rate(csi_agent_connection_attempts{success="false"}[5m])

# Reconnection frequency
rate(csi_agent_connection_attempts{success="true"}[1h])
```

### csi_retries_total

**Type:** Counter

**Description:** Number of operations that required retries.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `operation` | CSI RPC names | The operation that was retried |

**Example queries:**

```promql
# Retry rate
rate(csi_retries_total[5m])

# Operations with most retries
topk(5, sum by (operation) (rate(csi_retries_total[1h])))
```

---

## ctld-agent Metrics

The ctld-agent exposes metrics on the configured port (default: 9091).

### ctld_storage_operations_total

**Type:** Counter

**Description:** Total storage operations performed by the agent.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `operation` | `create_volume`, `delete_volume`, `expand_volume`, `create_snapshot`, `delete_snapshot`, `get_volume`, `list_volumes` | Storage operation type |
| `status` | `success`, `error`, `invalid_argument`, `not_found` | Operation result |

**Example queries:**

```promql
# Success rate
sum(rate(ctld_storage_operations_total{status="success"}[5m])) / sum(rate(ctld_storage_operations_total[5m]))

# Error breakdown
sum by (status) (rate(ctld_storage_operations_total{status!="success"}[5m]))
```

### ctld_storage_operation_duration_seconds

**Type:** Histogram

**Description:** Duration of storage operations in seconds.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `operation` | Storage operation types | The operation being measured |

**Example queries:**

```promql
# ZFS volume creation latency p95
histogram_quantile(0.95, rate(ctld_storage_operation_duration_seconds_bucket{operation="create_volume"}[5m]))
```

### ctld_volumes_total

**Type:** Gauge

**Description:** Current number of CSI-managed volumes.

**Example queries:**

```promql
# Current volume count
ctld_volumes_total

# Volume growth rate
deriv(ctld_volumes_total[1h])
```

### ctld_exports_total

**Type:** Gauge

**Description:** Current number of active exports by protocol type.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `type` | `iscsi`, `nvmeof` | Export protocol |

**Example queries:**

```promql
# Total exports
sum(ctld_exports_total)

# iSCSI vs NVMeoF ratio
ctld_exports_total{type="iscsi"} / ctld_exports_total{type="nvmeof"}
```

### ctld_rate_limited_total

**Type:** Counter

**Description:** Number of operations that were rate-limited.

**Labels:**
| Label | Values | Description |
|-------|--------|-------------|
| `operation` | Storage operation types | The rate-limited operation |

**Example queries:**

```promql
# Rate limiting frequency
rate(ctld_rate_limited_total[5m])

# Alert on excessive rate limiting
sum(rate(ctld_rate_limited_total[5m])) > 1
```

### ctld_concurrent_ops

**Type:** Gauge

**Description:** Current number of concurrent operations in progress.

**Example queries:**

```promql
# Current concurrency
ctld_concurrent_ops

# Peak concurrency over 1 hour
max_over_time(ctld_concurrent_ops[1h])
```

---

## Grafana Dashboards

### Recommended Dashboard Panels

#### CSI Driver Overview

```json
{
  "panels": [
    {
      "title": "Operations Rate",
      "type": "graph",
      "targets": [
        {
          "expr": "sum by (operation) (rate(csi_operations_total[5m]))",
          "legendFormat": "{{operation}}"
        }
      ]
    },
    {
      "title": "Error Rate",
      "type": "singlestat",
      "targets": [
        {
          "expr": "sum(rate(csi_operations_total{status!=\"success\"}[5m])) / sum(rate(csi_operations_total[5m])) * 100",
          "legendFormat": "Error %"
        }
      ]
    },
    {
      "title": "Operation Latency (p99)",
      "type": "graph",
      "targets": [
        {
          "expr": "histogram_quantile(0.99, sum by (operation, le) (rate(csi_operation_duration_seconds_bucket[5m])))",
          "legendFormat": "{{operation}}"
        }
      ]
    },
    {
      "title": "Agent Connection",
      "type": "stat",
      "targets": [
        {
          "expr": "csi_agent_connected",
          "legendFormat": "Connected"
        }
      ]
    }
  ]
}
```

#### Storage Agent Overview

```json
{
  "panels": [
    {
      "title": "Volume Count",
      "type": "stat",
      "targets": [
        {
          "expr": "ctld_volumes_total",
          "legendFormat": "Volumes"
        }
      ]
    },
    {
      "title": "Exports by Type",
      "type": "piechart",
      "targets": [
        {
          "expr": "ctld_exports_total",
          "legendFormat": "{{type}}"
        }
      ]
    },
    {
      "title": "Concurrent Operations",
      "type": "gauge",
      "targets": [
        {
          "expr": "ctld_concurrent_ops",
          "legendFormat": "Current"
        }
      ]
    },
    {
      "title": "Rate Limited Operations",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(ctld_rate_limited_total[5m])",
          "legendFormat": "{{operation}}"
        }
      ]
    }
  ]
}
```

---

## Alerting Rules

### Prometheus Alert Rules

```yaml
groups:
  - name: freebsd-csi-alerts
    rules:
      # High error rate
      - alert: CSIHighErrorRate
        expr: |
          sum(rate(csi_operations_total{status!="success"}[5m]))
          / sum(rate(csi_operations_total[5m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "CSI error rate above 5%"
          description: "Error rate is {{ $value | humanizePercentage }}"

      # Critical error rate
      - alert: CSICriticalErrorRate
        expr: |
          sum(rate(csi_operations_total{status!="success"}[5m]))
          / sum(rate(csi_operations_total[5m])) > 0.20
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "CSI error rate above 20%"

      # Agent disconnected
      - alert: CSIAgentDisconnected
        expr: csi_agent_connected == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "CSI driver lost connection to storage agent"

      # Slow operations
      - alert: CSISlowOperations
        expr: |
          histogram_quantile(0.99,
            sum by (operation, le) (rate(csi_operation_duration_seconds_bucket[5m]))
          ) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "CSI operations taking >30s at p99"
          description: "{{ $labels.operation }} latency is {{ $value }}s"

      # High retry rate
      - alert: CSIHighRetryRate
        expr: sum(rate(csi_retries_total[5m])) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "CSI operations requiring frequent retries"

      # Rate limiting active
      - alert: CTLDRateLimitingActive
        expr: sum(rate(ctld_rate_limited_total[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Storage agent is rate limiting operations"

      # High concurrency
      - alert: CTLDHighConcurrency
        expr: ctld_concurrent_ops > 8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Storage agent nearing concurrency limit"
          description: "{{ $value }} concurrent operations (limit: 10)"
```

### ServiceMonitor for Prometheus Operator

The Helm chart creates a ServiceMonitor when enabled:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: freebsd-csi
  labels:
    app.kubernetes.io/name: freebsd-csi
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: freebsd-csi
      app.kubernetes.io/component: controller
  endpoints:
    - port: metrics
      path: /metrics
      interval: 30s
```

---

## Metric Endpoints

| Component | Default Port | Path | Description |
|-----------|--------------|------|-------------|
| CSI Driver (Controller) | 9090 | `/metrics` | CSI operation metrics |
| ctld-agent | 9091 | `/metrics` | Storage operation metrics |

Both endpoints return metrics in Prometheus text format.

**Example:**
```bash
# Fetch CSI driver metrics
curl http://localhost:9090/metrics

# Fetch agent metrics (from FreeBSD node)
curl http://192.168.1.100:9091/metrics
```
