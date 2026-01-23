//! Prometheus metrics for the ctld-agent
//!
//! Provides metrics for monitoring storage operations, ZFS/CTL health,
//! and agent performance.

use std::net::SocketAddr;
use std::time::Instant;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

/// Metric names
pub mod names {
    /// Counter: Total storage operations by type and status
    pub const STORAGE_OPERATIONS_TOTAL: &str = "ctld_storage_operations_total";
    /// Histogram: Duration of storage operations in seconds
    pub const STORAGE_OPERATION_DURATION_SECONDS: &str = "ctld_storage_operation_duration_seconds";
    /// Gauge: Number of active volumes
    pub const VOLUMES_TOTAL: &str = "ctld_volumes_total";
    /// Gauge: Number of active exports by type (iscsi/nvmeof)
    pub const EXPORTS_TOTAL: &str = "ctld_exports_total";
    /// Counter: Number of rate-limited operations
    pub const RATE_LIMITED_TOTAL: &str = "ctld_rate_limited_total";
    /// Gauge: Current concurrent operations in progress
    pub const CONCURRENT_OPS: &str = "ctld_concurrent_ops";
}

/// Initialize the Prometheus metrics exporter
///
/// Starts an HTTP server on the specified address that serves metrics
/// at the `/metrics` endpoint.
pub fn init_metrics(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()?;

    info!("Metrics server listening on http://{}/metrics", addr);
    Ok(())
}

/// Record a storage operation with its result
pub fn record_operation(operation: &str, status: &str, duration_secs: f64) {
    counter!(names::STORAGE_OPERATIONS_TOTAL, "operation" => operation.to_string(), "status" => status.to_string())
        .increment(1);
    histogram!(names::STORAGE_OPERATION_DURATION_SECONDS, "operation" => operation.to_string())
        .record(duration_secs);
}

/// Set the number of active volumes
pub fn set_volumes_count(count: usize) {
    gauge!(names::VOLUMES_TOTAL).set(count as f64);
}

/// Set the number of active exports by type
pub fn set_exports_count(export_type: &str, count: usize) {
    gauge!(names::EXPORTS_TOTAL, "type" => export_type.to_string()).set(count as f64);
}

/// Record a rate-limited operation
pub fn record_rate_limited(operation: &str) {
    counter!(names::RATE_LIMITED_TOTAL, "operation" => operation.to_string()).increment(1);
}

/// Set the current number of concurrent operations
pub fn set_concurrent_ops(count: usize) {
    gauge!(names::CONCURRENT_OPS).set(count as f64);
}

/// Helper for timing operations
pub struct OperationTimer {
    operation: String,
    start: Instant,
}

impl OperationTimer {
    /// Start timing an operation
    pub fn new(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
            start: Instant::now(),
        }
    }

    /// Complete the operation with success
    pub fn success(self) {
        let duration = self.start.elapsed().as_secs_f64();
        record_operation(&self.operation, "success", duration);
    }

    /// Complete the operation with failure
    pub fn failure(self, error_code: &str) {
        let duration = self.start.elapsed().as_secs_f64();
        record_operation(&self.operation, error_code, duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_timer() {
        let timer = OperationTimer::new("test_operation");
        std::thread::sleep(std::time::Duration::from_millis(10));
        // Just verify it doesn't panic - actual metrics recording requires init
        drop(timer);
    }
}
