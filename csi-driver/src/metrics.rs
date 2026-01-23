//! Prometheus metrics for the CSI driver
//!
//! Provides metrics for monitoring CSI operations, agent connectivity,
//! and overall driver health.

use std::net::SocketAddr;
use std::time::Instant;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

/// Metric names
pub mod names {
    /// Counter: Total number of CSI operations by type and status
    pub const CSI_OPERATIONS_TOTAL: &str = "csi_operations_total";
    /// Histogram: Duration of CSI operations in seconds
    pub const CSI_OPERATION_DURATION_SECONDS: &str = "csi_operation_duration_seconds";
    /// Gauge: Agent connection status (1 = connected, 0 = disconnected)
    pub const CSI_AGENT_CONNECTED: &str = "csi_agent_connected";
    /// Counter: Number of agent connection attempts
    pub const CSI_AGENT_CONNECTION_ATTEMPTS: &str = "csi_agent_connection_attempts";
    /// Counter: Number of retried operations
    pub const CSI_RETRIES_TOTAL: &str = "csi_retries_total";
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

/// Record a CSI operation with its result
pub fn record_operation(operation: &str, status: &str, duration_secs: f64) {
    counter!(names::CSI_OPERATIONS_TOTAL, "operation" => operation.to_string(), "status" => status.to_string())
        .increment(1);
    histogram!(names::CSI_OPERATION_DURATION_SECONDS, "operation" => operation.to_string())
        .record(duration_secs);
}

/// Record agent connection status
pub fn set_agent_connected(connected: bool) {
    gauge!(names::CSI_AGENT_CONNECTED).set(if connected { 1.0 } else { 0.0 });
}

/// Record an agent connection attempt
pub fn record_connection_attempt(success: bool) {
    counter!(names::CSI_AGENT_CONNECTION_ATTEMPTS, "success" => success.to_string()).increment(1);
}

/// Record a retry attempt
pub fn record_retry(operation: &str) {
    counter!(names::CSI_RETRIES_TOTAL, "operation" => operation.to_string()).increment(1);
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
