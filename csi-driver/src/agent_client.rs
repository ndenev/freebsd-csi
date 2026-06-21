//! Agent Client Wrapper
//!
//! Provides a wrapper around the ctld-agent gRPC client for volume and snapshot operations.
//! Includes automatic retry with exponential backoff for transient failures.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tracing::{debug, warn};

use crate::metrics;

/// Maximum number of retry attempts for transient failures
const MAX_RETRIES: u32 = 3;
/// Initial backoff delay in milliseconds
const INITIAL_BACKOFF_MS: u64 = 100;
/// Maximum backoff delay in milliseconds
const MAX_BACKOFF_MS: u64 = 5000;
/// Backoff multiplier (exponential factor)
const BACKOFF_MULTIPLIER: u64 = 2;

use crate::agent::{
    AgentFeature, CreateSnapshotRequest, CreateVolumeRequest, DeleteSnapshotRequest,
    DeleteVolumeRequest, ExpandVolumeRequest, ExportType, GetAgentInfoRequest,
    GetAgentInfoResponse, GetCapacityRequest, GetVolumeRequest, ListSnapshotsRequest,
    ListVolumesRequest, Snapshot, Volume, VolumeContentSource,
    storage_agent_client::StorageAgentClient,
};

const REQUIRED_AGENT_PROTOCOL_VERSION: u32 = 2;

/// TLS configuration for connecting to ctld-agent
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    pub domain: String,
}

/// Client wrapper for the ctld-agent storage service.
#[derive(Debug, Clone)]
pub struct AgentClient {
    client: StorageAgentClient<Channel>,
}

/// Check if a gRPC status code indicates a retryable error.
///
/// Retryable errors are transient failures that may succeed on retry:
/// - Unavailable: Server temporarily unavailable
/// - ResourceExhausted: Rate limited, may succeed after backoff
/// - Aborted: Operation aborted, can be retried
/// - Unknown: Unknown error, might be transient
fn is_retryable(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::ResourceExhausted
            | tonic::Code::Aborted
            | tonic::Code::Unknown
    )
}

/// Execute an async operation with exponential backoff retry.
///
/// Retries the operation up to MAX_RETRIES times for retryable errors,
/// with exponential backoff between attempts.
async fn with_retry<T, F, Fut>(operation_name: &str, mut operation: F) -> Result<T, tonic::Status>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    let mut attempt = 0;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(status) => {
                attempt += 1;

                if !is_retryable(&status) || attempt > MAX_RETRIES {
                    if attempt > 1 {
                        warn!(
                            operation = operation_name,
                            attempts = attempt,
                            code = ?status.code(),
                            "Operation failed after retries"
                        );
                    }
                    return Err(status);
                }

                warn!(
                    operation = operation_name,
                    attempt = attempt,
                    max_retries = MAX_RETRIES,
                    code = ?status.code(),
                    backoff_ms = backoff_ms,
                    "Retryable error, backing off"
                );

                // Record retry metric
                metrics::record_retry(operation_name);

                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                // Exponential backoff with cap
                backoff_ms = (backoff_ms * BACKOFF_MULTIPLIER).min(MAX_BACKOFF_MS);
            }
        }
    }
}

fn validate_agent_compatibility(info: &GetAgentInfoResponse) -> Result<(), tonic::Status> {
    if info.protocol_version < REQUIRED_AGENT_PROTOCOL_VERSION {
        return Err(tonic::Status::failed_precondition(format!(
            "ctld-agent protocol version {} is too old; version {} is required",
            info.protocol_version, REQUIRED_AGENT_PROTOCOL_VERSION
        )));
    }

    if !info
        .features
        .contains(&(AgentFeature::OperatorAuthGroup as i32))
    {
        return Err(tonic::Status::failed_precondition(
            "ctld-agent does not support operator-managed auth groups",
        ));
    }

    Ok(())
}

async fn verify_agent_compatibility(
    client: &mut StorageAgentClient<Channel>,
) -> Result<(), tonic::Status> {
    let info = client
        .get_agent_info(GetAgentInfoRequest {})
        .await
        .map_err(|status| {
            if status.code() == tonic::Code::Unimplemented {
                tonic::Status::failed_precondition(
                    "ctld-agent protocol is too old; upgrade ctld-agent before using this driver",
                )
            } else {
                status
            }
        })?
        .into_inner();

    validate_agent_compatibility(&info)
}

impl AgentClient {
    /// Connect to the ctld-agent at the specified endpoint (plaintext).
    pub async fn connect(endpoint: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut client = StorageAgentClient::connect(endpoint.to_string()).await?;
        verify_agent_compatibility(&mut client).await?;
        Ok(Self { client })
    }

    /// Connect to ctld-agent with optional mTLS and robust connection settings.
    ///
    /// Connection settings:
    /// - 10 second connect timeout (fail fast if agent unreachable)
    /// - 30 second request timeout
    /// - TCP keepalive every 60 seconds
    /// - HTTP/2 keepalive every 30 seconds with 10 second timeout
    /// - Keepalive while idle to detect dead connections
    pub async fn connect_with_tls(
        endpoint: &str,
        tls: Option<TlsConfig>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut endpoint_builder = Endpoint::from_shared(endpoint.to_string())?
            // Connection establishment timeout
            .connect_timeout(Duration::from_secs(10))
            // Overall request timeout
            .timeout(Duration::from_secs(30))
            // TCP keepalive to detect dead connections at OS level
            .tcp_keepalive(Some(Duration::from_secs(60)))
            // Disable Nagle's algorithm for lower latency
            .tcp_nodelay(true)
            // HTTP/2 keepalive ping interval
            .http2_keep_alive_interval(Duration::from_secs(30))
            // How long to wait for keepalive response
            .keep_alive_timeout(Duration::from_secs(10))
            // Send keepalive even when no requests in flight
            .keep_alive_while_idle(true);

        if let Some(tls) = tls {
            let cert = tokio::fs::read(&tls.cert_path).await?;
            let key = tokio::fs::read(&tls.key_path).await?;
            let ca = tokio::fs::read(&tls.ca_path).await?;

            let tls_config = ClientTlsConfig::new()
                .identity(Identity::from_pem(cert, key))
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name(&tls.domain);

            endpoint_builder = endpoint_builder.tls_config(tls_config)?;
        }

        let channel = endpoint_builder.connect().await?;
        let mut client = StorageAgentClient::new(channel);
        verify_agent_compatibility(&mut client).await?;
        Ok(Self { client })
    }

    /// Create a new volume with the specified parameters.
    ///
    /// If `auth_group` is provided, the target references that operator-managed
    /// CTL auth-group. Credential material is never sent to the agent.
    ///
    /// If `content_source` is provided, the volume will be created from the specified snapshot.
    /// The clone_mode determines whether to use fast linking (zfs clone) or full copy (zfs send/recv).
    ///
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn create_volume(
        &mut self,
        name: &str,
        size_bytes: i64,
        export_type: ExportType,
        parameters: HashMap<String, String>,
        auth_group: String,
        content_source: Option<VolumeContentSource>,
    ) -> Result<Volume, tonic::Status> {
        let request = CreateVolumeRequest {
            name: name.to_string(),
            size_bytes,
            export_type: export_type as i32,
            parameters,
            legacy_auth: None,
            auth_group,
            content_source,
        };

        debug!(name = name, "Creating volume with retry");

        let client = self.client.clone();
        with_retry("create_volume", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.create_volume(req).await?;
                response
                    .into_inner()
                    .volume
                    .ok_or_else(|| tonic::Status::internal("Agent returned empty volume"))
            }
        })
        .await
    }

    /// Delete a volume by its ID.
    ///
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn delete_volume(&mut self, volume_id: &str) -> Result<(), tonic::Status> {
        let request = DeleteVolumeRequest {
            volume_id: volume_id.to_string(),
        };

        debug!(volume_id = volume_id, "Deleting volume with retry");

        let client = self.client.clone();
        with_retry("delete_volume", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                c.delete_volume(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// Expand a volume to a new size.
    ///
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn expand_volume(
        &mut self,
        volume_id: &str,
        new_size_bytes: i64,
    ) -> Result<i64, tonic::Status> {
        let request = ExpandVolumeRequest {
            volume_id: volume_id.to_string(),
            new_size_bytes,
        };

        debug!(
            volume_id = volume_id,
            new_size_bytes = new_size_bytes,
            "Expanding volume with retry"
        );

        let client = self.client.clone();
        with_retry("expand_volume", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.expand_volume(req).await?;
                Ok(response.into_inner().size_bytes)
            }
        })
        .await
    }

    /// Get volume information by ID.
    ///
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn get_volume(&mut self, volume_id: &str) -> Result<Volume, tonic::Status> {
        let request = GetVolumeRequest {
            volume_id: volume_id.to_string(),
        };

        debug!(volume_id = volume_id, "Getting volume with retry");

        let client = self.client.clone();
        with_retry("get_volume", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.get_volume(req).await?;
                response
                    .into_inner()
                    .volume
                    .ok_or_else(|| tonic::Status::not_found("Volume not found"))
            }
        })
        .await
    }

    /// Create a snapshot of a volume.
    ///
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn create_snapshot(
        &mut self,
        source_volume_id: &str,
        name: &str,
    ) -> Result<Snapshot, tonic::Status> {
        let request = CreateSnapshotRequest {
            source_volume_id: source_volume_id.to_string(),
            name: name.to_string(),
        };

        debug!(
            source_volume_id = source_volume_id,
            name = name,
            "Creating snapshot with retry"
        );

        let client = self.client.clone();
        with_retry("create_snapshot", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.create_snapshot(req).await?;
                response
                    .into_inner()
                    .snapshot
                    .ok_or_else(|| tonic::Status::internal("Agent returned empty snapshot"))
            }
        })
        .await
    }

    /// Delete a snapshot by its ID.
    ///
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn delete_snapshot(&mut self, snapshot_id: &str) -> Result<(), tonic::Status> {
        let request = DeleteSnapshotRequest {
            snapshot_id: snapshot_id.to_string(),
        };

        debug!(snapshot_id = snapshot_id, "Deleting snapshot with retry");

        let client = self.client.clone();
        with_retry("delete_snapshot", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                c.delete_snapshot(req).await?;
                Ok(())
            }
        })
        .await
    }

    /// List all volumes with optional pagination.
    ///
    /// Returns a tuple of (volumes, next_token) where next_token is None if there are no more results.
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn list_volumes(
        &mut self,
        max_entries: i32,
        starting_token: Option<&str>,
    ) -> Result<(Vec<Volume>, Option<String>), tonic::Status> {
        let request = ListVolumesRequest {
            max_entries,
            starting_token: starting_token.unwrap_or("").to_string(),
        };

        debug!(max_entries, starting_token = ?starting_token, "Listing volumes with retry");

        let client = self.client.clone();
        with_retry("list_volumes", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.list_volumes(req).await?;
                let inner = response.into_inner();
                let next_token = if inner.next_token.is_empty() {
                    None
                } else {
                    Some(inner.next_token)
                };
                Ok((inner.volumes, next_token))
            }
        })
        .await
    }

    /// Get storage capacity information.
    ///
    /// Returns (available_capacity, total_capacity) in bytes.
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn get_capacity(&mut self) -> Result<(i64, i64), tonic::Status> {
        let request = GetCapacityRequest {
            parameters: HashMap::new(),
        };

        debug!("Getting capacity with retry");

        let client = self.client.clone();
        with_retry("get_capacity", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.get_capacity(req).await?;
                let inner = response.into_inner();
                Ok((inner.available_capacity, inner.total_capacity))
            }
        })
        .await
    }

    /// List snapshots with optional volume filter and pagination.
    ///
    /// Returns a tuple of (snapshots, next_token) where next_token is None if there are no more results.
    /// Automatically retries on transient failures with exponential backoff.
    pub async fn list_snapshots(
        &mut self,
        source_volume_id: Option<&str>,
        max_entries: i32,
        starting_token: Option<&str>,
    ) -> Result<(Vec<Snapshot>, Option<String>), tonic::Status> {
        let request = ListSnapshotsRequest {
            source_volume_id: source_volume_id.unwrap_or("").to_string(),
            max_entries,
            starting_token: starting_token.unwrap_or("").to_string(),
        };

        debug!(
            source_volume_id = ?source_volume_id,
            max_entries,
            starting_token = ?starting_token,
            "Listing snapshots with retry"
        );

        let client = self.client.clone();
        with_retry("list_snapshots", || {
            let req = request.clone();
            let mut c = client.clone();
            async move {
                let response = c.list_snapshots(req).await?;
                let inner = response.into_inner();
                let next_token = if inner.next_token.is_empty() {
                    None
                } else {
                    Some(inner.next_token)
                };
                Ok((inner.snapshots, next_token))
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_export_type_conversion() {
        // Verify export type enum values
        assert_eq!(ExportType::Unspecified as i32, 0);
        assert_eq!(ExportType::Iscsi as i32, 1);
        assert_eq!(ExportType::Nvmeof as i32, 2);
    }

    #[test]
    fn test_is_retryable() {
        // Retryable errors
        assert!(is_retryable(&tonic::Status::unavailable("server down")));
        assert!(is_retryable(&tonic::Status::resource_exhausted(
            "rate limited"
        )));
        assert!(is_retryable(&tonic::Status::aborted("transaction aborted")));
        assert!(is_retryable(&tonic::Status::unknown("unknown error")));

        // Non-retryable errors
        assert!(!is_retryable(&tonic::Status::not_found("not found")));
        assert!(!is_retryable(&tonic::Status::invalid_argument("bad arg")));
        assert!(!is_retryable(&tonic::Status::permission_denied("denied")));
        assert!(!is_retryable(&tonic::Status::already_exists("exists")));
        assert!(!is_retryable(&tonic::Status::internal("internal error")));
    }

    #[test]
    fn test_validate_agent_compatibility_accepts_required_feature() {
        let info = GetAgentInfoResponse {
            protocol_version: REQUIRED_AGENT_PROTOCOL_VERSION,
            version: "0.5.0".to_string(),
            features: vec![AgentFeature::OperatorAuthGroup as i32],
        };

        assert!(validate_agent_compatibility(&info).is_ok());
    }

    #[test]
    fn test_validate_agent_compatibility_rejects_old_protocol() {
        let info = GetAgentInfoResponse {
            protocol_version: REQUIRED_AGENT_PROTOCOL_VERSION - 1,
            version: "0.4.1".to_string(),
            features: vec![AgentFeature::OperatorAuthGroup as i32],
        };

        let err = validate_agent_compatibility(&info).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("protocol version"));
    }

    #[test]
    fn test_validate_agent_compatibility_rejects_missing_auth_group_feature() {
        let info = GetAgentInfoResponse {
            protocol_version: REQUIRED_AGENT_PROTOCOL_VERSION,
            version: "0.5.0".to_string(),
            features: vec![],
        };

        let err = validate_agent_compatibility(&info).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("operator-managed auth groups"));
    }

    #[tokio::test]
    async fn test_with_retry_success_first_attempt() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<i32, tonic::Status> = with_retry("test", || {
            let c = counter_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(42)
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_with_retry_success_after_retries() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<i32, tonic::Status> = with_retry("test", || {
            let c = counter_clone.clone();
            async move {
                let attempt = c.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(tonic::Status::unavailable("temporarily unavailable"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_retry_non_retryable_error() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<i32, tonic::Status> = with_retry("test", || {
            let c = counter_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(tonic::Status::not_found("not found"))
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
        // Should not retry on non-retryable error
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_with_retry_exhausted() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<i32, tonic::Status> = with_retry("test", || {
            let c = counter_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err(tonic::Status::unavailable("always unavailable"))
            }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
        // Should retry MAX_RETRIES + 1 times (initial + retries)
        assert_eq!(counter.load(Ordering::SeqCst), MAX_RETRIES + 1);
    }
}
