//! Agent Client Wrapper
//!
//! Provides a wrapper around the ctld-agent gRPC client for volume and snapshot operations.

use std::collections::HashMap;
use std::path::PathBuf;

use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

use crate::agent::{
    CreateSnapshotRequest, CreateVolumeRequest, DeleteSnapshotRequest, DeleteVolumeRequest,
    ExpandVolumeRequest, ExportType, GetVolumeRequest, Snapshot, Volume,
    storage_agent_client::StorageAgentClient,
};

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

impl AgentClient {
    /// Connect to the ctld-agent at the specified endpoint (plaintext).
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        let client = StorageAgentClient::connect(endpoint.to_string()).await?;
        Ok(Self { client })
    }

    /// Connect to ctld-agent with optional mTLS
    pub async fn connect_with_tls(
        endpoint: &str,
        tls: Option<TlsConfig>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let channel = if let Some(tls) = tls {
            let cert = tokio::fs::read(&tls.cert_path).await?;
            let key = tokio::fs::read(&tls.key_path).await?;
            let ca = tokio::fs::read(&tls.ca_path).await?;

            let tls_config = ClientTlsConfig::new()
                .identity(Identity::from_pem(cert, key))
                .ca_certificate(Certificate::from_pem(ca))
                .domain_name(&tls.domain);

            Channel::from_shared(endpoint.to_string())?
                .tls_config(tls_config)?
                .connect()
                .await?
        } else {
            Channel::from_shared(endpoint.to_string())?
                .connect()
                .await?
        };

        let client = StorageAgentClient::new(channel);
        Ok(Self { client })
    }

    /// Create a new volume with the specified parameters.
    pub async fn create_volume(
        &mut self,
        name: &str,
        size_bytes: i64,
        export_type: ExportType,
        parameters: HashMap<String, String>,
    ) -> Result<Volume, tonic::Status> {
        let request = CreateVolumeRequest {
            name: name.to_string(),
            size_bytes,
            export_type: export_type as i32,
            parameters,
        };

        let response = self.client.create_volume(request).await?;
        response
            .into_inner()
            .volume
            .ok_or_else(|| tonic::Status::internal("Agent returned empty volume"))
    }

    /// Delete a volume by its ID.
    pub async fn delete_volume(&mut self, volume_id: &str) -> Result<(), tonic::Status> {
        let request = DeleteVolumeRequest {
            volume_id: volume_id.to_string(),
        };

        self.client.delete_volume(request).await?;
        Ok(())
    }

    /// Expand a volume to a new size.
    pub async fn expand_volume(
        &mut self,
        volume_id: &str,
        new_size_bytes: i64,
    ) -> Result<i64, tonic::Status> {
        let request = ExpandVolumeRequest {
            volume_id: volume_id.to_string(),
            new_size_bytes,
        };

        let response = self.client.expand_volume(request).await?;
        Ok(response.into_inner().size_bytes)
    }

    /// Get volume information by ID.
    pub async fn get_volume(&mut self, volume_id: &str) -> Result<Volume, tonic::Status> {
        let request = GetVolumeRequest {
            volume_id: volume_id.to_string(),
        };

        let response = self.client.get_volume(request).await?;
        response
            .into_inner()
            .volume
            .ok_or_else(|| tonic::Status::not_found("Volume not found"))
    }

    /// Create a snapshot of a volume.
    pub async fn create_snapshot(
        &mut self,
        source_volume_id: &str,
        name: &str,
    ) -> Result<Snapshot, tonic::Status> {
        let request = CreateSnapshotRequest {
            source_volume_id: source_volume_id.to_string(),
            name: name.to_string(),
        };

        let response = self.client.create_snapshot(request).await?;
        response
            .into_inner()
            .snapshot
            .ok_or_else(|| tonic::Status::internal("Agent returned empty snapshot"))
    }

    /// Delete a snapshot by its ID.
    pub async fn delete_snapshot(&mut self, snapshot_id: &str) -> Result<(), tonic::Status> {
        let request = DeleteSnapshotRequest {
            snapshot_id: snapshot_id.to_string(),
        };

        self.client.delete_snapshot(request).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_type_conversion() {
        // Verify export type enum values
        assert_eq!(ExportType::Unspecified as i32, 0);
        assert_eq!(ExportType::Iscsi as i32, 1);
        assert_eq!(ExportType::Nvmeof as i32, 2);
    }
}
