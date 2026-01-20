//! CSI Controller Service Implementation
//!
//! Handles volume and snapshot lifecycle operations by calling the ctld-agent daemon.

use std::collections::HashMap;

use prost_types::Timestamp;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::agent::ExportType;
use crate::agent_client::{AgentClient, TlsConfig};
use crate::csi;

/// Default volume size: 1GB
const DEFAULT_VOLUME_SIZE: i64 = 1024 * 1024 * 1024;

/// CSI Controller Service
///
/// Implements the CSI Controller service which handles:
/// - Volume creation and deletion
/// - Volume expansion
/// - Snapshot creation and deletion
/// - Capability reporting
pub struct ControllerService {
    /// Agent endpoint for ctld-agent connection
    agent_endpoint: String,
    /// TLS configuration for mTLS connection to ctld-agent
    tls_config: Option<TlsConfig>,
    /// Lazily initialized agent client connection
    client: Mutex<Option<AgentClient>>,
}

impl ControllerService {
    /// Create a new ControllerService with the specified agent endpoint.
    pub fn new(agent_endpoint: String) -> Self {
        Self {
            agent_endpoint,
            tls_config: None,
            client: Mutex::new(None),
        }
    }

    /// Create a new ControllerService with mTLS configuration.
    pub fn with_tls(agent_endpoint: String, tls_config: Option<TlsConfig>) -> Self {
        Self {
            agent_endpoint,
            tls_config,
            client: Mutex::new(None),
        }
    }

    /// Get or create the agent client connection.
    async fn get_client(&self) -> Result<AgentClient, Status> {
        let mut guard = self.client.lock().await;
        if let Some(ref client) = *guard {
            return Ok(client.clone());
        }

        info!(endpoint = %self.agent_endpoint, tls = %self.tls_config.is_some(), "Connecting to ctld-agent");
        let client = AgentClient::connect_with_tls(&self.agent_endpoint, self.tls_config.clone())
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to connect to ctld-agent");
                Status::unavailable("Agent connection failed")
            })?;

        *guard = Some(client.clone());
        Ok(client)
    }

    /// Parse export type from storage class parameters.
    fn parse_export_type(parameters: &HashMap<String, String>) -> ExportType {
        parameters
            .get("exportType")
            .or_else(|| parameters.get("export_type"))
            .map(|s| match s.to_lowercase().as_str() {
                "iscsi" => ExportType::Iscsi,
                "nvmeof" | "nvme" => ExportType::Nvmeof,
                _ => ExportType::Iscsi, // Default to iSCSI
            })
            .unwrap_or(ExportType::Iscsi)
    }

    /// Get required volume size from capacity range.
    fn get_volume_size(capacity_range: Option<&csi::CapacityRange>) -> i64 {
        capacity_range
            .map(|range| {
                if range.required_bytes > 0 {
                    range.required_bytes
                } else if range.limit_bytes > 0 {
                    range.limit_bytes
                } else {
                    DEFAULT_VOLUME_SIZE
                }
            })
            .unwrap_or(DEFAULT_VOLUME_SIZE)
    }

    /// Convert agent Volume to CSI Volume.
    ///
    /// `parameters` contains the original StorageClass parameters which may include
    /// portal addresses and filesystem type needed by the node service.
    fn agent_volume_to_csi(volume: &crate::agent::Volume, parameters: &HashMap<String, String>) -> csi::Volume {
        let mut volume_context = HashMap::new();
        volume_context.insert("target_name".to_string(), volume.target_name.clone());
        volume_context.insert("lun_id".to_string(), volume.lun_id.to_string());
        volume_context.insert("zfs_dataset".to_string(), volume.zfs_dataset.clone());

        let export_type_str = match ExportType::try_from(volume.export_type).unwrap_or(ExportType::Unspecified) {
            ExportType::Iscsi => "iscsi",
            ExportType::Nvmeof => "nvmeof",
            ExportType::Unspecified => "unspecified",
        };
        volume_context.insert("export_type".to_string(), export_type_str.to_string());

        // Pass through portal/address info for node service (required on Linux)
        // StorageClass parameters like: portal, targetPortal, transportAddr, transportPort
        if let Some(portal) = parameters.get("portal").or_else(|| parameters.get("targetPortal")) {
            volume_context.insert("portal".to_string(), portal.clone());
        }

        // For NVMeoF, pass transport address and port
        if let Some(addr) = parameters.get("transportAddr").or_else(|| parameters.get("transport_addr")) {
            volume_context.insert("transport_addr".to_string(), addr.clone());
        }
        if let Some(port) = parameters.get("transportPort").or_else(|| parameters.get("transport_port")) {
            volume_context.insert("transport_port".to_string(), port.clone());
        }

        // Pass through filesystem type for node service
        if let Some(fs_type) = parameters.get("fsType").or_else(|| parameters.get("fs_type")) {
            volume_context.insert("fs_type".to_string(), fs_type.clone());
        }

        csi::Volume {
            capacity_bytes: volume.size_bytes,
            volume_id: volume.id.clone(),
            volume_context,
            content_source: None,
            accessible_topology: vec![],
        }
    }

    /// Convert agent Snapshot to CSI Snapshot.
    fn agent_snapshot_to_csi(snapshot: &crate::agent::Snapshot) -> csi::Snapshot {
        csi::Snapshot {
            size_bytes: snapshot.size_bytes,
            snapshot_id: snapshot.id.clone(),
            source_volume_id: snapshot.source_volume_id.clone(),
            creation_time: Some(Timestamp {
                seconds: snapshot.creation_time,
                nanos: 0,
            }),
            ready_to_use: true,
            group_snapshot_id: String::new(),
        }
    }
}

#[tonic::async_trait]
impl csi::controller_server::Controller for ControllerService {
    /// Create a new volume.
    async fn create_volume(
        &self,
        request: Request<csi::CreateVolumeRequest>,
    ) -> Result<Response<csi::CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        let name = &req.name;

        if name.is_empty() {
            return Err(Status::invalid_argument("Volume name is required"));
        }

        info!(name = %name, "CreateVolume request");

        let size_bytes = Self::get_volume_size(req.capacity_range.as_ref());
        let export_type = Self::parse_export_type(&req.parameters);

        debug!(
            name = %name,
            size_bytes = size_bytes,
            export_type = ?export_type,
            "Creating volume"
        );

        let mut client = self.get_client().await?;
        let volume = client
            .create_volume(name, size_bytes, export_type, req.parameters.clone())
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to create volume via agent");
                e
            })?;

        info!(
            volume_id = %volume.id,
            name = %volume.name,
            size_bytes = volume.size_bytes,
            "Volume created successfully"
        );

        Ok(Response::new(csi::CreateVolumeResponse {
            volume: Some(Self::agent_volume_to_csi(&volume, &req.parameters)),
        }))
    }

    /// Delete a volume.
    async fn delete_volume(
        &self,
        request: Request<csi::DeleteVolumeRequest>,
    ) -> Result<Response<csi::DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        info!(volume_id = %volume_id, "DeleteVolume request");

        let mut client = self.get_client().await?;
        client.delete_volume(volume_id).await.map_err(|e| {
            // NOT_FOUND is acceptable - volume may have already been deleted
            if e.code() == tonic::Code::NotFound {
                warn!(volume_id = %volume_id, "Volume not found, treating as already deleted");
                return Status::ok("");
            }
            error!(error = %e, "Failed to delete volume via agent");
            e
        })?;

        info!(volume_id = %volume_id, "Volume deleted successfully");

        Ok(Response::new(csi::DeleteVolumeResponse {}))
    }

    /// Expand a volume.
    async fn controller_expand_volume(
        &self,
        request: Request<csi::ControllerExpandVolumeRequest>,
    ) -> Result<Response<csi::ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        let capacity_range = req.capacity_range.as_ref().ok_or_else(|| {
            Status::invalid_argument("Capacity range is required for volume expansion")
        })?;

        let new_size_bytes = if capacity_range.required_bytes > 0 {
            capacity_range.required_bytes
        } else {
            capacity_range.limit_bytes
        };

        if new_size_bytes <= 0 {
            return Err(Status::invalid_argument(
                "Required or limit bytes must be positive",
            ));
        }

        info!(
            volume_id = %volume_id,
            new_size_bytes = new_size_bytes,
            "ControllerExpandVolume request"
        );

        let mut client = self.get_client().await?;
        let actual_size = client
            .expand_volume(volume_id, new_size_bytes)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to expand volume via agent");
                e
            })?;

        info!(
            volume_id = %volume_id,
            actual_size = actual_size,
            "Volume expanded successfully"
        );

        Ok(Response::new(csi::ControllerExpandVolumeResponse {
            capacity_bytes: actual_size,
            node_expansion_required: true, // Filesystem may need expansion
        }))
    }

    /// Report controller capabilities.
    async fn controller_get_capabilities(
        &self,
        _request: Request<csi::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<csi::ControllerGetCapabilitiesResponse>, Status> {
        let capabilities = vec![
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: csi::controller_service_capability::rpc::Type::CreateDeleteVolume
                            as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: csi::controller_service_capability::rpc::Type::CreateDeleteSnapshot
                            as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: csi::controller_service_capability::rpc::Type::ExpandVolume as i32,
                    },
                )),
            },
        ];

        Ok(Response::new(csi::ControllerGetCapabilitiesResponse {
            capabilities,
        }))
    }

    /// Create a snapshot.
    async fn create_snapshot(
        &self,
        request: Request<csi::CreateSnapshotRequest>,
    ) -> Result<Response<csi::CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        let source_volume_id = &req.source_volume_id;
        let name = &req.name;

        if source_volume_id.is_empty() {
            return Err(Status::invalid_argument("Source volume ID is required"));
        }

        if name.is_empty() {
            return Err(Status::invalid_argument("Snapshot name is required"));
        }

        info!(
            source_volume_id = %source_volume_id,
            name = %name,
            "CreateSnapshot request"
        );

        let mut client = self.get_client().await?;
        let snapshot = client
            .create_snapshot(source_volume_id, name)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to create snapshot via agent");
                e
            })?;

        info!(
            snapshot_id = %snapshot.id,
            source_volume_id = %snapshot.source_volume_id,
            "Snapshot created successfully"
        );

        Ok(Response::new(csi::CreateSnapshotResponse {
            snapshot: Some(Self::agent_snapshot_to_csi(&snapshot)),
        }))
    }

    /// Delete a snapshot.
    async fn delete_snapshot(
        &self,
        request: Request<csi::DeleteSnapshotRequest>,
    ) -> Result<Response<csi::DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        let snapshot_id = &req.snapshot_id;

        if snapshot_id.is_empty() {
            return Err(Status::invalid_argument("Snapshot ID is required"));
        }

        info!(snapshot_id = %snapshot_id, "DeleteSnapshot request");

        let mut client = self.get_client().await?;
        client.delete_snapshot(snapshot_id).await.map_err(|e| {
            // NOT_FOUND is acceptable - snapshot may have already been deleted
            if e.code() == tonic::Code::NotFound {
                warn!(snapshot_id = %snapshot_id, "Snapshot not found, treating as already deleted");
                return Status::ok("");
            }
            error!(error = %e, "Failed to delete snapshot via agent");
            e
        })?;

        info!(snapshot_id = %snapshot_id, "Snapshot deleted successfully");

        Ok(Response::new(csi::DeleteSnapshotResponse {}))
    }

    /// Validate volume capabilities.
    async fn validate_volume_capabilities(
        &self,
        request: Request<csi::ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<csi::ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        info!(volume_id = %volume_id, "ValidateVolumeCapabilities request");

        // Verify the volume exists
        let mut client = self.get_client().await?;
        client.get_volume(volume_id).await?;

        // If the volume exists, confirm the requested capabilities
        Ok(Response::new(csi::ValidateVolumeCapabilitiesResponse {
            confirmed: Some(csi::validate_volume_capabilities_response::Confirmed {
                volume_context: req.volume_context,
                volume_capabilities: req.volume_capabilities,
                parameters: req.parameters,
                mutable_parameters: req.mutable_parameters,
            }),
            message: String::new(),
        }))
    }

    /// Publish a volume to a node (not implemented for external attacher).
    async fn controller_publish_volume(
        &self,
        _request: Request<csi::ControllerPublishVolumeRequest>,
    ) -> Result<Response<csi::ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "ControllerPublishVolume is not supported",
        ))
    }

    /// Unpublish a volume from a node (not implemented for external attacher).
    async fn controller_unpublish_volume(
        &self,
        _request: Request<csi::ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<csi::ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "ControllerUnpublishVolume is not supported",
        ))
    }

    /// List volumes (not implemented).
    async fn list_volumes(
        &self,
        _request: Request<csi::ListVolumesRequest>,
    ) -> Result<Response<csi::ListVolumesResponse>, Status> {
        Err(Status::unimplemented("ListVolumes is not supported"))
    }

    /// Get capacity (not implemented).
    async fn get_capacity(
        &self,
        _request: Request<csi::GetCapacityRequest>,
    ) -> Result<Response<csi::GetCapacityResponse>, Status> {
        Err(Status::unimplemented("GetCapacity is not supported"))
    }

    /// List snapshots (not implemented).
    async fn list_snapshots(
        &self,
        _request: Request<csi::ListSnapshotsRequest>,
    ) -> Result<Response<csi::ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("ListSnapshots is not supported"))
    }

    /// Get volume (not implemented).
    async fn controller_get_volume(
        &self,
        _request: Request<csi::ControllerGetVolumeRequest>,
    ) -> Result<Response<csi::ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("ControllerGetVolume is not supported"))
    }

    /// Modify volume (not implemented).
    async fn controller_modify_volume(
        &self,
        _request: Request<csi::ControllerModifyVolumeRequest>,
    ) -> Result<Response<csi::ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "ControllerModifyVolume is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_export_type() {
        let mut params = HashMap::new();

        // Default to iSCSI
        assert_eq!(
            ControllerService::parse_export_type(&params),
            ExportType::Iscsi
        );

        // Explicit iSCSI
        params.insert("exportType".to_string(), "iscsi".to_string());
        assert_eq!(
            ControllerService::parse_export_type(&params),
            ExportType::Iscsi
        );

        // NVMeoF
        params.insert("exportType".to_string(), "nvmeof".to_string());
        assert_eq!(
            ControllerService::parse_export_type(&params),
            ExportType::Nvmeof
        );

        // Alternative key
        params.clear();
        params.insert("export_type".to_string(), "nvme".to_string());
        assert_eq!(
            ControllerService::parse_export_type(&params),
            ExportType::Nvmeof
        );
    }

    #[test]
    fn test_get_volume_size() {
        // No capacity range
        assert_eq!(ControllerService::get_volume_size(None), DEFAULT_VOLUME_SIZE);

        // Required bytes takes precedence
        let range = csi::CapacityRange {
            required_bytes: 2 * 1024 * 1024 * 1024,
            limit_bytes: 5 * 1024 * 1024 * 1024,
        };
        assert_eq!(
            ControllerService::get_volume_size(Some(&range)),
            2 * 1024 * 1024 * 1024
        );

        // Fall back to limit_bytes if required_bytes is 0
        let range = csi::CapacityRange {
            required_bytes: 0,
            limit_bytes: 5 * 1024 * 1024 * 1024,
        };
        assert_eq!(
            ControllerService::get_volume_size(Some(&range)),
            5 * 1024 * 1024 * 1024
        );

        // Default if both are 0
        let range = csi::CapacityRange {
            required_bytes: 0,
            limit_bytes: 0,
        };
        assert_eq!(
            ControllerService::get_volume_size(Some(&range)),
            DEFAULT_VOLUME_SIZE
        );
    }
}
