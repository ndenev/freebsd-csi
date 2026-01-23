//! CSI Controller Service Implementation
//!
//! Handles volume and snapshot lifecycle operations by calling the ctld-agent daemon.

use std::collections::HashMap;

use prost_types::Timestamp;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::agent::{
    AuthCredentials, ExportType, IscsiChapCredentials, NvmeAuthCredentials, auth_credentials,
};
use crate::agent_client::{AgentClient, TlsConfig};
use crate::csi;
use crate::metrics::{self, OperationTimer};

// Standard CSI secret keys for iSCSI CHAP authentication
// These follow the Linux open-iscsi naming conventions used by the CSI spec
const CHAP_USERNAME_KEY: &str = "node.session.auth.username";
const CHAP_PASSWORD_KEY: &str = "node.session.auth.password";
const CHAP_MUTUAL_USERNAME_KEY: &str = "node.session.auth.username_in";
const CHAP_MUTUAL_PASSWORD_KEY: &str = "node.session.auth.password_in";

// Secret keys for NVMeoF DH-HMAC-CHAP authentication
const NVME_HOST_NQN_KEY: &str = "nvme.auth.host_nqn";
const NVME_SECRET_KEY: &str = "nvme.auth.secret";
const NVME_HASH_FUNCTION_KEY: &str = "nvme.auth.hash_function";
const NVME_DH_GROUP_KEY: &str = "nvme.auth.dh_group";

/// Default volume size: 1GB
const DEFAULT_VOLUME_SIZE: i64 = 1024 * 1024 * 1024;

/// CSI Controller Service
///
/// Implements the CSI Controller service which handles:
/// - Volume creation and deletion
/// - Volume expansion
/// - Snapshot creation and deletion
/// - Capability reporting
///
/// Uses RwLock for the client cache to allow concurrent read access
/// (multiple operations can share the cached client) while still
/// providing exclusive access for cache updates.
pub struct ControllerService {
    /// Agent endpoint for ctld-agent connection
    agent_endpoint: String,
    /// TLS configuration for mTLS connection to ctld-agent
    tls_config: Option<TlsConfig>,
    /// Lazily initialized agent client connection (RwLock for better concurrency)
    client: RwLock<Option<AgentClient>>,
}

impl ControllerService {
    /// Create a new ControllerService with the specified agent endpoint.
    pub fn new(agent_endpoint: String) -> Self {
        Self {
            agent_endpoint,
            tls_config: None,
            client: RwLock::new(None),
        }
    }

    /// Create a new ControllerService with mTLS configuration.
    pub fn with_tls(agent_endpoint: String, tls_config: Option<TlsConfig>) -> Self {
        Self {
            agent_endpoint,
            tls_config,
            client: RwLock::new(None),
        }
    }

    /// Get or create the agent client connection.
    ///
    /// Uses a read lock first to check for an existing client (fast path),
    /// then upgrades to a write lock only if connection is needed.
    async fn get_client(&self) -> Result<AgentClient, Status> {
        // Fast path: read lock to check if client exists
        {
            let guard = self.client.read().await;
            if let Some(ref client) = *guard {
                return Ok(client.clone());
            }
        }

        // Slow path: write lock to create client
        let mut guard = self.client.write().await;
        // Double-check after acquiring write lock (another task may have connected)
        if let Some(ref client) = *guard {
            return Ok(client.clone());
        }

        info!(endpoint = %self.agent_endpoint, tls = %self.tls_config.is_some(), "Connecting to ctld-agent");
        let client = AgentClient::connect_with_tls(&self.agent_endpoint, self.tls_config.clone())
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to connect to ctld-agent");
                metrics::record_connection_attempt(false);
                metrics::set_agent_connected(false);
                Status::unavailable("Agent connection failed")
            })?;

        metrics::record_connection_attempt(true);
        metrics::set_agent_connected(true);
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Clear the cached connection (call on transport errors).
    async fn clear_client(&self) {
        let mut guard = self.client.write().await;
        if guard.is_some() {
            warn!("Clearing stale agent connection");
            metrics::set_agent_connected(false);
            *guard = None;
        }
    }

    /// Check if error indicates a transport failure that should clear the connection cache.
    fn is_transport_error(status: &Status) -> bool {
        matches!(
            status.code(),
            tonic::Code::Unavailable | tonic::Code::Unknown | tonic::Code::Internal
        ) && (status.message().contains("transport")
            || status.message().contains("connection")
            || status.message().contains("broken pipe")
            || status.message().contains("reset by peer"))
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

    /// Extract authentication credentials from CSI secrets based on export type.
    ///
    /// For iSCSI, extracts CHAP credentials using standard open-iscsi key names.
    /// For NVMeoF, extracts DH-HMAC-CHAP credentials for FreeBSD 15+ support.
    ///
    /// Returns None if no authentication credentials are provided, allowing
    /// backward compatibility with unauthenticated targets.
    fn extract_auth_credentials(
        secrets: &HashMap<String, String>,
        export_type: ExportType,
    ) -> Option<AuthCredentials> {
        match export_type {
            ExportType::Iscsi => Self::extract_iscsi_chap(secrets),
            ExportType::Nvmeof => Self::extract_nvme_auth(secrets),
            ExportType::Unspecified => None,
        }
    }

    /// Extract iSCSI CHAP credentials from secrets.
    fn extract_iscsi_chap(secrets: &HashMap<String, String>) -> Option<AuthCredentials> {
        let username = secrets.get(CHAP_USERNAME_KEY)?;
        let password = secrets.get(CHAP_PASSWORD_KEY)?;

        // Username and password are required for CHAP
        if username.is_empty() || password.is_empty() {
            return None;
        }

        let credentials = IscsiChapCredentials {
            username: username.clone(),
            secret: password.clone(),
            // Mutual CHAP is optional
            mutual_username: secrets
                .get(CHAP_MUTUAL_USERNAME_KEY)
                .cloned()
                .unwrap_or_default(),
            mutual_secret: secrets
                .get(CHAP_MUTUAL_PASSWORD_KEY)
                .cloned()
                .unwrap_or_default(),
        };

        debug!(
            username = %credentials.username,
            has_mutual = !credentials.mutual_username.is_empty(),
            "Extracted iSCSI CHAP credentials"
        );

        Some(AuthCredentials {
            credentials: Some(auth_credentials::Credentials::IscsiChap(credentials)),
        })
    }

    /// Extract NVMeoF DH-HMAC-CHAP credentials from secrets.
    fn extract_nvme_auth(secrets: &HashMap<String, String>) -> Option<AuthCredentials> {
        let host_nqn = secrets.get(NVME_HOST_NQN_KEY)?;
        let secret = secrets.get(NVME_SECRET_KEY)?;

        // Host NQN and secret are required
        if host_nqn.is_empty() || secret.is_empty() {
            return None;
        }

        let credentials = NvmeAuthCredentials {
            host_nqn: host_nqn.clone(),
            secret: secret.clone(),
            // Hash function and DH group have sensible defaults
            hash_function: secrets
                .get(NVME_HASH_FUNCTION_KEY)
                .cloned()
                .unwrap_or_else(|| "SHA-256".to_string()),
            dh_group: secrets.get(NVME_DH_GROUP_KEY).cloned().unwrap_or_default(),
        };

        debug!(
            host_nqn = %credentials.host_nqn,
            hash_function = %credentials.hash_function,
            has_dh = !credentials.dh_group.is_empty(),
            "Extracted NVMeoF DH-HMAC-CHAP credentials"
        );

        Some(AuthCredentials {
            credentials: Some(auth_credentials::Credentials::NvmeAuth(credentials)),
        })
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
    fn agent_volume_to_csi(
        volume: &crate::agent::Volume,
        parameters: &HashMap<String, String>,
    ) -> csi::Volume {
        let mut volume_context = HashMap::new();
        volume_context.insert("target_name".to_string(), volume.target_name.clone());
        volume_context.insert("lun_id".to_string(), volume.lun_id.to_string());
        volume_context.insert("zfs_dataset".to_string(), volume.zfs_dataset.clone());

        let export_type_str =
            match ExportType::try_from(volume.export_type).unwrap_or(ExportType::Unspecified) {
                ExportType::Iscsi => "iscsi",
                ExportType::Nvmeof => "nvmeof",
                ExportType::Unspecified => "unspecified",
            };
        volume_context.insert("export_type".to_string(), export_type_str.to_string());

        // Pass through portal/address info for node service (required on Linux)
        // StorageClass parameters like: portal, targetPortal, transportAddr, transportPort
        if let Some(portal) = parameters
            .get("portal")
            .or_else(|| parameters.get("targetPortal"))
        {
            volume_context.insert("portal".to_string(), portal.clone());
        }

        // For NVMeoF, pass transport address and port
        if let Some(addr) = parameters
            .get("transportAddr")
            .or_else(|| parameters.get("transport_addr"))
        {
            volume_context.insert("transport_addr".to_string(), addr.clone());
        }
        if let Some(port) = parameters
            .get("transportPort")
            .or_else(|| parameters.get("transport_port"))
        {
            volume_context.insert("transport_port".to_string(), port.clone());
        }

        // Pass through filesystem type for node service
        if let Some(fs_type) = parameters
            .get("fsType")
            .or_else(|| parameters.get("fs_type"))
        {
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
        let timer = OperationTimer::new("create_volume");
        let req = request.into_inner();
        let name = &req.name;

        if name.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("Volume name is required"));
        }

        info!(name = %name, "CreateVolume request");

        let size_bytes = Self::get_volume_size(req.capacity_range.as_ref());
        let export_type = Self::parse_export_type(&req.parameters);

        // Extract authentication credentials from CSI secrets
        let auth = Self::extract_auth_credentials(&req.secrets, export_type);

        debug!(
            name = %name,
            size_bytes = size_bytes,
            export_type = ?export_type,
            has_auth = auth.is_some(),
            "Creating volume"
        );

        let mut client = self.get_client().await?;
        let volume = match client
            .create_volume(name, size_bytes, export_type, req.parameters.clone(), auth)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Failed to create volume via agent");
                if Self::is_transport_error(&e) {
                    self.clear_client().await;
                }
                timer.failure(&e.code().to_string());
                return Err(e);
            }
        };

        info!(
            volume_id = %volume.id,
            name = %volume.name,
            size_bytes = volume.size_bytes,
            "Volume created successfully"
        );

        timer.success();
        Ok(Response::new(csi::CreateVolumeResponse {
            volume: Some(Self::agent_volume_to_csi(&volume, &req.parameters)),
        }))
    }

    /// Delete a volume.
    async fn delete_volume(
        &self,
        request: Request<csi::DeleteVolumeRequest>,
    ) -> Result<Response<csi::DeleteVolumeResponse>, Status> {
        let timer = OperationTimer::new("delete_volume");
        let req = request.into_inner();
        let volume_id = &req.volume_id;

        if volume_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        info!(volume_id = %volume_id, "DeleteVolume request");

        let mut client = self.get_client().await?;
        if let Err(e) = client.delete_volume(volume_id).await {
            // NOT_FOUND is acceptable - volume may have already been deleted
            if e.code() == tonic::Code::NotFound {
                warn!(volume_id = %volume_id, "Volume not found, treating as already deleted");
            } else {
                error!(error = %e, "Failed to delete volume via agent");
                if Self::is_transport_error(&e) {
                    self.clear_client().await;
                }
                timer.failure(&e.code().to_string());
                return Err(e);
            }
        }

        info!(volume_id = %volume_id, "Volume deleted successfully");

        timer.success();
        Ok(Response::new(csi::DeleteVolumeResponse {}))
    }

    /// Expand a volume.
    async fn controller_expand_volume(
        &self,
        request: Request<csi::ControllerExpandVolumeRequest>,
    ) -> Result<Response<csi::ControllerExpandVolumeResponse>, Status> {
        let timer = OperationTimer::new("expand_volume");
        let req = request.into_inner();
        let volume_id = &req.volume_id;

        if volume_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        let capacity_range = match req.capacity_range.as_ref() {
            Some(r) => r,
            None => {
                timer.failure("invalid_argument");
                return Err(Status::invalid_argument(
                    "Capacity range is required for volume expansion",
                ));
            }
        };

        let new_size_bytes = if capacity_range.required_bytes > 0 {
            capacity_range.required_bytes
        } else {
            capacity_range.limit_bytes
        };

        if new_size_bytes <= 0 {
            timer.failure("invalid_argument");
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
        let actual_size = match client.expand_volume(volume_id, new_size_bytes).await {
            Ok(size) => size,
            Err(e) => {
                error!(error = %e, "Failed to expand volume via agent");
                if Self::is_transport_error(&e) {
                    self.clear_client().await;
                }
                timer.failure(&e.code().to_string());
                return Err(e);
            }
        };

        info!(
            volume_id = %volume_id,
            actual_size = actual_size,
            "Volume expanded successfully"
        );

        timer.success();
        Ok(Response::new(csi::ControllerExpandVolumeResponse {
            capacity_bytes: actual_size,
            // Node expansion is required to resize the filesystem layer.
            // - For ext4/xfs: NodeExpandVolume runs resize2fs/xfs_growfs
            // - For ZFS/UFS: NodeExpandVolume detects this and returns success
            //   (filesystem expansion is automatic for these types)
            node_expansion_required: true,
        }))
    }

    /// Report controller capabilities.
    async fn controller_get_capabilities(
        &self,
        _request: Request<csi::ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<csi::ControllerGetCapabilitiesResponse>, Status> {
        use csi::controller_service_capability::rpc::Type;

        let capabilities = vec![
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: Type::CreateDeleteVolume as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: Type::CreateDeleteSnapshot as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: Type::ExpandVolume as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: Type::ListVolumes as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: Type::GetCapacity as i32,
                    },
                )),
            },
            csi::ControllerServiceCapability {
                r#type: Some(csi::controller_service_capability::Type::Rpc(
                    csi::controller_service_capability::Rpc {
                        r#type: Type::ListSnapshots as i32,
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
        let timer = OperationTimer::new("create_snapshot");
        let req = request.into_inner();
        let source_volume_id = &req.source_volume_id;
        let name = &req.name;

        if source_volume_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("Source volume ID is required"));
        }

        if name.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("Snapshot name is required"));
        }

        info!(
            source_volume_id = %source_volume_id,
            name = %name,
            "CreateSnapshot request"
        );

        let mut client = self.get_client().await?;
        let snapshot = match client.create_snapshot(source_volume_id, name).await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to create snapshot via agent");
                if Self::is_transport_error(&e) {
                    self.clear_client().await;
                }
                timer.failure(&e.code().to_string());
                return Err(e);
            }
        };

        info!(
            snapshot_id = %snapshot.id,
            source_volume_id = %snapshot.source_volume_id,
            "Snapshot created successfully"
        );

        timer.success();
        Ok(Response::new(csi::CreateSnapshotResponse {
            snapshot: Some(Self::agent_snapshot_to_csi(&snapshot)),
        }))
    }

    /// Delete a snapshot.
    async fn delete_snapshot(
        &self,
        request: Request<csi::DeleteSnapshotRequest>,
    ) -> Result<Response<csi::DeleteSnapshotResponse>, Status> {
        let timer = OperationTimer::new("delete_snapshot");
        let req = request.into_inner();
        let snapshot_id = &req.snapshot_id;

        if snapshot_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("Snapshot ID is required"));
        }

        info!(snapshot_id = %snapshot_id, "DeleteSnapshot request");

        let mut client = self.get_client().await?;
        if let Err(e) = client.delete_snapshot(snapshot_id).await {
            // NOT_FOUND is acceptable - snapshot may have already been deleted
            if e.code() == tonic::Code::NotFound {
                warn!(snapshot_id = %snapshot_id, "Snapshot not found, treating as already deleted");
            } else {
                error!(error = %e, "Failed to delete snapshot via agent");
                if Self::is_transport_error(&e) {
                    self.clear_client().await;
                }
                timer.failure(&e.code().to_string());
                return Err(e);
            }
        }

        info!(snapshot_id = %snapshot_id, "Snapshot deleted successfully");

        timer.success();
        Ok(Response::new(csi::DeleteSnapshotResponse {}))
    }

    /// Validate volume capabilities.
    ///
    /// Per CSI spec, returns "confirmed" only if ALL requested capabilities are supported.
    /// This driver supports:
    /// - Mount volumes with various filesystems
    /// - Block volumes (raw device access)
    /// - Access modes: SINGLE_NODE_WRITER, SINGLE_NODE_READER_ONLY, MULTI_NODE_READER_ONLY
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

        // Validate each requested capability
        let mut unsupported_reasons: Vec<String> = Vec::new();

        for cap in &req.volume_capabilities {
            // Determine if this is a block volume request
            let is_block = matches!(
                &cap.access_type,
                Some(csi::volume_capability::AccessType::Block(_))
            );

            // Check access type (mount vs block)
            match &cap.access_type {
                Some(csi::volume_capability::AccessType::Mount(_)) => {
                    // Mount volumes are fully supported
                }
                Some(csi::volume_capability::AccessType::Block(_)) => {
                    // Block volumes are supported (raw device access)
                }
                None => {
                    unsupported_reasons
                        .push("Volume capability must specify access type".to_string());
                }
            }

            // Check access mode
            if let Some(access_mode) = &cap.access_mode {
                use csi::volume_capability::access_mode::Mode;
                match Mode::try_from(access_mode.mode) {
                    Ok(Mode::SingleNodeWriter) => {
                        // ReadWriteOnce (RWO) - fully supported
                    }
                    Ok(Mode::SingleNodeReaderOnly) => {
                        // ReadOnlyOnce - supported
                    }
                    Ok(Mode::MultiNodeReaderOnly) => {
                        // ReadOnlyMany (ROX) - supported (iSCSI/NVMeoF allows multiple readers)
                    }
                    Ok(Mode::MultiNodeSingleWriter) => {
                        // Multiple nodes attached, single writer - useful for active-passive failover.
                        // Supported for block volumes (application/SCSI PR handles coordination).
                        if !is_block {
                            unsupported_reasons.push(
                                "MULTI_NODE_SINGLE_WRITER not supported for mount volumes"
                                    .to_string(),
                            );
                        }
                    }
                    Ok(Mode::MultiNodeMultiWriter) => {
                        // ReadWriteMany (RWX) - supported for block volumes (application handles coordination),
                        // but not for mount volumes (standard filesystems can't handle concurrent writers)
                        if !is_block {
                            unsupported_reasons.push(
                                "MULTI_NODE_MULTI_WRITER not supported for mount volumes (requires cluster filesystem)"
                                    .to_string(),
                            );
                        }
                    }
                    Ok(Mode::SingleNodeSingleWriter) => {
                        // ReadWriteOncePod (RWOP) - GA in Kubernetes 1.29+
                        // Kubernetes enforces single-pod constraint, driver just allows it
                    }
                    Ok(Mode::SingleNodeMultiWriter) => {
                        // Single node, multiple writers - supported (same as RWO semantically)
                    }
                    Ok(Mode::Unknown) | Err(_) => {
                        unsupported_reasons
                            .push(format!("Unknown access mode: {}", access_mode.mode));
                    }
                }
            }
        }

        // If any capability is unsupported, return without confirmed
        if !unsupported_reasons.is_empty() {
            let message = unsupported_reasons.join("; ");
            warn!(volume_id = %volume_id, message = %message, "Volume capabilities not supported");
            return Ok(Response::new(csi::ValidateVolumeCapabilitiesResponse {
                confirmed: None,
                message,
            }));
        }

        // All capabilities are supported
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

    /// List all volumes.
    ///
    /// Returns volumes with optional pagination support.
    async fn list_volumes(
        &self,
        request: Request<csi::ListVolumesRequest>,
    ) -> Result<Response<csi::ListVolumesResponse>, Status> {
        let req = request.into_inner();

        info!(
            max_entries = req.max_entries,
            starting_token = %req.starting_token,
            "ListVolumes request"
        );

        let mut client = self.get_client().await?;
        let starting_token = if req.starting_token.is_empty() {
            None
        } else {
            Some(req.starting_token.as_str())
        };

        let (volumes, next_token) = client.list_volumes(req.max_entries, starting_token).await?;

        // Convert agent volumes to CSI list entries
        // Note: We use empty parameters since we don't have the original StorageClass params
        // The volume_context from the agent already contains the essential info
        let entries: Vec<csi::list_volumes_response::Entry> = volumes
            .iter()
            .map(|v| {
                let volume = Self::agent_volume_to_csi(v, &HashMap::new());
                csi::list_volumes_response::Entry {
                    volume: Some(volume),
                    status: None, // We don't track published nodes currently
                }
            })
            .collect();

        info!(count = entries.len(), "ListVolumes completed");

        Ok(Response::new(csi::ListVolumesResponse {
            entries,
            next_token: next_token.unwrap_or_default(),
        }))
    }

    /// Get storage capacity.
    ///
    /// Returns the available capacity from the ZFS storage pool.
    async fn get_capacity(
        &self,
        request: Request<csi::GetCapacityRequest>,
    ) -> Result<Response<csi::GetCapacityResponse>, Status> {
        let req = request.into_inner();

        info!(
            parameters = ?req.parameters,
            "GetCapacity request"
        );

        let mut client = self.get_client().await?;
        let (available_capacity, _total_capacity) = client.get_capacity().await?;

        info!(available_capacity, "GetCapacity completed");

        Ok(Response::new(csi::GetCapacityResponse {
            available_capacity,
            maximum_volume_size: None, // No per-volume limit
            minimum_volume_size: None, // No minimum
        }))
    }

    /// List snapshots.
    ///
    /// Returns snapshots with optional filtering by source volume and pagination.
    async fn list_snapshots(
        &self,
        request: Request<csi::ListSnapshotsRequest>,
    ) -> Result<Response<csi::ListSnapshotsResponse>, Status> {
        let req = request.into_inner();

        info!(
            source_volume_id = %req.source_volume_id,
            snapshot_id = %req.snapshot_id,
            max_entries = req.max_entries,
            starting_token = %req.starting_token,
            "ListSnapshots request"
        );

        // If snapshot_id is specified, just return that one snapshot
        if !req.snapshot_id.is_empty() {
            // For single snapshot lookup, we'd need a get_snapshot method
            // For now, filter from the list
            let mut client = self.get_client().await?;
            let source_filter = if req.source_volume_id.is_empty() {
                None
            } else {
                Some(req.source_volume_id.as_str())
            };

            let (snapshots, _) = client
                .list_snapshots(source_filter, 0, None) // Get all
                .await?;

            let matching: Vec<csi::list_snapshots_response::Entry> = snapshots
                .iter()
                .filter(|s| s.id == req.snapshot_id)
                .map(|s| csi::list_snapshots_response::Entry {
                    snapshot: Some(Self::agent_snapshot_to_csi(s)),
                })
                .collect();

            return Ok(Response::new(csi::ListSnapshotsResponse {
                entries: matching,
                next_token: String::new(),
            }));
        }

        let mut client = self.get_client().await?;
        let source_filter = if req.source_volume_id.is_empty() {
            None
        } else {
            Some(req.source_volume_id.as_str())
        };
        let starting_token = if req.starting_token.is_empty() {
            None
        } else {
            Some(req.starting_token.as_str())
        };

        let (snapshots, next_token) = client
            .list_snapshots(source_filter, req.max_entries, starting_token)
            .await?;

        let entries: Vec<csi::list_snapshots_response::Entry> = snapshots
            .iter()
            .map(|s| csi::list_snapshots_response::Entry {
                snapshot: Some(Self::agent_snapshot_to_csi(s)),
            })
            .collect();

        info!(count = entries.len(), "ListSnapshots completed");

        Ok(Response::new(csi::ListSnapshotsResponse {
            entries,
            next_token: next_token.unwrap_or_default(),
        }))
    }

    /// Get volume (not implemented).
    async fn controller_get_volume(
        &self,
        _request: Request<csi::ControllerGetVolumeRequest>,
    ) -> Result<Response<csi::ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented(
            "ControllerGetVolume is not supported",
        ))
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
        assert_eq!(
            ControllerService::get_volume_size(None),
            DEFAULT_VOLUME_SIZE
        );

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
