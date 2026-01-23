//! gRPC Storage Agent service implementation.
//!
//! This module provides the gRPC service layer that ties together ZFS volume management
//! and iSCSI/NVMeoF export functionality.
//!
//! Rate limiting is implemented using a semaphore to prevent overload from concurrent
//! storage operations.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::{RwLock, Semaphore};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument, warn};

/// Default maximum number of concurrent storage operations
const DEFAULT_MAX_CONCURRENT_OPS: usize = 10;

use crate::ctl::{
    AuthConfig, CtlError, CtlManager, ExportType as CtlExportType, IscsiChapAuth, NvmeAuth,
};
use crate::metrics::{self, OperationTimer};
use crate::zfs::{VolumeMetadata as ZfsVolumeMetadata, ZfsManager};

/// Generated protobuf types and service trait
pub mod proto {
    tonic::include_proto!("ctld_agent.v1");
}

use proto::storage_agent_server::StorageAgent;
use proto::{
    AuthCredentials, CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest,
    CreateVolumeResponse, DeleteSnapshotRequest, DeleteSnapshotResponse, DeleteVolumeRequest,
    DeleteVolumeResponse, ExpandVolumeRequest, ExpandVolumeResponse, ExportType,
    GetCapacityRequest, GetCapacityResponse, GetSnapshotRequest, GetSnapshotResponse,
    GetVolumeRequest, GetVolumeResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    ListVolumesRequest, ListVolumesResponse, Snapshot, Volume,
};

/// Convert proto ExportType to CTL ExportType
fn to_ctl_export_type(export_type: ExportType) -> Option<CtlExportType> {
    match export_type {
        ExportType::Iscsi => Some(CtlExportType::Iscsi),
        ExportType::Nvmeof => Some(CtlExportType::Nvmeof),
        ExportType::Unspecified => None,
    }
}

/// Convert proto ExportType to CTL ExportType (for ZFS metadata storage)
fn proto_to_ctl_export_type(export_type: ExportType) -> Option<CtlExportType> {
    match export_type {
        ExportType::Iscsi => Some(CtlExportType::Iscsi),
        ExportType::Nvmeof => Some(CtlExportType::Nvmeof),
        ExportType::Unspecified => None,
    }
}

/// Convert CTL ExportType to proto ExportType
fn ctl_to_proto_export_type(export_type: CtlExportType) -> ExportType {
    match export_type {
        CtlExportType::Iscsi => ExportType::Iscsi,
        CtlExportType::Nvmeof => ExportType::Nvmeof,
    }
}

/// Convert proto AuthCredentials to CTL AuthConfig
fn proto_to_ctl_auth(auth: Option<&AuthCredentials>) -> AuthConfig {
    use proto::auth_credentials::Credentials;

    match auth.and_then(|a| a.credentials.as_ref()) {
        None => AuthConfig::None,
        Some(Credentials::IscsiChap(chap)) => {
            let has_mutual = !chap.mutual_username.is_empty() && !chap.mutual_secret.is_empty();
            if has_mutual {
                AuthConfig::IscsiChap(IscsiChapAuth::with_mutual(
                    &chap.username,
                    &chap.secret,
                    &chap.mutual_username,
                    &chap.mutual_secret,
                ))
            } else {
                AuthConfig::IscsiChap(IscsiChapAuth::new(&chap.username, &chap.secret))
            }
        }
        Some(Credentials::NvmeAuth(nvme)) => {
            let mut auth = NvmeAuth::new(&nvme.host_nqn, &nvme.secret, &nvme.hash_function);
            if !nvme.dh_group.is_empty() {
                auth = auth.with_dh_group(&nvme.dh_group);
            }
            AuthConfig::NvmeAuth(auth)
        }
    }
}

/// Get current Unix timestamp in seconds
fn unix_timestamp_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Apply pagination to a list of items
fn paginate<T>(items: Vec<T>, max_entries: i32, starting_token: &str) -> (Vec<T>, String) {
    let max_entries = if max_entries > 0 {
        max_entries as usize
    } else {
        items.len()
    };

    let start_idx = if !starting_token.is_empty() {
        starting_token.parse::<usize>().unwrap_or(0)
    } else {
        0
    };

    let total_len = items.len();
    let end_idx = std::cmp::min(start_idx + max_entries, total_len);

    let paginated: Vec<T> = items
        .into_iter()
        .skip(start_idx)
        .take(end_idx - start_idx)
        .collect();

    let next_token = if end_idx < total_len {
        end_idx.to_string()
    } else {
        String::new()
    };

    (paginated, next_token)
}

/// Internal tracking of volume metadata
#[derive(Debug, Clone)]
struct VolumeMetadata {
    /// Volume ID (same as name for now)
    id: String,
    /// Volume name
    name: String,
    /// Export type (iSCSI or NVMeoF)
    export_type: ExportType,
    /// Target name (IQN or NQN)
    target_name: String,
    /// LUN ID
    lun_id: i32,
    /// Additional parameters
    parameters: HashMap<String, String>,
}

/// Internal tracking of snapshot metadata
#[derive(Debug, Clone)]
struct SnapshotMetadata {
    /// Snapshot ID (format: volume_id@snap_name)
    id: String,
    /// Source volume ID
    source_volume_id: String,
    /// Snapshot name
    name: String,
    /// Creation timestamp (Unix seconds)
    creation_time: i64,
}

/// gRPC Storage Agent service
///
/// Uses a semaphore to limit concurrent operations and prevent overload.
/// When the semaphore is exhausted, new requests will receive ResourceExhausted.
pub struct StorageService {
    /// ZFS volume manager
    zfs: Arc<RwLock<ZfsManager>>,
    /// Unified CTL manager (handles both iSCSI and NVMeoF)
    ctl: Arc<RwLock<CtlManager>>,
    /// Volume metadata tracking
    volumes: Arc<RwLock<HashMap<String, VolumeMetadata>>>,
    /// Snapshot metadata tracking
    snapshots: Arc<RwLock<HashMap<String, SnapshotMetadata>>>,
    /// Semaphore for rate limiting concurrent operations
    ops_semaphore: Arc<Semaphore>,
    /// Maximum concurrent operations (for error messages)
    max_concurrent_ops: usize,
}

impl StorageService {
    /// Create a new StorageService with default rate limiting (10 concurrent ops)
    pub fn new(zfs: Arc<RwLock<ZfsManager>>, ctl: Arc<RwLock<CtlManager>>) -> Self {
        Self::with_concurrency_limit(zfs, ctl, DEFAULT_MAX_CONCURRENT_OPS)
    }

    /// Create a new StorageService with configurable concurrency limit
    pub fn with_concurrency_limit(
        zfs: Arc<RwLock<ZfsManager>>,
        ctl: Arc<RwLock<CtlManager>>,
        max_concurrent_ops: usize,
    ) -> Self {
        Self {
            zfs,
            ctl,
            volumes: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            ops_semaphore: Arc::new(Semaphore::new(max_concurrent_ops)),
            max_concurrent_ops,
        }
    }

    /// Acquire rate limiting permit, returning ResourceExhausted if too many concurrent ops
    async fn acquire_permit(
        &self,
        operation: &str,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, Status> {
        match self.ops_semaphore.try_acquire() {
            Ok(permit) => {
                // Track current concurrent operations
                let current_ops = self.max_concurrent_ops - self.ops_semaphore.available_permits();
                metrics::set_concurrent_ops(current_ops);
                Ok(permit)
            }
            Err(_) => {
                warn!(
                    "Rate limit exceeded: {} concurrent operations already in progress",
                    self.max_concurrent_ops
                );
                metrics::record_rate_limited(operation);
                Err(Status::resource_exhausted(format!(
                    "Too many concurrent operations (max: {}). Please retry later.",
                    self.max_concurrent_ops
                )))
            }
        }
    }

    /// Restore volume metadata from ZFS user properties on startup
    pub async fn restore_from_zfs(&self) -> Result<usize, String> {
        info!("Restoring volume metadata from ZFS user properties");

        let volumes_with_metadata = {
            let zfs = self.zfs.read().await;
            zfs.list_volumes_with_metadata()
                .map_err(|e| format!("failed to list volumes with metadata: {}", e))?
        };

        let mut restored_count = 0;
        let mut volumes = self.volumes.write().await;

        for (vol_name, zfs_meta) in volumes_with_metadata {
            // Convert CTL ExportType to proto ExportType
            let export_type = ctl_to_proto_export_type(zfs_meta.export_type);

            let metadata = VolumeMetadata {
                id: vol_name.clone(),
                name: vol_name.clone(),
                export_type,
                target_name: zfs_meta.target_name.clone(),
                lun_id: zfs_meta.lun_id.unwrap_or(0) as i32,
                parameters: zfs_meta.parameters.clone(),
            };

            volumes.insert(vol_name.clone(), metadata);
            restored_count += 1;
            info!(
                "Restored volume '{}' (export_type={}, target={})",
                vol_name, zfs_meta.export_type, zfs_meta.target_name
            );
        }

        info!(
            "Restored {} volume(s) from ZFS user properties",
            restored_count
        );
        Ok(restored_count)
    }

    /// Reconcile exports: ensure all volumes in ZFS metadata are exported
    ///
    /// This should be called after restore_from_zfs and load_config to ensure
    /// that CTL exports match the ZFS metadata (source of truth for what volumes exist).
    /// After reconciliation, writes the unified UCL config.
    pub async fn reconcile_exports(&self) -> Result<usize, String> {
        info!("Reconciling CTL exports with ZFS metadata");

        let volumes = self.volumes.read().await;
        let mut reconciled_count = 0;

        for (vol_name, metadata) in volumes.iter() {
            // Get device path for this volume
            let device_path = {
                let zfs = self.zfs.read().await;
                zfs.get_device_path(vol_name)
            };

            // Check if export exists in CtlManager
            let needs_export = {
                let ctl = self.ctl.read().await;
                ctl.get_export(vol_name).is_none()
            };

            if !needs_export {
                continue;
            }

            let Some(ctl_export_type) = to_ctl_export_type(metadata.export_type) else {
                debug!(
                    "Volume '{}' has no export type, skipping reconciliation",
                    vol_name
                );
                continue;
            };

            let ctl = self.ctl.read().await;
            // Note: Auth credentials are not persisted, so reconciliation
            // re-exports without authentication. This is acceptable as the
            // CSI driver will need to recreate volumes if auth is required.
            match ctl.export_volume(
                vol_name,
                &device_path,
                ctl_export_type,
                metadata.lun_id as u32,
                AuthConfig::None,
            ) {
                Ok(_) => {
                    info!(
                        "Reconciled: re-exported {:?} target for '{}'",
                        ctl_export_type, vol_name
                    );
                    reconciled_count += 1;
                }
                Err(e) => {
                    // Ignore "already exists" errors (race with load_config)
                    if !e.to_string().contains("exists") {
                        warn!("Failed to reconcile export for '{}': {}", vol_name, e);
                    }
                }
            }
        }
        drop(volumes);

        // Write unified UCL config after reconciliation
        if reconciled_count > 0 {
            let ctl = self.ctl.read().await;
            if let Err(e) = ctl.write_config() {
                warn!("Failed to write CTL config after reconciliation: {}", e);
            }
        }

        info!("Reconciled {} export(s)", reconciled_count);
        Ok(reconciled_count)
    }

    /// Convert ZFS dataset info to proto Volume
    fn dataset_to_volume(
        &self,
        dataset: &crate::zfs::Dataset,
        metadata: &VolumeMetadata,
    ) -> Volume {
        // Use volsize for zvols (the actual volume capacity), fall back to referenced
        let size_bytes = dataset.volsize.unwrap_or(dataset.referenced) as i64;
        Volume {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            size_bytes,
            zfs_dataset: dataset.name.clone(),
            export_type: metadata.export_type.into(),
            target_name: metadata.target_name.clone(),
            lun_id: metadata.lun_id,
            parameters: metadata.parameters.clone(),
        }
    }
}

#[tonic::async_trait]
impl StorageAgent for StorageService {
    /// Create a new volume, export via iSCSI or NVMeoF
    #[instrument(skip(self, request))]
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let timer = OperationTimer::new("create_volume");

        // Rate limiting: acquire permit before proceeding
        let _permit = self.acquire_permit("create_volume").await?;

        let req = request.into_inner();
        info!(
            "CreateVolume request: name={}, size={}",
            req.name, req.size_bytes
        );

        if req.name.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("volume name cannot be empty"));
        }
        if req.size_bytes <= 0 {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("size_bytes must be positive"));
        }

        let export_type = ExportType::try_from(req.export_type).unwrap_or(ExportType::Unspecified);

        if export_type == ExportType::Unspecified {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument(
                "export_type must be ISCSI or NVMEOF",
            ));
        }

        // Create ZFS volume
        let dataset = {
            let zfs = self.zfs.read().await;
            match zfs.create_volume(&req.name, req.size_bytes as u64) {
                Ok(d) => d,
                Err(e) => {
                    timer.failure("zfs_error");
                    return Err(Status::internal(format!(
                        "failed to create ZFS volume: {}",
                        e
                    )));
                }
            }
        };

        // Get device path
        let device_path = {
            let zfs = self.zfs.read().await;
            zfs.get_device_path(&req.name)
        };

        // Default LUN/Namespace ID
        // Note: iSCSI LUN IDs can start at 0, but NVMeoF namespace IDs must start at 1
        // (NSID 0 is reserved per NVMe spec)
        let lun_id: i32 = match export_type {
            ExportType::Iscsi => 0,
            ExportType::Nvmeof => 1,
            _ => 0,
        };

        // Export the volume via unified CTL manager
        let ctl_export_type = to_ctl_export_type(export_type).expect("already validated");

        // Convert auth credentials from proto to CTL format
        let auth_config = proto_to_ctl_auth(req.auth.as_ref());
        let has_auth = auth_config.is_some();

        let target_name = {
            let ctl = self.ctl.read().await;
            match ctl.export_volume(
                &req.name,
                &device_path,
                ctl_export_type,
                lun_id as u32,
                auth_config,
            ) {
                Ok(export) => export.target_name.to_string(),
                Err(e) => {
                    warn!("Failed to export volume: {}", e);
                    timer.failure("export_error");
                    return Err(Status::internal(format!("failed to export volume: {}", e)));
                }
            }
        };

        if has_auth {
            info!("Exported volume {} with authentication enabled", req.name);
        }

        // Write UCL config and reload ctld
        // CRITICAL: If this fails, ctld won't know about the export and
        // initiators won't be able to connect. We must return error.
        {
            let ctl = self.ctl.read().await;
            if let Err(e) = ctl.write_config() {
                error!("Failed to write CTL config: {}", e);
                timer.failure("config_write_error");
                return Err(Status::internal(format!(
                    "Volume created but CTL config write failed: {}. Target may be inaccessible.",
                    e
                )));
            }
        }

        // Store metadata
        let metadata = VolumeMetadata {
            id: req.name.clone(),
            name: req.name.clone(),
            export_type,
            target_name: target_name.clone(),
            lun_id,
            parameters: req.parameters.clone(),
        };

        {
            let mut volumes = self.volumes.write().await;
            volumes.insert(req.name.clone(), metadata.clone());
        }

        // Persist metadata to ZFS user property
        let ctl_export_type_for_zfs =
            proto_to_ctl_export_type(export_type).expect("already validated");
        let zfs_metadata = ZfsVolumeMetadata {
            export_type: ctl_export_type_for_zfs,
            target_name: target_name.clone(),
            lun_id: Some(lun_id as u32),
            namespace_id: None,
            parameters: req.parameters.clone(),
            created_at: unix_timestamp_now(),
        };

        {
            let zfs = self.zfs.read().await;
            if let Err(e) = zfs.set_volume_metadata(&req.name, &zfs_metadata) {
                warn!("Failed to persist volume metadata to ZFS: {}", e);
                // Continue anyway - the volume is created, metadata is in memory
            }
        }

        let volume = self.dataset_to_volume(&dataset, &metadata);
        info!("Created volume: {}", req.name);

        // Update volume count metric
        {
            let volumes = self.volumes.read().await;
            metrics::set_volumes_count(volumes.len());
        }

        timer.success();
        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume),
        }))
    }

    /// Delete a volume and unexport from iSCSI/NVMeoF
    ///
    /// This operation is idempotent per CSI spec:
    /// - If volume doesn't exist in cache, still try to clean up ZFS
    /// - If unexport fails with "not found", treat as already unexported
    /// - If ZFS volume doesn't exist, treat as already deleted
    #[instrument(skip(self, request))]
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let timer = OperationTimer::new("delete_volume");

        // Rate limiting: acquire permit before proceeding
        let _permit = self.acquire_permit("delete_volume").await?;

        let req = request.into_inner();
        info!("DeleteVolume request: volume_id={}", req.volume_id);

        if req.volume_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("volume_id cannot be empty"));
        }

        // Get volume metadata - if not in cache, we'll still try to clean up ZFS
        let metadata = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.volume_id).cloned()
        };

        // Try to unexport the volume via unified CTL manager
        // This is idempotent - if already unexported, we continue
        {
            let ctl = self.ctl.read().await;
            match ctl.unexport_volume(&req.volume_id) {
                Ok(()) => {
                    // Write UCL config with updated (removed) export entries
                    // CRITICAL: If this fails, export will reappear on ctld restart
                    // pointing to a deleted zvol, causing errors for initiators.
                    if let Err(e) = ctl.write_config() {
                        error!("Failed to write CTL config after unexport: {}", e);
                        timer.failure("config_write_error");
                        return Err(Status::internal(format!(
                            "Unexport succeeded but CTL config write failed: {}. Export may reappear on restart.",
                            e
                        )));
                    }
                }
                Err(CtlError::TargetNotFound(_)) => {
                    // Already unexported - this is fine (idempotent per CSI spec)
                    debug!("Volume {} already unexported (idempotent)", req.volume_id);
                }
                Err(e) => {
                    // Unexport failed - export is still active.
                    // CRITICAL: If we delete the ZFS dataset now, we'll have an orphan
                    // export pointing to a non-existent zvol. Return error.
                    error!("Failed to unexport volume: {}", e);
                    timer.failure("unexport_error");
                    return Err(Status::internal(format!(
                        "Failed to unexport volume: {}. Cannot safely delete while exported.",
                        e
                    )));
                }
            }
        }

        // Clear ZFS metadata before deleting (for consistency)
        // Use volume_id as the name since metadata might be None
        let volume_name = metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_else(|| req.volume_id.clone());

        {
            let zfs = self.zfs.read().await;
            if let Err(e) = zfs.clear_volume_metadata(&volume_name) {
                debug!(
                    "Failed to clear volume metadata from ZFS: {} (may already be cleared)",
                    e
                );
                // Continue anyway - we're deleting the volume
            }
        }

        // Delete ZFS volume (this is now idempotent - returns Ok if doesn't exist)
        {
            let zfs = self.zfs.read().await;
            if let Err(e) = zfs.delete_volume(&volume_name) {
                timer.failure("zfs_error");
                return Err(Status::internal(format!(
                    "failed to delete ZFS volume: {}",
                    e
                )));
            }
        }

        // Remove metadata from cache
        {
            let mut volumes = self.volumes.write().await;
            volumes.remove(&req.volume_id);
            metrics::set_volumes_count(volumes.len());
        }

        info!("Deleted volume: {}", req.volume_id);
        timer.success();
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    /// Expand (resize) a volume
    #[instrument(skip(self, request))]
    async fn expand_volume(
        &self,
        request: Request<ExpandVolumeRequest>,
    ) -> Result<Response<ExpandVolumeResponse>, Status> {
        let timer = OperationTimer::new("expand_volume");

        // Rate limiting: acquire permit before proceeding
        let _permit = self.acquire_permit("expand_volume").await?;

        let req = request.into_inner();
        info!(
            "ExpandVolume request: volume_id={}, new_size={}",
            req.volume_id, req.new_size_bytes
        );

        if req.volume_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("volume_id cannot be empty"));
        }
        if req.new_size_bytes <= 0 {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("new_size_bytes must be positive"));
        }

        // Verify volume exists
        let metadata = {
            let volumes = self.volumes.read().await;
            match volumes.get(&req.volume_id).cloned() {
                Some(m) => m,
                None => {
                    timer.failure("not_found");
                    return Err(Status::not_found(format!(
                        "volume '{}' not found",
                        req.volume_id
                    )));
                }
            }
        };

        // Resize ZFS volume
        {
            let zfs = self.zfs.read().await;
            if let Err(e) = zfs.resize_volume(&metadata.name, req.new_size_bytes as u64) {
                timer.failure("zfs_error");
                return Err(Status::internal(format!("failed to resize volume: {}", e)));
            }
        }

        info!(
            "Expanded volume {} to {} bytes",
            req.volume_id, req.new_size_bytes
        );

        timer.success();
        Ok(Response::new(ExpandVolumeResponse {
            size_bytes: req.new_size_bytes,
        }))
    }

    /// List all volumes
    #[instrument(skip(self, request))]
    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "ListVolumes request: max_entries={}, starting_token={}",
            req.max_entries, req.starting_token
        );

        // Get ZFS datasets
        let datasets = {
            let zfs = self.zfs.read().await;
            zfs.list_volumes()
                .map_err(|e| Status::internal(format!("failed to list volumes: {}", e)))?
        };

        // Build response with metadata
        let volumes_meta = self.volumes.read().await;
        let mut volumes = Vec::new();

        for dataset in &datasets {
            // Extract volume name from full dataset path
            let name = dataset.name.rsplit('/').next().unwrap_or(&dataset.name);

            if let Some(metadata) = volumes_meta.get(name) {
                volumes.push(self.dataset_to_volume(dataset, metadata));
            } else {
                // Volume exists in ZFS but not in our metadata (orphaned or created externally)
                debug!("Found ZFS volume without metadata: {}", name);
            }
        }

        let (paginated_volumes, next_token) =
            paginate(volumes, req.max_entries, &req.starting_token);

        Ok(Response::new(ListVolumesResponse {
            volumes: paginated_volumes,
            next_token,
        }))
    }

    /// Get a single volume by ID
    #[instrument(skip(self, request))]
    async fn get_volume(
        &self,
        request: Request<GetVolumeRequest>,
    ) -> Result<Response<GetVolumeResponse>, Status> {
        let req = request.into_inner();
        debug!("GetVolume request: volume_id={}", req.volume_id);

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id cannot be empty"));
        }

        // Get metadata
        let metadata = {
            let volumes = self.volumes.read().await;
            volumes
                .get(&req.volume_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("volume '{}' not found", req.volume_id)))?
        };

        // Get ZFS dataset info
        let dataset = {
            let zfs = self.zfs.read().await;
            zfs.get_dataset(&metadata.name)
                .map_err(|e| Status::internal(format!("failed to get volume info: {}", e)))?
        };

        let volume = self.dataset_to_volume(&dataset, &metadata);

        Ok(Response::new(GetVolumeResponse {
            volume: Some(volume),
        }))
    }

    /// Create a snapshot of a volume
    #[instrument(skip(self, request))]
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let timer = OperationTimer::new("create_snapshot");

        // Rate limiting: acquire permit before proceeding
        let _permit = self.acquire_permit("create_snapshot").await?;

        let req = request.into_inner();
        info!(
            "CreateSnapshot request: source_volume_id={}, name={}",
            req.source_volume_id, req.name
        );

        if req.source_volume_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("source_volume_id cannot be empty"));
        }
        if req.name.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("snapshot name cannot be empty"));
        }

        // Verify source volume exists
        let _metadata = {
            let volumes = self.volumes.read().await;
            match volumes.get(&req.source_volume_id).cloned() {
                Some(m) => m,
                None => {
                    timer.failure("not_found");
                    return Err(Status::not_found(format!(
                        "source volume '{}' not found",
                        req.source_volume_id
                    )));
                }
            }
        };

        // Create ZFS snapshot
        let snapshot_name = {
            let zfs = self.zfs.read().await;
            match zfs.create_snapshot(&req.source_volume_id, &req.name) {
                Ok(n) => n,
                Err(e) => {
                    timer.failure("zfs_error");
                    return Err(Status::internal(format!(
                        "failed to create snapshot: {}",
                        e
                    )));
                }
            }
        };

        // Create snapshot ID and timestamp
        let snapshot_id = format!("{}@{}", req.source_volume_id, req.name);
        let creation_time = unix_timestamp_now();

        // Store metadata
        let snap_metadata = SnapshotMetadata {
            id: snapshot_id.clone(),
            source_volume_id: req.source_volume_id.clone(),
            name: req.name.clone(),
            creation_time,
        };

        {
            let mut snapshots = self.snapshots.write().await;
            snapshots.insert(snapshot_id.clone(), snap_metadata);
        }

        let snapshot = Snapshot {
            id: snapshot_id,
            source_volume_id: req.source_volume_id,
            name: snapshot_name,
            creation_time,
            size_bytes: 0, // ZFS snapshots don't consume space until divergence
        };

        info!("Created snapshot: {}", snapshot.id);

        timer.success();
        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    /// Delete a snapshot
    #[instrument(skip(self, request))]
    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let timer = OperationTimer::new("delete_snapshot");

        // Rate limiting: acquire permit before proceeding
        let _permit = self.acquire_permit("delete_snapshot").await?;

        let req = request.into_inner();
        info!("DeleteSnapshot request: snapshot_id={}", req.snapshot_id);

        if req.snapshot_id.is_empty() {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument("snapshot_id cannot be empty"));
        }

        // Parse snapshot ID (format: volume_id@snap_name)
        let parts: Vec<&str> = req.snapshot_id.split('@').collect();
        if parts.len() != 2 {
            timer.failure("invalid_argument");
            return Err(Status::invalid_argument(
                "invalid snapshot_id format, expected 'volume_id@snap_name'",
            ));
        }

        let volume_name = parts[0];
        let snap_name = parts[1];

        // Delete ZFS snapshot using ZfsManager (validates input to prevent command injection)
        {
            let zfs = self.zfs.read().await;
            if let Err(e) = zfs.delete_snapshot(volume_name, snap_name) {
                use crate::zfs::ZfsError;
                let (status, error_type) = match e {
                    ZfsError::DatasetNotFound(_) => (
                        Status::not_found(format!("snapshot '{}' not found", req.snapshot_id)),
                        "not_found",
                    ),
                    ZfsError::InvalidName(msg) => {
                        (Status::invalid_argument(msg), "invalid_argument")
                    }
                    _ => (
                        Status::internal(format!("failed to delete snapshot: {}", e)),
                        "zfs_error",
                    ),
                };
                timer.failure(error_type);
                return Err(status);
            }
        }

        // Remove metadata
        {
            let mut snapshots = self.snapshots.write().await;
            snapshots.remove(&req.snapshot_id);
        }

        info!(
            "Deleted snapshot: {} (volume={}, snap={})",
            req.snapshot_id, volume_name, snap_name
        );
        timer.success();
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    /// List snapshots, optionally filtered by source volume
    #[instrument(skip(self, request))]
    async fn list_snapshots(
        &self,
        request: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "ListSnapshots request: source_volume_id={}, max_entries={}, starting_token={}",
            req.source_volume_id, req.max_entries, req.starting_token
        );

        let snapshots_meta = self.snapshots.read().await;

        // Filter by source volume if specified
        let filtered: Vec<&SnapshotMetadata> = if req.source_volume_id.is_empty() {
            snapshots_meta.values().collect()
        } else {
            snapshots_meta
                .values()
                .filter(|s| s.source_volume_id == req.source_volume_id)
                .collect()
        };

        // Convert to proto snapshots
        let snapshots: Vec<Snapshot> = filtered
            .iter()
            .map(|s| Snapshot {
                id: s.id.clone(),
                source_volume_id: s.source_volume_id.clone(),
                name: s.name.clone(),
                creation_time: s.creation_time,
                size_bytes: 0,
            })
            .collect();

        let (paginated, next_token) = paginate(snapshots, req.max_entries, &req.starting_token);

        Ok(Response::new(ListSnapshotsResponse {
            snapshots: paginated,
            next_token,
        }))
    }

    /// Get a single snapshot by ID
    #[instrument(skip(self, request))]
    async fn get_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let req = request.into_inner();
        debug!("GetSnapshot request: snapshot_id={}", req.snapshot_id);

        if req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument("snapshot_id cannot be empty"));
        }

        // Get metadata
        let metadata = {
            let snapshots = self.snapshots.read().await;
            snapshots.get(&req.snapshot_id).cloned().ok_or_else(|| {
                Status::not_found(format!("snapshot '{}' not found", req.snapshot_id))
            })?
        };

        let snapshot = Snapshot {
            id: metadata.id,
            source_volume_id: metadata.source_volume_id,
            name: metadata.name,
            creation_time: metadata.creation_time,
            size_bytes: 0,
        };

        Ok(Response::new(GetSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    /// Get storage capacity information for the ZFS pool
    #[instrument(skip(self, _request))]
    async fn get_capacity(
        &self,
        _request: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        debug!("GetCapacity request");

        // Get capacity from ZFS parent dataset
        let zfs = self.zfs.read().await;
        let capacity = zfs
            .get_capacity()
            .map_err(|e| Status::internal(format!("failed to get capacity: {}", e)))?;

        info!(
            available = capacity.available,
            used = capacity.used,
            total = capacity.available + capacity.used,
            "Retrieved storage capacity"
        );

        Ok(Response::new(GetCapacityResponse {
            available_capacity: capacity.available as i64,
            total_capacity: (capacity.available + capacity.used) as i64,
            used_capacity: capacity.used as i64,
        }))
    }
}
