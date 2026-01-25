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
    AuthConfig, ConfigWriterHandle, CtlError, CtlManager, CtlOptions, ExportType as CtlExportType,
    IscsiChapAuth, NvmeAuth, spawn_config_writer,
};
use crate::metrics::{self, OperationTimer};
use crate::zfs::{VolumeMetadata as ZfsVolumeMetadata, ZfsManager};

/// Generated protobuf types and service trait
pub mod proto {
    tonic::include_proto!("ctld_agent.v1");
}

use proto::storage_agent_server::StorageAgent;
use proto::{
    AuthCredentials, CloneMode, CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest,
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

/// Parse CTL options from request parameters.
///
/// Supports the following StorageClass parameters:
/// - `blockSize` (or `block_size`): Logical block size (512 or 4096)
/// - `physicalBlockSize` (or `physical_block_size`, `pblocksize`): Physical block hint
/// - `enableUnmap` (or `enable_unmap`, `unmap`): Enable TRIM/discard ("true" or "false")
fn parse_ctl_options(params: &HashMap<String, String>) -> CtlOptions {
    let blocksize = params
        .get("blockSize")
        .or_else(|| params.get("block_size"))
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|&bs| bs == 512 || bs == 4096);

    let pblocksize = params
        .get("physicalBlockSize")
        .or_else(|| params.get("physical_block_size"))
        .or_else(|| params.get("pblocksize"))
        .and_then(|v| v.parse::<u32>().ok());

    let unmap = params
        .get("enableUnmap")
        .or_else(|| params.get("enable_unmap"))
        .or_else(|| params.get("unmap"))
        .and_then(|v| match v.to_lowercase().as_str() {
            "true" | "1" | "on" | "yes" => Some(true),
            "false" | "0" | "off" | "no" => Some(false),
            _ => None,
        });

    CtlOptions {
        blocksize,
        pblocksize,
        unmap,
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
fn paginate<T>(items: Vec<T>, max_entries: i32, starting_token: &str) -> Result<(Vec<T>, String), Status> {
    let max_entries = if max_entries > 0 {
        max_entries as usize
    } else {
        items.len()
    };

    let start_idx = if !starting_token.is_empty() {
        starting_token
            .parse::<usize>()
            .map_err(|_| Status::invalid_argument("Invalid starting_token"))?
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

    Ok((paginated, next_token))
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
    /// Authentication configuration
    auth: AuthConfig,
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
    /// Handle to the serialized config writer task
    config_writer: ConfigWriterHandle,
    /// Volume metadata tracking
    volumes: Arc<RwLock<HashMap<String, VolumeMetadata>>>,
    // Note: Snapshot metadata is stored in ZFS properties and queried directly.
    // No in-memory cache needed - ZFS is the single source of truth.
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
        // Spawn the serialized config writer task.
        // This ensures all config writes are serialized with debouncing,
        // preventing race conditions during parallel volume operations.
        let config_writer = spawn_config_writer(ctl.clone(), None);

        Self {
            zfs,
            ctl,
            config_writer,
            volumes: Arc::new(RwLock::new(HashMap::new())),
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

            // Reconstruct auth config from ZFS metadata.
            // We only store the auth-group NAME in ZFS, not credentials.
            // Credentials are in /etc/ctl.conf and persisted by ctld.
            let auth = if let Some(ref auth_group) = zfs_meta.auth_group {
                AuthConfig::GroupRef(auth_group.clone())
            } else {
                AuthConfig::None
            };

            let metadata = VolumeMetadata {
                id: vol_name.clone(),
                name: vol_name.clone(),
                export_type,
                target_name: zfs_meta.target_name.clone(),
                lun_id: zfs_meta.lun_id.unwrap_or(0).try_into().map_err(|_| {
                    format!(
                        "LUN ID {} for volume '{}' exceeds i32::MAX",
                        zfs_meta.lun_id.unwrap_or(0),
                        vol_name
                    )
                })?,
                parameters: zfs_meta.parameters.clone(),
                auth,
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

            // Validate lun_id can be safely converted to u32
            let lun_id: u32 = match metadata.lun_id.try_into() {
                Ok(id) => id,
                Err(_) => {
                    warn!(
                        "Volume '{}' has invalid LUN ID {}, skipping reconciliation",
                        vol_name, metadata.lun_id
                    );
                    continue;
                }
            };

            let ctl = self.ctl.read().await;
            // Auth-group NAME is stored in ZFS metadata; credentials are in ctl.conf.
            // GroupRef tells write_config() to reference the existing auth-group
            // without creating a new one (credentials already persisted in ctl.conf).
            // CTL options are not persisted in ZFS metadata, so use defaults on reconciliation.
            match ctl.export_volume(
                vol_name,
                &device_path,
                ctl_export_type,
                lun_id,
                metadata.auth.clone(),
                CtlOptions::default(),
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
        if reconciled_count > 0
            && let Err(e) = self.config_writer.write_config().await
        {
            warn!("Failed to write CTL config after reconciliation: {}", e);
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
        // Use volsize for zvols (the actual volume capacity)
        // For zvols, volsize should always be present - if not, log warning and use 0
        // Do NOT fall back to 'referenced' as it's semantically different (allocated space, not capacity)
        let size_bytes = match dataset.volsize {
            Some(volsize) => volsize as i64,
            None => {
                warn!(
                    dataset = %dataset.name,
                    referenced = dataset.referenced,
                    "zvol missing volsize property - reporting 0 capacity"
                );
                0
            }
        };
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

    /// Helper to create a volume from a snapshot (used by both snapshot restore and volume clone).
    ///
    /// Returns Ok(Dataset) on success, or Err(Status) on failure.
    /// Caller is responsible for calling timer.failure() on error.
    /// Metadata is set atomically during creation to ensure crash safety.
    async fn create_volume_from_snapshot(
        &self,
        target_name: &str,
        source_volume: &str,
        snap_name: &str,
        clone_mode: CloneMode,
        metadata: &crate::zfs::VolumeMetadata,
    ) -> Result<crate::zfs::Dataset, Status> {
        let zfs = self.zfs.read().await;

        match clone_mode {
            CloneMode::Copy => {
                // Full independent copy via zfs send/recv (slow but no dependencies)
                info!(
                    target = %target_name,
                    source = %source_volume,
                    snapshot = %snap_name,
                    "Creating volume using COPY mode (zfs send/recv)"
                );
                zfs.copy_from_snapshot(source_volume, snap_name, target_name, metadata)
                    .map_err(|e| {
                        Status::internal(format!("failed to copy volume from snapshot: {}", e))
                    })
            }
            CloneMode::Linked | CloneMode::Unspecified => {
                // Fast clone (instant but creates dependency on snapshot)
                info!(
                    target = %target_name,
                    source = %source_volume,
                    snapshot = %snap_name,
                    "Creating volume using LINKED mode (zfs clone)"
                );
                zfs.clone_from_snapshot(source_volume, snap_name, target_name, metadata)
                    .map_err(|e| {
                        Status::internal(format!("failed to clone volume from snapshot: {}", e))
                    })
            }
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

        // Compute export parameters before volume creation so we can set metadata atomically
        // Default LUN/Namespace ID
        // Note: iSCSI LUN IDs can start at 0, but NVMeoF namespace IDs must start at 1
        // (NSID 0 is reserved per NVMe spec)
        let lun_id: u32 = match export_type {
            ExportType::Iscsi => 0,
            ExportType::Nvmeof => 1,
            _ => 0,
        };

        // Generate target name (IQN/NQN) before volume creation
        let ctl_export_type = to_ctl_export_type(export_type).expect("already validated");
        let target_name = {
            let ctl = self.ctl.read().await;
            match ctl_export_type {
                crate::ctl::ExportType::Iscsi => ctl
                    .generate_iqn(&req.name)
                    .map(|iqn| iqn.to_string())
                    .map_err(|e| Status::internal(format!("failed to generate IQN: {}", e)))?,
                crate::ctl::ExportType::Nvmeof => ctl
                    .generate_nqn(&req.name)
                    .map(|nqn| nqn.to_string())
                    .map_err(|e| Status::internal(format!("failed to generate NQN: {}", e)))?,
            }
        };

        // Extract auth config for CTL export (credentials used in ctl.conf)
        let auth_config = proto_to_ctl_auth(req.auth.as_ref());

        // Compute auth-group name for ZFS metadata (credentials NOT stored in ZFS)
        let auth_group_name = if auth_config.is_some() {
            Some(auth_config.auth_group_name(&req.name))
        } else {
            None
        };

        // Build ZFS metadata to set atomically during volume creation
        // SECURITY: Only the auth-group NAME is stored, not credentials.
        // Credentials are persisted in /etc/ctl.conf (root-only).
        let zfs_metadata = ZfsVolumeMetadata {
            export_type: ctl_export_type,
            target_name: target_name.clone(),
            lun_id: Some(lun_id),
            namespace_id: None,
            parameters: req.parameters.clone(),
            created_at: unix_timestamp_now(),
            auth_group: auth_group_name,
        };

        // Create ZFS volume - either fresh or from content source (snapshot/volume)
        let dataset = if let Some(ref content_source) = req.content_source {
            use proto::volume_content_source::Source;

            // Determine clone mode
            let clone_mode =
                CloneMode::try_from(content_source.clone_mode).unwrap_or(CloneMode::Unspecified);

            match &content_source.source {
                Some(Source::SnapshotId(snapshot_id)) => {
                    // Volume creation from existing snapshot
                    if snapshot_id.is_empty() {
                        timer.failure("invalid_argument");
                        return Err(Status::invalid_argument(
                            "content_source.snapshot_id cannot be empty",
                        ));
                    }

                    // Parse snapshot ID (format: volume_id@snap_name)
                    let parts: Vec<&str> = snapshot_id.split('@').collect();
                    if parts.len() != 2 {
                        timer.failure("invalid_argument");
                        return Err(Status::invalid_argument(format!(
                            "invalid snapshot_id format '{}', expected 'volume_id@snap_name'",
                            snapshot_id
                        )));
                    }

                    let source_volume = parts[0];
                    let snap_name = parts[1];

                    match self
                        .create_volume_from_snapshot(
                            &req.name,
                            source_volume,
                            snap_name,
                            clone_mode,
                            &zfs_metadata,
                        )
                        .await
                    {
                        Ok(d) => d,
                        Err(e) => {
                            timer.failure("zfs_error");
                            return Err(e);
                        }
                    }
                }
                Some(Source::SourceVolumeId(source_volume_id)) => {
                    // Volume creation from existing volume (PVC cloning)
                    // We create a temporary snapshot, then clone from it.
                    //
                    // Cleanup behavior:
                    // - COPY mode: temp snapshot deleted after send/recv completes
                    // - LINKED mode: temp snapshot preserved (clone depends on it)
                    //   When the clone is later deleted, our auto-promote logic
                    //   transfers snapshot ownership if needed.
                    if source_volume_id.is_empty() {
                        timer.failure("invalid_argument");
                        return Err(Status::invalid_argument(
                            "content_source.source_volume_id cannot be empty",
                        ));
                    }

                    // Generate unique snapshot name using target volume name + timestamp
                    // Using timestamp avoids collision if same target name is retried
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis())
                        .unwrap_or(0);
                    let temp_snap_name = format!("pvc-clone-{}-{}", &req.name, timestamp);

                    info!(
                        source_volume = %source_volume_id,
                        temp_snapshot = %temp_snap_name,
                        target_volume = %req.name,
                        clone_mode = ?clone_mode,
                        "Cloning volume from existing PVC"
                    );

                    // Create temporary snapshot of source volume
                    {
                        let zfs = self.zfs.read().await;
                        if let Err(e) = zfs.create_snapshot(source_volume_id, &temp_snap_name) {
                            timer.failure("zfs_error");
                            return Err(Status::internal(format!(
                                "failed to create temporary snapshot for volume clone: {}",
                                e
                            )));
                        }
                    }

                    // Clone from the temporary snapshot
                    let result = self
                        .create_volume_from_snapshot(
                            &req.name,
                            source_volume_id,
                            &temp_snap_name,
                            clone_mode,
                            &zfs_metadata,
                        )
                        .await;

                    // Handle cleanup based on result and clone mode
                    match (&result, clone_mode) {
                        (Ok(_), CloneMode::Copy) => {
                            // Success with COPY mode - clean up temp snapshot
                            let zfs = self.zfs.read().await;
                            if let Err(e) = zfs.delete_snapshot(source_volume_id, &temp_snap_name) {
                                warn!(
                                    source_volume = %source_volume_id,
                                    snapshot = %temp_snap_name,
                                    error = %e,
                                    "Failed to clean up temporary snapshot after copy"
                                );
                            }
                        }
                        (Ok(_), _) => {
                            // Success with LINKED mode - keep temp snapshot
                            info!(
                                source_volume = %source_volume_id,
                                snapshot = %temp_snap_name,
                                "Temporary snapshot preserved (LINKED mode clone depends on it)"
                            );
                        }
                        (Err(_), _) => {
                            // Failed - always clean up temp snapshot
                            let zfs = self.zfs.read().await;
                            if let Err(e) = zfs.delete_snapshot(source_volume_id, &temp_snap_name) {
                                warn!(
                                    source_volume = %source_volume_id,
                                    snapshot = %temp_snap_name,
                                    error = %e,
                                    "Failed to clean up temporary snapshot after failed clone"
                                );
                            }
                        }
                    }

                    match result {
                        Ok(d) => d,
                        Err(e) => {
                            timer.failure("zfs_error");
                            return Err(e);
                        }
                    }
                }
                None => {
                    timer.failure("invalid_argument");
                    return Err(Status::invalid_argument(
                        "content_source must specify either snapshot_id or source_volume_id",
                    ));
                }
            }
        } else {
            // Fresh volume creation with metadata set atomically
            let zfs = self.zfs.read().await;
            match zfs.create_volume(&req.name, req.size_bytes as u64, &zfs_metadata) {
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

        // auth_config was extracted earlier for ZFS metadata persistence
        let has_auth = auth_config.is_some();

        // Parse CTL options from request parameters
        let ctl_options = parse_ctl_options(&req.parameters);

        // Export the volume via unified CTL manager
        {
            let ctl = self.ctl.read().await;
            if let Err(e) = ctl.export_volume(
                &req.name,
                &device_path,
                ctl_export_type,
                lun_id,
                auth_config.clone(),
                ctl_options,
            ) {
                warn!("Failed to export volume: {}", e);
                timer.failure("export_error");
                return Err(Status::internal(format!("failed to export volume: {}", e)));
            }
        }

        if has_auth {
            info!("Exported volume {} with authentication enabled", req.name);
        }

        // Write UCL config and reload ctld
        // CRITICAL: If this fails, ctld won't know about the export and
        // initiators won't be able to connect. We must return error.
        if let Err(e) = self.config_writer.write_config().await {
            error!("Failed to write CTL config: {}", e);
            timer.failure("config_write_error");
            return Err(Status::internal(format!(
                "Volume created but CTL config write failed: {}. Target may be inaccessible.",
                e
            )));
        }

        // Store in-memory metadata (ZFS metadata was set atomically during creation)
        let metadata = VolumeMetadata {
            id: req.name.clone(),
            name: req.name.clone(),
            export_type,
            target_name: target_name.clone(),
            lun_id: lun_id
                .try_into()
                .map_err(|_| Status::internal(format!("LUN ID {} exceeds i32::MAX", lun_id)))?,
            parameters: req.parameters.clone(),
            auth: auth_config,
        };

        {
            let mut volumes = self.volumes.write().await;
            volumes.insert(req.name.clone(), metadata.clone());
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

        // Determine volume name early (needed for snapshot check)
        let volume_name = metadata
            .as_ref()
            .map(|m| m.name.clone())
            .unwrap_or_else(|| req.volume_id.clone());

        // Handle clone dependencies: auto-promote clones to allow source deletion.
        // When volume A has snapshot A@snap with clone B, we must promote B first
        // so that A can be deleted. After promotion, A@snap becomes B@snap and
        // A becomes deletable (or becomes a clone of B@snap, which we then delete).
        {
            let zfs = self.zfs.read().await;
            match zfs.list_clones_for_volume(&volume_name) {
                Ok(clones) if !clones.is_empty() => {
                    info!(
                        volume = %volume_name,
                        clone_count = clones.len(),
                        "Volume has clones, promoting them to allow deletion"
                    );

                    // Promote each clone to reverse the dependency
                    for (snap_name, clone_full_path) in &clones {
                        // Extract clone name from full path (e.g., "tank/csi/clone1" -> "clone1")
                        let clone_name = clone_full_path
                            .rsplit('/')
                            .next()
                            .unwrap_or(clone_full_path);

                        info!(
                            volume = %volume_name,
                            snapshot = %snap_name,
                            clone = %clone_name,
                            "Promoting clone to transfer snapshot ownership"
                        );

                        if let Err(e) = zfs.promote_clone(clone_name) {
                            warn!(
                                clone = %clone_name,
                                error = %e,
                                "Failed to promote clone (may already be promoted or not under our parent)"
                            );
                            // Continue anyway - the clone might be outside our managed dataset
                        }
                    }
                }
                Ok(_) => {
                    debug!(volume = %volume_name, "No clones to promote");
                }
                Err(e) => {
                    debug!(
                        volume = %volume_name,
                        error = %e,
                        "Could not check for clones (volume may not exist)"
                    );
                }
            }
        }

        // CSI Spec compliance: Check for dependent snapshots before deletion
        // Per CSI spec, if volume has snapshots and we don't treat them as independent,
        // we must return FAILED_PRECONDITION so the user can delete snapshots first.
        // Note: After promoting clones above, the original snapshots may have moved
        // to the promoted clone, so this check is for remaining snapshots only.
        {
            let zfs = self.zfs.read().await;
            match zfs.list_snapshots_for_volume(&volume_name) {
                Ok(snapshots) if !snapshots.is_empty() => {
                    let snapshot_list = snapshots.join(", ");
                    warn!(
                        volume = %volume_name,
                        snapshot_count = snapshots.len(),
                        snapshots = %snapshot_list,
                        "Cannot delete volume with dependent snapshots"
                    );
                    timer.failure("has_snapshots");
                    return Err(Status::failed_precondition(format!(
                        "Volume '{}' has {} dependent snapshot(s): [{}]. \
                         Delete all VolumeSnapshots referencing this volume before deletion. \
                         If these are external snapshots (not CSI-managed), remove them manually with: \
                         zfs destroy {}@<snapshot_name>",
                        volume_name,
                        snapshots.len(),
                        snapshot_list,
                        volume_name
                    )));
                }
                Ok(_) => {
                    // No snapshots, proceed with deletion
                    debug!(volume = %volume_name, "No dependent snapshots, proceeding with deletion");
                }
                Err(e) => {
                    // Failed to check snapshots - log but continue
                    // This maintains idempotency for volumes that don't exist
                    debug!(
                        volume = %volume_name,
                        error = %e,
                        "Could not check for snapshots (volume may not exist)"
                    );
                }
            }
        }

        // Check if this volume is a clone (has an origin snapshot)
        // We need this info BEFORE deletion to clean up temp snapshots afterward
        let origin_info: Option<String> = {
            let zfs = self.zfs.read().await;
            match zfs.get_origin(&volume_name) {
                Ok(origin) => origin,
                Err(e) => {
                    debug!(
                        volume = %volume_name,
                        error = %e,
                        "Could not get origin (volume may not exist)"
                    );
                    None
                }
            }
        };

        // Try to unexport the volume via unified CTL manager
        // This is idempotent - if already unexported, we continue
        // Track whether we need to write config
        let needs_config_write = {
            let ctl = self.ctl.read().await;
            match ctl.unexport_volume(&req.volume_id) {
                Ok(()) => true,
                Err(CtlError::TargetNotFound(_)) => {
                    // Already unexported - this is fine (idempotent per CSI spec)
                    debug!("Volume {} already unexported (idempotent)", req.volume_id);
                    false
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
        };

        // Write UCL config with updated (removed) export entries
        // CRITICAL: If this fails, export will reappear on ctld restart
        // pointing to a deleted zvol, causing errors for initiators.
        if needs_config_write && let Err(e) = self.config_writer.write_config().await {
            error!("Failed to write CTL config after unexport: {}", e);
            timer.failure("config_write_error");
            return Err(Status::internal(format!(
                "Unexport succeeded but CTL config write failed: {}. Export may reappear on restart.",
                e
            )));
        }

        // Clear ZFS metadata before deleting (for consistency)
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

        // Clean up origin snapshot if this was a clone from PVC-to-PVC cloning
        // We only clean up snapshots with our "pvc-clone-" prefix to avoid
        // accidentally deleting user-created snapshots
        if let Some(origin) = origin_info
            && let Some(snap_name) = origin.rsplit('@').next()
            && snap_name.starts_with("pvc-clone-")
            && let Some(source_path) = origin.rsplit_once('@').map(|(p, _)| p)
        {
            // Origin format: "pool/dataset/volume@snapshot_name"
            // Extract just the volume name from the full path
            // SAFETY: Validate the path contains '/' before extracting - if not, skip cleanup
            let Some(source_volume) = source_path.rsplit('/').next() else {
                warn!(
                    origin = %origin,
                    source_path = %source_path,
                    "Unexpected origin path format (no '/'), skipping snapshot cleanup"
                );
                // Don't continue with cleanup - path parsing failed
                return Ok(Response::new(DeleteVolumeResponse {}));
            };

            // Additional validation: volume name should not be empty or contain unexpected chars
            if source_volume.is_empty() || source_volume.contains('@') {
                warn!(
                    origin = %origin,
                    source_volume = %source_volume,
                    "Invalid source volume name extracted, skipping snapshot cleanup"
                );
                return Ok(Response::new(DeleteVolumeResponse {}));
            }

            info!(
                deleted_clone = %volume_name,
                origin_snapshot = %origin,
                source_volume = %source_volume,
                "Attempting to clean up temp snapshot from PVC cloning"
            );

            let zfs = self.zfs.read().await;
            // Check if snapshot still has other clones
            match zfs.list_clones_for_volume(source_volume) {
                Ok(clones) => {
                    // Filter to clones of this specific snapshot
                    let snap_clones: Vec<_> =
                        clones.iter().filter(|(sn, _)| sn == snap_name).collect();

                    if snap_clones.is_empty() {
                        // No more clones, safe to delete the temp snapshot
                        if let Err(e) = zfs.delete_snapshot(source_volume, snap_name) {
                            warn!(
                                snapshot = %snap_name,
                                source_volume = %source_volume,
                                error = %e,
                                "Failed to clean up temp snapshot (may already be deleted)"
                            );
                        } else {
                            info!(
                                snapshot = %snap_name,
                                source_volume = %source_volume,
                                "Cleaned up temp snapshot from PVC cloning"
                            );
                        }
                    } else {
                        debug!(
                            snapshot = %snap_name,
                            remaining_clones = snap_clones.len(),
                            "Temp snapshot still has clones, not deleting"
                        );
                    }
                }
                Err(e) => {
                    debug!(
                        source_volume = %source_volume,
                        error = %e,
                        "Could not check clones for cleanup"
                    );
                }
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
            paginate(volumes, req.max_entries, &req.starting_token)?;

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

        // Note: Snapshot metadata is stored in ZFS properties by create_snapshot().
        // ListSnapshots and GetSnapshot query ZFS directly, so no in-memory cache needed.

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

        // Delete ZFS snapshot
        // First try the direct path (fast path for common case)
        // If not found, search by CSI snapshot ID property (handles promoted clones)
        {
            let zfs = self.zfs.read().await;

            // Try direct deletion first
            match zfs.delete_snapshot(volume_name, snap_name) {
                Ok(()) => {
                    info!(
                        snapshot_id = %req.snapshot_id,
                        "Deleted snapshot via direct path"
                    );
                }
                Err(crate::zfs::ZfsError::DatasetNotFound(_)) => {
                    // Snapshot not found at expected path
                    // This can happen if the source volume was deleted and a clone was promoted,
                    // which moves the snapshot to the promoted clone's dataset.
                    // Search for the snapshot by its CSI snapshot ID property.
                    info!(
                        snapshot_id = %req.snapshot_id,
                        "Snapshot not at expected path, searching by CSI ID property"
                    );

                    use crate::zfs::FindSnapshotResult;
                    match zfs.find_snapshot_by_id(&req.snapshot_id) {
                        Ok(FindSnapshotResult::NotFound) => {
                            // Snapshot truly doesn't exist - treat as already deleted (idempotent)
                            info!(
                                snapshot_id = %req.snapshot_id,
                                "Snapshot not found anywhere, treating as already deleted"
                            );
                        }
                        Ok(FindSnapshotResult::Found(path)) => {
                            // Found the snapshot at a different location (promoted clone)
                            info!(
                                snapshot_id = %req.snapshot_id,
                                path = %path,
                                "Found migrated snapshot, deleting"
                            );
                            if let Err(e) = zfs.delete_snapshot_by_path(&path) {
                                timer.failure("zfs_error");
                                return Err(Status::internal(format!(
                                    "failed to delete migrated snapshot at {}: {}",
                                    path, e
                                )));
                            }
                        }
                        Ok(FindSnapshotResult::Ambiguous(count)) => {
                            // Multiple snapshots with same ID - this should never happen
                            // Refuse to delete to avoid data loss
                            timer.failure("ambiguous_snapshot");
                            return Err(Status::failed_precondition(format!(
                                "Found {} snapshots with ID '{}' - refusing to delete. \
                                 This indicates a bug or data corruption.",
                                count, req.snapshot_id
                            )));
                        }
                        Err(e) => {
                            timer.failure("zfs_error");
                            return Err(Status::internal(format!(
                                "failed to search for snapshot: {}",
                                e
                            )));
                        }
                    }
                }
                Err(crate::zfs::ZfsError::InvalidName(msg)) => {
                    timer.failure("invalid_argument");
                    return Err(Status::invalid_argument(msg));
                }
                Err(e) => {
                    timer.failure("zfs_error");
                    return Err(Status::internal(format!(
                        "failed to delete snapshot: {}",
                        e
                    )));
                }
            }
        }

        // Note: No in-memory cache to update - ZFS is the source of truth.

        info!(
            "Deleted snapshot: {} (volume={}, snap={})",
            req.snapshot_id, volume_name, snap_name
        );
        timer.success();
        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    /// List snapshots, optionally filtered by source volume
    ///
    /// This queries ZFS directly for snapshots with the CSI metadata property,
    /// ensuring the list survives restarts and always reflects the actual ZFS state.
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

        // Query ZFS directly for all CSI snapshots
        let csi_snapshots = {
            let zfs = self.zfs.read().await;
            zfs.list_csi_snapshots().map_err(|e| {
                Status::internal(format!("failed to list snapshots from ZFS: {}", e))
            })?
        };

        // Filter by source volume if specified
        let filtered: Vec<_> = if req.source_volume_id.is_empty() {
            csi_snapshots
        } else {
            csi_snapshots
                .into_iter()
                .filter(|s| s.source_volume_id == req.source_volume_id)
                .collect()
        };

        // Convert to proto snapshots
        let snapshots: Vec<Snapshot> = filtered
            .iter()
            .map(|s| Snapshot {
                id: s.snapshot_id.clone(),
                source_volume_id: s.source_volume_id.clone(),
                name: s.name.clone(),
                creation_time: s.creation_time,
                size_bytes: 0, // ZFS snapshots don't consume space until divergence
            })
            .collect();

        let (paginated, next_token) = paginate(snapshots, req.max_entries, &req.starting_token)?;

        Ok(Response::new(ListSnapshotsResponse {
            snapshots: paginated,
            next_token,
        }))
    }

    /// Get a single snapshot by ID
    ///
    /// This queries ZFS directly for the snapshot, ensuring accurate results
    /// that survive restarts.
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

        // Query ZFS directly for all CSI snapshots and find the matching one
        let csi_snapshots = {
            let zfs = self.zfs.read().await;
            zfs.list_csi_snapshots().map_err(|e| {
                Status::internal(format!("failed to query snapshots from ZFS: {}", e))
            })?
        };

        let snapshot_info = csi_snapshots
            .into_iter()
            .find(|s| s.snapshot_id == req.snapshot_id)
            .ok_or_else(|| {
                Status::not_found(format!("snapshot '{}' not found", req.snapshot_id))
            })?;

        let snapshot = Snapshot {
            id: snapshot_info.snapshot_id,
            source_volume_id: snapshot_info.source_volume_id,
            name: snapshot_info.name,
            creation_time: snapshot_info.creation_time,
            size_bytes: 0, // ZFS snapshots don't consume space until divergence
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paginate_empty_token() {
        let items = vec![1, 2, 3, 4, 5];
        let (result, next_token) = paginate(items, 2, "").unwrap();
        assert_eq!(result, vec![1, 2]);
        assert_eq!(next_token, "2");
    }

    #[test]
    fn test_paginate_valid_token() {
        let items = vec![1, 2, 3, 4, 5];
        let (result, next_token) = paginate(items, 2, "2").unwrap();
        assert_eq!(result, vec![3, 4]);
        assert_eq!(next_token, "4");
    }

    #[test]
    fn test_paginate_invalid_token_returns_error() {
        let items = vec![1, 2, 3, 4, 5];
        let result = paginate(items, 2, "invalid");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Invalid starting_token"));
    }

    #[test]
    fn test_paginate_last_page() {
        let items = vec![1, 2, 3, 4, 5];
        let (result, next_token) = paginate(items, 2, "4").unwrap();
        assert_eq!(result, vec![5]);
        assert!(next_token.is_empty()); // No more pages
    }

    #[test]
    fn test_paginate_zero_max_entries_returns_all() {
        let items = vec![1, 2, 3];
        let (result, next_token) = paginate(items, 0, "").unwrap();
        assert_eq!(result, vec![1, 2, 3]);
        assert!(next_token.is_empty());
    }
}
