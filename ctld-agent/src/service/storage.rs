//! gRPC Storage Agent service implementation.
//!
//! This module provides the gRPC service layer that ties together ZFS volume management
//! and iSCSI/NVMeoF export functionality.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument, warn};

use crate::ctl::{CtlManager, ExportType as CtlExportType};
use crate::zfs::{VolumeMetadata as ZfsVolumeMetadata, ZfsManager};

/// Generated protobuf types and service trait
pub mod proto {
    tonic::include_proto!("ctld_agent.v1");
}

use proto::storage_agent_server::StorageAgent;
use proto::{
    CreateSnapshotRequest, CreateSnapshotResponse, CreateVolumeRequest, CreateVolumeResponse,
    DeleteSnapshotRequest, DeleteSnapshotResponse, DeleteVolumeRequest, DeleteVolumeResponse,
    ExpandVolumeRequest, ExpandVolumeResponse, ExportType, GetSnapshotRequest, GetSnapshotResponse,
    GetVolumeRequest, GetVolumeResponse, ListSnapshotsRequest, ListSnapshotsResponse,
    ListVolumesRequest, ListVolumesResponse, Snapshot, Volume,
};

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
pub struct StorageService {
    /// ZFS volume manager
    zfs: Arc<RwLock<ZfsManager>>,
    /// Unified CTL manager (handles both iSCSI and NVMeoF)
    ctl: Arc<RwLock<CtlManager>>,
    /// Volume metadata tracking
    volumes: Arc<RwLock<HashMap<String, VolumeMetadata>>>,
    /// Snapshot metadata tracking
    snapshots: Arc<RwLock<HashMap<String, SnapshotMetadata>>>,
}

impl StorageService {
    /// Create a new StorageService
    pub fn new(zfs: Arc<RwLock<ZfsManager>>, ctl: Arc<RwLock<CtlManager>>) -> Self {
        Self {
            zfs,
            ctl,
            volumes: Arc::new(RwLock::new(HashMap::new())),
            snapshots: Arc::new(RwLock::new(HashMap::new())),
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
            let export_type = match zfs_meta.export_type.as_str() {
                "ISCSI" => ExportType::Iscsi,
                "NVMEOF" => ExportType::Nvmeof,
                _ => {
                    warn!(
                        "Unknown export type '{}' for volume '{}', skipping",
                        zfs_meta.export_type, vol_name
                    );
                    continue;
                }
            };

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

            let ctl_export_type = match metadata.export_type {
                ExportType::Iscsi => CtlExportType::Iscsi,
                ExportType::Nvmeof => CtlExportType::Nvmeof,
                ExportType::Unspecified => {
                    debug!("Volume '{}' has no export type, skipping reconciliation", vol_name);
                    continue;
                }
            };

            let ctl = self.ctl.read().await;
            match ctl.export_volume(vol_name, &device_path, ctl_export_type, metadata.lun_id as u32)
            {
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
                        warn!(
                            "Failed to reconcile export for '{}': {}",
                            vol_name, e
                        );
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
        let req = request.into_inner();
        info!(
            "CreateVolume request: name={}, size={}",
            req.name, req.size_bytes
        );

        if req.name.is_empty() {
            return Err(Status::invalid_argument("volume name cannot be empty"));
        }
        if req.size_bytes <= 0 {
            return Err(Status::invalid_argument("size_bytes must be positive"));
        }

        let export_type = ExportType::try_from(req.export_type).unwrap_or(ExportType::Unspecified);

        if export_type == ExportType::Unspecified {
            return Err(Status::invalid_argument(
                "export_type must be ISCSI or NVMEOF",
            ));
        }

        // Create ZFS volume
        let dataset = {
            let zfs = self.zfs.read().await;
            zfs.create_volume(&req.name, req.size_bytes as u64)
                .map_err(|e| Status::internal(format!("failed to create ZFS volume: {}", e)))?
        };

        // Get device path
        let device_path = {
            let zfs = self.zfs.read().await;
            zfs.get_device_path(&req.name)
        };

        // Default LUN ID
        let lun_id: i32 = 0;

        // Export the volume via unified CTL manager
        let ctl_export_type = match export_type {
            ExportType::Iscsi => CtlExportType::Iscsi,
            ExportType::Nvmeof => CtlExportType::Nvmeof,
            ExportType::Unspecified => unreachable!(),
        };

        let target_name = {
            let ctl = self.ctl.read().await;
            let export = ctl
                .export_volume(&req.name, &device_path, ctl_export_type, lun_id as u32)
                .map_err(|e| {
                    warn!("Failed to export volume: {}", e);
                    Status::internal(format!("failed to export volume: {}", e))
                })?;
            export.target_name
        };

        // Write UCL config and reload ctld
        {
            let ctl = self.ctl.read().await;
            if let Err(e) = ctl.write_config() {
                warn!("Failed to write CTL config: {}", e);
                // Continue anyway - export is in cache, config will persist on next operation
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
        let export_type_str = match export_type {
            ExportType::Iscsi => "ISCSI",
            ExportType::Nvmeof => "NVMEOF",
            ExportType::Unspecified => "UNSPECIFIED",
        };
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let zfs_metadata = ZfsVolumeMetadata {
            export_type: export_type_str.to_string(),
            target_name: target_name.clone(),
            lun_id: Some(lun_id as u32),
            namespace_id: None,
            parameters: req.parameters.clone(),
            created_at,
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

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume),
        }))
    }

    /// Delete a volume and unexport from iSCSI/NVMeoF
    #[instrument(skip(self, request))]
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteVolume request: volume_id={}", req.volume_id);

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id cannot be empty"));
        }

        // Get volume metadata
        let metadata = {
            let volumes = self.volumes.read().await;
            volumes
                .get(&req.volume_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("volume '{}' not found", req.volume_id)))?
        };

        // Unexport the volume via unified CTL manager
        if metadata.export_type != ExportType::Unspecified {
            let ctl = self.ctl.read().await;
            ctl.unexport_volume(&metadata.name).map_err(|e| {
                warn!("Failed to unexport volume: {}", e);
                Status::internal(format!("failed to unexport volume: {}", e))
            })?;

            // Write UCL config with updated (removed) export entries
            if let Err(e) = ctl.write_config() {
                warn!("Failed to write CTL config: {}", e);
                // Continue anyway - unexport is in cache, config will persist on next operation
            }
        } else {
            debug!("Volume has no export type, skipping unexport");
        }

        // Clear ZFS metadata before deleting (for consistency)
        {
            let zfs = self.zfs.read().await;
            if let Err(e) = zfs.clear_volume_metadata(&metadata.name) {
                warn!("Failed to clear volume metadata from ZFS: {}", e);
                // Continue anyway - we're deleting the volume
            }
        }

        // Delete ZFS volume
        {
            let zfs = self.zfs.read().await;
            zfs.delete_volume(&metadata.name)
                .map_err(|e| Status::internal(format!("failed to delete ZFS volume: {}", e)))?;
        }

        // Remove metadata
        {
            let mut volumes = self.volumes.write().await;
            volumes.remove(&req.volume_id);
        }

        info!("Deleted volume: {}", req.volume_id);
        Ok(Response::new(DeleteVolumeResponse {}))
    }

    /// Expand (resize) a volume
    #[instrument(skip(self, request))]
    async fn expand_volume(
        &self,
        request: Request<ExpandVolumeRequest>,
    ) -> Result<Response<ExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        info!(
            "ExpandVolume request: volume_id={}, new_size={}",
            req.volume_id, req.new_size_bytes
        );

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("volume_id cannot be empty"));
        }
        if req.new_size_bytes <= 0 {
            return Err(Status::invalid_argument("new_size_bytes must be positive"));
        }

        // Verify volume exists
        let metadata = {
            let volumes = self.volumes.read().await;
            volumes
                .get(&req.volume_id)
                .cloned()
                .ok_or_else(|| Status::not_found(format!("volume '{}' not found", req.volume_id)))?
        };

        // Resize ZFS volume
        {
            let zfs = self.zfs.read().await;
            zfs.resize_volume(&metadata.name, req.new_size_bytes as u64)
                .map_err(|e| Status::internal(format!("failed to resize volume: {}", e)))?;
        }

        info!(
            "Expanded volume {} to {} bytes",
            req.volume_id, req.new_size_bytes
        );

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

        // Handle pagination
        let max_entries = if req.max_entries > 0 {
            req.max_entries as usize
        } else {
            volumes.len()
        };

        let start_idx = if !req.starting_token.is_empty() {
            req.starting_token.parse::<usize>().unwrap_or(0)
        } else {
            0
        };

        let end_idx = std::cmp::min(start_idx + max_entries, volumes.len());
        let paginated_volumes: Vec<Volume> = volumes
            .into_iter()
            .skip(start_idx)
            .take(end_idx - start_idx)
            .collect();

        let next_token = if end_idx < volumes_meta.len() {
            end_idx.to_string()
        } else {
            String::new()
        };

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
        let req = request.into_inner();
        info!(
            "CreateSnapshot request: source_volume_id={}, name={}",
            req.source_volume_id, req.name
        );

        if req.source_volume_id.is_empty() {
            return Err(Status::invalid_argument("source_volume_id cannot be empty"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("snapshot name cannot be empty"));
        }

        // Verify source volume exists
        let _metadata = {
            let volumes = self.volumes.read().await;
            volumes.get(&req.source_volume_id).cloned().ok_or_else(|| {
                Status::not_found(format!(
                    "source volume '{}' not found",
                    req.source_volume_id
                ))
            })?
        };

        // Create ZFS snapshot
        let snapshot_name = {
            let zfs = self.zfs.read().await;
            zfs.create_snapshot(&req.source_volume_id, &req.name)
                .map_err(|e| Status::internal(format!("failed to create snapshot: {}", e)))?
        };

        // Get creation time
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        // Create snapshot ID
        let snapshot_id = format!("{}@{}", req.source_volume_id, req.name);

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
        let req = request.into_inner();
        info!("DeleteSnapshot request: snapshot_id={}", req.snapshot_id);

        if req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument("snapshot_id cannot be empty"));
        }

        // Parse snapshot ID (format: volume_id@snap_name)
        let parts: Vec<&str> = req.snapshot_id.split('@').collect();
        if parts.len() != 2 {
            return Err(Status::invalid_argument(
                "invalid snapshot_id format, expected 'volume_id@snap_name'",
            ));
        }

        let volume_name = parts[0];
        let snap_name = parts[1];

        // Delete ZFS snapshot using ZfsManager (validates input to prevent command injection)
        {
            let zfs = self.zfs.read().await;
            zfs.delete_snapshot(volume_name, snap_name).map_err(|e| {
                use crate::zfs::ZfsError;
                match e {
                    ZfsError::DatasetNotFound(_) => {
                        Status::not_found(format!("snapshot '{}' not found", req.snapshot_id))
                    }
                    ZfsError::InvalidName(msg) => Status::invalid_argument(msg),
                    _ => Status::internal(format!("failed to delete snapshot: {}", e)),
                }
            })?;
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

        // Handle pagination
        let max_entries = if req.max_entries > 0 {
            req.max_entries as usize
        } else {
            snapshots.len()
        };

        let start_idx = if !req.starting_token.is_empty() {
            req.starting_token.parse::<usize>().unwrap_or(0)
        } else {
            0
        };

        let end_idx = std::cmp::min(start_idx + max_entries, snapshots.len());
        let paginated: Vec<Snapshot> = snapshots
            .into_iter()
            .skip(start_idx)
            .take(end_idx - start_idx)
            .collect();

        let next_token = if end_idx < filtered.len() {
            end_idx.to_string()
        } else {
            String::new()
        };

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
}
