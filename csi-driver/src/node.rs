//! CSI Node Service Implementation
//!
//! Handles volume staging and publishing on worker nodes using
//! iSCSI/NVMeoF connections and bind mounts.
//!
//! Platform-specific operations (iSCSI, NVMe, filesystem, bind mounts)
//! are delegated to the `platform` module which provides compile-time
//! platform selection for FreeBSD and Linux.
//!
//! ## Target Naming Convention
//!
//! Target names are derived from volume IDs using a consistent pattern:
//! - iSCSI: `iqn.2024-01.org.freebsd.csi:<volume_id>`
//! - NVMeoF: `nqn.2024-01.org.freebsd.csi:<volume_id>`
//!
//! This allows NodeUnstageVolume to determine the target to disconnect
//! without requiring local metadata storage.

use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;

// Note: fs module used for symlink operations and directory creation only,
// no local metadata storage - device paths are queried from active sessions.

use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::csi;
use crate::platform::{Platform, StorageOps};

/// Base IQN prefix for iSCSI targets (must match ctld-agent configuration)
const BASE_IQN: &str = "iqn.2024-01.org.freebsd.csi";

/// Base NQN prefix for NVMeoF targets (must match ctld-agent configuration)
const BASE_NQN: &str = "nqn.2024-01.org.freebsd.csi";

/// CSI Node Service
///
/// Implements the CSI Node service which handles:
/// - Volume staging (connect to iSCSI/NVMeoF target, format if needed, mount to staging path)
/// - Volume unstaging (unmount from staging path)
/// - Volume publishing (bind mount from staging to target path)
/// - Volume unpublishing (unmount from target path)
/// - Node capability reporting
pub struct NodeService {
    /// The node identifier for this CSI node
    node_id: String,
}

impl NodeService {
    /// Create a new NodeService with the specified node ID.
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    /// Validate that a path is safe to use in shell commands.
    /// Returns an error if the path contains dangerous characters.
    fn validate_path(path: &str) -> Result<(), Status> {
        if path.is_empty() {
            return Err(Status::invalid_argument("Path cannot be empty"));
        }

        // Check for absolute path
        if !path.starts_with('/') {
            return Err(Status::invalid_argument("Path must be absolute"));
        }

        // Disallow dangerous characters that could enable shell injection
        let dangerous_chars = [
            ';', '|', '&', '$', '`', '(', ')', '{', '}', '<', '>', '\n', '\r',
        ];
        for c in dangerous_chars {
            if path.contains(c) {
                return Err(Status::invalid_argument(format!(
                    "Path contains dangerous character: '{}'",
                    c
                )));
            }
        }

        // Disallow path traversal
        if path.contains("..") {
            return Err(Status::invalid_argument(
                "Path cannot contain '..' (path traversal)",
            ));
        }

        Ok(())
    }

    /// Validate that a target name (IQN or NQN) is safe to use.
    fn validate_target_name(target: &str) -> Result<(), Status> {
        if target.is_empty() {
            return Err(Status::invalid_argument("Target name cannot be empty"));
        }

        // Target names should only contain alphanumeric, dots, colons, and dashes
        let valid = target
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == ':' || c == '-' || c == '_');

        if !valid {
            return Err(Status::invalid_argument(
                "Target name contains invalid characters",
            ));
        }

        Ok(())
    }

    /// Get the current capacity of a mounted volume.
    fn get_volume_capacity(path: &str) -> Result<i64, Status> {
        Self::validate_path(path)?;

        let output = std::process::Command::new("df")
            .args(["-k", path])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute df");
                Status::internal(format!("Failed to get volume capacity: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse df output (second line, second column is total size in KB)
        if let Some(line) = stdout.lines().nth(1)
            && let Some(size_kb) = line.split_whitespace().nth(1)
            && let Ok(size) = size_kb.parse::<i64>()
        {
            return Ok(size * 1024); // Convert KB to bytes
        }

        Err(Status::internal("Could not parse volume capacity"))
    }

    /// Detect the filesystem type of a mounted path.
    fn detect_filesystem_type(path: &str) -> Result<String, Status> {
        Self::validate_path(path)?;

        // Use df -T on Linux, or mount on FreeBSD to get filesystem type
        #[cfg(target_os = "linux")]
        {
            let output = std::process::Command::new("df")
                .args(["-T", path])
                .output()
                .map_err(|e| {
                    error!(error = %e, "Failed to execute df -T");
                    Status::internal(format!("Failed to detect filesystem type: {}", e))
                })?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            // df -T output: Filesystem Type ... (second column is type)
            if let Some(line) = stdout.lines().nth(1)
                && let Some(fs_type) = line.split_whitespace().nth(1)
            {
                return Ok(fs_type.to_string());
            }
        }

        #[cfg(target_os = "freebsd")]
        {
            let output = std::process::Command::new("mount").output().map_err(|e| {
                error!(error = %e, "Failed to execute mount");
                Status::internal(format!("Failed to detect filesystem type: {}", e))
            })?;

            let stdout = String::from_utf8_lossy(&output.stdout);
            // mount output: /dev/xxx on /path (fstype, options)
            for line in stdout.lines() {
                if line.contains(&format!(" on {} ", path))
                    || line.contains(&format!(" on {} (", path))
                {
                    // Extract filesystem type from parentheses
                    if let Some(start) = line.rfind('(') {
                        let rest = &line[start + 1..];
                        if let Some(fs_type) = rest.split(',').next() {
                            return Ok(fs_type.trim().to_string());
                        }
                    }
                }
            }
        }

        // Default to unknown
        Ok("unknown".to_string())
    }

    /// Resize the filesystem to use all available space on the device.
    /// Returns true if resize was performed, false if not needed.
    ///
    /// Note: The underlying storage is a ZFS zvol on the FreeBSD storage node,
    /// but the FILESYSTEM on top (formatted by the initiator) is typically:
    /// - Linux initiators: ext4 or xfs
    /// - FreeBSD initiators: ufs
    ///
    /// ZFS filesystem on a remote iSCSI target is not a valid use case.
    fn resize_filesystem(path: &str, fs_type: &str) -> Result<bool, Status> {
        match fs_type {
            "ext4" | "ext3" | "ext2" => {
                // Get the device for this mount point
                let device = Self::get_mount_device(path)?;
                info!(device = %device, fs_type = %fs_type, "Resizing ext filesystem");

                let output = std::process::Command::new("resize2fs")
                    .arg(&device)
                    .output()
                    .map_err(|e| {
                        error!(error = %e, "Failed to execute resize2fs");
                        Status::internal(format!("Failed to resize ext filesystem: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    // "Nothing to do" is not an error
                    if stderr.contains("Nothing to do") || stderr.contains("already") {
                        return Ok(false);
                    }
                    error!(stderr = %stderr, "resize2fs failed");
                    return Err(Status::internal(format!("resize2fs failed: {}", stderr)));
                }
                Ok(true)
            }
            "xfs" => {
                info!(path = %path, "Resizing XFS filesystem");

                let output = std::process::Command::new("xfs_growfs")
                    .arg(path)
                    .output()
                    .map_err(|e| {
                        error!(error = %e, "Failed to execute xfs_growfs");
                        Status::internal(format!("Failed to resize XFS filesystem: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    // Already at max size is not an error
                    if stderr.contains("data size unchanged") {
                        return Ok(false);
                    }
                    error!(stderr = %stderr, "xfs_growfs failed");
                    return Err(Status::internal(format!("xfs_growfs failed: {}", stderr)));
                }
                Ok(true)
            }
            "ufs" => {
                // FreeBSD UFS: use growfs command
                let device = Self::get_mount_device(path)?;
                info!(device = %device, "Resizing UFS filesystem");

                let output = std::process::Command::new("growfs")
                    .args(["-y", &device])
                    .output()
                    .map_err(|e| {
                        error!(error = %e, "Failed to execute growfs");
                        Status::internal(format!("Failed to resize UFS filesystem: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!(stderr = %stderr, "growfs failed");
                    return Err(Status::internal(format!("growfs failed: {}", stderr)));
                }
                Ok(true)
            }
            _ => {
                warn!(fs_type = %fs_type, "Unknown filesystem type, skipping resize");
                Ok(false)
            }
        }
    }

    /// Get the device backing a mount point.
    fn get_mount_device(path: &str) -> Result<String, Status> {
        Self::validate_path(path)?;

        let output = std::process::Command::new("df")
            .arg(path)
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute df");
                Status::internal(format!("Failed to get mount device: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        // df output: Filesystem ... (first column is device)
        if let Some(line) = stdout.lines().nth(1)
            && let Some(device) = line.split_whitespace().next()
        {
            return Ok(device.to_string());
        }

        Err(Status::internal("Could not determine mount device"))
    }

    /// Derive the iSCSI target IQN from a volume ID.
    fn derive_iqn(volume_id: &str) -> String {
        format!("{}:{}", BASE_IQN, volume_id)
    }

    /// Derive the NVMeoF target NQN from a volume ID.
    fn derive_nqn(volume_id: &str) -> String {
        format!("{}:{}", BASE_NQN, volume_id)
    }

    /// Find and disconnect any iSCSI/NVMeoF targets for this volume.
    /// Uses session queries to find connected targets matching the volume ID.
    ///
    /// Returns error if disconnect fails - this is critical for correctness.
    /// Returning success when still connected would lie to Kubernetes and
    /// could cause data corruption (zombie LUNs, dual-attach scenarios).
    fn disconnect_volume_targets(volume_id: &str) -> Result<(), Status> {
        debug!(volume_id = %volume_id, "Attempting to disconnect volume targets");

        // Try iSCSI first
        let iqn = Self::derive_iqn(volume_id);
        let iscsi_connected = Platform::is_iscsi_connected(&iqn);
        debug!(target = %iqn, connected = %iscsi_connected, "Checking iSCSI target");

        if iscsi_connected {
            info!(target = %iqn, "Disconnecting iSCSI target");
            Platform::disconnect_iscsi(&iqn).map_err(|e| {
                error!(error = %e, target = %iqn, "Failed to disconnect iSCSI target");
                Status::internal(format!(
                    "Failed to disconnect iSCSI target {}: {}. Volume may still be connected.",
                    iqn, e
                ))
            })?;

            // Verify disconnect succeeded
            if Platform::is_iscsi_connected(&iqn) {
                error!(target = %iqn, "iSCSI target still connected after disconnect");
                return Err(Status::internal(format!(
                    "iSCSI target {} still connected after disconnect attempt",
                    iqn
                )));
            }
        }

        // Try NVMeoF
        let nqn = Self::derive_nqn(volume_id);
        let nvme_connected = Platform::is_nvmeof_connected(&nqn);
        debug!(target = %nqn, connected = %nvme_connected, "Checking NVMeoF target");

        if nvme_connected {
            info!(target = %nqn, "Disconnecting NVMeoF target");
            Platform::disconnect_nvmeof(&nqn).map_err(|e| {
                error!(error = %e, target = %nqn, "Failed to disconnect NVMeoF target");
                Status::internal(format!(
                    "Failed to disconnect NVMeoF target {}: {}. Volume may still be connected.",
                    nqn, e
                ))
            })?;

            // Verify disconnect succeeded
            if Platform::is_nvmeof_connected(&nqn) {
                error!(target = %nqn, "NVMeoF target still connected after disconnect");
                return Err(Status::internal(format!(
                    "NVMeoF target {} still connected after disconnect attempt",
                    nqn
                )));
            }
            info!(target = %nqn, "NVMeoF disconnect verified successful");
        } else {
            debug!(target = %nqn, "NVMeoF target not connected (nothing to disconnect)");
        }

        Ok(())
    }

    /// Check if a volume capability is for block (raw device) access.
    fn is_block_volume(volume_capability: &Option<csi::VolumeCapability>) -> bool {
        matches!(
            volume_capability
                .as_ref()
                .and_then(|cap| cap.access_type.as_ref()),
            Some(csi::volume_capability::AccessType::Block(_))
        )
    }

    /// Get filesystem type from volume capability, with platform default fallback.
    fn get_fs_type_from_capability(
        volume_capability: &Option<csi::VolumeCapability>,
        volume_context: &std::collections::HashMap<String, String>,
    ) -> Result<&'static str, Status> {
        // Try to get from volume capability first
        if let Some(cap) = volume_capability
            && let Some(csi::volume_capability::AccessType::Mount(mount)) = &cap.access_type
            && !mount.fs_type.is_empty()
        {
            return Platform::validate_fs_type(&mount.fs_type);
        }

        // Fall back to volume_context
        let fs_type_raw = volume_context
            .get("fs_type")
            .or_else(|| volume_context.get("fsType"))
            .map(|s| s.as_str())
            .unwrap_or("");

        Platform::validate_fs_type(fs_type_raw)
    }

    /// Check if a block volume is staged by checking for an active target session.
    ///
    /// For block volumes, "staged" means the target session is connected.
    /// We check both iSCSI and NVMeoF based on the derived target names.
    fn is_block_volume_staged(volume_id: &str) -> bool {
        let iqn = Self::derive_iqn(volume_id);
        if Platform::is_iscsi_connected(&iqn) {
            return true;
        }

        let nqn = Self::derive_nqn(volume_id);
        Platform::is_nvmeof_connected(&nqn)
    }

    /// Find the block device for a volume by querying active sessions.
    ///
    /// Tries iSCSI first, then NVMeoF. Returns the device path if found.
    fn find_block_device(volume_id: &str) -> Result<String, Status> {
        // Try iSCSI first
        let iqn = Self::derive_iqn(volume_id);
        if Platform::is_iscsi_connected(&iqn) {
            return Platform::find_iscsi_device(&iqn);
        }

        // Try NVMeoF
        let nqn = Self::derive_nqn(volume_id);
        if Platform::is_nvmeof_connected(&nqn) {
            return Platform::find_nvmeof_device(&nqn);
        }

        Err(Status::failed_precondition(format!(
            "No active session found for volume {}",
            volume_id
        )))
    }
}

#[tonic::async_trait]
impl csi::node_server::Node for NodeService {
    /// Stage a volume to a staging path.
    ///
    /// For filesystem volumes: connects to iSCSI/NVMeoF target, formats if needed, and mounts.
    /// For block volumes: connects to target and stores device path (no mount).
    async fn node_stage_volume(
        &self,
        request: Request<csi::NodeStageVolumeRequest>,
    ) -> Result<Response<csi::NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let staging_target_path = &req.staging_target_path;
        let volume_context = &req.volume_context;
        let is_block = Self::is_block_volume(&req.volume_capability);

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        if staging_target_path.is_empty() {
            return Err(Status::invalid_argument("Staging target path is required"));
        }

        Self::validate_path(staging_target_path)?;

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            is_block = %is_block,
            "NodeStageVolume request"
        );

        // Get volume context parameters
        let target_name = volume_context
            .get("target_name")
            .or_else(|| volume_context.get("targetName"))
            .ok_or_else(|| Status::invalid_argument("target_name is required in volume context"))?;

        Self::validate_target_name(target_name)?;

        let export_type = volume_context
            .get("export_type")
            .or_else(|| volume_context.get("exportType"))
            .map(|s| s.as_str())
            .unwrap_or("iscsi");

        // Get portal/address info (required on Linux, optional on FreeBSD)
        let portal = volume_context
            .get("portal")
            .or_else(|| volume_context.get("targetPortal"))
            .map(|s| s.as_str());

        let transport_addr = volume_context
            .get("transport_addr")
            .or_else(|| volume_context.get("transportAddr"))
            .map(|s| s.as_str());

        let transport_port = volume_context
            .get("transport_port")
            .or_else(|| volume_context.get("transportPort"))
            .map(|s| s.as_str());

        // Check if already staged
        if is_block {
            // Block volume: check if target session is active
            if Self::is_block_volume_staged(volume_id) {
                info!(volume_id = %volume_id, "Block volume already staged (session active)");
                return Ok(Response::new(csi::NodeStageVolumeResponse {}));
            }
        } else {
            // Mount volume: check if mounted
            if Platform::is_mounted(staging_target_path)? {
                info!(staging_target_path = %staging_target_path, "Volume already staged");
                return Ok(Response::new(csi::NodeStageVolumeResponse {}));
            }
        }

        // Connect to target and get device
        let device = match export_type.to_lowercase().as_str() {
            "iscsi" => Platform::connect_iscsi(target_name, portal)?,
            "nvmeof" | "nvme" => {
                Platform::connect_nvmeof(target_name, transport_addr, transport_port)?
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported export type: {}",
                    other
                )));
            }
        };

        if is_block {
            // Block volume: connection is complete, device will be queried at publish time
            // No local state stored - device path is discovered from session
            info!(
                volume_id = %volume_id,
                device = %device,
                "Block volume staged successfully (session connected)"
            );
        } else {
            // Mount volume: format if needed and mount
            let fs_type =
                Self::get_fs_type_from_capability(&req.volume_capability, volume_context)?;

            if Platform::needs_formatting(&device)? {
                Platform::format_device(&device, fs_type)?;
            }

            // Mount the device to staging path
            Platform::mount_device(&device, staging_target_path, fs_type)?;

            info!(
                volume_id = %volume_id,
                staging_target_path = %staging_target_path,
                device = %device,
                fs_type = %fs_type,
                "Mount volume staged successfully"
            );
        }

        Ok(Response::new(csi::NodeStageVolumeResponse {}))
    }

    /// Unstage a volume from the staging path.
    ///
    /// For filesystem volumes: unmounts the staging path.
    /// For block volumes: just disconnects the target (no local state to clean).
    async fn node_unstage_volume(
        &self,
        request: Request<csi::NodeUnstageVolumeRequest>,
    ) -> Result<Response<csi::NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let staging_target_path = &req.staging_target_path;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        if staging_target_path.is_empty() {
            return Err(Status::invalid_argument("Staging target path is required"));
        }

        Self::validate_path(staging_target_path)?;

        // Determine volume type: if staging path is mounted, it's a filesystem volume
        // Block volumes don't mount anything, they just have an active session
        let is_mounted = Platform::is_mounted(staging_target_path)?;

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            is_mounted = %is_mounted,
            "NodeUnstageVolume request"
        );

        if is_mounted {
            // Filesystem volume: unmount from staging path
            Platform::unmount(staging_target_path)?;
        }
        // Block volumes have no mount to clean up

        // Disconnect any iSCSI/NVMeoF targets for this volume.
        // Target names are derived from volume_id using our naming convention.
        // IMPORTANT: We must return error if disconnect fails - lying to Kubernetes
        // about the disconnect state can cause data corruption (zombie LUNs).
        Self::disconnect_volume_targets(volume_id)?;

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            "Volume unstaged successfully"
        );

        Ok(Response::new(csi::NodeUnstageVolumeResponse {}))
    }

    /// Publish a volume to a target path.
    ///
    /// For filesystem volumes: bind mount from staging to target.
    /// For block volumes: create symlink from device to target path.
    async fn node_publish_volume(
        &self,
        request: Request<csi::NodePublishVolumeRequest>,
    ) -> Result<Response<csi::NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let target_path = &req.target_path;
        let staging_target_path = &req.staging_target_path;
        let is_block = Self::is_block_volume(&req.volume_capability);

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        if target_path.is_empty() {
            return Err(Status::invalid_argument("Target path is required"));
        }

        Self::validate_path(target_path)?;

        if staging_target_path.is_empty() {
            return Err(Status::invalid_argument(
                "Staging target path is required (STAGE_UNSTAGE_VOLUME capability is enabled)",
            ));
        }

        Self::validate_path(staging_target_path)?;

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            target_path = %target_path,
            readonly = %req.readonly,
            is_block = %is_block,
            "NodePublishVolume request"
        );

        if is_block {
            // Block volume: query device from active session and create symlink
            let device = Self::find_block_device(volume_id)?;

            // Check if already published (symlink exists and points to same device)
            if let Ok(existing) = fs::read_link(target_path) {
                if existing.to_string_lossy() == device {
                    info!(target_path = %target_path, "Block volume already published");
                    return Ok(Response::new(csi::NodePublishVolumeResponse {}));
                }
                // Remove stale symlink
                fs::remove_file(target_path).map_err(|e| {
                    error!(error = %e, path = %target_path, "Failed to remove stale symlink");
                    Status::internal(format!("Failed to remove stale symlink: {}", e))
                })?;
            }

            // Create parent directory if needed
            if let Some(parent) = Path::new(target_path).parent()
                && !parent.exists()
            {
                fs::create_dir_all(parent).map_err(|e| {
                    error!(error = %e, path = ?parent, "Failed to create parent directory");
                    Status::internal(format!("Failed to create parent directory: {}", e))
                })?;
            }

            // Create symlink to device
            symlink(&device, target_path).map_err(|e| {
                error!(error = %e, device = %device, target = %target_path, "Failed to create device symlink");
                Status::internal(format!("Failed to create device symlink: {}", e))
            })?;

            info!(
                volume_id = %volume_id,
                target_path = %target_path,
                device = %device,
                "Block volume published successfully"
            );
        } else {
            // Mount volume: bind mount from staging
            // Check if staging path is mounted
            if !Platform::is_mounted(staging_target_path)? {
                return Err(Status::failed_precondition(format!(
                    "Volume not staged at {}",
                    staging_target_path
                )));
            }

            // Check if already published
            if Platform::is_mounted(target_path)? {
                info!(target_path = %target_path, "Volume already published");
                return Ok(Response::new(csi::NodePublishVolumeResponse {}));
            }

            // Create bind mount from staging to target
            Platform::bind_mount(staging_target_path, target_path)?;

            // Handle readonly mount if requested
            if req.readonly {
                // Remount as read-only
                let output = std::process::Command::new("mount")
                    .args(["-o", "remount,ro", target_path])
                    .output()
                    .map_err(|e| {
                        error!(error = %e, "Failed to remount as readonly");
                        Status::internal(format!("Failed to remount as readonly: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!(stderr = %stderr, target_path = %target_path, "Failed to set readonly mount");
                    // Unmount and fail - readonly was explicitly requested
                    if let Err(e) = Platform::unmount(target_path) {
                        warn!(error = %e, "Failed to unmount after readonly failure");
                    }
                    return Err(Status::internal(format!(
                        "Failed to set readonly mount: {}",
                        stderr
                    )));
                }
            }

            info!(
                volume_id = %volume_id,
                target_path = %target_path,
                "Mount volume published successfully"
            );
        }

        Ok(Response::new(csi::NodePublishVolumeResponse {}))
    }

    /// Unpublish a volume from the target path.
    ///
    /// For filesystem volumes: unmount the bind mount.
    /// For block volumes: remove the symlink.
    async fn node_unpublish_volume(
        &self,
        request: Request<csi::NodeUnpublishVolumeRequest>,
    ) -> Result<Response<csi::NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let target_path = &req.target_path;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        if target_path.is_empty() {
            return Err(Status::invalid_argument("Target path is required"));
        }

        Self::validate_path(target_path)?;

        let target = Path::new(target_path);

        // Determine if this is a block volume by checking if target is a symlink
        let is_block = target
            .symlink_metadata()
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false);

        info!(
            volume_id = %volume_id,
            target_path = %target_path,
            is_block = %is_block,
            "NodeUnpublishVolume request"
        );

        if is_block {
            // Block volume: remove the symlink
            if let Err(e) = fs::remove_file(target_path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                error!(error = %e, path = %target_path, "Failed to remove block device symlink");
                return Err(Status::internal(format!("Failed to remove symlink: {}", e)));
            }
            info!(volume_id = %volume_id, target_path = %target_path, "Block volume unpublished");
        } else {
            // Mount volume: unmount from target path
            Platform::unmount(target_path)?;

            // Try to remove the target directory
            if target.exists()
                && let Err(e) = fs::remove_dir(target_path)
            {
                // Only warn, don't fail - the directory might not be empty
                warn!(error = %e, target_path = %target_path, "Could not remove target directory");
            }
            info!(volume_id = %volume_id, target_path = %target_path, "Mount volume unpublished");
        }

        Ok(Response::new(csi::NodeUnpublishVolumeResponse {}))
    }

    /// Get information about this node.
    async fn node_get_info(
        &self,
        _request: Request<csi::NodeGetInfoRequest>,
    ) -> Result<Response<csi::NodeGetInfoResponse>, Status> {
        info!(node_id = %self.node_id, "NodeGetInfo request");

        Ok(Response::new(csi::NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0, // No limit
            accessible_topology: None,
        }))
    }

    /// Report node capabilities.
    async fn node_get_capabilities(
        &self,
        _request: Request<csi::NodeGetCapabilitiesRequest>,
    ) -> Result<Response<csi::NodeGetCapabilitiesResponse>, Status> {
        let capabilities = vec![
            csi::NodeServiceCapability {
                r#type: Some(csi::node_service_capability::Type::Rpc(
                    csi::node_service_capability::Rpc {
                        r#type: csi::node_service_capability::rpc::Type::StageUnstageVolume as i32,
                    },
                )),
            },
            csi::NodeServiceCapability {
                r#type: Some(csi::node_service_capability::Type::Rpc(
                    csi::node_service_capability::Rpc {
                        r#type: csi::node_service_capability::rpc::Type::ExpandVolume as i32,
                    },
                )),
            },
        ];

        Ok(Response::new(csi::NodeGetCapabilitiesResponse {
            capabilities,
        }))
    }

    /// Expand a volume on this node.
    ///
    /// This resizes the filesystem to use all available space on the underlying
    /// block device. The controller has already expanded the ZFS zvol; this
    /// method handles the filesystem layer.
    ///
    /// - ZFS/UFS: Expansion is automatic at the zvol level
    /// - ext4/ext3/ext2: Uses resize2fs
    /// - XFS: Uses xfs_growfs
    async fn node_expand_volume(
        &self,
        request: Request<csi::NodeExpandVolumeRequest>,
    ) -> Result<Response<csi::NodeExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let volume_path = &req.volume_path;

        if volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID is required"));
        }

        if volume_path.is_empty() {
            return Err(Status::invalid_argument("Volume path is required"));
        }

        Self::validate_path(volume_path)?;

        info!(
            volume_id = %volume_id,
            volume_path = %volume_path,
            "NodeExpandVolume request"
        );

        // Detect filesystem type and resize if needed
        let fs_type = Self::detect_filesystem_type(volume_path)?;
        debug!(volume_id = %volume_id, fs_type = %fs_type, "Detected filesystem type");

        // Perform filesystem-specific resize
        let resized = Self::resize_filesystem(volume_path, &fs_type)?;
        if resized {
            info!(volume_id = %volume_id, fs_type = %fs_type, "Filesystem resized successfully");
        } else {
            debug!(volume_id = %volume_id, fs_type = %fs_type, "Filesystem resize not needed or automatic");
        }

        // Get final capacity after resize
        let capacity_bytes = Self::get_volume_capacity(volume_path)?;

        info!(
            volume_id = %volume_id,
            capacity_bytes = capacity_bytes,
            "Volume expansion completed"
        );

        Ok(Response::new(csi::NodeExpandVolumeResponse {
            capacity_bytes,
        }))
    }

    /// Get volume statistics (not implemented).
    async fn node_get_volume_stats(
        &self,
        _request: Request<csi::NodeGetVolumeStatsRequest>,
    ) -> Result<Response<csi::NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("NodeGetVolumeStats is not supported"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_path_valid() {
        assert!(NodeService::validate_path("/var/lib/csi/staging").is_ok());
        assert!(NodeService::validate_path("/mnt/volume").is_ok());
        assert!(NodeService::validate_path("/a/b/c/d/e").is_ok());
    }

    #[test]
    fn test_validate_path_invalid() {
        // Empty path
        assert!(NodeService::validate_path("").is_err());

        // Relative path
        assert!(NodeService::validate_path("var/lib").is_err());

        // Path traversal
        assert!(NodeService::validate_path("/var/../etc").is_err());

        // Dangerous characters
        assert!(NodeService::validate_path("/var;ls").is_err());
        assert!(NodeService::validate_path("/var|cat").is_err());
        assert!(NodeService::validate_path("/var$HOME").is_err());
        assert!(NodeService::validate_path("/var`id`").is_err());
    }

    #[test]
    fn test_validate_target_name_valid() {
        assert!(
            NodeService::validate_target_name("iqn.2023-01.com.example:storage.target1").is_ok()
        );
        assert!(NodeService::validate_target_name("nqn.2023-01.com.example:nvme.target1").is_ok());
    }

    #[test]
    fn test_validate_target_name_invalid() {
        // Empty
        assert!(NodeService::validate_target_name("").is_err());

        // Contains shell characters
        assert!(NodeService::validate_target_name("target;rm -rf").is_err());
        assert!(NodeService::validate_target_name("target$(id)").is_err());
    }

    #[test]
    fn test_node_service_creation() {
        let service = NodeService::new("test-node-1".to_string());
        assert_eq!(service.node_id, "test-node-1");
    }
}
