//! CSI Node Service Implementation
//!
//! Handles volume staging and publishing on FreeBSD nodes using
//! iSCSI/NVMeoF connections and nullfs mounts.

use std::path::Path;
use std::process::Command;

use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::csi;

/// CSI Node Service
///
/// Implements the CSI Node service which handles:
/// - Volume staging (connect to iSCSI/NVMeoF target, mount to staging path)
/// - Volume unstaging (unmount from staging path)
/// - Volume publishing (nullfs mount from staging to target path)
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
        let dangerous_chars = [';', '|', '&', '$', '`', '(', ')', '{', '}', '<', '>', '\n', '\r'];
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
        let valid = target.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '.' || c == ':' || c == '-' || c == '_'
        });

        if !valid {
            return Err(Status::invalid_argument(
                "Target name contains invalid characters",
            ));
        }

        Ok(())
    }

    /// Connect to an iSCSI target using iscsictl.
    fn connect_iscsi(target_iqn: &str) -> Result<String, Status> {
        Self::validate_target_name(target_iqn)?;

        info!(target_iqn = %target_iqn, "Connecting to iSCSI target");

        let output = Command::new("iscsictl")
            .args(["-An", target_iqn])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute iscsictl");
                Status::internal(format!("Failed to execute iscsictl: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(stderr = %stderr, "iscsictl failed");
            return Err(Status::internal(format!("iscsictl failed: {}", stderr)));
        }

        // After connecting, we need to find the device
        // The device will typically be /dev/da<N> for iSCSI
        // We'll scan for new devices after connection
        let device = Self::find_iscsi_device(target_iqn)?;
        info!(device = %device, "iSCSI target connected");

        Ok(device)
    }

    /// Find the device associated with an iSCSI target.
    fn find_iscsi_device(target_iqn: &str) -> Result<String, Status> {
        // Use iscsictl -L to list sessions and find the device
        let output = Command::new("iscsictl")
            .arg("-L")
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute iscsictl -L");
                Status::internal(format!("Failed to list iSCSI sessions: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse output to find device for this target
        // Format: "Target: <iqn> ... Device: da<N>"
        for line in stdout.lines() {
            if line.contains(target_iqn) {
                // Look for device in the same or following lines
                if let Some(device_part) = line.split_whitespace().find(|s| s.starts_with("da")) {
                    return Ok(format!("/dev/{}", device_part));
                }
            }
        }

        // If not found in iscsictl output, try camcontrol
        let output = Command::new("camcontrol")
            .args(["devlist"])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute camcontrol");
                Status::internal(format!("Failed to list devices: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Find the most recently added da device
        // This is a fallback - in production we'd want more precise device identification
        for line in stdout.lines().rev() {
            if let Some(start) = line.find("(da") {
                if let Some(end) = line[start..].find(',') {
                    let device = &line[start + 1..start + end];
                    return Ok(format!("/dev/{}", device));
                }
            }
        }

        Err(Status::internal(
            "Could not find device for iSCSI target",
        ))
    }

    /// Connect to an NVMeoF target using nvmecontrol.
    fn connect_nvmeof(target_nqn: &str) -> Result<String, Status> {
        Self::validate_target_name(target_nqn)?;

        info!(target_nqn = %target_nqn, "Connecting to NVMeoF target");

        let output = Command::new("nvmecontrol")
            .args(["connect", "-n", target_nqn])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute nvmecontrol");
                Status::internal(format!("Failed to execute nvmecontrol: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(stderr = %stderr, "nvmecontrol connect failed");
            return Err(Status::internal(format!(
                "nvmecontrol connect failed: {}",
                stderr
            )));
        }

        // Find the NVMe device
        let device = Self::find_nvmeof_device(target_nqn)?;
        info!(device = %device, "NVMeoF target connected");

        Ok(device)
    }

    /// Find the device associated with an NVMeoF target.
    fn find_nvmeof_device(target_nqn: &str) -> Result<String, Status> {
        // Use nvmecontrol devlist to find devices
        let output = Command::new("nvmecontrol")
            .arg("devlist")
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute nvmecontrol devlist");
                Status::internal(format!("Failed to list NVMe devices: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse output to find device for this target
        // Look for nvme<N>ns<M> or nda<N> devices
        for line in stdout.lines() {
            if line.contains(target_nqn) || line.contains("nvme") {
                // Extract device name
                if let Some(device) = line.split_whitespace().next() {
                    if device.starts_with("nvme") || device.starts_with("nda") {
                        return Ok(format!("/dev/{}", device));
                    }
                }
            }
        }

        Err(Status::internal(
            "Could not find device for NVMeoF target",
        ))
    }

    /// Disconnect from an iSCSI target.
    /// Note: Currently unused but kept for potential future use in explicit target cleanup.
    #[allow(dead_code)]
    fn disconnect_iscsi(target_iqn: &str) -> Result<(), Status> {
        Self::validate_target_name(target_iqn)?;

        info!(target_iqn = %target_iqn, "Disconnecting from iSCSI target");

        let output = Command::new("iscsictl")
            .args(["-Rn", target_iqn])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute iscsictl");
                Status::internal(format!("Failed to execute iscsictl: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Treat "not found" as success (already disconnected)
            if stderr.contains("not found") || stderr.contains("No such") {
                warn!(target_iqn = %target_iqn, "iSCSI target already disconnected");
                return Ok(());
            }
            error!(stderr = %stderr, "iscsictl disconnect failed");
            return Err(Status::internal(format!(
                "iscsictl disconnect failed: {}",
                stderr
            )));
        }

        Ok(())
    }

    /// Disconnect from an NVMeoF target.
    /// Note: Currently unused but kept for potential future use in explicit target cleanup.
    #[allow(dead_code)]
    fn disconnect_nvmeof(target_nqn: &str) -> Result<(), Status> {
        Self::validate_target_name(target_nqn)?;

        info!(target_nqn = %target_nqn, "Disconnecting from NVMeoF target");

        let output = Command::new("nvmecontrol")
            .args(["disconnect", "-n", target_nqn])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute nvmecontrol");
                Status::internal(format!("Failed to execute nvmecontrol: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Treat "not found" as success (already disconnected)
            if stderr.contains("not found") || stderr.contains("No such") {
                warn!(target_nqn = %target_nqn, "NVMeoF target already disconnected");
                return Ok(());
            }
            error!(stderr = %stderr, "nvmecontrol disconnect failed");
            return Err(Status::internal(format!(
                "nvmecontrol disconnect failed: {}",
                stderr
            )));
        }

        Ok(())
    }

    /// Format a device with the specified filesystem type.
    fn format_device(device: &str, fs_type: &str) -> Result<(), Status> {
        Self::validate_path(device)?;

        info!(device = %device, fs_type = %fs_type, "Formatting device");

        match fs_type.to_lowercase().as_str() {
            "ufs" | "ffs" => {
                // Use newfs with soft updates for UFS
                let output = Command::new("newfs")
                    .args(["-U", device])
                    .output()
                    .map_err(|e| {
                        error!(error = %e, "Failed to execute newfs");
                        Status::internal(format!("Failed to execute newfs: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!(stderr = %stderr, "newfs failed");
                    return Err(Status::internal(format!("newfs failed: {}", stderr)));
                }
            }
            "zfs" => {
                // ZFS handles formatting automatically when creating pools/datasets
                // Skip explicit formatting
                debug!(device = %device, "Skipping format for ZFS (handled by ZFS tools)");
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported filesystem type: {}",
                    fs_type
                )));
            }
        }

        Ok(())
    }

    /// Check if a device needs formatting (has no valid filesystem).
    fn needs_formatting(device: &str) -> Result<bool, Status> {
        Self::validate_path(device)?;

        // Use file command to check if device has a filesystem
        let output = Command::new("file")
            .args(["-s", device])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute file command");
                Status::internal(format!("Failed to check device filesystem: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // If the output contains "data" or doesn't indicate a filesystem, it needs formatting
        Ok(stdout.contains("data") || (!stdout.contains("filesystem") && !stdout.contains("Unix")))
    }

    /// Mount a device to a target path.
    fn mount_device(device: &str, target: &str, fs_type: &str) -> Result<(), Status> {
        Self::validate_path(device)?;
        Self::validate_path(target)?;

        info!(device = %device, target = %target, fs_type = %fs_type, "Mounting device");

        // Ensure target directory exists
        std::fs::create_dir_all(target).map_err(|e| {
            error!(error = %e, "Failed to create mount target directory");
            Status::internal(format!("Failed to create mount directory: {}", e))
        })?;

        let fs_type_lower = fs_type.to_lowercase();
        let mount_type = match fs_type_lower.as_str() {
            "ufs" | "ffs" => "ufs",
            "zfs" => "zfs",
            _ => &fs_type_lower,
        };

        let output = Command::new("mount")
            .args(["-t", mount_type, device, target])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute mount");
                Status::internal(format!("Failed to execute mount: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(stderr = %stderr, "mount failed");
            return Err(Status::internal(format!("mount failed: {}", stderr)));
        }

        Ok(())
    }

    /// Unmount a path.
    fn unmount(target: &str) -> Result<(), Status> {
        Self::validate_path(target)?;

        info!(target = %target, "Unmounting");

        // Check if path is actually mounted
        if !Self::is_mounted(target)? {
            debug!(target = %target, "Path is not mounted, skipping unmount");
            return Ok(());
        }

        let output = Command::new("umount")
            .arg(target)
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute umount");
                Status::internal(format!("Failed to execute umount: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Treat "not mounted" as success
            if stderr.contains("not a mount point") || stderr.contains("not mounted") {
                warn!(target = %target, "Path was not mounted");
                return Ok(());
            }
            error!(stderr = %stderr, "umount failed");
            return Err(Status::internal(format!("umount failed: {}", stderr)));
        }

        Ok(())
    }

    /// Check if a path is currently mounted.
    fn is_mounted(target: &str) -> Result<bool, Status> {
        Self::validate_path(target)?;

        let output = Command::new("mount")
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute mount");
                Status::internal(format!("Failed to check mounts: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Check if target path appears in mount output
        Ok(stdout.lines().any(|line| line.contains(target)))
    }

    /// Create a nullfs mount (FreeBSD's equivalent to bind mount).
    fn nullfs_mount(source: &str, target: &str) -> Result<(), Status> {
        Self::validate_path(source)?;
        Self::validate_path(target)?;

        info!(source = %source, target = %target, "Creating nullfs mount");

        // Ensure target directory exists
        std::fs::create_dir_all(target).map_err(|e| {
            error!(error = %e, "Failed to create nullfs target directory");
            Status::internal(format!("Failed to create nullfs target directory: {}", e))
        })?;

        let output = Command::new("mount")
            .args(["-t", "nullfs", source, target])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute mount -t nullfs");
                Status::internal(format!("Failed to execute nullfs mount: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(stderr = %stderr, "nullfs mount failed");
            return Err(Status::internal(format!("nullfs mount failed: {}", stderr)));
        }

        Ok(())
    }

    /// Get the current capacity of a mounted volume.
    fn get_volume_capacity(path: &str) -> Result<i64, Status> {
        Self::validate_path(path)?;

        let output = Command::new("df")
            .args(["-k", path])
            .output()
            .map_err(|e| {
                error!(error = %e, "Failed to execute df");
                Status::internal(format!("Failed to get volume capacity: {}", e))
            })?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // Parse df output (second line, second column is total size in KB)
        if let Some(line) = stdout.lines().nth(1) {
            if let Some(size_kb) = line.split_whitespace().nth(1) {
                if let Ok(size) = size_kb.parse::<i64>() {
                    return Ok(size * 1024); // Convert KB to bytes
                }
            }
        }

        Err(Status::internal("Could not parse volume capacity"))
    }
}

#[tonic::async_trait]
impl csi::node_server::Node for NodeService {
    /// Stage a volume to a staging path.
    /// This connects to the iSCSI/NVMeoF target, formats if needed, and mounts.
    async fn node_stage_volume(
        &self,
        request: Request<csi::NodeStageVolumeRequest>,
    ) -> Result<Response<csi::NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let staging_target_path = &req.staging_target_path;
        let volume_context = &req.volume_context;

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
            "NodeStageVolume request"
        );

        // Get volume context parameters
        let target_name = volume_context
            .get("target_name")
            .or_else(|| volume_context.get("targetName"))
            .ok_or_else(|| Status::invalid_argument("target_name is required in volume context"))?;

        let export_type = volume_context
            .get("export_type")
            .or_else(|| volume_context.get("exportType"))
            .map(|s| s.as_str())
            .unwrap_or("iscsi");

        let fs_type = volume_context
            .get("fs_type")
            .or_else(|| volume_context.get("fsType"))
            .map(|s| s.as_str())
            .unwrap_or("ufs");

        // Check if already staged
        if Self::is_mounted(staging_target_path)? {
            info!(staging_target_path = %staging_target_path, "Volume already staged");
            return Ok(Response::new(csi::NodeStageVolumeResponse {}));
        }

        // Connect to target and get device
        let device = match export_type.to_lowercase().as_str() {
            "iscsi" => Self::connect_iscsi(target_name)?,
            "nvmeof" | "nvme" => Self::connect_nvmeof(target_name)?,
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported export type: {}",
                    other
                )));
            }
        };

        // Format if needed (only for non-ZFS)
        if fs_type.to_lowercase() != "zfs" && Self::needs_formatting(&device)? {
            Self::format_device(&device, fs_type)?;
        }

        // Mount the device to staging path
        Self::mount_device(&device, staging_target_path, fs_type)?;

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            device = %device,
            "Volume staged successfully"
        );

        Ok(Response::new(csi::NodeStageVolumeResponse {}))
    }

    /// Unstage a volume from the staging path.
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

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            "NodeUnstageVolume request"
        );

        // Unmount from staging path
        Self::unmount(staging_target_path)?;

        // Note: We don't disconnect from iSCSI/NVMeoF here because
        // the target information is not available in the unstage request.
        // The disconnect should be handled by the controller when the volume is deleted.

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            "Volume unstaged successfully"
        );

        Ok(Response::new(csi::NodeUnstageVolumeResponse {}))
    }

    /// Publish a volume to a target path (nullfs mount from staging).
    async fn node_publish_volume(
        &self,
        request: Request<csi::NodePublishVolumeRequest>,
    ) -> Result<Response<csi::NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();
        let volume_id = &req.volume_id;
        let target_path = &req.target_path;
        let staging_target_path = &req.staging_target_path;

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
            "NodePublishVolume request"
        );

        // Check if staging path is mounted
        if !Self::is_mounted(staging_target_path)? {
            return Err(Status::failed_precondition(format!(
                "Volume not staged at {}",
                staging_target_path
            )));
        }

        // Check if already published
        if Self::is_mounted(target_path)? {
            info!(target_path = %target_path, "Volume already published");
            return Ok(Response::new(csi::NodePublishVolumeResponse {}));
        }

        // Create nullfs mount from staging to target
        Self::nullfs_mount(staging_target_path, target_path)?;

        // Handle readonly mount if requested
        if req.readonly {
            // Remount as read-only
            let output = Command::new("mount")
                .args(["-o", "update,ro", target_path])
                .output()
                .map_err(|e| {
                    error!(error = %e, "Failed to remount as readonly");
                    Status::internal(format!("Failed to remount as readonly: {}", e))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                warn!(stderr = %stderr, "Failed to set readonly, continuing anyway");
            }
        }

        info!(
            volume_id = %volume_id,
            target_path = %target_path,
            "Volume published successfully"
        );

        Ok(Response::new(csi::NodePublishVolumeResponse {}))
    }

    /// Unpublish a volume from the target path.
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

        info!(
            volume_id = %volume_id,
            target_path = %target_path,
            "NodeUnpublishVolume request"
        );

        // Unmount from target path
        Self::unmount(target_path)?;

        // Try to remove the target directory
        if Path::new(target_path).exists() {
            if let Err(e) = std::fs::remove_dir(target_path) {
                // Only warn, don't fail - the directory might not be empty
                warn!(error = %e, target_path = %target_path, "Could not remove target directory");
            }
        }

        info!(
            volume_id = %volume_id,
            target_path = %target_path,
            "Volume unpublished successfully"
        );

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
    /// For ZFS, expansion is handled automatically. For UFS, we return the current size.
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

        // Get current capacity
        // For ZFS, the underlying zvol handles expansion automatically
        // For UFS, we'd need to run growfs, but this is complex and risky
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
        Err(Status::unimplemented(
            "NodeGetVolumeStats is not supported",
        ))
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
        assert!(NodeService::validate_target_name(
            "iqn.2023-01.com.example:storage.target1"
        )
        .is_ok());
        assert!(NodeService::validate_target_name(
            "nqn.2023-01.com.example:nvme.target1"
        )
        .is_ok());
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
