//! CSI Node Service Implementation
//!
//! Handles volume staging and publishing on worker nodes using
//! iSCSI/NVMeoF connections and bind mounts.
//!
//! Platform-specific operations (iSCSI, NVMe, filesystem, bind mounts)
//! are delegated to the `platform` module which provides compile-time
//! platform selection for FreeBSD and Linux.

use std::path::Path;

use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::csi;
use crate::platform;

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

        Self::validate_target_name(target_name)?;

        let export_type = volume_context
            .get("export_type")
            .or_else(|| volume_context.get("exportType"))
            .map(|s| s.as_str())
            .unwrap_or("iscsi");

        // Get filesystem type, using platform default if not specified
        let fs_type_raw = volume_context
            .get("fs_type")
            .or_else(|| volume_context.get("fsType"))
            .map(|s| s.as_str())
            .unwrap_or("");

        // Validate and normalize filesystem type for this platform
        let fs_type = platform::validate_fs_type(fs_type_raw)?;

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
        if platform::is_mounted(staging_target_path)? {
            info!(staging_target_path = %staging_target_path, "Volume already staged");
            return Ok(Response::new(csi::NodeStageVolumeResponse {}));
        }

        // Connect to target and get device
        let device = match export_type.to_lowercase().as_str() {
            "iscsi" => platform::connect_iscsi(target_name, portal)?,
            "nvmeof" | "nvme" => {
                platform::connect_nvmeof(target_name, transport_addr, transport_port)?
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unsupported export type: {}",
                    other
                )));
            }
        };

        // Format if needed (only for non-ZFS)
        if fs_type != "zfs" && platform::needs_formatting(&device)? {
            platform::format_device(&device, fs_type)?;
        }

        // Mount the device to staging path
        platform::mount_device(&device, staging_target_path, fs_type)?;

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
        platform::unmount(staging_target_path)?;

        // Disconnect from iSCSI/NVMeoF targets
        // We derive the target names from the volume_id using the CSI naming convention.
        // Since we don't know which protocol was used, we try both (they handle "not connected" gracefully).
        let target_iqn = format!("iqn.2024-01.org.freebsd.csi:{}", volume_id);
        let target_nqn = format!("nqn.2024-01.org.freebsd.csi:{}", volume_id);

        // Try iSCSI disconnect (logs out and cleans up /etc/iscsi/nodes/)
        if let Err(e) = platform::disconnect_iscsi(&target_iqn) {
            // Log but don't fail - the volume may have been NVMeoF, not iSCSI
            debug!(error = %e, target_iqn = %target_iqn, "iSCSI disconnect failed (may be NVMeoF volume)");
        }

        // Try NVMeoF disconnect
        if let Err(e) = platform::disconnect_nvmeof(&target_nqn) {
            // Log but don't fail - the volume may have been iSCSI, not NVMeoF
            debug!(error = %e, target_nqn = %target_nqn, "NVMeoF disconnect failed (may be iSCSI volume)");
        }

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            "Volume unstaged successfully"
        );

        Ok(Response::new(csi::NodeUnstageVolumeResponse {}))
    }

    /// Publish a volume to a target path (bind mount from staging).
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
        if !platform::is_mounted(staging_target_path)? {
            return Err(Status::failed_precondition(format!(
                "Volume not staged at {}",
                staging_target_path
            )));
        }

        // Check if already published
        if platform::is_mounted(target_path)? {
            info!(target_path = %target_path, "Volume already published");
            return Ok(Response::new(csi::NodePublishVolumeResponse {}));
        }

        // Create bind mount from staging to target
        platform::bind_mount(staging_target_path, target_path)?;

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
        platform::unmount(target_path)?;

        // Try to remove the target directory
        if Path::new(target_path).exists()
            && let Err(e) = std::fs::remove_dir(target_path)
        {
            // Only warn, don't fail - the directory might not be empty
            warn!(error = %e, target_path = %target_path, "Could not remove target directory");
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
    /// For ZFS, expansion is handled automatically. For other filesystems,
    /// filesystem-specific resize tools would be needed.
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
        // For ext4/xfs, we'd need to run resize2fs/xfs_growfs
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
