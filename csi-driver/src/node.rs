//! CSI Node Service Implementation
//!
//! Handles volume staging and publishing on Linux worker nodes using
//! iSCSI/NVMeoF connections and bind mounts.
//!
//! Platform-specific operations (iSCSI, NVMe, filesystem, bind mounts)
//! are delegated to the `platform` module.
//!
//! ## Target Naming Convention
//!
//! Target names are derived from volume IDs using a consistent pattern:
//! - iSCSI: `iqn.2024-01.org.freebsd.csi:<volume_id>`
//! - NVMeoF: `nqn.2024-01.org.freebsd.csi:<volume_id>`
//!
//! This allows NodeUnstageVolume to determine the target to disconnect
//! without requiring local metadata storage.

use std::path::Path;

// Note: fs operations use tokio::fs for async file I/O,
// Command uses tokio::process::Command for async process execution.
// no local metadata storage - device paths are queried from active sessions.

use tokio::process::Command;

use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use std::collections::HashMap;

use crate::csi;
use crate::platform;
use crate::platform::{IscsiChapCredentials, NvmeAuthCredentials};
use crate::types::{Endpoints, ExportType};

/// Base IQN prefix for iSCSI targets (must match ctld-agent configuration)
const BASE_IQN: &str = "iqn.2024-01.org.freebsd.csi";

/// Base NQN prefix for NVMeoF targets (must match ctld-agent configuration)
const BASE_NQN: &str = "nqn.2024-01.org.freebsd.csi";

// Standard CSI secret keys for iSCSI CHAP authentication
// These follow the Linux open-iscsi naming conventions used by the CSI spec
const CHAP_USERNAME_KEY: &str = "node.session.auth.username";
const CHAP_PASSWORD_KEY: &str = "node.session.auth.password";
const CHAP_MUTUAL_USERNAME_KEY: &str = "node.session.auth.username_in";
const CHAP_MUTUAL_PASSWORD_KEY: &str = "node.session.auth.password_in";

// Secret keys for NVMeoF DH-HMAC-CHAP authentication
// These follow the nvme-cli naming conventions
const NVME_SECRET_KEY: &str = "nvme.auth.secret";
const NVME_CTRL_SECRET_KEY: &str = "nvme.auth.ctrl_secret";

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

    /// Extract iSCSI CHAP credentials from secrets map.
    ///
    /// Returns None if no CHAP credentials are present or if required fields are missing.
    fn extract_iscsi_chap(secrets: &HashMap<String, String>) -> Option<IscsiChapCredentials> {
        let username = secrets.get(CHAP_USERNAME_KEY)?;
        let password = secrets.get(CHAP_PASSWORD_KEY)?;

        // Username and password are required for CHAP
        if username.is_empty() || password.is_empty() {
            return None;
        }

        let credentials = IscsiChapCredentials {
            username: username.clone(),
            password: password.clone(),
            // Mutual CHAP is optional
            mutual_username: secrets
                .get(CHAP_MUTUAL_USERNAME_KEY)
                .filter(|s| !s.is_empty())
                .cloned(),
            mutual_password: secrets
                .get(CHAP_MUTUAL_PASSWORD_KEY)
                .filter(|s| !s.is_empty())
                .cloned(),
        };

        debug!(
            username = %credentials.username,
            has_mutual = credentials.mutual_username.is_some(),
            "Extracted iSCSI CHAP credentials from secrets"
        );

        Some(credentials)
    }

    /// Extract NVMeoF DH-HMAC-CHAP credentials from secrets map.
    ///
    /// Returns None if no NVMeoF auth credentials are present or if required fields are missing.
    fn extract_nvme_auth(secrets: &HashMap<String, String>) -> Option<NvmeAuthCredentials> {
        let secret = secrets.get(NVME_SECRET_KEY)?;

        // Secret is required for DH-HMAC-CHAP
        if secret.is_empty() {
            return None;
        }

        let credentials = NvmeAuthCredentials {
            secret: secret.clone(),
            ctrl_secret: secrets
                .get(NVME_CTRL_SECRET_KEY)
                .filter(|s| !s.is_empty())
                .cloned(),
        };

        debug!(
            has_ctrl_secret = credentials.ctrl_secret.is_some(),
            "Extracted NVMeoF DH-HMAC-CHAP credentials from secrets"
        );

        Some(credentials)
    }

    /// Get the current capacity of a mounted volume.
    async fn get_volume_capacity(path: &str) -> Result<i64, Status> {
        Self::validate_path(path)?;

        let output = Command::new("df")
            .args(["-k", path])
            .output()
            .await
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
    async fn detect_filesystem_type(path: &str) -> Result<String, Status> {
        Self::validate_path(path)?;

        let output = Command::new("df")
            .args(["-T", path])
            .output()
            .await
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

        Ok("unknown".to_string())
    }

    /// Resize the filesystem to use all available space on the device.
    /// Returns true if resize was performed, false if not needed.
    ///
    /// Note: The underlying storage is a ZFS zvol on the FreeBSD storage node,
    /// but the FILESYSTEM on top (formatted by the initiator) is ext4 or xfs.
    async fn resize_filesystem(path: &str, fs_type: &str) -> Result<bool, Status> {
        match fs_type {
            "ext4" | "ext3" | "ext2" => {
                let device = Self::get_mount_device(path).await?;
                info!(device = %device, fs_type = %fs_type, "Resizing ext filesystem");

                let output = Command::new("resize2fs")
                    .arg(&device)
                    .output()
                    .await
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

                let output = Command::new("xfs_growfs")
                    .arg(path)
                    .output()
                    .await
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
            _ => {
                warn!(fs_type = %fs_type, "Unknown filesystem type, skipping resize");
                Ok(false)
            }
        }
    }

    /// Get the device backing a mount point.
    ///
    /// Uses `findmnt -n -o SOURCE` for reliable device lookup.
    async fn get_mount_device(path: &str) -> Result<String, Status> {
        Self::validate_path(path)?;

        let output = Command::new("findmnt")
            .args(["-n", "-o", "SOURCE", path])
            .output()
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to execute findmnt");
                Status::internal(format!("Failed to get mount device: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(path = %path, stderr = %stderr, "findmnt failed");
            return Err(Status::internal(format!(
                "Path {} is not a mount point",
                path
            )));
        }

        let device = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if device.is_empty() {
            return Err(Status::internal(format!(
                "Could not determine device for mount point {}",
                path
            )));
        }

        // Validate device path looks reasonable (starts with /dev/)
        if !device.starts_with("/dev/") {
            warn!(
                device = %device,
                path = %path,
                "Mount device is not a block device path"
            );
        }

        Ok(device)
    }

    /// Parse endpoints from volume_context for multipath support.
    ///
    /// Format: "host:port,host2:port2,..." - supports IPs, hostnames, and IPv6.
    /// All endpoints are returned for multipath connections.
    ///
    /// Default ports: iSCSI=3260, NVMeoF=4420
    fn parse_endpoints(
        volume_context: &std::collections::HashMap<String, String>,
        export_type: ExportType,
    ) -> Result<Endpoints, Status> {
        let endpoints_str = volume_context
            .get("endpoints")
            .ok_or_else(|| Status::invalid_argument("Missing 'endpoints' in volume_context"))?;

        Endpoints::parse(endpoints_str, export_type.default_port())
            .map_err(|e| Status::invalid_argument(e.to_string()))
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
    async fn disconnect_volume_targets(volume_id: &str) -> Result<(), Status> {
        debug!(volume_id = %volume_id, "Attempting to disconnect volume targets");

        // Try iSCSI first
        let iqn = Self::derive_iqn(volume_id);
        let iscsi_connected = platform::is_iscsi_connected(&iqn).await;
        debug!(target = %iqn, connected = %iscsi_connected, "Checking iSCSI target");

        if iscsi_connected {
            info!(target = %iqn, "Disconnecting iSCSI target");
            platform::disconnect_iscsi(&iqn).await.map_err(|e| {
                error!(error = %e, target = %iqn, "Failed to disconnect iSCSI target");
                Status::internal(format!(
                    "Failed to disconnect iSCSI target {}: {}. Volume may still be connected.",
                    iqn, e
                ))
            })?;

            // Verify disconnect succeeded
            if platform::is_iscsi_connected(&iqn).await {
                error!(target = %iqn, "iSCSI target still connected after disconnect");
                return Err(Status::internal(format!(
                    "iSCSI target {} still connected after disconnect attempt",
                    iqn
                )));
            }
        }

        // Try NVMeoF
        let nqn = Self::derive_nqn(volume_id);
        let nvme_connected = platform::is_nvmeof_connected(&nqn).await;
        debug!(target = %nqn, connected = %nvme_connected, "Checking NVMeoF target");

        if nvme_connected {
            info!(target = %nqn, "Disconnecting NVMeoF target");
            platform::disconnect_nvmeof(&nqn).await.map_err(|e| {
                error!(error = %e, target = %nqn, "Failed to disconnect NVMeoF target");
                Status::internal(format!(
                    "Failed to disconnect NVMeoF target {}: {}. Volume may still be connected.",
                    nqn, e
                ))
            })?;

            // Verify disconnect succeeded
            if platform::is_nvmeof_connected(&nqn).await {
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
            return platform::validate_fs_type(&mount.fs_type);
        }

        // Fall back to volume_context
        let fs_type_raw = volume_context
            .get("fsType")
            .map(|s| s.as_str())
            .unwrap_or("");

        platform::validate_fs_type(fs_type_raw)
    }

    /// Check if a block volume is staged by checking for an active target session.
    ///
    /// For block volumes, "staged" means the target session is connected.
    /// We check both iSCSI and NVMeoF based on the derived target names.
    async fn is_block_volume_staged(volume_id: &str) -> bool {
        let iqn = Self::derive_iqn(volume_id);
        if platform::is_iscsi_connected(&iqn).await {
            return true;
        }

        let nqn = Self::derive_nqn(volume_id);
        platform::is_nvmeof_connected(&nqn).await
    }

    /// Find the block device for a volume by querying active sessions.
    ///
    /// Tries iSCSI first, then NVMeoF. Returns the device path if found.
    async fn find_block_device(volume_id: &str) -> Result<String, Status> {
        // Try iSCSI first
        let iqn = Self::derive_iqn(volume_id);
        if platform::is_iscsi_connected(&iqn).await {
            return platform::find_iscsi_device(&iqn).await;
        }

        // Try NVMeoF
        let nqn = Self::derive_nqn(volume_id);
        if platform::is_nvmeof_connected(&nqn).await {
            return platform::find_nvmeof_device(&nqn).await;
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
            .get("targetName")
            .ok_or_else(|| Status::invalid_argument("targetName is required in volume context"))?;

        Self::validate_target_name(target_name)?;

        let export_type: ExportType = volume_context
            .get("exportType")
            .and_then(|s| s.parse().ok())
            .unwrap_or_default();

        // Parse all endpoints from volume_context for multipath support
        let endpoints = Self::parse_endpoints(volume_context, export_type)?;

        debug!(
            volume_id = %volume_id,
            endpoints = %endpoints.to_portal_string(),
            multipath = endpoints.is_multipath(),
            "Parsed endpoints for staging"
        );

        // Check if already staged
        if is_block {
            // Block volume: check if target session is active
            if Self::is_block_volume_staged(volume_id).await {
                info!(volume_id = %volume_id, "Block volume already staged (session active)");
                return Ok(Response::new(csi::NodeStageVolumeResponse {}));
            }
        } else {
            // Mount volume: check if mounted
            if platform::is_mounted(staging_target_path).await? {
                info!(staging_target_path = %staging_target_path, "Volume already staged");
                return Ok(Response::new(csi::NodeStageVolumeResponse {}));
            }
        }

        // Extract authentication credentials from secrets based on export type
        let secrets = &req.secrets;

        // Connect to target and get device (multipath: connects to all endpoints)
        let device = match export_type {
            ExportType::Iscsi => {
                let chap_creds = Self::extract_iscsi_chap(secrets);
                platform::connect_iscsi(target_name, endpoints.as_slice(), chap_creds.as_ref())
                    .await?
            }
            ExportType::Nvmeof => {
                let nvme_creds = Self::extract_nvme_auth(secrets);
                platform::connect_nvmeof(target_name, endpoints.as_slice(), nvme_creds.as_ref())
                    .await?
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

            if platform::needs_formatting(&device).await? {
                platform::format_device(&device, fs_type).await?;
            }

            // Mount the device to staging path
            platform::mount_device(&device, staging_target_path, fs_type).await?;

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
        let is_mounted = platform::is_mounted(staging_target_path).await?;

        info!(
            volume_id = %volume_id,
            staging_target_path = %staging_target_path,
            is_mounted = %is_mounted,
            "NodeUnstageVolume request"
        );

        if is_mounted {
            // Filesystem volume: unmount from staging path
            platform::unmount(staging_target_path).await?;
        }
        // Block volumes have no mount to clean up

        // Disconnect any iSCSI/NVMeoF targets for this volume.
        // Target names are derived from volume_id using our naming convention.
        // IMPORTANT: We must return error if disconnect fails - lying to Kubernetes
        // about the disconnect state can cause data corruption (zombie LUNs).
        Self::disconnect_volume_targets(volume_id).await?;

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
            let device = Self::find_block_device(volume_id).await?;

            // Check if already published (symlink exists and points to same device)
            if let Ok(existing) = tokio::fs::read_link(target_path).await {
                if existing.to_string_lossy() == device {
                    info!(target_path = %target_path, "Block volume already published");
                    return Ok(Response::new(csi::NodePublishVolumeResponse {}));
                }
                // Remove stale symlink
                tokio::fs::remove_file(target_path).await.map_err(|e| {
                    error!(error = %e, path = %target_path, "Failed to remove stale symlink");
                    Status::internal(format!("Failed to remove stale symlink: {}", e))
                })?;
            }

            // Create parent directory if needed
            if let Some(parent) = Path::new(target_path).parent()
                && !tokio::fs::try_exists(parent).await.unwrap_or(false)
            {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    error!(error = %e, path = ?parent, "Failed to create parent directory");
                    Status::internal(format!("Failed to create parent directory: {}", e))
                })?;
            }

            // Create symlink to device
            tokio::fs::symlink(&device, target_path).await.map_err(|e| {
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
            if !platform::is_mounted(staging_target_path).await? {
                return Err(Status::failed_precondition(format!(
                    "Volume not staged at {}",
                    staging_target_path
                )));
            }

            // Check if already published
            if platform::is_mounted(target_path).await? {
                info!(target_path = %target_path, "Volume already published");
                return Ok(Response::new(csi::NodePublishVolumeResponse {}));
            }

            // Create bind mount from staging to target
            platform::bind_mount(staging_target_path, target_path).await?;

            // Handle readonly mount if requested
            if req.readonly {
                // Remount as read-only
                let output = Command::new("mount")
                    .args(["-o", "remount,ro", target_path])
                    .output()
                    .await
                    .map_err(|e| {
                        error!(error = %e, "Failed to remount as readonly");
                        Status::internal(format!("Failed to remount as readonly: {}", e))
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    error!(stderr = %stderr, target_path = %target_path, "Failed to set readonly mount");
                    // Unmount and fail - readonly was explicitly requested
                    if let Err(e) = platform::unmount(target_path).await {
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
        let is_block = tokio::fs::symlink_metadata(target)
            .await
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
            if let Err(e) = tokio::fs::remove_file(target_path).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                error!(error = %e, path = %target_path, "Failed to remove block device symlink");
                return Err(Status::internal(format!("Failed to remove symlink: {}", e)));
            }
            info!(volume_id = %volume_id, target_path = %target_path, "Block volume unpublished");
        } else {
            // Mount volume: unmount from target path
            platform::unmount(target_path).await?;

            // Try to remove the target directory
            if tokio::fs::try_exists(target).await.unwrap_or(false)
                && let Err(e) = tokio::fs::remove_dir(target_path).await
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
        let fs_type = Self::detect_filesystem_type(volume_path).await?;
        debug!(volume_id = %volume_id, fs_type = %fs_type, "Detected filesystem type");

        // Perform filesystem-specific resize
        let resized = Self::resize_filesystem(volume_path, &fs_type).await?;
        if resized {
            info!(volume_id = %volume_id, fs_type = %fs_type, "Filesystem resized successfully");
        } else {
            debug!(volume_id = %volume_id, fs_type = %fs_type, "Filesystem resize not needed or automatic");
        }

        // Get final capacity after resize
        let capacity_bytes = Self::get_volume_capacity(volume_path).await?;

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

    #[test]
    fn test_parse_endpoints_single_with_port() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("endpoints".to_string(), "192.168.1.1:3260".to_string());

        let endpoints = NodeService::parse_endpoints(&ctx, ExportType::Iscsi).unwrap();
        assert_eq!(endpoints.len(), 1);
        assert!(!endpoints.is_multipath());
        assert_eq!(endpoints.first().unwrap().host, "192.168.1.1");
        assert_eq!(endpoints.first().unwrap().port, 3260);
    }

    #[test]
    fn test_parse_endpoints_default_iscsi_port() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("endpoints".to_string(), "192.168.1.1".to_string());

        let endpoints = NodeService::parse_endpoints(&ctx, ExportType::Iscsi).unwrap();
        assert_eq!(endpoints.first().unwrap().host, "192.168.1.1");
        assert_eq!(endpoints.first().unwrap().port, 3260);
    }

    #[test]
    fn test_parse_endpoints_default_nvmeof_port() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("endpoints".to_string(), "192.168.1.1".to_string());

        let endpoints = NodeService::parse_endpoints(&ctx, ExportType::Nvmeof).unwrap();
        assert_eq!(endpoints.first().unwrap().host, "192.168.1.1");
        assert_eq!(endpoints.first().unwrap().port, 4420);
    }

    #[test]
    fn test_parse_endpoints_multipath() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert(
            "endpoints".to_string(),
            "192.168.1.1:3260,192.168.1.2:3260,192.168.1.3:3260".to_string(),
        );

        let endpoints = NodeService::parse_endpoints(&ctx, ExportType::Iscsi).unwrap();
        assert_eq!(endpoints.len(), 3);
        assert!(endpoints.is_multipath());
        assert_eq!(
            endpoints.to_portal_string(),
            "192.168.1.1:3260,192.168.1.2:3260,192.168.1.3:3260"
        );
    }

    #[test]
    fn test_parse_endpoints_missing() {
        use crate::types::ExportType;
        let ctx = std::collections::HashMap::new();
        assert!(NodeService::parse_endpoints(&ctx, ExportType::Iscsi).is_err());
    }

    #[test]
    fn test_parse_endpoints_empty() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("endpoints".to_string(), "".to_string());
        assert!(NodeService::parse_endpoints(&ctx, ExportType::Iscsi).is_err());
    }

    #[test]
    fn test_parse_endpoints_custom_port() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("endpoints".to_string(), "10.0.0.1:9999".to_string());

        let endpoints = NodeService::parse_endpoints(&ctx, ExportType::Nvmeof).unwrap();
        assert_eq!(endpoints.first().unwrap().host, "10.0.0.1");
        assert_eq!(endpoints.first().unwrap().port, 9999);
    }

    #[test]
    fn test_parse_endpoints_trims_whitespace() {
        use crate::types::ExportType;
        let mut ctx = std::collections::HashMap::new();
        ctx.insert("endpoints".to_string(), " 192.168.1.1:3260 ".to_string());

        let endpoints = NodeService::parse_endpoints(&ctx, ExportType::Iscsi).unwrap();
        assert_eq!(endpoints.first().unwrap().host, "192.168.1.1");
        assert_eq!(endpoints.first().unwrap().port, 3260);
    }

    #[test]
    fn test_extract_iscsi_chap_full_credentials() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "node.session.auth.username".to_string(),
            "test_user".to_string(),
        );
        secrets.insert(
            "node.session.auth.password".to_string(),
            "test_pass".to_string(),
        );
        secrets.insert(
            "node.session.auth.username_in".to_string(),
            "mutual_user".to_string(),
        );
        secrets.insert(
            "node.session.auth.password_in".to_string(),
            "mutual_pass".to_string(),
        );

        let creds = NodeService::extract_iscsi_chap(&secrets).unwrap();
        assert_eq!(creds.username, "test_user");
        assert_eq!(creds.password, "test_pass");
        assert_eq!(creds.mutual_username, Some("mutual_user".to_string()));
        assert_eq!(creds.mutual_password, Some("mutual_pass".to_string()));
    }

    #[test]
    fn test_extract_iscsi_chap_basic_only() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "node.session.auth.username".to_string(),
            "test_user".to_string(),
        );
        secrets.insert(
            "node.session.auth.password".to_string(),
            "test_pass".to_string(),
        );

        let creds = NodeService::extract_iscsi_chap(&secrets).unwrap();
        assert_eq!(creds.username, "test_user");
        assert_eq!(creds.password, "test_pass");
        assert!(creds.mutual_username.is_none());
        assert!(creds.mutual_password.is_none());
    }

    #[test]
    fn test_extract_iscsi_chap_missing_username() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "node.session.auth.password".to_string(),
            "test_pass".to_string(),
        );

        assert!(NodeService::extract_iscsi_chap(&secrets).is_none());
    }

    #[test]
    fn test_extract_iscsi_chap_missing_password() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "node.session.auth.username".to_string(),
            "test_user".to_string(),
        );

        assert!(NodeService::extract_iscsi_chap(&secrets).is_none());
    }

    #[test]
    fn test_extract_iscsi_chap_empty_username() {
        let mut secrets = HashMap::new();
        secrets.insert("node.session.auth.username".to_string(), "".to_string());
        secrets.insert(
            "node.session.auth.password".to_string(),
            "test_pass".to_string(),
        );

        assert!(NodeService::extract_iscsi_chap(&secrets).is_none());
    }

    #[test]
    fn test_extract_iscsi_chap_empty_mutual_ignored() {
        let mut secrets = HashMap::new();
        secrets.insert(
            "node.session.auth.username".to_string(),
            "test_user".to_string(),
        );
        secrets.insert(
            "node.session.auth.password".to_string(),
            "test_pass".to_string(),
        );
        // Empty mutual credentials should be ignored
        secrets.insert("node.session.auth.username_in".to_string(), "".to_string());
        secrets.insert("node.session.auth.password_in".to_string(), "".to_string());

        let creds = NodeService::extract_iscsi_chap(&secrets).unwrap();
        assert!(creds.mutual_username.is_none());
        assert!(creds.mutual_password.is_none());
    }

    #[test]
    fn test_extract_iscsi_chap_empty_secrets() {
        let secrets = HashMap::new();
        assert!(NodeService::extract_iscsi_chap(&secrets).is_none());
    }

    #[test]
    fn test_extract_nvme_auth_with_secret() {
        let mut secrets = HashMap::new();
        secrets.insert("nvme.auth.secret".to_string(), "DHHC-1:00:test-secret-key".to_string());

        let creds = NodeService::extract_nvme_auth(&secrets).unwrap();
        assert_eq!(creds.secret, "DHHC-1:00:test-secret-key");
        assert!(creds.ctrl_secret.is_none());
    }

    #[test]
    fn test_extract_nvme_auth_with_ctrl_secret() {
        let mut secrets = HashMap::new();
        secrets.insert("nvme.auth.secret".to_string(), "DHHC-1:00:host-secret".to_string());
        secrets.insert("nvme.auth.ctrl_secret".to_string(), "DHHC-1:00:ctrl-secret".to_string());

        let creds = NodeService::extract_nvme_auth(&secrets).unwrap();
        assert_eq!(creds.secret, "DHHC-1:00:host-secret");
        assert_eq!(creds.ctrl_secret, Some("DHHC-1:00:ctrl-secret".to_string()));
    }

    #[test]
    fn test_extract_nvme_auth_missing_secret() {
        let secrets = HashMap::new();
        assert!(NodeService::extract_nvme_auth(&secrets).is_none());
    }

    #[test]
    fn test_extract_nvme_auth_empty_secret() {
        let mut secrets = HashMap::new();
        secrets.insert("nvme.auth.secret".to_string(), "".to_string());
        assert!(NodeService::extract_nvme_auth(&secrets).is_none());
    }

    #[test]
    fn test_extract_nvme_auth_empty_ctrl_secret_ignored() {
        let mut secrets = HashMap::new();
        secrets.insert("nvme.auth.secret".to_string(), "DHHC-1:00:host-secret".to_string());
        secrets.insert("nvme.auth.ctrl_secret".to_string(), "".to_string());

        let creds = NodeService::extract_nvme_auth(&secrets).unwrap();
        assert_eq!(creds.secret, "DHHC-1:00:host-secret");
        assert!(creds.ctrl_secret.is_none());
    }
}
