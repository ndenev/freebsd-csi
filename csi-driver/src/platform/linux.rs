//! Linux platform implementation for CSI Node operations
//!
//! Uses Linux-specific tools:
//! - iscsiadm for iSCSI (open-iscsi)
//! - nvme for NVMeoF (nvme-cli)
//! - mkfs.ext4/mkfs.xfs for filesystem formatting
//! - mount --bind for bind mounts

use std::fs;
use std::path::Path;
use std::process::Command;

use tonic::Status;
use tracing::{debug, error, info, warn};

use super::PlatformResult;

/// Default filesystem type for Linux
pub const DEFAULT_FS_TYPE: &str = "ext4";

/// Connect to an iSCSI target using iscsiadm.
///
/// On Linux, we need the portal address. If not provided, we try to discover
/// the target first using sendtargets.
pub fn connect_iscsi(target_iqn: &str, portal: Option<&str>) -> PlatformResult<String> {
    info!(target_iqn = %target_iqn, portal = ?portal, "Connecting to iSCSI target");

    let portal = portal.ok_or_else(|| {
        Status::invalid_argument(
            "Portal address is required for iSCSI on Linux (pass via volume_context)",
        )
    })?;

    // First, discover the target if not already known
    let discover_output = Command::new("iscsiadm")
        .args([
            "-m",
            "discoverydb",
            "-t",
            "sendtargets",
            "-p",
            portal,
            "--discover",
        ])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute iscsiadm discovery");
            Status::internal(format!("Failed to execute iscsiadm discovery: {}", e))
        })?;

    if !discover_output.status.success() {
        let stderr = String::from_utf8_lossy(&discover_output.stderr);
        // Discovery failure is not always fatal - target may already be known
        warn!(stderr = %stderr, "iscsiadm discovery returned error (may be expected if target already known)");
    }

    // Create a node entry for the target
    let _node_output = Command::new("iscsiadm")
        .args(["-m", "node", "-T", target_iqn, "-p", portal, "-o", "new"])
        .output();
    // Ignore error - node may already exist

    // Log in to the target
    let login_output = Command::new("iscsiadm")
        .args(["-m", "node", "-T", target_iqn, "-p", portal, "--login"])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute iscsiadm login");
            Status::internal(format!("Failed to execute iscsiadm login: {}", e))
        })?;

    if !login_output.status.success() {
        let stderr = String::from_utf8_lossy(&login_output.stderr);
        // Check if already logged in
        if stderr.contains("already present") || stderr.contains("session already exists") {
            info!(target_iqn = %target_iqn, "iSCSI session already exists");
        } else {
            error!(stderr = %stderr, "iscsiadm login failed");
            return Err(Status::internal(format!(
                "iscsiadm login failed: {}",
                stderr
            )));
        }
    }

    // Wait a moment for the device to appear
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Find the device
    let device = find_iscsi_device(target_iqn)?;
    info!(device = %device, "iSCSI target connected");

    Ok(device)
}

/// Find the device associated with an iSCSI target.
///
/// Linux provides stable device paths in /dev/disk/by-path/ for iSCSI devices.
pub fn find_iscsi_device(target_iqn: &str) -> PlatformResult<String> {
    // Try to find device via /dev/disk/by-path/ which has stable iSCSI paths
    let by_path = Path::new("/dev/disk/by-path");
    if by_path.exists()
        && let Ok(entries) = fs::read_dir(by_path)
    {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // iSCSI paths look like: ip-<ip>:<port>-iscsi-<iqn>-lun-<lun>
            if name_str.contains("iscsi")
                && name_str.contains(target_iqn)
                && let Ok(link_target) = fs::canonicalize(entry.path())
            {
                return Ok(link_target.to_string_lossy().to_string());
            }
        }
    }

    // Fallback: Query iscsiadm for session info
    let output = Command::new("iscsiadm")
        .args(["-m", "session", "-P", "3"])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute iscsiadm session query");
            Status::internal(format!("Failed to list iSCSI sessions: {}", e))
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut found_target = false;

    // Parse the detailed session output
    for line in stdout.lines() {
        if line.contains(target_iqn) {
            found_target = true;
        }
        if found_target
            && line.contains("Attached scsi disk")
            && let Some(device) = line.split_whitespace().nth(3)
            && device.starts_with("sd")
        {
            // Format: "Attached scsi disk sda ..."
            return Ok(format!("/dev/{}", device));
        }
    }

    // Try /sys/class/iscsi_session approach
    let iscsi_sessions = Path::new("/sys/class/iscsi_session");
    if iscsi_sessions.exists()
        && let Ok(entries) = fs::read_dir(iscsi_sessions)
    {
        for entry in entries.flatten() {
            let target_path = entry.path().join("targetname");
            if let Ok(targetname) = fs::read_to_string(&target_path)
                && targetname.trim() == target_iqn
            {
                // Found the session, now find the device
                let session_name = entry.file_name();
                let _device_path = Path::new("/sys/class/iscsi_session")
                    .join(&session_name)
                    .join("device/target*/*/block/*");

                // Use glob-like search for device
                let session_path = entry.path().join("device");
                if let Ok(device_entries) = fs::read_dir(&session_path) {
                    for dev_entry in device_entries.flatten() {
                        let name = dev_entry.file_name();
                        if name.to_string_lossy().starts_with("target")
                            && let Ok(target_contents) = fs::read_dir(dev_entry.path())
                        {
                            // Look for block devices under this target
                            for scsi_entry in target_contents.flatten() {
                                let block_path = scsi_entry.path().join("block");
                                if block_path.exists()
                                    && let Ok(block_entries) = fs::read_dir(&block_path)
                                {
                                    for block_entry in block_entries.flatten() {
                                        let dev_name = block_entry.file_name();
                                        return Ok(format!("/dev/{}", dev_name.to_string_lossy()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Err(Status::internal(
        "Could not find device for iSCSI target. Ensure the iSCSI initiator tools are installed and the session is established.",
    ))
}

/// Disconnect from an iSCSI target.
pub fn disconnect_iscsi(target_iqn: &str) -> PlatformResult<()> {
    info!(target_iqn = %target_iqn, "Disconnecting from iSCSI target");

    let output = Command::new("iscsiadm")
        .args(["-m", "node", "-T", target_iqn, "--logout"])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute iscsiadm logout");
            Status::internal(format!("Failed to execute iscsiadm: {}", e))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Treat "not logged in" as success
        if stderr.contains("No matching sessions") || stderr.contains("not logged in") {
            warn!(target_iqn = %target_iqn, "iSCSI target was not logged in");
            return Ok(());
        }
        error!(stderr = %stderr, "iscsiadm logout failed");
        return Err(Status::internal(format!(
            "iscsiadm logout failed: {}",
            stderr
        )));
    }

    Ok(())
}

/// Connect to an NVMeoF target using nvme-cli.
pub fn connect_nvmeof(
    target_nqn: &str,
    transport_addr: Option<&str>,
    transport_port: Option<&str>,
) -> PlatformResult<String> {
    info!(target_nqn = %target_nqn, transport_addr = ?transport_addr, "Connecting to NVMeoF target");

    let addr = transport_addr.ok_or_else(|| {
        Status::invalid_argument(
            "Transport address is required for NVMeoF on Linux (pass via volume_context)",
        )
    })?;

    let port = transport_port.unwrap_or("4420"); // Default NVMe-oF port

    // Connect using nvme-cli
    let output = Command::new("nvme")
        .args([
            "connect", "-t", "tcp", "-a", addr, "-s", port, "-n", target_nqn,
        ])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute nvme connect");
            Status::internal(format!("Failed to execute nvme connect: {}", e))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Check if already connected
        if stderr.contains("already connected") {
            info!(target_nqn = %target_nqn, "NVMeoF target already connected");
        } else {
            error!(stderr = %stderr, "nvme connect failed");
            return Err(Status::internal(format!("nvme connect failed: {}", stderr)));
        }
    }

    // Wait a moment for the device to appear
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Find the device
    let device = find_nvmeof_device(target_nqn)?;
    info!(device = %device, "NVMeoF target connected");

    Ok(device)
}

/// Find the device associated with an NVMeoF target.
pub fn find_nvmeof_device(target_nqn: &str) -> PlatformResult<String> {
    // Use nvme list to find devices
    let output = Command::new("nvme")
        .args(["list", "-o", "json"])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute nvme list");
            Status::internal(format!("Failed to list NVMe devices: {}", e))
        })?;

    // Try to parse JSON output
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Simple parsing - look for device paths
    // In production, we'd use serde_json to properly parse
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&stdout)
        && let Some(devices) = json.get("Devices").and_then(|d| d.as_array())
    {
        for device in devices {
            if let Some(dev_path) = device.get("DevicePath").and_then(|p| p.as_str()) {
                // Check if this device is associated with our NQN
                // The NQN might be in the SubsystemNQN or ModelNumber field
                let subsys_nqn = device
                    .get("SubsystemNQN")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                if subsys_nqn == target_nqn || subsys_nqn.contains(target_nqn) {
                    return Ok(dev_path.to_string());
                }
            }
        }
    }

    // Fallback: Check /sys/class/nvme-fabrics/
    let nvme_subsys = Path::new("/sys/class/nvme-subsystem");
    if nvme_subsys.exists()
        && let Ok(entries) = fs::read_dir(nvme_subsys)
    {
        for entry in entries.flatten() {
            let nqn_path = entry.path().join("subsysnqn");
            if let Ok(nqn) = fs::read_to_string(&nqn_path)
                && nqn.trim() == target_nqn
            {
                // Found the subsystem, now find the namespace device
                if let Ok(ns_entries) = fs::read_dir(entry.path()) {
                    for ns_entry in ns_entries.flatten() {
                        let name = ns_entry.file_name();
                        let name_str = name.to_string_lossy();
                        if name_str.starts_with("nvme") && name_str.contains("n") {
                            return Ok(format!("/dev/{}", name_str));
                        }
                    }
                }
            }
        }
    }

    // Last resort: find most recent nvme device
    let output = Command::new("nvme").arg("list").output().map_err(|e| {
        error!(error = %e, "Failed to execute nvme list");
        Status::internal(format!("Failed to list NVMe devices: {}", e))
    })?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.starts_with("/dev/nvme")
            && let Some(device) = line.split_whitespace().next()
        {
            return Ok(device.to_string());
        }
    }

    Err(Status::internal(
        "Could not find device for NVMeoF target. Ensure nvme-cli is installed and the connection succeeded.",
    ))
}

/// Disconnect from an NVMeoF target.
pub fn disconnect_nvmeof(target_nqn: &str) -> PlatformResult<()> {
    info!(target_nqn = %target_nqn, "Disconnecting from NVMeoF target");

    let output = Command::new("nvme")
        .args(["disconnect", "-n", target_nqn])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute nvme disconnect");
            Status::internal(format!("Failed to execute nvme disconnect: {}", e))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Treat "not found" as success
        if stderr.contains("not found") || stderr.contains("No subsystem") {
            warn!(target_nqn = %target_nqn, "NVMeoF target was not connected");
            return Ok(());
        }
        error!(stderr = %stderr, "nvme disconnect failed");
        return Err(Status::internal(format!(
            "nvme disconnect failed: {}",
            stderr
        )));
    }

    Ok(())
}

/// Format a device with the specified filesystem type.
pub fn format_device(device: &str, fs_type: &str) -> PlatformResult<()> {
    info!(device = %device, fs_type = %fs_type, "Formatting device");

    match fs_type.to_lowercase().as_str() {
        "ext4" => {
            let output = Command::new("mkfs.ext4")
                .args(["-F", device]) // -F to force (don't prompt)
                .output()
                .map_err(|e| {
                    error!(error = %e, "Failed to execute mkfs.ext4");
                    Status::internal(format!("Failed to execute mkfs.ext4: {}", e))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                error!(stderr = %stderr, "mkfs.ext4 failed");
                return Err(Status::internal(format!("mkfs.ext4 failed: {}", stderr)));
            }
        }
        "xfs" => {
            let output = Command::new("mkfs.xfs")
                .args(["-f", device]) // -f to force
                .output()
                .map_err(|e| {
                    error!(error = %e, "Failed to execute mkfs.xfs");
                    Status::internal(format!("Failed to execute mkfs.xfs: {}", e))
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                error!(stderr = %stderr, "mkfs.xfs failed");
                return Err(Status::internal(format!("mkfs.xfs failed: {}", stderr)));
            }
        }
        "zfs" => {
            // ZFS handles formatting automatically
            debug!(device = %device, "Skipping format for ZFS (handled by ZFS tools)");
        }
        "ufs" | "ffs" => {
            return Err(Status::invalid_argument(
                "UFS/FFS are not supported on Linux. Use 'ext4' or 'xfs' instead",
            ));
        }
        _ => {
            return Err(Status::invalid_argument(format!(
                "Unsupported filesystem type on Linux: {}. Supported: ext4, xfs",
                fs_type
            )));
        }
    }

    Ok(())
}

/// Check if a device needs formatting (has no valid filesystem).
pub fn needs_formatting(device: &str) -> PlatformResult<bool> {
    // Use blkid to check for existing filesystem
    let output = Command::new("blkid")
        .args(["-p", device])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute blkid");
            Status::internal(format!("Failed to check device filesystem: {}", e))
        })?;

    // blkid returns non-zero if no filesystem found
    if !output.status.success() {
        return Ok(true); // No filesystem, needs formatting
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // If output contains TYPE=, there's a filesystem
    Ok(!stdout.contains("TYPE="))
}

/// Mount a device to a target path.
pub fn mount_device(device: &str, target: &str, fs_type: &str) -> PlatformResult<()> {
    info!(device = %device, target = %target, fs_type = %fs_type, "Mounting device");

    // Ensure target directory exists
    std::fs::create_dir_all(target).map_err(|e| {
        error!(error = %e, "Failed to create mount target directory");
        Status::internal(format!("Failed to create mount directory: {}", e))
    })?;

    let fs_type_lower = fs_type.to_lowercase();

    let output = Command::new("mount")
        .args(["-t", &fs_type_lower, device, target])
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

/// Create a bind mount.
pub fn bind_mount(source: &str, target: &str) -> PlatformResult<()> {
    info!(source = %source, target = %target, "Creating bind mount");

    // Ensure target directory exists
    std::fs::create_dir_all(target).map_err(|e| {
        error!(error = %e, "Failed to create bind mount target directory");
        Status::internal(format!(
            "Failed to create bind mount target directory: {}",
            e
        ))
    })?;

    let output = Command::new("mount")
        .args(["--bind", source, target])
        .output()
        .map_err(|e| {
            error!(error = %e, "Failed to execute mount --bind");
            Status::internal(format!("Failed to execute bind mount: {}", e))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        error!(stderr = %stderr, "bind mount failed");
        return Err(Status::internal(format!("bind mount failed: {}", stderr)));
    }

    Ok(())
}

/// Unmount a path.
pub fn unmount(target: &str) -> PlatformResult<()> {
    info!(target = %target, "Unmounting");

    // Check if path is actually mounted
    if !is_mounted(target)? {
        debug!(target = %target, "Path is not mounted, skipping unmount");
        return Ok(());
    }

    let output = Command::new("umount").arg(target).output().map_err(|e| {
        error!(error = %e, "Failed to execute umount");
        Status::internal(format!("Failed to execute umount: {}", e))
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Treat "not mounted" as success
        if stderr.contains("not mounted") || stderr.contains("no mount point") {
            warn!(target = %target, "Path was not mounted");
            return Ok(());
        }
        error!(stderr = %stderr, "umount failed");
        return Err(Status::internal(format!("umount failed: {}", stderr)));
    }

    Ok(())
}

/// Check if a path is currently mounted.
pub fn is_mounted(target: &str) -> PlatformResult<bool> {
    // On Linux, check /proc/mounts for efficiency
    if let Ok(mounts) = fs::read_to_string("/proc/mounts") {
        return Ok(mounts
            .lines()
            .any(|line| line.split_whitespace().nth(1) == Some(target)));
    }

    // Fallback to mount command
    let output = Command::new("mount").output().map_err(|e| {
        error!(error = %e, "Failed to execute mount");
        Status::internal(format!("Failed to check mounts: {}", e))
    })?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().any(|line| line.contains(target)))
}

/// Validate filesystem type for Linux.
pub fn validate_fs_type(fs_type: &str) -> PlatformResult<&'static str> {
    match fs_type.to_lowercase().as_str() {
        "ext4" | "" => Ok("ext4"),
        "xfs" => Ok("xfs"),
        "zfs" => Err(Status::invalid_argument(
            "ZFS cannot be used as fsType for block volumes (ZFS manages its own storage)",
        )),
        "ufs" | "ffs" => Err(Status::invalid_argument(
            "UFS/FFS are not supported on Linux. Use 'ext4' or 'xfs' instead",
        )),
        _ => Err(Status::invalid_argument(format!(
            "Unsupported filesystem on Linux: {}. Supported: ext4, xfs",
            fs_type
        ))),
    }
}

/// Get the default filesystem type for Linux.
pub fn default_fs_type() -> &'static str {
    DEFAULT_FS_TYPE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_fs_type_valid() {
        assert_eq!(validate_fs_type("ext4").unwrap(), "ext4");
        assert_eq!(validate_fs_type("xfs").unwrap(), "xfs");
        assert_eq!(validate_fs_type("").unwrap(), "ext4");
        assert_eq!(validate_fs_type("EXT4").unwrap(), "ext4");
    }

    #[test]
    fn test_validate_fs_type_invalid() {
        assert!(validate_fs_type("ufs").is_err());
        assert!(validate_fs_type("ffs").is_err());
        assert!(validate_fs_type("zfs").is_err());
        assert!(validate_fs_type("ntfs").is_err());
    }

    #[test]
    fn test_default_fs_type() {
        assert_eq!(default_fs_type(), "ext4");
    }
}
