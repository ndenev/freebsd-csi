//! FreeBSD platform implementation for CSI Node operations
//!
//! Uses FreeBSD-specific tools:
//! - iscsictl for iSCSI
//! - nvmecontrol for NVMeoF
//! - newfs for UFS formatting
//! - nullfs for bind mounts

use std::process::Command;

use tonic::Status;
use tracing::{debug, error, info, warn};

use super::PlatformResult;

/// Default filesystem type for FreeBSD
pub const DEFAULT_FS_TYPE: &str = "ufs";

/// Check if an iSCSI target is currently connected.
pub fn is_iscsi_connected(target_iqn: &str) -> bool {
    // Check iscsictl -L output for this target
    let output = Command::new("iscsictl").arg("-L").output();

    match output {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout.contains(target_iqn)
        }
        _ => false,
    }
}

/// Check if an NVMeoF target is currently connected.
pub fn is_nvmeof_connected(target_nqn: &str) -> bool {
    // Check nvmecontrol devlist output for this NQN
    let output = Command::new("nvmecontrol").arg("devlist").output();

    match output {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout.contains(target_nqn)
        }
        _ => false,
    }
}

/// Connect to an iSCSI target using iscsictl.
///
/// On FreeBSD, the portal is typically configured in /etc/iscsi.conf
/// or discovered automatically. The `portal` parameter is optional.
pub fn connect_iscsi(target_iqn: &str, _portal: Option<&str>) -> PlatformResult<String> {
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

    // After connecting, find the device
    let device = find_iscsi_device(target_iqn)?;
    info!(device = %device, "iSCSI target connected");

    Ok(device)
}

/// Find the device associated with an iSCSI target.
pub fn find_iscsi_device(target_iqn: &str) -> PlatformResult<String> {
    // Use iscsictl -L to list sessions and find the device
    let output = Command::new("iscsictl").arg("-L").output().map_err(|e| {
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
    for line in stdout.lines().rev() {
        if let Some(start) = line.find("(da")
            && let Some(end) = line[start..].find(',')
        {
            let device = &line[start + 1..start + end];
            return Ok(format!("/dev/{}", device));
        }
    }

    Err(Status::internal("Could not find device for iSCSI target"))
}

/// Disconnect from an iSCSI target.
#[allow(dead_code)] // Platform API for future use
pub fn disconnect_iscsi(target_iqn: &str) -> PlatformResult<()> {
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

/// Connect to an NVMeoF target using nvmecontrol.
pub fn connect_nvmeof(
    target_nqn: &str,
    _transport_addr: Option<&str>,
    _transport_port: Option<&str>,
) -> PlatformResult<String> {
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
    let device = find_nvmeof_device(target_nqn)?;
    info!(device = %device, "NVMeoF target connected");

    Ok(device)
}

/// Find the device associated with an NVMeoF target.
pub fn find_nvmeof_device(target_nqn: &str) -> PlatformResult<String> {
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
            if let Some(device) = line.split_whitespace().next()
                && (device.starts_with("nvme") || device.starts_with("nda"))
            {
                return Ok(format!("/dev/{}", device));
            }
        }
    }

    Err(Status::internal("Could not find device for NVMeoF target"))
}

/// Disconnect from an NVMeoF target.
#[allow(dead_code)] // Platform API for future use
pub fn disconnect_nvmeof(target_nqn: &str) -> PlatformResult<()> {
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
pub fn format_device(device: &str, fs_type: &str) -> PlatformResult<()> {
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
            debug!(device = %device, "Skipping format for ZFS (handled by ZFS tools)");
        }
        _ => {
            return Err(Status::invalid_argument(format!(
                "Unsupported filesystem type on FreeBSD: {}. Supported: ufs, ffs",
                fs_type
            )));
        }
    }

    Ok(())
}

/// Check if a device needs formatting (has no valid filesystem).
pub fn needs_formatting(device: &str) -> PlatformResult<bool> {
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
pub fn mount_device(device: &str, target: &str, fs_type: &str) -> PlatformResult<()> {
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

/// Create a nullfs mount (FreeBSD's equivalent to bind mount).
pub fn bind_mount(source: &str, target: &str) -> PlatformResult<()> {
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
pub fn is_mounted(target: &str) -> PlatformResult<bool> {
    let output = Command::new("mount").output().map_err(|e| {
        error!(error = %e, "Failed to execute mount");
        Status::internal(format!("Failed to check mounts: {}", e))
    })?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Check if target path appears in mount output
    Ok(stdout.lines().any(|line| line.contains(target)))
}

/// Validate filesystem type for FreeBSD.
pub fn validate_fs_type(fs_type: &str) -> PlatformResult<&'static str> {
    match fs_type.to_lowercase().as_str() {
        "ufs" | "ffs" | "" => Ok("ufs"),
        "zfs" => Err(Status::invalid_argument(
            "ZFS cannot be used as fsType for block volumes (ZFS manages its own storage)",
        )),
        "ext4" | "xfs" => Err(Status::invalid_argument(
            "ext4/xfs are not supported on FreeBSD. Use 'ufs' instead",
        )),
        _ => Err(Status::invalid_argument(format!(
            "Unsupported filesystem on FreeBSD: {}. Supported: ufs",
            fs_type
        ))),
    }
}

/// Get the default filesystem type for FreeBSD.
#[allow(dead_code)] // Platform API for future use
pub fn default_fs_type() -> &'static str {
    DEFAULT_FS_TYPE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_fs_type_valid() {
        assert_eq!(validate_fs_type("ufs").unwrap(), "ufs");
        assert_eq!(validate_fs_type("ffs").unwrap(), "ufs");
        assert_eq!(validate_fs_type("").unwrap(), "ufs");
        assert_eq!(validate_fs_type("UFS").unwrap(), "ufs");
    }

    #[test]
    fn test_validate_fs_type_invalid() {
        assert!(validate_fs_type("ext4").is_err());
        assert!(validate_fs_type("xfs").is_err());
        assert!(validate_fs_type("zfs").is_err());
        assert!(validate_fs_type("ntfs").is_err());
    }

    #[test]
    fn test_default_fs_type() {
        assert_eq!(default_fs_type(), "ufs");
    }
}
