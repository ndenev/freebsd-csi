//! Linux platform implementation for CSI Node operations
//!
//! Uses Linux-specific tools:
//! - iscsiadm for iSCSI (open-iscsi)
//! - nvme for NVMeoF (nvme-cli)
//! - mkfs.ext4/mkfs.xfs for filesystem formatting
//! - mount --bind for bind mounts

use std::path::Path;

use tokio::process::Command;
use tonic::Status;
use tracing::{debug, error, info, warn};

use super::PlatformResult;
use crate::types::Endpoint;

/// Default filesystem type for Linux
pub const DEFAULT_FS_TYPE: &str = "ext4";

/// Check if an iSCSI target is currently connected.
pub async fn is_iscsi_connected(target_iqn: &str) -> bool {
    // Check iscsiadm session list for this target
    let output = Command::new("iscsiadm")
        .args(["-m", "session"])
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout.contains(target_iqn)
        }
        _ => false,
    }
}

/// Check if an NVMeoF target is currently connected.
pub async fn is_nvmeof_connected(target_nqn: &str) -> bool {
    // Check /sys/class/nvme-subsystem/ for this NQN
    let nvme_subsys = Path::new("/sys/class/nvme-subsystem");
    if !tokio::fs::try_exists(nvme_subsys).await.unwrap_or(false) {
        return false;
    }

    if let Ok(mut entries) = tokio::fs::read_dir(nvme_subsys).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let nqn_path = entry.path().join("subsysnqn");
            if let Ok(nqn) = tokio::fs::read_to_string(&nqn_path).await
                && nqn.trim() == target_nqn
            {
                return true;
            }
        }
    }
    false
}

/// Check if a device is claimed by multipath and return the multipath device path.
///
/// This checks if the raw device (e.g., /dev/sda, /dev/nvme0n1) is a slave
/// of a device-mapper multipath device and returns the dm device path instead.
///
/// Returns the original device if not multipathed, or the dm device path if it is.
async fn resolve_multipath_device(device: &str) -> String {
    // Extract device name from path (e.g., "/dev/sda" -> "sda")
    let dev_name = device.rsplit('/').next().unwrap_or(device);

    // Check /sys/block/<device>/holders/ for dm-* entries
    let holders_path = format!("/sys/block/{}/holders", dev_name);
    if let Ok(mut entries) = tokio::fs::read_dir(&holders_path).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let holder_name = entry.file_name();
            let holder_str = holder_name.to_string_lossy();
            if holder_str.starts_with("dm-") {
                // Found a dm device holding this device
                // Try to find the friendly name in /dev/mapper/
                let dm_device = format!("/dev/{}", holder_str);

                // Check if there's a symlink in /dev/mapper pointing to this dm device
                if let Ok(mut mapper_entries) = tokio::fs::read_dir("/dev/mapper").await {
                    while let Ok(Some(mapper_entry)) = mapper_entries.next_entry().await {
                        if let Ok(link_target) = tokio::fs::read_link(mapper_entry.path()).await {
                            let link_target_str = link_target.to_string_lossy();
                            if link_target_str.ends_with(&*holder_str)
                                || link_target_str.contains(&*holder_str)
                            {
                                let mapper_path = mapper_entry.path();
                                info!(
                                    original = %device,
                                    multipath = %mapper_path.display(),
                                    "Device is multipathed, using dm device"
                                );
                                return mapper_path.to_string_lossy().to_string();
                            }
                        }
                    }
                }

                // Fall back to dm device if no mapper name found
                info!(
                    original = %device,
                    multipath = %dm_device,
                    "Device is multipathed, using dm device"
                );
                return dm_device;
            }
        }
    }

    // Not multipathed, return original device
    device.to_string()
}

/// Check if NVMe native multipath is enabled.
///
/// Returns true if the kernel's nvme_core module has multipath enabled,
/// which means NVMe devices are handled by kernel multipath instead of dm-multipath.
async fn is_nvme_native_multipath_enabled() -> bool {
    let multipath_path = "/sys/module/nvme_core/parameters/multipath";
    if let Ok(value) = tokio::fs::read_to_string(multipath_path).await {
        let v = value.trim();
        v == "Y" || v == "1"
    } else {
        false
    }
}

/// Connect to an iSCSI target using iscsiadm with support for multiple portals.
///
/// When multiple endpoints are provided, this function will:
/// 1. Run sendtargets discovery against each portal
/// 2. Login to the target via each portal
/// 3. Wait for dm-multipath to combine the paths
/// 4. Return the multipath device (or single device if only one portal)
///
/// # Arguments
/// * `target_iqn` - The iSCSI Qualified Name of the target
/// * `endpoints` - One or more endpoints (host:port pairs) for multipath support
pub async fn connect_iscsi(target_iqn: &str, endpoints: &[Endpoint]) -> PlatformResult<String> {
    if endpoints.is_empty() {
        return Err(Status::invalid_argument(
            "At least one endpoint is required for iSCSI connection",
        ));
    }

    let multipath_mode = endpoints.len() > 1;

    info!(
        target_iqn = %target_iqn,
        endpoints = ?endpoints.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        multipath = multipath_mode,
        "Connecting to iSCSI target"
    );

    // Track successful logins for multipath
    let mut successful_logins = 0;

    // Step 1 & 2: Discover and login to each portal
    for endpoint in endpoints {
        let portal = endpoint.to_portal_string();

        // Run sendtargets discovery to populate node database
        let discover_output = Command::new("iscsiadm")
            .args(["-m", "discovery", "-t", "sendtargets", "-p", &portal])
            .output()
            .await
            .map_err(|e| {
                error!(error = %e, portal = %portal, "Failed to execute iscsiadm discovery");
                Status::internal(format!("Failed to execute iscsiadm discovery: {}", e))
            })?;

        if !discover_output.status.success() {
            let stderr = String::from_utf8_lossy(&discover_output.stderr);
            let stdout = String::from_utf8_lossy(&discover_output.stdout);
            warn!(
                stderr = %stderr,
                stdout = %stdout,
                portal = %portal,
                "iscsiadm discovery returned error (may be expected if target already known)"
            );
        } else {
            let stdout = String::from_utf8_lossy(&discover_output.stdout);
            info!(output = %stdout, portal = %portal, "iSCSI discovery successful");
        }

        // Login to the target via this portal
        let login_output = Command::new("iscsiadm")
            .args(["-m", "node", "-T", target_iqn, "-p", &portal, "--login"])
            .output()
            .await
            .map_err(|e| {
                error!(error = %e, portal = %portal, "Failed to execute iscsiadm login");
                Status::internal(format!("Failed to execute iscsiadm login: {}", e))
            })?;

        if !login_output.status.success() {
            let stderr = String::from_utf8_lossy(&login_output.stderr);
            // Check if already logged in
            if stderr.contains("already present") || stderr.contains("session already exists") {
                info!(target_iqn = %target_iqn, portal = %portal, "iSCSI session already exists");
                successful_logins += 1;
            } else {
                // In multipath mode, warn but continue; in single mode, fail
                if multipath_mode {
                    warn!(
                        stderr = %stderr,
                        portal = %portal,
                        "iscsiadm login failed for portal (continuing with other portals)"
                    );
                } else {
                    error!(stderr = %stderr, "iscsiadm login failed");
                    return Err(Status::internal(format!(
                        "iscsiadm login failed: {}",
                        stderr
                    )));
                }
            }
        } else {
            info!(target_iqn = %target_iqn, portal = %portal, "iSCSI login successful");
            successful_logins += 1;
        }
    }

    // Ensure at least one login succeeded
    if successful_logins == 0 {
        return Err(Status::internal(
            "Failed to login to any iSCSI portal".to_string(),
        ));
    }

    // Step 3: Wait for devices to appear and multipath to settle
    // Longer wait for multipath to allow dm-multipath to combine paths
    let settle_time = if multipath_mode { 3000 } else { 1000 };
    info!(
        settle_time_ms = settle_time,
        successful_logins = successful_logins,
        "Waiting for device(s) to settle"
    );
    tokio::time::sleep(std::time::Duration::from_millis(settle_time)).await;

    // Step 4: Find the device (with multipath awareness)
    let device = find_iscsi_device(target_iqn).await?;
    info!(
        device = %device,
        multipath = multipath_mode,
        paths = successful_logins,
        "iSCSI target connected"
    );

    Ok(device)
}

/// Find the device associated with an iSCSI target.
///
/// Linux provides stable device paths in /dev/disk/by-path/ for iSCSI devices.
/// This function also checks if the device is claimed by multipath and returns
/// the dm device path in that case.
pub async fn find_iscsi_device(target_iqn: &str) -> PlatformResult<String> {
    // Try to find device via /dev/disk/by-path/ which has stable iSCSI paths
    let by_path = Path::new("/dev/disk/by-path");
    if tokio::fs::try_exists(by_path).await.unwrap_or(false)
        && let Ok(mut entries) = tokio::fs::read_dir(by_path).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // iSCSI paths look like: ip-<ip>:<port>-iscsi-<iqn>-lun-<lun>
            if name_str.contains("iscsi")
                && name_str.contains(target_iqn)
                && let Ok(link_target) = tokio::fs::canonicalize(entry.path()).await
            {
                let raw_device = link_target.to_string_lossy().to_string();
                // Check if device is multipathed and return dm device if so
                return Ok(resolve_multipath_device(&raw_device).await);
            }
        }
    }

    // Fallback: Query iscsiadm for session info
    let output = Command::new("iscsiadm")
        .args(["-m", "session", "-P", "3"])
        .output()
        .await
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
            let raw_device = format!("/dev/{}", device);
            return Ok(resolve_multipath_device(&raw_device).await);
        }
    }

    // Try /sys/class/iscsi_session approach
    let iscsi_sessions = Path::new("/sys/class/iscsi_session");
    if tokio::fs::try_exists(iscsi_sessions).await.unwrap_or(false)
        && let Ok(mut entries) = tokio::fs::read_dir(iscsi_sessions).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let target_path = entry.path().join("targetname");
            if let Ok(targetname) = tokio::fs::read_to_string(&target_path).await
                && targetname.trim() == target_iqn
            {
                // Found the session, now find the device
                let session_path = entry.path().join("device");
                if let Ok(mut device_entries) = tokio::fs::read_dir(&session_path).await {
                    while let Ok(Some(dev_entry)) = device_entries.next_entry().await {
                        let name = dev_entry.file_name();
                        if name.to_string_lossy().starts_with("target")
                            && let Ok(mut target_contents) =
                                tokio::fs::read_dir(dev_entry.path()).await
                        {
                            // Look for block devices under this target
                            while let Ok(Some(scsi_entry)) = target_contents.next_entry().await {
                                let block_path = scsi_entry.path().join("block");
                                if tokio::fs::try_exists(&block_path).await.unwrap_or(false)
                                    && let Ok(mut block_entries) =
                                        tokio::fs::read_dir(&block_path).await
                                    && let Ok(Some(block_entry)) = block_entries.next_entry().await
                                {
                                    let dev_name = block_entry.file_name();
                                    let raw_device = format!("/dev/{}", dev_name.to_string_lossy());
                                    return Ok(resolve_multipath_device(&raw_device).await);
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

/// Disconnect from an iSCSI target and clean up node database entries.
pub async fn disconnect_iscsi(target_iqn: &str) -> PlatformResult<()> {
    info!(target_iqn = %target_iqn, "Disconnecting from iSCSI target");

    // Step 1: Logout from the target
    let output = Command::new("iscsiadm")
        .args(["-m", "node", "-T", target_iqn, "--logout"])
        .output()
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to execute iscsiadm logout");
            Status::internal(format!("Failed to execute iscsiadm: {}", e))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Treat "not logged in" as success - continue to cleanup
        if !stderr.contains("No matching sessions") && !stderr.contains("not logged in") {
            error!(stderr = %stderr, "iscsiadm logout failed");
            return Err(Status::internal(format!(
                "iscsiadm logout failed: {}",
                stderr
            )));
        }
        debug!(target_iqn = %target_iqn, "iSCSI target was not logged in, continuing to cleanup");
    }

    // Step 2: Delete the node database entry to clean up /etc/iscsi/nodes/
    let delete_output = Command::new("iscsiadm")
        .args(["-m", "node", "-T", target_iqn, "-o", "delete"])
        .output()
        .await;

    match delete_output {
        Ok(output) if !output.status.success() => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ignore "no records found" - target may not exist in database
            if !stderr.contains("no records found") && !stderr.contains("No records found") {
                warn!(
                    stderr = %stderr,
                    target_iqn = %target_iqn,
                    "Failed to delete iSCSI node entry (non-fatal)"
                );
            }
        }
        Ok(_) => {
            info!(target_iqn = %target_iqn, "Deleted iSCSI node database entry");
        }
        Err(e) => {
            warn!(
                error = %e,
                target_iqn = %target_iqn,
                "Failed to execute iscsiadm delete (non-fatal)"
            );
        }
    }

    Ok(())
}

/// Connect to an NVMeoF target using nvme-cli with support for multiple endpoints.
///
/// When multiple endpoints are provided, this function will:
/// 1. Connect to each endpoint (each with its own host:port)
/// 2. Wait for multipath to combine the paths (native NVMe multipath or dm-multipath)
/// 3. Return the multipath device (or single device if only one endpoint)
///
/// # Arguments
/// * `target_nqn` - The NVMe Qualified Name of the target
/// * `endpoints` - One or more endpoints (host:port pairs) for multipath support
pub async fn connect_nvmeof(target_nqn: &str, endpoints: &[Endpoint]) -> PlatformResult<String> {
    if endpoints.is_empty() {
        return Err(Status::invalid_argument(
            "At least one endpoint is required for NVMeoF connection",
        ));
    }

    let multipath_mode = endpoints.len() > 1;

    info!(
        target_nqn = %target_nqn,
        endpoints = ?endpoints.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        multipath = multipath_mode,
        "Connecting to NVMeoF target"
    );

    // Track successful connections
    let mut successful_connects = 0;

    // Connect to each endpoint (each with its own host:port)
    for endpoint in endpoints {
        let addr = &endpoint.host;
        let port = endpoint.port.to_string();

        let output = Command::new("nvme")
            .args([
                "connect", "-t", "tcp", "-a", addr, "-s", &port, "-n", target_nqn,
            ])
            .output()
            .await
            .map_err(|e| {
                error!(error = %e, endpoint = %endpoint, "Failed to execute nvme connect");
                Status::internal(format!("Failed to execute nvme connect: {}", e))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Check if already connected
            if stderr.contains("already connected") {
                info!(target_nqn = %target_nqn, endpoint = %endpoint, "NVMeoF target already connected");
                successful_connects += 1;
            } else {
                // In multipath mode, warn but continue; in single mode, fail
                if multipath_mode {
                    warn!(
                        stderr = %stderr,
                        endpoint = %endpoint,
                        "nvme connect failed for endpoint (continuing with other endpoints)"
                    );
                } else {
                    error!(stderr = %stderr, "nvme connect failed");
                    return Err(Status::internal(format!("nvme connect failed: {}", stderr)));
                }
            }
        } else {
            info!(target_nqn = %target_nqn, endpoint = %endpoint, "NVMeoF connect successful");
            successful_connects += 1;
        }
    }

    // Ensure at least one connection succeeded
    if successful_connects == 0 {
        return Err(Status::internal(
            "Failed to connect to any NVMeoF endpoint".to_string(),
        ));
    }

    // Wait for devices to appear and multipath to settle
    // Longer wait for multipath to allow kernel/dm to combine paths
    let settle_time = if multipath_mode { 3000 } else { 1000 };
    info!(
        settle_time_ms = settle_time,
        successful_connects = successful_connects,
        "Waiting for device(s) to settle"
    );
    tokio::time::sleep(std::time::Duration::from_millis(settle_time)).await;

    // Find the device (with multipath awareness)
    let device = find_nvmeof_device(target_nqn).await?;
    info!(
        device = %device,
        multipath = multipath_mode,
        paths = successful_connects,
        "NVMeoF target connected"
    );

    Ok(device)
}

/// Check if a device path is an NVMe namespace device (nvmeXnY) not just a controller (nvmeX).
fn is_nvme_namespace_device(path: &str) -> bool {
    // Extract device name from path (e.g., "/dev/nvme0n1" -> "nvme0n1")
    let name = path.rsplit('/').next().unwrap_or(path);
    // Pattern: nvme followed by digits, then 'n', then more digits
    // e.g., nvme0n1, nvme1n2, etc.
    let mut chars = name.chars().peekable();

    // Must start with "nvme"
    if !name.starts_with("nvme") {
        return false;
    }

    // Skip "nvme"
    for _ in 0..4 {
        chars.next();
    }

    // Must have at least one digit for controller number
    if !chars.peek().is_some_and(|c| c.is_ascii_digit()) {
        return false;
    }
    while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
        chars.next();
    }

    // Must have 'n' for namespace
    if chars.next() != Some('n') {
        return false;
    }

    // Must have at least one digit for namespace number
    chars.peek().is_some_and(|c| c.is_ascii_digit())
}

/// Helper to find NVMe device via `nvme list-subsys` command.
/// Returns the device path (e.g., "/dev/nvme0n1") if found, None otherwise.
async fn find_device_via_list_subsys(target_nqn: &str) -> Option<String> {
    let output = Command::new("nvme")
        .args(["list-subsys", "-o", "json"])
        .output()
        .await
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).ok()?;

    // nvme list-subsys output: {"Subsystems":[{"NQN":"...", "Paths":[{"Name":"nvme0n1"}]}]}
    let subsystems = json.get("Subsystems")?.as_array()?;

    for subsys in subsystems {
        let nqn = subsys.get("NQN").and_then(|n| n.as_str()).unwrap_or("");
        if nqn != target_nqn {
            continue;
        }

        // Try "Paths" array first (common format)
        if let Some(paths) = subsys.get("Paths").and_then(|p| p.as_array()) {
            for path in paths {
                let name = path.get("Name").and_then(|n| n.as_str())?;
                if is_nvme_namespace_device(name) {
                    return Some(format!("/dev/{}", name));
                }
            }
        }

        // Try "Namespaces" array (alternative format)
        if let Some(namespaces) = subsys.get("Namespaces").and_then(|n| n.as_array()) {
            for ns in namespaces {
                let name = ns.get("NameSpace").and_then(|n| n.as_str())?;
                if is_nvme_namespace_device(name) {
                    return Some(format!("/dev/{}", name));
                }
            }
        }
    }

    None
}

/// Find the device associated with an NVMeoF target.
///
/// This function handles both NVMe native multipath and dm-multipath:
/// - Always checks if dm-multipath has claimed the device first
/// - If dm-multipath owns the device, returns the dm device path
/// - Otherwise returns the raw NVMe namespace device
///
/// Note: Even with native NVMe multipath enabled (nvme_core.multipath=Y),
/// dm-multipath may still be configured to claim NVMe devices. We must
/// always check for dm devices to avoid "device in use" errors.
pub async fn find_nvmeof_device(target_nqn: &str) -> PlatformResult<String> {
    let native_multipath = is_nvme_native_multipath_enabled().await;
    debug!(
        native_multipath = native_multipath,
        "Checking NVMe native multipath status"
    );

    // Wait for udev to finish processing device events (best practice from other CSI drivers)
    // This ensures /sys entries are fully populated after nvme connect
    let _ = Command::new("udevadm")
        .args(["settle", "--timeout=5"])
        .output()
        .await;

    // Method 1: Use nvme list-subsys which directly maps NQN to devices
    // This is the most reliable method as it's specifically designed for this purpose
    if let Some(device) = find_device_via_list_subsys(target_nqn).await {
        info!(
            device = %device,
            target_nqn = %target_nqn,
            "Found NVMeoF device via nvme list-subsys"
        );
        return Ok(resolve_multipath_device(&device).await);
    }

    // Method 2: Use nvme list with JSON output
    let output = Command::new("nvme")
        .args(["list", "-o", "json"])
        .output()
        .await
        .map_err(|e| {
            error!(error = %e, "Failed to execute nvme list");
            Status::internal(format!("Failed to list NVMe devices: {}", e))
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout);

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&stdout)
        && let Some(devices) = json.get("Devices").and_then(|d| d.as_array())
    {
        for device in devices {
            if let Some(dev_path) = device.get("DevicePath").and_then(|p| p.as_str()) {
                // Check if this device is associated with our NQN
                let subsys_nqn = device
                    .get("SubsystemNQN")
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                // CRITICAL: Require exact NQN match, not substring match
                if subsys_nqn == target_nqn && is_nvme_namespace_device(dev_path) {
                    info!(
                        device = %dev_path,
                        target_nqn = %target_nqn,
                        "Found NVMeoF device via nvme list"
                    );
                    return Ok(resolve_multipath_device(dev_path).await);
                }
            }
        }
    }

    // Method 3: Check /sys/class/nvme-subsystem/ (kernel 4.15+)
    let nvme_subsys = Path::new("/sys/class/nvme-subsystem");
    if tokio::fs::try_exists(nvme_subsys).await.unwrap_or(false)
        && let Ok(mut entries) = tokio::fs::read_dir(nvme_subsys).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let nqn_path = entry.path().join("subsysnqn");
            if let Ok(nqn) = tokio::fs::read_to_string(&nqn_path).await
                && nqn.trim() == target_nqn
            {
                // Found the subsystem, now find the namespace device
                if let Ok(mut ns_entries) = tokio::fs::read_dir(entry.path()).await {
                    while let Ok(Some(ns_entry)) = ns_entries.next_entry().await {
                        let name = ns_entry.file_name();
                        let name_str = name.to_string_lossy();
                        // Only match namespace devices like nvme0n1, not controller devices like nvme0
                        if is_nvme_namespace_device(&name_str) {
                            let raw_device = format!("/dev/{}", name_str);
                            info!(
                                device = %raw_device,
                                target_nqn = %target_nqn,
                                "Found NVMeoF device via /sys/class/nvme-subsystem"
                            );
                            // Always check for dm-multipath
                            return Ok(resolve_multipath_device(&raw_device).await);
                        }
                    }
                }
            }
        }
    }

    // Method 4: Check /sys/block/nvme*/device/subsysnqn (direct block device lookup)
    // This path works on all kernel versions with NVMe support
    let sys_block = Path::new("/sys/block");
    if tokio::fs::try_exists(sys_block).await.unwrap_or(false)
        && let Ok(mut entries) = tokio::fs::read_dir(sys_block).await
    {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Only check nvme namespace devices (nvmeXnY)
            if !is_nvme_namespace_device(&name_str) {
                continue;
            }

            let nqn_path = entry.path().join("device/subsysnqn");
            if let Ok(nqn) = tokio::fs::read_to_string(&nqn_path).await {
                let nqn_trimmed = nqn.trim();
                debug!(
                    device = %name_str,
                    nqn = %nqn_trimmed,
                    target_nqn = %target_nqn,
                    "Checking NVMe device NQN"
                );
                if nqn_trimmed == target_nqn {
                    let raw_device = format!("/dev/{}", name_str);
                    info!(
                        device = %raw_device,
                        target_nqn = %target_nqn,
                        "Found NVMeoF device via /sys/block"
                    );
                    // Always check for dm-multipath
                    return Ok(resolve_multipath_device(&raw_device).await);
                }
            }
        }
    }

    // No device found - return error with diagnostic info
    // CRITICAL: Do NOT return an arbitrary device - this causes data corruption!
    error!(
        target_nqn = %target_nqn,
        "No NVMe device found matching target NQN. Device may not be connected."
    );
    Err(Status::internal(format!(
        "No NVMe device found for NQN '{}'. Ensure the target is connected and the NQN is correct.",
        target_nqn
    )))
}

/// Disconnect from an NVMeoF target.
pub async fn disconnect_nvmeof(target_nqn: &str) -> PlatformResult<()> {
    info!(target_nqn = %target_nqn, "Disconnecting from NVMeoF target");

    let output = Command::new("nvme")
        .args(["disconnect", "-n", target_nqn])
        .output()
        .await
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
pub async fn format_device(device: &str, fs_type: &str) -> PlatformResult<()> {
    info!(device = %device, fs_type = %fs_type, "Formatting device");

    match fs_type.to_lowercase().as_str() {
        "ext4" => {
            let output = Command::new("mkfs.ext4")
                .args(["-F", device]) // -F to force (don't prompt)
                .output()
                .await
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
                .await
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
pub async fn needs_formatting(device: &str) -> PlatformResult<bool> {
    // Use blkid to check for existing filesystem
    let output = Command::new("blkid")
        .args(["-p", device])
        .output()
        .await
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
pub async fn mount_device(device: &str, target: &str, fs_type: &str) -> PlatformResult<()> {
    info!(device = %device, target = %target, fs_type = %fs_type, "Mounting device");

    // Ensure target directory exists
    tokio::fs::create_dir_all(target).await.map_err(|e| {
        error!(error = %e, "Failed to create mount target directory");
        Status::internal(format!("Failed to create mount directory: {}", e))
    })?;

    let fs_type_lower = fs_type.to_lowercase();

    let output = Command::new("mount")
        .args(["-t", &fs_type_lower, device, target])
        .output()
        .await
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
pub async fn bind_mount(source: &str, target: &str) -> PlatformResult<()> {
    info!(source = %source, target = %target, "Creating bind mount");

    // Ensure target directory exists
    tokio::fs::create_dir_all(target).await.map_err(|e| {
        error!(error = %e, "Failed to create bind mount target directory");
        Status::internal(format!(
            "Failed to create bind mount target directory: {}",
            e
        ))
    })?;

    let output = Command::new("mount")
        .args(["--bind", source, target])
        .output()
        .await
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
pub async fn unmount(target: &str) -> PlatformResult<()> {
    info!(target = %target, "Unmounting");

    // Check if path is actually mounted
    if !is_mounted(target).await? {
        debug!(target = %target, "Path is not mounted, skipping unmount");
        return Ok(());
    }

    let output = Command::new("umount")
        .arg(target)
        .output()
        .await
        .map_err(|e| {
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
pub async fn is_mounted(target: &str) -> PlatformResult<bool> {
    // On Linux, check /proc/mounts for efficiency
    if let Ok(mounts) = tokio::fs::read_to_string("/proc/mounts").await {
        return Ok(mounts
            .lines()
            .any(|line| line.split_whitespace().nth(1) == Some(target)));
    }

    // Fallback to mount command
    let output = Command::new("mount").output().await.map_err(|e| {
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

    #[test]
    fn test_is_nvme_namespace_device() {
        // Valid namespace devices
        assert!(is_nvme_namespace_device("/dev/nvme0n1"));
        assert!(is_nvme_namespace_device("/dev/nvme1n2"));
        assert!(is_nvme_namespace_device("/dev/nvme10n15"));
        assert!(is_nvme_namespace_device("nvme0n1")); // Without /dev/ prefix

        // Invalid - controller devices (not namespaces)
        assert!(!is_nvme_namespace_device("/dev/nvme0"));
        assert!(!is_nvme_namespace_device("/dev/nvme1"));
        assert!(!is_nvme_namespace_device("nvme0"));

        // Invalid - other formats
        assert!(!is_nvme_namespace_device("/dev/sda"));
        assert!(!is_nvme_namespace_device("/dev/nvme"));
        assert!(!is_nvme_namespace_device(""));
        assert!(!is_nvme_namespace_device("/dev/nvme0n")); // Missing namespace number
    }
}
