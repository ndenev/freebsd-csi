//! Platform abstraction for CSI Node operations
//!
//! Provides Linux-specific implementations for iSCSI, NVMeoF, filesystem
//! operations, and bind mounts.
//!
//! # Usage
//!
//! ```ignore
//! use crate::platform::Platform;
//!
//! let device = Platform::connect_iscsi(target_iqn, portal)?;
//! Platform::format_device(&device, "ext4")?;
//! ```

mod linux;

use tonic::Status;

/// Result type for platform operations
pub type PlatformResult<T> = Result<T, Status>;

/// Platform interface for storage operations.
///
/// Defines the required operations for iSCSI, NVMeoF, filesystem formatting,
/// and mount operations.
pub trait StorageOps {
    /// Check if an iSCSI target is currently connected.
    fn is_iscsi_connected(target_iqn: &str) -> bool;

    /// Check if an NVMeoF target is currently connected.
    fn is_nvmeof_connected(target_nqn: &str) -> bool;

    /// Connect to an iSCSI target and return the device path.
    fn connect_iscsi(target_iqn: &str, portal: Option<&str>) -> PlatformResult<String>;

    /// Find the device associated with an iSCSI target.
    fn find_iscsi_device(target_iqn: &str) -> PlatformResult<String>;

    /// Disconnect from an iSCSI target.
    fn disconnect_iscsi(target_iqn: &str) -> PlatformResult<()>;

    /// Connect to an NVMeoF target and return the device path.
    fn connect_nvmeof(
        target_nqn: &str,
        transport_addr: Option<&str>,
        transport_port: Option<&str>,
    ) -> PlatformResult<String>;

    /// Find the device associated with an NVMeoF target.
    fn find_nvmeof_device(target_nqn: &str) -> PlatformResult<String>;

    /// Disconnect from an NVMeoF target.
    fn disconnect_nvmeof(target_nqn: &str) -> PlatformResult<()>;

    /// Format a device with the specified filesystem type.
    fn format_device(device: &str, fs_type: &str) -> PlatformResult<()>;

    /// Check if a device needs formatting (has no valid filesystem).
    fn needs_formatting(device: &str) -> PlatformResult<bool>;

    /// Mount a device to a target path.
    fn mount_device(device: &str, target: &str, fs_type: &str) -> PlatformResult<()>;

    /// Create a bind mount.
    fn bind_mount(source: &str, target: &str) -> PlatformResult<()>;

    /// Unmount a path.
    fn unmount(target: &str) -> PlatformResult<()>;

    /// Check if a path is currently mounted.
    fn is_mounted(target: &str) -> PlatformResult<bool>;

    /// Validate filesystem type for this platform.
    fn validate_fs_type(fs_type: &str) -> PlatformResult<&'static str>;

    /// Get the default filesystem type for this platform.
    fn default_fs_type() -> &'static str;
}

pub use linux::LinuxPlatform as Platform;
