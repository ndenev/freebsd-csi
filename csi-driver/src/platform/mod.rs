//! Platform operations for CSI Node service
//!
//! Provides Linux-specific implementations for iSCSI, NVMeoF, filesystem
//! operations, and bind mounts.
//!
//! # Usage
//!
//! ```ignore
//! use crate::platform;
//!
//! let device = platform::connect_iscsi(target_iqn, portal)?;
//! platform::format_device(&device, "ext4")?;
//! ```

mod linux;

use tonic::Status;

/// Result type for platform operations
pub type PlatformResult<T> = Result<T, Status>;

// Re-export all platform functions
pub use linux::{
    bind_mount, connect_iscsi, connect_nvmeof, default_fs_type, disconnect_iscsi,
    disconnect_nvmeof, find_iscsi_device, find_nvmeof_device, format_device, is_iscsi_connected,
    is_mounted, is_nvmeof_connected, mount_device, needs_formatting, unmount, validate_fs_type,
};
