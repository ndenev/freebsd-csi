//! Platform operations for CSI Node service
//!
//! Provides Linux-specific implementations for iSCSI, NVMeoF, filesystem
//! operations, and bind mounts.
//!
//! # Usage
//!
//! ```ignore
//! use crate::platform;
//! use crate::types::{Endpoint, Endpoints};
//!
//! let endpoints = Endpoints::parse("10.0.0.1:3260,10.0.0.2:3260", 3260)?;
//! let device = platform::connect_iscsi(target_iqn, endpoints.as_slice())?;
//! platform::format_device(&device, "ext4")?;
//! ```

mod linux;

use tonic::Status;

/// Result type for platform operations
pub type PlatformResult<T> = Result<T, Status>;

// Re-export all platform functions and types
pub use linux::{
    IscsiChapCredentials, NvmeAuthCredentials, bind_mount, connect_iscsi, connect_nvmeof,
    default_fs_type, disconnect_iscsi, disconnect_nvmeof, find_iscsi_device, find_nvmeof_device,
    format_device, is_iscsi_connected, is_mounted, is_nvmeof_connected, mount_device,
    needs_formatting, unmount, validate_fs_type,
};
