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

pub use linux::LinuxPlatform as Platform;
