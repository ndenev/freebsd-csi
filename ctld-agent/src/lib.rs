//! FreeBSD CTL Storage Agent Library
//!
//! This library provides ZFS volume management and iSCSI/NVMeoF export functionality
//! for the FreeBSD CSI driver.
//!
//! The library is split into several modules:
//! - `ctl`: CTL (CAM Target Layer) management for iSCSI and NVMeoF exports
//! - `zfs`: ZFS volume and snapshot management
//! - `service`: gRPC service implementation
//! - `metrics`: Prometheus metrics collection

pub mod auth;
pub mod ctl;
pub mod metrics;
pub mod service;
pub mod zfs;

pub use ctl::{AuthConfig, CtlError, CtlManager, ExportType, IscsiChapAuth, NvmeAuth};
pub use service::{StorageService, proto};
pub use zfs::ZfsManager;
