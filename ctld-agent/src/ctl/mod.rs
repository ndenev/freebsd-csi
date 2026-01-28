mod config_manager;
mod config_validator;
mod csi_config;
pub mod ctl_manager;
pub mod error;
pub mod types;
pub mod ucl_config;

pub use config_manager::{ConfigManager, ConfigManagerError};
pub use config_validator::{
    ValidationError, validate_portal_group_exists, validate_transport_group_exists,
};
pub use csi_config::CsiConfigGenerator;

// Re-exports for module API
pub use ctl_manager::{ConfigWriterHandle, CtlManager, spawn_config_writer};
pub use error::CtlError;
pub use types::ExportType;

// Re-export types that may be used externally
#[allow(unused_imports)]
pub use types::{AuthConfig, DevicePath, Iqn, IscsiChapAuth, Nqn, NvmeAuth, TargetName};
pub use ucl_config::{CtlOptions, validate_chap_credentials};
