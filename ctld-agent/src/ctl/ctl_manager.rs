//! Unified CTL (CAM Target Layer) manager for iSCSI and NVMeoF exports.
//!
//! This module provides a single manager for both iSCSI targets and NVMeoF controllers,
//! simplifying the architecture and reducing code duplication.

use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use super::error::{CtlError, Result};
use super::ucl_config::{Controller, CtlConfig, Target, UclConfigManager};

/// Export type for CTL volumes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportType {
    Iscsi,
    Nvmeof,
}

/// Represents a CTL export (either iSCSI target or NVMeoF controller)
#[derive(Debug, Clone)]
pub struct Export {
    /// Volume name (used as key)
    pub volume_name: String,
    /// Path to the backing device (e.g., /dev/zvol/tank/csi/vol1)
    pub device_path: String,
    /// Export type (iSCSI or NVMeoF)
    pub export_type: ExportType,
    /// Target name (IQN for iSCSI, NQN for NVMeoF)
    pub target_name: String,
    /// LUN ID (for iSCSI) or Namespace ID (for NVMeoF)
    pub lun_id: u32,
}

/// Validate that a name is safe for use in CTL commands.
/// For IQN/NQN format, allows: alphanumeric, underscore, hyphen, period, colon.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CtlError::InvalidName("name cannot be empty".into()));
    }

    if name.len() > 223 {
        return Err(CtlError::InvalidName(format!(
            "name '{}' exceeds maximum length of 223 characters",
            name
        )));
    }

    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
    {
        return Err(CtlError::InvalidName(format!(
            "invalid characters in name '{}': only alphanumeric, underscore, hyphen, period, and colon allowed",
            name
        )));
    }

    if name.contains("..") {
        return Err(CtlError::InvalidName(format!(
            "name '{}' contains path traversal sequence",
            name
        )));
    }

    Ok(())
}

/// Validate a device path is a valid zvol path
fn validate_device_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(CtlError::InvalidName("device path cannot be empty".into()));
    }

    if !path.starts_with("/dev/zvol/") {
        return Err(CtlError::InvalidName(format!(
            "device path '{}' must be under /dev/zvol/",
            path
        )));
    }

    if path.contains("..") {
        return Err(CtlError::InvalidName(format!(
            "device path '{}' contains path traversal sequence",
            path
        )));
    }

    let path_part = &path["/dev/zvol/".len()..];
    if !path_part
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/')
    {
        return Err(CtlError::InvalidName(format!(
            "device path '{}' contains invalid characters",
            path
        )));
    }

    Ok(())
}

/// Unified manager for CTL exports (iSCSI and NVMeoF)
pub struct CtlManager {
    /// Base IQN prefix for iSCSI targets
    base_iqn: String,
    /// Base NQN prefix for NVMeoF controllers
    base_nqn: String,
    /// Auth group name for UCL config
    auth_group: String,
    /// Portal group name for iSCSI
    portal_group_name: String,
    /// Transport group name for NVMeoF
    transport_group: String,
    /// In-memory cache of all exports, keyed by volume name
    exports: RwLock<HashMap<String, Export>>,
    /// UCL config manager for persistent configuration
    ucl_manager: UclConfigManager,
}

impl CtlManager {
    /// Create a new CtlManager
    ///
    /// # Arguments
    /// * `base_iqn` - Base IQN prefix for iSCSI targets
    /// * `base_nqn` - Base NQN prefix for NVMeoF controllers
    /// * `portal_group_name` - Portal group name for UCL config
    /// * `config_path` - Path to the UCL config file
    /// * `auth_group` - Auth group name for UCL config
    /// * `transport_group` - Transport group name for NVMeoF
    pub fn new(
        base_iqn: String,
        base_nqn: String,
        portal_group_name: String,
        config_path: String,
        auth_group: String,
        transport_group: String,
    ) -> Result<Self> {
        validate_name(&base_iqn)?;
        validate_name(&base_nqn)?;

        let ucl_manager = UclConfigManager::new(config_path);

        info!(
            "Initializing CtlManager with base_iqn={}, base_nqn={}, portal_group={}",
            base_iqn, base_nqn, portal_group_name
        );

        Ok(Self {
            base_iqn,
            base_nqn,
            auth_group,
            portal_group_name,
            transport_group,
            exports: RwLock::new(HashMap::new()),
            ucl_manager,
        })
    }

    /// Generate an IQN for a volume
    pub fn generate_iqn(&self, volume_name: &str) -> String {
        format!("{}:{}", self.base_iqn, volume_name)
    }

    /// Generate an NQN for a volume
    pub fn generate_nqn(&self, volume_name: &str) -> String {
        format!("{}:{}", self.base_nqn, volume_name.replace('/', "-"))
    }

    /// Load existing exports from UCL config file
    #[instrument(skip(self))]
    pub fn load_config(&mut self) -> Result<()> {
        let config_path = &self.ucl_manager.config_path;

        // Parse the UCL config using uclicious
        let config = CtlConfig::from_file(config_path)?;

        let mut exports = self.exports.write().unwrap();
        let mut loaded_iscsi = 0;
        let mut loaded_nvmeof = 0;

        // Load iSCSI targets matching our base IQN
        for (iqn, target) in config.targets_with_prefix(&self.base_iqn) {
            let Some(volume_name) = iqn.rsplit(':').next() else {
                continue;
            };

            // Get first LUN
            let Some((lun_id_str, lun)) = target.lun.iter().next() else {
                continue;
            };
            let lun_id = lun_id_str.parse::<u32>().unwrap_or(0);

            let export = Export {
                volume_name: volume_name.to_string(),
                device_path: lun.path.clone(),
                export_type: ExportType::Iscsi,
                target_name: iqn.clone(),
                lun_id,
            };
            exports.insert(export.volume_name.clone(), export);
            loaded_iscsi += 1;
        }

        // Load NVMeoF controllers matching our base NQN
        for (nqn, controller) in config.controllers_with_prefix(&self.base_nqn) {
            let Some(volume_name) = nqn.rsplit(':').next() else {
                continue;
            };

            // Get first namespace
            let Some((ns_id_str, ns)) = controller.namespace.iter().next() else {
                continue;
            };
            let ns_id = ns_id_str.parse::<u32>().unwrap_or(0);

            let export = Export {
                volume_name: volume_name.to_string(),
                device_path: ns.path.clone(),
                export_type: ExportType::Nvmeof,
                target_name: nqn.clone(),
                lun_id: ns_id,
            };
            exports.insert(export.volume_name.clone(), export);
            loaded_nvmeof += 1;
        }

        info!(
            "Loaded {} iSCSI targets and {} NVMeoF controllers from UCL config",
            loaded_iscsi, loaded_nvmeof
        );
        Ok(())
    }

    /// Export a volume via iSCSI or NVMeoF
    ///
    /// Updates in-memory cache only. Call `write_config()` to persist.
    #[instrument(skip(self))]
    pub fn export_volume(
        &self,
        volume_name: &str,
        device_path: &str,
        export_type: ExportType,
        lun_id: u32,
    ) -> Result<Export> {
        validate_name(volume_name)?;
        validate_device_path(device_path)?;

        let target_name = match export_type {
            ExportType::Iscsi => self.generate_iqn(volume_name),
            ExportType::Nvmeof => self.generate_nqn(volume_name),
        };

        debug!(
            "Exporting volume {} as {:?} target {}",
            volume_name, export_type, target_name
        );

        // Check if already exists
        {
            let exports = self.exports.read().unwrap();
            if exports.contains_key(volume_name) {
                return Err(CtlError::TargetExists(volume_name.to_string()));
            }
        }

        let export = Export {
            volume_name: volume_name.to_string(),
            device_path: device_path.to_string(),
            export_type,
            target_name,
            lun_id,
        };

        // Store in cache
        {
            let mut exports = self.exports.write().unwrap();
            exports.insert(volume_name.to_string(), export.clone());
        }

        info!("Exported {} as {:?} (cache only)", volume_name, export_type);
        Ok(export)
    }

    /// Unexport a volume
    ///
    /// Updates in-memory cache only. Call `write_config()` to persist.
    #[instrument(skip(self))]
    pub fn unexport_volume(&self, volume_name: &str) -> Result<()> {
        validate_name(volume_name)?;

        debug!("Unexporting volume {}", volume_name);

        {
            let mut exports = self.exports.write().unwrap();
            if exports.remove(volume_name).is_none() {
                return Err(CtlError::TargetNotFound(volume_name.to_string()));
            }
        }

        info!("Unexported {} (cache only)", volume_name);
        Ok(())
    }

    /// Get an export by volume name
    pub fn get_export(&self, volume_name: &str) -> Option<Export> {
        let exports = self.exports.read().unwrap();
        exports.get(volume_name).cloned()
    }

    /// Write UCL config and reload ctld.
    ///
    /// Preserves user-managed targets while updating CSI-managed targets.
    #[instrument(skip(self))]
    pub fn write_config(&self) -> Result<()> {
        let exports = self.exports.read().unwrap();

        // Convert exports to Target/Controller types
        let mut iscsi_targets: Vec<(String, Target)> = Vec::new();
        let mut nvme_controllers: Vec<(String, Controller)> = Vec::new();

        for export in exports.values() {
            match export.export_type {
                ExportType::Iscsi => {
                    let target = Target::new(
                        self.auth_group.clone(),
                        self.portal_group_name.clone(),
                        export.lun_id,
                        export.device_path.clone(),
                    );
                    iscsi_targets.push((export.target_name.clone(), target));
                }
                ExportType::Nvmeof => {
                    let controller = Controller::new(
                        self.auth_group.clone(),
                        self.transport_group.clone(),
                        export.lun_id,
                        export.device_path.clone(),
                    );
                    nvme_controllers.push((export.target_name.clone(), controller));
                }
            }
        }

        drop(exports);

        info!(
            "Writing UCL config with {} iSCSI targets and {} NVMeoF controllers",
            iscsi_targets.len(),
            nvme_controllers.len()
        );

        // Read user content (non-CSI targets)
        let user_content = self.ucl_manager.read_user_content()?;

        // Write config with CSI targets
        self.ucl_manager
            .write_config(&user_content, &iscsi_targets, &nvme_controllers)?;

        info!("UCL config updated successfully");

        self.reload_ctld()?;

        Ok(())
    }

    /// Reload ctld configuration
    fn reload_ctld(&self) -> Result<()> {
        debug!("Reloading ctld configuration");

        let output = Command::new("service").args(["ctld", "reload"]).output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("ctld reload failed: {}", stderr);
            return Err(CtlError::CommandFailed(format!(
                "service ctld reload failed: {}",
                stderr
            )));
        }

        info!("Successfully reloaded ctld configuration");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name("volume1").is_ok());
        assert!(validate_name("vol-1").is_ok());
        assert!(validate_name("vol_1").is_ok());
        assert!(validate_name("vol.1").is_ok());
        assert!(validate_name("iqn.2024-01.com.example:target").is_ok());
        assert!(validate_name("nqn.2024-01.com.example:target").is_ok());
    }

    #[test]
    fn test_validate_name_invalid() {
        assert!(validate_name("").is_err());
        assert!(validate_name("vol/name").is_err());
        assert!(validate_name("vol@snap").is_err());
        assert!(validate_name("vol name").is_err());
        assert!(validate_name("vol;rm -rf /").is_err());
        assert!(validate_name("..").is_err());
    }

    #[test]
    fn test_validate_device_path_valid() {
        assert!(validate_device_path("/dev/zvol/tank/vol1").is_ok());
        assert!(validate_device_path("/dev/zvol/tank/csi/pvc-123").is_ok());
    }

    #[test]
    fn test_validate_device_path_invalid() {
        assert!(validate_device_path("").is_err());
        assert!(validate_device_path("/dev/da0").is_err());
        assert!(validate_device_path("/dev/zvol/../etc/passwd").is_err());
    }

    #[test]
    fn test_export_struct() {
        let export = Export {
            volume_name: "vol1".to_string(),
            device_path: "/dev/zvol/tank/vol1".to_string(),
            export_type: ExportType::Iscsi,
            target_name: "iqn.2024-01.com.example:vol1".to_string(),
            lun_id: 0,
        };

        assert_eq!(export.volume_name, "vol1");
        assert_eq!(export.export_type, ExportType::Iscsi);
    }
}
