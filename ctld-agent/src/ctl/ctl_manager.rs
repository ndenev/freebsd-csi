//! Unified CTL (CAM Target Layer) manager for iSCSI and NVMeoF exports.
//!
//! This module provides a single manager for both iSCSI targets and NVMeoF controllers,
//! simplifying the architecture and reducing code duplication.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::process::Command;
use std::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use super::error::{CtlError, Result};
use super::types::{AuthConfig, DevicePath, ExportType, Iqn, Nqn, TargetName};
use super::ucl_config::{AuthGroup, Controller, CtlConfig, Target, UclConfigManager};

/// Represents a CTL export (either iSCSI target or NVMeoF controller)
#[derive(Debug, Clone)]
pub struct Export {
    /// Volume name (used as key)
    pub volume_name: String,
    /// Path to the backing device (e.g., /dev/zvol/tank/csi/vol1)
    pub device_path: DevicePath,
    /// Export type (iSCSI or NVMeoF)
    pub export_type: ExportType,
    /// Target name (IQN for iSCSI, NQN for NVMeoF)
    pub target_name: TargetName,
    /// LUN ID (for iSCSI) or Namespace ID (for NVMeoF)
    pub lun_id: u32,
    /// Authentication configuration (CHAP for iSCSI, DH-HMAC-CHAP for NVMeoF)
    pub auth: AuthConfig,
}

/// Unified manager for CTL exports (iSCSI and NVMeoF)
pub struct CtlManager {
    /// Base IQN prefix for iSCSI targets
    base_iqn: String,
    /// Base NQN prefix for NVMeoF controllers
    base_nqn: String,
    /// Auth group name for UCL config
    #[allow(dead_code)] // TODO: Use for CHAP authentication support
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
        // Validate base IQN/NQN format
        Iqn::parse(&base_iqn)
            .map_err(|_| CtlError::InvalidName(format!("invalid base IQN format: {}", base_iqn)))?;
        Nqn::parse(&base_nqn)
            .map_err(|_| CtlError::InvalidName(format!("invalid base NQN format: {}", base_nqn)))?;

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
    pub fn generate_iqn(&self, volume_name: &str) -> Result<Iqn> {
        Iqn::new(&self.base_iqn, volume_name)
    }

    /// Generate an NQN for a volume
    pub fn generate_nqn(&self, volume_name: &str) -> Result<Nqn> {
        Nqn::new(&self.base_nqn, volume_name)
    }

    /// Load existing exports from UCL config file.
    ///
    /// Note: This is currently unused as ZFS user properties are the source of truth.
    /// Kept for potential debugging/recovery purposes.
    #[allow(dead_code)]
    #[instrument(skip(self))]
    pub fn load_config(&mut self) -> Result<()> {
        let config_path = &self.ucl_manager.config_path;
        let config = CtlConfig::from_file(config_path)?;

        let mut exports = self.exports.write().unwrap();
        let mut loaded_iscsi = 0;
        let mut loaded_nvmeof = 0;

        // Load iSCSI targets matching our base IQN using filter_map
        let iscsi_exports: Vec<_> = config
            .targets_with_prefix(&self.base_iqn)
            .filter_map(|(iqn_str, target)| {
                let volume_name = iqn_str.rsplit(':').next()?;
                let (lun_id_str, lun) = target.lun.iter().next()?;
                let lun_id = lun_id_str.parse::<u32>().unwrap_or(0);
                let iqn = Iqn::parse(iqn_str).ok()?;
                let device_path = DevicePath::parse(&lun.path).ok()?;

                Some(Export {
                    volume_name: volume_name.to_string(),
                    device_path,
                    export_type: ExportType::Iscsi,
                    target_name: iqn.into(),
                    lun_id,
                    // Auth is not persisted in UCL config, defaults to none
                    auth: AuthConfig::None,
                })
            })
            .collect();

        for export in iscsi_exports {
            exports.insert(export.volume_name.clone(), export);
            loaded_iscsi += 1;
        }

        // Load NVMeoF controllers matching our base NQN using filter_map
        let nvmeof_exports: Vec<_> = config
            .controllers_with_prefix(&self.base_nqn)
            .filter_map(|(nqn_str, controller)| {
                let volume_name = nqn_str.rsplit(':').next()?;
                let (ns_id_str, ns) = controller.namespace.iter().next()?;
                let ns_id = ns_id_str.parse::<u32>().unwrap_or(0);
                let nqn = Nqn::parse(nqn_str).ok()?;
                let device_path = DevicePath::parse(&ns.path).ok()?;

                Some(Export {
                    volume_name: volume_name.to_string(),
                    device_path,
                    export_type: ExportType::Nvmeof,
                    target_name: nqn.into(),
                    lun_id: ns_id,
                    // Auth is not persisted in UCL config, defaults to none
                    auth: AuthConfig::None,
                })
            })
            .collect();

        for export in nvmeof_exports {
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
    ///
    /// # Arguments
    /// * `volume_name` - Unique name for the volume
    /// * `device_path` - Path to the backing device (e.g., /dev/zvol/tank/csi/vol1)
    /// * `export_type` - iSCSI or NVMeoF
    /// * `lun_id` - LUN ID for iSCSI or Namespace ID for NVMeoF
    /// * `auth` - Optional authentication configuration (CHAP/DH-HMAC-CHAP)
    #[instrument(skip(self, auth))]
    pub fn export_volume(
        &self,
        volume_name: &str,
        device_path: &str,
        export_type: ExportType,
        lun_id: u32,
        auth: AuthConfig,
    ) -> Result<Export> {
        // Validate and parse inputs using newtypes
        let device_path = DevicePath::parse(device_path)?;
        let target_name: TargetName = match export_type {
            ExportType::Iscsi => self.generate_iqn(volume_name)?.into(),
            ExportType::Nvmeof => self.generate_nqn(volume_name)?.into(),
        };

        debug!(
            "Exporting volume {} as {} target {} (auth={})",
            volume_name,
            export_type,
            target_name,
            if auth.is_some() { "enabled" } else { "none" }
        );

        let export = Export {
            volume_name: volume_name.to_string(),
            device_path,
            export_type,
            target_name,
            lun_id,
            auth,
        };

        // Use Entry API for atomic check-and-insert
        let mut exports = self.exports.write().unwrap();
        match exports.entry(volume_name.to_string()) {
            Entry::Occupied(_) => {
                return Err(CtlError::TargetExists(volume_name.to_string()));
            }
            Entry::Vacant(vacant) => {
                vacant.insert(export.clone());
            }
        }

        info!("Exported {} as {} (cache only)", volume_name, export_type);
        Ok(export)
    }

    /// Unexport a volume
    ///
    /// Updates in-memory cache only. Call `write_config()` to persist.
    #[instrument(skip(self))]
    pub fn unexport_volume(&self, volume_name: &str) -> Result<()> {
        debug!("Unexporting volume {}", volume_name);

        let mut exports = self.exports.write().unwrap();
        if exports.remove(volume_name).is_none() {
            return Err(CtlError::TargetNotFound(volume_name.to_string()));
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
    /// Generates per-volume auth-groups for targets that require authentication.
    #[instrument(skip(self))]
    pub fn write_config(&self) -> Result<()> {
        let exports = self.exports.read().unwrap();

        // Convert exports to Target/Controller types and collect auth groups
        let mut iscsi_targets: Vec<(String, Target)> = Vec::new();
        let mut nvme_controllers: Vec<(String, Controller)> = Vec::new();
        let mut auth_groups: Vec<(String, AuthGroup)> = Vec::new();

        for export in exports.values() {
            // Get auth group name (either "no-authentication" or per-volume "ag-<name>")
            let auth_group_name = export.auth.auth_group_name(&export.volume_name);

            // If this export has authentication, create an auth group entry
            if let Some(ag) = AuthGroup::from_auth_config(&export.auth, &export.volume_name) {
                auth_groups.push((auth_group_name.clone(), ag));
            }

            match export.export_type {
                ExportType::Iscsi => {
                    let target = Target::new(
                        auth_group_name,
                        self.portal_group_name.clone(),
                        export.lun_id,
                        export.device_path.as_str().to_string(),
                        &export.volume_name,
                    );
                    iscsi_targets.push((export.target_name.to_string(), target));
                }
                ExportType::Nvmeof => {
                    let controller = Controller::new(
                        auth_group_name,
                        self.transport_group.clone(),
                        export.lun_id,
                        export.device_path.as_str().to_string(),
                        &export.volume_name,
                    );
                    nvme_controllers.push((export.target_name.to_string(), controller));
                }
            }
        }

        drop(exports);

        info!(
            "Writing UCL config with {} iSCSI targets, {} NVMeoF controllers, {} auth groups",
            iscsi_targets.len(),
            nvme_controllers.len(),
            auth_groups.len()
        );

        // Read user content (non-CSI targets)
        let user_content = self.ucl_manager.read_user_content()?;

        // Write config with CSI targets and auth groups
        self.ucl_manager.write_config_with_auth(
            &user_content,
            &iscsi_targets,
            &nvme_controllers,
            &auth_groups,
        )?;

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
    fn test_export_struct() {
        let device_path = DevicePath::parse("/dev/zvol/tank/vol1").unwrap();
        let iqn = Iqn::parse("iqn.2024-01.com.example:vol1").unwrap();

        let export = Export {
            volume_name: "vol1".to_string(),
            device_path,
            export_type: ExportType::Iscsi,
            target_name: iqn.into(),
            lun_id: 0,
            auth: AuthConfig::None,
        };

        assert_eq!(export.volume_name, "vol1");
        assert_eq!(export.export_type, ExportType::Iscsi);
        assert!(!export.auth.is_some());
    }

    #[test]
    fn test_export_with_chap_auth() {
        use super::super::types::IscsiChapAuth;

        let device_path = DevicePath::parse("/dev/zvol/tank/vol2").unwrap();
        let iqn = Iqn::parse("iqn.2024-01.com.example:vol2").unwrap();

        let chap = IscsiChapAuth::new("testuser", "testsecret");
        let export = Export {
            volume_name: "vol2".to_string(),
            device_path,
            export_type: ExportType::Iscsi,
            target_name: iqn.into(),
            lun_id: 0,
            auth: AuthConfig::IscsiChap(chap),
        };

        assert!(export.auth.is_some());
        assert_eq!(export.auth.auth_group_name("vol2"), "ag-vol2");
    }
}
