//! Unified CTL (CAM Target Layer) manager for iSCSI and NVMeoF exports.
//!
//! This module provides a single manager for both iSCSI targets and NVMeoF controllers,
//! simplifying the architecture and reducing code duplication.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::process::Command;

use tokio::sync::{RwLock as TokioRwLock, mpsc, oneshot};
use tracing::{debug, error, info, instrument, warn};

use std::io::Write as IoWrite;
use std::path::Path;

use tempfile::NamedTempFile;

use super::error::{CtlError, Result};
use super::types::{AuthConfig, DevicePath, ExportType, Iqn, Nqn, TargetName};
use super::ucl_config::{AuthGroup, Controller, CtlOptions, Target, ToUcl};

/// Default path for CSI-managed targets config
const CSI_CONFIG_PATH: &str = "/var/db/ctld-agent/csi-targets.conf";

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
    /// CTL options (blocksize, pblocksize, unmap)
    pub ctl_options: CtlOptions,
}

/// Unified manager for CTL exports (iSCSI and NVMeoF)
pub struct CtlManager {
    /// Base IQN prefix for iSCSI targets
    base_iqn: String,
    /// Base NQN prefix for NVMeoF controllers
    base_nqn: String,
    /// Auth group name for UCL config
    #[allow(dead_code)] // Legacy: superseded by per-volume auth groups in AuthConfig
    auth_group: String,
    /// Portal group name for iSCSI
    portal_group_name: String,
    /// Transport group name for NVMeoF
    transport_group: String,
    /// Parent ZFS dataset for device path validation (e.g., "tank/csi")
    parent_dataset: String,
    /// In-memory cache of all exports, keyed by volume name
    exports: RwLock<HashMap<String, Export>>,
    /// Path to write CSI-managed targets config
    csi_config_path: String,
}

impl CtlManager {
    /// Create a new CtlManager
    ///
    /// # Arguments
    /// * `base_iqn` - Base IQN prefix for iSCSI targets
    /// * `base_nqn` - Base NQN prefix for NVMeoF controllers
    /// * `portal_group_name` - Portal group name for UCL config
    /// * `auth_group` - Auth group name for UCL config
    /// * `transport_group` - Transport group name for NVMeoF
    /// * `parent_dataset` - Parent ZFS dataset for device path validation (e.g., "tank/csi")
    pub fn new(
        base_iqn: String,
        base_nqn: String,
        portal_group_name: String,
        auth_group: String,
        transport_group: String,
        parent_dataset: String,
    ) -> Result<Self> {
        // Validate base IQN/NQN format
        Iqn::parse(&base_iqn)
            .map_err(|_| CtlError::InvalidName(format!("invalid base IQN format: {}", base_iqn)))?;
        Nqn::parse(&base_nqn)
            .map_err(|_| CtlError::InvalidName(format!("invalid base NQN format: {}", base_nqn)))?;

        // Validate parent_dataset is not empty
        if parent_dataset.is_empty() {
            return Err(CtlError::InvalidName(
                "parent_dataset cannot be empty".to_string(),
            ));
        }

        info!(
            "Initializing CtlManager with base_iqn={}, base_nqn={}, portal_group={}, parent_dataset={}",
            base_iqn, base_nqn, portal_group_name, parent_dataset
        );

        Ok(Self {
            base_iqn,
            base_nqn,
            auth_group,
            portal_group_name,
            transport_group,
            parent_dataset,
            exports: RwLock::new(HashMap::new()),
            csi_config_path: CSI_CONFIG_PATH.to_string(),
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
    /// * `ctl_options` - CTL options (blocksize, pblocksize, unmap)
    #[instrument(skip(self, auth, ctl_options))]
    pub fn export_volume(
        &self,
        volume_name: &str,
        device_path: &str,
        export_type: ExportType,
        lun_id: u32,
        auth: AuthConfig,
        ctl_options: CtlOptions,
    ) -> Result<Export> {
        // Validate and parse inputs using newtypes
        let device_path = DevicePath::parse(device_path)?;

        // SECURITY: Validate device path is under the configured parent dataset.
        // This prevents privilege escalation by ensuring we can only export
        // volumes within our managed ZFS dataset hierarchy.
        device_path.validate_parent_dataset(&self.parent_dataset)?;

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
            ctl_options,
        };

        // Use Entry API for atomic check-and-insert
        let mut exports = self
            .exports
            .write()
            .map_err(|e| CtlError::ConfigError(format!("Lock poisoned: {}", e)))?;
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

        let mut exports = self
            .exports
            .write()
            .map_err(|e| CtlError::ConfigError(format!("Lock poisoned: {}", e)))?;
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

    /// Write CSI-managed targets to config file and reload ctld.
    ///
    /// Writes to /var/db/ctld-agent/csi-targets.conf which is included by
    /// /etc/ctl.conf via .include directive. This keeps CSI-managed targets
    /// separate from user-managed targets.
    ///
    /// Generates per-volume auth-groups for targets that require authentication.
    #[instrument(skip(self))]
    pub async fn write_config(&self) -> Result<()> {
        use std::fmt::Write;

        // Collect targets and auth groups while holding the lock
        // Use a block to ensure the lock guard is dropped before any await points
        let (iscsi_targets, nvme_controllers, auth_groups) = {
            let exports = self.exports.read().unwrap();

            let mut iscsi_targets: Vec<(String, Target)> = Vec::new();
            let mut nvme_controllers: Vec<(String, Controller)> = Vec::new();
            let mut auth_groups: Vec<(String, AuthGroup)> = Vec::new();

            for export in exports.values() {
                // Get auth group name (either "no-authentication" or per-volume "ag-<name>")
                let auth_group_name = export.auth.auth_group_name(&export.volume_name);

                // If this export has authentication, create an auth group entry
                // This validates CHAP credentials don't contain characters that would corrupt UCL
                if let Some(ag) = AuthGroup::from_auth_config(&export.auth, &export.volume_name)? {
                    auth_groups.push((auth_group_name.clone(), ag));
                }

                match export.export_type {
                    ExportType::Iscsi => {
                        let target = Target::with_options(
                            auth_group_name,
                            self.portal_group_name.clone(),
                            export.lun_id,
                            export.device_path.as_str().to_string(),
                            &export.volume_name,
                            &export.ctl_options,
                        );
                        iscsi_targets.push((export.target_name.to_string(), target));
                    }
                    ExportType::Nvmeof => {
                        let controller = Controller::with_options(
                            auth_group_name,
                            self.transport_group.clone(),
                            export.lun_id,
                            export.device_path.as_str().to_string(),
                            &export.volume_name,
                            &export.ctl_options,
                        );
                        nvme_controllers.push((export.target_name.to_string(), controller));
                    }
                }
            }

            (iscsi_targets, nvme_controllers, auth_groups)
        };

        info!(
            "Writing CSI config to {} with {} iSCSI targets, {} NVMeoF controllers, {} auth groups",
            self.csi_config_path,
            iscsi_targets.len(),
            nvme_controllers.len(),
            auth_groups.len()
        );

        // Generate UCL config content
        let mut config = String::new();
        writeln!(config, "# CSI-managed targets - DO NOT EDIT MANUALLY").unwrap();
        writeln!(config, "# Generated by ctld-agent").unwrap();
        writeln!(
            config,
            "# This file is included by /etc/ctl.conf via .include directive"
        )
        .unwrap();
        writeln!(config).unwrap();

        // Write auth groups
        for (name, auth_group) in &auth_groups {
            writeln!(config, "auth-group \"{}\" {{", name).unwrap();
            write!(config, "{}", auth_group.to_ucl(1)).unwrap();
            writeln!(config, "}}").unwrap();
            writeln!(config).unwrap();
        }

        // Write iSCSI targets
        for (iqn, target) in &iscsi_targets {
            writeln!(config, "target \"{}\" {{", iqn).unwrap();
            write!(config, "{}", target.to_ucl(1)).unwrap();
            writeln!(config, "}}").unwrap();
            writeln!(config).unwrap();
        }

        // Write NVMeoF controllers
        for (nqn, controller) in &nvme_controllers {
            writeln!(config, "controller \"{}\" {{", nqn).unwrap();
            write!(config, "{}", controller.to_ucl(1)).unwrap();
            writeln!(config, "}}").unwrap();
            writeln!(config).unwrap();
        }

        // Write atomically using temp file + rename
        let config_path = Path::new(&self.csi_config_path);
        let config_dir = config_path
            .parent()
            .unwrap_or(Path::new("/var/db/ctld-agent"));

        // Ensure config directory exists
        if !config_dir.exists() {
            std::fs::create_dir_all(config_dir).map_err(CtlError::Io)?;
        }

        let mut temp_file = NamedTempFile::new_in(config_dir).map_err(CtlError::Io)?;
        temp_file
            .write_all(config.as_bytes())
            .map_err(CtlError::Io)?;
        temp_file
            .persist(&self.csi_config_path)
            .map_err(|e| CtlError::Io(e.error))?;

        info!("CSI config written to {}", self.csi_config_path);

        self.reload_ctld().await?;

        Ok(())
    }

    /// Reload ctld configuration
    async fn reload_ctld(&self) -> Result<()> {
        debug!("Reloading ctld configuration");

        let output = Command::new("service")
            .args(["ctld", "reload"])
            .output()
            .await?;

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

// ============================================================================
// Serialized Config Writer
// ============================================================================

/// Default debounce duration for config writes.
/// Multiple write requests within this window are batched into one write.
const CONFIG_WRITE_DEBOUNCE_MS: u64 = 50;

/// A write request with an optional response channel.
struct WriteRequest {
    /// Channel to send the result back to the caller.
    /// If None, this is a fire-and-forget request.
    response_tx: Option<oneshot::Sender<Result<()>>>,
}

/// Handle for requesting config writes.
///
/// This is a cloneable sender that can be passed to multiple tasks.
/// Write requests are debounced and serialized by the background writer task.
#[derive(Clone)]
pub struct ConfigWriterHandle {
    tx: mpsc::Sender<WriteRequest>,
}

impl ConfigWriterHandle {
    /// Request a config write and wait for completion.
    ///
    /// This blocks until the config is written and ctld is reloaded.
    /// Use this for CSI operations that must guarantee the volume is
    /// accessible before returning success.
    ///
    /// Multiple concurrent requests are batched - all waiters receive
    /// the result of the same write operation.
    pub async fn write_config(&self) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(WriteRequest {
                response_tx: Some(response_tx),
            })
            .await
            .map_err(|_| CtlError::ConfigError("config writer task shut down".into()))?;

        response_rx
            .await
            .map_err(|_| CtlError::ConfigError("config writer task dropped response".into()))?
    }

    /// Request a config write without waiting for completion.
    ///
    /// Use this only for non-critical operations where you don't need
    /// to guarantee the write completed before continuing.
    pub fn request_write_async(&self) {
        let _ = self.tx.try_send(WriteRequest { response_tx: None });
    }
}

/// Spawn the background config writer task.
///
/// Returns a handle that can be used to request config writes.
/// The task will run until the handle is dropped (all senders closed).
///
/// # Arguments
/// * `ctl_manager` - Arc to the CtlManager (for calling write_config)
/// * `debounce_ms` - Debounce duration in milliseconds (0 to disable)
pub fn spawn_config_writer(
    ctl_manager: Arc<TokioRwLock<CtlManager>>,
    debounce_ms: Option<u64>,
) -> ConfigWriterHandle {
    let (tx, rx) = mpsc::channel::<WriteRequest>(32);
    let debounce = Duration::from_millis(debounce_ms.unwrap_or(CONFIG_WRITE_DEBOUNCE_MS));

    tokio::spawn(config_writer_task(ctl_manager, rx, debounce));

    ConfigWriterHandle { tx }
}

/// Background task that handles serialized config writes with debouncing.
async fn config_writer_task(
    ctl_manager: Arc<TokioRwLock<CtlManager>>,
    mut rx: mpsc::Receiver<WriteRequest>,
    debounce: Duration,
) {
    info!("Config writer task started (debounce: {:?})", debounce);

    while let Some(first_request) = rx.recv().await {
        // Collect response channels from this batch
        let mut response_channels: Vec<oneshot::Sender<Result<()>>> = Vec::new();
        if let Some(tx) = first_request.response_tx {
            response_channels.push(tx);
        }

        // Debounce: wait for more requests to batch
        if !debounce.is_zero() {
            tokio::time::sleep(debounce).await;
        }

        // Drain any pending requests (they'll be handled by this write)
        while let Ok(req) = rx.try_recv() {
            if let Some(tx) = req.response_tx {
                response_channels.push(tx);
            }
        }

        if !response_channels.is_empty() {
            debug!(
                "Batching {} write requests into single operation",
                response_channels.len()
            );
        }

        // Perform the actual write
        let result = {
            let ctl = ctl_manager.read().await;
            ctl.write_config().await
        };

        // Log the result
        match &result {
            Ok(()) => debug!("Config write completed successfully"),
            Err(e) => error!("Config write failed: {}", e),
        }

        // Notify all waiters with the result
        // Convert the result to a cloneable form (error as string)
        let send_result: Result<()> = match &result {
            Ok(()) => Ok(()),
            Err(e) => Err(CtlError::ConfigError(e.to_string())),
        };

        for tx in response_channels {
            // Clone the wrapped result for each waiter
            let _ = tx.send(match &send_result {
                Ok(()) => Ok(()),
                Err(e) => Err(CtlError::ConfigError(e.to_string())),
            });
        }
    }

    info!("Config writer task shutting down");
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
            ctl_options: CtlOptions::default(),
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
            ctl_options: CtlOptions::default(),
        };

        assert!(export.auth.is_some());
        assert_eq!(export.auth.auth_group_name("vol2"), "ag-vol2");
    }
}
