//! NVMe over Fabrics (NVMeoF) export management module.
//!
//! This module provides functionality to export ZFS volumes as NVMeoF subsystems
//! using FreeBSD's CTL (CAM Target Layer) via ctladm.
//!
//! ## Note on Configuration Persistence
//!
//! FreeBSD 15.0+ ctld supports NVMeoF via UCL configuration using `controller` blocks
//! (instead of `target` for iSCSI) and `transport-group` (instead of `portal-group`).
//!
//! Currently, this implementation uses ctladm commands directly for simplicity.
//! This means NVMeoF exports are ephemeral and won't persist across reboots.
//! A future enhancement could add UCL config support similar to IscsiManager.
//!
//! For persistent NVMeoF configuration, manually add to `/etc/ctl.ucl`:
//! ```text
//! controller "nqn.2024-01.org.freebsd.csi:vol-name" {
//!     auth-group = "no-authentication"
//!     transport-group = "tg0"
//!     namespace {
//!         1 {
//!             path = "/dev/zvol/tank/csi/vol-name"
//!         }
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use super::error::{CtlError, Result};

/// Validate that a name is safe for use in CTL/NVMeoF commands.
/// For NQN format, allows: alphanumeric, underscore, hyphen, period, colon.
/// This is an allowlist approach to prevent command injection.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CtlError::InvalidName("name cannot be empty".into()));
    }

    // Maximum reasonable length for NQN names (NVMe spec allows up to 223 characters)
    if name.len() > 223 {
        return Err(CtlError::InvalidName(format!(
            "name '{}' exceeds maximum length of 223 characters",
            name
        )));
    }

    // Allowlist: alphanumeric, underscore, hyphen, period, colon (for NQN format)
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
    {
        return Err(CtlError::InvalidName(format!(
            "invalid characters in name '{}': only alphanumeric, underscore, hyphen, period, and colon allowed",
            name
        )));
    }

    // Prevent path traversal attempts
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

    // Must be under /dev/zvol/
    if !path.starts_with("/dev/zvol/") {
        return Err(CtlError::InvalidName(format!(
            "device path '{}' must be under /dev/zvol/",
            path
        )));
    }

    // Check for path traversal
    if path.contains("..") {
        return Err(CtlError::InvalidName(format!(
            "device path '{}' contains path traversal sequence",
            path
        )));
    }

    // Only allow safe characters in the path
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

/// Represents an NVMe subsystem configuration
#[derive(Debug, Clone)]
pub struct NvmeSubsystem {
    /// NVMe Qualified Name for the subsystem
    pub nqn: String,
    /// Namespace ID within the subsystem
    pub namespace_id: u32,
    /// Path to the backing device (e.g., /dev/zvol/tank/csi/vol1)
    pub device_path: String,
}

impl NvmeSubsystem {
    /// Create a new NVMe subsystem
    pub fn new(nqn: String, namespace_id: u32, device_path: String) -> Self {
        Self {
            nqn,
            namespace_id,
            device_path,
        }
    }
}

/// Manager for NVMeoF subsystem operations via CTL
#[derive(Debug)]
pub struct NvmeofManager {
    /// Base NQN prefix (e.g., "nqn.2024-01.com.example.storage")
    base_nqn: String,
    /// In-memory cache of active subsystems, keyed by NQN
    subsystems: RwLock<HashMap<String, NvmeSubsystem>>,
}

impl NvmeofManager {
    /// Create a new NvmeofManager with the given base NQN
    pub fn new(base_nqn: String) -> Self {
        info!("Initializing NvmeofManager with base_nqn={}", base_nqn);

        Self {
            base_nqn,
            subsystems: RwLock::new(HashMap::new()),
        }
    }

    /// Generate an NQN for a volume
    ///
    /// # Arguments
    /// * `base_nqn` - The base NQN prefix
    /// * `volume_name` - The volume name (slashes will be replaced with hyphens)
    ///
    /// # Returns
    /// A fully qualified NQN string
    pub fn generate_nqn(base_nqn: &str, volume_name: &str) -> String {
        format!("{}:{}", base_nqn, volume_name.replace('/', "-"))
    }

    /// Export a ZFS volume as an NVMeoF subsystem
    ///
    /// # Arguments
    /// * `volume_name` - The volume name (used to generate NQN)
    /// * `device_path` - Path to the backing device
    /// * `namespace_id` - Namespace ID for the volume
    ///
    /// # Returns
    /// The created NvmeSubsystem on success
    #[instrument(skip(self))]
    pub fn export_volume(
        &self,
        volume_name: &str,
        device_path: &str,
        namespace_id: u32,
    ) -> Result<NvmeSubsystem> {
        // Validate inputs
        validate_name(volume_name)?;
        validate_device_path(device_path)?;

        let nqn = Self::generate_nqn(&self.base_nqn, volume_name);
        debug!(
            "Exporting volume {} as NVMeoF subsystem {}",
            volume_name, nqn
        );

        // Validate generated NQN
        validate_name(&nqn)?;

        // Check if subsystem already exists
        {
            let subsystems = self.subsystems.read().unwrap();
            if subsystems.contains_key(&nqn) {
                return Err(CtlError::TargetExists(nqn));
            }
        }

        // Create the subsystem via ctladm
        self.create_subsystem_live(&nqn, device_path, volume_name)?;

        // Build subsystem configuration
        let subsystem = NvmeSubsystem::new(nqn.clone(), namespace_id, device_path.to_string());

        // Store in cache
        {
            let mut subsystems = self.subsystems.write().unwrap();
            subsystems.insert(nqn.clone(), subsystem.clone());
        }

        info!(
            "Successfully exported {} as NVMeoF subsystem {}",
            volume_name, nqn
        );
        Ok(subsystem)
    }

    /// Unexport an NVMeoF subsystem (remove it)
    ///
    /// # Arguments
    /// * `nqn` - The NQN of the subsystem to remove
    #[instrument(skip(self))]
    pub fn unexport_volume(&self, nqn: &str) -> Result<()> {
        // Validate input
        validate_name(nqn)?;

        debug!("Unexporting NVMeoF subsystem {}", nqn);

        // Verify subsystem exists in cache
        {
            let subsystems = self.subsystems.read().unwrap();
            if !subsystems.contains_key(nqn) {
                return Err(CtlError::TargetNotFound(nqn.to_string()));
            }
        }

        // Remove the subsystem via ctladm
        self.remove_subsystem_live(nqn)?;

        // Remove from cache
        {
            let mut subsystems = self.subsystems.write().unwrap();
            subsystems.remove(nqn);
        }

        info!("Successfully unexported NVMeoF subsystem {}", nqn);
        Ok(())
    }

    /// Get a subsystem by NQN
    ///
    /// # Arguments
    /// * `nqn` - The NQN of the subsystem to retrieve
    ///
    /// # Returns
    /// Some(NvmeSubsystem) if found, None otherwise
    pub fn get_subsystem(&self, nqn: &str) -> Option<NvmeSubsystem> {
        let subsystems = self.subsystems.read().unwrap();
        subsystems.get(nqn).cloned()
    }

    /// List all active subsystems
    ///
    /// # Returns
    /// A vector of all NvmeSubsystem instances
    pub fn list_subsystems(&self) -> Vec<NvmeSubsystem> {
        let subsystems = self.subsystems.read().unwrap();
        subsystems.values().cloned().collect()
    }

    /// Get the base NQN
    pub fn base_nqn(&self) -> &str {
        &self.base_nqn
    }

    /// Create a subsystem via ctladm (live operation)
    fn create_subsystem_live(
        &self,
        nqn: &str,
        device_path: &str,
        volume_name: &str,
    ) -> Result<()> {
        // ctladm create -b block -o file=<path> -o vendor=FreeBSD -o product=<name> -S <nqn>
        debug!(
            "Running ctladm create for subsystem {} with device {}",
            nqn, device_path
        );

        let output = Command::new("ctladm")
            .args([
                "create",
                "-b",
                "block",
                "-o",
                &format!("file={}", device_path),
                "-o",
                "vendor=FreeBSD",
                "-o",
                &format!("product={}", volume_name),
                "-S",
                nqn,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("already exists") || stderr.contains("in use") {
                return Err(CtlError::TargetExists(nqn.to_string()));
            }
            return Err(CtlError::CommandFailed(format!(
                "ctladm create failed: {}",
                stderr
            )));
        }

        debug!("Created NVMeoF subsystem {}", nqn);
        Ok(())
    }

    /// Remove a subsystem via ctladm (live operation)
    fn remove_subsystem_live(&self, nqn: &str) -> Result<()> {
        // ctladm remove -b block -S <nqn>
        debug!("Running ctladm remove for subsystem {}", nqn);

        let output = Command::new("ctladm")
            .args(["remove", "-b", "block", "-S", nqn])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("not found") || stderr.contains("does not exist") {
                return Err(CtlError::TargetNotFound(nqn.to_string()));
            }
            return Err(CtlError::CommandFailed(format!(
                "ctladm remove failed: {}",
                stderr
            )));
        }

        debug!("Removed NVMeoF subsystem {}", nqn);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_name_valid() {
        // Valid names
        assert!(validate_name("volume1").is_ok());
        assert!(validate_name("vol-1").is_ok());
        assert!(validate_name("vol_1").is_ok());
        assert!(validate_name("vol.1").is_ok());
        assert!(validate_name("nqn.2024-01.com.example:target").is_ok());
        assert!(validate_name("Vol-1_test.target:lun0").is_ok());
    }

    #[test]
    fn test_validate_name_invalid() {
        // Empty name
        assert!(validate_name("").is_err());

        // Invalid characters
        assert!(validate_name("vol/name").is_err());
        assert!(validate_name("vol@snap").is_err());
        assert!(validate_name("vol name").is_err());
        assert!(validate_name("vol;rm -rf /").is_err());
        assert!(validate_name("$(whoami)").is_err());
        assert!(validate_name("vol`id`").is_err());
        assert!(validate_name("vol|cat").is_err());
        assert!(validate_name("vol>file").is_err());
        assert!(validate_name("vol<file").is_err());
        assert!(validate_name("vol&bg").is_err());

        // Path traversal
        assert!(validate_name("..").is_err());
        assert!(validate_name("vol../other").is_err());
    }

    #[test]
    fn test_validate_name_length() {
        // Max length is 223
        let long_name = "a".repeat(223);
        assert!(validate_name(&long_name).is_ok());

        let too_long = "a".repeat(224);
        assert!(validate_name(&too_long).is_err());
    }

    #[test]
    fn test_validate_device_path_valid() {
        assert!(validate_device_path("/dev/zvol/tank/vol1").is_ok());
        assert!(validate_device_path("/dev/zvol/tank/csi/pvc-123").is_ok());
        assert!(validate_device_path("/dev/zvol/tank/csi/vol-1_test.snap").is_ok());
    }

    #[test]
    fn test_validate_device_path_invalid() {
        // Empty path
        assert!(validate_device_path("").is_err());

        // Not under /dev/zvol/
        assert!(validate_device_path("/dev/da0").is_err());
        assert!(validate_device_path("/tmp/fake").is_err());
        assert!(validate_device_path("relative/path").is_err());

        // Path traversal
        assert!(validate_device_path("/dev/zvol/../etc/passwd").is_err());
        assert!(validate_device_path("/dev/zvol/tank/../other").is_err());

        // Invalid characters
        assert!(validate_device_path("/dev/zvol/tank/$(id)").is_err());
        assert!(validate_device_path("/dev/zvol/tank/vol;rm").is_err());
    }

    #[test]
    fn test_generate_nqn() {
        assert_eq!(
            NvmeofManager::generate_nqn("nqn.2024-01.com.example.storage", "vol1"),
            "nqn.2024-01.com.example.storage:vol1"
        );

        // Test with slashes in volume name (should be replaced with hyphens)
        assert_eq!(
            NvmeofManager::generate_nqn("nqn.2024-01.com.example.storage", "tank/csi/vol1"),
            "nqn.2024-01.com.example.storage:tank-csi-vol1"
        );
    }

    #[test]
    fn test_nvme_subsystem_new() {
        let subsystem = NvmeSubsystem::new(
            "nqn.2024-01.com.example:vol1".to_string(),
            1,
            "/dev/zvol/tank/vol1".to_string(),
        );

        assert_eq!(subsystem.nqn, "nqn.2024-01.com.example:vol1");
        assert_eq!(subsystem.namespace_id, 1);
        assert_eq!(subsystem.device_path, "/dev/zvol/tank/vol1");
    }

    #[test]
    fn test_nvmeof_manager_new() {
        let manager = NvmeofManager::new("nqn.2024-01.com.example.storage".to_string());
        assert_eq!(manager.base_nqn(), "nqn.2024-01.com.example.storage");
    }

    #[test]
    fn test_list_subsystems_empty() {
        let manager = NvmeofManager::new("nqn.2024-01.com.example".to_string());
        assert!(manager.list_subsystems().is_empty());
    }

    #[test]
    fn test_get_subsystem_not_found() {
        let manager = NvmeofManager::new("nqn.2024-01.com.example".to_string());
        let result = manager.get_subsystem("nqn.2024-01.com.example:nonexistent");
        assert!(result.is_none());
    }

    #[test]
    fn test_unexport_volume_not_found() {
        let manager = NvmeofManager::new("nqn.2024-01.com.example".to_string());
        let result = manager.unexport_volume("nqn.2024-01.com.example:nonexistent");
        assert!(result.is_err());
        match result {
            Err(CtlError::TargetNotFound(name)) => {
                assert_eq!(name, "nqn.2024-01.com.example:nonexistent")
            }
            _ => panic!("expected TargetNotFound error"),
        }
    }

    #[test]
    fn test_export_volume_invalid_name() {
        let manager = NvmeofManager::new("nqn.2024-01.com.example".to_string());
        let result = manager.export_volume("vol;rm -rf /", "/dev/zvol/tank/vol1", 1);
        assert!(result.is_err());
        match result {
            Err(CtlError::InvalidName(_)) => {}
            _ => panic!("expected InvalidName error"),
        }
    }

    #[test]
    fn test_export_volume_invalid_device_path() {
        let manager = NvmeofManager::new("nqn.2024-01.com.example".to_string());
        let result = manager.export_volume("vol1", "/tmp/fake", 1);
        assert!(result.is_err());
        match result {
            Err(CtlError::InvalidName(_)) => {}
            _ => panic!("expected InvalidName error"),
        }
    }
}
