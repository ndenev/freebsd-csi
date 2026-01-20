use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use super::config::{IscsiTarget, Lun, PortalGroup};
use super::error::{CtlError, Result};

/// Validate that a name is safe for use in CTL/iSCSI commands.
/// For IQN format, allows: alphanumeric, underscore, hyphen, period, colon.
/// This is an allowlist approach to prevent command injection.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(CtlError::InvalidName("name cannot be empty".into()));
    }

    // Maximum reasonable length for target names
    if name.len() > 223 {
        return Err(CtlError::InvalidName(format!(
            "name '{}' exceeds maximum length of 223 characters",
            name
        )));
    }

    // Allowlist: alphanumeric, underscore, hyphen, period, colon (for IQN format)
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

/// Manager for iSCSI target operations via CTL
pub struct IscsiManager {
    /// Base IQN prefix (e.g., "iqn.2024-01.com.example.storage")
    base_iqn: String,
    /// Portal group configuration
    portal_group: PortalGroup,
    /// In-memory cache of active targets
    targets: RwLock<HashMap<String, IscsiTarget>>,
}

impl IscsiManager {
    /// Create a new IscsiManager with the given base IQN and portal group
    pub fn new(base_iqn: String, portal_group: PortalGroup) -> Result<Self> {
        // Validate base IQN
        validate_name(&base_iqn)?;

        info!(
            "Initializing IscsiManager with base_iqn={}, portal_group={}",
            base_iqn, portal_group.name
        );

        Ok(Self {
            base_iqn,
            portal_group,
            targets: RwLock::new(HashMap::new()),
        })
    }

    /// Generate a full IQN for a volume name
    fn generate_iqn(&self, volume_name: &str) -> String {
        format!("{}:{}", self.base_iqn, volume_name)
    }

    /// Export a ZFS volume as an iSCSI target
    #[instrument(skip(self))]
    pub fn export_volume(
        &self,
        volume_name: &str,
        device_path: &str,
        lun_id: u32,
    ) -> Result<IscsiTarget> {
        // Validate inputs
        validate_name(volume_name)?;
        validate_device_path(device_path)?;

        let iqn = self.generate_iqn(volume_name);
        debug!("Exporting volume {} as iSCSI target {}", volume_name, iqn);

        // Check if target already exists
        {
            let targets = self.targets.read().unwrap();
            if targets.contains_key(volume_name) {
                return Err(CtlError::TargetExists(volume_name.to_string()));
            }
        }

        // Create the LUN via ctladm
        let ctl_lun_id = self.add_target_live(volume_name, device_path)?;

        // Build target configuration
        let mut lun = Lun::new(lun_id, device_path.to_string());
        lun.ctl_lun_id = Some(ctl_lun_id);

        let target = IscsiTarget::new(volume_name.to_string(), iqn)
            .with_portal_group(self.portal_group.tag)
            .with_lun(lun);

        // Store in cache
        {
            let mut targets = self.targets.write().unwrap();
            targets.insert(volume_name.to_string(), target.clone());
        }

        info!("Successfully exported {} as iSCSI target", volume_name);
        Ok(target)
    }

    /// Unexport an iSCSI target (remove it)
    #[instrument(skip(self))]
    pub fn unexport_volume(&self, target_name: &str) -> Result<()> {
        // Validate input
        validate_name(target_name)?;

        debug!("Unexporting iSCSI target {}", target_name);

        // Get target from cache to verify it exists (LUN ID is embedded in name)
        let _target = {
            let targets = self.targets.read().unwrap();
            targets
                .get(target_name)
                .cloned()
                .ok_or_else(|| CtlError::TargetNotFound(target_name.to_string()))?
        };

        // Remove the LUN via ctladm
        self.remove_target_live(target_name)?;

        // Remove from cache
        {
            let mut targets = self.targets.write().unwrap();
            targets.remove(target_name);
        }

        info!("Successfully unexported iSCSI target {}", target_name);
        Ok(())
    }

    /// Get a target by name
    pub fn get_target(&self, name: &str) -> Result<IscsiTarget> {
        validate_name(name)?;

        let targets = self.targets.read().unwrap();
        targets
            .get(name)
            .cloned()
            .ok_or_else(|| CtlError::TargetNotFound(name.to_string()))
    }

    /// List all active targets
    pub fn list_targets(&self) -> Vec<IscsiTarget> {
        let targets = self.targets.read().unwrap();
        targets.values().cloned().collect()
    }

    /// Get the portal group configuration
    pub fn portal_group(&self) -> &PortalGroup {
        &self.portal_group
    }

    /// Get the base IQN
    pub fn base_iqn(&self) -> &str {
        &self.base_iqn
    }

    /// Add a target/LUN via ctladm (live operation)
    fn add_target_live(&self, target_name: &str, device_path: &str) -> Result<u32> {
        // ctladm create -b block -o file=<path> -d <target_name>
        debug!(
            "Running ctladm create for target {} with device {}",
            target_name, device_path
        );

        let output = Command::new("ctladm")
            .args([
                "create",
                "-b",
                "block",
                "-o",
                &format!("file={}", device_path),
                "-d",
                target_name,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("already exists") || stderr.contains("in use") {
                return Err(CtlError::TargetExists(target_name.to_string()));
            }
            return Err(CtlError::CommandFailed(format!(
                "ctladm create failed: {}",
                stderr
            )));
        }

        // Parse the CTL LUN ID from output
        // Output format: "LUN created successfully\nbackend: block\nlun_id: <N>\n..."
        let stdout = String::from_utf8_lossy(&output.stdout);
        let lun_id = self.parse_lun_id(&stdout)?;

        debug!("Created CTL LUN {} for target {}", lun_id, target_name);
        Ok(lun_id)
    }

    /// Remove a target/LUN via ctladm (live operation)
    fn remove_target_live(&self, target_name: &str) -> Result<()> {
        // ctladm remove -b block -d <target_name>
        debug!("Running ctladm remove for target {}", target_name);

        let output = Command::new("ctladm")
            .args(["remove", "-b", "block", "-d", target_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("not found") || stderr.contains("does not exist") {
                return Err(CtlError::TargetNotFound(target_name.to_string()));
            }
            return Err(CtlError::CommandFailed(format!(
                "ctladm remove failed: {}",
                stderr
            )));
        }

        debug!("Removed CTL LUN for target {}", target_name);
        Ok(())
    }

    /// Reload ctld configuration
    #[allow(dead_code)]
    fn reload_ctld(&self) -> Result<()> {
        debug!("Reloading ctld configuration");

        let output = Command::new("service")
            .args(["ctld", "reload"])
            .output()?;

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

    /// Parse LUN ID from ctladm output
    fn parse_lun_id(&self, output: &str) -> Result<u32> {
        // Look for "lun_id: <N>" or "LUN ID: <N>" pattern
        for line in output.lines() {
            let line = line.trim().to_lowercase();
            if line.starts_with("lun_id:") || line.starts_with("lun id:") {
                let parts: Vec<&str> = line.split(':').collect();
                if parts.len() >= 2 {
                    let id_str = parts[1].trim();
                    return id_str.parse().map_err(|_| {
                        CtlError::ParseError(format!("invalid LUN ID: {}", id_str))
                    });
                }
            }
        }

        // If we can't find the LUN ID in the output, try to parse it differently
        // Some versions of ctladm just output the number
        if let Ok(id) = output.trim().parse::<u32>() {
            return Ok(id);
        }

        Err(CtlError::ParseError(format!(
            "could not find LUN ID in output: {}",
            output
        )))
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
        assert!(validate_name("iqn.2024-01.com.example:target").is_ok());
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
    fn test_generate_iqn() {
        let pg = PortalGroup::new(1, "pg1".to_string());
        let manager = IscsiManager {
            base_iqn: "iqn.2024-01.com.example.storage".to_string(),
            portal_group: pg,
            targets: RwLock::new(HashMap::new()),
        };

        assert_eq!(
            manager.generate_iqn("vol1"),
            "iqn.2024-01.com.example.storage:vol1"
        );
    }

    #[test]
    fn test_parse_lun_id() {
        let pg = PortalGroup::new(1, "pg1".to_string());
        let manager = IscsiManager {
            base_iqn: "iqn.2024-01.com.example".to_string(),
            portal_group: pg,
            targets: RwLock::new(HashMap::new()),
        };

        // Test typical ctladm output format
        let output = "LUN created successfully\nbackend: block\nlun_id: 5\ndevice_id: 12345";
        assert_eq!(manager.parse_lun_id(output).unwrap(), 5);

        // Test with different casing
        let output2 = "LUN ID: 10\nSome other info";
        assert_eq!(manager.parse_lun_id(output2).unwrap(), 10);

        // Test simple numeric output
        assert_eq!(manager.parse_lun_id("42").unwrap(), 42);

        // Test invalid output
        assert!(manager.parse_lun_id("no lun id here").is_err());
    }

    #[test]
    fn test_list_targets_empty() {
        let pg = PortalGroup::new(1, "pg1".to_string());
        let manager = IscsiManager {
            base_iqn: "iqn.2024-01.com.example".to_string(),
            portal_group: pg,
            targets: RwLock::new(HashMap::new()),
        };

        assert!(manager.list_targets().is_empty());
    }

    #[test]
    fn test_get_target_not_found() {
        let pg = PortalGroup::new(1, "pg1".to_string());
        let manager = IscsiManager {
            base_iqn: "iqn.2024-01.com.example".to_string(),
            portal_group: pg,
            targets: RwLock::new(HashMap::new()),
        };

        let result = manager.get_target("nonexistent");
        assert!(result.is_err());
        match result {
            Err(CtlError::TargetNotFound(name)) => assert_eq!(name, "nonexistent"),
            _ => panic!("expected TargetNotFound error"),
        }
    }
}
