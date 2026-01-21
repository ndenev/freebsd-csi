//! UCL configuration file management for ctld.
//!
//! This module handles reading and writing ctld UCL configuration files,
//! allowing CSI-managed targets to coexist with user-managed targets.

use std::fmt::Write;
use std::fs;
use std::io::BufRead;
use std::path::Path;

use super::error::{CtlError, Result};

/// Default config file path
#[allow(dead_code)] // Useful constant for external users
pub const DEFAULT_CONFIG_PATH: &str = "/etc/ctl.ucl";

/// Validate a string for safe use in UCL configuration.
/// Rejects characters that could corrupt UCL syntax: ", {, }, \
/// Also validates reasonable length.
fn validate_ucl_string(value: &str, field_name: &str) -> Result<()> {
    if value.is_empty() {
        return Err(CtlError::ConfigError(format!(
            "{} cannot be empty",
            field_name
        )));
    }

    // Maximum reasonable length
    if value.len() > 1024 {
        return Err(CtlError::ConfigError(format!(
            "{} '{}...' exceeds maximum length of 1024 characters",
            field_name,
            &value[..50]
        )));
    }

    // Reject characters that could corrupt UCL syntax
    const FORBIDDEN_CHARS: &[char] = &['"', '{', '}', '\\'];
    for c in FORBIDDEN_CHARS {
        if value.contains(*c) {
            return Err(CtlError::ConfigError(format!(
                "{} contains forbidden character '{}': {}",
                field_name, c, value
            )));
        }
    }

    Ok(())
}

/// Marker comment for CSI-managed section start
const CSI_SECTION_START: &str = "# BEGIN CSI-MANAGED TARGETS - DO NOT EDIT";
/// Marker comment for CSI-managed section end
const CSI_SECTION_END: &str = "# END CSI-MANAGED TARGETS";

/// Represents a LUN in UCL format
#[derive(Debug, Clone)]
pub struct LunUcl {
    pub id: u32,
    pub path: String,
    pub blocksize: u32,
}

/// Represents an iSCSI target in UCL format
#[derive(Debug, Clone)]
pub struct IscsiTargetUcl {
    pub iqn: String,
    pub auth_group: String,
    pub portal_group: String,
    pub luns: Vec<LunUcl>,
}

impl IscsiTargetUcl {
    /// Generate UCL string representation of this target
    ///
    /// Returns an error if any field contains characters that could corrupt UCL syntax.
    pub fn to_ucl_string(&self) -> Result<String> {
        // Validate all string fields before generating UCL
        validate_ucl_string(&self.iqn, "IQN")?;
        validate_ucl_string(&self.auth_group, "auth-group")?;
        validate_ucl_string(&self.portal_group, "portal-group")?;
        for lun in &self.luns {
            validate_ucl_string(&lun.path, "LUN path")?;
        }

        let mut s = String::new();
        writeln!(s, "target \"{}\" {{", self.iqn).unwrap();
        writeln!(s, "    auth-group = \"{}\"", self.auth_group).unwrap();
        writeln!(s, "    portal-group = \"{}\"", self.portal_group).unwrap();
        for lun in &self.luns {
            writeln!(s, "    lun {} {{", lun.id).unwrap();
            writeln!(s, "        path = \"{}\"", lun.path).unwrap();
            writeln!(s, "        blocksize = {}", lun.blocksize).unwrap();
            writeln!(s, "    }}").unwrap();
        }
        writeln!(s, "}}").unwrap();
        Ok(s)
    }
}

/// Represents an NVMeoF controller in UCL format (FreeBSD 15.0+)
///
/// FreeBSD 15.0+ ctld supports NVMeoF via `controller` blocks:
/// ```ucl
/// controller "nqn.2024-01.org.freebsd.csi:vol-name" {
///     auth-group = "no-authentication"
///     transport-group = "tg0"
///     namespace {
///         1 {
///             path = "/dev/zvol/tank/csi/vol-name"
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct NvmeControllerUcl {
    pub nqn: String,
    pub auth_group: String,
    pub transport_group: String,
    pub namespaces: Vec<NvmeNamespaceUcl>,
}

/// Represents an NVMe namespace
#[derive(Debug, Clone)]
pub struct NvmeNamespaceUcl {
    pub id: u32,
    pub path: String,
}

impl NvmeControllerUcl {
    /// Generate UCL string representation of this controller (FreeBSD 15.0+)
    ///
    /// Returns an error if any field contains characters that could corrupt UCL syntax.
    pub fn to_ucl_string(&self) -> Result<String> {
        // Validate all string fields before generating UCL
        validate_ucl_string(&self.nqn, "NQN")?;
        validate_ucl_string(&self.auth_group, "auth-group")?;
        validate_ucl_string(&self.transport_group, "transport-group")?;
        for ns in &self.namespaces {
            validate_ucl_string(&ns.path, "namespace path")?;
        }

        let mut s = String::new();
        writeln!(s, "controller \"{}\" {{", self.nqn).unwrap();
        writeln!(s, "    auth-group = \"{}\"", self.auth_group).unwrap();
        writeln!(s, "    transport-group = \"{}\"", self.transport_group).unwrap();
        for ns in &self.namespaces {
            writeln!(s, "    namespace {{").unwrap();
            writeln!(s, "        {} {{", ns.id).unwrap();
            writeln!(s, "            path = \"{}\"", ns.path).unwrap();
            writeln!(s, "        }}").unwrap();
            writeln!(s, "    }}").unwrap();
        }
        writeln!(s, "}}").unwrap();
        Ok(s)
    }
}

/// Manager for UCL configuration files
pub struct UclConfigManager {
    pub config_path: String,
    pub auth_group: String,
    pub portal_group: String,
    /// Transport group for NVMeoF (FreeBSD 15.0+)
    pub transport_group: String,
}

impl UclConfigManager {
    /// Create a new UclConfigManager
    pub fn new(
        config_path: String,
        auth_group: String,
        portal_group: String,
        transport_group: String,
    ) -> Self {
        Self {
            config_path,
            auth_group,
            portal_group,
            transport_group,
        }
    }

    /// Read the current config file, extracting non-CSI content
    pub fn read_user_config(&self) -> Result<String> {
        let path = Path::new(&self.config_path);
        if !path.exists() {
            return Ok(String::new());
        }

        let file = fs::File::open(path)?;

        let reader = std::io::BufReader::new(file);
        let mut user_content = String::new();
        let mut in_csi_section = false;

        for line in reader.lines() {
            let line = line?;

            if line.trim() == CSI_SECTION_START {
                in_csi_section = true;
                continue;
            }
            if line.trim() == CSI_SECTION_END {
                in_csi_section = false;
                continue;
            }

            if !in_csi_section {
                user_content.push_str(&line);
                user_content.push('\n');
            }
        }

        Ok(user_content)
    }

    /// Write the config file with user content + CSI-managed iSCSI targets and NVMeoF controllers
    pub fn write_config(
        &self,
        user_content: &str,
        iscsi_targets: &[IscsiTargetUcl],
        nvme_controllers: &[NvmeControllerUcl],
    ) -> Result<()> {
        let mut content = user_content.to_string();

        // Ensure newline before CSI section
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }

        // Add CSI-managed section
        content.push_str(CSI_SECTION_START);
        content.push('\n');

        // Write iSCSI targets
        for target in iscsi_targets {
            content.push_str(&target.to_ucl_string()?);
            content.push('\n');
        }

        // Write NVMeoF controllers (FreeBSD 15.0+)
        for controller in nvme_controllers {
            content.push_str(&controller.to_ucl_string()?);
            content.push('\n');
        }

        content.push_str(CSI_SECTION_END);
        content.push('\n');

        // Write atomically via temp file
        let temp_path = format!("{}.tmp", self.config_path);
        fs::write(&temp_path, &content)?;

        fs::rename(&temp_path, &self.config_path).map_err(|e| {
            // Best effort cleanup of temp file on rename failure
            let _ = fs::remove_file(&temp_path);
            CtlError::Io(e)
        })?;

        Ok(())
    }

    /// Create an IscsiTargetUcl with the manager's default auth/portal groups
    #[allow(dead_code)] // Helper method for future use
    pub fn create_iscsi_target(&self, iqn: &str, device_path: &str, lun_id: u32) -> IscsiTargetUcl {
        IscsiTargetUcl {
            iqn: iqn.to_string(),
            auth_group: self.auth_group.clone(),
            portal_group: self.portal_group.clone(),
            luns: vec![LunUcl {
                id: lun_id,
                path: device_path.to_string(),
                blocksize: 512,
            }],
        }
    }

    /// Create an NvmeControllerUcl with the manager's default auth/transport groups
    pub fn create_nvme_controller(
        &self,
        nqn: &str,
        device_path: &str,
        namespace_id: u32,
    ) -> NvmeControllerUcl {
        NvmeControllerUcl {
            nqn: nqn.to_string(),
            auth_group: self.auth_group.clone(),
            transport_group: self.transport_group.clone(),
            namespaces: vec![NvmeNamespaceUcl {
                id: namespace_id,
                path: device_path.to_string(),
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_iscsi_target_ucl() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![LunUcl {
                id: 0,
                path: "/dev/zvol/tank/csi/vol1".to_string(),
                blocksize: 512,
            }],
        };

        let ucl = target.to_ucl_string().unwrap();
        assert!(ucl.contains("iqn.2024-01.org.freebsd.csi:vol1"));
        assert!(ucl.contains("auth-group"));
        assert!(ucl.contains("ag0"));
        assert!(ucl.contains("portal-group"));
        assert!(ucl.contains("pg0"));
        assert!(ucl.contains("/dev/zvol/tank/csi/vol1"));
    }

    #[test]
    fn test_generate_multi_lun_target() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol2".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![
                LunUcl {
                    id: 0,
                    path: "/dev/zvol/tank/csi/vol2-data".to_string(),
                    blocksize: 512,
                },
                LunUcl {
                    id: 1,
                    path: "/dev/zvol/tank/csi/vol2-log".to_string(),
                    blocksize: 4096,
                },
            ],
        };

        let ucl = target.to_ucl_string().unwrap();
        assert!(ucl.contains("lun 0"));
        assert!(ucl.contains("lun 1"));
        assert!(ucl.contains("vol2-data"));
        assert!(ucl.contains("vol2-log"));
        assert!(ucl.contains("blocksize = 4096"));
    }

    #[test]
    fn test_ucl_config_manager_create_iscsi_target() {
        let manager = UclConfigManager::new(
            "/etc/ctl.ucl".to_string(),
            "ag0".to_string(),
            "pg0".to_string(),
            "tg0".to_string(),
        );

        let target = manager
            .create_iscsi_target("iqn.2024-01.org.freebsd.csi:test", "/dev/zvol/tank/test", 0);

        assert_eq!(target.iqn, "iqn.2024-01.org.freebsd.csi:test");
        assert_eq!(target.auth_group, "ag0");
        assert_eq!(target.portal_group, "pg0");
        assert_eq!(target.luns.len(), 1);
        assert_eq!(target.luns[0].path, "/dev/zvol/tank/test");
    }

    #[test]
    fn test_ucl_config_manager_create_nvme_controller() {
        let manager = UclConfigManager::new(
            "/etc/ctl.ucl".to_string(),
            "no-authentication".to_string(),
            "pg0".to_string(),
            "tg0".to_string(),
        );

        let controller = manager.create_nvme_controller(
            "nqn.2024-01.org.freebsd.csi:test",
            "/dev/zvol/tank/test",
            1,
        );

        assert_eq!(controller.nqn, "nqn.2024-01.org.freebsd.csi:test");
        assert_eq!(controller.auth_group, "no-authentication");
        assert_eq!(controller.transport_group, "tg0");
        assert_eq!(controller.namespaces.len(), 1);
        assert_eq!(controller.namespaces[0].id, 1);
        assert_eq!(controller.namespaces[0].path, "/dev/zvol/tank/test");
    }

    #[test]
    fn test_nvme_controller_to_ucl_string() {
        let controller = NvmeControllerUcl {
            nqn: "nqn.2024-01.org.freebsd.csi:vol1".to_string(),
            auth_group: "no-authentication".to_string(),
            transport_group: "tg0".to_string(),
            namespaces: vec![NvmeNamespaceUcl {
                id: 1,
                path: "/dev/zvol/tank/csi/vol1".to_string(),
            }],
        };

        let ucl = controller.to_ucl_string().unwrap();
        assert!(ucl.contains("controller \"nqn.2024-01.org.freebsd.csi:vol1\""));
        assert!(ucl.contains("auth-group = \"no-authentication\""));
        assert!(ucl.contains("transport-group = \"tg0\""));
        assert!(ucl.contains("namespace {"));
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\""));
    }

    #[test]
    fn test_validate_ucl_string_valid() {
        // Valid strings
        assert!(validate_ucl_string("ag0", "test").is_ok());
        assert!(validate_ucl_string("iqn.2024-01.org.freebsd.csi:vol1", "test").is_ok());
        assert!(validate_ucl_string("/dev/zvol/tank/csi/vol1", "test").is_ok());
        assert!(validate_ucl_string("portal-group-1", "test").is_ok());
    }

    #[test]
    fn test_validate_ucl_string_invalid_chars() {
        // Double quotes
        assert!(validate_ucl_string("test\"value", "field").is_err());

        // Braces
        assert!(validate_ucl_string("test{value", "field").is_err());
        assert!(validate_ucl_string("test}value", "field").is_err());

        // Backslash
        assert!(validate_ucl_string("test\\value", "field").is_err());
    }

    #[test]
    fn test_validate_ucl_string_empty() {
        assert!(validate_ucl_string("", "field").is_err());
    }

    #[test]
    fn test_to_ucl_string_rejects_invalid_iqn() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol1\"injection".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![LunUcl {
                id: 0,
                path: "/dev/zvol/tank/csi/vol1".to_string(),
                blocksize: 512,
            }],
        };

        assert!(target.to_ucl_string().is_err());
    }

    #[test]
    fn test_to_ucl_string_rejects_invalid_path() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![LunUcl {
                id: 0,
                path: "/dev/zvol/tank/csi/vol1\"}; malicious".to_string(),
                blocksize: 512,
            }],
        };

        assert!(target.to_ucl_string().is_err());
    }
}
