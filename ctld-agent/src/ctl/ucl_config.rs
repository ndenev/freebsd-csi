//! UCL configuration file management for ctld.
//!
//! This module handles reading and writing ctld UCL configuration files,
//! allowing CSI-managed targets to coexist with user-managed targets.

use std::fmt::Write as FmtWrite;
use std::fs;
use std::io::BufRead;
use std::path::Path;

use super::error::{CtlError, Result};

/// Default config file path
pub const DEFAULT_CONFIG_PATH: &str = "/etc/ctl.ucl";

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
    pub fn to_ucl_string(&self) -> String {
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
        s
    }
}

/// Represents an NVMeoF subsystem in UCL format (if ctld supports it)
#[derive(Debug, Clone)]
pub struct NvmeSubsystemUcl {
    pub nqn: String,
    pub namespaces: Vec<NvmeNamespaceUcl>,
}

/// Represents an NVMe namespace
#[derive(Debug, Clone)]
pub struct NvmeNamespaceUcl {
    pub id: u32,
    pub path: String,
}

impl NvmeSubsystemUcl {
    /// Generate UCL string representation of this subsystem
    /// Note: ctld may not support NVMeoF via config - verify with FreeBSD docs
    pub fn to_ucl_string(&self) -> String {
        let mut s = String::new();
        writeln!(s, "# NVMeoF subsystem (may require ctladm for now)").unwrap();
        writeln!(s, "# nqn: {}", self.nqn).unwrap();
        for ns in &self.namespaces {
            writeln!(s, "# namespace {}: {}", ns.id, ns.path).unwrap();
        }
        s
    }
}

/// Manager for UCL configuration files
pub struct UclConfigManager {
    pub config_path: String,
    pub auth_group: String,
    pub portal_group: String,
}

impl UclConfigManager {
    /// Create a new UclConfigManager
    pub fn new(config_path: String, auth_group: String, portal_group: String) -> Self {
        Self {
            config_path,
            auth_group,
            portal_group,
        }
    }

    /// Read the current config file, extracting non-CSI content
    pub fn read_user_config(&self) -> Result<String> {
        let path = Path::new(&self.config_path);
        if !path.exists() {
            return Ok(String::new());
        }

        let file = fs::File::open(path).map_err(|e| {
            CtlError::CommandFailed(format!("Failed to open {}: {}", self.config_path, e))
        })?;

        let reader = std::io::BufReader::new(file);
        let mut user_content = String::new();
        let mut in_csi_section = false;

        for line in reader.lines() {
            let line = line.map_err(|e| {
                CtlError::CommandFailed(format!("Failed to read {}: {}", self.config_path, e))
            })?;

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

    /// Write the config file with user content + CSI-managed targets
    pub fn write_config(&self, user_content: &str, targets: &[IscsiTargetUcl]) -> Result<()> {
        let mut content = user_content.to_string();

        // Ensure newline before CSI section
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }

        // Add CSI-managed section
        content.push_str(CSI_SECTION_START);
        content.push('\n');

        for target in targets {
            content.push_str(&target.to_ucl_string());
            content.push('\n');
        }

        content.push_str(CSI_SECTION_END);
        content.push('\n');

        // Write atomically via temp file
        let temp_path = format!("{}.tmp", self.config_path);
        fs::write(&temp_path, &content).map_err(|e| {
            CtlError::CommandFailed(format!("Failed to write {}: {}", temp_path, e))
        })?;

        fs::rename(&temp_path, &self.config_path).map_err(|e| {
            CtlError::CommandFailed(format!(
                "Failed to rename {} to {}: {}",
                temp_path, self.config_path, e
            ))
        })?;

        Ok(())
    }

    /// Create an IscsiTargetUcl with the manager's default auth/portal groups
    pub fn create_target(&self, iqn: &str, device_path: &str, lun_id: u32) -> IscsiTargetUcl {
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

        let ucl = target.to_ucl_string();
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

        let ucl = target.to_ucl_string();
        assert!(ucl.contains("lun 0"));
        assert!(ucl.contains("lun 1"));
        assert!(ucl.contains("vol2-data"));
        assert!(ucl.contains("vol2-log"));
        assert!(ucl.contains("blocksize = 4096"));
    }

    #[test]
    fn test_ucl_config_manager_create_target() {
        let manager = UclConfigManager::new(
            "/etc/ctl.ucl".to_string(),
            "ag0".to_string(),
            "pg0".to_string(),
        );

        let target = manager.create_target(
            "iqn.2024-01.org.freebsd.csi:test",
            "/dev/zvol/tank/test",
            0,
        );

        assert_eq!(target.iqn, "iqn.2024-01.org.freebsd.csi:test");
        assert_eq!(target.auth_group, "ag0");
        assert_eq!(target.portal_group, "pg0");
        assert_eq!(target.luns.len(), 1);
        assert_eq!(target.luns[0].path, "/dev/zvol/tank/test");
    }
}
