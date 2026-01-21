//! UCL configuration types for ctld.
//!
//! This module provides types for reading and writing ctld UCL configuration,
//! using uclicious for parsing and a ToUcl trait for serialization.

use std::collections::HashMap;
use std::fmt::Write;
use std::fs;
use std::path::Path;

use uclicious::{DEFAULT_DUPLICATE_STRATEGY, Priority, Uclicious};

use super::error::{CtlError, Result};

// ============================================================================
// ToUcl trait for serialization
// ============================================================================

/// Trait for types that can be serialized to UCL format.
pub trait ToUcl {
    /// Serialize to UCL string with the given indentation level.
    fn to_ucl(&self, indent: usize) -> String;

    /// Serialize to UCL string with no indentation.
    #[allow(dead_code)]
    fn to_ucl_string(&self) -> String {
        self.to_ucl(0)
    }
}

/// Helper to create indentation string
fn indent(level: usize) -> String {
    "    ".repeat(level)
}

/// Escape a string value for UCL (currently just wraps in quotes)
fn ucl_quote(s: &str) -> String {
    // UCL strings are quoted with double quotes
    // We validate that strings don't contain problematic characters elsewhere
    format!("\"{}\"", s)
}

// ============================================================================
// LUN / Namespace types
// ============================================================================

/// A LUN (Logical Unit Number) in an iSCSI target
#[derive(Debug, Clone, Uclicious)]
pub struct Lun {
    /// Path to the backing device
    pub path: String,
    /// Block size (optional, defaults to 512)
    #[ucl(default)]
    pub blocksize: Option<u32>,
}

impl Lun {
    /// Create a new LUN
    pub fn new(path: String) -> Self {
        Self {
            path,
            blocksize: None,
        }
    }

    /// Create a new LUN with explicit blocksize
    #[allow(dead_code)]
    pub fn with_blocksize(path: String, blocksize: u32) -> Self {
        Self {
            path,
            blocksize: Some(blocksize),
        }
    }
}

impl ToUcl for Lun {
    fn to_ucl(&self, level: usize) -> String {
        let mut s = String::new();
        let ind = indent(level);
        writeln!(s, "{}path = {};", ind, ucl_quote(&self.path)).unwrap();
        if let Some(bs) = self.blocksize {
            writeln!(s, "{}blocksize = {};", ind, bs).unwrap();
        }
        s
    }
}

/// An NVMe namespace
#[derive(Debug, Clone, Uclicious)]
pub struct Namespace {
    /// Path to the backing device
    pub path: String,
}

impl Namespace {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

impl ToUcl for Namespace {
    fn to_ucl(&self, level: usize) -> String {
        let ind = indent(level);
        format!("{}path = {};\n", ind, ucl_quote(&self.path))
    }
}

// ============================================================================
// Target / Controller types
// ============================================================================

/// An iSCSI target
#[derive(Debug, Clone, Uclicious)]
pub struct Target {
    /// Auth group name
    #[ucl(path = "auth-group")]
    pub auth_group: String,
    /// Portal group name
    #[ucl(path = "portal-group")]
    pub portal_group: String,
    /// LUNs indexed by ID
    #[ucl(default)]
    pub lun: HashMap<String, Lun>,
}

impl Target {
    /// Create a new target with a single LUN
    pub fn new(auth_group: String, portal_group: String, lun_id: u32, device_path: String) -> Self {
        let mut lun = HashMap::new();
        lun.insert(lun_id.to_string(), Lun::new(device_path));
        Self {
            auth_group,
            portal_group,
            lun,
        }
    }
}

impl ToUcl for Target {
    fn to_ucl(&self, level: usize) -> String {
        let mut s = String::new();
        let ind = indent(level);

        writeln!(s, "{}auth-group = {};", ind, ucl_quote(&self.auth_group)).unwrap();
        writeln!(
            s,
            "{}portal-group = {};",
            ind,
            ucl_quote(&self.portal_group)
        )
        .unwrap();

        // Sort LUN IDs for consistent output
        let mut lun_ids: Vec<_> = self.lun.keys().collect();
        lun_ids.sort_by_key(|k| k.parse::<u32>().unwrap_or(0));

        for lun_id in lun_ids {
            if let Some(lun) = self.lun.get(lun_id) {
                writeln!(s, "{}lun {} {{", ind, lun_id).unwrap();
                s.push_str(&lun.to_ucl(level + 1));
                writeln!(s, "{}}}", ind).unwrap();
            }
        }

        s
    }
}

/// An NVMeoF controller (FreeBSD 15.0+)
#[derive(Debug, Clone, Uclicious)]
pub struct Controller {
    /// Auth group name
    #[ucl(path = "auth-group")]
    pub auth_group: String,
    /// Transport group name
    #[ucl(path = "transport-group")]
    pub transport_group: String,
    /// Namespaces indexed by ID
    #[ucl(default)]
    pub namespace: HashMap<String, Namespace>,
}

impl Controller {
    /// Create a new controller with a single namespace
    pub fn new(
        auth_group: String,
        transport_group: String,
        ns_id: u32,
        device_path: String,
    ) -> Self {
        let mut namespace = HashMap::new();
        namespace.insert(ns_id.to_string(), Namespace::new(device_path));
        Self {
            auth_group,
            transport_group,
            namespace,
        }
    }
}

impl ToUcl for Controller {
    fn to_ucl(&self, level: usize) -> String {
        let mut s = String::new();
        let ind = indent(level);

        writeln!(s, "{}auth-group = {};", ind, ucl_quote(&self.auth_group)).unwrap();
        writeln!(
            s,
            "{}transport-group = {};",
            ind,
            ucl_quote(&self.transport_group)
        )
        .unwrap();

        // Sort namespace IDs for consistent output
        let mut ns_ids: Vec<_> = self.namespace.keys().collect();
        ns_ids.sort_by_key(|k| k.parse::<u32>().unwrap_or(0));

        for ns_id in ns_ids {
            if let Some(ns) = self.namespace.get(ns_id) {
                writeln!(s, "{}namespace {} {{", ind, ns_id).unwrap();
                s.push_str(&ns.to_ucl(level + 1));
                writeln!(s, "{}}}", ind).unwrap();
            }
        }

        s
    }
}

// ============================================================================
// Top-level config
// ============================================================================

/// The complete ctld UCL configuration.
///
/// Note: Currently unused as ZFS user properties are the source of truth.
/// Kept for potential debugging/recovery purposes.
#[allow(dead_code)]
#[derive(Debug, Clone, Default, Uclicious)]
pub struct CtlConfig {
    /// iSCSI targets indexed by IQN
    #[ucl(default)]
    pub target: HashMap<String, Target>,

    /// NVMeoF controllers indexed by NQN (FreeBSD 15.0+)
    #[ucl(default)]
    pub controller: HashMap<String, Controller>,
}

#[allow(dead_code)]
impl CtlConfig {
    /// Parse a UCL config file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Ok(Self::default());
        }

        // Read file content
        let content = fs::read_to_string(path).map_err(|e| {
            CtlError::ConfigError(format!("Failed to read {}: {}", path.display(), e))
        })?;

        // Use uclicious builder to parse
        let mut builder = Self::builder()
            .map_err(|e| CtlError::ParseError(format!("Failed to create parser: {}", e)))?;

        builder
            .add_chunk_full(&content, Priority::default(), DEFAULT_DUPLICATE_STRATEGY)
            .map_err(|e| {
                CtlError::ParseError(format!("Failed to parse {}: {}", path.display(), e))
            })?;

        builder
            .build()
            .map_err(|e| CtlError::ParseError(format!("Failed to build config: {}", e)))
    }

    /// Get all iSCSI targets matching a prefix
    pub fn targets_with_prefix(&self, prefix: &str) -> impl Iterator<Item = (&String, &Target)> {
        self.target
            .iter()
            .filter(move |(iqn, _)| iqn.starts_with(prefix))
    }

    /// Get all NVMeoF controllers matching a prefix
    pub fn controllers_with_prefix(
        &self,
        prefix: &str,
    ) -> impl Iterator<Item = (&String, &Controller)> {
        self.controller
            .iter()
            .filter(move |(nqn, _)| nqn.starts_with(prefix))
    }
}

// ============================================================================
// UCL Config Manager - handles file I/O with section preservation
// ============================================================================

/// Marker comment for CSI-managed section start
const CSI_SECTION_START: &str = "# BEGIN CSI-MANAGED TARGETS - DO NOT EDIT";
/// Marker comment for CSI-managed section end
const CSI_SECTION_END: &str = "# END CSI-MANAGED TARGETS";

/// Manager for UCL configuration files.
/// Preserves user-managed sections while updating CSI-managed targets.
pub struct UclConfigManager {
    pub config_path: String,
}

impl UclConfigManager {
    pub fn new(config_path: String) -> Self {
        Self { config_path }
    }

    /// Read the user-managed portion of the config (excluding CSI section)
    pub fn read_user_content(&self) -> Result<String> {
        let path = Path::new(&self.config_path);
        if !path.exists() {
            return Ok(String::new());
        }

        let content = fs::read_to_string(path)?;
        let mut user_content = String::new();
        let mut in_csi_section = false;

        for line in content.lines() {
            if line.trim() == CSI_SECTION_START {
                in_csi_section = true;
                continue;
            }
            if line.trim() == CSI_SECTION_END {
                in_csi_section = false;
                continue;
            }
            if !in_csi_section {
                user_content.push_str(line);
                user_content.push('\n');
            }
        }

        Ok(user_content)
    }

    /// Write the config file with user content + CSI-managed targets
    pub fn write_config(
        &self,
        user_content: &str,
        iscsi_targets: &[(String, Target)],
        nvme_controllers: &[(String, Controller)],
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
        for (iqn, target) in iscsi_targets {
            writeln!(content, "target {} {{", ucl_quote(iqn)).unwrap();
            content.push_str(&target.to_ucl(1));
            writeln!(content, "}}").unwrap();
        }

        // Write NVMeoF controllers
        for (nqn, controller) in nvme_controllers {
            writeln!(content, "controller {} {{", ucl_quote(nqn)).unwrap();
            content.push_str(&controller.to_ucl(1));
            writeln!(content, "}}").unwrap();
        }

        content.push_str(CSI_SECTION_END);
        content.push('\n');

        // Write atomically via temp file
        let temp_path = format!("{}.tmp", self.config_path);
        fs::write(&temp_path, &content)?;

        fs::rename(&temp_path, &self.config_path).map_err(|e| {
            let _ = fs::remove_file(&temp_path);
            CtlError::Io(e)
        })?;

        Ok(())
    }
}

// ============================================================================
// Validation helpers
// ============================================================================

/// Validate a string for safe use in UCL configuration.
#[allow(dead_code)]
pub fn validate_ucl_string(value: &str, field_name: &str) -> Result<()> {
    if value.is_empty() {
        return Err(CtlError::ConfigError(format!(
            "{} cannot be empty",
            field_name
        )));
    }

    if value.len() > 1024 {
        return Err(CtlError::ConfigError(format!(
            "{} exceeds maximum length of 1024 characters",
            field_name
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lun_to_ucl() {
        let lun = Lun::new("/dev/zvol/tank/csi/vol1".to_string());
        let ucl = lun.to_ucl(0);
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\";"));
        assert!(!ucl.contains("blocksize"));

        let lun_with_bs = Lun::with_blocksize("/dev/zvol/tank/csi/vol1".to_string(), 4096);
        let ucl = lun_with_bs.to_ucl(0);
        assert!(ucl.contains("blocksize = 4096;"));
    }

    #[test]
    fn test_target_to_ucl() {
        let target = Target::new(
            "ag0".to_string(),
            "pg0".to_string(),
            0,
            "/dev/zvol/tank/csi/vol1".to_string(),
        );
        let ucl = target.to_ucl(0);

        assert!(ucl.contains("auth-group = \"ag0\";"));
        assert!(ucl.contains("portal-group = \"pg0\";"));
        assert!(ucl.contains("lun 0 {"));
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\";"));
    }

    #[test]
    fn test_controller_to_ucl() {
        let controller = Controller::new(
            "no-authentication".to_string(),
            "tg0".to_string(),
            1,
            "/dev/zvol/tank/csi/vol1".to_string(),
        );
        let ucl = controller.to_ucl(0);

        assert!(ucl.contains("auth-group = \"no-authentication\";"));
        assert!(ucl.contains("transport-group = \"tg0\";"));
        assert!(ucl.contains("namespace 1 {"));
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\";"));
    }

    #[test]
    fn test_validate_ucl_string() {
        assert!(validate_ucl_string("ag0", "test").is_ok());
        assert!(validate_ucl_string("iqn.2024-01.org.freebsd.csi:vol1", "test").is_ok());
        assert!(validate_ucl_string("", "test").is_err());
        assert!(validate_ucl_string("test\"value", "test").is_err());
        assert!(validate_ucl_string("test{value", "test").is_err());
    }
}
