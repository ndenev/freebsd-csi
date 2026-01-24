//! UCL configuration types for ctld.
//!
//! This module provides types for reading and writing ctld UCL configuration,
//! using uclicious for parsing and a ToUcl trait for serialization.

use std::collections::HashMap;
use std::fmt::Write;
use std::fs;
use std::io::Write as IoWrite;
use std::path::Path;

use tempfile::NamedTempFile;

use uclicious::{DEFAULT_DUPLICATE_STRATEGY, Priority, Uclicious};

use super::error::{CtlError, Result};

// ============================================================================
// ToUcl trait for serialization
// ============================================================================

/// Trait for types that can be serialized to UCL format.
pub trait ToUcl {
    /// Serialize to UCL string with the given indentation level.
    fn to_ucl(&self, indent: usize) -> String;
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
    /// Serial number for unique device identification
    #[ucl(default)]
    pub serial: Option<String>,
    /// Device ID for unique device identification (T10 vendor format)
    #[ucl(path = "device-id", default)]
    pub device_id: Option<String>,
}

impl Lun {
    /// Create a new LUN with a unique serial based on volume name
    pub fn new(path: String, volume_name: &str) -> Self {
        // Generate unique identifiers from volume name
        // Serial: Use first 16 chars of volume name (SCSI serial limit)
        // Device-ID: Use T10 vendor format for unique identification
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);

        Self {
            path,
            blocksize: None,
            serial: Some(serial),
            device_id: Some(device_id),
        }
    }

    /// Create a new LUN with explicit blocksize
    #[allow(dead_code)]
    pub fn with_blocksize(path: String, volume_name: &str, blocksize: u32) -> Self {
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);

        Self {
            path,
            blocksize: Some(blocksize),
            serial: Some(serial),
            device_id: Some(device_id),
        }
    }

    /// Generate a unique serial number from volume name.
    /// SCSI serial numbers are limited to 16 characters.
    /// Uses SHA-256 hash to ensure uniqueness across the full volume name.
    fn generate_serial(volume_name: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(volume_name.as_bytes());
        let hash = hasher.finalize();
        // Take first 8 bytes (16 hex chars) of SHA-256 hash
        hex::encode(&hash[..8])
    }

    /// Generate a device ID using T10 vendor format
    fn generate_device_id(volume_name: &str) -> String {
        // T10 vendor format: "FreeBSD <volume_name>"
        format!("FreeBSD {}", volume_name)
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
        if let Some(ref serial) = self.serial {
            writeln!(s, "{}serial = {};", ind, ucl_quote(serial)).unwrap();
        }
        if let Some(ref device_id) = self.device_id {
            writeln!(s, "{}device-id = {};", ind, ucl_quote(device_id)).unwrap();
        }
        s
    }
}

/// An NVMe namespace
#[derive(Debug, Clone, Uclicious)]
pub struct Namespace {
    /// Path to the backing device
    pub path: String,
    /// Device ID for unique namespace identification
    #[ucl(path = "device-id", default)]
    pub device_id: Option<String>,
}

impl Namespace {
    /// Create a new namespace with a unique device ID based on volume name
    pub fn new(path: String, volume_name: &str) -> Self {
        let device_id = Self::generate_device_id(volume_name);
        Self {
            path,
            device_id: Some(device_id),
        }
    }

    /// Generate a device ID for NVMe namespace
    /// NVMe uses NGUID/EUI64 for unique identification
    fn generate_device_id(volume_name: &str) -> String {
        // Use the volume name as a unique identifier
        // CTL will use this for the namespace's device-id
        format!("FreeBSD {}", volume_name)
    }
}

impl ToUcl for Namespace {
    fn to_ucl(&self, level: usize) -> String {
        let mut s = String::new();
        let ind = indent(level);
        writeln!(s, "{}path = {};", ind, ucl_quote(&self.path)).unwrap();
        if let Some(ref device_id) = self.device_id {
            writeln!(s, "{}device-id = {};", ind, ucl_quote(device_id)).unwrap();
        }
        s
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
    pub fn new(
        auth_group: String,
        portal_group: String,
        lun_id: u32,
        device_path: String,
        volume_name: &str,
    ) -> Self {
        let mut lun = HashMap::new();
        lun.insert(lun_id.to_string(), Lun::new(device_path, volume_name));
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
        volume_name: &str,
    ) -> Self {
        let mut namespace = HashMap::new();
        namespace.insert(ns_id.to_string(), Namespace::new(device_path, volume_name));
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
// Auth Group types
// ============================================================================

use super::types::{AuthConfig, IscsiChapAuth, NvmeAuth};

/// Authentication group for ctld.
///
/// Generates UCL auth-group blocks with CHAP credentials for iSCSI
/// or host-nqn access control for NVMeoF.
///
/// Note: FreeBSD 15's ctld does not yet support DH-HMAC-CHAP for NVMeoF.
/// NVMeoF auth-groups only support host-nqn and host-address restrictions.
#[derive(Debug, Clone)]
pub struct AuthGroup {
    /// CHAP credentials (optional, iSCSI only)
    pub chap: Option<ChapCredential>,
    /// Mutual CHAP credentials (optional, iSCSI only)
    pub chap_mutual: Option<ChapCredential>,
    /// NVMeoF host NQN restriction (optional)
    pub host_nqn: Option<String>,
}

/// CHAP credential for UCL output
#[derive(Debug, Clone)]
pub struct ChapCredential {
    pub username: String,
    pub secret: String,
}

impl AuthGroup {
    /// Create an AuthGroup from an AuthConfig.
    /// Returns None if no authentication is configured or if the config
    /// is a GroupRef (referencing an existing auth-group).
    pub fn from_auth_config(auth: &AuthConfig, _volume_name: &str) -> Option<Self> {
        match auth {
            AuthConfig::None => None,
            AuthConfig::IscsiChap(chap) => Some(Self::from_iscsi_chap(chap)),
            AuthConfig::NvmeAuth(nvme) => Some(Self::from_nvme_auth(nvme)),
            // GroupRef means the auth-group already exists in the config,
            // so we don't need to create a new one
            AuthConfig::GroupRef(_) => None,
        }
    }

    /// Create from iSCSI CHAP credentials
    fn from_iscsi_chap(chap: &IscsiChapAuth) -> Self {
        let chap_cred = ChapCredential {
            username: chap.username.clone(),
            secret: chap.secret.clone(),
        };

        let chap_mutual = if chap.has_mutual() {
            Some(ChapCredential {
                username: chap.mutual_username.clone().unwrap_or_default(),
                secret: chap.mutual_secret.clone().unwrap_or_default(),
            })
        } else {
            None
        };

        Self {
            chap: Some(chap_cred),
            chap_mutual,
            host_nqn: None,
        }
    }

    /// Create from NVMeoF auth credentials
    ///
    /// Note: FreeBSD 15's ctld does not support DH-HMAC-CHAP for NVMeoF.
    /// We generate host-nqn based access control instead, which restricts
    /// which NVMe hosts can connect to the controller.
    fn from_nvme_auth(nvme: &NvmeAuth) -> Self {
        Self {
            chap: None,
            chap_mutual: None,
            host_nqn: Some(nvme.host_nqn.clone()),
        }
    }
}

impl ToUcl for AuthGroup {
    fn to_ucl(&self, level: usize) -> String {
        let mut s = String::new();
        let ind = indent(level);

        // Write CHAP credentials (iSCSI)
        if let Some(ref chap) = self.chap {
            writeln!(
                s,
                "{}chap {} {};",
                ind,
                ucl_quote(&chap.username),
                ucl_quote(&chap.secret)
            )
            .unwrap();
        }

        // Write mutual CHAP credentials (iSCSI)
        if let Some(ref mutual) = self.chap_mutual {
            writeln!(
                s,
                "{}chap-mutual {} {};",
                ind,
                ucl_quote(&mutual.username),
                ucl_quote(&mutual.secret)
            )
            .unwrap();
        }

        // Write host-nqn restriction (NVMeoF)
        if let Some(ref nqn) = self.host_nqn {
            writeln!(s, "{}host-nqn = {};", ind, ucl_quote(nqn)).unwrap();
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

    /// Write the config file with user content + CSI-managed targets + auth groups.
    ///
    /// This extended version supports per-volume authentication groups for CHAP.
    pub fn write_config_with_auth(
        &self,
        user_content: &str,
        iscsi_targets: &[(String, Target)],
        nvme_controllers: &[(String, Controller)],
        auth_groups: &[(String, AuthGroup)],
    ) -> Result<()> {
        let mut content = user_content.to_string();

        // Ensure newline before CSI section
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }

        // Add CSI-managed section
        content.push_str(CSI_SECTION_START);
        content.push('\n');

        // Write auth groups first (they must be defined before targets reference them)
        for (name, auth_group) in auth_groups {
            writeln!(content, "auth-group {} {{", ucl_quote(name)).unwrap();
            content.push_str(&auth_group.to_ucl(1));
            writeln!(content, "}}").unwrap();
        }

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

        // Write atomically via unique temp file in the same directory.
        // Using NamedTempFile ensures each concurrent write gets a unique file,
        // avoiding race conditions where multiple writers use the same temp path.
        let config_dir = Path::new(&self.config_path)
            .parent()
            .unwrap_or(Path::new("/etc"));

        let mut temp_file = NamedTempFile::new_in(config_dir).map_err(CtlError::Io)?;

        temp_file
            .write_all(content.as_bytes())
            .map_err(CtlError::Io)?;

        // Persist and rename atomically
        temp_file
            .persist(&self.config_path)
            .map_err(|e| CtlError::Io(e.error))?;

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
        let lun = Lun::new(
            "/dev/zvol/tank/csi/vol1".to_string(),
            "pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f",
        );
        let ucl = lun.to_ucl(0);
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\";"));
        assert!(!ucl.contains("blocksize"));
        // Check for serial (SHA-256 hash, 16 hex chars)
        assert!(
            ucl.contains("serial = \""),
            "UCL should contain serial field"
        );
        // Check for device-id
        assert!(ucl.contains("device-id = \"FreeBSD pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f\";"));

        let lun_with_bs =
            Lun::with_blocksize("/dev/zvol/tank/csi/vol1".to_string(), "pvc-test", 4096);
        let ucl = lun_with_bs.to_ucl(0);
        assert!(ucl.contains("blocksize = 4096;"));
    }

    #[test]
    fn test_lun_serial_generation() {
        // Test PVC name with UUID - SHA-256 hash produces consistent output
        let serial = Lun::generate_serial("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_eq!(serial.len(), 16, "Serial must be 16 chars (SCSI limit)");
        assert!(
            serial.chars().all(|c| c.is_ascii_hexdigit()),
            "Serial must be hex"
        );

        // Test another PVC - must produce different serial
        let serial2 = Lun::generate_serial("pvc-5c1830ef-0beb-412d-8015-5a6a941b7390");
        assert_eq!(serial2.len(), 16);
        assert_ne!(
            serial, serial2,
            "Different volumes must have different serials"
        );

        // Test non-PVC name - still works with hash
        let serial3 = Lun::generate_serial("my-volume");
        assert_eq!(serial3.len(), 16);
        assert_ne!(serial, serial3);
        assert_ne!(serial2, serial3);

        // Same input always produces same output (deterministic)
        let serial_repeat = Lun::generate_serial("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_eq!(serial, serial_repeat, "Same input must produce same serial");
    }

    #[test]
    fn test_target_to_ucl() {
        let target = Target::new(
            "ag0".to_string(),
            "pg0".to_string(),
            0,
            "/dev/zvol/tank/csi/vol1".to_string(),
            "pvc-test-volume",
        );
        let ucl = target.to_ucl(0);

        // Print actual output for debugging
        println!("=== UCL OUTPUT ===\n{}\n==================", ucl);

        assert!(ucl.contains("auth-group = \"ag0\";"));
        assert!(ucl.contains("portal-group = \"pg0\";"));
        assert!(ucl.contains("lun 0 {"));
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\";"));
        assert!(ucl.contains("serial ="), "Missing serial in:\n{}", ucl);
        assert!(
            ucl.contains("device-id ="),
            "Missing device-id in:\n{}",
            ucl
        );
    }

    #[test]
    fn test_controller_to_ucl() {
        let controller = Controller::new(
            "no-authentication".to_string(),
            "tg0".to_string(),
            1,
            "/dev/zvol/tank/csi/vol1".to_string(),
            "pvc-test-volume",
        );
        let ucl = controller.to_ucl(0);

        assert!(ucl.contains("auth-group = \"no-authentication\";"));
        assert!(ucl.contains("transport-group = \"tg0\";"));
        assert!(ucl.contains("namespace 1 {"));
        assert!(ucl.contains("path = \"/dev/zvol/tank/csi/vol1\";"));
        assert!(ucl.contains("device-id = \"FreeBSD pvc-test-volume\";"));
    }

    #[test]
    fn test_validate_ucl_string() {
        assert!(validate_ucl_string("ag0", "test").is_ok());
        assert!(validate_ucl_string("iqn.2024-01.org.freebsd.csi:vol1", "test").is_ok());
        assert!(validate_ucl_string("", "test").is_err());
        assert!(validate_ucl_string("test\"value", "test").is_err());
        assert!(validate_ucl_string("test{value", "test").is_err());
    }

    #[test]
    fn test_auth_group_chap_only() {
        let auth_group = AuthGroup {
            chap: Some(ChapCredential {
                username: "testuser".to_string(),
                secret: "testsecret".to_string(),
            }),
            chap_mutual: None,
            host_nqn: None,
        };
        let ucl = auth_group.to_ucl(0);

        assert!(ucl.contains("chap \"testuser\" \"testsecret\";"));
        assert!(!ucl.contains("chap-mutual"));
        assert!(!ucl.contains("host-nqn"));
    }

    #[test]
    fn test_auth_group_chap_with_mutual() {
        let auth_group = AuthGroup {
            chap: Some(ChapCredential {
                username: "initiator".to_string(),
                secret: "initsecret".to_string(),
            }),
            chap_mutual: Some(ChapCredential {
                username: "target".to_string(),
                secret: "targetsecret".to_string(),
            }),
            host_nqn: None,
        };
        let ucl = auth_group.to_ucl(0);

        assert!(ucl.contains("chap \"initiator\" \"initsecret\";"));
        assert!(ucl.contains("chap-mutual \"target\" \"targetsecret\";"));
        assert!(!ucl.contains("host-nqn"));
    }

    #[test]
    fn test_auth_group_nvme_host_nqn() {
        let auth_group = AuthGroup {
            chap: None,
            chap_mutual: None,
            host_nqn: Some("nqn.2024-01.org.freebsd:initiator".to_string()),
        };
        let ucl = auth_group.to_ucl(0);

        assert!(ucl.contains("host-nqn = \"nqn.2024-01.org.freebsd:initiator\";"));
        assert!(!ucl.contains("chap "));
        assert!(!ucl.contains("chap-mutual"));
    }

    #[test]
    fn test_auth_group_indentation() {
        let auth_group = AuthGroup {
            chap: Some(ChapCredential {
                username: "user".to_string(),
                secret: "pass".to_string(),
            }),
            chap_mutual: None,
            host_nqn: None,
        };

        // Test with indentation level 1 (inside auth-group block)
        let ucl = auth_group.to_ucl(1);
        assert!(ucl.starts_with("    chap"), "Should be indented: {}", ucl);
    }

    #[test]
    fn test_auth_group_from_iscsi_chap() {
        use super::super::types::IscsiChapAuth;

        // Test basic CHAP
        let chap = IscsiChapAuth::new("user1", "secret1");
        let auth_config = AuthConfig::IscsiChap(chap);
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(auth_group.is_some());
        let ag = auth_group.unwrap();
        assert!(ag.chap.is_some());
        assert_eq!(ag.chap.as_ref().unwrap().username, "user1");
        assert_eq!(ag.chap.as_ref().unwrap().secret, "secret1");
        assert!(ag.chap_mutual.is_none());
    }

    #[test]
    fn test_auth_group_from_iscsi_chap_mutual() {
        use super::super::types::IscsiChapAuth;

        // Test mutual CHAP
        let chap = IscsiChapAuth::with_mutual("user1", "secret1", "target1", "tsecret1");
        let auth_config = AuthConfig::IscsiChap(chap);
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(auth_group.is_some());
        let ag = auth_group.unwrap();
        assert!(ag.chap.is_some());
        assert!(ag.chap_mutual.is_some());
        assert_eq!(ag.chap_mutual.as_ref().unwrap().username, "target1");
        assert_eq!(ag.chap_mutual.as_ref().unwrap().secret, "tsecret1");
    }

    #[test]
    fn test_auth_group_from_nvme_auth() {
        use super::super::types::NvmeAuth;

        let nvme = NvmeAuth::new(
            "nqn.2024-01.org.example:host1",
            "test-secret-key-base64",
            "SHA-256",
        );
        let auth_config = AuthConfig::NvmeAuth(nvme);
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(auth_group.is_some());
        let ag = auth_group.unwrap();
        assert!(ag.chap.is_none());
        assert!(ag.chap_mutual.is_none());
        // Note: only host_nqn is used from NvmeAuth (FreeBSD 15 doesn't support DH-HMAC-CHAP yet)
        assert_eq!(
            ag.host_nqn.as_ref().unwrap(),
            "nqn.2024-01.org.example:host1"
        );
    }

    #[test]
    fn test_auth_group_none_returns_none() {
        let auth_config = AuthConfig::None;
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume");
        assert!(auth_group.is_none());
    }


    #[test]
    fn test_auth_group_group_ref_returns_none() {
        // GroupRef means the auth-group already exists, so we don't create a new one
        let auth_config = AuthConfig::GroupRef("ag-existing-vol".to_string());
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume");
        assert!(auth_group.is_none());
    }
}
