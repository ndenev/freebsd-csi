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
    /// Logical block size (optional, 512 or 4096, defaults to 512)
    #[ucl(default)]
    pub blocksize: Option<u32>,
    /// Physical block size hint for alignment optimization (optional)
    #[ucl(default)]
    pub pblocksize: Option<u32>,
    /// Enable UNMAP/TRIM/discard passthrough (optional, "on" or "off")
    #[ucl(default)]
    pub unmap: Option<String>,
    /// Serial number for unique device identification
    #[ucl(default)]
    pub serial: Option<String>,
    /// Device ID for unique device identification (T10 vendor format)
    #[ucl(path = "device-id", default)]
    pub device_id: Option<String>,
}

/// CTL LUN/Namespace options parsed from StorageClass parameters
#[derive(Debug, Clone, Default)]
pub struct CtlOptions {
    /// Logical block size (512 or 4096)
    pub blocksize: Option<u32>,
    /// Physical block size hint for alignment
    pub pblocksize: Option<u32>,
    /// Enable UNMAP/TRIM/discard passthrough
    pub unmap: Option<bool>,
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
            pblocksize: None,
            unmap: None,
            serial: Some(serial),
            device_id: Some(device_id),
        }
    }

    /// Create a new LUN with CTL options (blocksize, pblocksize, unmap)
    pub fn with_options(path: String, volume_name: &str, options: &CtlOptions) -> Self {
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);

        Self {
            path,
            blocksize: options.blocksize,
            pblocksize: options.pblocksize,
            unmap: options.unmap.map(|b| {
                if b {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
            }),
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
            pblocksize: None,
            unmap: None,
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
        // CTL backend options go in an options { } block
        if self.pblocksize.is_some() || self.unmap.is_some() {
            writeln!(s, "{}options {{", ind).unwrap();
            let opts_ind = indent(level + 1);
            if let Some(pbs) = self.pblocksize {
                writeln!(s, "{}pblocksize = {};", opts_ind, pbs).unwrap();
            }
            if let Some(ref unmap) = self.unmap {
                writeln!(s, "{}unmap = {};", opts_ind, ucl_quote(unmap)).unwrap();
            }
            writeln!(s, "{}}}", ind).unwrap();
        }
        s
    }
}

/// An NVMe namespace
#[derive(Debug, Clone, Uclicious)]
pub struct Namespace {
    /// Path to the backing device
    pub path: String,
    /// Logical block size (optional, 512 or 4096, defaults to 512)
    #[ucl(default)]
    pub blocksize: Option<u32>,
    /// Physical block size hint for alignment optimization (optional)
    #[ucl(default)]
    pub pblocksize: Option<u32>,
    /// Enable UNMAP/TRIM/discard passthrough (optional, "on" or "off")
    #[ucl(default)]
    pub unmap: Option<String>,
    /// Serial number for unique namespace identification (used by multipath)
    #[ucl(default)]
    pub serial: Option<String>,
    /// Device ID for unique namespace identification
    #[ucl(path = "device-id", default)]
    pub device_id: Option<String>,
    /// UUID for unique namespace identification (RFC 4122 format).
    /// This is CRITICAL for NVMe multipath - ctld ignores the serial field
    /// for NVMe namespaces and uses UUID/EUI/NAA for WWID construction.
    /// Without a unique UUID, all namespaces get the same WWID causing
    /// multipath to incorrectly combine different volumes.
    #[ucl(default)]
    pub uuid: Option<String>,
}

impl Namespace {
    /// Create a new namespace with unique serial and device ID based on volume name
    pub fn new(path: String, volume_name: &str) -> Self {
        // Generate unique identifiers from volume name for multipath support
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);
        let uuid = Self::generate_uuid(volume_name);
        Self {
            path,
            blocksize: None,
            pblocksize: None,
            unmap: None,
            serial: Some(serial),
            device_id: Some(device_id),
            uuid: Some(uuid),
        }
    }

    /// Create a new namespace with CTL options (blocksize, pblocksize, unmap)
    pub fn with_options(path: String, volume_name: &str, options: &CtlOptions) -> Self {
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);
        let uuid = Self::generate_uuid(volume_name);
        Self {
            path,
            blocksize: options.blocksize,
            pblocksize: options.pblocksize,
            unmap: options.unmap.map(|b| {
                if b {
                    "on".to_string()
                } else {
                    "off".to_string()
                }
            }),
            serial: Some(serial),
            device_id: Some(device_id),
            uuid: Some(uuid),
        }
    }

    /// Generate a unique serial number from volume name.
    /// Uses SHA-256 hash to ensure uniqueness across the full volume name.
    /// This is consistent with the iSCSI LUN serial generation.
    fn generate_serial(volume_name: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(volume_name.as_bytes());
        let hash = hasher.finalize();
        // Take first 8 bytes (16 hex chars) of SHA-256 hash
        // This matches the iSCSI LUN serial format for consistency
        hex::encode(&hash[..8])
    }

    /// Generate a device ID for NVMe namespace using T10 vendor format
    fn generate_device_id(volume_name: &str) -> String {
        // T10 vendor format: "FreeBSD <volume_name>"
        // Consistent with iSCSI LUN device-id
        format!("FreeBSD {}", volume_name)
    }

    /// Generate a unique UUID from volume name for NVMe namespace identification.
    ///
    /// This is CRITICAL for NVMe multipath support. ctld ignores the `serial`
    /// field for NVMe namespaces and uses UUID/EUI/NAA for WWID construction.
    /// Without a unique UUID, all namespaces get the same WWID (based on host
    /// identifier), causing dm-multipath to incorrectly combine different volumes.
    ///
    /// Uses SHA-256 hash formatted as RFC 4122 UUID (version 4 variant).
    fn generate_uuid(volume_name: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        // Use "nvme-uuid:" prefix to get different hash than serial
        hasher.update(b"nvme-uuid:");
        hasher.update(volume_name.as_bytes());
        let hash = hasher.finalize();

        // Format as RFC 4122 UUID: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        // Use first 16 bytes of SHA-256 hash
        // Set version (4) and variant (RFC 4122) bits for valid UUID format
        let mut uuid_bytes = [0u8; 16];
        uuid_bytes.copy_from_slice(&hash[..16]);

        // Set version to 4 (random UUID) - bits 12-15 of time_hi_and_version
        uuid_bytes[6] = (uuid_bytes[6] & 0x0f) | 0x40;
        // Set variant to RFC 4122 - bits 6-7 of clock_seq_hi_and_reserved
        uuid_bytes[8] = (uuid_bytes[8] & 0x3f) | 0x80;

        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            uuid_bytes[0],
            uuid_bytes[1],
            uuid_bytes[2],
            uuid_bytes[3],
            uuid_bytes[4],
            uuid_bytes[5],
            uuid_bytes[6],
            uuid_bytes[7],
            uuid_bytes[8],
            uuid_bytes[9],
            uuid_bytes[10],
            uuid_bytes[11],
            uuid_bytes[12],
            uuid_bytes[13],
            uuid_bytes[14],
            uuid_bytes[15]
        )
    }
}

impl ToUcl for Namespace {
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
        // CTL backend options go in an options { } block.
        // CRITICAL: The uuid option is required for NVMe multipath support.
        // ctld ignores the serial field for NVMe and uses uuid for WWID construction.
        let has_options = self.pblocksize.is_some() || self.unmap.is_some() || self.uuid.is_some();
        if has_options {
            writeln!(s, "{}options {{", ind).unwrap();
            let opts_ind = indent(level + 1);
            if let Some(pbs) = self.pblocksize {
                writeln!(s, "{}pblocksize = {};", opts_ind, pbs).unwrap();
            }
            if let Some(ref unmap) = self.unmap {
                writeln!(s, "{}unmap = {};", opts_ind, ucl_quote(unmap)).unwrap();
            }
            // UUID is CRITICAL for NVMe multipath - ensures unique WWID per namespace
            if let Some(ref uuid) = self.uuid {
                writeln!(s, "{}uuid = {};", opts_ind, ucl_quote(uuid)).unwrap();
            }
            writeln!(s, "{}}}", ind).unwrap();
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

    /// Create a new target with a single LUN and CTL options
    pub fn with_options(
        auth_group: String,
        portal_group: String,
        lun_id: u32,
        device_path: String,
        volume_name: &str,
        options: &CtlOptions,
    ) -> Self {
        let mut lun = HashMap::new();
        lun.insert(
            lun_id.to_string(),
            Lun::with_options(device_path, volume_name, options),
        );
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
    /// Controller serial number for multipath identification
    #[ucl(default)]
    pub serial: Option<String>,
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
        let serial = Self::generate_serial(volume_name);
        let mut namespace = HashMap::new();
        namespace.insert(ns_id.to_string(), Namespace::new(device_path, volume_name));
        Self {
            auth_group,
            transport_group,
            serial: Some(serial),
            namespace,
        }
    }

    /// Create a new controller with a single namespace and CTL options
    pub fn with_options(
        auth_group: String,
        transport_group: String,
        ns_id: u32,
        device_path: String,
        volume_name: &str,
        options: &CtlOptions,
    ) -> Self {
        let serial = Self::generate_serial(volume_name);
        let mut namespace = HashMap::new();
        namespace.insert(
            ns_id.to_string(),
            Namespace::with_options(device_path, volume_name, options),
        );
        Self {
            auth_group,
            transport_group,
            serial: Some(serial),
            namespace,
        }
    }

    /// Generate a unique serial number for the controller from volume name.
    /// Uses SHA-256 hash with a different prefix to ensure uniqueness from namespace serial.
    /// This serial identifies the controller for multipath purposes.
    fn generate_serial(volume_name: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        // Use "ctrl:" prefix to differentiate from namespace serial
        hasher.update(b"ctrl:");
        hasher.update(volume_name.as_bytes());
        let hash = hasher.finalize();
        // Take first 10 bytes (20 hex chars) for controller serial
        // NVMe controller serial can be up to 20 bytes (40 hex chars)
        hex::encode(&hash[..10])
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
        if let Some(ref serial) = self.serial {
            writeln!(s, "{}serial = {};", ind, ucl_quote(serial)).unwrap();
        }

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
    ///
    /// Returns `Ok(None)` if no authentication is configured or if the config
    /// is a GroupRef (referencing an existing auth-group).
    ///
    /// Returns `Err` if CHAP credentials contain characters that would corrupt
    /// UCL syntax (e.g., `"`, `{`, `}`, `\`).
    pub fn from_auth_config(auth: &AuthConfig, _volume_name: &str) -> Result<Option<Self>> {
        match auth {
            AuthConfig::None => Ok(None),
            AuthConfig::IscsiChap(chap) => Ok(Some(Self::from_iscsi_chap(chap)?)),
            AuthConfig::NvmeAuth(nvme) => Ok(Some(Self::from_nvme_auth(nvme))),
            // GroupRef means the auth-group already exists in the config,
            // so we don't need to create a new one
            AuthConfig::GroupRef(_) => Ok(None),
        }
    }

    /// Create from iSCSI CHAP credentials.
    ///
    /// Validates that all credential strings are safe for UCL output.
    fn from_iscsi_chap(chap: &IscsiChapAuth) -> Result<Self> {
        // Validate forward CHAP credentials
        validate_ucl_string(&chap.username, "CHAP username")?;
        validate_ucl_string(&chap.secret, "CHAP secret")?;

        let chap_cred = ChapCredential {
            username: chap.username.clone(),
            secret: chap.secret.clone(),
        };

        // Validate and create mutual CHAP credentials if present
        let chap_mutual = if chap.has_mutual() {
            let mutual_user = chap.mutual_username.clone().unwrap_or_default();
            let mutual_secret = chap.mutual_secret.clone().unwrap_or_default();
            validate_ucl_string(&mutual_user, "mutual CHAP username")?;
            validate_ucl_string(&mutual_secret, "mutual CHAP secret")?;
            Some(ChapCredential {
                username: mutual_user,
                secret: mutual_secret,
            })
        } else {
            None
        };

        Ok(Self {
            chap: Some(chap_cred),
            chap_mutual,
            host_nqn: None,
        })
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
        // CRITICAL: Verify UUID is included in options block for NVMe multipath
        assert!(
            ucl.contains("options {"),
            "UCL should have options block for UUID: {}",
            ucl
        );
        assert!(
            ucl.contains("uuid = \""),
            "UCL should contain uuid for NVMe multipath: {}",
            ucl
        );
    }

    #[test]
    fn test_namespace_uuid_generation() {
        // Test PVC name with UUID - generates unique UUID
        let uuid = Namespace::generate_uuid("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_eq!(uuid.len(), 36, "UUID must be 36 chars (RFC 4122 format)");
        assert!(uuid.contains('-'), "UUID must contain hyphens");
        // Verify UUID format: 8-4-4-4-12
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(
            parts.len(),
            5,
            "UUID must have 5 parts separated by hyphens"
        );
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);

        // Verify version 4 and RFC 4122 variant bits
        let version_char = parts[2].chars().next().unwrap();
        assert_eq!(version_char, '4', "UUID version must be 4");
        let variant_char = parts[3].chars().next().unwrap();
        assert!(
            "89ab".contains(variant_char),
            "UUID variant must be RFC 4122 (8, 9, a, or b)"
        );

        // Test another PVC - must produce different UUID
        let uuid2 = Namespace::generate_uuid("pvc-5c1830ef-0beb-412d-8015-5a6a941b7390");
        assert_eq!(uuid2.len(), 36);
        assert_ne!(uuid, uuid2, "Different volumes must have different UUIDs");

        // Same input always produces same output (deterministic)
        let uuid_repeat = Namespace::generate_uuid("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_eq!(uuid, uuid_repeat, "Same input must produce same UUID");

        // UUID must be different from serial for same volume
        let serial = Namespace::generate_serial("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_ne!(
            uuid.replace('-', ""),
            serial,
            "UUID must be different from serial"
        );
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
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume")
            .expect("validation should pass");

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
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume")
            .expect("validation should pass");

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
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume")
            .expect("validation should pass");

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
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume")
            .expect("validation should pass");
        assert!(auth_group.is_none());
    }

    #[test]
    fn test_auth_group_group_ref_returns_none() {
        // GroupRef means the auth-group already exists, so we don't create a new one
        let auth_config = AuthConfig::GroupRef("ag-existing-vol".to_string());
        let auth_group = AuthGroup::from_auth_config(&auth_config, "test-volume")
            .expect("validation should pass");
        assert!(auth_group.is_none());
    }

    // ============================================================================
    // UCL validation tests
    // ============================================================================

    #[test]
    fn test_validate_ucl_string_rejects_double_quote() {
        use super::super::types::IscsiChapAuth;

        let chap = IscsiChapAuth::new("user", "pass\"word");
        let auth_config = AuthConfig::IscsiChap(chap);
        let result = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("forbidden character"),
            "Error should mention forbidden character: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_ucl_string_rejects_curly_braces() {
        use super::super::types::IscsiChapAuth;

        let chap = IscsiChapAuth::new("user{name}", "secret");
        let auth_config = AuthConfig::IscsiChap(chap);
        let result = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("forbidden character"),
            "Error should mention forbidden character: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_ucl_string_rejects_backslash() {
        use super::super::types::IscsiChapAuth;

        let chap = IscsiChapAuth::new("user", "pass\\word");
        let auth_config = AuthConfig::IscsiChap(chap);
        let result = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("forbidden character"),
            "Error should mention forbidden character: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_ucl_string_rejects_empty() {
        use super::super::types::IscsiChapAuth;

        let chap = IscsiChapAuth::new("", "secret");
        let auth_config = AuthConfig::IscsiChap(chap);
        let result = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("cannot be empty"),
            "Error should mention empty: {}",
            err_msg
        );
    }

    #[test]
    fn test_validate_ucl_string_accepts_safe_special_chars() {
        use super::super::types::IscsiChapAuth;

        // These special characters should be allowed
        let chap = IscsiChapAuth::new("user@domain.com", "p@ss!w0rd#$%^&*()");
        let auth_config = AuthConfig::IscsiChap(chap);
        let result = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(result.is_ok(), "Safe special chars should be allowed");
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_validate_ucl_string_mutual_chap_credentials() {
        use super::super::types::IscsiChapAuth;

        // Test that mutual CHAP credentials are also validated
        let chap = IscsiChapAuth::with_mutual("user1", "secret1", "target\"name", "tsecret");
        let auth_config = AuthConfig::IscsiChap(chap);
        let result = AuthGroup::from_auth_config(&auth_config, "test-volume");

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("mutual CHAP username"),
            "Error should mention mutual CHAP username: {}",
            err_msg
        );
    }

    // ============================================================================
    // CTL Options tests
    // ============================================================================

    #[test]
    fn test_ctl_options_default() {
        let opts = CtlOptions::default();
        assert!(opts.blocksize.is_none());
        assert!(opts.pblocksize.is_none());
        assert!(opts.unmap.is_none());
    }

    #[test]
    fn test_lun_with_options() {
        let opts = CtlOptions {
            blocksize: Some(4096),
            pblocksize: Some(4096),
            unmap: Some(true),
        };
        let lun = Lun::with_options("/dev/zvol/tank/csi/vol1".to_string(), "pvc-test", &opts);
        let ucl = lun.to_ucl(0);

        assert!(ucl.contains("blocksize = 4096;"), "UCL: {}", ucl);
        assert!(
            ucl.contains("options {"),
            "UCL should have options block: {}",
            ucl
        );
        assert!(ucl.contains("pblocksize = 4096;"), "UCL: {}", ucl);
        assert!(ucl.contains("unmap = \"on\";"), "UCL: {}", ucl);
        assert!(ucl.contains("serial ="), "UCL: {}", ucl);
        assert!(ucl.contains("device-id ="), "UCL: {}", ucl);
    }

    #[test]
    fn test_lun_with_options_unmap_off() {
        let opts = CtlOptions {
            blocksize: None,
            pblocksize: None,
            unmap: Some(false),
        };
        let lun = Lun::with_options("/dev/zvol/tank/csi/vol1".to_string(), "pvc-test", &opts);
        let ucl = lun.to_ucl(0);

        assert!(
            !ucl.contains("blocksize ="),
            "UCL should not have blocksize: {}",
            ucl
        );
        assert!(
            ucl.contains("options {"),
            "UCL should have options block: {}",
            ucl
        );
        assert!(
            !ucl.contains("pblocksize"),
            "UCL should not have pblocksize: {}",
            ucl
        );
        assert!(ucl.contains("unmap = \"off\";"), "UCL: {}", ucl);
    }

    #[test]
    fn test_namespace_with_options() {
        let opts = CtlOptions {
            blocksize: Some(4096),
            pblocksize: Some(4096),
            unmap: Some(true),
        };
        let ns = Namespace::with_options("/dev/zvol/tank/csi/vol1".to_string(), "pvc-test", &opts);
        let ucl = ns.to_ucl(0);

        assert!(ucl.contains("blocksize = 4096;"), "UCL: {}", ucl);
        assert!(
            ucl.contains("options {"),
            "UCL should have options block: {}",
            ucl
        );
        assert!(ucl.contains("pblocksize = 4096;"), "UCL: {}", ucl);
        assert!(ucl.contains("unmap = \"on\";"), "UCL: {}", ucl);
        assert!(ucl.contains("serial ="), "UCL: {}", ucl);
        assert!(ucl.contains("device-id ="), "UCL: {}", ucl);
        // CRITICAL: Verify UUID is included for NVMe multipath support
        assert!(
            ucl.contains("uuid = \""),
            "UCL should contain uuid for NVMe multipath: {}",
            ucl
        );
    }

    #[test]
    fn test_target_with_options() {
        let opts = CtlOptions {
            blocksize: Some(4096),
            pblocksize: Some(4096),
            unmap: Some(true),
        };
        let target = Target::with_options(
            "no-authentication".to_string(),
            "pg0".to_string(),
            0,
            "/dev/zvol/tank/csi/vol1".to_string(),
            "pvc-test",
            &opts,
        );
        let ucl = target.to_ucl(0);

        assert!(
            ucl.contains("auth-group = \"no-authentication\";"),
            "UCL: {}",
            ucl
        );
        assert!(ucl.contains("portal-group = \"pg0\";"), "UCL: {}", ucl);
        assert!(ucl.contains("lun 0 {"), "UCL: {}", ucl);
        assert!(ucl.contains("blocksize = 4096;"), "UCL: {}", ucl);
        assert!(
            ucl.contains("options {"),
            "UCL should have options block: {}",
            ucl
        );
        assert!(ucl.contains("pblocksize = 4096;"), "UCL: {}", ucl);
        assert!(ucl.contains("unmap = \"on\";"), "UCL: {}", ucl);
    }

    #[test]
    fn test_controller_with_options() {
        let opts = CtlOptions {
            blocksize: Some(4096),
            pblocksize: Some(4096),
            unmap: Some(true),
        };
        let controller = Controller::with_options(
            "no-authentication".to_string(),
            "tg0".to_string(),
            1,
            "/dev/zvol/tank/csi/vol1".to_string(),
            "pvc-test",
            &opts,
        );
        let ucl = controller.to_ucl(0);

        assert!(
            ucl.contains("auth-group = \"no-authentication\";"),
            "UCL: {}",
            ucl
        );
        assert!(ucl.contains("transport-group = \"tg0\";"), "UCL: {}", ucl);
        assert!(ucl.contains("namespace 1 {"), "UCL: {}", ucl);
        assert!(ucl.contains("blocksize = 4096;"), "UCL: {}", ucl);
        assert!(
            ucl.contains("options {"),
            "UCL should have options block: {}",
            ucl
        );
        assert!(ucl.contains("pblocksize = 4096;"), "UCL: {}", ucl);
        assert!(ucl.contains("unmap = \"on\";"), "UCL: {}", ucl);
    }

    #[test]
    fn test_lun_no_options_block_when_empty() {
        let opts = CtlOptions {
            blocksize: Some(4096),
            pblocksize: None,
            unmap: None,
        };
        let lun = Lun::with_options("/dev/zvol/tank/csi/vol1".to_string(), "pvc-test", &opts);
        let ucl = lun.to_ucl(0);

        assert!(ucl.contains("blocksize = 4096;"), "UCL: {}", ucl);
        assert!(
            !ucl.contains("options {"),
            "UCL should not have options block: {}",
            ucl
        );
    }
}
