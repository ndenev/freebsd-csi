//! UCL configuration types for ctld.
//!
//! This module provides types for reading and writing ctld UCL configuration,
//! using uclicious for parsing and a ToUcl trait for serialization.

use std::collections::HashMap;
use std::fmt::Write;

use uclicious::Uclicious;

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
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    /// NAA (Network Address Authority) identifier for unique namespace identification.
    /// This is CRITICAL for NVMe multipath - FreeBSD's CTL kernel populates
    /// nsdata->nguid ONLY from NAA/EUI64 identifiers, NOT from UUID.
    /// Without a unique NAA, all namespaces get the same nguid causing
    /// Linux multipath to incorrectly combine different volumes.
    /// Format: NAA Type 6 (128-bit), 32 hex chars, first nibble = '6'.
    #[ucl(default)]
    pub naa: Option<String>,
}

impl Namespace {
    /// Create a new namespace with unique serial and device ID based on volume name
    pub fn new(path: String, volume_name: &str) -> Self {
        // Generate unique identifiers from volume name for multipath support
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);
        let naa = Self::generate_naa(volume_name);
        Self {
            path,
            blocksize: None,
            pblocksize: None,
            unmap: None,
            serial: Some(serial),
            device_id: Some(device_id),
            naa: Some(naa),
        }
    }

    /// Create a new namespace with CTL options (blocksize, pblocksize, unmap)
    pub fn with_options(path: String, volume_name: &str, options: &CtlOptions) -> Self {
        let serial = Self::generate_serial(volume_name);
        let device_id = Self::generate_device_id(volume_name);
        let naa = Self::generate_naa(volume_name);
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
            naa: Some(naa),
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

    /// Generate a unique NAA Type 6 identifier from volume name for NVMe namespace.
    ///
    /// This is CRITICAL for NVMe multipath support. FreeBSD's CTL kernel
    /// populates nsdata->nguid ONLY from NAA/EUI64 device ID descriptors,
    /// NOT from UUID. Without a unique NAA, all namespaces get the same nguid
    /// (all zeros), causing Linux dm-multipath to incorrectly combine different
    /// volumes.
    ///
    /// NAA Type 6 format: 32 hex chars (16 bytes / 128 bits)
    /// - First nibble: 6 (NAA Type 6)
    /// - Remaining 120 bits: unique identifier from SHA-256 hash
    fn generate_naa(volume_name: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        // Use "nvme-naa:" prefix to get different hash than serial
        hasher.update(b"nvme-naa:");
        hasher.update(volume_name.as_bytes());
        let hash = hasher.finalize();

        // Use first 16 bytes of SHA-256 hash
        let mut naa_bytes = [0u8; 16];
        naa_bytes.copy_from_slice(&hash[..16]);

        // Set first nibble to 6 (NAA Type 6)
        // NAA Type 6 = IEEE Registered Extended (128-bit)
        naa_bytes[0] = (naa_bytes[0] & 0x0f) | 0x60;

        // Format as 32 hex chars (no separators for ctld)
        hex::encode(naa_bytes)
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
        // CRITICAL: The naa option is required for NVMe multipath support.
        // FreeBSD's CTL kernel populates nsdata->nguid ONLY from NAA/EUI64.
        let has_options = self.pblocksize.is_some() || self.unmap.is_some() || self.naa.is_some();
        if has_options {
            writeln!(s, "{}options {{", ind).unwrap();
            let opts_ind = indent(level + 1);
            if let Some(pbs) = self.pblocksize {
                writeln!(s, "{}pblocksize = {};", opts_ind, pbs).unwrap();
            }
            if let Some(ref unmap) = self.unmap {
                writeln!(s, "{}unmap = {};", opts_ind, ucl_quote(unmap)).unwrap();
            }
            // NAA is CRITICAL for NVMe multipath - populates nsdata->nguid
            if let Some(ref naa) = self.naa {
                writeln!(s, "{}naa = {};", opts_ind, ucl_quote(naa)).unwrap();
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
        // CRITICAL: Verify NAA is included in options block for NVMe multipath
        assert!(
            ucl.contains("options {"),
            "UCL should have options block for NAA: {}",
            ucl
        );
        assert!(
            ucl.contains("naa = \""),
            "UCL should contain naa for NVMe multipath: {}",
            ucl
        );
    }

    #[test]
    fn test_namespace_naa_generation() {
        // Test PVC name - generates unique NAA Type 6 identifier
        let naa = Namespace::generate_naa("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_eq!(naa.len(), 32, "NAA must be 32 hex chars (16 bytes)");
        assert!(
            naa.chars().all(|c| c.is_ascii_hexdigit()),
            "NAA must be hex"
        );

        // Verify NAA Type 6 - first nibble must be '6'
        let first_char = naa.chars().next().unwrap();
        assert_eq!(first_char, '6', "NAA Type must be 6, got: {}", first_char);

        // Test another PVC - must produce different NAA
        let naa2 = Namespace::generate_naa("pvc-5c1830ef-0beb-412d-8015-5a6a941b7390");
        assert_eq!(naa2.len(), 32);
        assert_ne!(naa, naa2, "Different volumes must have different NAAs");

        // Same input always produces same output (deterministic)
        let naa_repeat = Namespace::generate_naa("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_eq!(naa, naa_repeat, "Same input must produce same NAA");

        // NAA must be different from serial for same volume
        let serial = Namespace::generate_serial("pvc-c2e56d00-9afa-42ec-9404-22e317aadd8f");
        assert_ne!(naa, serial, "NAA must be different from serial");
    }

    #[test]
    fn test_validate_ucl_string() {
        assert!(validate_ucl_string("ag0", "test").is_ok());
        assert!(validate_ucl_string("iqn.2024-01.org.freebsd.csi:vol1", "test").is_ok());
        assert!(validate_ucl_string("", "test").is_err());
        assert!(validate_ucl_string("test\"value", "test").is_err());
        assert!(validate_ucl_string("test{value", "test").is_err());
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
        // CRITICAL: Verify NAA is included for NVMe multipath support
        assert!(
            ucl.contains("naa = \""),
            "UCL should contain naa for NVMe multipath: {}",
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
