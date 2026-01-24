//! Type-safe wrappers for CTL identifiers.
//!
//! This module provides newtypes for iSCSI IQNs, NVMeoF NQNs, and device paths,
//! ensuring type safety and proper validation throughout the codebase.

use std::fmt::{self, Display};
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::error::{CtlError, Result};

// ============================================================================
// ExportType enum
// ============================================================================

/// Export type for CTL volumes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExportType {
    Iscsi,
    Nvmeof,
}

impl Display for ExportType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportType::Iscsi => write!(f, "ISCSI"),
            ExportType::Nvmeof => write!(f, "NVMEOF"),
        }
    }
}

impl FromStr for ExportType {
    type Err = CtlError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "ISCSI" => Ok(ExportType::Iscsi),
            "NVMEOF" => Ok(ExportType::Nvmeof),
            _ => Err(CtlError::InvalidName(format!(
                "unknown export type '{}': expected ISCSI or NVMEOF",
                s
            ))),
        }
    }
}

// ============================================================================
// IQN (iSCSI Qualified Name)
// ============================================================================

/// iSCSI Qualified Name (IQN).
///
/// Format: `iqn.YYYY-MM.reverse.domain:identifier`
/// Example: `iqn.2024-01.org.freebsd.csi:volume-name`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Iqn(String);

#[allow(dead_code)]
impl Iqn {
    /// Create a new IQN from a base prefix and volume name.
    pub fn new(base_iqn: &str, volume_name: &str) -> Result<Self> {
        validate_identifier(base_iqn, "base IQN")?;
        validate_identifier(volume_name, "volume name")?;
        Ok(Self(format!("{}:{}", base_iqn, volume_name)))
    }

    /// Parse an existing IQN string.
    pub fn parse(s: &str) -> Result<Self> {
        validate_identifier(s, "IQN")?;
        if !s.starts_with("iqn.") {
            return Err(CtlError::InvalidName(format!(
                "IQN '{}' must start with 'iqn.'",
                s
            )));
        }
        Ok(Self(s.to_string()))
    }

    /// Extract the volume name (part after the last colon).
    pub fn volume_name(&self) -> Option<&str> {
        self.0.rsplit(':').next()
    }

    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Iqn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Iqn {
    type Err = CtlError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl AsRef<str> for Iqn {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// ============================================================================
// NQN (NVMe Qualified Name)
// ============================================================================

/// NVMe Qualified Name (NQN).
///
/// Format: `nqn.YYYY-MM.reverse.domain:identifier`
/// Example: `nqn.2024-01.org.freebsd.csi:volume-name`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Nqn(String);

#[allow(dead_code)]
impl Nqn {
    /// Create a new NQN from a base prefix and volume name.
    /// Note: Forward slashes in the volume name are replaced with hyphens.
    pub fn new(base_nqn: &str, volume_name: &str) -> Result<Self> {
        validate_identifier(base_nqn, "base NQN")?;
        let safe_name = volume_name.replace('/', "-");
        validate_identifier(&safe_name, "volume name")?;
        Ok(Self(format!("{}:{}", base_nqn, safe_name)))
    }

    /// Parse an existing NQN string.
    pub fn parse(s: &str) -> Result<Self> {
        validate_identifier(s, "NQN")?;
        if !s.starts_with("nqn.") {
            return Err(CtlError::InvalidName(format!(
                "NQN '{}' must start with 'nqn.'",
                s
            )));
        }
        Ok(Self(s.to_string()))
    }

    /// Extract the volume name (part after the last colon).
    pub fn volume_name(&self) -> Option<&str> {
        self.0.rsplit(':').next()
    }

    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for Nqn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Nqn {
    type Err = CtlError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl AsRef<str> for Nqn {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// ============================================================================
// TargetName (union of IQN or NQN)
// ============================================================================

/// A target name that can be either an IQN (iSCSI) or NQN (NVMeoF).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TargetName {
    Iqn(Iqn),
    Nqn(Nqn),
}

#[allow(dead_code)]
impl TargetName {
    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        match self {
            TargetName::Iqn(iqn) => iqn.as_str(),
            TargetName::Nqn(nqn) => nqn.as_str(),
        }
    }

    /// Extract the volume name from the target.
    pub fn volume_name(&self) -> Option<&str> {
        match self {
            TargetName::Iqn(iqn) => iqn.volume_name(),
            TargetName::Nqn(nqn) => nqn.volume_name(),
        }
    }
}

impl Display for TargetName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetName::Iqn(iqn) => write!(f, "{}", iqn),
            TargetName::Nqn(nqn) => write!(f, "{}", nqn),
        }
    }
}

impl From<Iqn> for TargetName {
    fn from(iqn: Iqn) -> Self {
        TargetName::Iqn(iqn)
    }
}

impl From<Nqn> for TargetName {
    fn from(nqn: Nqn) -> Self {
        TargetName::Nqn(nqn)
    }
}

// ============================================================================
// DevicePath
// ============================================================================

/// A validated ZFS device path.
///
/// Device paths must be under `/dev/zvol/` and contain only safe characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DevicePath(String);

#[allow(dead_code)]
impl DevicePath {
    const PREFIX: &'static str = "/dev/zvol/";

    /// Validate that this device path is under the specified parent dataset.
    ///
    /// This is a security check to prevent privilege escalation by ensuring
    /// device paths can only reference volumes under the CSI-managed dataset.
    ///
    /// # Arguments
    /// * `parent_dataset` - The ZFS parent dataset (e.g., "tank/csi")
    ///
    /// # Examples
    /// ```ignore
    /// let path = DevicePath::parse("/dev/zvol/tank/csi/vol1")?;
    /// path.validate_parent_dataset("tank/csi")?; // Ok
    /// path.validate_parent_dataset("other/pool")?; // Error
    /// ```
    pub fn validate_parent_dataset(&self, parent_dataset: &str) -> Result<()> {
        let expected_prefix = format!("{}{}/", Self::PREFIX, parent_dataset);
        if !self.0.starts_with(&expected_prefix) {
            return Err(CtlError::InvalidName(format!(
                "device path '{}' must be under {}",
                self.0, expected_prefix
            )));
        }
        Ok(())
    }

    /// Create a device path from a ZFS dataset name.
    pub fn from_dataset(dataset_name: &str) -> Result<Self> {
        if dataset_name.is_empty() {
            return Err(CtlError::InvalidName("dataset name cannot be empty".into()));
        }
        if dataset_name.contains("..") {
            return Err(CtlError::InvalidName(format!(
                "dataset name '{}' contains path traversal",
                dataset_name
            )));
        }
        // Validate characters in dataset name
        if !dataset_name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/')
        {
            return Err(CtlError::InvalidName(format!(
                "dataset name '{}' contains invalid characters",
                dataset_name
            )));
        }
        Ok(Self(format!("{}{}", Self::PREFIX, dataset_name)))
    }

    /// Parse an existing device path string.
    pub fn parse(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Err(CtlError::InvalidName("device path cannot be empty".into()));
        }
        if !s.starts_with(Self::PREFIX) {
            return Err(CtlError::InvalidName(format!(
                "device path '{}' must be under {}",
                s,
                Self::PREFIX
            )));
        }
        if s.contains("..") {
            return Err(CtlError::InvalidName(format!(
                "device path '{}' contains path traversal",
                s
            )));
        }
        let path_part = &s[Self::PREFIX.len()..];
        if !path_part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/')
        {
            return Err(CtlError::InvalidName(format!(
                "device path '{}' contains invalid characters",
                s
            )));
        }
        Ok(Self(s.to_string()))
    }

    /// Get the inner string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the dataset name (path without /dev/zvol/ prefix).
    pub fn dataset_name(&self) -> &str {
        &self.0[Self::PREFIX.len()..]
    }
}

impl Display for DevicePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DevicePath {
    type Err = CtlError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl AsRef<str> for DevicePath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<Path> for DevicePath {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

// ============================================================================
// Authentication credentials
// ============================================================================

/// iSCSI CHAP authentication credentials.
///
/// Supports both forward CHAP (initiator authenticates to target) and
/// mutual CHAP (bidirectional authentication).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IscsiChapAuth {
    /// Forward CHAP username (initiator → target)
    pub username: String,
    /// Forward CHAP secret
    pub secret: String,
    /// Mutual CHAP username (target → initiator, optional)
    pub mutual_username: Option<String>,
    /// Mutual CHAP secret (optional)
    pub mutual_secret: Option<String>,
}

impl IscsiChapAuth {
    /// Create new CHAP credentials with forward authentication only.
    pub fn new(username: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            secret: secret.into(),
            mutual_username: None,
            mutual_secret: None,
        }
    }

    /// Create new CHAP credentials with mutual authentication.
    pub fn with_mutual(
        username: impl Into<String>,
        secret: impl Into<String>,
        mutual_username: impl Into<String>,
        mutual_secret: impl Into<String>,
    ) -> Self {
        Self {
            username: username.into(),
            secret: secret.into(),
            mutual_username: Some(mutual_username.into()),
            mutual_secret: Some(mutual_secret.into()),
        }
    }

    /// Check if mutual authentication is configured.
    pub fn has_mutual(&self) -> bool {
        self.mutual_username.is_some() && self.mutual_secret.is_some()
    }
}

/// NVMeoF DH-HMAC-CHAP authentication credentials.
///
/// Implements NVMe-oF in-band authentication per the NVMe specification.
/// Requires FreeBSD 15+ with NVMeoF controller auth support.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NvmeAuth {
    /// Host NQN for authentication
    pub host_nqn: String,
    /// Pre-shared key (32-48 bytes, base64 encoded)
    pub secret: String,
    /// Hash function: SHA-256, SHA-384, or SHA-512
    pub hash_function: String,
    /// DH group (empty for HMAC-CHAP only, without key agreement)
    pub dh_group: Option<String>,
}

impl NvmeAuth {
    /// Create new NVMeoF auth credentials.
    pub fn new(
        host_nqn: impl Into<String>,
        secret: impl Into<String>,
        hash_function: impl Into<String>,
    ) -> Self {
        Self {
            host_nqn: host_nqn.into(),
            secret: secret.into(),
            hash_function: hash_function.into(),
            dh_group: None,
        }
    }

    /// Create credentials with DH key agreement.
    pub fn with_dh_group(mut self, dh_group: impl Into<String>) -> Self {
        self.dh_group = Some(dh_group.into());
        self
    }
}

/// Authentication configuration for a CTL export.
///
/// Wraps protocol-specific authentication credentials or references
/// an existing auth-group by name.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum AuthConfig {
    /// No authentication required
    #[default]
    None,
    /// iSCSI CHAP authentication (contains credentials)
    IscsiChap(IscsiChapAuth),
    /// NVMeoF DH-HMAC-CHAP authentication (contains credentials)
    NvmeAuth(NvmeAuth),
    /// Reference to an existing auth-group by name (no credentials stored).
    ///
    /// Used when reconciling volumes from ZFS metadata where credentials
    /// are already persisted in `/etc/ctl.conf`. This avoids storing
    /// plaintext secrets in ZFS user properties.
    GroupRef(String),
}

impl AuthConfig {
    /// Check if authentication is configured.
    ///
    /// Returns true for all variants except `None`.
    pub fn is_some(&self) -> bool {
        !matches!(self, AuthConfig::None)
    }

    /// Get the auth group name for UCL config.
    ///
    /// Returns "no-authentication" if no auth is configured,
    /// the stored name for `GroupRef`, or a generated name for credentials.
    pub fn auth_group_name(&self, volume_name: &str) -> String {
        match self {
            AuthConfig::None => "no-authentication".to_string(),
            AuthConfig::IscsiChap(_) | AuthConfig::NvmeAuth(_) => {
                format!("ag-{}", volume_name)
            }
            AuthConfig::GroupRef(name) => name.clone(),
        }
    }
}

// ============================================================================
// Validation helpers
// ============================================================================

/// Validate an identifier (IQN/NQN segment or volume name).
fn validate_identifier(s: &str, field_name: &str) -> Result<()> {
    if s.is_empty() {
        return Err(CtlError::InvalidName(format!(
            "{} cannot be empty",
            field_name
        )));
    }

    if s.len() > 223 {
        return Err(CtlError::InvalidName(format!(
            "{} '{}' exceeds maximum length of 223 characters",
            field_name, s
        )));
    }

    // Allow alphanumeric, underscore, hyphen, period, and colon
    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':')
    {
        return Err(CtlError::InvalidName(format!(
            "{} '{}' contains invalid characters",
            field_name, s
        )));
    }

    if s.contains("..") {
        return Err(CtlError::InvalidName(format!(
            "{} '{}' contains path traversal sequence",
            field_name, s
        )));
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
    fn test_export_type_display() {
        assert_eq!(ExportType::Iscsi.to_string(), "ISCSI");
        assert_eq!(ExportType::Nvmeof.to_string(), "NVMEOF");
    }

    #[test]
    fn test_export_type_from_str() {
        assert_eq!("ISCSI".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("iscsi".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("NVMEOF".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("nvmeof".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert!("unknown".parse::<ExportType>().is_err());
    }

    #[test]
    fn test_export_type_serde() {
        let iscsi = ExportType::Iscsi;
        let json = serde_json::to_string(&iscsi).unwrap();
        assert_eq!(json, "\"ISCSI\"");
        let parsed: ExportType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ExportType::Iscsi);
    }

    #[test]
    fn test_iqn_new() {
        let iqn = Iqn::new("iqn.2024-01.org.freebsd.csi", "vol1").unwrap();
        assert_eq!(iqn.as_str(), "iqn.2024-01.org.freebsd.csi:vol1");
        assert_eq!(iqn.volume_name(), Some("vol1"));
    }

    #[test]
    fn test_iqn_parse() {
        let iqn = Iqn::parse("iqn.2024-01.org.freebsd.csi:vol1").unwrap();
        assert_eq!(iqn.volume_name(), Some("vol1"));
        assert!(Iqn::parse("nqn.2024-01.org.freebsd.csi:vol1").is_err());
    }

    #[test]
    fn test_nqn_new() {
        let nqn = Nqn::new("nqn.2024-01.org.freebsd.csi", "vol1").unwrap();
        assert_eq!(nqn.as_str(), "nqn.2024-01.org.freebsd.csi:vol1");
    }

    #[test]
    fn test_nqn_slash_replacement() {
        let nqn = Nqn::new("nqn.2024-01.org.freebsd.csi", "path/to/vol").unwrap();
        assert_eq!(nqn.as_str(), "nqn.2024-01.org.freebsd.csi:path-to-vol");
    }

    #[test]
    fn test_device_path_from_dataset() {
        let path = DevicePath::from_dataset("tank/csi/vol1").unwrap();
        assert_eq!(path.as_str(), "/dev/zvol/tank/csi/vol1");
        assert_eq!(path.dataset_name(), "tank/csi/vol1");
    }

    #[test]
    fn test_device_path_parse() {
        let path = DevicePath::parse("/dev/zvol/tank/csi/vol1").unwrap();
        assert_eq!(path.dataset_name(), "tank/csi/vol1");
    }

    #[test]
    fn test_device_path_validation() {
        assert!(DevicePath::parse("").is_err());
        assert!(DevicePath::parse("/dev/da0").is_err());
        assert!(DevicePath::parse("/dev/zvol/../etc/passwd").is_err());
    }

    #[test]
    fn test_device_path_parent_dataset_validation() {
        // Valid: path is under parent dataset
        let path = DevicePath::parse("/dev/zvol/tank/csi/pvc-abc123").unwrap();
        assert!(path.validate_parent_dataset("tank/csi").is_ok());

        // Valid: nested path under parent dataset
        let nested = DevicePath::parse("/dev/zvol/tank/csi/ns/vol1").unwrap();
        assert!(nested.validate_parent_dataset("tank/csi").is_ok());

        // Invalid: different pool
        let other_pool = DevicePath::parse("/dev/zvol/other/pool/vol").unwrap();
        assert!(other_pool.validate_parent_dataset("tank/csi").is_err());

        // Invalid: sibling dataset
        let sibling = DevicePath::parse("/dev/zvol/tank/other/vol").unwrap();
        assert!(sibling.validate_parent_dataset("tank/csi").is_err());

        // Invalid: parent dataset itself (no volume name)
        let parent_only = DevicePath::from_dataset("tank/csi").unwrap();
        assert!(parent_only.validate_parent_dataset("tank/csi").is_err());

        // Invalid: path traversal attempt (already blocked by parse, but double-check)
        assert!(DevicePath::parse("/dev/zvol/tank/csi/../other/vol").is_err());
    }

    #[test]
    fn test_target_name() {
        let iqn = Iqn::new("iqn.2024-01.org.freebsd.csi", "vol1").unwrap();
        let target: TargetName = iqn.into();
        assert_eq!(target.volume_name(), Some("vol1"));
    }

    #[test]
    fn test_auth_config_auth_group_name() {
        // None returns "no-authentication"
        assert_eq!(
            AuthConfig::None.auth_group_name("vol1"),
            "no-authentication"
        );

        // IscsiChap generates auth group name from volume
        let chap = IscsiChapAuth::new("user", "secret");
        assert_eq!(
            AuthConfig::IscsiChap(chap).auth_group_name("vol1"),
            "ag-vol1"
        );

        // NvmeAuth generates auth group name from volume
        let nvme = NvmeAuth::new("nqn.host", "secret", "sha256");
        assert_eq!(
            AuthConfig::NvmeAuth(nvme).auth_group_name("vol1"),
            "ag-vol1"
        );

        // GroupRef returns the stored name directly
        assert_eq!(
            AuthConfig::GroupRef("ag-custom".to_string()).auth_group_name("vol1"),
            "ag-custom"
        );
    }

    #[test]
    fn test_auth_config_group_ref_serde() {
        let group_ref = AuthConfig::GroupRef("ag-vol1".to_string());
        let json = serde_json::to_string(&group_ref).unwrap();
        assert_eq!(json, r#"{"GroupRef":"ag-vol1"}"#);

        let parsed: AuthConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, group_ref);
    }

    #[test]
    fn test_auth_config_is_some() {
        assert!(!AuthConfig::None.is_some());

        let chap = IscsiChapAuth::new("user", "secret");
        assert!(AuthConfig::IscsiChap(chap).is_some());

        let nvme = NvmeAuth::new("nqn.host", "secret", "sha256");
        assert!(AuthConfig::NvmeAuth(nvme).is_some());

        assert!(AuthConfig::GroupRef("ag-vol1".to_string()).is_some());
    }
}
