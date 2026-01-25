//! Type-safe wrappers for CSI parameters.
//!
//! These types provide compile-time safety for parameters that are parsed
//! from StorageClass parameters and volume contexts. Each type implements
//! `FromStr` for parsing at API boundaries and converts to proto types
//! when calling the ctld-agent.

use std::fmt::{self, Display};
use std::str::FromStr;

use crate::agent;

// ============================================================================
// ExportType
// ============================================================================

/// Storage export protocol type.
///
/// Determines whether volumes are exported via iSCSI or NVMeoF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportType {
    /// iSCSI protocol (default)
    #[default]
    Iscsi,
    /// NVMe over Fabrics protocol
    Nvmeof,
}

impl ExportType {
    /// Default port for this protocol.
    pub const fn default_port(self) -> u16 {
        match self {
            ExportType::Iscsi => 3260,
            ExportType::Nvmeof => 4420,
        }
    }
}

impl Display for ExportType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExportType::Iscsi => write!(f, "iscsi"),
            ExportType::Nvmeof => write!(f, "nvmeof"),
        }
    }
}

impl FromStr for ExportType {
    type Err = ExportTypeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "iscsi" => Ok(ExportType::Iscsi),
            "nvmeof" | "nvme" => Ok(ExportType::Nvmeof),
            _ => Err(ExportTypeParseError(s.to_string())),
        }
    }
}

/// Error returned when parsing an invalid export type.
#[derive(Debug, Clone)]
pub struct ExportTypeParseError(String);

impl Display for ExportTypeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown export type '{}': expected 'iscsi' or 'nvmeof'",
            self.0
        )
    }
}

impl std::error::Error for ExportTypeParseError {}

impl From<ExportType> for agent::ExportType {
    fn from(value: ExportType) -> Self {
        match value {
            ExportType::Iscsi => agent::ExportType::Iscsi,
            ExportType::Nvmeof => agent::ExportType::Nvmeof,
        }
    }
}

// ============================================================================
// CloneMode
// ============================================================================

/// Clone mode for creating volumes from snapshots.
///
/// Determines whether to use ZFS clone (linked, fast) or ZFS send/recv (copy, independent).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CloneMode {
    /// Server chooses (defaults to Linked)
    #[default]
    Unspecified,
    /// Fast clone using ZFS clone (creates dependency on source)
    Linked,
    /// Full copy using ZFS send/recv (independent volume)
    Copy,
}

impl Display for CloneMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloneMode::Unspecified => write!(f, "unspecified"),
            CloneMode::Linked => write!(f, "linked"),
            CloneMode::Copy => write!(f, "copy"),
        }
    }
}

impl FromStr for CloneMode {
    type Err = CloneModeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "copy" | "independent" => Ok(CloneMode::Copy),
            "linked" | "clone" => Ok(CloneMode::Linked),
            "" | "unspecified" | "default" => Ok(CloneMode::Unspecified),
            _ => Err(CloneModeParseError(s.to_string())),
        }
    }
}

/// Error returned when parsing an invalid clone mode.
#[derive(Debug, Clone)]
pub struct CloneModeParseError(String);

impl Display for CloneModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown clone mode '{}': expected 'copy', 'linked', or 'unspecified'",
            self.0
        )
    }
}

impl std::error::Error for CloneModeParseError {}

impl From<CloneMode> for agent::CloneMode {
    fn from(value: CloneMode) -> Self {
        match value {
            CloneMode::Unspecified => agent::CloneMode::Unspecified,
            CloneMode::Linked => agent::CloneMode::Linked,
            CloneMode::Copy => agent::CloneMode::Copy,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_type_from_str() {
        assert_eq!("iscsi".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("ISCSI".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("iScSi".parse::<ExportType>().unwrap(), ExportType::Iscsi);
        assert_eq!("nvmeof".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("NVMEOF".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("nvme".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert_eq!("NvMe".parse::<ExportType>().unwrap(), ExportType::Nvmeof);
        assert!("unknown".parse::<ExportType>().is_err());
    }

    #[test]
    fn test_export_type_display() {
        assert_eq!(ExportType::Iscsi.to_string(), "iscsi");
        assert_eq!(ExportType::Nvmeof.to_string(), "nvmeof");
    }

    #[test]
    fn test_export_type_default_port() {
        assert_eq!(ExportType::Iscsi.default_port(), 3260);
        assert_eq!(ExportType::Nvmeof.default_port(), 4420);
    }

    #[test]
    fn test_clone_mode_from_str() {
        assert_eq!("copy".parse::<CloneMode>().unwrap(), CloneMode::Copy);
        assert_eq!(
            "independent".parse::<CloneMode>().unwrap(),
            CloneMode::Copy
        );
        assert_eq!("linked".parse::<CloneMode>().unwrap(), CloneMode::Linked);
        assert_eq!("clone".parse::<CloneMode>().unwrap(), CloneMode::Linked);
        assert_eq!(
            "".parse::<CloneMode>().unwrap(),
            CloneMode::Unspecified
        );
        assert_eq!(
            "unspecified".parse::<CloneMode>().unwrap(),
            CloneMode::Unspecified
        );
        assert!("unknown".parse::<CloneMode>().is_err());
    }

    #[test]
    fn test_clone_mode_display() {
        assert_eq!(CloneMode::Unspecified.to_string(), "unspecified");
        assert_eq!(CloneMode::Linked.to_string(), "linked");
        assert_eq!(CloneMode::Copy.to_string(), "copy");
    }

    #[test]
    fn test_export_type_to_proto() {
        let proto: agent::ExportType = ExportType::Iscsi.into();
        assert_eq!(proto, agent::ExportType::Iscsi);

        let proto: agent::ExportType = ExportType::Nvmeof.into();
        assert_eq!(proto, agent::ExportType::Nvmeof);
    }

    #[test]
    fn test_clone_mode_to_proto() {
        let proto: agent::CloneMode = CloneMode::Unspecified.into();
        assert_eq!(proto, agent::CloneMode::Unspecified);

        let proto: agent::CloneMode = CloneMode::Linked.into();
        assert_eq!(proto, agent::CloneMode::Linked);

        let proto: agent::CloneMode = CloneMode::Copy.into();
        assert_eq!(proto, agent::CloneMode::Copy);
    }
}
