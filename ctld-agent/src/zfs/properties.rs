//! ZFS user properties for CSI metadata persistence

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ctl::ExportType;

/// Metadata stored as ZFS user property for each volume
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMetadata {
    /// Export type (iSCSI or NVMeoF)
    pub export_type: ExportType,
    /// Target name (IQN for iSCSI, NQN for NVMeoF)
    pub target_name: String,
    /// LUN ID for iSCSI exports
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lun_id: Option<u32>,
    /// Namespace ID for NVMeoF exports
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<u32>,
    /// Custom parameters from StorageClass
    #[serde(default)]
    pub parameters: HashMap<String, String>,
    /// Creation timestamp (Unix epoch)
    pub created_at: i64,
}

/// ZFS user property name for CSI metadata
pub const METADATA_PROPERTY: &str = "user:csi:metadata";
