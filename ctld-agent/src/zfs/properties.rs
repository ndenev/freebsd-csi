//! ZFS user properties for CSI metadata persistence

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ctl::ExportType;

/// Metadata stored as ZFS user property for each volume
///
/// # Security Note
/// This struct is serialized to ZFS user properties which are readable
/// by anyone with root access. Authentication credentials must NOT be
/// stored here. Only the auth-group NAME is stored; actual credentials
/// are persisted in `/etc/ctl.conf` (root-only, managed by ctld).
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
    /// Auth-group name for authentication (e.g., "ag-vol-xyz").
    /// Credentials are stored in /etc/ctl.conf, not here.
    /// None means "no-authentication".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_group: Option<String>,
}

/// ZFS user property name for CSI metadata
pub const METADATA_PROPERTY: &str = "user:csi:metadata";

/// ZFS user property name for CSI snapshot ID
/// This property is set on snapshots to track them even after promotion moves them
pub const SNAPSHOT_ID_PROPERTY: &str = "user:csi:snapshot_id";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_metadata_serialization_with_auth_group() {
        let metadata = VolumeMetadata {
            export_type: ExportType::Iscsi,
            target_name: "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            lun_id: Some(0),
            namespace_id: None,
            parameters: HashMap::new(),
            created_at: 1234567890,
            auth_group: Some("ag-vol1".to_string()),
        };

        let json = serde_json::to_string(&metadata).unwrap();

        // Verify auth_group is present
        assert!(json.contains("ag-vol1"));
        // Verify no credential-like content (security check)
        assert!(!json.contains("IscsiChap"));
        assert!(!json.contains("secret"));
    }

    #[test]
    fn test_volume_metadata_deserialization_with_auth_group() {
        let json = r#"{
            "export_type": "ISCSI",
            "target_name": "iqn.2024-01.org.freebsd.csi:vol1",
            "lun_id": 0,
            "parameters": {},
            "created_at": 1234567890,
            "auth_group": "ag-vol1"
        }"#;

        let metadata: VolumeMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(metadata.auth_group, Some("ag-vol1".to_string()));
    }

    #[test]
    fn test_volume_metadata_no_auth() {
        // Volume without authentication
        let json = r#"{
            "export_type": "NVMEOF",
            "target_name": "nqn.2024-01.org.freebsd.csi:vol1",
            "parameters": {},
            "created_at": 1234567890
        }"#;

        let metadata: VolumeMetadata = serde_json::from_str(json).unwrap();

        assert!(metadata.auth_group.is_none());
    }

    #[test]
    fn test_volume_metadata_roundtrip() {
        let metadata = VolumeMetadata {
            export_type: ExportType::Iscsi,
            target_name: "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            lun_id: Some(0),
            namespace_id: None,
            parameters: HashMap::new(),
            created_at: 1234567890,
            auth_group: Some("ag-vol1".to_string()),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let parsed: VolumeMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.export_type, ExportType::Iscsi);
        assert_eq!(parsed.target_name, "iqn.2024-01.org.freebsd.csi:vol1");
        assert_eq!(parsed.auth_group, Some("ag-vol1".to_string()));
    }
}
