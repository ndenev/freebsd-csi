//! ZFS user properties for CSI metadata persistence

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ctl::ExportType;

/// Current metadata schema version.
/// Increment when making breaking changes to VolumeMetadata.
pub const CURRENT_SCHEMA_VERSION: u32 = 2;

/// Metadata stored as ZFS user property for each volume
///
/// # Schema Versioning
/// The `schema_version` field tracks the metadata format version.
/// - Version 1: Original format (implicit, missing field)
/// - Version 2: Standardized camelCase parameters
///
/// Old metadata without `schema_version` is treated as v1 and remains compatible.
///
/// # Security Note
/// This struct is serialized to ZFS user properties which are readable
/// by anyone with root access. Authentication credentials must NOT be
/// stored here. Only the auth-group NAME is stored; actual credentials
/// are persisted in `/etc/ctl.conf` (root-only, managed by ctld).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMetadata {
    /// Schema version for forward compatibility
    /// Defaults to 1 for backwards compatibility with old metadata
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
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

/// Default schema version for deserialization of old metadata
fn default_schema_version() -> u32 {
    1
}

impl VolumeMetadata {
    /// Create new metadata with current schema version
    pub fn new(
        export_type: ExportType,
        target_name: String,
        lun_id: Option<u32>,
        namespace_id: Option<u32>,
        parameters: HashMap<String, String>,
        created_at: i64,
        auth_group: Option<String>,
    ) -> Self {
        Self {
            schema_version: CURRENT_SCHEMA_VERSION,
            export_type,
            target_name,
            lun_id,
            namespace_id,
            parameters,
            created_at,
            auth_group,
        }
    }

    /// Check if metadata needs migration to current version
    pub fn needs_migration(&self) -> bool {
        self.schema_version < CURRENT_SCHEMA_VERSION
    }

    /// Migrate metadata to current schema version.
    /// Returns true if migration was performed.
    pub fn migrate(&mut self) -> bool {
        if !self.needs_migration() {
            return false;
        }

        // Migration from v1 to v2: parameter keys may use old snake_case names
        // Convert any snake_case keys to camelCase in parameters
        if self.schema_version == 1 {
            let migrations = [
                ("export_type", "exportType"),
                ("fs_type", "fsType"),
                ("block_size", "blockSize"),
                ("physical_block_size", "physicalBlockSize"),
                ("pblocksize", "physicalBlockSize"),
                ("enable_unmap", "enableUnmap"),
                ("unmap", "enableUnmap"),
                ("clone_mode", "cloneMode"),
            ];

            for (old_key, new_key) in migrations {
                if let Some(value) = self.parameters.remove(old_key) {
                    self.parameters.insert(new_key.to_string(), value);
                }
            }
        }

        self.schema_version = CURRENT_SCHEMA_VERSION;
        true
    }
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
    fn test_volume_metadata_new_has_current_version() {
        let metadata = VolumeMetadata::new(
            ExportType::Iscsi,
            "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            Some(0),
            None,
            HashMap::new(),
            1234567890,
            None,
        );

        assert_eq!(metadata.schema_version, CURRENT_SCHEMA_VERSION);
    }

    #[test]
    fn test_volume_metadata_old_format_gets_version_1() {
        // Old format without schema_version field
        let json = r#"{
            "export_type": "ISCSI",
            "target_name": "iqn.2024-01.org.freebsd.csi:vol1",
            "lun_id": 0,
            "parameters": {},
            "created_at": 1234567890
        }"#;

        let metadata: VolumeMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(metadata.schema_version, 1);
        assert!(metadata.needs_migration());
    }

    #[test]
    fn test_volume_metadata_migration_v1_to_v2() {
        let mut params = HashMap::new();
        params.insert("fs_type".to_string(), "ext4".to_string());
        params.insert("block_size".to_string(), "4096".to_string());
        params.insert("enable_unmap".to_string(), "true".to_string());

        let json = serde_json::json!({
            "export_type": "ISCSI",
            "target_name": "iqn.2024-01.org.freebsd.csi:vol1",
            "lun_id": 0,
            "parameters": params,
            "created_at": 1234567890
        });

        let mut metadata: VolumeMetadata = serde_json::from_value(json).unwrap();
        assert_eq!(metadata.schema_version, 1);
        assert!(metadata.needs_migration());

        // Perform migration
        let migrated = metadata.migrate();
        assert!(migrated);
        assert_eq!(metadata.schema_version, CURRENT_SCHEMA_VERSION);
        assert!(!metadata.needs_migration());

        // Check parameters were migrated
        assert_eq!(metadata.parameters.get("fsType"), Some(&"ext4".to_string()));
        assert_eq!(
            metadata.parameters.get("blockSize"),
            Some(&"4096".to_string())
        );
        assert_eq!(
            metadata.parameters.get("enableUnmap"),
            Some(&"true".to_string())
        );

        // Old keys should be removed
        assert!(metadata.parameters.get("fs_type").is_none());
        assert!(metadata.parameters.get("block_size").is_none());
        assert!(metadata.parameters.get("enable_unmap").is_none());
    }

    #[test]
    fn test_volume_metadata_no_migration_needed() {
        let metadata = VolumeMetadata::new(
            ExportType::Iscsi,
            "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            Some(0),
            None,
            HashMap::new(),
            1234567890,
            None,
        );

        assert!(!metadata.needs_migration());
    }

    #[test]
    fn test_volume_metadata_serialization_includes_version() {
        let metadata = VolumeMetadata::new(
            ExportType::Iscsi,
            "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            Some(0),
            None,
            HashMap::new(),
            1234567890,
            Some("ag-vol1".to_string()),
        );

        let json = serde_json::to_string(&metadata).unwrap();

        assert!(json.contains("schema_version"));
        assert!(json.contains(&CURRENT_SCHEMA_VERSION.to_string()));
    }

    #[test]
    fn test_volume_metadata_deserialization_with_auth_group() {
        let json = r#"{
            "schema_version": 2,
            "export_type": "ISCSI",
            "target_name": "iqn.2024-01.org.freebsd.csi:vol1",
            "lun_id": 0,
            "parameters": {},
            "created_at": 1234567890,
            "auth_group": "ag-vol1"
        }"#;

        let metadata: VolumeMetadata = serde_json::from_str(json).unwrap();

        assert_eq!(metadata.schema_version, 2);
        assert_eq!(metadata.auth_group, Some("ag-vol1".to_string()));
    }

    #[test]
    fn test_volume_metadata_roundtrip() {
        let metadata = VolumeMetadata::new(
            ExportType::Iscsi,
            "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            Some(0),
            None,
            HashMap::new(),
            1234567890,
            Some("ag-vol1".to_string()),
        );

        let json = serde_json::to_string(&metadata).unwrap();
        let parsed: VolumeMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.schema_version, CURRENT_SCHEMA_VERSION);
        assert_eq!(parsed.export_type, ExportType::Iscsi);
        assert_eq!(parsed.target_name, "iqn.2024-01.org.freebsd.csi:vol1");
        assert_eq!(parsed.auth_group, Some("ag-vol1".to_string()));
    }
}
