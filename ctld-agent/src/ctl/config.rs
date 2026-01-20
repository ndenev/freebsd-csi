use serde::{Deserialize, Serialize};

/// Represents a LUN (Logical Unit Number) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lun {
    /// LUN ID (0-255)
    pub id: u32,
    /// Backend type (typically "block")
    pub backend: String,
    /// Path to the backing device (e.g., /dev/zvol/tank/csi/vol1)
    pub device_path: String,
    /// CTL device ID (assigned by ctladm)
    pub ctl_lun_id: Option<u32>,
    /// Block size in bytes (default: 512)
    pub blocksize: u32,
    /// Optional serial number
    pub serial: Option<String>,
}

impl Lun {
    /// Create a new LUN configuration with default settings
    pub fn new(id: u32, device_path: String) -> Self {
        Self {
            id,
            backend: "block".to_string(),
            device_path,
            ctl_lun_id: None,
            blocksize: 512,
            serial: None,
        }
    }

    /// Set a custom block size
    pub fn with_blocksize(mut self, blocksize: u32) -> Self {
        self.blocksize = blocksize;
        self
    }

    /// Set a custom serial number
    pub fn with_serial(mut self, serial: String) -> Self {
        self.serial = Some(serial);
        self
    }
}

/// Represents an iSCSI target configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IscsiTarget {
    /// Target name (the volume/short name, not full IQN)
    pub name: String,
    /// Full iSCSI Qualified Name
    pub iqn: String,
    /// Portal group tag (default: 1)
    pub portal_group_tag: u32,
    /// Associated LUNs
    pub luns: Vec<Lun>,
    /// Target alias (optional human-readable name)
    pub alias: Option<String>,
    /// Authentication group (optional)
    pub auth_group: Option<String>,
}

impl IscsiTarget {
    /// Create a new iSCSI target
    pub fn new(name: String, iqn: String) -> Self {
        Self {
            name,
            iqn,
            portal_group_tag: 1,
            luns: Vec::new(),
            alias: None,
            auth_group: None,
        }
    }

    /// Generate an IQN for a volume
    pub fn generate_iqn(base_iqn: &str, volume_name: &str) -> String {
        format!("{}:{}", base_iqn, volume_name.replace('/', "-"))
    }

    /// Set the portal group tag
    pub fn with_portal_group(mut self, tag: u32) -> Self {
        self.portal_group_tag = tag;
        self
    }

    /// Add a LUN to this target
    pub fn with_lun(mut self, lun: Lun) -> Self {
        self.luns.push(lun);
        self
    }

    /// Set an alias for this target
    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    /// Set an authentication group for this target
    pub fn with_auth_group(mut self, auth_group: String) -> Self {
        self.auth_group = Some(auth_group);
        self
    }
}

/// Represents a portal group configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalGroup {
    /// Portal group tag
    pub tag: u32,
    /// Portal group name
    pub name: String,
    /// Listen addresses (e.g., ["0.0.0.0:3260"])
    pub listen: Vec<String>,
    /// Discovery authentication group (optional)
    pub discovery_auth_group: Option<String>,
}

impl PortalGroup {
    /// Create a new portal group
    pub fn new(tag: u32, name: String) -> Self {
        Self {
            tag,
            name,
            listen: vec!["0.0.0.0:3260".to_string()],
            discovery_auth_group: None,
        }
    }

    /// Add a listen address
    pub fn with_listen(mut self, address: String) -> Self {
        self.listen.push(address);
        self
    }

    /// Set the discovery auth group
    pub fn with_discovery_auth(mut self, auth_group: String) -> Self {
        self.discovery_auth_group = Some(auth_group);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lun_new() {
        let lun = Lun::new(0, "/dev/zvol/tank/vol1".to_string());
        assert_eq!(lun.id, 0);
        assert_eq!(lun.backend, "block");
        assert_eq!(lun.device_path, "/dev/zvol/tank/vol1");
        assert_eq!(lun.blocksize, 512);
        assert!(lun.serial.is_none());
    }

    #[test]
    fn test_lun_builder() {
        let lun = Lun::new(1, "/dev/zvol/tank/vol2".to_string())
            .with_blocksize(4096)
            .with_serial("SN12345".to_string());

        assert_eq!(lun.id, 1);
        assert_eq!(lun.blocksize, 4096);
        assert_eq!(lun.serial.as_deref(), Some("SN12345"));
    }

    #[test]
    fn test_iscsi_target_new() {
        let target = IscsiTarget::new(
            "vol1".to_string(),
            "iqn.2024-01.com.example:vol1".to_string(),
        );

        assert_eq!(target.name, "vol1");
        assert_eq!(target.iqn, "iqn.2024-01.com.example:vol1");
        assert_eq!(target.portal_group_tag, 1);
        assert!(target.luns.is_empty());
    }

    #[test]
    fn test_iscsi_target_builder() {
        let lun = Lun::new(0, "/dev/zvol/tank/vol1".to_string());
        let target = IscsiTarget::new(
            "vol1".to_string(),
            "iqn.2024-01.com.example:vol1".to_string(),
        )
        .with_portal_group(2)
        .with_lun(lun)
        .with_alias("Test Volume".to_string());

        assert_eq!(target.portal_group_tag, 2);
        assert_eq!(target.luns.len(), 1);
        assert_eq!(target.alias.as_deref(), Some("Test Volume"));
    }

    #[test]
    fn test_portal_group_new() {
        let pg = PortalGroup::new(1, "pg1".to_string());

        assert_eq!(pg.tag, 1);
        assert_eq!(pg.name, "pg1");
        assert_eq!(pg.listen, vec!["0.0.0.0:3260"]);
    }
}
