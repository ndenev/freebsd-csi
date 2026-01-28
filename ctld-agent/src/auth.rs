//! Authentication credential storage for CHAP persistence.
//!
//! Stores CHAP credentials in a JSON file (`/var/db/ctld-agent/auth.json`)
//! that survives agent restarts. Credentials are stored securely with
//! restricted file permissions (0600).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CHAP credentials for a volume.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChapCredentials {
    /// Initiator username (required)
    pub user: String,
    /// Initiator secret (required)
    pub secret: String,
    /// Target username for mutual CHAP (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutual_user: Option<String>,
    /// Target secret for mutual CHAP (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutual_secret: Option<String>,
}

impl ChapCredentials {
    /// Create new CHAP credentials without mutual authentication.
    pub fn new(user: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            secret: secret.into(),
            mutual_user: None,
            mutual_secret: None,
        }
    }

    /// Create new CHAP credentials with mutual authentication.
    pub fn with_mutual(
        user: impl Into<String>,
        secret: impl Into<String>,
        mutual_user: impl Into<String>,
        mutual_secret: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            secret: secret.into(),
            mutual_user: Some(mutual_user.into()),
            mutual_secret: Some(mutual_secret.into()),
        }
    }

    /// Check if mutual CHAP is configured.
    pub fn has_mutual(&self) -> bool {
        self.mutual_user.is_some() && self.mutual_secret.is_some()
    }
}

/// Authentication database mapping volume names to CHAP credentials.
pub type AuthDb = HashMap<String, ChapCredentials>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chap_credentials_roundtrip() {
        let creds = ChapCredentials {
            user: "testuser".to_string(),
            secret: "testsecret".to_string(),
            mutual_user: Some("mutualuser".to_string()),
            mutual_secret: Some("mutualsecret".to_string()),
        };

        let json = serde_json::to_string(&creds).unwrap();
        let parsed: ChapCredentials = serde_json::from_str(&json).unwrap();

        assert_eq!(creds.user, parsed.user);
        assert_eq!(creds.secret, parsed.secret);
        assert_eq!(creds.mutual_user, parsed.mutual_user);
        assert_eq!(creds.mutual_secret, parsed.mutual_secret);
    }

    #[test]
    fn test_auth_db_roundtrip() {
        let mut db = AuthDb::new();
        db.insert(
            "pvc-abc123".to_string(),
            ChapCredentials {
                user: "user1".to_string(),
                secret: "secret1".to_string(),
                mutual_user: None,
                mutual_secret: None,
            },
        );
        db.insert(
            "pvc-def456".to_string(),
            ChapCredentials {
                user: "user2".to_string(),
                secret: "secret:with:colons".to_string(),
                mutual_user: Some("mutual".to_string()),
                mutual_secret: Some("msecret".to_string()),
            },
        );

        let json = serde_json::to_string_pretty(&db).unwrap();
        let parsed: AuthDb = serde_json::from_str(&json).unwrap();

        assert_eq!(db.len(), parsed.len());
        assert_eq!(db.get("pvc-abc123").unwrap().user, "user1");
        assert_eq!(db.get("pvc-def456").unwrap().secret, "secret:with:colons");
    }

    #[test]
    fn test_chap_credentials_skip_none_fields() {
        let creds = ChapCredentials {
            user: "user".to_string(),
            secret: "secret".to_string(),
            mutual_user: None,
            mutual_secret: None,
        };

        let json = serde_json::to_string(&creds).unwrap();
        assert!(!json.contains("mutual_user"));
        assert!(!json.contains("mutual_secret"));
    }
}
