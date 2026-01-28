//! Authentication credential storage for CHAP persistence.
//!
//! Stores CHAP credentials in a JSON file (`/var/db/ctld-agent/auth.json`)
//! that survives agent restarts. Credentials are stored securely with
//! restricted file permissions (0600).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::Path;

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

/// Error type for auth operations.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Load the auth database from a JSON file.
///
/// Returns an empty AuthDb if the file doesn't exist.
/// Returns an error if the file exists but cannot be parsed.
pub async fn load_auth_db(path: impl AsRef<Path>) -> Result<AuthDb, AuthError> {
    let path = path.as_ref();

    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Ok(AuthDb::new());
    }

    let content = tokio::fs::read_to_string(path).await?;
    let db: AuthDb = serde_json::from_str(&content)?;
    Ok(db)
}

/// Write the auth database to a JSON file atomically.
///
/// Uses the crash-safe pattern:
/// 1. Write to .new file
/// 2. Copy current to .old (backup)
/// 3. Rename .new to current (atomic)
///
/// File is written with 0600 permissions (owner read/write only).
pub async fn write_auth_db(path: impl AsRef<Path>, db: &AuthDb) -> Result<(), AuthError> {
    use tokio::io::AsyncWriteExt;

    let path = path.as_ref();
    let new_path = path.with_extension("json.new");
    let old_path = path.with_extension("json.old");

    // 1. Write to .new file
    let content = serde_json::to_string_pretty(db)?;
    let mut file = tokio::fs::File::create(&new_path).await?;
    file.write_all(content.as_bytes()).await?;
    file.sync_all().await?;
    drop(file);

    // Set permissions to 0600 (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(&new_path, perms).await?;
    }

    // 2. Copy current to .old (if exists)
    if tokio::fs::try_exists(path).await.unwrap_or(false) {
        tokio::fs::copy(path, &old_path).await?;
    }

    // 3. Rename .new to current (atomic on POSIX)
    tokio::fs::rename(&new_path, path).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_auth_db_file_roundtrip() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let auth_path = temp_dir.path().join("auth.json");

        let mut db = AuthDb::new();
        db.insert("vol1".to_string(), ChapCredentials::new("user1", "secret1"));
        db.insert(
            "vol2".to_string(),
            ChapCredentials::with_mutual("user2", "secret2", "mutual2", "msecret2"),
        );

        // Write
        write_auth_db(&auth_path, &db).await.unwrap();

        // Verify file permissions (Unix only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(&auth_path).unwrap().permissions();
            assert_eq!(perms.mode() & 0o777, 0o600, "File should be 0600");
        }

        // Read back
        let loaded = load_auth_db(&auth_path).await.unwrap();
        assert_eq!(db.len(), loaded.len());
        assert_eq!(
            db.get("vol1").unwrap().user,
            loaded.get("vol1").unwrap().user
        );
    }

    #[tokio::test]
    async fn test_load_auth_db_missing_file() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let auth_path = temp_dir.path().join("nonexistent.json");

        let db = load_auth_db(&auth_path).await.unwrap();
        assert!(db.is_empty(), "Missing file should return empty AuthDb");
    }

    #[tokio::test]
    async fn test_write_auth_db_creates_backup() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let auth_path = temp_dir.path().join("auth.json");
        let backup_path = temp_dir.path().join("auth.json.old");

        // Write initial
        let mut db1 = AuthDb::new();
        db1.insert("vol1".to_string(), ChapCredentials::new("user1", "secret1"));
        write_auth_db(&auth_path, &db1).await.unwrap();

        // Write updated (should create backup)
        let mut db2 = AuthDb::new();
        db2.insert("vol2".to_string(), ChapCredentials::new("user2", "secret2"));
        write_auth_db(&auth_path, &db2).await.unwrap();

        // Verify backup exists and contains old data
        assert!(backup_path.exists(), "Backup file should exist");
        let backup_content = tokio::fs::read_to_string(&backup_path).await.unwrap();
        assert!(
            backup_content.contains("vol1"),
            "Backup should contain old data"
        );

        // Verify current contains new data
        let loaded = load_auth_db(&auth_path).await.unwrap();
        assert!(loaded.contains_key("vol2"));
        assert!(!loaded.contains_key("vol1"));
    }

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
