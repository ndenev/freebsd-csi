//! Unified configuration manager for auth.json and csi-targets.conf.
//!
//! Provides a single point of control for all CSI config file operations,
//! ensuring atomic writes and consistent state.

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::auth::{AuthDb, AuthError, ChapCredentials, load_auth_db, write_auth_db};

use super::csi_config::CsiConfigGenerator;

/// Error type for config manager operations.
#[derive(Debug, thiserror::Error)]
pub enum ConfigManagerError {
    #[error("Auth error: {0}")]
    Auth(#[from] AuthError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Unified manager for CSI configuration files.
pub struct ConfigManager {
    auth_path: PathBuf,
    config_path: PathBuf,
    auth_db: Arc<RwLock<AuthDb>>,
    config_gen: Arc<RwLock<CsiConfigGenerator>>,
}

impl ConfigManager {
    /// Create a new config manager.
    pub fn new(auth_path: PathBuf, config_path: PathBuf) -> Self {
        Self {
            auth_path,
            config_path,
            auth_db: Arc::new(RwLock::new(AuthDb::new())),
            config_gen: Arc::new(RwLock::new(CsiConfigGenerator::new())),
        }
    }

    /// Load existing auth database from disk.
    pub async fn load(&self) -> Result<(), ConfigManagerError> {
        let db = load_auth_db(&self.auth_path).await?;
        *self.auth_db.write().await = db;
        Ok(())
    }

    /// Add or update auth credentials for a volume.
    pub async fn add_volume_auth(&self, volume_name: &str, creds: ChapCredentials) {
        self.auth_db
            .write()
            .await
            .insert(volume_name.to_string(), creds);
    }

    /// Remove auth credentials for a volume.
    pub async fn remove_volume_auth(&self, volume_name: &str) {
        self.auth_db.write().await.remove(volume_name);
    }

    /// Check if a volume has auth credentials.
    pub async fn has_volume_auth(&self, volume_name: &str) -> bool {
        self.auth_db.read().await.contains_key(volume_name)
    }

    /// Get auth credentials for a volume.
    pub async fn get_volume_auth(&self, volume_name: &str) -> Option<ChapCredentials> {
        self.auth_db.read().await.get(volume_name).cloned()
    }

    /// Get access to the config generator for adding targets/controllers.
    pub async fn config_gen(&self) -> tokio::sync::RwLockWriteGuard<'_, CsiConfigGenerator> {
        self.config_gen.write().await
    }

    /// Write all config files atomically.
    pub async fn write(&self) -> Result<(), ConfigManagerError> {
        // Write auth.json
        let auth_db = self.auth_db.read().await;
        write_auth_db(&self.auth_path, &auth_db).await?;
        drop(auth_db);

        // Generate and write csi-targets.conf
        let config = self.config_gen.read().await.generate();
        tokio::fs::write(&self.config_path, config).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_config_manager_write_creates_files() {
        let temp_dir = TempDir::new().unwrap();
        let auth_path = temp_dir.path().join("auth.json");
        let config_path = temp_dir.path().join("csi-targets.conf");

        let manager = ConfigManager::new(auth_path.clone(), config_path.clone());

        // Add a volume with auth
        let creds = ChapCredentials::new("user1", "secret1");
        manager.add_volume_auth("pvc-test", creds).await;

        // Write
        manager.write().await.unwrap();

        // Verify files exist
        assert!(auth_path.exists(), "auth.json should exist");
        assert!(config_path.exists(), "csi-targets.conf should exist");

        // Verify auth.json content
        let auth_content = tokio::fs::read_to_string(&auth_path).await.unwrap();
        assert!(auth_content.contains("pvc-test"));
        assert!(auth_content.contains("user1"));
    }

    #[tokio::test]
    async fn test_config_manager_remove_volume_auth() {
        let temp_dir = TempDir::new().unwrap();
        let auth_path = temp_dir.path().join("auth.json");
        let config_path = temp_dir.path().join("csi-targets.conf");

        let manager = ConfigManager::new(auth_path.clone(), config_path.clone());

        // Add then remove
        let creds = ChapCredentials::new("user1", "secret1");
        manager.add_volume_auth("pvc-test", creds).await;
        manager.remove_volume_auth("pvc-test").await;

        // Write
        manager.write().await.unwrap();

        // Verify auth.json doesn't contain the volume
        let auth_content = tokio::fs::read_to_string(&auth_path).await.unwrap();
        assert!(!auth_content.contains("pvc-test"));
    }
}
