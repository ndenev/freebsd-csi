# Robustness Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve ctld-agent robustness with config separation, auth persistence, idempotent operations, and startup validation.

**Architecture:** Replace marker-based config with `.include` directive. Store CHAP credentials in `auth.json`. Add validate-first + recover-on-retry pattern to CreateVolume. Validate portal/transport groups on startup.

**Tech Stack:** Rust, serde_json, tokio, UCL config format

**Design Document:** See `docs/plans/2025-01-27-robustness-improvements.md` for full design rationale.

---

## Phase 1: Auth Persistence Module

Create the foundational auth.json storage module.

### Task 1.1: Create auth.rs module with types

**Files:**
- Create: `ctld-agent/src/auth.rs`
- Modify: `ctld-agent/src/lib.rs`

**Step 1: Write the failing test**

```rust
// In ctld-agent/src/auth.rs at the bottom

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
        db.insert("pvc-abc123".to_string(), ChapCredentials {
            user: "user1".to_string(),
            secret: "secret1".to_string(),
            mutual_user: None,
            mutual_secret: None,
        });
        db.insert("pvc-def456".to_string(), ChapCredentials {
            user: "user2".to_string(),
            secret: "secret:with:colons".to_string(),
            mutual_user: Some("mutual".to_string()),
            mutual_secret: Some("msecret".to_string()),
        });

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
```

**Step 2: Run test to verify it fails**

Run: `cd ctld-agent && cargo test auth::tests --no-run 2>&1 | head -20`
Expected: Compilation error - module auth not found

**Step 3: Write minimal implementation**

```rust
// ctld-agent/src/auth.rs
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
```

**Step 4: Add module to lib.rs**

```rust
// Add to ctld-agent/src/lib.rs after existing pub mod declarations
pub mod auth;
```

**Step 5: Run test to verify it passes**

Run: `cd ctld-agent && cargo test auth::tests -v`
Expected: All 3 tests pass

**Step 6: Commit**

```bash
git add ctld-agent/src/auth.rs ctld-agent/src/lib.rs
git commit -m "feat(auth): add ChapCredentials and AuthDb types for CHAP persistence"
```

---

### Task 1.2: Add auth.json file I/O with atomic writes

**Files:**
- Modify: `ctld-agent/src/auth.rs`

**Step 1: Write the failing test**

```rust
// Add to ctld-agent/src/auth.rs tests module

#[tokio::test]
async fn test_auth_db_file_roundtrip() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let auth_path = temp_dir.path().join("auth.json");

    let mut db = AuthDb::new();
    db.insert("vol1".to_string(), ChapCredentials::new("user1", "secret1"));
    db.insert("vol2".to_string(), ChapCredentials::with_mutual(
        "user2", "secret2", "mutual2", "msecret2"
    ));

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
    assert_eq!(db.get("vol1").unwrap().user, loaded.get("vol1").unwrap().user);
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
    assert!(backup_content.contains("vol1"), "Backup should contain old data");

    // Verify current contains new data
    let loaded = load_auth_db(&auth_path).await.unwrap();
    assert!(loaded.contains_key("vol2"));
    assert!(!loaded.contains_key("vol1"));
}
```

**Step 2: Run test to verify it fails**

Run: `cd ctld-agent && cargo test auth::tests::test_auth_db_file --no-run 2>&1 | head -20`
Expected: Compilation error - write_auth_db and load_auth_db not found

**Step 3: Write minimal implementation**

```rust
// Add to ctld-agent/src/auth.rs after AuthDb type definition

use std::path::Path;
use std::io;

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
```

**Step 4: Add thiserror to Cargo.toml if not present**

Run: `grep -q "thiserror" ctld-agent/Cargo.toml || echo 'thiserror = "1"' >> ctld-agent/Cargo.toml`

**Step 5: Run test to verify it passes**

Run: `cd ctld-agent && cargo test auth::tests -v`
Expected: All 6 tests pass

**Step 6: Commit**

```bash
git add ctld-agent/src/auth.rs ctld-agent/Cargo.toml
git commit -m "feat(auth): add auth.json file I/O with atomic writes and backup"
```

---

## Phase 2: Standalone Config Generation

Refactor UCL config to generate standalone `csi-targets.conf` instead of marker-based sections.

### Task 2.1: Create CsiConfigGenerator for standalone config

**Files:**
- Create: `ctld-agent/src/ctl/csi_config.rs`
- Modify: `ctld-agent/src/ctl/mod.rs`

**Step 1: Write the failing test**

```rust
// In ctld-agent/src/ctl/csi_config.rs at the bottom

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctl::ucl_config::{Target, Controller, CtlOptions};
    use crate::auth::ChapCredentials;

    #[test]
    fn test_generate_empty_config() {
        let gen = CsiConfigGenerator::new();
        let config = gen.generate();

        assert!(config.contains("# CSI-managed targets"));
        assert!(config.contains("# Generated by ctld-agent"));
    }

    #[test]
    fn test_generate_with_iscsi_target() {
        let mut gen = CsiConfigGenerator::new();

        let target = Target::new(
            "ag-pvc-test".to_string(),
            "pg0".to_string(),
            0,
            "/dev/zvol/tank/csi/pvc-test".to_string(),
            "pvc-test",
        );
        gen.add_iscsi_target("iqn.2024-01.org.freebsd.csi:pvc-test", target);

        let config = gen.generate();

        assert!(config.contains("target \"iqn.2024-01.org.freebsd.csi:pvc-test\""));
        assert!(config.contains("auth-group = \"ag-pvc-test\""));
        assert!(config.contains("portal-group = \"pg0\""));
        assert!(config.contains("lun 0 {"));
    }

    #[test]
    fn test_generate_with_auth_group() {
        let mut gen = CsiConfigGenerator::new();

        let creds = ChapCredentials::new("user1", "secret1");
        gen.add_auth_group("ag-pvc-test", &creds);

        let target = Target::new(
            "ag-pvc-test".to_string(),
            "pg0".to_string(),
            0,
            "/dev/zvol/tank/csi/pvc-test".to_string(),
            "pvc-test",
        );
        gen.add_iscsi_target("iqn.2024-01.org.freebsd.csi:pvc-test", target);

        let config = gen.generate();

        // Auth group should appear before target
        let auth_pos = config.find("auth-group \"ag-pvc-test\"").unwrap();
        let target_pos = config.find("target \"iqn.2024-01.org.freebsd.csi:pvc-test\"").unwrap();
        assert!(auth_pos < target_pos, "Auth group must be defined before target");

        assert!(config.contains("chap ["));
        assert!(config.contains("user = \"user1\""));
        assert!(config.contains("secret = \"secret1\""));
    }

    #[test]
    fn test_generate_with_nvmeof_controller() {
        let mut gen = CsiConfigGenerator::new();

        let controller = Controller::new(
            "no-authentication".to_string(),
            "tg0".to_string(),
            1,
            "/dev/zvol/tank/csi/pvc-nvme".to_string(),
            "pvc-nvme",
        );
        gen.add_nvmeof_controller("nqn.2024-01.org.freebsd.csi:pvc-nvme", controller);

        let config = gen.generate();

        assert!(config.contains("controller \"nqn.2024-01.org.freebsd.csi:pvc-nvme\""));
        assert!(config.contains("transport-group = \"tg0\""));
        assert!(config.contains("namespace 1 {"));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd ctld-agent && cargo test ctl::csi_config::tests --no-run 2>&1 | head -20`
Expected: Compilation error - module csi_config not found

**Step 3: Write minimal implementation**

```rust
// ctld-agent/src/ctl/csi_config.rs
//! Standalone CSI config generator.
//!
//! Generates a complete UCL config file (`csi-targets.conf`) containing only
//! CSI-managed auth-groups, targets, and controllers. This file is included
//! via `.include` directive in the user's main `/etc/ctl.conf`.

use std::fmt::Write;

use crate::auth::ChapCredentials;
use super::ucl_config::{Target, Controller, ToUcl, AuthGroup, ChapCredential};

/// Generator for standalone CSI config file.
pub struct CsiConfigGenerator {
    auth_groups: Vec<(String, AuthGroup)>,
    iscsi_targets: Vec<(String, Target)>,
    nvmeof_controllers: Vec<(String, Controller)>,
}

impl CsiConfigGenerator {
    /// Create a new empty generator.
    pub fn new() -> Self {
        Self {
            auth_groups: Vec::new(),
            iscsi_targets: Vec::new(),
            nvmeof_controllers: Vec::new(),
        }
    }

    /// Add an auth group with CHAP credentials.
    pub fn add_auth_group(&mut self, name: &str, creds: &ChapCredentials) {
        let auth_group = AuthGroup {
            chap: Some(ChapCredential {
                username: creds.user.clone(),
                secret: creds.secret.clone(),
            }),
            chap_mutual: creds.mutual_user.as_ref().map(|u| ChapCredential {
                username: u.clone(),
                secret: creds.mutual_secret.clone().unwrap_or_default(),
            }),
            host_nqn: None,
        };
        self.auth_groups.push((name.to_string(), auth_group));
    }

    /// Add an iSCSI target.
    pub fn add_iscsi_target(&mut self, iqn: &str, target: Target) {
        self.iscsi_targets.push((iqn.to_string(), target));
    }

    /// Add an NVMeoF controller.
    pub fn add_nvmeof_controller(&mut self, nqn: &str, controller: Controller) {
        self.nvmeof_controllers.push((nqn.to_string(), controller));
    }

    /// Generate the complete UCL config content.
    pub fn generate(&self) -> String {
        let mut config = String::new();

        // Header
        writeln!(config, "# CSI-managed targets - DO NOT EDIT MANUALLY").unwrap();
        writeln!(config, "# Generated by ctld-agent").unwrap();
        writeln!(config, "# This file is included by /etc/ctl.conf").unwrap();
        writeln!(config).unwrap();

        // Auth groups (must come before targets that reference them)
        for (name, auth_group) in &self.auth_groups {
            writeln!(config, "auth-group \"{}\" {{", name).unwrap();
            config.push_str(&auth_group.to_ucl(1));
            writeln!(config, "}}").unwrap();
            writeln!(config).unwrap();
        }

        // iSCSI targets
        for (iqn, target) in &self.iscsi_targets {
            writeln!(config, "target \"{}\" {{", iqn).unwrap();
            config.push_str(&target.to_ucl(1));
            writeln!(config, "}}").unwrap();
            writeln!(config).unwrap();
        }

        // NVMeoF controllers
        for (nqn, controller) in &self.nvmeof_controllers {
            writeln!(config, "controller \"{}\" {{", nqn).unwrap();
            config.push_str(&controller.to_ucl(1));
            writeln!(config, "}}").unwrap();
            writeln!(config).unwrap();
        }

        config
    }

    /// Clear all entries.
    pub fn clear(&mut self) {
        self.auth_groups.clear();
        self.iscsi_targets.clear();
        self.nvmeof_controllers.clear();
    }
}

impl Default for CsiConfigGenerator {
    fn default() -> Self {
        Self::new()
    }
}
```

**Step 4: Add module to ctl/mod.rs**

```rust
// Add to ctld-agent/src/ctl/mod.rs after existing mod declarations
mod csi_config;
pub use csi_config::CsiConfigGenerator;
```

**Step 5: Run test to verify it passes**

Run: `cd ctld-agent && cargo test ctl::csi_config::tests -v`
Expected: All 4 tests pass

**Step 6: Commit**

```bash
git add ctld-agent/src/ctl/csi_config.rs ctld-agent/src/ctl/mod.rs
git commit -m "feat(ctl): add CsiConfigGenerator for standalone config files"
```

---

## Phase 3: Portal/Transport Group Validation

Add startup validation to ensure referenced groups exist.

### Task 3.1: Rename --portal-group-name to --portal-group

**Files:**
- Modify: `ctld-agent/src/main.rs`

**Step 1: Update argument definition**

Change in `ctld-agent/src/main.rs`:

```rust
// FROM:
    /// Portal group name for iSCSI targets (used in UCL config)
    #[arg(long, env = "CTL_PORTAL_GROUP_NAME", default_value = "pg0")]
    portal_group_name: String,

// TO:
    /// Portal group name for iSCSI targets (used in UCL config)
    #[arg(long, env = "CTL_PORTAL_GROUP", default_value = "pg0")]
    portal_group: String,
```

**Step 2: Update all usages of portal_group_name to portal_group**

Run: `cd ctld-agent && grep -n "portal_group_name" src/main.rs`

Update each occurrence to `portal_group`.

**Step 3: Run to verify it compiles**

Run: `cd ctld-agent && cargo build`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add ctld-agent/src/main.rs
git commit -m "refactor(args): rename --portal-group-name to --portal-group"
```

---

### Task 3.2: Add config parser for portal/transport group validation

**Files:**
- Create: `ctld-agent/src/ctl/config_validator.rs`
- Modify: `ctld-agent/src/ctl/mod.rs`

**Step 1: Write the failing test**

```rust
// In ctld-agent/src/ctl/config_validator.rs at the bottom

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[tokio::test]
    async fn test_find_portal_group_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"
portal-group pg0 {{
    listen = "0.0.0.0:3260"
}}
        "#).unwrap();

        let result = validate_portal_group_exists(file.path(), "pg0").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_portal_group_not_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"
portal-group pg0 {{
    listen = "0.0.0.0:3260"
}}
        "#).unwrap();

        let result = validate_portal_group_exists(file.path(), "pg1").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_find_transport_group_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"
transport-group tg0 {{
    listen {{
        tcp = "0.0.0.0:4420"
    }}
}}
        "#).unwrap();

        let result = validate_transport_group_exists(file.path(), "tg0").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_transport_group_not_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"
transport-group tg0 {{
    listen {{
        tcp = "0.0.0.0:4420"
    }}
}}
        "#).unwrap();

        let result = validate_transport_group_exists(file.path(), "tg1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_missing_config_file() {
        let result = validate_portal_group_exists("/nonexistent/path", "pg0").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found") ||
                result.unwrap_err().to_string().contains("No such file"));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd ctld-agent && cargo test ctl::config_validator::tests --no-run 2>&1 | head -20`
Expected: Compilation error - module config_validator not found

**Step 3: Write minimal implementation**

```rust
// ctld-agent/src/ctl/config_validator.rs
//! Configuration validation for portal and transport groups.
//!
//! Validates that portal-group (iSCSI) and transport-group (NVMeoF)
//! references in agent arguments actually exist in /etc/ctl.conf.

use std::path::Path;
use regex::Regex;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Config file not found: {0}")]
    FileNotFound(String),
    #[error("I/O error reading config: {0}")]
    Io(#[from] std::io::Error),
    #[error("portal-group '{0}' not found in {1}")]
    PortalGroupNotFound(String, String),
    #[error("transport-group '{0}' not found in {1}")]
    TransportGroupNotFound(String, String),
}

/// Validate that a portal-group with the given name exists in the config file.
pub async fn validate_portal_group_exists(
    config_path: impl AsRef<Path>,
    group_name: &str,
) -> Result<(), ValidationError> {
    let path = config_path.as_ref();

    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Err(ValidationError::FileNotFound(path.display().to_string()));
    }

    let content = tokio::fs::read_to_string(path).await?;

    // Match portal-group declarations: portal-group name { or portal-group "name" {
    let pattern = format!(
        r#"portal-group\s+(?:"{0}"|\b{0}\b)\s*\{{"#,
        regex::escape(group_name)
    );
    let re = Regex::new(&pattern).expect("Invalid regex pattern");

    if re.is_match(&content) {
        Ok(())
    } else {
        Err(ValidationError::PortalGroupNotFound(
            group_name.to_string(),
            path.display().to_string(),
        ))
    }
}

/// Validate that a transport-group with the given name exists in the config file.
pub async fn validate_transport_group_exists(
    config_path: impl AsRef<Path>,
    group_name: &str,
) -> Result<(), ValidationError> {
    let path = config_path.as_ref();

    if !tokio::fs::try_exists(path).await.unwrap_or(false) {
        return Err(ValidationError::FileNotFound(path.display().to_string()));
    }

    let content = tokio::fs::read_to_string(path).await?;

    // Match transport-group declarations: transport-group name { or transport-group "name" {
    let pattern = format!(
        r#"transport-group\s+(?:"{0}"|\b{0}\b)\s*\{{"#,
        regex::escape(group_name)
    );
    let re = Regex::new(&pattern).expect("Invalid regex pattern");

    if re.is_match(&content) {
        Ok(())
    } else {
        Err(ValidationError::TransportGroupNotFound(
            group_name.to_string(),
            path.display().to_string(),
        ))
    }
}
```

**Step 4: Add regex to Cargo.toml if not present**

Run: `grep -q "^regex" ctld-agent/Cargo.toml || echo 'regex = "1"' >> ctld-agent/Cargo.toml`

**Step 5: Add module to ctl/mod.rs**

```rust
// Add to ctld-agent/src/ctl/mod.rs
mod config_validator;
pub use config_validator::{validate_portal_group_exists, validate_transport_group_exists, ValidationError};
```

**Step 6: Run test to verify it passes**

Run: `cd ctld-agent && cargo test ctl::config_validator::tests -v`
Expected: All 5 tests pass

**Step 7: Commit**

```bash
git add ctld-agent/src/ctl/config_validator.rs ctld-agent/src/ctl/mod.rs ctld-agent/Cargo.toml
git commit -m "feat(ctl): add portal/transport group validation"
```

---

### Task 3.3: Integrate startup validation in main.rs

**Files:**
- Modify: `ctld-agent/src/main.rs`

**Step 1: Add validation calls after parsing arguments**

Add after the logging setup in main():

```rust
// Validate portal group exists if specified
if !args.portal_group.is_empty() {
    ctld_agent::ctl::validate_portal_group_exists(&args.ctl_config, &args.portal_group)
        .await
        .map_err(|e| format!("Startup validation failed: {}", e))?;
    info!("Validated portal-group '{}' exists in config", args.portal_group);
}

// Validate transport group exists if specified
if !args.transport_group_name.is_empty() {
    ctld_agent::ctl::validate_transport_group_exists(&args.ctl_config, &args.transport_group_name)
        .await
        .map_err(|e| format!("Startup validation failed: {}", e))?;
    info!("Validated transport-group '{}' exists in config", args.transport_group_name);
}
```

**Step 2: Run to verify it compiles**

Run: `cd ctld-agent && cargo build`
Expected: Build succeeds

**Step 3: Commit**

```bash
git add ctld-agent/src/main.rs
git commit -m "feat(startup): add portal/transport group validation on startup"
```

---

## Phase 4: Unified Config Writer with Auth Support

Integrate auth.json with the config writer.

### Task 4.1: Create unified ConfigManager

**Files:**
- Create: `ctld-agent/src/ctl/config_manager.rs`
- Modify: `ctld-agent/src/ctl/mod.rs`

**Step 1: Write the failing test**

```rust
// In ctld-agent/src/ctl/config_manager.rs at the bottom

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
```

**Step 2: Write minimal implementation**

```rust
// ctld-agent/src/ctl/config_manager.rs
//! Unified configuration manager for auth.json and csi-targets.conf.
//!
//! Provides a single point of control for all CSI config file operations,
//! ensuring atomic writes and consistent state.

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::auth::{AuthDb, ChapCredentials, load_auth_db, write_auth_db, AuthError};
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
        self.auth_db.write().await.insert(volume_name.to_string(), creds);
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
```

**Step 3: Add module to ctl/mod.rs**

```rust
// Add to ctld-agent/src/ctl/mod.rs
mod config_manager;
pub use config_manager::{ConfigManager, ConfigManagerError};
```

**Step 4: Run test to verify it passes**

Run: `cd ctld-agent && cargo test ctl::config_manager::tests -v`
Expected: All 2 tests pass

**Step 5: Commit**

```bash
git add ctld-agent/src/ctl/config_manager.rs ctld-agent/src/ctl/mod.rs
git commit -m "feat(ctl): add unified ConfigManager for auth and config"
```

---

## Phase 5: Idempotent CreateVolume

Add validate-first and recover-on-retry logic.

### Task 5.1: Add CHAP validation helper

**Files:**
- Modify: `ctld-agent/src/ctl/ucl_config.rs`

**Step 1: Add test for CHAP validation**

```rust
// Add to existing tests in ucl_config.rs

#[test]
fn test_validate_chap_credentials_valid() {
    let result = validate_chap_credentials("user", "secret123");
    assert!(result.is_ok());
}

#[test]
fn test_validate_chap_credentials_empty_user() {
    let result = validate_chap_credentials("", "secret");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("empty"));
}

#[test]
fn test_validate_chap_credentials_forbidden_chars() {
    // Double quote
    assert!(validate_chap_credentials("user\"name", "secret").is_err());
    // Curly braces
    assert!(validate_chap_credentials("user", "sec{ret}").is_err());
    // Backslash
    assert!(validate_chap_credentials("user", "sec\\ret").is_err());
}

#[test]
fn test_validate_chap_credentials_special_chars_allowed() {
    // These should be allowed
    assert!(validate_chap_credentials("user@domain.com", "p@ss!w0rd#$%").is_ok());
    assert!(validate_chap_credentials("user", "secret:with:colons").is_ok());
}
```

**Step 2: Add validation function**

```rust
// Add to ctld-agent/src/ctl/ucl_config.rs after validate_ucl_string

/// Validate CHAP credentials for safe UCL output.
///
/// Returns Ok if both username and secret are valid for UCL.
/// Returns Err with descriptive message if validation fails.
pub fn validate_chap_credentials(username: &str, secret: &str) -> Result<()> {
    validate_ucl_string(username, "CHAP username")?;
    validate_ucl_string(secret, "CHAP secret")?;
    Ok(())
}
```

**Step 3: Export from ctl/mod.rs**

```rust
// Add to pub use ucl_config line
pub use ucl_config::{..., validate_chap_credentials};
```

**Step 4: Run test to verify it passes**

Run: `cd ctld-agent && cargo test ucl_config::tests::test_validate_chap -v`
Expected: All 4 tests pass

**Step 5: Commit**

```bash
git add ctld-agent/src/ctl/ucl_config.rs ctld-agent/src/ctl/mod.rs
git commit -m "feat(ctl): add validate_chap_credentials helper"
```

---

### Task 5.2: Add recovery logic for existing volumes

**Files:**
- Modify: `ctld-agent/src/service/storage.rs`

This is a larger change. The key insight is that in `create_volume`, we need to:

1. Validate inputs BEFORE creating ZFS volume
2. If ZFS returns "already exists", compare parameters
3. If parameters match, return success (idempotent)
4. If parameters mismatch, return ALREADY_EXISTS error

**Step 1: Add validation at start of create_volume**

Find the `create_volume` method in storage.rs and add early validation:

```rust
// At the start of create_volume, before any ZFS operations:

// Phase 1: Validate all inputs (no state changes)
let auth_config = proto_to_ctl_auth(req.auth.as_ref());

// Validate CHAP credentials if present
if let AuthConfig::IscsiChap(ref chap) = auth_config {
    crate::ctl::validate_chap_credentials(&chap.username, &chap.secret)
        .map_err(|e| {
            timer.failure("validation_error");
            Status::invalid_argument(format!("Invalid CHAP credentials: {}", e))
        })?;

    if chap.has_mutual() {
        if let (Some(ref mu), Some(ref ms)) = (&chap.mutual_username, &chap.mutual_secret) {
            crate::ctl::validate_chap_credentials(mu, ms)
                .map_err(|e| {
                    timer.failure("validation_error");
                    Status::invalid_argument(format!("Invalid mutual CHAP credentials: {}", e))
                })?;
        }
    }
}
```

**Step 2: Add recovery logic for "already exists" error**

In the ZFS volume creation section, wrap the error handling:

```rust
// When creating ZFS volume, handle "already exists" specially:
match zfs.create_zvol(&name, size_bytes, sparse).await {
    Ok(()) => { /* continue */ }
    Err(e) if e.to_string().contains("dataset already exists") => {
        // Recovery: check if existing volume matches requested parameters
        info!(volume = %name, "Volume already exists, checking parameters for idempotency");

        match self.check_existing_volume_params(&name, size_bytes, export_type_ctl, &timer).await {
            Ok(volume_response) => {
                // Parameters match - return existing volume (idempotent)
                info!(volume = %name, "Existing volume matches requested parameters");
                return Ok(Response::new(volume_response));
            }
            Err(status) => {
                // Parameters mismatch - return error
                return Err(status);
            }
        }
    }
    Err(e) => {
        timer.failure("zfs_create_error");
        return Err(Status::internal(format!("Failed to create ZFS volume: {}", e)));
    }
}
```

**Step 3: Add helper method for checking existing volume parameters**

```rust
// Add to StorageService impl block

/// Check if an existing volume's parameters match a create request.
/// Returns Ok(CreateVolumeResponse) if they match (for idempotent success).
/// Returns Err(Status) if they don't match.
async fn check_existing_volume_params(
    &self,
    name: &str,
    requested_size: u64,
    requested_export_type: Option<CtlExportType>,
    timer: &OperationTimer,
) -> Result<CreateVolumeResponse, Status> {
    let zfs = self.zfs.read().await;

    // Get existing volume metadata
    let metadata = zfs.get_volume_metadata(name).await
        .map_err(|e| {
            timer.failure("zfs_metadata_error");
            Status::internal(format!("Failed to read existing volume metadata: {}", e))
        })?
        .ok_or_else(|| {
            timer.failure("metadata_not_found");
            Status::internal("Volume exists but metadata not found")
        })?;

    // Check size matches (allow existing to be >= requested)
    let existing_size = zfs.get_zvol_size(name).await
        .map_err(|e| {
            timer.failure("zfs_size_error");
            Status::internal(format!("Failed to get existing volume size: {}", e))
        })?;

    if existing_size < requested_size {
        timer.failure("size_mismatch");
        return Err(Status::already_exists(format!(
            "Volume '{}' exists with size {} but {} was requested",
            name, existing_size, requested_size
        )));
    }

    // Check export type matches if specified
    if let Some(req_type) = requested_export_type {
        let existing_type = metadata.export_type.ok_or_else(|| {
            timer.failure("export_type_missing");
            Status::internal("Existing volume has no export type")
        })?;

        if existing_type != req_type {
            timer.failure("export_type_mismatch");
            return Err(Status::already_exists(format!(
                "Volume '{}' exists with export type {:?} but {:?} was requested",
                name, existing_type, req_type
            )));
        }
    }

    // Parameters match - build response
    timer.success();
    Ok(CreateVolumeResponse {
        volume: Some(Volume {
            id: name.to_string(),
            name: name.to_string(),
            capacity_bytes: existing_size as i64,
            export_type: metadata.export_type.map(ctl_to_proto_export_type).unwrap_or(ExportType::Unspecified) as i32,
            target_name: metadata.target_name.unwrap_or_default(),
            lun_id: metadata.lun_id.unwrap_or(0) as i32,
            parameters: HashMap::new(),
        }),
    })
}
```

**Step 4: Run tests**

Run: `cd ctld-agent && cargo test`
Expected: All tests pass

**Step 5: Commit**

```bash
git add ctld-agent/src/service/storage.rs
git commit -m "feat(storage): add validate-first and recover-on-retry for CreateVolume"
```

---

## Phase 6: csi-driver iSCSI Direct Node Creation

Replace discovery with direct node creation.

### Task 6.1: Replace iscsiadm discovery with direct node creation

**Files:**
- Modify: `csi-driver/src/platform/linux.rs`

**Step 1: Find and replace the discovery code**

Locate the section that runs `iscsiadm -m discovery -t sendtargets` and replace with:

```rust
// OLD (remove this):
// let output = Command::new("iscsiadm")
//     .args(["-m", "discovery", "-t", "sendtargets", "-p", &portal])
//     .output()
//     .await?;

// NEW (add this):
// Create node entry directly without discovery
// This is more robust as it doesn't depend on discovery auth settings
let output = Command::new("iscsiadm")
    .args(["-m", "node", "-T", &target_iqn, "-p", &portal, "--op", "new"])
    .output()
    .await
    .map_err(|e| {
        error!(error = %e, portal = %portal, "Failed to execute iscsiadm node create");
        Status::internal(format!("Failed to create iSCSI node: {}", e))
    })?;

if !output.status.success() {
    let stderr = String::from_utf8_lossy(&output.stderr);
    // "already exists" is fine - node was created previously
    if !stderr.contains("already exists") {
        warn!(
            stderr = %stderr,
            portal = %portal,
            target = %target_iqn,
            "iscsiadm node create returned error"
        );
    }
}

info!(portal = %portal, target = %target_iqn, "iSCSI node entry created");
```

**Step 2: Verify the change doesn't break tests**

Run: `cd csi-driver && cargo test`
Expected: All tests pass (or update tests if they mock discovery)

**Step 3: Commit**

```bash
git add csi-driver/src/platform/linux.rs
git commit -m "feat(iscsi): replace discovery with direct node creation

This eliminates the dependency on discovery auth settings, making CSI
orthogonal to user's discovery-auth-group configuration."
```

---

## Phase 7: Documentation Updates

### Task 7.1: Update installation documentation

**Files:**
- Modify: `docs/installation.md` (or create if doesn't exist)

**Step 1: Add UCL mode and include directive documentation**

Create comprehensive installation guide covering:
- UCL mode requirement (`ctld_flags="-u"`)
- Include directive setup
- Directory creation and permissions
- Portal/transport group configuration
- Migration from old marker-based config

**Step 2: Commit**

```bash
git add docs/installation.md
git commit -m "docs: add comprehensive installation guide for robustness changes"
```

---

## Summary

This plan implements the robustness improvements in 7 phases:

1. **Auth Persistence Module** - ChapCredentials, AuthDb, file I/O
2. **Standalone Config Generation** - CsiConfigGenerator
3. **Portal/Transport Group Validation** - Startup checks
4. **Unified Config Writer** - ConfigManager
5. **Idempotent CreateVolume** - Validate-first, recover-on-retry
6. **iSCSI Direct Node Creation** - Skip discovery
7. **Documentation** - Installation guide updates

Each phase has granular tasks with TDD approach (failing test → implementation → passing test → commit).
