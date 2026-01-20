# UCL Config Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor ctld-agent to manage iSCSI/NVMeoF targets via UCL config files instead of ephemeral ctladm commands.

**Architecture:** Replace direct `ctladm` calls with UCL config file generation. Write CSI-managed targets to `/etc/ctl.ucl`, then reload ctld to apply changes. This ensures targets persist across reboots and coexist with manually-configured targets.

**Tech Stack:** Rust, libucl crate (0.2.3), FreeBSD ctld with UCL format (-u flag)

---

## Background

### Current State (Broken)
- `ctladm create` commands create ephemeral LUNs in kernel
- Lost on reboot
- `reload_ctld()` exists but is useless since we don't write config

### Target State
- Write targets to `/etc/ctl.ucl`
- Reload ctld to apply changes
- Targets persist and coexist with user-managed targets

### UCL Format for iSCSI
```ucl
target "iqn.2024-01.org.freebsd.csi:vol-abc123" {
    auth-group = "ag0"
    portal-group = "pg0"
    lun 0 {
        path = "/dev/zvol/tank/csi/vol-abc123"
        blocksize = 512
    }
}
```

### User's Environment
- Config file: `/etc/ctl.ucl`
- Portal group: `pg0`
- Auth group: `ag0` for access, no-auth for discovery
- ctld started via `/etc/rc.local` with `-u` flag

---

## Task 1: Add libucl Dependency

**Files:**
- Modify: `ctld-agent/Cargo.toml`

**Step 1: Add libucl to dependencies**

```toml
[dependencies]
# ... existing deps ...
libucl = "0.2"
```

**Step 2: Verify build**

Run: `cd ctld-agent && cargo check`
Expected: Compiles successfully (may need cmake/clang on build system)

**Step 3: Commit**

```bash
git add ctld-agent/Cargo.toml
git commit -m "feat(ctld-agent): add libucl dependency for UCL config management"
```

---

## Task 2: Create UCL Config Module

**Files:**
- Create: `ctld-agent/src/ctl/ucl_config.rs`
- Modify: `ctld-agent/src/ctl/mod.rs`

**Step 1: Write the failing test**

In `ctld-agent/src/ctl/ucl_config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_iscsi_target_ucl() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![LunUcl {
                id: 0,
                path: "/dev/zvol/tank/csi/vol1".to_string(),
                blocksize: 512,
            }],
        };

        let ucl = target.to_ucl_string();
        assert!(ucl.contains("iqn.2024-01.org.freebsd.csi:vol1"));
        assert!(ucl.contains("auth-group"));
        assert!(ucl.contains("ag0"));
        assert!(ucl.contains("portal-group"));
        assert!(ucl.contains("pg0"));
        assert!(ucl.contains("/dev/zvol/tank/csi/vol1"));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cd ctld-agent && cargo test ucl_config`
Expected: FAIL - module doesn't exist

**Step 3: Write minimal implementation**

Create `ctld-agent/src/ctl/ucl_config.rs`:

```rust
//! UCL configuration file management for ctld.
//!
//! This module handles reading and writing ctld UCL configuration files,
//! allowing CSI-managed targets to coexist with user-managed targets.

use std::fmt::Write as FmtWrite;
use std::fs;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use super::error::{CtlError, Result};

/// Default config file path
pub const DEFAULT_CONFIG_PATH: &str = "/etc/ctl.ucl";

/// Marker comment for CSI-managed section start
const CSI_SECTION_START: &str = "# BEGIN CSI-MANAGED TARGETS - DO NOT EDIT";
/// Marker comment for CSI-managed section end
const CSI_SECTION_END: &str = "# END CSI-MANAGED TARGETS";

/// Represents a LUN in UCL format
#[derive(Debug, Clone)]
pub struct LunUcl {
    pub id: u32,
    pub path: String,
    pub blocksize: u32,
}

/// Represents an iSCSI target in UCL format
#[derive(Debug, Clone)]
pub struct IscsiTargetUcl {
    pub iqn: String,
    pub auth_group: String,
    pub portal_group: String,
    pub luns: Vec<LunUcl>,
}

impl IscsiTargetUcl {
    /// Generate UCL string representation of this target
    pub fn to_ucl_string(&self) -> String {
        let mut s = String::new();
        writeln!(s, "target \"{}\" {{", self.iqn).unwrap();
        writeln!(s, "    auth-group = \"{}\"", self.auth_group).unwrap();
        writeln!(s, "    portal-group = \"{}\"", self.portal_group).unwrap();
        for lun in &self.luns {
            writeln!(s, "    lun {} {{", lun.id).unwrap();
            writeln!(s, "        path = \"{}\"", lun.path).unwrap();
            writeln!(s, "        blocksize = {}", lun.blocksize).unwrap();
            writeln!(s, "    }}").unwrap();
        }
        writeln!(s, "}}").unwrap();
        s
    }
}

/// Represents an NVMeoF subsystem in UCL format (if ctld supports it)
#[derive(Debug, Clone)]
pub struct NvmeSubsystemUcl {
    pub nqn: String,
    pub namespaces: Vec<NvmeNamespaceUcl>,
}

/// Represents an NVMe namespace
#[derive(Debug, Clone)]
pub struct NvmeNamespaceUcl {
    pub id: u32,
    pub path: String,
}

impl NvmeSubsystemUcl {
    /// Generate UCL string representation of this subsystem
    /// Note: ctld may not support NVMeoF via config - verify with FreeBSD docs
    pub fn to_ucl_string(&self) -> String {
        let mut s = String::new();
        writeln!(s, "# NVMeoF subsystem (may require ctladm for now)").unwrap();
        writeln!(s, "# nqn: {}", self.nqn).unwrap();
        for ns in &self.namespaces {
            writeln!(s, "# namespace {}: {}", ns.id, ns.path).unwrap();
        }
        s
    }
}

/// Manager for UCL configuration files
pub struct UclConfigManager {
    config_path: String,
    auth_group: String,
    portal_group: String,
}

impl UclConfigManager {
    /// Create a new UclConfigManager
    pub fn new(config_path: String, auth_group: String, portal_group: String) -> Self {
        Self {
            config_path,
            auth_group,
            portal_group,
        }
    }

    /// Read the current config file, extracting non-CSI content
    pub fn read_user_config(&self) -> Result<String> {
        let path = Path::new(&self.config_path);
        if !path.exists() {
            return Ok(String::new());
        }

        let file = fs::File::open(path).map_err(|e| {
            CtlError::CommandFailed(format!("Failed to open {}: {}", self.config_path, e))
        })?;

        let reader = BufReader::new(file);
        let mut user_content = String::new();
        let mut in_csi_section = false;

        for line in reader.lines() {
            let line = line.map_err(|e| {
                CtlError::CommandFailed(format!("Failed to read {}: {}", self.config_path, e))
            })?;

            if line.trim() == CSI_SECTION_START {
                in_csi_section = true;
                continue;
            }
            if line.trim() == CSI_SECTION_END {
                in_csi_section = false;
                continue;
            }

            if !in_csi_section {
                user_content.push_str(&line);
                user_content.push('\n');
            }
        }

        Ok(user_content)
    }

    /// Write the config file with user content + CSI-managed targets
    pub fn write_config(&self, user_content: &str, targets: &[IscsiTargetUcl]) -> Result<()> {
        let mut content = user_content.to_string();

        // Ensure newline before CSI section
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }

        // Add CSI-managed section
        content.push_str(CSI_SECTION_START);
        content.push('\n');

        for target in targets {
            content.push_str(&target.to_ucl_string());
            content.push('\n');
        }

        content.push_str(CSI_SECTION_END);
        content.push('\n');

        // Write atomically via temp file
        let temp_path = format!("{}.tmp", self.config_path);
        fs::write(&temp_path, &content).map_err(|e| {
            CtlError::CommandFailed(format!("Failed to write {}: {}", temp_path, e))
        })?;

        fs::rename(&temp_path, &self.config_path).map_err(|e| {
            CtlError::CommandFailed(format!(
                "Failed to rename {} to {}: {}",
                temp_path, self.config_path, e
            ))
        })?;

        Ok(())
    }

    /// Create an IscsiTargetUcl with the manager's default auth/portal groups
    pub fn create_target(&self, iqn: &str, device_path: &str, lun_id: u32) -> IscsiTargetUcl {
        IscsiTargetUcl {
            iqn: iqn.to_string(),
            auth_group: self.auth_group.clone(),
            portal_group: self.portal_group.clone(),
            luns: vec![LunUcl {
                id: lun_id,
                path: device_path.to_string(),
                blocksize: 512,
            }],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_iscsi_target_ucl() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![LunUcl {
                id: 0,
                path: "/dev/zvol/tank/csi/vol1".to_string(),
                blocksize: 512,
            }],
        };

        let ucl = target.to_ucl_string();
        assert!(ucl.contains("iqn.2024-01.org.freebsd.csi:vol1"));
        assert!(ucl.contains("auth-group"));
        assert!(ucl.contains("ag0"));
        assert!(ucl.contains("portal-group"));
        assert!(ucl.contains("pg0"));
        assert!(ucl.contains("/dev/zvol/tank/csi/vol1"));
    }

    #[test]
    fn test_generate_multi_lun_target() {
        let target = IscsiTargetUcl {
            iqn: "iqn.2024-01.org.freebsd.csi:vol2".to_string(),
            auth_group: "ag0".to_string(),
            portal_group: "pg0".to_string(),
            luns: vec![
                LunUcl {
                    id: 0,
                    path: "/dev/zvol/tank/csi/vol2-data".to_string(),
                    blocksize: 512,
                },
                LunUcl {
                    id: 1,
                    path: "/dev/zvol/tank/csi/vol2-log".to_string(),
                    blocksize: 4096,
                },
            ],
        };

        let ucl = target.to_ucl_string();
        assert!(ucl.contains("lun 0"));
        assert!(ucl.contains("lun 1"));
        assert!(ucl.contains("vol2-data"));
        assert!(ucl.contains("vol2-log"));
        assert!(ucl.contains("blocksize = 4096"));
    }

    #[test]
    fn test_ucl_config_manager_create_target() {
        let manager = UclConfigManager::new(
            "/etc/ctl.ucl".to_string(),
            "ag0".to_string(),
            "pg0".to_string(),
        );

        let target = manager.create_target(
            "iqn.2024-01.org.freebsd.csi:test",
            "/dev/zvol/tank/test",
            0,
        );

        assert_eq!(target.iqn, "iqn.2024-01.org.freebsd.csi:test");
        assert_eq!(target.auth_group, "ag0");
        assert_eq!(target.portal_group, "pg0");
        assert_eq!(target.luns.len(), 1);
        assert_eq!(target.luns[0].path, "/dev/zvol/tank/test");
    }
}
```

**Step 4: Update mod.rs**

Add to `ctld-agent/src/ctl/mod.rs`:

```rust
pub mod ucl_config;

pub use ucl_config::{UclConfigManager, IscsiTargetUcl, LunUcl};
```

**Step 5: Run tests to verify pass**

Run: `cd ctld-agent && cargo test ucl_config`
Expected: All tests PASS

**Step 6: Commit**

```bash
git add ctld-agent/src/ctl/ucl_config.rs ctld-agent/src/ctl/mod.rs
git commit -m "feat(ctld-agent): add UCL config management module"
```

---

## Task 3: Add CLI Arguments for UCL Config

**Files:**
- Modify: `ctld-agent/src/main.rs`

**Step 1: Read current main.rs**

First understand the existing CLI structure.

**Step 2: Add UCL-related CLI arguments**

Add to Args struct:

```rust
/// Path to ctld UCL config file
#[arg(long, env = "CTL_CONFIG_PATH", default_value = "/etc/ctl.ucl")]
ctl_config: PathBuf,

/// Auth group name for iSCSI targets
#[arg(long, env = "CTL_AUTH_GROUP", default_value = "ag0")]
auth_group: String,

/// Portal group name for iSCSI targets
#[arg(long, env = "CTL_PORTAL_GROUP", default_value = "pg0")]
portal_group: String,
```

**Step 3: Build and verify**

Run: `cd ctld-agent && cargo build`
Expected: Compiles successfully

**Step 4: Verify help output**

Run: `cd ctld-agent && cargo run -- --help`
Expected: Shows new --ctl-config, --auth-group, --portal-group options

**Step 5: Commit**

```bash
git add ctld-agent/src/main.rs
git commit -m "feat(ctld-agent): add CLI args for UCL config path and auth/portal groups"
```

---

## Task 4: Refactor IscsiManager to Use UCL Config

**Files:**
- Modify: `ctld-agent/src/ctl/iscsi.rs`

**Step 1: Write failing test**

Add to existing tests in `iscsi.rs`:

```rust
#[test]
fn test_iscsi_manager_generates_ucl() {
    let pg = PortalGroup::new(0, "pg0".to_string());
    let manager = IscsiManager::new_with_ucl(
        "iqn.2024-01.org.freebsd.csi".to_string(),
        pg,
        "/tmp/test-ctl.ucl".to_string(),
        "ag0".to_string(),
    ).unwrap();

    // Verify the manager can generate UCL for a target
    let ucl = manager.generate_target_ucl("vol1", "/dev/zvol/tank/csi/vol1", 0);
    assert!(ucl.contains("iqn.2024-01.org.freebsd.csi:vol1"));
    assert!(ucl.contains("ag0"));
    assert!(ucl.contains("pg0"));
}
```

**Step 2: Run test to verify failure**

Run: `cd ctld-agent && cargo test test_iscsi_manager_generates_ucl`
Expected: FAIL - method doesn't exist

**Step 3: Implement UCL integration in IscsiManager**

Modify `IscsiManager` struct to include:

```rust
use super::ucl_config::{UclConfigManager, IscsiTargetUcl, LunUcl};

pub struct IscsiManager {
    base_iqn: String,
    portal_group: PortalGroup,
    targets: RwLock<HashMap<String, IscsiTarget>>,
    /// UCL config manager for persistent configuration
    ucl_manager: UclConfigManager,
}

impl IscsiManager {
    /// Create a new IscsiManager with UCL config support
    pub fn new_with_ucl(
        base_iqn: String,
        portal_group: PortalGroup,
        config_path: String,
        auth_group: String,
    ) -> Result<Self> {
        validate_name(&base_iqn)?;

        let ucl_manager = UclConfigManager::new(
            config_path,
            auth_group,
            portal_group.name.clone(),
        );

        info!(
            "Initializing IscsiManager with base_iqn={}, portal_group={}",
            base_iqn, portal_group.name
        );

        Ok(Self {
            base_iqn,
            portal_group,
            targets: RwLock::new(HashMap::new()),
            ucl_manager,
        })
    }

    /// Generate UCL for a target (for testing/preview)
    pub fn generate_target_ucl(&self, volume_name: &str, device_path: &str, lun_id: u32) -> String {
        let iqn = IscsiTarget::generate_iqn(&self.base_iqn, volume_name);
        let target = self.ucl_manager.create_target(&iqn, device_path, lun_id);
        target.to_ucl_string()
    }
}
```

**Step 4: Refactor export_volume to write UCL config**

Replace `add_target_live()` call with UCL config update:

```rust
pub fn export_volume(
    &self,
    volume_name: &str,
    device_path: &str,
    lun_id: u32,
) -> Result<IscsiTarget> {
    validate_name(volume_name)?;
    validate_device_path(device_path)?;

    let iqn = IscsiTarget::generate_iqn(&self.base_iqn, volume_name);
    debug!("Exporting volume {} as iSCSI target {}", volume_name, iqn);

    // Check if target already exists
    {
        let targets = self.targets.read().unwrap();
        if targets.contains_key(volume_name) {
            return Err(CtlError::TargetExists(volume_name.to_string()));
        }
    }

    // Build target configuration
    let lun = Lun::new(lun_id, device_path.to_string());
    let target = IscsiTarget::new(volume_name.to_string(), iqn.clone())
        .with_portal_group(self.portal_group.tag)
        .with_lun(lun);

    // Store in cache first
    {
        let mut targets = self.targets.write().unwrap();
        targets.insert(volume_name.to_string(), target.clone());
    }

    // Write UCL config and reload ctld
    self.write_config_and_reload()?;

    info!("Successfully exported {} as iSCSI target", volume_name);
    Ok(target)
}
```

**Step 5: Implement write_config_and_reload**

```rust
/// Write all targets to UCL config and reload ctld
fn write_config_and_reload(&self) -> Result<()> {
    // Read user content (non-CSI targets)
    let user_content = self.ucl_manager.read_user_config()?;

    // Convert cached targets to UCL format
    let targets = self.targets.read().unwrap();
    let ucl_targets: Vec<IscsiTargetUcl> = targets
        .values()
        .map(|t| {
            IscsiTargetUcl {
                iqn: t.iqn.clone(),
                auth_group: self.ucl_manager.auth_group.clone(),
                portal_group: self.ucl_manager.portal_group.clone(),
                luns: t.luns.iter().map(|l| LunUcl {
                    id: l.id,
                    path: l.device_path.clone(),
                    blocksize: l.blocksize,
                }).collect(),
            }
        })
        .collect();
    drop(targets);

    // Write config
    self.ucl_manager.write_config(&user_content, &ucl_targets)?;

    // Reload ctld
    self.reload_ctld()?;

    Ok(())
}
```

**Step 6: Update unexport_volume similarly**

Remove from cache, write config, reload ctld.

**Step 7: Run all tests**

Run: `cd ctld-agent && cargo test`
Expected: All tests PASS

**Step 8: Commit**

```bash
git add ctld-agent/src/ctl/iscsi.rs
git commit -m "feat(ctld-agent): refactor IscsiManager to use UCL config instead of ctladm"
```

---

## Task 5: Update Service Initialization

**Files:**
- Modify: `ctld-agent/src/service/storage.rs` (or wherever StorageService initializes)
- Modify: `ctld-agent/src/main.rs`

**Step 1: Update StorageService to accept UCL config params**

Pass config_path, auth_group, portal_group through to IscsiManager.

**Step 2: Update main.rs to wire the args**

```rust
let iscsi_manager = IscsiManager::new_with_ucl(
    args.base_iqn,
    portal_group,
    args.ctl_config.to_string_lossy().to_string(),
    args.auth_group,
)?;
```

**Step 3: Build and verify**

Run: `cd ctld-agent && cargo build`
Expected: Compiles

**Step 4: Commit**

```bash
git add ctld-agent/src/service/storage.rs ctld-agent/src/main.rs
git commit -m "feat(ctld-agent): wire UCL config args through to IscsiManager"
```

---

## Task 6: Add Startup Recovery from UCL Config

**Files:**
- Modify: `ctld-agent/src/ctl/iscsi.rs`

**Step 1: Implement load_config using libucl**

```rust
use libucl::Parser;

/// Load existing configuration from ctld UCL file
#[instrument(skip(self))]
pub fn load_config(&mut self) -> Result<()> {
    let config_path = &self.ucl_manager.config_path;
    let path = std::path::Path::new(config_path);

    if !path.exists() {
        debug!("Config file {} does not exist, starting fresh", config_path);
        return Ok(());
    }

    let content = std::fs::read_to_string(path).map_err(|e| {
        CtlError::CommandFailed(format!("Failed to read {}: {}", config_path, e))
    })?;

    let parser = Parser::new();
    let doc = parser.parse(&content).map_err(|e| {
        CtlError::ParseError(format!("Failed to parse {}: {:?}", config_path, e))
    })?;

    // Extract targets from parsed UCL
    // Look for target blocks with our base IQN prefix
    let mut targets = self.targets.write().unwrap();

    // Parse targets from UCL document
    // This requires iterating over the UCL object tree
    // Implementation depends on libucl API

    info!("Loaded {} existing targets from config", targets.len());
    Ok(())
}
```

**Step 2: Call load_config on startup**

In main.rs or service init, call `iscsi_manager.load_config()`.

**Step 3: Test startup recovery**

Manual test: Create a target, restart ctld-agent, verify target is recovered.

**Step 4: Commit**

```bash
git add ctld-agent/src/ctl/iscsi.rs ctld-agent/src/main.rs
git commit -m "feat(ctld-agent): add startup recovery from UCL config"
```

---

## Task 7: Handle NVMeoF (Investigation Required)

**Files:**
- Modify: `ctld-agent/src/ctl/nvmeof.rs`

**Note:** ctld's UCL format may not support NVMeoF configuration. Need to verify with FreeBSD documentation.

**Step 1: Research ctld NVMeoF support**

Check `man ctl.conf` and `man ctld` for NVMeoF configuration options.

**Step 2: If supported, implement similar to iSCSI**

Follow same pattern as Task 4.

**Step 3: If not supported, keep ctladm for NVMeoF**

Document limitation: NVMeoF targets created via ctladm are ephemeral. Consider using ZFS metadata for recovery on restart.

**Step 4: Commit**

```bash
git add ctld-agent/src/ctl/nvmeof.rs
git commit -m "feat(ctld-agent): document NVMeoF config approach"
```

---

## Task 8: Update Documentation

**Files:**
- Modify: `docs/configuration.md`

**Step 1: Add UCL configuration section**

Document:
- Required ctld setup (UCL format, -u flag)
- Portal group and auth group configuration
- CSI-managed section markers
- How targets are persisted

**Step 2: Add troubleshooting section**

- How to manually inspect `/etc/ctl.ucl`
- How to recover from config issues
- ctld reload vs restart

**Step 3: Commit**

```bash
git add docs/configuration.md
git commit -m "docs: add UCL configuration documentation"
```

---

## Verification Checklist

### Unit Tests
```bash
cd ctld-agent && cargo test
```

### Integration Test (Manual)
1. Start ctld-agent with UCL config
2. Create a volume via gRPC
3. Check `/etc/ctl.ucl` contains the target
4. Restart ctld-agent
5. Verify target is recovered
6. Delete volume
7. Check target removed from config

### Build Verification
```bash
cargo build --workspace
```

---

## Implementation Notes

### UCL Config Strategy

We use a **marked section** approach:
1. Read existing config, preserve everything outside CSI markers
2. Generate CSI targets within marked section
3. Write combined config atomically
4. Reload ctld

This allows:
- User-managed targets to coexist
- Clear visibility of CSI-managed targets
- Safe atomic updates

### Backward Compatibility

The old `IscsiManager::new()` constructor is preserved for tests. Production code should use `new_with_ucl()`.

### Error Handling

- Config write failures are fatal (rollback cache changes)
- ctld reload failures are logged but non-fatal (config is still persisted)
- Parse errors on startup are logged, proceed with empty state
