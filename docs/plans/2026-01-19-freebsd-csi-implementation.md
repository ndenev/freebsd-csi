# FreeBSD CSI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a complete CSI (Container Storage Interface) solution for Kubernetes that provisions ZFS volumes on FreeBSD and exports them via iSCSI and/or NVMeoF using the CTL subsystem.

**Architecture:** Two-component system: (1) A Rust daemon (`ctld-agent`) running on FreeBSD nodes that manages ZFS datasets and CTL exports, and (2) A Kubernetes CSI driver that receives volume requests from the cluster and orchestrates the daemon. Communication between components uses gRPC with mTLS.

**Tech Stack:** Rust (daemon + CSI driver), gRPC/protobuf, ZFS, FreeBSD CTL, Kubernetes CSI spec v1.9+

---

## Phase 1: Project Foundation

### Task 1: Initialize Rust Workspace

**Files:**
- Create: `Cargo.toml` (workspace root)
- Create: `ctld-agent/Cargo.toml`
- Create: `ctld-agent/src/main.rs`
- Create: `proto/ctld_agent.proto`

**Step 1: Create workspace Cargo.toml**

```toml
[workspace]
resolver = "2"
members = [
    "ctld-agent",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "BSD-2-Clause"
repository = "https://github.com/ndenev/freebsd-csi"

[workspace.dependencies]
tokio = { version = "1.43", features = ["full"] }
tonic = "0.12"
prost = "0.13"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
clap = { version = "4.5", features = ["derive"] }
```

**Step 2: Create ctld-agent/Cargo.toml**

```toml
[package]
name = "ctld-agent"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
tokio.workspace = true
tonic.workspace = true
prost.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
clap.workspace = true

[build-dependencies]
tonic-build = "0.12"
```

**Step 3: Create minimal main.rs**

```rust
use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "ctld-agent")]
#[command(about = "FreeBSD ZFS/CTL storage agent for Kubernetes CSI")]
struct Args {
    /// gRPC listen address
    #[arg(long, default_value = "[::1]:50051")]
    listen: String,

    /// ZFS parent dataset for volumes
    #[arg(long)]
    zfs_parent: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!("Starting ctld-agent on {}", args.listen);
    info!("ZFS parent dataset: {}", args.zfs_parent);
    Ok(())
}
```

**Step 4: Verify it compiles**

Run: `cargo build -p ctld-agent`
Expected: Build succeeds with no errors

**Step 5: Commit**

```bash
git add Cargo.toml ctld-agent/ proto/
git commit -m "feat: initialize rust workspace and ctld-agent skeleton"
```

---

### Task 2: Define gRPC Protocol

**Files:**
- Create: `proto/ctld_agent.proto`
- Create: `ctld-agent/build.rs`

**Step 1: Create protobuf definition**

```protobuf
syntax = "proto3";

package ctld_agent.v1;

// Volume represents a ZFS dataset exported via iSCSI or NVMeoF
message Volume {
    string id = 1;
    string name = 2;
    int64 size_bytes = 3;
    string zfs_dataset = 4;
    string export_type = 5;  // "iscsi" or "nvmeof"
    string target_name = 6;  // iSCSI IQN or NVMeoF NQN
    int32 lun_id = 7;
    map<string, string> parameters = 8;
}

message CreateVolumeRequest {
    string name = 1;
    int64 size_bytes = 2;
    string export_type = 3;
    map<string, string> parameters = 4;
}

message CreateVolumeResponse {
    Volume volume = 1;
}

message DeleteVolumeRequest {
    string volume_id = 1;
}

message DeleteVolumeResponse {}

message ExpandVolumeRequest {
    string volume_id = 1;
    int64 new_size_bytes = 2;
}

message ExpandVolumeResponse {
    int64 size_bytes = 1;
}

message ListVolumesRequest {
    int32 max_entries = 1;
    string starting_token = 2;
}

message ListVolumesResponse {
    repeated Volume volumes = 1;
    string next_token = 2;
}

message GetVolumeRequest {
    string volume_id = 1;
}

message GetVolumeResponse {
    Volume volume = 1;
}

// Snapshot operations
message Snapshot {
    string id = 1;
    string source_volume_id = 2;
    string name = 3;
    int64 creation_time = 4;
    int64 size_bytes = 5;
}

message CreateSnapshotRequest {
    string source_volume_id = 1;
    string name = 2;
}

message CreateSnapshotResponse {
    Snapshot snapshot = 1;
}

message DeleteSnapshotRequest {
    string snapshot_id = 1;
}

message DeleteSnapshotResponse {}

// The storage agent service
service StorageAgent {
    // Volume operations
    rpc CreateVolume(CreateVolumeRequest) returns (CreateVolumeResponse);
    rpc DeleteVolume(DeleteVolumeRequest) returns (DeleteVolumeResponse);
    rpc ExpandVolume(ExpandVolumeRequest) returns (ExpandVolumeResponse);
    rpc ListVolumes(ListVolumesRequest) returns (ListVolumesResponse);
    rpc GetVolume(GetVolumeRequest) returns (GetVolumeResponse);

    // Snapshot operations
    rpc CreateSnapshot(CreateSnapshotRequest) returns (CreateSnapshotResponse);
    rpc DeleteSnapshot(DeleteSnapshotRequest) returns (DeleteSnapshotResponse);
}
```

**Step 2: Create build.rs for protobuf compilation**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/ctld_agent.proto")?;
    Ok(())
}
```

**Step 3: Verify proto compilation**

Run: `cargo build -p ctld-agent`
Expected: Build succeeds, generates Rust types from proto

**Step 4: Commit**

```bash
git add proto/ ctld-agent/build.rs
git commit -m "feat: add gRPC protocol definition for storage agent"
```

---

## Phase 2: ZFS Operations Module

### Task 3: ZFS Command Interface

**Files:**
- Create: `ctld-agent/src/zfs/mod.rs`
- Create: `ctld-agent/src/zfs/dataset.rs`
- Create: `ctld-agent/src/zfs/error.rs`
- Modify: `ctld-agent/src/main.rs`

**Step 1: Create ZFS error types**

```rust
// ctld-agent/src/zfs/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZfsError {
    #[error("dataset '{0}' not found")]
    DatasetNotFound(String),

    #[error("dataset '{0}' already exists")]
    DatasetExists(String),

    #[error("invalid dataset name: {0}")]
    InvalidName(String),

    #[error("zfs command failed: {0}")]
    CommandFailed(String),

    #[error("failed to parse zfs output: {0}")]
    ParseError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ZfsError>;
```

**Step 2: Create dataset operations**

```rust
// ctld-agent/src/zfs/dataset.rs
use super::error::{Result, ZfsError};
use std::process::Command;
use tracing::{debug, instrument};

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub used: u64,
    pub available: u64,
    pub referenced: u64,
    pub mountpoint: Option<String>,
}

#[derive(Debug)]
pub struct ZfsManager {
    parent_dataset: String,
}

impl ZfsManager {
    pub fn new(parent_dataset: String) -> Result<Self> {
        // Verify parent dataset exists
        let output = Command::new("zfs")
            .args(["list", "-H", "-o", "name", &parent_dataset])
            .output()?;

        if !output.status.success() {
            return Err(ZfsError::DatasetNotFound(parent_dataset));
        }

        Ok(Self { parent_dataset })
    }

    #[instrument(skip(self))]
    pub fn create_volume(&self, name: &str, size_bytes: u64) -> Result<Dataset> {
        let full_name = format!("{}/{}", self.parent_dataset, name);

        // Create zvol with specified size
        let output = Command::new("zfs")
            .args([
                "create",
                "-V", &format!("{}B", size_bytes),
                "-o", "volmode=dev",
                &full_name,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("dataset already exists") {
                return Err(ZfsError::DatasetExists(full_name));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        debug!("Created zvol: {}", full_name);
        self.get_dataset(&full_name)
    }

    #[instrument(skip(self))]
    pub fn delete_volume(&self, name: &str) -> Result<()> {
        let full_name = if name.starts_with(&self.parent_dataset) {
            name.to_string()
        } else {
            format!("{}/{}", self.parent_dataset, name)
        };

        let output = Command::new("zfs")
            .args(["destroy", &full_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") {
                return Err(ZfsError::DatasetNotFound(full_name));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        debug!("Deleted zvol: {}", full_name);
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<()> {
        let full_name = if name.starts_with(&self.parent_dataset) {
            name.to_string()
        } else {
            format!("{}/{}", self.parent_dataset, name)
        };

        let output = Command::new("zfs")
            .args(["set", &format!("volsize={}B", new_size_bytes), &full_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        debug!("Resized zvol {} to {} bytes", full_name, new_size_bytes);
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn create_snapshot(&self, volume_name: &str, snap_name: &str) -> Result<String> {
        let full_vol = if volume_name.starts_with(&self.parent_dataset) {
            volume_name.to_string()
        } else {
            format!("{}/{}", self.parent_dataset, volume_name)
        };
        let snapshot_name = format!("{}@{}", full_vol, snap_name);

        let output = Command::new("zfs")
            .args(["snapshot", &snapshot_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        debug!("Created snapshot: {}", snapshot_name);
        Ok(snapshot_name)
    }

    pub fn get_dataset(&self, name: &str) -> Result<Dataset> {
        let output = Command::new("zfs")
            .args([
                "list", "-H", "-p",
                "-o", "name,used,available,referenced,mountpoint",
                name,
            ])
            .output()?;

        if !output.status.success() {
            return Err(ZfsError::DatasetNotFound(name.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let parts: Vec<&str> = stdout.trim().split('\t').collect();

        if parts.len() < 5 {
            return Err(ZfsError::ParseError("unexpected output format".into()));
        }

        Ok(Dataset {
            name: parts[0].to_string(),
            used: parts[1].parse().unwrap_or(0),
            available: parts[2].parse().unwrap_or(0),
            referenced: parts[3].parse().unwrap_or(0),
            mountpoint: if parts[4] == "-" || parts[4] == "none" {
                None
            } else {
                Some(parts[4].to_string())
            },
        })
    }

    pub fn list_volumes(&self) -> Result<Vec<Dataset>> {
        let output = Command::new("zfs")
            .args([
                "list", "-H", "-p", "-t", "volume",
                "-o", "name,used,available,referenced,mountpoint",
                "-r", &self.parent_dataset,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut datasets = Vec::new();

        for line in stdout.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 5 {
                datasets.push(Dataset {
                    name: parts[0].to_string(),
                    used: parts[1].parse().unwrap_or(0),
                    available: parts[2].parse().unwrap_or(0),
                    referenced: parts[3].parse().unwrap_or(0),
                    mountpoint: if parts[4] == "-" || parts[4] == "none" {
                        None
                    } else {
                        Some(parts[4].to_string())
                    },
                });
            }
        }

        Ok(datasets)
    }

    /// Returns the device path for a zvol
    pub fn get_device_path(&self, name: &str) -> String {
        let vol_name = if name.starts_with(&self.parent_dataset) {
            name.to_string()
        } else {
            format!("{}/{}", self.parent_dataset, name)
        };
        format!("/dev/zvol/{}", vol_name)
    }
}
```

**Step 3: Create module file**

```rust
// ctld-agent/src/zfs/mod.rs
pub mod dataset;
pub mod error;

pub use dataset::{Dataset, ZfsManager};
pub use error::{ZfsError, Result};
```

**Step 4: Add module to main.rs**

Add to `main.rs`:
```rust
mod zfs;
```

**Step 5: Verify compilation**

Run: `cargo build -p ctld-agent`
Expected: Build succeeds

**Step 6: Commit**

```bash
git add ctld-agent/src/zfs/
git commit -m "feat: add ZFS volume management module"
```

---

## Phase 3: CTL (CAM Target Layer) Integration

### Task 4: CTL iSCSI Export Module

**Files:**
- Create: `ctld-agent/src/ctl/mod.rs`
- Create: `ctld-agent/src/ctl/iscsi.rs`
- Create: `ctld-agent/src/ctl/error.rs`
- Create: `ctld-agent/src/ctl/config.rs`

**Step 1: Create CTL error types**

```rust
// ctld-agent/src/ctl/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CtlError {
    #[error("target '{0}' not found")]
    TargetNotFound(String),

    #[error("target '{0}' already exists")]
    TargetExists(String),

    #[error("LUN {0} already in use")]
    LunInUse(u32),

    #[error("ctld command failed: {0}")]
    CommandFailed(String),

    #[error("failed to parse ctld output: {0}")]
    ParseError(String),

    #[error("configuration error: {0}")]
    ConfigError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CtlError>;
```

**Step 2: Create CTL config types**

```rust
// ctld-agent/src/ctl/config.rs
use serde::{Deserialize, Serialize};

/// Represents a CTL LUN (Logical Unit)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lun {
    pub id: u32,
    pub backend: String,  // "block" for zvols
    pub path: String,     // /dev/zvol/...
    pub options: Vec<(String, String)>,
}

/// Represents an iSCSI target
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IscsiTarget {
    pub name: String,  // IQN
    pub alias: Option<String>,
    pub portal_group: String,
    pub luns: Vec<Lun>,
    pub auth_group: Option<String>,
}

/// Represents a portal group (network endpoints)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalGroup {
    pub name: String,
    pub listen: Vec<String>,  // e.g., ["0.0.0.0:3260"]
}

impl Lun {
    pub fn new_block(id: u32, device_path: &str) -> Self {
        Self {
            id,
            backend: "block".to_string(),
            path: device_path.to_string(),
            options: vec![],
        }
    }
}

impl IscsiTarget {
    /// Generate an IQN for a volume
    pub fn generate_iqn(base_iqn: &str, volume_name: &str) -> String {
        // Format: iqn.2024-01.com.example:storage:volume-name
        format!("{}:{}", base_iqn, volume_name.replace('/', "-"))
    }
}
```

**Step 3: Create iSCSI manager**

```rust
// ctld-agent/src/ctl/iscsi.rs
use super::config::{IscsiTarget, Lun, PortalGroup};
use super::error::{CtlError, Result};
use std::collections::HashMap;
use std::fs;
use std::process::Command;
use tracing::{debug, info, instrument};

const CTL_CONF_PATH: &str = "/etc/ctl.conf";
const CTLD_SOCKET: &str = "/var/run/ctld.sock";

#[derive(Debug)]
pub struct IscsiManager {
    base_iqn: String,
    portal_group: String,
    targets: HashMap<String, IscsiTarget>,
}

impl IscsiManager {
    pub fn new(base_iqn: String, portal_group: String) -> Self {
        Self {
            base_iqn,
            portal_group,
            targets: HashMap::new(),
        }
    }

    /// Load existing configuration from ctld
    #[instrument(skip(self))]
    pub fn load_config(&mut self) -> Result<()> {
        // Parse existing ctl.conf to populate targets map
        // For now, start with empty state - full implementation would parse the config
        debug!("Loading CTL configuration");
        Ok(())
    }

    /// Export a zvol as an iSCSI target
    #[instrument(skip(self))]
    pub fn export_volume(
        &mut self,
        volume_name: &str,
        device_path: &str,
        lun_id: u32,
    ) -> Result<IscsiTarget> {
        let iqn = IscsiTarget::generate_iqn(&self.base_iqn, volume_name);

        if self.targets.contains_key(&iqn) {
            return Err(CtlError::TargetExists(iqn));
        }

        let lun = Lun::new_block(lun_id, device_path);
        let target = IscsiTarget {
            name: iqn.clone(),
            alias: Some(volume_name.to_string()),
            portal_group: self.portal_group.clone(),
            luns: vec![lun],
            auth_group: None,
        };

        // Add target via ctladm
        self.add_target_live(&target)?;

        self.targets.insert(iqn.clone(), target.clone());
        info!("Exported volume {} as iSCSI target {}", volume_name, iqn);

        Ok(target)
    }

    /// Remove an iSCSI target
    #[instrument(skip(self))]
    pub fn unexport_volume(&mut self, target_name: &str) -> Result<()> {
        if !self.targets.contains_key(target_name) {
            return Err(CtlError::TargetNotFound(target_name.to_string()));
        }

        self.remove_target_live(target_name)?;
        self.targets.remove(target_name);

        info!("Removed iSCSI target {}", target_name);
        Ok(())
    }

    /// Add target using ctladm (live, without restart)
    fn add_target_live(&self, target: &IscsiTarget) -> Result<()> {
        // First, create the LUN
        for lun in &target.luns {
            let output = Command::new("ctladm")
                .args([
                    "create",
                    "-b", &lun.backend,
                    "-o", &format!("file={}", lun.path),
                    "-d", &target.name,
                ])
                .output()?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(CtlError::CommandFailed(format!(
                    "ctladm create failed: {}", stderr
                )));
            }
        }

        // Reload ctld to pick up the new target
        self.reload_ctld()?;

        Ok(())
    }

    /// Remove target using ctladm
    fn remove_target_live(&self, target_name: &str) -> Result<()> {
        // Find and remove LUNs associated with this target
        let output = Command::new("ctladm")
            .args(["devlist", "-v"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CtlError::CommandFailed(format!(
                "ctladm devlist failed: {}", stderr
            )));
        }

        // Parse output to find LUN IDs for this target and remove them
        // This is simplified - full implementation would parse XML output
        let output = Command::new("ctladm")
            .args(["remove", "-b", "block", "-d", target_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // Ignore "not found" errors
            if !stderr.contains("not found") {
                return Err(CtlError::CommandFailed(format!(
                    "ctladm remove failed: {}", stderr
                )));
            }
        }

        self.reload_ctld()?;
        Ok(())
    }

    /// Signal ctld to reload configuration
    fn reload_ctld(&self) -> Result<()> {
        let output = Command::new("service")
            .args(["ctld", "reload"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            debug!("ctld reload warning: {}", stderr);
            // Don't fail on reload issues - ctld might not be running yet
        }

        Ok(())
    }

    /// Get target by name
    pub fn get_target(&self, name: &str) -> Option<&IscsiTarget> {
        self.targets.get(name)
    }

    /// List all targets
    pub fn list_targets(&self) -> Vec<&IscsiTarget> {
        self.targets.values().collect()
    }
}
```

**Step 4: Create CTL module file**

```rust
// ctld-agent/src/ctl/mod.rs
pub mod config;
pub mod error;
pub mod iscsi;

pub use config::{IscsiTarget, Lun, PortalGroup};
pub use error::{CtlError, Result};
pub use iscsi::IscsiManager;
```

**Step 5: Add module to main.rs**

Add to `main.rs`:
```rust
mod ctl;
```

**Step 6: Verify compilation**

Run: `cargo build -p ctld-agent`
Expected: Build succeeds

**Step 7: Commit**

```bash
git add ctld-agent/src/ctl/
git commit -m "feat: add CTL iSCSI export module"
```

---

### Task 5: NVMeoF Export Module (Optional Extension)

**Files:**
- Create: `ctld-agent/src/ctl/nvmeof.rs`
- Modify: `ctld-agent/src/ctl/mod.rs`

**Step 1: Create NVMeoF manager**

```rust
// ctld-agent/src/ctl/nvmeof.rs
use super::error::{CtlError, Result};
use std::collections::HashMap;
use std::process::Command;
use tracing::{debug, info, instrument};

/// NVMe Qualified Name for a subsystem
#[derive(Debug, Clone)]
pub struct NvmeSubsystem {
    pub nqn: String,
    pub namespace_id: u32,
    pub device_path: String,
}

#[derive(Debug)]
pub struct NvmeofManager {
    base_nqn: String,
    subsystems: HashMap<String, NvmeSubsystem>,
}

impl NvmeofManager {
    pub fn new(base_nqn: String) -> Self {
        Self {
            base_nqn,
            subsystems: HashMap::new(),
        }
    }

    /// Generate an NQN for a volume
    pub fn generate_nqn(&self, volume_name: &str) -> String {
        format!("{}:{}", self.base_nqn, volume_name.replace('/', "-"))
    }

    /// Export a zvol as an NVMeoF namespace
    #[instrument(skip(self))]
    pub fn export_volume(
        &mut self,
        volume_name: &str,
        device_path: &str,
        namespace_id: u32,
    ) -> Result<NvmeSubsystem> {
        let nqn = self.generate_nqn(volume_name);

        if self.subsystems.contains_key(&nqn) {
            return Err(CtlError::TargetExists(nqn));
        }

        // Use nvmetcli or ctladm for NVMeoF configuration
        // FreeBSD's CTL supports NVMeoF via the same infrastructure
        let output = Command::new("ctladm")
            .args([
                "create",
                "-b", "block",
                "-o", &format!("file={}", device_path),
                "-o", "vendor=FreeBSD",
                "-o", &format!("product={}", volume_name),
                "-S", &nqn,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CtlError::CommandFailed(format!(
                "ctladm create (nvmeof) failed: {}", stderr
            )));
        }

        let subsystem = NvmeSubsystem {
            nqn: nqn.clone(),
            namespace_id,
            device_path: device_path.to_string(),
        };

        self.subsystems.insert(nqn.clone(), subsystem.clone());
        info!("Exported volume {} as NVMeoF subsystem {}", volume_name, nqn);

        Ok(subsystem)
    }

    /// Remove an NVMeoF subsystem
    #[instrument(skip(self))]
    pub fn unexport_volume(&mut self, nqn: &str) -> Result<()> {
        if !self.subsystems.contains_key(nqn) {
            return Err(CtlError::TargetNotFound(nqn.to_string()));
        }

        let output = Command::new("ctladm")
            .args(["remove", "-b", "block", "-S", nqn])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.contains("not found") {
                return Err(CtlError::CommandFailed(format!(
                    "ctladm remove (nvmeof) failed: {}", stderr
                )));
            }
        }

        self.subsystems.remove(nqn);
        info!("Removed NVMeoF subsystem {}", nqn);

        Ok(())
    }

    pub fn get_subsystem(&self, nqn: &str) -> Option<&NvmeSubsystem> {
        self.subsystems.get(nqn)
    }

    pub fn list_subsystems(&self) -> Vec<&NvmeSubsystem> {
        self.subsystems.values().collect()
    }
}
```

**Step 2: Update mod.rs to include NVMeoF**

```rust
// ctld-agent/src/ctl/mod.rs
pub mod config;
pub mod error;
pub mod iscsi;
pub mod nvmeof;

pub use config::{IscsiTarget, Lun, PortalGroup};
pub use error::{CtlError, Result};
pub use iscsi::IscsiManager;
pub use nvmeof::{NvmeofManager, NvmeSubsystem};
```

**Step 3: Verify compilation**

Run: `cargo build -p ctld-agent`
Expected: Build succeeds

**Step 4: Commit**

```bash
git add ctld-agent/src/ctl/
git commit -m "feat: add NVMeoF export support"
```

---

## Phase 4: gRPC Service Implementation

### Task 6: Storage Agent Service

**Files:**
- Create: `ctld-agent/src/service/mod.rs`
- Create: `ctld-agent/src/service/storage.rs`
- Modify: `ctld-agent/src/main.rs`

**Step 1: Create storage service implementation**

```rust
// ctld-agent/src/service/storage.rs
use crate::ctl::{IscsiManager, NvmeofManager};
use crate::zfs::ZfsManager;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("ctld_agent.v1");
}

use proto::storage_agent_server::StorageAgent;
use proto::*;

pub struct StorageService {
    zfs: Arc<RwLock<ZfsManager>>,
    iscsi: Arc<RwLock<IscsiManager>>,
    nvmeof: Arc<RwLock<NvmeofManager>>,
}

impl StorageService {
    pub fn new(
        zfs: ZfsManager,
        iscsi: IscsiManager,
        nvmeof: NvmeofManager,
    ) -> Self {
        Self {
            zfs: Arc::new(RwLock::new(zfs)),
            iscsi: Arc::new(RwLock::new(iscsi)),
            nvmeof: Arc::new(RwLock::new(nvmeof)),
        }
    }
}

#[tonic::async_trait]
impl StorageAgent for StorageService {
    #[instrument(skip(self))]
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("CreateVolume request: name={}, size={}", req.name, req.size_bytes);

        // Create ZFS volume
        let zfs = self.zfs.write().await;
        let dataset = zfs
            .create_volume(&req.name, req.size_bytes as u64)
            .map_err(|e| Status::internal(format!("ZFS error: {}", e)))?;

        let device_path = zfs.get_device_path(&req.name);
        drop(zfs);

        // Export based on type
        let (target_name, lun_id) = match req.export_type.as_str() {
            "nvmeof" => {
                let mut nvmeof = self.nvmeof.write().await;
                let subsystem = nvmeof
                    .export_volume(&req.name, &device_path, 1)
                    .map_err(|e| Status::internal(format!("NVMeoF error: {}", e)))?;
                (subsystem.nqn, subsystem.namespace_id as i32)
            }
            _ => {
                // Default to iSCSI
                let mut iscsi = self.iscsi.write().await;
                let target = iscsi
                    .export_volume(&req.name, &device_path, 0)
                    .map_err(|e| Status::internal(format!("iSCSI error: {}", e)))?;
                (target.name, 0)
            }
        };

        let volume = Volume {
            id: req.name.clone(),
            name: req.name.clone(),
            size_bytes: req.size_bytes,
            zfs_dataset: dataset.name,
            export_type: req.export_type,
            target_name,
            lun_id,
            parameters: req.parameters,
        };

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume),
        }))
    }

    #[instrument(skip(self))]
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteVolume request: id={}", req.volume_id);

        // First unexport from iSCSI/NVMeoF
        // Try both - one will fail silently if not exported there
        {
            let mut iscsi = self.iscsi.write().await;
            let iqn = format!("{}:{}", "iqn.2024-01.freebsd.csi", req.volume_id.replace('/', "-"));
            let _ = iscsi.unexport_volume(&iqn);
        }
        {
            let mut nvmeof = self.nvmeof.write().await;
            let nqn = format!("{}:{}", "nqn.2024-01.freebsd.csi", req.volume_id.replace('/', "-"));
            let _ = nvmeof.unexport_volume(&nqn);
        }

        // Delete ZFS volume
        let zfs = self.zfs.write().await;
        zfs.delete_volume(&req.volume_id)
            .map_err(|e| Status::internal(format!("ZFS error: {}", e)))?;

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    #[instrument(skip(self))]
    async fn expand_volume(
        &self,
        request: Request<ExpandVolumeRequest>,
    ) -> Result<Response<ExpandVolumeResponse>, Status> {
        let req = request.into_inner();
        info!("ExpandVolume request: id={}, new_size={}", req.volume_id, req.new_size_bytes);

        let zfs = self.zfs.write().await;
        zfs.resize_volume(&req.volume_id, req.new_size_bytes as u64)
            .map_err(|e| Status::internal(format!("ZFS error: {}", e)))?;

        Ok(Response::new(ExpandVolumeResponse {
            size_bytes: req.new_size_bytes,
        }))
    }

    #[instrument(skip(self))]
    async fn list_volumes(
        &self,
        request: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        let _req = request.into_inner();

        let zfs = self.zfs.read().await;
        let datasets = zfs
            .list_volumes()
            .map_err(|e| Status::internal(format!("ZFS error: {}", e)))?;

        let volumes: Vec<Volume> = datasets
            .into_iter()
            .map(|d| Volume {
                id: d.name.clone(),
                name: d.name.split('/').last().unwrap_or(&d.name).to_string(),
                size_bytes: d.referenced as i64,
                zfs_dataset: d.name,
                export_type: String::new(),
                target_name: String::new(),
                lun_id: 0,
                parameters: std::collections::HashMap::new(),
            })
            .collect();

        Ok(Response::new(ListVolumesResponse {
            volumes,
            next_token: String::new(),
        }))
    }

    #[instrument(skip(self))]
    async fn get_volume(
        &self,
        request: Request<GetVolumeRequest>,
    ) -> Result<Response<GetVolumeResponse>, Status> {
        let req = request.into_inner();

        let zfs = self.zfs.read().await;
        let dataset = zfs
            .get_dataset(&req.volume_id)
            .map_err(|e| Status::not_found(format!("Volume not found: {}", e)))?;

        let volume = Volume {
            id: dataset.name.clone(),
            name: dataset.name.split('/').last().unwrap_or(&dataset.name).to_string(),
            size_bytes: dataset.referenced as i64,
            zfs_dataset: dataset.name,
            export_type: String::new(),
            target_name: String::new(),
            lun_id: 0,
            parameters: std::collections::HashMap::new(),
        };

        Ok(Response::new(GetVolumeResponse {
            volume: Some(volume),
        }))
    }

    #[instrument(skip(self))]
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        info!("CreateSnapshot request: source={}, name={}", req.source_volume_id, req.name);

        let zfs = self.zfs.write().await;
        let snapshot_name = zfs
            .create_snapshot(&req.source_volume_id, &req.name)
            .map_err(|e| Status::internal(format!("ZFS error: {}", e)))?;

        let snapshot = Snapshot {
            id: snapshot_name.clone(),
            source_volume_id: req.source_volume_id,
            name: req.name,
            creation_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            size_bytes: 0,
        };

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    #[instrument(skip(self))]
    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();
        info!("DeleteSnapshot request: id={}", req.snapshot_id);

        // ZFS snapshot deletion uses the same destroy command
        let zfs = self.zfs.write().await;
        zfs.delete_volume(&req.snapshot_id)
            .map_err(|e| Status::internal(format!("ZFS error: {}", e)))?;

        Ok(Response::new(DeleteSnapshotResponse {}))
    }
}
```

**Step 2: Create service module**

```rust
// ctld-agent/src/service/mod.rs
pub mod storage;

pub use storage::{proto, StorageService};
```

**Step 3: Update main.rs to run gRPC server**

```rust
// ctld-agent/src/main.rs
use clap::Parser;
use tonic::transport::Server;
use tracing::info;

mod ctl;
mod service;
mod zfs;

use ctl::{IscsiManager, NvmeofManager};
use service::{proto::storage_agent_server::StorageAgentServer, StorageService};
use zfs::ZfsManager;

#[derive(Parser, Debug)]
#[command(name = "ctld-agent")]
#[command(about = "FreeBSD ZFS/CTL storage agent for Kubernetes CSI")]
struct Args {
    /// gRPC listen address
    #[arg(long, default_value = "[::1]:50051")]
    listen: String,

    /// ZFS parent dataset for volumes
    #[arg(long)]
    zfs_parent: String,

    /// Base IQN for iSCSI targets
    #[arg(long, default_value = "iqn.2024-01.freebsd.csi")]
    base_iqn: String,

    /// Base NQN for NVMeoF subsystems
    #[arg(long, default_value = "nqn.2024-01.freebsd.csi")]
    base_nqn: String,

    /// Portal group for iSCSI
    #[arg(long, default_value = "pg0")]
    portal_group: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("Starting ctld-agent on {}", args.listen);
    info!("ZFS parent dataset: {}", args.zfs_parent);

    // Initialize managers
    let zfs = ZfsManager::new(args.zfs_parent)?;
    let iscsi = IscsiManager::new(args.base_iqn, args.portal_group);
    let nvmeof = NvmeofManager::new(args.base_nqn);

    let service = StorageService::new(zfs, iscsi, nvmeof);

    let addr = args.listen.parse()?;
    info!("Listening on {}", addr);

    Server::builder()
        .add_service(StorageAgentServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
```

**Step 4: Verify compilation**

Run: `cargo build -p ctld-agent`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add ctld-agent/src/service/ ctld-agent/src/main.rs
git commit -m "feat: implement gRPC storage agent service"
```

---

## Phase 5: Kubernetes CSI Driver (Rust)

### Task 7: Add CSI Driver Crate to Workspace

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Create: `csi-driver/Cargo.toml`
- Create: `csi-driver/src/main.rs`
- Create: `csi-driver/build.rs`
- Create: `proto/csi.proto` (copy from CSI spec)

**Step 1: Update workspace Cargo.toml**

Add `csi-driver` to workspace members and add new dependencies:

```toml
[workspace]
resolver = "2"
members = [
    "ctld-agent",
    "csi-driver",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
license = "BSD-2-Clause"
repository = "https://github.com/ndenev/freebsd-csi"

[workspace.dependencies]
tokio = { version = "1.43", features = ["full", "process"] }
tonic = "0.12"
prost = "0.13"
prost-types = "0.13"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
clap = { version = "4.5", features = ["derive"] }
uuid = { version = "1.11", features = ["v4"] }
```

**Step 2: Create csi-driver/Cargo.toml**

```toml
[package]
name = "csi-driver"
version.workspace = true
edition.workspace = true
license.workspace = true

[dependencies]
tokio.workspace = true
tonic.workspace = true
prost.workspace = true
prost-types.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
clap.workspace = true
uuid.workspace = true

[build-dependencies]
tonic-build = "0.12"
```

**Step 3: Create csi-driver/build.rs**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile CSI proto (from official CSI spec)
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../proto/csi.proto"], &["../proto"])?;

    // Compile agent proto for client
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["../proto/ctld_agent.proto"], &["../proto"])?;

    Ok(())
}
```

**Step 4: Download CSI proto specification**

Run: `curl -o proto/csi.proto https://raw.githubusercontent.com/container-storage-interface/spec/v1.9.0/csi.proto`

**Step 5: Create minimal main.rs**

```rust
use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "csi-driver")]
#[command(about = "FreeBSD CSI driver for Kubernetes")]
struct Args {
    /// CSI endpoint (unix socket path)
    #[arg(long, default_value = "unix:///csi/csi.sock")]
    endpoint: String,

    /// Node ID for this driver instance
    #[arg(long, env = "NODE_NAME")]
    node_id: Option<String>,

    /// ctld-agent gRPC endpoint
    #[arg(long, default_value = "http://localhost:50051")]
    agent_endpoint: String,

    /// Run as controller (otherwise runs as node)
    #[arg(long)]
    controller: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let node_id = args.node_id.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    });

    info!("Starting FreeBSD CSI driver");
    info!("Endpoint: {}", args.endpoint);
    info!("Node ID: {}", node_id);
    info!("Agent endpoint: {}", args.agent_endpoint);
    info!("Mode: {}", if args.controller { "controller" } else { "node" });

    Ok(())
}
```

**Step 6: Verify compilation**

Run: `cargo build -p csi-driver`
Expected: Build succeeds

**Step 7: Commit**

```bash
git add Cargo.toml csi-driver/ proto/csi.proto
git commit -m "feat: initialize Rust CSI driver crate"
```

---

### Task 8: CSI Identity Service (Rust)

**Files:**
- Create: `csi-driver/src/identity.rs`
- Create: `csi-driver/src/proto.rs`
- Modify: `csi-driver/src/main.rs`

**Step 1: Create proto module**

```rust
// csi-driver/src/proto.rs
pub mod csi {
    tonic::include_proto!("csi.v1");
}

pub mod agent {
    tonic::include_proto!("ctld_agent.v1");
}
```

**Step 2: Create identity service**

```rust
// csi-driver/src/identity.rs
use crate::proto::csi::identity_server::Identity;
use crate::proto::csi::*;
use tonic::{Request, Response, Status};

pub const DRIVER_NAME: &str = "csi.freebsd.org";
pub const DRIVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct IdentityService;

impl IdentityService {
    pub fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl Identity for IdentityService {
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: DRIVER_NAME.to_string(),
            vendor_version: DRIVER_VERSION.to_string(),
            manifest: std::collections::HashMap::new(),
        }))
    }

    async fn get_plugin_capabilities(
        &self,
        _request: Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        use plugin_capability::service::Type as ServiceType;
        use plugin_capability::volume_expansion::Type as ExpansionType;
        use plugin_capability::Service;
        use plugin_capability::VolumeExpansion;

        let capabilities = vec![
            PluginCapability {
                r#type: Some(plugin_capability::Type::Service(Service {
                    r#type: ServiceType::ControllerService as i32,
                })),
            },
            PluginCapability {
                r#type: Some(plugin_capability::Type::VolumeExpansion(VolumeExpansion {
                    r#type: ExpansionType::Online as i32,
                })),
            },
        ];

        Ok(Response::new(GetPluginCapabilitiesResponse { capabilities }))
    }

    async fn probe(
        &self,
        _request: Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        Ok(Response::new(ProbeResponse { ready: Some(true) }))
    }
}
```

**Step 3: Update main.rs to include module**

Add to `main.rs`:
```rust
mod identity;
mod proto;
```

**Step 4: Verify compilation**

Run: `cargo build -p csi-driver`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add csi-driver/src/
git commit -m "feat: implement CSI identity service in Rust"
```

---

### Task 9: CSI Controller Service (Rust)

**Files:**
- Create: `csi-driver/src/controller.rs`
- Create: `csi-driver/src/agent_client.rs`

**Step 1: Create agent client**

```rust
// csi-driver/src/agent_client.rs
use crate::proto::agent::storage_agent_client::StorageAgentClient;
use crate::proto::agent::*;
use tonic::transport::Channel;
use tracing::instrument;

#[derive(Clone)]
pub struct AgentClient {
    client: StorageAgentClient<Channel>,
}

impl AgentClient {
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        let client = StorageAgentClient::connect(endpoint.to_string()).await?;
        Ok(Self { client })
    }

    #[instrument(skip(self))]
    pub async fn create_volume(
        &mut self,
        name: &str,
        size_bytes: i64,
        export_type: &str,
        parameters: std::collections::HashMap<String, String>,
    ) -> Result<Volume, tonic::Status> {
        let request = CreateVolumeRequest {
            name: name.to_string(),
            size_bytes,
            export_type: export_type.to_string(),
            parameters,
        };
        let response = self.client.create_volume(request).await?;
        response
            .into_inner()
            .volume
            .ok_or_else(|| tonic::Status::internal("No volume in response"))
    }

    #[instrument(skip(self))]
    pub async fn delete_volume(&mut self, volume_id: &str) -> Result<(), tonic::Status> {
        let request = DeleteVolumeRequest {
            volume_id: volume_id.to_string(),
        };
        self.client.delete_volume(request).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn expand_volume(
        &mut self,
        volume_id: &str,
        new_size_bytes: i64,
    ) -> Result<i64, tonic::Status> {
        let request = ExpandVolumeRequest {
            volume_id: volume_id.to_string(),
            new_size_bytes,
        };
        let response = self.client.expand_volume(request).await?;
        Ok(response.into_inner().size_bytes)
    }

    #[instrument(skip(self))]
    pub async fn get_volume(&mut self, volume_id: &str) -> Result<Volume, tonic::Status> {
        let request = GetVolumeRequest {
            volume_id: volume_id.to_string(),
        };
        let response = self.client.get_volume(request).await?;
        response
            .into_inner()
            .volume
            .ok_or_else(|| tonic::Status::not_found("Volume not found"))
    }

    #[instrument(skip(self))]
    pub async fn create_snapshot(
        &mut self,
        source_volume_id: &str,
        name: &str,
    ) -> Result<Snapshot, tonic::Status> {
        let request = CreateSnapshotRequest {
            source_volume_id: source_volume_id.to_string(),
            name: name.to_string(),
        };
        let response = self.client.create_snapshot(request).await?;
        response
            .into_inner()
            .snapshot
            .ok_or_else(|| tonic::Status::internal("No snapshot in response"))
    }

    #[instrument(skip(self))]
    pub async fn delete_snapshot(&mut self, snapshot_id: &str) -> Result<(), tonic::Status> {
        let request = DeleteSnapshotRequest {
            snapshot_id: snapshot_id.to_string(),
        };
        self.client.delete_snapshot(request).await?;
        Ok(())
    }
}
```

**Step 2: Create controller service**

```rust
// csi-driver/src/controller.rs
use crate::agent_client::AgentClient;
use crate::proto::csi::controller_server::Controller;
use crate::proto::csi::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

const DEFAULT_VOLUME_SIZE: i64 = 1024 * 1024 * 1024; // 1GB

pub struct ControllerService {
    agent_endpoint: String,
    client: Arc<Mutex<Option<AgentClient>>>,
}

impl ControllerService {
    pub fn new(agent_endpoint: String) -> Self {
        Self {
            agent_endpoint,
            client: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_client(&self) -> Result<AgentClient, Status> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            let client = AgentClient::connect(&self.agent_endpoint)
                .await
                .map_err(|e| Status::internal(format!("Failed to connect to agent: {}", e)))?;
            *guard = Some(client);
        }
        Ok(guard.clone().unwrap())
    }
}

#[tonic::async_trait]
impl Controller for ControllerService {
    #[instrument(skip(self))]
    async fn create_volume(
        &self,
        request: Request<CreateVolumeRequest>,
    ) -> Result<Response<CreateVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Err(Status::invalid_argument("Volume name required"));
        }

        let capacity = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes)
            .unwrap_or(DEFAULT_VOLUME_SIZE);

        let export_type = req
            .parameters
            .get("exportType")
            .map(|s| s.as_str())
            .unwrap_or("iscsi");

        info!(
            "CreateVolume: name={}, size={}, exportType={}",
            req.name, capacity, export_type
        );

        let mut client = self.get_client().await?;
        let vol = client
            .create_volume(&req.name, capacity, export_type, req.parameters)
            .await?;

        let mut context = HashMap::new();
        context.insert("targetName".to_string(), vol.target_name);
        context.insert("exportType".to_string(), vol.export_type);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                volume_id: vol.id,
                capacity_bytes: vol.size_bytes,
                volume_context: context,
                content_source: None,
                accessible_topology: vec![],
            }),
        }))
    }

    #[instrument(skip(self))]
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<DeleteVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }

        info!("DeleteVolume: id={}", req.volume_id);

        let mut client = self.get_client().await?;
        client.delete_volume(&req.volume_id).await?;

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    #[instrument(skip(self))]
    async fn controller_expand_volume(
        &self,
        request: Request<ControllerExpandVolumeRequest>,
    ) -> Result<Response<ControllerExpandVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }

        let new_size = req
            .capacity_range
            .as_ref()
            .map(|r| r.required_bytes)
            .ok_or_else(|| Status::invalid_argument("Capacity range required"))?;

        info!(
            "ControllerExpandVolume: id={}, newSize={}",
            req.volume_id, new_size
        );

        let mut client = self.get_client().await?;
        let size = client.expand_volume(&req.volume_id, new_size).await?;

        Ok(Response::new(ControllerExpandVolumeResponse {
            capacity_bytes: size,
            node_expansion_required: false,
        }))
    }

    async fn controller_get_capabilities(
        &self,
        _request: Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<Response<ControllerGetCapabilitiesResponse>, Status> {
        use controller_service_capability::rpc::Type;
        use controller_service_capability::Rpc;

        let caps = vec![
            Type::CreateDeleteVolume,
            Type::CreateDeleteSnapshot,
            Type::ExpandVolume,
        ];

        let capabilities = caps
            .into_iter()
            .map(|t| ControllerServiceCapability {
                r#type: Some(controller_service_capability::Type::Rpc(Rpc {
                    r#type: t as i32,
                })),
            })
            .collect();

        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities,
        }))
    }

    #[instrument(skip(self))]
    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();

        if req.source_volume_id.is_empty() {
            return Err(Status::invalid_argument("Source volume ID required"));
        }
        if req.name.is_empty() {
            return Err(Status::invalid_argument("Snapshot name required"));
        }

        info!(
            "CreateSnapshot: source={}, name={}",
            req.source_volume_id, req.name
        );

        let mut client = self.get_client().await?;
        let snap = client
            .create_snapshot(&req.source_volume_id, &req.name)
            .await?;

        Ok(Response::new(CreateSnapshotResponse {
            snapshot: Some(Snapshot {
                snapshot_id: snap.id,
                source_volume_id: snap.source_volume_id,
                creation_time: Some(prost_types::Timestamp {
                    seconds: snap.creation_time,
                    nanos: 0,
                }),
                ready_to_use: true,
                size_bytes: snap.size_bytes,
                group_snapshot_id: String::new(),
            }),
        }))
    }

    #[instrument(skip(self))]
    async fn delete_snapshot(
        &self,
        request: Request<DeleteSnapshotRequest>,
    ) -> Result<Response<DeleteSnapshotResponse>, Status> {
        let req = request.into_inner();

        if req.snapshot_id.is_empty() {
            return Err(Status::invalid_argument("Snapshot ID required"));
        }

        info!("DeleteSnapshot: id={}", req.snapshot_id);

        let mut client = self.get_client().await?;
        client.delete_snapshot(&req.snapshot_id).await?;

        Ok(Response::new(DeleteSnapshotResponse {}))
    }

    async fn validate_volume_capabilities(
        &self,
        request: Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<Response<ValidateVolumeCapabilitiesResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }

        let mut client = self.get_client().await?;
        client.get_volume(&req.volume_id).await?;

        Ok(Response::new(ValidateVolumeCapabilitiesResponse {
            confirmed: Some(validate_volume_capabilities_response::Confirmed {
                volume_context: HashMap::new(),
                volume_capabilities: req.volume_capabilities,
                parameters: HashMap::new(),
            }),
            message: String::new(),
        }))
    }

    // Unimplemented methods return UNIMPLEMENTED status
    async fn controller_publish_volume(
        &self,
        _: Request<ControllerPublishVolumeRequest>,
    ) -> Result<Response<ControllerPublishVolumeResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn controller_unpublish_volume(
        &self,
        _: Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<Response<ControllerUnpublishVolumeResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn list_volumes(
        &self,
        _: Request<ListVolumesRequest>,
    ) -> Result<Response<ListVolumesResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn get_capacity(
        &self,
        _: Request<GetCapacityRequest>,
    ) -> Result<Response<GetCapacityResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn list_snapshots(
        &self,
        _: Request<ListSnapshotsRequest>,
    ) -> Result<Response<ListSnapshotsResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn controller_get_volume(
        &self,
        _: Request<ControllerGetVolumeRequest>,
    ) -> Result<Response<ControllerGetVolumeResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn controller_modify_volume(
        &self,
        _: Request<ControllerModifyVolumeRequest>,
    ) -> Result<Response<ControllerModifyVolumeResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }
}
```

**Step 3: Add modules to main.rs**

```rust
mod agent_client;
mod controller;
```

**Step 4: Verify compilation**

Run: `cargo build -p csi-driver`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add csi-driver/src/
git commit -m "feat: implement CSI controller service in Rust"
```

---

### Task 10: CSI Node Service (Rust)

**Files:**
- Create: `csi-driver/src/node.rs`
- Modify: `csi-driver/src/main.rs`

**Step 1: Create node service**

```rust
// csi-driver/src/node.rs
use crate::proto::csi::node_server::Node;
use crate::proto::csi::*;
use std::path::Path;
use std::process::Command;
use tokio::fs;
use tonic::{Request, Response, Status};
use tracing::{info, instrument, warn};

pub struct NodeService {
    node_id: String,
}

impl NodeService {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    fn connect_iscsi(&self, target_iqn: &str) -> Result<String, Status> {
        // Use FreeBSD iscsictl to connect
        let output = Command::new("iscsictl")
            .args(["-An", target_iqn])
            .output()
            .map_err(|e| Status::internal(format!("Failed to run iscsictl: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("iscsictl failed: {}", stderr)));
        }

        // In production, wait for device and discover actual path
        // For now, return placeholder
        Ok("/dev/da0".to_string())
    }

    fn connect_nvmeof(&self, target_nqn: &str) -> Result<String, Status> {
        // Use FreeBSD nvmecontrol to connect
        let output = Command::new("nvmecontrol")
            .args(["connect", "-n", target_nqn])
            .output()
            .map_err(|e| Status::internal(format!("Failed to run nvmecontrol: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("nvmecontrol failed: {}", stderr)));
        }

        Ok("/dev/nvme0ns1".to_string())
    }

    fn format_device(&self, device: &str, fs_type: &str) -> Result<(), Status> {
        // Check if already formatted
        let output = Command::new("file")
            .args(["-s", device])
            .output()
            .map_err(|e| Status::internal(format!("file command failed: {}", e)))?;

        let stdout = String::from_utf8_lossy(&output.stdout);

        // If shows "data", it's not formatted
        if stdout.contains(": data") {
            info!("Formatting {} as {}", device, fs_type);

            let result = match fs_type {
                "ufs" => Command::new("newfs").args(["-U", device]).output(),
                "zfs" => return Ok(()), // ZFS zvols don't need formatting
                _ => Command::new("newfs").args(["-U", device]).output(),
            };

            let output = result.map_err(|e| Status::internal(format!("Format failed: {}", e)))?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Status::internal(format!("Format failed: {}", stderr)));
            }
        }

        Ok(())
    }

    fn mount_device(&self, device: &str, target: &str, fs_type: &str) -> Result<(), Status> {
        let output = Command::new("mount")
            .args(["-t", fs_type, device, target])
            .output()
            .map_err(|e| Status::internal(format!("mount failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("mount failed: {}", stderr)));
        }

        Ok(())
    }

    fn unmount(&self, target: &str) -> Result<(), Status> {
        let output = Command::new("umount")
            .arg(target)
            .output()
            .map_err(|e| Status::internal(format!("umount failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("umount warning: {}", stderr);
        }

        Ok(())
    }

    fn nullfs_mount(&self, source: &str, target: &str) -> Result<(), Status> {
        // FreeBSD uses nullfs instead of bind mounts
        let output = Command::new("mount")
            .args(["-t", "nullfs", source, target])
            .output()
            .map_err(|e| Status::internal(format!("nullfs mount failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Status::internal(format!("nullfs mount failed: {}", stderr)));
        }

        Ok(())
    }
}

#[tonic::async_trait]
impl Node for NodeService {
    #[instrument(skip(self))]
    async fn node_stage_volume(
        &self,
        request: Request<NodeStageVolumeRequest>,
    ) -> Result<Response<NodeStageVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }
        if req.staging_target_path.is_empty() {
            return Err(Status::invalid_argument("Staging target path required"));
        }

        let export_type = req.volume_context.get("exportType").map(|s| s.as_str()).unwrap_or("iscsi");
        let target_name = req.volume_context.get("targetName").ok_or_else(|| {
            Status::invalid_argument("targetName required in volume context")
        })?;

        info!(
            "NodeStageVolume: id={}, target={}, exportType={}, stagingPath={}",
            req.volume_id, target_name, export_type, req.staging_target_path
        );

        // Connect to target
        let device_path = match export_type {
            "nvmeof" => self.connect_nvmeof(target_name)?,
            _ => self.connect_iscsi(target_name)?,
        };

        // Create staging directory
        fs::create_dir_all(&req.staging_target_path)
            .await
            .map_err(|e| Status::internal(format!("Failed to create staging dir: {}", e)))?;

        // Format and mount
        let fs_type = req.volume_context.get("fsType").map(|s| s.as_str()).unwrap_or("ufs");
        self.format_device(&device_path, fs_type)?;
        self.mount_device(&device_path, &req.staging_target_path, fs_type)?;

        Ok(Response::new(NodeStageVolumeResponse {}))
    }

    #[instrument(skip(self))]
    async fn node_unstage_volume(
        &self,
        request: Request<NodeUnstageVolumeRequest>,
    ) -> Result<Response<NodeUnstageVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }

        info!(
            "NodeUnstageVolume: id={}, stagingPath={}",
            req.volume_id, req.staging_target_path
        );

        self.unmount(&req.staging_target_path)?;

        Ok(Response::new(NodeUnstageVolumeResponse {}))
    }

    #[instrument(skip(self))]
    async fn node_publish_volume(
        &self,
        request: Request<NodePublishVolumeRequest>,
    ) -> Result<Response<NodePublishVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }
        if req.target_path.is_empty() {
            return Err(Status::invalid_argument("Target path required"));
        }

        info!(
            "NodePublishVolume: id={}, stagingPath={}, targetPath={}",
            req.volume_id, req.staging_target_path, req.target_path
        );

        // Create target directory
        fs::create_dir_all(&req.target_path)
            .await
            .map_err(|e| Status::internal(format!("Failed to create target dir: {}", e)))?;

        // Nullfs mount from staging to target
        self.nullfs_mount(&req.staging_target_path, &req.target_path)?;

        Ok(Response::new(NodePublishVolumeResponse {}))
    }

    #[instrument(skip(self))]
    async fn node_unpublish_volume(
        &self,
        request: Request<NodeUnpublishVolumeRequest>,
    ) -> Result<Response<NodeUnpublishVolumeResponse>, Status> {
        let req = request.into_inner();

        if req.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID required"));
        }

        info!(
            "NodeUnpublishVolume: id={}, targetPath={}",
            req.volume_id, req.target_path
        );

        self.unmount(&req.target_path)?;

        Ok(Response::new(NodeUnpublishVolumeResponse {}))
    }

    async fn node_get_info(
        &self,
        _request: Request<NodeGetInfoRequest>,
    ) -> Result<Response<NodeGetInfoResponse>, Status> {
        Ok(Response::new(NodeGetInfoResponse {
            node_id: self.node_id.clone(),
            max_volumes_per_node: 0, // No limit
            accessible_topology: None,
        }))
    }

    async fn node_get_capabilities(
        &self,
        _request: Request<NodeGetCapabilitiesRequest>,
    ) -> Result<Response<NodeGetCapabilitiesResponse>, Status> {
        use node_service_capability::rpc::Type;
        use node_service_capability::Rpc;

        let caps = vec![
            Type::StageUnstageVolume,
            Type::ExpandVolume,
        ];

        let capabilities = caps
            .into_iter()
            .map(|t| NodeServiceCapability {
                r#type: Some(node_service_capability::Type::Rpc(Rpc {
                    r#type: t as i32,
                })),
            })
            .collect();

        Ok(Response::new(NodeGetCapabilitiesResponse { capabilities }))
    }

    async fn node_get_volume_stats(
        &self,
        _: Request<NodeGetVolumeStatsRequest>,
    ) -> Result<Response<NodeGetVolumeStatsResponse>, Status> {
        Err(Status::unimplemented("Not supported"))
    }

    async fn node_expand_volume(
        &self,
        _: Request<NodeExpandVolumeRequest>,
    ) -> Result<Response<NodeExpandVolumeResponse>, Status> {
        // ZFS handles expansion automatically
        Ok(Response::new(NodeExpandVolumeResponse {
            capacity_bytes: 0,
        }))
    }
}
```

**Step 2: Update main.rs for full driver**

```rust
// csi-driver/src/main.rs
use clap::Parser;
use std::path::Path;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tracing::info;

mod agent_client;
mod controller;
mod identity;
mod node;
mod proto;

use controller::ControllerService;
use identity::IdentityService;
use node::NodeService;
use proto::csi::{controller_server::ControllerServer, identity_server::IdentityServer, node_server::NodeServer};

#[derive(Parser, Debug)]
#[command(name = "csi-driver")]
#[command(about = "FreeBSD CSI driver for Kubernetes")]
struct Args {
    /// CSI endpoint (unix socket path)
    #[arg(long, default_value = "unix:///csi/csi.sock")]
    endpoint: String,

    /// Node ID for this driver instance
    #[arg(long, env = "NODE_NAME")]
    node_id: Option<String>,

    /// ctld-agent gRPC endpoint
    #[arg(long, default_value = "http://localhost:50051")]
    agent_endpoint: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let node_id = args.node_id.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    });

    info!("Starting FreeBSD CSI driver");
    info!("Endpoint: {}", args.endpoint);
    info!("Node ID: {}", node_id);
    info!("Agent endpoint: {}", args.agent_endpoint);

    // Parse endpoint
    let socket_path = args
        .endpoint
        .strip_prefix("unix://")
        .ok_or("Endpoint must start with unix://")?;

    // Remove existing socket if present
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    // Create parent directory if needed
    if let Some(parent) = Path::new(socket_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Create services
    let identity = IdentityService::new();
    let controller = ControllerService::new(args.agent_endpoint.clone());
    let node = NodeService::new(node_id);

    // Bind to Unix socket
    let listener = UnixListener::bind(socket_path)?;
    let stream = UnixListenerStream::new(listener);

    info!("Listening on {}", socket_path);

    Server::builder()
        .add_service(IdentityServer::new(identity))
        .add_service(ControllerServer::new(controller))
        .add_service(NodeServer::new(node))
        .serve_with_incoming(stream)
        .await?;

    Ok(())
}
```

**Step 3: Add dependencies to Cargo.toml**

Add to csi-driver/Cargo.toml:
```toml
hostname = "0.4"
tokio-stream = "0.1"
```

**Step 4: Verify compilation**

Run: `cargo build -p csi-driver`
Expected: Build succeeds

**Step 5: Commit**

```bash
git add csi-driver/
git commit -m "feat: implement CSI node service and complete driver in Rust"
```

---

## Phase 6: Kubernetes Deployment Manifests

### Task 11: Create Kubernetes Manifests

**Files:**
- Create: `deploy/kubernetes/csi-driver.yaml`
- Create: `deploy/kubernetes/storageclass.yaml`
- Create: `deploy/kubernetes/rbac.yaml`

**Step 1: Create RBAC manifest**

```yaml
# deploy/kubernetes/rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: freebsd-csi-controller
  namespace: kube-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: freebsd-csi-controller
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "delete", "patch"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "update"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["storageclasses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["list", "watch", "create", "update", "patch"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshots"]
    verbs: ["get", "list"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshotcontents"]
    verbs: ["get", "list", "watch", "update", "patch"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["csinodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["volumeattachments"]
    verbs: ["get", "list", "watch", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: freebsd-csi-controller
subjects:
  - kind: ServiceAccount
    name: freebsd-csi-controller
    namespace: kube-system
roleRef:
  kind: ClusterRole
  name: freebsd-csi-controller
  apiGroup: rbac.authorization.k8s.io
```

**Step 2: Create CSI driver deployment manifest**

```yaml
# deploy/kubernetes/csi-driver.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: freebsd-csi-controller
  namespace: kube-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: freebsd-csi-controller
  template:
    metadata:
      labels:
        app: freebsd-csi-controller
    spec:
      serviceAccountName: freebsd-csi-controller
      containers:
        - name: csi-provisioner
          image: registry.k8s.io/sig-storage/csi-provisioner:v4.0.0
          args:
            - --csi-address=/csi/csi.sock
            - --v=5
          volumeMounts:
            - name: socket-dir
              mountPath: /csi

        - name: csi-attacher
          image: registry.k8s.io/sig-storage/csi-attacher:v4.5.0
          args:
            - --csi-address=/csi/csi.sock
            - --v=5
          volumeMounts:
            - name: socket-dir
              mountPath: /csi

        - name: csi-resizer
          image: registry.k8s.io/sig-storage/csi-resizer:v1.10.0
          args:
            - --csi-address=/csi/csi.sock
            - --v=5
          volumeMounts:
            - name: socket-dir
              mountPath: /csi

        - name: csi-snapshotter
          image: registry.k8s.io/sig-storage/csi-snapshotter:v7.0.1
          args:
            - --csi-address=/csi/csi.sock
            - --v=5
          volumeMounts:
            - name: socket-dir
              mountPath: /csi

        - name: freebsd-csi-driver
          image: ghcr.io/ndenev/freebsd-csi-driver:latest
          args:
            - --endpoint=unix:///csi/csi.sock
            - --agent-endpoint=$(AGENT_ENDPOINT)
          env:
            - name: AGENT_ENDPOINT
              value: "ctld-agent.storage.svc.cluster.local:50051"
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
      volumes:
        - name: socket-dir
          emptyDir: {}
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: freebsd-csi-node
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: freebsd-csi-node
  template:
    metadata:
      labels:
        app: freebsd-csi-node
    spec:
      hostNetwork: true
      containers:
        - name: csi-node-driver-registrar
          image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.10.0
          args:
            - --csi-address=/csi/csi.sock
            - --kubelet-registration-path=/var/lib/kubelet/plugins/csi.freebsd.org/csi.sock
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: registration-dir
              mountPath: /registration

        - name: freebsd-csi-driver
          image: ghcr.io/ndenev/freebsd-csi-driver:latest
          args:
            - --endpoint=unix:///csi/csi.sock
            - --agent-endpoint=$(AGENT_ENDPOINT)
            - --node-id=$(NODE_NAME)
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: AGENT_ENDPOINT
              value: "ctld-agent.storage.svc.cluster.local:50051"
          securityContext:
            privileged: true
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: kubelet-dir
              mountPath: /var/lib/kubelet
              mountPropagation: Bidirectional
            - name: dev
              mountPath: /dev
      volumes:
        - name: socket-dir
          hostPath:
            path: /var/lib/kubelet/plugins/csi.freebsd.org
            type: DirectoryOrCreate
        - name: registration-dir
          hostPath:
            path: /var/lib/kubelet/plugins_registry
            type: Directory
        - name: kubelet-dir
          hostPath:
            path: /var/lib/kubelet
            type: Directory
        - name: dev
          hostPath:
            path: /dev
            type: Directory
```

**Step 3: Create StorageClass manifest**

```yaml
# deploy/kubernetes/storageclass.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fsType: ufs
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-nvmeof
provisioner: csi.freebsd.org
parameters:
  exportType: nvmeof
  fsType: ufs
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**Step 4: Commit**

```bash
git add deploy/
git commit -m "feat: add Kubernetes deployment manifests"
```

---

## Phase 7: Testing & Documentation

### Task 12: Add Integration Tests

**Files:**
- Create: `ctld-agent/tests/integration_test.rs`
- Create: `csi-driver/pkg/driver/driver_test.go`

(Test implementations would follow TDD approach - write failing tests first)

### Task 13: Create Documentation

**Files:**
- Modify: `README.md`
- Create: `docs/installation.md`
- Create: `docs/configuration.md`

---

## Summary

This plan provides a complete implementation path for the FreeBSD CSI project:

1. **Phases 1-4**: Rust daemon (`ctld-agent`) for ZFS volume management and CTL exports
2. **Phase 5**: Rust CSI driver implementing the Container Storage Interface
3. **Phase 6**: Kubernetes deployment manifests
4. **Phase 7**: Testing and documentation

**Key architectural decisions:**
- Pure Rust implementation for both daemon and CSI driver
- gRPC for daemon<->CSI communication (efficient, strongly-typed)
- Separate concerns: ZFS operations, CTL iSCSI, CTL NVMeoF as independent modules
- Support both iSCSI and NVMeoF from day one
- Standard CSI sidecars for Kubernetes integration
- Unix socket for CSI driver communication with kubelet

**Dependencies:**
- FreeBSD 13+ (CTL with NVMeoF support)
- ZFS
- Rust 1.75+
- Kubernetes 1.28+
