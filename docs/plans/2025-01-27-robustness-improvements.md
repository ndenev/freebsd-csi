# Robustness Improvements for ctld-agent

**Date:** 2025-01-27
**Status:** Draft
**Author:** Design session with Claude

## Overview

This document describes improvements to the robustness of the FreeBSD CSI driver, focusing on:

1. Operation atomicity and idempotency (Issues #39, #42)
2. Authentication credential persistence across restarts
3. Clean separation of CSI-managed config from user-managed config
4. Serialized configuration writes to prevent corruption

## Problem Statement

### Current Issues

**Idempotency Gap (Issue #42):** CreateVolume is not fully idempotent. If ZFS volume creation succeeds but UCL config write fails (e.g., CHAP validation error), retries fail with "dataset already exists" instead of returning the existing volume.

**Atomicity Gap (Issue #39):** CreateVolume performs multiple steps (ZFS create, CTL export, HashMap update, UCL write) that can partially fail, leaving inconsistent state.

**Auth Persistence:** CHAP credentials are written to UCL config but not persisted in a way that survives agent restart. ZFS metadata cannot store credentials securely (world-readable via `zfs get`).

**Config Corruption Risk:** Current marker-based approach (`# BEGIN CSI-MANAGED`) for separating CSI and user config is fragile - users could corrupt markers, and there's potential for race conditions during config updates.

## Solution Architecture

### Config File Separation

Instead of marker-based sections in a shared config file, we use complete separation:

```
/etc/ctl.conf (user's config - we add ONE include line, never parse/modify)
├── portal-group pg0 { ... }
├── transport-group tg0 { ... }
├── # User's manual targets
└── .include "/var/db/ctld-agent/csi-targets.conf"

/var/db/ctld-agent/
├── csi-targets.conf      (0644) - Fully regenerated UCL config
├── auth.json             (0600) - Credentials source of truth
└── state/                        - Optional future state files
```

**Benefits:**
- No marker parsing, no corruption risk
- `csi-targets.conf` is regenerated completely each time - atomic and idempotent
- User can edit `/etc/ctl.conf` freely - we never parse or modify it (except one-time include setup)
- `auth.json` is a secure persistent store readable only by root

### Credential Storage (auth.json)

JSON format with serde serialization:

```json
{
  "pvc-abc123": {
    "user": "initiator",
    "secret": "password:with:special:chars",
    "mutual_user": "target",
    "mutual_secret": "target-password"
  },
  "pvc-def456": {
    "user": "user2",
    "secret": "simple-secret"
  }
}
```

**Rust types:**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChapCredentials {
    pub user: String,
    pub secret: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutual_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutual_secret: Option<String>,
}

pub type AuthDb = HashMap<String, ChapCredentials>;
```

**File permissions:** `0600 root:wheel` - only root can read credentials.

**Lifecycle:**
- Read on startup to rebuild in-memory auth state
- Updated atomically (temp file + rename) when volumes with auth are created/deleted

### UCL Include Requirement

ctld must run in UCL mode for `.include` directives to work:

```bash
sysrc ctld_flags="-u"
```

The `.include` directive is handled by libucl (built into ctld), which reads included files with ctld's privileges (root).

### Validate-First Strategy

Before creating any ZFS resources, validate ALL inputs in code:

```
CreateVolume(params)
│
├─ Phase 1: VALIDATE ALL INPUTS (no state changes, no external calls)
│   ├─ Validate volume name format
│   ├─ Validate size constraints
│   ├─ Validate CHAP credentials format (no forbidden chars: " { } \)
│   └─ If any fail → return error immediately (no ZFS touched)
│
├─ Phase 2: CREATE WITH RECOVERY
│   ├─ Create ZFS zvol
│   │   └─ On "already exists" → jump to RECOVERY
│   ├─ Set ZFS metadata properties
│   ├─ Update auth.json (if CHAP enabled)
│   ├─ Update in-memory export state
│   ├─ Queue config write (regenerate UCL, reload ctld)
│   │   └─ On ctld reload failure → return error with details
│   └─ Return success
│
└─ RECOVERY (on "already exists"):
    ├─ Read existing volume metadata from ZFS
    ├─ Compare requested params vs existing
    │   ├─ Match → return success with existing volume info
    │   └─ Mismatch → return ALREADY_EXISTS error
    └─ If volume exists but not exported → re-export it
```

**Why no dry-run with `ctld -t`:** We generate the UCL config ourselves from validated inputs, so syntax errors are bugs in our code, not user input. If ctld reload fails, we surface that error clearly. This avoids the complexity of merging configs for validation and the latency overhead on every operation.

### Serialized Config Writer

Single background task owns all file writes, preventing corruption from concurrent access:

```rust
/// Unified config writer handling both auth.json and csi-targets.conf
pub struct ConfigWriter {
    auth_path: PathBuf,        // /var/db/ctld-agent/auth.json
    config_path: PathBuf,      // /var/db/ctld-agent/csi-targets.conf
    ctl_manager: Arc<RwLock<CtlManager>>,
    auth_db: AuthDb,           // In-memory auth state
}

impl ConfigWriter {
    /// Write request handler - called from background task
    async fn handle_write(&mut self) -> Result<()> {
        // 1. Write auth.json atomically
        self.write_auth_json().await?;

        // 2. Regenerate csi-targets.conf from ctl_manager + auth_db
        let config_content = self.generate_csi_config().await?;

        // 3. Atomic write csi-targets.conf
        self.write_config_atomic(&config_content).await?;

        // 4. Reload ctld
        self.reload_ctld().await?;

        Ok(())
    }

    async fn write_auth_json(&self) -> Result<()> {
        let content = serde_json::to_string_pretty(&self.auth_db)?;
        atomic_write(&self.auth_path, &content, 0o600).await
    }
}
```

**Existing debounce pattern preserved:** Multiple write requests within debounce window are batched into single write operation. All waiters receive the result.

### Startup Reconciliation

On agent startup:

```
1. Load auth.json
   └─ If missing/corrupt → start with empty auth, log warning

2. Scan ZFS volumes with org.freebsd.csi:managed=true
   └─ Rebuild in-memory volume state

3. For each volume:
   ├─ Check if auth entry exists in auth.json
   │   └─ If has_auth=true in ZFS but missing from auth.json → log warning
   ├─ Check if CTL export exists
   │   └─ If missing → re-export from metadata
   └─ Add to in-memory state

4. Regenerate csi-targets.conf
   └─ Ensures config matches actual state

5. Reload ctld
```

## Portal and Transport Group Management

### User-Managed Groups

Portal groups (iSCSI) and transport groups (NVMeoF) are **user-managed** in `/etc/ctl.conf`. The agent references them by name but does not create or modify them.

**Rationale:**
- User controls network binding (listen addresses, ports)
- User controls discovery auth if desired
- Avoids agent needing network configuration knowledge
- Clear separation of concerns

### Agent Arguments

Rename `--portal-group-name` to `--portal-group` for consistency:

| Argument | Purpose | Required |
|----------|---------|----------|
| `--portal-group` | iSCSI portal group name to reference | Only if serving iSCSI |
| `--transport-group` | NVMeoF transport group name to reference | Only if serving NVMeoF |

### Startup Validation

On startup, the agent validates that referenced groups exist:

```
Agent Startup
│
├─ Load /etc/ctl.conf (parse for group names only)
│
├─ If --portal-group is set:
│   ├─ Check portal-group exists in config
│   └─ If not found → FATAL: "portal-group 'pg0' not found in /etc/ctl.conf"
│
├─ If --transport-group is set:
│   ├─ Check transport-group exists in config
│   └─ If not found → FATAL: "transport-group 'tg0' not found in /etc/ctl.conf"
│
└─ Continue startup...
```

### Request Validation

Requests for a transport type require the corresponding group argument:

| Request | Required Argument | Error if Missing |
|---------|-------------------|------------------|
| CreateVolume (iSCSI) | `--portal-group` | INVALID_ARGUMENT: "iSCSI export requested but --portal-group not configured" |
| CreateVolume (NVMeoF) | `--transport-group` | INVALID_ARGUMENT: "NVMeoF export requested but --transport-group not configured" |

### Discovery Auth (Orthogonal to CSI)

The CSI driver connects directly to targets without using protocol-level discovery:

| Protocol | Discovery Mechanism | Used by CSI? |
|----------|---------------------|--------------|
| iSCSI | SendTargets (`iscsiadm -m discovery`) | **No** - creates node entry directly |
| NVMeoF | Discovery controller (port 8009) | **No** - connects directly to known NQN |

**iSCSI Change (csi-driver):** Replace discovery with direct node creation:

```bash
# OLD: Discovery (fails if discovery auth is enabled)
iscsiadm -m discovery -t sendtargets -p <portal>

# NEW: Direct node creation (no discovery needed)
iscsiadm -m node -T <iqn> -p <portal> --op new
```

Since we already know the target IQN and portal address from the volume context, discovery is unnecessary.

**Implications:**
- `discovery-auth-group` in portal-group has no effect on CSI (users can enable it freely)
- `discovery-tcp` in transport-group has no effect on CSI
- One fewer external command execution per iSCSI connection
- Users can configure discovery auth for manual exploration without affecting CSI

### Example User Configuration

Minimal `/etc/ctl.conf` for CSI:

```
# iSCSI portal group (required for iSCSI volumes)
portal-group pg0 {
    discovery-auth-group = no-authentication
    listen = 0.0.0.0:3260
}

# NVMeoF transport group (required for NVMeoF volumes)
transport-group tg0 {
    listen {
        tcp = 0.0.0.0:4420
    }
}

# Include CSI-managed targets
.include "/var/db/ctld-agent/csi-targets.conf"
```

With discovery auth (optional, does not affect CSI):

```
# Auth group for manual discovery (CSI bypasses this)
auth-group discovery-ag {
    auth-type = chap
    chap { user = "discovery-user"; secret = "discovery-secret"; }
}

portal-group pg0 {
    discovery-auth-group = discovery-ag    # For manual iscsiadm discovery only
    listen = 0.0.0.0:3260
}

transport-group tg0 {
    listen {
        tcp = 0.0.0.0:4420
        discovery-tcp = 0.0.0.0:8009       # For manual nvme discover only
    }
}

.include "/var/db/ctld-agent/csi-targets.conf"
```

## Installation Changes

### Package Post-Install Message

FreeBSD convention is to not modify system configs during package install. Instead, display instructions:

```
===========================================================================

ctld-agent has been installed.

To enable CSI volume management:

1. Ensure ctld runs in UCL mode by adding to /etc/rc.conf:

       sysrc ctld_flags="-u"

   IMPORTANT: UCL mode requires your /etc/ctl.conf to be in UCL format.
   If you have an existing config in the old (non-UCL) format, you must
   convert it first. See ctl.conf(5) for UCL format examples.

   If starting fresh, create a minimal /etc/ctl.conf:

       portal-group pg0 {
           discovery-auth-group = no-authentication
           listen = 0.0.0.0:3260
       }

       .include "/var/db/ctld-agent/csi-targets.conf"

2. If you have an existing UCL config, add this line to /etc/ctl.conf:

       .include "/var/db/ctld-agent/csi-targets.conf"

3. Create the data directory:

       mkdir -p /var/db/ctld-agent
       chmod 700 /var/db/ctld-agent

4. Start or restart the services:

       service ctld restart
       service ctld_agent start

===========================================================================
```

### Documentation Updates

Update `docs/installation.md` with:

1. UCL mode requirement (`ctld_flags="-u"`)
2. Include directive setup
3. Directory permissions
4. Explanation of shared config coexistence
5. Warning: Don't create targets with `iqn.*.csi:` or `nqn.*.csi:` prefixes

## ZFS Metadata Changes

Add one new property to track auth presence:

| Property | Values | Purpose |
|----------|--------|---------|
| `org.freebsd.csi:has_auth` | `"true"` / absent | Volume has CHAP credentials in auth.json |

Credentials themselves are NOT stored in ZFS (security: world-readable).

## Error Handling

### CreateVolume Failures

| Failure Point | Action | User Experience |
|---------------|--------|-----------------|
| Validation (CHAP chars) | Return error immediately | Clear error: "CHAP secret contains invalid characters: \" { } \\ not allowed" |
| Validation (size/name) | Return error immediately | Clear error describing the constraint violation |
| ZFS create (exists) | Recovery: check params | Success if params match, ALREADY_EXISTS if mismatch |
| ZFS create (other) | Return error | "Failed to create volume: ..." |
| Auth.json write | Return error | "Failed to persist credentials: ..." |
| Config write | Return error | "Failed to write config: ..." |
| ctld reload | Return error | "Failed to reload ctld: ..." (volume exists, will work on retry) |

### Startup Failures

| Failure | Action |
|---------|--------|
| auth.json missing | Start with empty auth, log info |
| auth.json corrupt | Start with empty auth, log warning, backup corrupt file |
| auth.json unreadable | Fatal error - permissions issue |
| ZFS scan fails | Fatal error - can't determine state |
| ctld reload fails | Log error, continue - user may need to fix config |

## File Permissions Summary

| Path | Mode | Owner | Purpose |
|------|------|-------|---------|
| `/var/db/ctld-agent/` | 0700 | root:wheel | Agent data directory |
| `/var/db/ctld-agent/auth.json` | 0600 | root:wheel | CHAP credentials |
| `/var/db/ctld-agent/csi-targets.conf` | 0644 | root:wheel | Generated UCL config |
| `/etc/ctl.conf` | 0644 | root:wheel | User's config (we don't modify) |

## Migration

For existing installations:

1. **No automatic migration** - user must manually:
   - Add `.include` line to `/etc/ctl.conf`
   - Enable UCL mode (`ctld_flags="-u"`)

2. **First startup after upgrade:**
   - Agent detects missing `csi-targets.conf`
   - Scans ZFS for existing volumes
   - Generates initial `csi-targets.conf`
   - **Note:** Existing CHAP credentials are NOT migrated (they were in old UCL config, not recoverable)
   - Volumes with CHAP will need credentials re-applied via PVC recreation or manual auth.json edit

3. **Documentation:** Add migration guide section

## Testing

### Unit Tests

- [ ] `auth.json` serialization/deserialization roundtrip
- [ ] UCL config generation with various auth scenarios
- [ ] Validate-first rejects bad CHAP characters
- [ ] Recovery logic: existing volume with matching params
- [ ] Recovery logic: existing volume with mismatched params
- [ ] Startup fails if referenced portal-group not found
- [ ] Startup fails if referenced transport-group not found
- [ ] iSCSI request rejected if --portal-group not set
- [ ] NVMeoF request rejected if --transport-group not set

### Integration Tests

- [ ] Atomic file writes don't corrupt on concurrent access
- [ ] Startup reconciliation rebuilds state correctly
- [ ] CHAP credentials survive agent restart
- [ ] ctld reload failure surfaces clear error message

### E2E Tests

- [ ] CreateVolume with CHAP succeeds
- [ ] CreateVolume retry after partial failure succeeds (idempotency)
- [ ] DeleteVolume cleans up auth.json entry
- [ ] Agent restart preserves CHAP-authenticated volumes

## Estimated Effort

### ctld-agent Changes

| Component | Changes | Estimate |
|-----------|---------|----------|
| `auth.rs` (new) | AuthDb type, JSON read/write | ~100 lines |
| `ucl_config.rs` | Refactor to generate full standalone config | ~150 lines |
| `ctl_manager.rs` | Integrate with new config writer | ~80 lines |
| `storage.rs` | Validate-first, recovery logic, transport type checks | ~220 lines |
| `config_writer.rs` | Unified writer with auth support | ~120 lines |
| `main.rs` | Startup validation (portal/transport groups), reconciliation | ~80 lines |
| `args.rs` | Rename `--portal-group-name` to `--portal-group` | ~10 lines |

### csi-driver Changes

| Component | Changes | Estimate |
|-----------|---------|----------|
| `platform/linux.rs` | Replace discovery with direct node creation | ~30 lines |

### Shared

| Component | Changes | Estimate |
|-----------|---------|----------|
| Tests | Unit + integration | ~280 lines |
| Documentation | Installation, migration guide | ~250 lines |
| **Total** | | **~1320 lines** |

## Resolved Questions

1. **Backup strategy for auth.json?**
   - **Decision:** Keep single `.old` backup on each write
   - Atomic write pattern: write `.new` → copy current to `.old` → rename `.new` to current
   - `auth.json` always exists throughout the operation (crash-safe)
   - Simple, provides one-step recovery if needed

2. **Migration from old marker-based config?**
   - **Decision:** Provide a migration helper script
   - Script reads old config, extracts CSI targets/auth, writes `auth.json`
   - Script outputs manual steps for user (add `.include`, enable UCL mode)
   - Not automatic - user runs script explicitly during upgrade
