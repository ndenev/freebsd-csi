# Snapshot/Clone Dependency Handling

**Date:** 2025-01-26
**Status:** Draft
**Author:** Design session with Claude

## Problem

ZFS snapshots and clones have inherent dependencies that conflict with Kubernetes' independent resource model:

```
ZFS constraints:
- Can't delete snapshot if it has clones
- Can't delete volume if it has snapshots
- Clone depends on snapshot depends on volume

Kubernetes expectation:
- PVC and VolumeSnapshot are independent resources
- User can delete in any order
```

**Current behavior:** When dependencies prevent deletion, ZFS fails silently, CSI returns a generic error, and Kubernetes retries forever. Users have no visibility into what's blocking the operation.

## Solution

Return explicit `FAILED_PRECONDITION` errors with clear, actionable messages. This is explicitly allowed by the CSI spec for drivers that don't treat volumes and snapshots as independent.

### CSI Spec Reference

From the [CSI Specification](https://github.com/container-storage-interface/spec/blob/master/spec.md):

> "When a Controller Plugin does not support deleting a volume without affecting its existing snapshots, then the volume MUST NOT be altered in any way by the request and the operation must return the `FAILED_PRECONDITION` error code and MAY include meaningful human-readable information in the `status.message` field."

## Design

### DeleteSnapshot Changes

**Before attempting delete, check if snapshot has clones:**

```rust
// New function in dataset.rs
pub async fn snapshot_has_clones(&self, snapshot_path: &str) -> Result<Vec<String>>
```

**Modified flow:**

```
DeleteSnapshot(snapshot_id)
│
├─ Parse snapshot_id → (volume_name, snap_name)
├─ Build snapshot path
│
├─ Check: Does snapshot have clones?
│   │
│   ├─ YES → Return FAILED_PRECONDITION:
│   │        "Cannot delete snapshot 'X': N PVC(s) were restored from it: [A, B].
│   │         Delete those PVCs first, then retry."
│   │
│   └─ NO → Proceed with delete
│
├─ If not found at expected path:
│   └─ Search by user:csi:snapshot_id property (existing logic)
│       └─ If found, check clones at new location too
│
└─ Delete snapshot
```

### DeleteVolume Changes

**Keep existing auto-promote logic** (helps with simple single-clone cases).

**Improve error messages** for the FAILED_PRECONDITION on snapshots:

```
"Cannot delete volume 'X': N VolumeSnapshot(s) exist: [snap1, snap2].
 Delete those VolumeSnapshots first.
 Note: If snapshots have dependent PVCs, delete those PVCs first."
```

### Implementation Details

#### New Function: `snapshot_has_clones`

```rust
// In ctld-agent/src/zfs/dataset.rs

/// Check if a snapshot has any clones.
/// Returns list of clone dataset paths.
pub async fn snapshot_has_clones(&self, snapshot_path: &str) -> Result<Vec<String>> {
    let output = Command::new("zfs")
        .args(["get", "-H", "-o", "value", "clones", snapshot_path])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("does not exist") {
            return Err(ZfsError::DatasetNotFound(snapshot_path.to_string()));
        }
        return Err(ZfsError::CommandFailed(format!(
            "failed to get clones: {}", stderr
        )));
    }

    let clones_str = String::from_utf8_lossy(&output.stdout).trim().to_string();

    // "-" means no clones
    if clones_str == "-" || clones_str.is_empty() {
        return Ok(Vec::new());
    }

    // Clones are comma-separated
    Ok(clones_str.split(',').map(|s| s.trim().to_string()).collect())
}
```

#### Modified DeleteSnapshot

```rust
// In ctld-agent/src/service/storage.rs, delete_snapshot method

// After parsing snapshot_id into volume_name and snap_name:

let snapshot_path = format!("{}@{}", zfs.full_path(volume_name), snap_name);

// Check for clones BEFORE attempting delete
match zfs.snapshot_has_clones(&snapshot_path).await {
    Ok(clones) if !clones.is_empty() => {
        let pvc_names: Vec<_> = clones
            .iter()
            .filter_map(|c| c.rsplit('/').next())
            .collect();

        timer.failure("has_clones");
        return Err(Status::failed_precondition(format!(
            "Cannot delete snapshot '{}': {} PVC(s) were restored from it: [{}]. \
             Delete those PVCs first, then retry.",
            req.snapshot_id,
            pvc_names.len(),
            pvc_names.join(", ")
        )));
    }
    Ok(_) => { /* No clones, proceed with deletion */ }
    Err(ZfsError::DatasetNotFound(_)) => {
        // Snapshot not at expected path, fall through to search by ID
    }
    Err(e) => {
        timer.failure("zfs_error");
        return Err(Status::internal(format!(
            "failed to check snapshot clones: {}", e
        )));
    }
}

// ... existing deletion logic ...

// ALSO: After finding snapshot by ID at new path, check clones there too
```

## User Experience

### Error Messages

**Deleting snapshot with clones:**
```
rpc error: code = FailedPrecondition desc = Cannot delete snapshot 'pvc-abc@snap1':
2 PVC(s) were restored from it: [pvc-def, pvc-ghi]. Delete those PVCs first, then retry.
```

**Deleting volume with snapshots:**
```
rpc error: code = FailedPrecondition desc = Cannot delete volume 'pvc-abc':
1 VolumeSnapshot(s) exist: [snap1]. Delete those VolumeSnapshots first.
Note: If snapshots have dependent PVCs, delete those PVCs first.
```

### Correct Deletion Order

Users must delete in dependency order:

```
1. Delete PVCs created from snapshots (clones)
2. Delete VolumeSnapshots
3. Delete source PVC
```

## What We Keep (Existing Smart Handling)

| Feature | Location | Purpose |
|---------|----------|---------|
| Auto-promote clones | `storage.rs` DeleteVolume | Allows source deletion when single clone exists |
| Snapshot ID tracking | `user:csi:snapshot_id` property | Find snapshots after promotion moves them |
| Find by ID | `find_snapshot_by_id()` | Locate promoted snapshots |
| Temp snapshot cleanup | DeleteVolume | Clean up `pvc-clone-*` snapshots |

## What We Add

| Feature | Location | Purpose |
|---------|----------|---------|
| `snapshot_has_clones()` | `dataset.rs` | Check clone dependencies |
| Clone check in DeleteSnapshot | `storage.rs` | FAILED_PRECONDITION before delete |
| Improved error messages | `storage.rs` | Actionable user guidance |

## What We Don't Add (Future Consideration)

For simplicity, this design explicitly does NOT include:

- Materialized snapshots (background copy to holder zvols)
- Automatic dependency resolution (cascading promotes)
- Background garbage collection
- Pending-delete state tracking
- ZFS channel programs for atomic operations

These could be added later if users need transparent "delete in any order" behavior.

## Testing

The existing stress tests in `e2e-tests/stress/` should be updated:

1. **`test_clone_chains.py`** - Verify FAILED_PRECONDITION errors with correct messages
2. **`test_delete_middle_of_chain`** - Verify clear error when deleting B with dependent C
3. **`test_multiple_clones_from_single_snapshot`** - Verify snapshot delete fails listing all clones

New test cases:
- Verify error message contains actual PVC names
- Verify K8s doesn't infinite-retry on FAILED_PRECONDITION
- Verify successful delete after dependencies removed

## Migration

No migration needed. This is purely additive:
- Existing volumes/snapshots work unchanged
- New error handling kicks in only when dependencies exist
- No new metadata or state to track

## Estimated Effort

| Component | Changes | Estimate |
|-----------|---------|----------|
| `dataset.rs` | Add `snapshot_has_clones()` | ~30 lines |
| `storage.rs` | Clone check in DeleteSnapshot | ~40 lines |
| `storage.rs` | Improve DeleteVolume error | ~10 lines |
| Tests | Update e2e stress tests | ~50 lines |
| **Total** | | **~130 lines** |
