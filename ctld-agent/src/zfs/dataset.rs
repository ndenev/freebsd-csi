use std::process::Output;
use tokio::process::Command;
use tracing::{debug, info, instrument, warn};

use super::error::{Result, ZfsError};
use super::properties::{
    CURRENT_SCHEMA_VERSION, METADATA_PROPERTY, SNAPSHOT_ID_PROPERTY, VolumeMetadata,
};

/// Result of searching for a snapshot by its CSI snapshot ID
#[derive(Debug)]
pub enum FindSnapshotResult {
    /// Snapshot not found
    NotFound,
    /// Exactly one snapshot found at the given path
    Found(String),
    /// Multiple snapshots found with the same ID (should not happen)
    Ambiguous(usize),
}

/// Information about a CSI snapshot retrieved from ZFS
#[derive(Debug, Clone)]
pub struct CsiSnapshotInfo {
    /// CSI snapshot ID (format: "volume_id@snap_name")
    pub snapshot_id: String,
    /// Source volume ID (parsed from snapshot_id)
    pub source_volume_id: String,
    /// Snapshot name (parsed from snapshot_id)
    pub name: String,
    /// Creation timestamp (Unix seconds)
    pub creation_time: i64,
}

/// Check command output for success or return appropriate error.
///
/// This helper reduces boilerplate for checking command results.
/// It handles common error patterns like "does not exist" and "already exists".
fn check_command_result(output: &Output, context: &str) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Map common error patterns to specific error types
    if stderr.contains("does not exist") || stderr.contains("not found") {
        return Err(ZfsError::DatasetNotFound(context.to_string()));
    }
    if stderr.contains("already exists") {
        return Err(ZfsError::DatasetExists(context.to_string()));
    }
    if stderr.contains("dataset is busy") {
        return Err(ZfsError::DatasetBusy(context.to_string()));
    }

    Err(ZfsError::CommandFailed(format!("{}: {}", context, stderr)))
}

/// Escape a string for safe use in shell commands.
/// Wraps the string in single quotes and escapes any embedded single quotes.
fn shell_escape(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Validate that a name is safe for use in ZFS commands.
/// Only allows alphanumeric characters, underscores, hyphens, and periods.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ZfsError::InvalidName("name cannot be empty".into()));
    }
    if name.contains("..") {
        return Err(ZfsError::InvalidName("path traversal not allowed".into()));
    }
    if !name
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        return Err(ZfsError::InvalidName(format!(
            "invalid characters in name '{}': only alphanumeric, underscore, hyphen, and period allowed",
            name
        )));
    }
    Ok(())
}

/// Serialize metadata into a ZFS property string (key=value format).
fn format_metadata_property(metadata: &VolumeMetadata) -> Result<String> {
    let json = serde_json::to_string(metadata)
        .map_err(|e| ZfsError::ParseError(format!("failed to serialize metadata: {}", e)))?;
    Ok(format!("{}={}", METADATA_PROPERTY, json))
}

/// Represents a ZFS dataset (filesystem or volume)
#[derive(Debug, Clone)]
pub struct Dataset {
    /// Full dataset name (e.g., "tank/csi/vol1")
    pub name: String,
    /// Referenced space in bytes
    pub referenced: u64,
    /// Volume size in bytes (only for zvols)
    pub volsize: Option<u64>,
}

/// Capacity information for the ZFS storage pool/dataset
#[derive(Debug, Clone)]
pub struct Capacity {
    /// Available space in bytes
    pub available: u64,
    /// Used space in bytes
    pub used: u64,
}

/// Manager for ZFS operations under a parent dataset
pub struct ZfsManager {
    /// Parent dataset under which all volumes are created
    parent_dataset: String,
}

impl ZfsManager {
    /// Create a new ZfsManager, verifying the parent dataset exists
    pub async fn new(parent_dataset: String) -> Result<Self> {
        info!(dataset = %parent_dataset, "Initializing ZFS manager");

        // Validate dataset name
        if parent_dataset.is_empty() {
            return Err(ZfsError::InvalidName(
                "dataset name cannot be empty".to_string(),
            ));
        }

        // Verify parent dataset exists
        let output = Command::new("zfs")
            .args(["list", "-H", "-o", "name", &parent_dataset])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") || stderr.contains("not found") {
                return Err(ZfsError::DatasetNotFound(parent_dataset));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        info!(dataset = %parent_dataset, "ZFS manager initialized successfully");
        Ok(Self { parent_dataset })
    }

    /// Get the full dataset path for a volume name
    fn full_path(&self, name: &str) -> String {
        format!("{}/{}", self.parent_dataset, name)
    }

    /// Create a new ZFS volume (zvol) with metadata set atomically
    ///
    /// The metadata is set as a ZFS user property during creation, ensuring
    /// that volumes always have metadata even if the agent crashes after creation.
    ///
    /// Supports thin/thick provisioning via `provisioningMode` parameter:
    /// - "thin" (default): No reservation, space allocated on write
    /// - "thick": Sets refreservation=volsize to guarantee space upfront
    #[instrument(skip(self, metadata))]
    pub async fn create_volume(
        &self,
        name: &str,
        size_bytes: u64,
        metadata: &VolumeMetadata,
    ) -> Result<Dataset> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);

        let metadata_property = format_metadata_property(metadata)?;

        // Check provisioning mode from StorageClass parameters
        let is_thick = metadata
            .parameters
            .get("provisioningMode")
            .map(|v| v.eq_ignore_ascii_case("thick"))
            .unwrap_or(false);

        info!(
            volume = %full_name,
            size_bytes,
            provisioning_mode = if is_thick { "thick" } else { "thin" },
            "Creating ZFS volume with metadata"
        );

        // Build command arguments
        let mut args = vec![
            "create".to_string(),
            "-V".to_string(),
            size_bytes.to_string(),
            "-o".to_string(),
            "volmode=dev".to_string(),
            "-o".to_string(),
            metadata_property,
        ];

        // For thick provisioning, set refreservation to guarantee space
        if is_thick {
            args.push("-o".to_string());
            args.push(format!("refreservation={}", size_bytes));
        }

        args.push(full_name.clone());

        // Create the volume with volmode=dev and metadata set atomically
        // Let zfs create fail if already exists (avoids TOCTOU race)
        let output = Command::new("zfs").args(&args).output().await?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(volume = %full_name, error = %e, "Failed to create volume");
            return Err(e);
        }

        info!(
            volume = %full_name,
            size_bytes,
            provisioning_mode = if is_thick { "thick" } else { "thin" },
            "ZFS volume created successfully with metadata"
        );
        // Return the created dataset info
        self.get_dataset(name).await
    }

    /// Delete a ZFS volume
    ///
    /// This operation is idempotent: if the volume doesn't exist, returns Ok.
    /// Retries on "dataset is busy" errors, which can occur briefly after
    /// unexport while ctld releases the device.
    #[instrument(skip(self))]
    pub async fn delete_volume(&self, name: &str) -> Result<()> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        info!(volume = %full_name, "Deleting ZFS volume");

        // Check if volume exists - if not, deletion is already complete (idempotent)
        if !self.dataset_exists(&full_name).await? {
            info!(volume = %full_name, "Volume already deleted (idempotent)");
            return Ok(());
        }

        // Retry loop for "dataset is busy" errors
        // After unexport, ctld may take a moment to release the zvol device
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 200;

        for attempt in 1..=MAX_RETRIES {
            let output = Command::new("zfs")
                .args(["destroy", &full_name])
                .output()
                .await?;

            match check_command_result(&output, &full_name) {
                Ok(()) => {
                    info!(volume = %full_name, "ZFS volume deleted successfully");
                    return Ok(());
                }
                Err(ZfsError::DatasetBusy(_)) if attempt < MAX_RETRIES => {
                    warn!(
                        volume = %full_name,
                        attempt = attempt,
                        max_retries = MAX_RETRIES,
                        "Dataset busy, retrying after {}ms",
                        RETRY_DELAY_MS
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                }
                Err(e) => {
                    warn!(volume = %full_name, error = %e, "Failed to delete volume");
                    return Err(e);
                }
            }
        }

        // Should not reach here, but satisfy the compiler
        Err(ZfsError::DatasetBusy(full_name))
    }

    /// Resize a ZFS volume
    #[instrument(skip(self))]
    pub async fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<()> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        info!(volume = %full_name, new_size_bytes, "Resizing ZFS volume");

        // Check if volume exists
        if !self.dataset_exists(&full_name).await? {
            warn!(volume = %full_name, "Volume not found for resize");
            return Err(ZfsError::DatasetNotFound(full_name));
        }

        let output = Command::new("zfs")
            .args(["set", &format!("volsize={}", new_size_bytes), &full_name])
            .output()
            .await?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(volume = %full_name, error = %e, "Failed to resize volume");
            return Err(e);
        }

        info!(volume = %full_name, new_size_bytes, "ZFS volume resized successfully");
        Ok(())
    }

    /// Create a snapshot of a volume
    ///
    /// The snapshot is tagged with a `user:csi:snapshot_id` property containing
    /// the CSI snapshot ID (format: "volume_name@snap_name"). This property
    /// persists even if the snapshot is moved due to clone promotion, allowing
    /// us to find and delete the snapshot regardless of its current location.
    #[instrument(skip(self))]
    pub async fn create_snapshot(&self, volume_name: &str, snap_name: &str) -> Result<String> {
        // Validate names for command injection prevention
        validate_name(volume_name)?;
        validate_name(snap_name)?;

        let full_volume = self.full_path(volume_name);
        let snapshot_path = format!("{}@{}", full_volume, snap_name);
        // CSI snapshot ID uses the volume name (not full path) for portability
        let snapshot_id = format!("{}@{}", volume_name, snap_name);
        info!(volume = %full_volume, snapshot = %snap_name, snapshot_id = %snapshot_id, "Creating ZFS snapshot");

        // Check if volume exists
        if !self.dataset_exists(&full_volume).await? {
            warn!(volume = %full_volume, "Volume not found for snapshot");
            return Err(ZfsError::DatasetNotFound(full_volume));
        }

        // Create snapshot with CSI snapshot ID property set atomically
        let property_arg = format!(
            "{}={}",
            super::properties::SNAPSHOT_ID_PROPERTY,
            snapshot_id
        );
        let output = Command::new("zfs")
            .args(["snapshot", "-o", &property_arg, &snapshot_path])
            .output()
            .await?;

        if let Err(e) = check_command_result(&output, &snapshot_path) {
            warn!(snapshot = %snapshot_path, error = %e, "Failed to create snapshot");
            return Err(e);
        }

        info!(snapshot = %snapshot_path, snapshot_id = %snapshot_id, "ZFS snapshot created successfully");
        Ok(snapshot_path)
    }

    /// Delete a snapshot
    #[instrument(skip(self))]
    pub async fn delete_snapshot(&self, volume_name: &str, snap_name: &str) -> Result<()> {
        // Validate both parts
        validate_name(volume_name)?;
        validate_name(snap_name)?;

        let full_name = format!("{}@{}", self.full_path(volume_name), snap_name);
        info!(snapshot = %full_name, "Deleting ZFS snapshot");

        let output = Command::new("zfs")
            .args(["destroy", &full_name])
            .output()
            .await?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(snapshot = %full_name, error = %e, "Failed to delete snapshot");
            return Err(e);
        }

        info!(snapshot = %full_name, "ZFS snapshot deleted successfully");
        Ok(())
    }

    /// List all snapshots for a specific volume
    ///
    /// Returns snapshot names (without the volume@ prefix) for the given volume.
    /// This is used to check for dependent snapshots before volume deletion.
    #[instrument(skip(self))]
    pub async fn list_snapshots_for_volume(&self, volume_name: &str) -> Result<Vec<String>> {
        validate_name(volume_name)?;

        let full_name = self.full_path(volume_name);
        debug!(volume = %full_name, "Listing snapshots for volume");

        // Check if volume exists first
        if !self.dataset_exists(&full_name).await? {
            // Volume doesn't exist, so no snapshots
            return Ok(Vec::new());
        }

        let output = Command::new("zfs")
            .args([
                "list", "-H", "-t", "snapshot", "-o", "name", "-r", "-d",
                "1", // Only direct snapshots, not nested
                &full_name,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // If no snapshots exist, zfs list may return error or empty
            if stderr.contains("does not exist") || stderr.contains("no datasets available") {
                return Ok(Vec::new());
            }
            warn!(volume = %full_name, error = %stderr, "Failed to list snapshots");
            return Err(ZfsError::CommandFailed(format!(
                "failed to list snapshots: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let prefix = format!("{}@", full_name);

        let snapshots: Vec<String> = stdout
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| {
                // Extract just the snapshot name (after the @)
                line.strip_prefix(&prefix).map(|s| s.to_string())
            })
            .collect();

        debug!(volume = %full_name, count = snapshots.len(), "Found snapshots");
        Ok(snapshots)
    }

    /// Find a snapshot by its CSI snapshot ID property
    ///
    /// This searches all snapshots under the parent dataset for one with the
    /// matching `user:csi:snapshot_id` property. This is used to find snapshots
    /// that may have moved due to clone promotion.
    ///
    /// Returns:
    /// - `NotFound` if no snapshot has the given ID
    /// - `Found(path)` if exactly one snapshot matches
    /// - `Ambiguous(count)` if multiple snapshots match (should not happen with UUIDs)
    #[instrument(skip(self))]
    pub async fn find_snapshot_by_id(&self, snapshot_id: &str) -> Result<FindSnapshotResult> {
        debug!(snapshot_id = %snapshot_id, "Searching for snapshot by CSI ID");

        // List all snapshots with their CSI snapshot ID property
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-t",
                "snapshot",
                "-o",
                &format!("name,{}", SNAPSHOT_ID_PROPERTY),
                "-r",
                &self.parent_dataset,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("no datasets available") {
                return Ok(FindSnapshotResult::NotFound);
            }
            return Err(ZfsError::CommandFailed(format!(
                "failed to search snapshots: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let matches: Vec<&str> = stdout
            .lines()
            .filter_map(|line| {
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() >= 2 && parts[1] == snapshot_id {
                    Some(parts[0])
                } else {
                    None
                }
            })
            .collect();

        match matches.len() {
            0 => {
                debug!(snapshot_id = %snapshot_id, "Snapshot not found");
                Ok(FindSnapshotResult::NotFound)
            }
            1 => {
                let path = matches[0].to_string();
                info!(snapshot_id = %snapshot_id, path = %path, "Found snapshot by CSI ID");
                Ok(FindSnapshotResult::Found(path))
            }
            n => {
                warn!(
                    snapshot_id = %snapshot_id,
                    count = n,
                    "Multiple snapshots found with same CSI ID - refusing to delete"
                );
                Ok(FindSnapshotResult::Ambiguous(n))
            }
        }
    }

    /// List all ZFS snapshots with CSI metadata
    ///
    /// This queries ZFS for all snapshots under the parent dataset that have the
    /// `user:csi:snapshot_id` property set. Returns snapshot information including
    /// the snapshot ID, source volume ID, name, and creation time.
    ///
    /// This is used by ListSnapshots to query ZFS directly instead of relying on
    /// an in-memory cache, ensuring the list survives restarts and always reflects
    /// the actual ZFS state.
    #[instrument(skip(self))]
    pub async fn list_csi_snapshots(&self) -> Result<Vec<CsiSnapshotInfo>> {
        debug!("Listing all CSI snapshots");

        // List all snapshots with their CSI snapshot ID property and creation time
        // Format: name<TAB>user:csi:snapshot_id<TAB>creation
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-t",
                "snapshot",
                "-o",
                &format!("name,{},creation", SNAPSHOT_ID_PROPERTY),
                "-r",
                &self.parent_dataset,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("no datasets available") {
                return Ok(Vec::new());
            }
            return Err(ZfsError::CommandFailed(format!(
                "failed to list CSI snapshots: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut snapshots = Vec::new();

        for line in stdout.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 3 {
                continue;
            }

            let _zfs_name = parts[0];
            let snapshot_id = parts[1];
            let creation_str = parts[2];

            // Skip snapshots without a CSI snapshot ID (indicated by "-" in ZFS output)
            if snapshot_id == "-" || snapshot_id.is_empty() {
                continue;
            }

            // Parse the snapshot ID to extract source_volume_id and name
            // Format: "volume_id@snap_name"
            let (source_volume_id, name) = match snapshot_id.split_once('@') {
                Some((vol, snap)) => (vol.to_string(), snap.to_string()),
                None => {
                    warn!(snapshot_id = %snapshot_id, "Invalid snapshot ID format, skipping");
                    continue;
                }
            };

            // Parse creation time - ZFS returns it in a human-readable format
            // We need to convert it to Unix timestamp
            let creation_time = Self::parse_zfs_creation_time(creation_str).await;

            snapshots.push(CsiSnapshotInfo {
                snapshot_id: snapshot_id.to_string(),
                source_volume_id,
                name,
                creation_time,
            });
        }

        debug!(count = snapshots.len(), "Found CSI snapshots");
        Ok(snapshots)
    }

    /// Parse ZFS creation time to Unix timestamp
    ///
    /// ZFS returns creation time in a locale-dependent format like:
    /// "Sat Jan 25 12:34:56 2025" or similar
    /// We use the `date` command to parse it robustly.
    async fn parse_zfs_creation_time(creation_str: &str) -> i64 {
        // Use date command to parse the ZFS timestamp
        let output = Command::new("date")
            .args(["-j", "-f", "%a %b %d %H:%M %Y", creation_str, "+%s"])
            .output()
            .await;

        match output {
            Ok(out) if out.status.success() => {
                let timestamp_str = String::from_utf8_lossy(&out.stdout);
                timestamp_str.trim().parse().unwrap_or(0)
            }
            _ => {
                // Fallback: try a simpler parse or return 0
                // ZFS on some systems may use different formats
                0
            }
        }
    }

    /// Delete a snapshot by its full ZFS path
    ///
    /// This is a lower-level method that takes the full path (e.g., "tank/csi/vol@snap")
    /// rather than separate volume and snapshot names.
    #[instrument(skip(self))]
    pub async fn delete_snapshot_by_path(&self, snapshot_path: &str) -> Result<()> {
        info!(snapshot = %snapshot_path, "Deleting ZFS snapshot by path");

        let output = Command::new("zfs")
            .args(["destroy", snapshot_path])
            .output()
            .await?;

        if let Err(e) = check_command_result(&output, snapshot_path) {
            warn!(snapshot = %snapshot_path, error = %e, "Failed to delete snapshot");
            return Err(e);
        }

        info!(snapshot = %snapshot_path, "ZFS snapshot deleted successfully");
        Ok(())
    }

    /// Get information about a specific dataset
    pub async fn get_dataset(&self, name: &str) -> Result<Dataset> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        self.get_dataset_info(&full_name).await
    }

    /// List all volumes under the parent dataset
    pub async fn list_volumes(&self) -> Result<Vec<Dataset>> {
        debug!(parent = %self.parent_dataset, "Listing volumes");

        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-p", // Machine-parseable output (bytes)
                "-t",
                "volume",
                "-r",
                "-o",
                "name,refer,volsize",
                &self.parent_dataset,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(error = %stderr, "Failed to list volumes");
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut datasets = Vec::new();

        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let dataset = self.parse_dataset_line(line)?;
            // Only include direct children (volumes under our parent)
            if dataset.name.starts_with(&self.parent_dataset) && dataset.name != self.parent_dataset
            {
                datasets.push(dataset);
            }
        }

        debug!(count = datasets.len(), "Found volumes");
        Ok(datasets)
    }

    /// Get the device path for a volume
    pub fn get_device_path(&self, name: &str) -> String {
        let full_name = self.full_path(name);
        format!("/dev/zvol/{}", full_name)
    }

    /// Save volume metadata to ZFS user property
    ///
    /// Note: This is primarily for recovery/repair scenarios. Normal volume creation
    /// sets metadata atomically via create_volume/clone_from_snapshot/copy_from_snapshot.
    #[allow(dead_code)]
    #[instrument(skip(self, metadata))]
    pub async fn set_volume_metadata(&self, name: &str, metadata: &VolumeMetadata) -> Result<()> {
        validate_name(name)?;
        let json = serde_json::to_string(metadata)
            .map_err(|e| ZfsError::ParseError(format!("failed to serialize metadata: {}", e)))?;

        let full_name = self.full_path(name);
        debug!(volume = %full_name, "Setting volume metadata");

        let property = format!("{}={}", METADATA_PROPERTY, json);

        let output = Command::new("zfs")
            .args(["set", &property, &full_name])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(volume = %full_name, error = %stderr, "Failed to set volume metadata");
            return Err(ZfsError::CommandFailed(format!(
                "failed to set metadata: {}",
                stderr
            )));
        }

        debug!(volume = %full_name, "Volume metadata saved");
        Ok(())
    }

    /// Clear volume metadata (on deletion)
    #[instrument(skip(self))]
    pub async fn clear_volume_metadata(&self, name: &str) -> Result<()> {
        validate_name(name)?;
        let full_name = self.full_path(name);

        // Use 'inherit' to remove user property
        let output = Command::new("zfs")
            .args(["inherit", METADATA_PROPERTY, &full_name])
            .output()
            .await?;

        // Ignore errors - property might not exist
        let _ = output;
        Ok(())
    }

    /// List all volumes with CSI metadata (for startup recovery)
    #[instrument(skip(self))]
    pub async fn list_volumes_with_metadata(&self) -> Result<Vec<(String, VolumeMetadata)>> {
        info!(parent = %self.parent_dataset, "Scanning for volumes with CSI metadata");

        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-r",
                "-t",
                "volume",
                "-o",
                &format!("name,{}", METADATA_PROPERTY),
                &self.parent_dataset,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(error = %stderr, "Failed to list volumes with metadata");
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut results = Vec::new();

        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 {
                continue;
            }

            let name = parts[0];
            let metadata_json = parts[1];

            // Skip parent dataset itself
            if name == self.parent_dataset {
                continue;
            }

            // Skip volumes without metadata
            if metadata_json.is_empty() || metadata_json == "-" {
                debug!(volume = %name, "Volume has no CSI metadata, skipping");
                continue;
            }

            // Parse metadata
            // Extract volume name (remove parent prefix)
            let vol_name = name
                .strip_prefix(&format!("{}/", self.parent_dataset))
                .unwrap_or(name)
                .to_string();

            match serde_json::from_str::<VolumeMetadata>(metadata_json) {
                Ok(mut metadata) => {
                    // Reject metadata from future versions we don't understand
                    if metadata.schema_version > CURRENT_SCHEMA_VERSION {
                        warn!(
                            volume = %vol_name,
                            metadata_version = metadata.schema_version,
                            supported_version = CURRENT_SCHEMA_VERSION,
                            "Metadata version too new, skipping (upgrade ctld-agent to manage this volume)"
                        );
                        continue;
                    }

                    // Migrate old metadata formats to current version and persist
                    if metadata.needs_migration() {
                        let from_version = metadata.schema_version;
                        metadata.migrate();
                        info!(
                            volume = %vol_name,
                            from_version = from_version,
                            to_version = CURRENT_SCHEMA_VERSION,
                            "Migrated metadata schema"
                        );
                        // Persist the migrated metadata back to ZFS
                        if let Err(e) = self.set_volume_metadata(&vol_name, &metadata).await {
                            warn!(
                                volume = %vol_name,
                                error = %e,
                                "Failed to persist migrated metadata (will retry on next scan)"
                            );
                        }
                    }
                    debug!(volume = %vol_name, "Found volume with valid CSI metadata");
                    results.push((vol_name, metadata));
                }
                Err(e) => {
                    warn!(volume = %name, error = %e, "Corrupt CSI metadata, skipping");
                }
            }
        }

        info!(count = results.len(), "Volume scan complete");
        Ok(results)
    }

    /// Clone a volume from an existing snapshot (instant, creates dependency).
    ///
    /// This creates a new volume that shares data blocks with the snapshot.
    /// The clone depends on the snapshot, which depends on the source volume.
    /// Use `promote_clone()` to reverse the dependency if needed.
    ///
    /// Metadata is set atomically during clone creation to ensure crash safety.
    #[instrument(skip(self, metadata))]
    pub async fn clone_from_snapshot(
        &self,
        source_volume: &str,
        snap_name: &str,
        target_volume: &str,
        metadata: &VolumeMetadata,
    ) -> Result<Dataset> {
        validate_name(source_volume)?;
        validate_name(snap_name)?;
        validate_name(target_volume)?;

        let snapshot_full = format!("{}@{}", self.full_path(source_volume), snap_name);
        let target_full = self.full_path(target_volume);
        let metadata_property = format_metadata_property(metadata)?;

        info!(
            snapshot = %snapshot_full,
            target = %target_full,
            "Cloning volume from snapshot with metadata"
        );

        // Verify snapshot exists
        let snap_check = Command::new("zfs")
            .args(["list", "-H", "-t", "snapshot", &snapshot_full])
            .output()
            .await?;

        if !snap_check.status.success() {
            warn!(snapshot = %snapshot_full, "Snapshot not found for clone");
            return Err(ZfsError::DatasetNotFound(snapshot_full));
        }

        // Create the clone with metadata set atomically
        let output = Command::new("zfs")
            .args([
                "clone",
                "-o",
                &metadata_property,
                &snapshot_full,
                &target_full,
            ])
            .output()
            .await?;

        if let Err(e) = check_command_result(&output, &target_full) {
            warn!(
                snapshot = %snapshot_full,
                target = %target_full,
                error = %e,
                "Failed to create clone"
            );
            return Err(e);
        }

        info!(
            snapshot = %snapshot_full,
            target = %target_full,
            "Clone created successfully with metadata"
        );

        self.get_dataset(target_volume).await
    }

    /// Copy a volume from a snapshot using zfs send/recv (slow, independent).
    ///
    /// This creates a fully independent volume with no dependencies.
    /// The data is physically copied, so this takes time proportional to volume size.
    ///
    /// Metadata is set atomically during receive to ensure crash safety.
    #[instrument(skip(self, metadata))]
    pub async fn copy_from_snapshot(
        &self,
        source_volume: &str,
        snap_name: &str,
        target_volume: &str,
        metadata: &VolumeMetadata,
    ) -> Result<Dataset> {
        validate_name(source_volume)?;
        validate_name(snap_name)?;
        validate_name(target_volume)?;

        let snapshot_full = format!("{}@{}", self.full_path(source_volume), snap_name);
        let target_full = self.full_path(target_volume);
        let metadata_property = format_metadata_property(metadata)?;

        info!(
            snapshot = %snapshot_full,
            target = %target_full,
            "Copying volume from snapshot via send/recv with metadata"
        );

        // Verify snapshot exists
        let snap_check = Command::new("zfs")
            .args(["list", "-H", "-t", "snapshot", &snapshot_full])
            .output()
            .await?;

        if !snap_check.status.success() {
            warn!(snapshot = %snapshot_full, "Snapshot not found for copy");
            return Err(ZfsError::DatasetNotFound(snapshot_full));
        }

        // Use zfs send | zfs recv pipeline with metadata property
        // We use sh -c to pipe the commands together
        // Note: zfs recv -o sets properties on the received dataset
        let pipeline = format!(
            "zfs send {} | zfs recv -o {} {}",
            shell_escape(&snapshot_full),
            shell_escape(&metadata_property),
            shell_escape(&target_full)
        );

        let output = Command::new("sh").args(["-c", &pipeline]).output().await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                snapshot = %snapshot_full,
                target = %target_full,
                error = %stderr,
                "Failed to copy volume via send/recv"
            );

            if stderr.contains("already exists") {
                return Err(ZfsError::DatasetExists(target_full));
            }
            return Err(ZfsError::CommandFailed(format!(
                "send/recv failed: {}",
                stderr
            )));
        }

        info!(
            snapshot = %snapshot_full,
            target = %target_full,
            "Volume copied successfully"
        );

        // After recv, we need to destroy the received snapshot to clean up
        // The recv creates <target>@<snap_name>
        let received_snap = format!("{}@{}", target_full, snap_name);
        let destroy_output = Command::new("zfs")
            .args(["destroy", &received_snap])
            .output()
            .await?;

        if !destroy_output.status.success() {
            // Log but don't fail - the volume was created successfully
            let stderr = String::from_utf8_lossy(&destroy_output.stderr);
            warn!(
                snapshot = %received_snap,
                error = %stderr,
                "Failed to clean up received snapshot"
            );
        }

        self.get_dataset(target_volume).await
    }

    /// List clones that depend on snapshots of a volume.
    ///
    /// Returns a list of (snapshot_name, clone_name) tuples for all clones
    /// that depend on snapshots of the specified volume.
    #[instrument(skip(self))]
    pub async fn list_clones_for_volume(&self, volume_name: &str) -> Result<Vec<(String, String)>> {
        validate_name(volume_name)?;

        let full_name = self.full_path(volume_name);
        debug!(volume = %full_name, "Listing clones for volume");

        // Get all snapshots for this volume with their clones property
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-t",
                "snapshot",
                "-o",
                "name,clones",
                "-r",
                "-d",
                "1",
                &full_name,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") {
                return Ok(Vec::new());
            }
            return Err(ZfsError::CommandFailed(format!(
                "failed to list clones: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut results = Vec::new();

        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() < 2 {
                continue;
            }

            let snap_full = parts[0];
            let clones = parts[1];

            // Skip if no clones (shown as "-")
            if clones == "-" || clones.is_empty() {
                continue;
            }

            // Extract snapshot name (after @)
            let snap_name = snap_full
                .rsplit('@')
                .next()
                .unwrap_or(snap_full)
                .to_string();

            // Clones are comma-separated
            for clone in clones.split(',') {
                let clone = clone.trim();
                if !clone.is_empty() {
                    results.push((snap_name.clone(), clone.to_string()));
                }
            }
        }

        debug!(volume = %full_name, count = results.len(), "Found clones");
        Ok(results)
    }

    /// Promote a clone to become the origin (reverses dependency).
    ///
    /// After promotion, the original parent becomes dependent on this clone.
    /// This allows deleting the original parent volume.
    #[instrument(skip(self))]
    pub async fn promote_clone(&self, clone_name: &str) -> Result<()> {
        validate_name(clone_name)?;

        let full_name = self.full_path(clone_name);
        info!(clone = %full_name, "Promoting clone");

        let output = Command::new("zfs")
            .args(["promote", &full_name])
            .output()
            .await?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(clone = %full_name, error = %e, "Failed to promote clone");
            return Err(e);
        }

        info!(clone = %full_name, "Clone promoted successfully");
        Ok(())
    }

    /// Get the origin snapshot of a clone, if any.
    ///
    /// Returns None if the dataset is not a clone.
    #[instrument(skip(self))]
    pub async fn get_origin(&self, name: &str) -> Result<Option<String>> {
        validate_name(name)?;

        let full_name = self.full_path(name);

        let output = Command::new("zfs")
            .args(["get", "-H", "-o", "value", "origin", &full_name])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") {
                return Err(ZfsError::DatasetNotFound(full_name));
            }
            return Err(ZfsError::CommandFailed(format!(
                "failed to get origin: {}",
                stderr
            )));
        }

        let origin = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // "-" means no origin (not a clone)
        if origin == "-" || origin.is_empty() {
            Ok(None)
        } else {
            Ok(Some(origin))
        }
    }

    /// Check if a snapshot has any clones.
    ///
    /// Returns a list of clone dataset paths that depend on this snapshot.
    /// Used to check dependencies before deleting a snapshot.
    #[instrument(skip(self))]
    pub async fn snapshot_has_clones(
        &self,
        volume_name: &str,
        snap_name: &str,
    ) -> Result<Vec<String>> {
        validate_name(volume_name)?;
        validate_name(snap_name)?;

        let snapshot_path = format!("{}@{}", self.full_path(volume_name), snap_name);
        self.snapshot_has_clones_by_path(&snapshot_path).await
    }

    /// Check if a snapshot (by full path) has any clones.
    ///
    /// This variant is used when the snapshot path is already known
    /// (e.g., when found via find_snapshot_by_id after promotion).
    #[instrument(skip(self))]
    pub async fn snapshot_has_clones_by_path(&self, snapshot_path: &str) -> Result<Vec<String>> {
        debug!(snapshot = %snapshot_path, "Checking for clones");

        let output = Command::new("zfs")
            .args(["get", "-H", "-o", "value", "clones", snapshot_path])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") || stderr.contains("not found") {
                return Err(ZfsError::DatasetNotFound(snapshot_path.to_string()));
            }
            return Err(ZfsError::CommandFailed(format!(
                "failed to get clones for {}: {}",
                snapshot_path, stderr
            )));
        }

        let clones_str = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // "-" means no clones
        if clones_str == "-" || clones_str.is_empty() {
            debug!(snapshot = %snapshot_path, "No clones found");
            return Ok(Vec::new());
        }

        // Clones are comma-separated
        let clones: Vec<String> = clones_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        debug!(snapshot = %snapshot_path, clone_count = clones.len(), "Found clones");
        Ok(clones)
    }

    /// Get capacity information for the parent dataset.
    ///
    /// Returns available and used space for the dataset that holds CSI volumes.
    #[instrument(skip(self))]
    pub async fn get_capacity(&self) -> Result<Capacity> {
        debug!(dataset = %self.parent_dataset, "Getting capacity");

        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-p", // Machine-parseable output (bytes)
                "-o",
                "available,used",
                &self.parent_dataset,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") || stderr.contains("not found") {
                return Err(ZfsError::DatasetNotFound(self.parent_dataset.clone()));
            }
            return Err(ZfsError::CommandFailed(format!(
                "failed to get capacity: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout
            .lines()
            .next()
            .ok_or_else(|| ZfsError::ParseError("empty output from zfs list".to_string()))?;

        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 2 {
            return Err(ZfsError::ParseError(format!(
                "expected 2 fields for capacity, got {}: {}",
                fields.len(),
                line
            )));
        }

        let available = Self::parse_size(fields[0])?;
        let used = Self::parse_size(fields[1])?;

        debug!(
            dataset = %self.parent_dataset,
            available_bytes = available,
            used_bytes = used,
            "Capacity retrieved"
        );

        Ok(Capacity { available, used })
    }

    /// Check if a dataset exists
    async fn dataset_exists(&self, full_name: &str) -> Result<bool> {
        let output = Command::new("zfs")
            .args(["list", "-H", "-o", "name", full_name])
            .output()
            .await?;

        Ok(output.status.success())
    }

    /// Get detailed information about a dataset by its full name
    async fn get_dataset_info(&self, full_name: &str) -> Result<Dataset> {
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-p", // Machine-parseable output (bytes)
                "-o",
                "name,refer,volsize",
                full_name,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") || stderr.contains("not found") {
                return Err(ZfsError::DatasetNotFound(full_name.to_string()));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout
            .lines()
            .next()
            .ok_or_else(|| ZfsError::ParseError("empty output from zfs list".to_string()))?;

        self.parse_dataset_line(line)
    }

    /// Parse a line of ZFS output into a Dataset (expects: name, refer, volsize)
    fn parse_dataset_line(&self, line: &str) -> Result<Dataset> {
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 3 {
            return Err(ZfsError::ParseError(format!(
                "expected 3 fields, got {}: {}",
                fields.len(),
                line
            )));
        }

        let name = fields[0].to_string();
        let referenced = Self::parse_size(fields[1])?;
        // volsize is only present for zvols, "-" for filesystems
        let volsize = if fields[2] == "-" || fields[2] == "none" {
            None
        } else {
            Some(Self::parse_size(fields[2])?)
        };

        Ok(Dataset {
            name,
            referenced,
            volsize,
        })
    }

    /// Parse a ZFS size string into bytes.
    /// With -p flag, ZFS outputs bytes directly as integers.
    fn parse_size(size_str: &str) -> Result<u64> {
        let size_str = size_str.trim();
        if size_str == "-" {
            return Ok(0);
        }

        size_str
            .parse::<u64>()
            .map_err(|_| ZfsError::ParseError(format!("invalid size value: {}", size_str)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        // With -p flag, ZFS outputs bytes directly
        assert_eq!(ZfsManager::parse_size("1024").unwrap(), 1024);
        assert_eq!(
            ZfsManager::parse_size("1073741824").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(ZfsManager::parse_size("-").unwrap(), 0);
        // Invalid input should error
        assert!(ZfsManager::parse_size("1K").is_err());
        assert!(ZfsManager::parse_size("invalid").is_err());
    }

    #[test]
    fn test_validate_name() {
        // Valid names
        assert!(validate_name("volume1").is_ok());
        assert!(validate_name("vol-1").is_ok());
        assert!(validate_name("vol_1").is_ok());
        assert!(validate_name("vol.1").is_ok());
        assert!(validate_name("Vol-1_test.snap").is_ok());

        // Invalid names
        assert!(validate_name("").is_err());
        assert!(validate_name("vol/name").is_err());
        assert!(validate_name("vol@snap").is_err());
        assert!(validate_name("vol name").is_err());
        assert!(validate_name("vol;rm -rf /").is_err());
        assert!(validate_name("$(whoami)").is_err());
        // Path traversal
        assert!(validate_name("..").is_err());
        assert!(validate_name("vol..name").is_err());
        assert!(validate_name("../../../etc/passwd").is_err());
    }

    #[test]
    fn test_full_path() {
        let manager = ZfsManager {
            parent_dataset: "tank/csi".to_string(),
        };
        assert_eq!(manager.full_path("vol1"), "tank/csi/vol1");
    }

    #[test]
    fn test_get_device_path() {
        let manager = ZfsManager {
            parent_dataset: "tank/csi".to_string(),
        };
        assert_eq!(manager.get_device_path("vol1"), "/dev/zvol/tank/csi/vol1");
    }
}
