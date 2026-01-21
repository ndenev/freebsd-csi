use std::process::{Command, Output};
use tracing::{debug, info, instrument, warn};

use super::error::{Result, ZfsError};
use super::properties::{METADATA_PROPERTY, VolumeMetadata};

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

    Err(ZfsError::CommandFailed(format!("{}: {}", context, stderr)))
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

/// Manager for ZFS operations under a parent dataset
pub struct ZfsManager {
    /// Parent dataset under which all volumes are created
    parent_dataset: String,
}

impl ZfsManager {
    /// Create a new ZfsManager, verifying the parent dataset exists
    pub fn new(parent_dataset: String) -> Result<Self> {
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
            .output()?;

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

    /// Create a new ZFS volume (zvol)
    #[instrument(skip(self))]
    pub fn create_volume(&self, name: &str, size_bytes: u64) -> Result<Dataset> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        info!(volume = %full_name, size_bytes, "Creating ZFS volume");

        // Create the volume with volmode=dev
        // Let zfs create fail if already exists (avoids TOCTOU race)
        let output = Command::new("zfs")
            .args([
                "create",
                "-V",
                &size_bytes.to_string(),
                "-o",
                "volmode=dev",
                &full_name,
            ])
            .output()?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(volume = %full_name, error = %e, "Failed to create volume");
            return Err(e);
        }

        info!(volume = %full_name, size_bytes, "ZFS volume created successfully");
        // Return the created dataset info
        self.get_dataset(name)
    }

    /// Delete a ZFS volume
    ///
    /// This operation is idempotent: if the volume doesn't exist, returns Ok.
    #[instrument(skip(self))]
    pub fn delete_volume(&self, name: &str) -> Result<()> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        info!(volume = %full_name, "Deleting ZFS volume");

        // Check if volume exists - if not, deletion is already complete (idempotent)
        if !self.dataset_exists(&full_name)? {
            info!(volume = %full_name, "Volume already deleted (idempotent)");
            return Ok(());
        }

        let output = Command::new("zfs").args(["destroy", &full_name]).output()?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(volume = %full_name, error = %e, "Failed to delete volume");
            return Err(e);
        }

        info!(volume = %full_name, "ZFS volume deleted successfully");
        Ok(())
    }

    /// Resize a ZFS volume
    #[instrument(skip(self))]
    pub fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<()> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        info!(volume = %full_name, new_size_bytes, "Resizing ZFS volume");

        // Check if volume exists
        if !self.dataset_exists(&full_name)? {
            warn!(volume = %full_name, "Volume not found for resize");
            return Err(ZfsError::DatasetNotFound(full_name));
        }

        let output = Command::new("zfs")
            .args(["set", &format!("volsize={}", new_size_bytes), &full_name])
            .output()?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(volume = %full_name, error = %e, "Failed to resize volume");
            return Err(e);
        }

        info!(volume = %full_name, new_size_bytes, "ZFS volume resized successfully");
        Ok(())
    }

    /// Create a snapshot of a volume
    #[instrument(skip(self))]
    pub fn create_snapshot(&self, volume_name: &str, snap_name: &str) -> Result<String> {
        // Validate names for command injection prevention
        validate_name(volume_name)?;
        validate_name(snap_name)?;

        let full_volume = self.full_path(volume_name);
        let snapshot_name = format!("{}@{}", full_volume, snap_name);
        info!(volume = %full_volume, snapshot = %snap_name, "Creating ZFS snapshot");

        // Check if volume exists
        if !self.dataset_exists(&full_volume)? {
            warn!(volume = %full_volume, "Volume not found for snapshot");
            return Err(ZfsError::DatasetNotFound(full_volume));
        }

        let output = Command::new("zfs")
            .args(["snapshot", &snapshot_name])
            .output()?;

        if let Err(e) = check_command_result(&output, &snapshot_name) {
            warn!(snapshot = %snapshot_name, error = %e, "Failed to create snapshot");
            return Err(e);
        }

        info!(snapshot = %snapshot_name, "ZFS snapshot created successfully");
        Ok(snapshot_name)
    }

    /// Delete a snapshot
    #[instrument(skip(self))]
    pub fn delete_snapshot(&self, volume_name: &str, snap_name: &str) -> Result<()> {
        // Validate both parts
        validate_name(volume_name)?;
        validate_name(snap_name)?;

        let full_name = format!("{}@{}", self.full_path(volume_name), snap_name);
        info!(snapshot = %full_name, "Deleting ZFS snapshot");

        let output = Command::new("zfs").args(["destroy", &full_name]).output()?;

        if let Err(e) = check_command_result(&output, &full_name) {
            warn!(snapshot = %full_name, error = %e, "Failed to delete snapshot");
            return Err(e);
        }

        info!(snapshot = %full_name, "ZFS snapshot deleted successfully");
        Ok(())
    }

    /// Get information about a specific dataset
    pub fn get_dataset(&self, name: &str) -> Result<Dataset> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);
        self.get_dataset_info(&full_name)
    }

    /// List all volumes under the parent dataset
    pub fn list_volumes(&self) -> Result<Vec<Dataset>> {
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
            .output()?;

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
    #[instrument(skip(self, metadata))]
    pub fn set_volume_metadata(&self, name: &str, metadata: &VolumeMetadata) -> Result<()> {
        validate_name(name)?;
        let json = serde_json::to_string(metadata)
            .map_err(|e| ZfsError::ParseError(format!("failed to serialize metadata: {}", e)))?;

        let full_name = self.full_path(name);
        debug!(volume = %full_name, "Setting volume metadata");

        let property = format!("{}={}", METADATA_PROPERTY, json);

        let output = Command::new("zfs")
            .args(["set", &property, &full_name])
            .output()?;

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
    pub fn clear_volume_metadata(&self, name: &str) -> Result<()> {
        validate_name(name)?;
        let full_name = self.full_path(name);

        // Use 'inherit' to remove user property
        let output = Command::new("zfs")
            .args(["inherit", METADATA_PROPERTY, &full_name])
            .output()?;

        // Ignore errors - property might not exist
        let _ = output;
        Ok(())
    }

    /// List all volumes with CSI metadata (for startup recovery)
    #[instrument(skip(self))]
    pub fn list_volumes_with_metadata(&self) -> Result<Vec<(String, VolumeMetadata)>> {
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
            .output()?;

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
                Ok(metadata) => {
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

    /// Check if a dataset exists
    fn dataset_exists(&self, full_name: &str) -> Result<bool> {
        let output = Command::new("zfs")
            .args(["list", "-H", "-o", "name", full_name])
            .output()?;

        Ok(output.status.success())
    }

    /// Get detailed information about a dataset by its full name
    fn get_dataset_info(&self, full_name: &str) -> Result<Dataset> {
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-p", // Machine-parseable output (bytes)
                "-o",
                "name,refer,volsize",
                full_name,
            ])
            .output()?;

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
