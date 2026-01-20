use std::process::Command;
use tracing::instrument;

use super::error::{Result, ZfsError};
use super::properties::{VolumeMetadata, METADATA_PROPERTY};

/// Validate that a name is safe for use in ZFS commands.
/// Only allows alphanumeric characters, underscores, hyphens, and periods.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ZfsError::InvalidName("name cannot be empty".into()));
    }
    if name.contains("..") {
        return Err(ZfsError::InvalidName("path traversal not allowed".into()));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.') {
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
    /// Used space in bytes
    pub used: u64,
    /// Available space in bytes
    pub available: u64,
    /// Referenced space in bytes
    pub referenced: u64,
    /// Mountpoint (None if not mounted or for volumes)
    pub mountpoint: Option<String>,
}

/// Manager for ZFS operations under a parent dataset
pub struct ZfsManager {
    /// Parent dataset under which all volumes are created
    parent_dataset: String,
}

impl ZfsManager {
    /// Create a new ZfsManager, verifying the parent dataset exists
    pub fn new(parent_dataset: String) -> Result<Self> {
        // Validate dataset name
        if parent_dataset.is_empty() {
            return Err(ZfsError::InvalidName("dataset name cannot be empty".to_string()));
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

        // Create the volume with volmode=dev
        // Let zfs create fail if already exists (avoids TOCTOU race)
        let output = Command::new("zfs")
            .args([
                "create",
                "-V", &size_bytes.to_string(),
                "-o", "volmode=dev",
                &full_name,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("already exists") {
                return Err(ZfsError::DatasetExists(full_name));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        // Return the created dataset info
        self.get_dataset(name)
    }

    /// Delete a ZFS volume
    #[instrument(skip(self))]
    pub fn delete_volume(&self, name: &str) -> Result<()> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);

        // Check if volume exists
        if !self.dataset_exists(&full_name)? {
            return Err(ZfsError::DatasetNotFound(full_name));
        }

        let output = Command::new("zfs")
            .args(["destroy", &full_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        Ok(())
    }

    /// Resize a ZFS volume
    #[instrument(skip(self))]
    pub fn resize_volume(&self, name: &str, new_size_bytes: u64) -> Result<()> {
        // Validate name for command injection prevention
        validate_name(name)?;

        let full_name = self.full_path(name);

        // Check if volume exists
        if !self.dataset_exists(&full_name)? {
            return Err(ZfsError::DatasetNotFound(full_name));
        }

        let output = Command::new("zfs")
            .args(["set", &format!("volsize={}", new_size_bytes), &full_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

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

        // Check if volume exists
        if !self.dataset_exists(&full_volume)? {
            return Err(ZfsError::DatasetNotFound(full_volume));
        }

        let output = Command::new("zfs")
            .args(["snapshot", &snapshot_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("already exists") {
                return Err(ZfsError::DatasetExists(snapshot_name));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        Ok(snapshot_name)
    }

    /// Delete a snapshot
    #[instrument(skip(self))]
    pub fn delete_snapshot(&self, volume_name: &str, snap_name: &str) -> Result<()> {
        // Validate both parts
        validate_name(volume_name)?;
        validate_name(snap_name)?;

        let full_name = format!("{}@{}", self.full_path(volume_name), snap_name);

        let output = Command::new("zfs")
            .args(["destroy", &full_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("does not exist") || stderr.contains("not found") {
                return Err(ZfsError::DatasetNotFound(full_name));
            }
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

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
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
                "-p",  // Machine-parseable output (bytes)
                "-t", "volume",
                "-r",
                "-o", "name,used,avail,refer,mountpoint",
                &self.parent_dataset,
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
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
            if dataset.name.starts_with(&self.parent_dataset)
                && dataset.name != self.parent_dataset
            {
                datasets.push(dataset);
            }
        }

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
        let property = format!("{}={}", METADATA_PROPERTY, json);

        let output = Command::new("zfs")
            .args(["set", &property, &full_name])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ZfsError::CommandFailed(format!(
                "failed to set metadata: {}",
                stderr
            )));
        }

        Ok(())
    }

    /// Get volume metadata from ZFS user property
    #[instrument(skip(self))]
    pub fn get_volume_metadata(&self, name: &str) -> Result<Option<VolumeMetadata>> {
        validate_name(name)?;
        let full_name = self.full_path(name);

        let output = Command::new("zfs")
            .args(["get", "-H", "-o", "value", METADATA_PROPERTY, &full_name])
            .output()?;

        if !output.status.success() {
            return Ok(None);
        }

        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if value.is_empty() || value == "-" {
            return Ok(None);
        }

        let metadata: VolumeMetadata = serde_json::from_str(&value)
            .map_err(|e| ZfsError::ParseError(format!("failed to parse metadata: {}", e)))?;

        Ok(Some(metadata))
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
                continue;
            }

            // Parse metadata
            // Extract volume name (remove parent prefix)
            let vol_name = name
                .strip_prefix(&format!("{}/", self.parent_dataset))
                .unwrap_or(name)
                .to_string();

            match serde_json::from_str::<VolumeMetadata>(metadata_json) {
                Ok(metadata) => results.push((vol_name, metadata)),
                Err(e) => {
                    tracing::warn!(volume = %name, error = %e, "corrupt CSI metadata, skipping");
                }
            }
        }

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
                "-p",  // Machine-parseable output (bytes)
                "-o", "name,used,avail,refer,mountpoint",
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
        let line = stdout.lines().next().ok_or_else(|| {
            ZfsError::ParseError("empty output from zfs list".to_string())
        })?;

        self.parse_dataset_line(line)
    }

    /// Parse a line of ZFS output into a Dataset
    fn parse_dataset_line(&self, line: &str) -> Result<Dataset> {
        let fields: Vec<&str> = line.split('\t').collect();
        if fields.len() < 5 {
            return Err(ZfsError::ParseError(format!(
                "expected 5 fields, got {}: {}",
                fields.len(),
                line
            )));
        }

        let name = fields[0].to_string();
        let used = Self::parse_size(fields[1])?;
        let available = Self::parse_size(fields[2])?;
        let referenced = Self::parse_size(fields[3])?;
        let mountpoint = if fields[4] == "-" || fields[4] == "none" {
            None
        } else {
            Some(fields[4].to_string())
        };

        Ok(Dataset {
            name,
            used,
            available,
            referenced,
            mountpoint,
        })
    }

    /// Parse a ZFS size string into bytes.
    /// With -p flag, ZFS outputs bytes directly as integers.
    fn parse_size(size_str: &str) -> Result<u64> {
        let size_str = size_str.trim();
        if size_str == "-" {
            return Ok(0);
        }

        size_str.parse::<u64>().map_err(|_| {
            ZfsError::ParseError(format!("invalid size value: {}", size_str))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        // With -p flag, ZFS outputs bytes directly
        assert_eq!(ZfsManager::parse_size("1024").unwrap(), 1024);
        assert_eq!(ZfsManager::parse_size("1073741824").unwrap(), 1024 * 1024 * 1024);
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
        assert_eq!(
            manager.get_device_path("vol1"),
            "/dev/zvol/tank/csi/vol1"
        );
    }
}
