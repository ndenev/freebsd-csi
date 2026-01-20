use std::process::Command;
use tracing::instrument;

use super::error::{Result, ZfsError};

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
        // Validate name
        if name.is_empty() || name.contains('/') {
            return Err(ZfsError::InvalidName(format!(
                "volume name '{}' is invalid (must be non-empty and not contain '/')",
                name
            )));
        }

        let full_name = self.full_path(name);

        // Check if volume already exists
        if self.dataset_exists(&full_name)? {
            return Err(ZfsError::DatasetExists(full_name));
        }

        // Create the volume with volmode=dev
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
            return Err(ZfsError::CommandFailed(stderr.to_string()));
        }

        // Return the created dataset info
        self.get_dataset(name)
    }

    /// Delete a ZFS volume
    #[instrument(skip(self))]
    pub fn delete_volume(&self, name: &str) -> Result<()> {
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
        // Validate snapshot name
        if snap_name.is_empty() || snap_name.contains('/') || snap_name.contains('@') {
            return Err(ZfsError::InvalidName(format!(
                "snapshot name '{}' is invalid",
                snap_name
            )));
        }

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

    /// Get information about a specific dataset
    pub fn get_dataset(&self, name: &str) -> Result<Dataset> {
        let full_name = self.full_path(name);
        self.get_dataset_info(&full_name)
    }

    /// List all volumes under the parent dataset
    pub fn list_volumes(&self) -> Result<Vec<Dataset>> {
        let output = Command::new("zfs")
            .args([
                "list",
                "-H",
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

    /// Parse a ZFS size string (with optional suffix) into bytes
    fn parse_size(size_str: &str) -> Result<u64> {
        let size_str = size_str.trim();
        if size_str == "-" {
            return Ok(0);
        }

        // Try parsing as plain number first (when -p flag would be used)
        if let Ok(bytes) = size_str.parse::<u64>() {
            return Ok(bytes);
        }

        // Parse with suffix (K, M, G, T, P, E)
        let len = size_str.len();
        if len < 2 {
            return Err(ZfsError::ParseError(format!(
                "invalid size string: {}",
                size_str
            )));
        }

        let (num_str, suffix) = size_str.split_at(len - 1);
        let num: f64 = num_str.parse().map_err(|_| {
            ZfsError::ParseError(format!("invalid size number: {}", num_str))
        })?;

        let multiplier: u64 = match suffix.to_uppercase().as_str() {
            "K" => 1024,
            "M" => 1024 * 1024,
            "G" => 1024 * 1024 * 1024,
            "T" => 1024 * 1024 * 1024 * 1024,
            "P" => 1024 * 1024 * 1024 * 1024 * 1024,
            "E" => 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
            _ => {
                return Err(ZfsError::ParseError(format!(
                    "unknown size suffix: {}",
                    suffix
                )))
            }
        };

        Ok((num * multiplier as f64) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(ZfsManager::parse_size("1024").unwrap(), 1024);
        assert_eq!(ZfsManager::parse_size("1K").unwrap(), 1024);
        assert_eq!(ZfsManager::parse_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(ZfsManager::parse_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(ZfsManager::parse_size("1.5G").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0) as u64);
        assert_eq!(ZfsManager::parse_size("-").unwrap(), 0);
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
