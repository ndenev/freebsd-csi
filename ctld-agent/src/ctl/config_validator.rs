//! Configuration validation for portal and transport groups.
//!
//! Validates that portal-group (iSCSI) and transport-group (NVMeoF)
//! references in agent arguments actually exist in /etc/ctl.conf.

use regex::Regex;
use std::path::Path;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_find_portal_group_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
portal-group pg0 {{
    listen = "0.0.0.0:3260"
}}
        "#
        )
        .unwrap();

        let result = validate_portal_group_exists(file.path(), "pg0").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_portal_group_not_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
portal-group pg0 {{
    listen = "0.0.0.0:3260"
}}
        "#
        )
        .unwrap();

        let result = validate_portal_group_exists(file.path(), "pg1").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_find_transport_group_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
transport-group tg0 {{
    listen {{
        tcp = "0.0.0.0:4420"
    }}
}}
        "#
        )
        .unwrap();

        let result = validate_transport_group_exists(file.path(), "tg0").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_find_transport_group_not_exists() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
transport-group tg0 {{
    listen {{
        tcp = "0.0.0.0:4420"
    }}
}}
        "#
        )
        .unwrap();

        let result = validate_transport_group_exists(file.path(), "tg1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_missing_config_file() {
        let result = validate_portal_group_exists("/nonexistent/path", "pg0").await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not found") || err_msg.contains("No such file"),
            "Error message should contain 'not found' or 'No such file', got: {}",
            err_msg
        );
    }
}
