//! Configuration validation for portal and transport groups.
//!
//! Validates that portal-group (iSCSI) and transport-group (NVMeoF)
//! references in agent arguments actually exist in /etc/ctl.conf.

use std::path::Path;
use thiserror::Error;
use uclicious::{DEFAULT_DUPLICATE_STRATEGY, Priority, raw::object::ObjectRef};

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Config file not found: {0}")]
    FileNotFound(String),
    #[error("I/O error reading config: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse UCL config: {0}")]
    ParseError(String),
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

    // Parse the UCL config
    let mut parser = uclicious::raw::Parser::default();
    parser
        .add_chunk_full(&content, Priority::default(), DEFAULT_DUPLICATE_STRATEGY)
        .map_err(|e| ValidationError::ParseError(e.to_string()))?;

    let obj = parser
        .get_object()
        .map_err(|e| ValidationError::ParseError(e.to_string()))?;

    // Look for portal-group section
    if let Some(portal_groups) = obj.lookup("portal-group") {
        // Check if our group name exists as a key in the portal-group object
        if find_group_in_object(&portal_groups, group_name) {
            return Ok(());
        }
    }

    Err(ValidationError::PortalGroupNotFound(
        group_name.to_string(),
        path.display().to_string(),
    ))
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

    // Parse the UCL config
    let mut parser = uclicious::raw::Parser::default();
    parser
        .add_chunk_full(&content, Priority::default(), DEFAULT_DUPLICATE_STRATEGY)
        .map_err(|e| ValidationError::ParseError(e.to_string()))?;

    let obj = parser
        .get_object()
        .map_err(|e| ValidationError::ParseError(e.to_string()))?;

    // Look for transport-group section
    if let Some(transport_groups) = obj.lookup("transport-group") {
        // Check if our group name exists as a key in the transport-group object
        if find_group_in_object(&transport_groups, group_name) {
            return Ok(());
        }
    }

    Err(ValidationError::TransportGroupNotFound(
        group_name.to_string(),
        path.display().to_string(),
    ))
}

/// Check if a group name exists in a UCL object.
/// Handles both inline format (portal-group pg0 { }) and nested format (portal-group { pg0 { } })
fn find_group_in_object(obj: &ObjectRef, group_name: &str) -> bool {
    // Try to find the group name as a key in the object
    if obj.lookup(group_name).is_some() {
        return true;
    }

    // Also check if this object itself has the group name as its key
    // (this handles the inline format where the key is the group name)
    if let Some(key) = obj.key()
        && key == group_name
    {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_find_portal_group_inline_format() {
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
        assert!(result.is_ok(), "Error: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_find_portal_group_nested_format() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
portal-group {{
    pg0 {{
        listen = "0.0.0.0:3260"
    }}
}}
        "#
        )
        .unwrap();

        let result = validate_portal_group_exists(file.path(), "pg0").await;
        assert!(result.is_ok(), "Error: {:?}", result.err());
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
    async fn test_find_transport_group_inline_format() {
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
        assert!(result.is_ok(), "Error: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_find_transport_group_nested_format() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
transport-group {{
    tg0 {{
        listen {{
            tcp = "0.0.0.0:4420"
        }}
    }}
}}
        "#
        )
        .unwrap();

        let result = validate_transport_group_exists(file.path(), "tg0").await;
        assert!(result.is_ok(), "Error: {:?}", result.err());
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

    #[tokio::test]
    async fn test_real_world_config_format() {
        // Test the actual format from the user's /etc/ctl.conf
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
debug = 9;
maxproc = 255;
auth-group {{
    ag0 {{
        chap [
            {{
                user = "san";
                secret = "SanLoginSecret";
            }}
        ]
    }}
}}
portal-group {{
    pg0 {{
        discovery-auth-group = "no-authentication";
        listen [
            "0.0.0.0",
            "::",
        ]
    }}
}}
transport-group {{
    tg0 {{
        discovery-auth-group = "no-authentication";
        option {{
            max_io_qsize = 1024;
        }}
        listen {{
            tcp = "0.0.0.0:4420";
            discovery-tcp = "0.0.0.0:8009";
        }}
    }}
}}
        "#
        )
        .unwrap();

        let pg_result = validate_portal_group_exists(file.path(), "pg0").await;
        assert!(
            pg_result.is_ok(),
            "Portal group pg0 should exist: {:?}",
            pg_result.err()
        );

        let tg_result = validate_transport_group_exists(file.path(), "tg0").await;
        assert!(
            tg_result.is_ok(),
            "Transport group tg0 should exist: {:?}",
            tg_result.err()
        );
    }
}
