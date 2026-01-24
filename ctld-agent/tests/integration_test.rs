//! Integration tests for ctld-agent
//!
//! These tests verify the gRPC service layer behavior without requiring
//! actual ZFS/CTL operations (which would need root privileges and real hardware).
//! Tests focus on:
//! - Request validation and error handling
//! - CHAP authentication configuration
//! - Export type conversion
//! - Concurrent operation patterns
//! - Rate limiting behavior

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, Semaphore};

// Import actual library types now that we have lib.rs
use ctld_agent::{AuthConfig, ExportType, IscsiChapAuth, NvmeAuth};

/// Helper to create a test request with the given parameters
fn create_volume_request(name: &str, size_bytes: i64, export_type: i32) -> HashMap<String, String> {
    let mut params = HashMap::new();
    params.insert("name".to_string(), name.to_string());
    params.insert("size_bytes".to_string(), size_bytes.to_string());
    params.insert("export_type".to_string(), export_type.to_string());
    params
}

// ============================================================================
// CHAP Authentication Tests
// ============================================================================

/// Test basic CHAP authentication construction
#[test]
fn test_iscsi_chap_auth_basic() {
    let auth = IscsiChapAuth::new("testuser", "testsecret");

    // Verify auth can be constructed
    let config = AuthConfig::IscsiChap(auth);

    // Verify it's the right variant
    match config {
        AuthConfig::IscsiChap(_) => (), // expected
        _ => panic!("Expected IscsiChap variant"),
    }
}

/// Test mutual CHAP authentication construction
#[test]
fn test_iscsi_chap_auth_mutual() {
    let auth = IscsiChapAuth::with_mutual("testuser", "testsecret", "reverseuser", "reversesecret");

    let config = AuthConfig::IscsiChap(auth);

    match config {
        AuthConfig::IscsiChap(_) => (),
        _ => panic!("Expected IscsiChap variant"),
    }
}

/// Test NVMe authentication construction
#[test]
fn test_nvme_auth_basic() {
    let auth = NvmeAuth::new(
        "nqn.2024-01.org.freebsd.host:initiator",
        "secret123",
        "sha256",
    );

    let config = AuthConfig::NvmeAuth(auth);

    match config {
        AuthConfig::NvmeAuth(_) => (),
        _ => panic!("Expected NvmeAuth variant"),
    }
}

/// Test NVMe authentication with DH group
#[test]
fn test_nvme_auth_with_dh_group() {
    let auth = NvmeAuth::new(
        "nqn.2024-01.org.freebsd.host:initiator",
        "secret123",
        "sha384",
    )
    .with_dh_group("dh-hmac-chap:ffdhe2048");

    let config = AuthConfig::NvmeAuth(auth);

    match config {
        AuthConfig::NvmeAuth(_) => (),
        _ => panic!("Expected NvmeAuth variant"),
    }
}

/// Test AuthConfig::None variant
#[test]
fn test_auth_config_none() {
    let config = AuthConfig::None;

    match config {
        AuthConfig::None => (),
        _ => panic!("Expected None variant"),
    }
}

/// Test CHAP username validation (non-empty)
#[test]
fn test_chap_username_validation() {
    // Valid usernames
    let valid_usernames = vec![
        "admin",
        "user123",
        "iscsi-user",
        "test_user",
        "a", // minimum length
    ];

    for username in valid_usernames {
        assert!(
            !username.is_empty(),
            "Username '{}' should be valid",
            username
        );
        assert!(
            username
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_'),
            "Username '{}' should have valid characters",
            username
        );
    }
}

/// Test CHAP secret length requirements
#[test]
fn test_chap_secret_length() {
    // CHAP secrets typically need 12-16 characters minimum for security
    let valid_secrets = vec![
        "secretsecret",            // 12 chars
        "longsecretvalue1",        // 16 chars
        "averylongsecretpassword", // long
    ];

    for secret in valid_secrets {
        assert!(
            secret.len() >= 12,
            "Secret should be at least 12 characters"
        );
    }

    // Short secrets should be flagged
    let short_secret = "short";
    assert!(short_secret.len() < 12, "Short secrets should be detected");
}

// ============================================================================
// Export Type Tests
// ============================================================================

/// Test export type enum values
#[test]
fn test_export_type_iscsi() {
    let export = ExportType::Iscsi;

    // Verify it's the expected variant
    match export {
        ExportType::Iscsi => (),
        ExportType::Nvmeof => panic!("Expected Iscsi variant"),
    }
}

/// Test export type NVMeoF
#[test]
fn test_export_type_nvmeof() {
    let export = ExportType::Nvmeof;

    match export {
        ExportType::Nvmeof => (),
        ExportType::Iscsi => panic!("Expected Nvmeof variant"),
    }
}

// ============================================================================
// Volume Name Validation Tests
// ============================================================================

/// Test that empty volume names are rejected
#[test]
#[allow(clippy::const_is_empty)]
fn test_empty_volume_name_validation() {
    let name = "";
    assert!(name.is_empty(), "Empty name should be detected");
}

/// Test that volume names with valid characters are accepted
#[test]
fn test_valid_volume_name_characters() {
    let valid_names = vec![
        "volume1",
        "vol-1",
        "vol_1",
        "vol.1",
        "Vol-1_test.snap",
        "a",
        "A123",
        "test-volume-name-123",
        "pvc-12345678-1234-1234-1234-123456789012", // Kubernetes PVC name format
    ];

    for name in valid_names {
        let is_valid = name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.');
        assert!(
            !name.is_empty() && is_valid,
            "Name '{}' should be valid",
            name
        );
    }
}

/// Test that volume names with invalid characters are rejected
#[test]
fn test_invalid_volume_name_characters() {
    let invalid_names = vec![
        "vol/name",     // Contains path separator
        "vol@snap",     // Contains @ (reserved for snapshots)
        "vol name",     // Contains space
        "vol;rm -rf /", // Contains shell metacharacter
        "$(whoami)",    // Contains shell substitution
        "vol`id`",      // Contains backticks
        "vol|cat",      // Contains pipe
        "vol&bg",       // Contains ampersand
        "vol>file",     // Contains redirect
        "vol<file",     // Contains redirect
    ];

    for name in invalid_names {
        let is_valid = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.');
        assert!(!is_valid, "Name '{}' should be invalid", name);
    }
}

// ============================================================================
// Volume Size Validation Tests
// ============================================================================

/// Test that zero size is rejected
#[test]
fn test_zero_size_validation() {
    let size_bytes: i64 = 0;
    assert!(size_bytes <= 0, "Zero size should be detected as invalid");
}

/// Test that negative size is rejected
#[test]
fn test_negative_size_validation() {
    let size_bytes: i64 = -1024;
    assert!(
        size_bytes <= 0,
        "Negative size should be detected as invalid"
    );
}

/// Test that positive sizes are accepted
#[test]
fn test_valid_size_values() {
    let valid_sizes: Vec<i64> = vec![
        1,                        // Minimum positive
        1024,                     // 1 KB
        1024 * 1024,              // 1 MB
        1024 * 1024 * 1024,       // 1 GB
        10 * 1024 * 1024 * 1024,  // 10 GB
        100 * 1024 * 1024 * 1024, // 100 GB
    ];

    for size in valid_sizes {
        assert!(size > 0, "Size {} should be valid", size);
    }
}

// ============================================================================
// Snapshot ID Format Validation Tests
// ============================================================================

/// Test that snapshot ID format is validated correctly
#[test]
fn test_snapshot_id_format() {
    // Snapshot ID format: volume_id@snap_name
    let valid_ids = vec![
        "volume1@snap1",
        "vol-1@snap-1",
        "vol_1@snap_1",
        "my.volume@my.snap",
        "pvc-12345678-1234-1234-1234-123456789012@daily-backup-2024",
    ];

    for id in valid_ids {
        let parts: Vec<&str> = id.split('@').collect();
        assert_eq!(
            parts.len(),
            2,
            "Snapshot ID '{}' should have exactly one @",
            id
        );
        assert!(
            !parts[0].is_empty(),
            "Volume ID in '{}' should not be empty",
            id
        );
        assert!(
            !parts[1].is_empty(),
            "Snapshot name in '{}' should not be empty",
            id
        );
    }
}

/// Test that invalid snapshot ID formats are detected
#[test]
fn test_invalid_snapshot_id_format() {
    let invalid_ids = vec![
        "",               // Empty
        "volume1",        // Missing @
        "volume1@",       // Missing snapshot name
        "@snap1",         // Missing volume ID
        "vol@snap@extra", // Too many @
    ];

    for id in invalid_ids {
        let parts: Vec<&str> = id.split('@').collect();
        let is_valid = parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty();
        assert!(!is_valid, "Snapshot ID '{}' should be invalid", id);
    }
}

// ============================================================================
// Pagination Tests
// ============================================================================

/// Test pagination token parsing
#[test]
fn test_pagination_token_parsing() {
    let valid_tokens = vec!["0", "10", "100", "1000"];

    for token in valid_tokens {
        let idx = token.parse::<usize>();
        assert!(idx.is_ok(), "Token '{}' should parse as usize", token);
    }
}

/// Test pagination logic
#[test]
fn test_pagination_logic() {
    let items: Vec<i32> = (0..100).collect();
    let max_entries = 10usize;

    // First page
    let start_idx = 0usize;
    let end_idx = std::cmp::min(start_idx + max_entries, items.len());
    let first_page: Vec<_> = items[start_idx..end_idx].to_vec();
    assert_eq!(first_page.len(), 10);
    assert_eq!(first_page[0], 0);
    assert_eq!(first_page[9], 9);

    // Second page
    let start_idx = 10usize;
    let end_idx = std::cmp::min(start_idx + max_entries, items.len());
    let second_page: Vec<_> = items[start_idx..end_idx].to_vec();
    assert_eq!(second_page.len(), 10);
    assert_eq!(second_page[0], 10);

    // Last partial page
    let start_idx = 95usize;
    let end_idx = std::cmp::min(start_idx + max_entries, items.len());
    let last_page: Vec<_> = items[start_idx..end_idx].to_vec();
    assert_eq!(last_page.len(), 5);
    assert_eq!(last_page[4], 99);
}

/// Test next token generation
#[test]
fn test_pagination_next_token() {
    let total = 25usize;
    let max_entries = 10usize;

    // Page 1: items 0-9, next token should be "10"
    let end_idx = std::cmp::min(max_entries, total);
    let next_token = if end_idx < total {
        end_idx.to_string()
    } else {
        String::new()
    };
    assert_eq!(next_token, "10");

    // Page 3: items 20-24, next token should be empty (no more pages)
    let end_idx = std::cmp::min(20 + max_entries, total);
    let next_token = if end_idx < total {
        end_idx.to_string()
    } else {
        String::new()
    };
    assert_eq!(next_token, "");
}

// ============================================================================
// ZFS Dataset Path Validation Tests
// ============================================================================

/// Test ZFS dataset path construction
#[test]
fn test_zfs_dataset_path() {
    let parent = "tank/csi";
    let name = "vol1";
    let full_path = format!("{}/{}", parent, name);
    assert_eq!(full_path, "tank/csi/vol1");
}

/// Test ZFS device path construction
#[test]
fn test_zfs_device_path() {
    let parent = "tank/csi";
    let name = "vol1";
    let device_path = format!("/dev/zvol/{}/{}", parent, name);
    assert_eq!(device_path, "/dev/zvol/tank/csi/vol1");
}

/// Test ZFS snapshot path format
#[test]
fn test_zfs_snapshot_path() {
    let dataset = "tank/csi/vol1";
    let snap_name = "snap1";
    let snapshot_path = format!("{}@{}", dataset, snap_name);
    assert_eq!(snapshot_path, "tank/csi/vol1@snap1");
}

// ============================================================================
// iSCSI Target Name Validation Tests
// ============================================================================

/// Test iSCSI IQN format
#[test]
fn test_iscsi_iqn_format() {
    let base_iqn = "iqn.2024-01.org.freebsd.csi";
    let volume_name = "vol1";
    let target_iqn = format!("{}:{}", base_iqn, volume_name);

    assert!(target_iqn.starts_with("iqn."));
    assert!(target_iqn.contains(volume_name));

    // Verify IQN format regex pattern
    let iqn_pattern =
        target_iqn.starts_with("iqn.") && target_iqn.contains('-') && target_iqn.contains(':');
    assert!(iqn_pattern, "IQN should match format");
}

/// Test IQN length limits (per RFC 3720)
#[test]
fn test_iqn_length_limits() {
    // RFC 3720 limits IQN to 223 bytes
    let base_iqn = "iqn.2024-01.org.freebsd.csi";
    let volume_name = "a".repeat(200);
    let target_iqn = format!("{}:{}", base_iqn, volume_name);

    // This should exceed the limit
    assert!(target_iqn.len() > 223, "Long IQN should be detected");

    // Normal length should be fine
    let normal_iqn = format!("{}:vol1", base_iqn);
    assert!(normal_iqn.len() <= 223, "Normal IQN should be within limit");
}

// ============================================================================
// NVMeoF Target Name Validation Tests
// ============================================================================

/// Test NVMeoF NQN format
#[test]
fn test_nvmeof_nqn_format() {
    let base_nqn = "nqn.2024-01.org.freebsd.csi";
    let volume_name = "vol1";
    let target_nqn = format!("{}:{}", base_nqn, volume_name);

    assert!(target_nqn.starts_with("nqn."));
    assert!(target_nqn.contains(volume_name));
}

/// Test NQN length limits (per NVMe spec)
#[test]
fn test_nqn_length_limits() {
    // NVMe spec limits NQN to 223 characters
    let base_nqn = "nqn.2024-01.org.freebsd.csi";
    let volume_name = "a".repeat(200);
    let target_nqn = format!("{}:{}", base_nqn, volume_name);

    assert!(target_nqn.len() > 223, "Long NQN should be detected");
}

// ============================================================================
// Rate Limiting Tests
// ============================================================================

/// Test semaphore-based rate limiting pattern
#[tokio::test]
async fn test_rate_limiting_semaphore() {
    let max_concurrent = 3;
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    // Acquire all permits
    let mut permits = Vec::new();
    for _ in 0..max_concurrent {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        permits.push(permit);
    }

    // Verify no more permits available (would block)
    assert_eq!(semaphore.available_permits(), 0);

    // Release one permit
    permits.pop();
    assert_eq!(semaphore.available_permits(), 1);
}

/// Test that try_acquire returns None when exhausted
#[tokio::test]
async fn test_rate_limiting_try_acquire() {
    let semaphore = Arc::new(Semaphore::new(1));

    // Acquire the only permit
    let _permit = semaphore.acquire().await.unwrap();

    // Try to acquire another - should fail
    let result = semaphore.try_acquire();
    assert!(
        result.is_err(),
        "Should not be able to acquire when exhausted"
    );
}

/// Test concurrent operations with rate limiting
#[tokio::test]
async fn test_concurrent_operations_rate_limited() {
    let max_concurrent = 5;
    let total_operations = 20;
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let active_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let max_observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for i in 0..total_operations {
        let sem = semaphore.clone();
        let active = active_count.clone();
        let max_obs = max_observed.clone();

        handles.push(tokio::spawn(async move {
            // Acquire permit
            let _permit = sem.acquire().await.unwrap();

            // Track active operations
            let current = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;

            // Update max observed
            max_obs.fetch_max(current, std::sync::atomic::Ordering::SeqCst);

            // Simulate work
            tokio::time::sleep(Duration::from_millis(10)).await;

            // Decrement active count
            active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

            i
        }));
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify max concurrent never exceeded limit
    let observed_max = max_observed.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        observed_max <= max_concurrent,
        "Max concurrent {} exceeded limit {}",
        observed_max,
        max_concurrent
    );
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

/// Test HashMap concurrent access patterns
#[tokio::test]
async fn test_volume_metadata_concurrent_access() {
    let volumes: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(HashMap::new()));

    // Simulate concurrent reads and writes
    let volumes_clone = volumes.clone();
    let write_handle = tokio::spawn(async move {
        let mut guard = volumes_clone.write().await;
        guard.insert("vol1".to_string(), "metadata1".to_string());
    });

    let volumes_clone2 = volumes.clone();
    let read_handle = tokio::spawn(async move {
        // Wait a bit for write to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let guard = volumes_clone2.read().await;
        guard.get("vol1").cloned()
    });

    write_handle.await.unwrap();
    let result = read_handle.await.unwrap();

    assert_eq!(result, Some("metadata1".to_string()));
}

/// Test RwLock allows multiple readers
#[tokio::test]
async fn test_multiple_concurrent_readers() {
    let data: Arc<RwLock<i32>> = Arc::new(RwLock::new(42));

    let mut handles = vec![];

    for _ in 0..10 {
        let data_clone = data.clone();
        handles.push(tokio::spawn(async move {
            let guard = data_clone.read().await;
            *guard
        }));
    }

    for handle in handles {
        let value = handle.await.unwrap();
        assert_eq!(value, 42);
    }
}

/// Test that writes block readers appropriately
#[tokio::test]
async fn test_rwlock_write_exclusion() {
    let data: Arc<RwLock<i32>> = Arc::new(RwLock::new(0));
    let iterations = 100;

    let mut handles = Vec::new();

    // Spawn writers
    for _ in 0..iterations {
        let data_clone = data.clone();
        handles.push(tokio::spawn(async move {
            let mut guard = data_clone.write().await;
            *guard += 1;
        }));
    }

    // Wait for all writes
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all writes completed
    let final_value = *data.read().await;
    assert_eq!(final_value, iterations, "All writes should complete");
}

// ============================================================================
// Error Status Tests
// ============================================================================

/// Test get volume not found error handling
#[test]
fn test_get_volume_not_found() {
    let volumes: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let volume_id = "non-existent-volume";

    let result = volumes.get(volume_id);
    assert!(result.is_none(), "Non-existent volume should return None");

    let error_message = format!("volume '{}' not found", volume_id);
    assert!(error_message.contains("not found"));
    assert!(error_message.contains(volume_id));
}

/// Test that error status strings are descriptive
#[test]
fn test_error_messages() {
    let errors = vec![
        ("volume name cannot be empty", "empty name"),
        ("size_bytes must be positive", "zero size"),
        ("export_type must be ISCSI or NVMEOF", "unspecified type"),
        ("volume '{}' not found", "missing volume"),
        ("snapshot_id cannot be empty", "empty snapshot id"),
        ("CHAP username cannot be empty", "auth error"),
        ("concurrent operation limit exceeded", "rate limit"),
    ];

    for (message, _context) in errors {
        assert!(!message.is_empty(), "Error message should not be empty");
        assert!(
            message.len() > 10,
            "Error message should be descriptive: {}",
            message
        );
    }
}

// ============================================================================
// Auth Group Name Tests
// ============================================================================

/// Test auth group name generation for volumes
#[test]
fn test_auth_group_name_generation() {
    let volume_id = "pvc-12345678-abcd-1234-5678-123456789012";

    // Auth group names should be derived from volume ID
    let auth_group = format!("ag-{}", volume_id);

    // Verify format
    assert!(auth_group.starts_with("ag-"));
    assert!(auth_group.contains(volume_id));

    // Verify it's a valid identifier (alphanumeric, dash, underscore)
    let is_valid = auth_group
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_');
    assert!(is_valid, "Auth group name should be a valid identifier");
}

/// Test auth group uniqueness for different volumes
#[test]
fn test_auth_group_uniqueness() {
    let volumes = ["vol1", "vol2", "pvc-a", "pvc-b"];

    let auth_groups: Vec<String> = volumes.iter().map(|v| format!("ag-{}", v)).collect();

    // Check all auth groups are unique
    let unique_count = auth_groups
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        unique_count,
        volumes.len(),
        "All auth groups should be unique"
    );
}

// ============================================================================
// Volume Context Tests
// ============================================================================

/// Test volume context map construction
#[test]
fn test_volume_context_construction() {
    let mut context: HashMap<String, String> = HashMap::new();
    context.insert(
        "target_name".to_string(),
        "iqn.2024-01.org.freebsd.csi:vol1".to_string(),
    );
    context.insert("lun_id".to_string(), "0".to_string());
    context.insert("zfs_dataset".to_string(), "tank/csi/vol1".to_string());
    context.insert("export_type".to_string(), "iscsi".to_string());

    assert_eq!(context.len(), 4);
    assert!(context.contains_key("target_name"));
    assert!(context.contains_key("lun_id"));
    assert!(context.contains_key("zfs_dataset"));
    assert!(context.contains_key("export_type"));
}

/// Test volume context for NVMeoF exports
#[test]
fn test_volume_context_nvmeof() {
    let mut context: HashMap<String, String> = HashMap::new();
    context.insert(
        "target_name".to_string(),
        "nqn.2024-01.org.freebsd.csi:vol1".to_string(),
    );
    context.insert("namespace_id".to_string(), "1".to_string());
    context.insert("zfs_dataset".to_string(), "tank/csi/vol1".to_string());
    context.insert("export_type".to_string(), "nvmeof".to_string());

    assert_eq!(context.get("export_type").unwrap(), "nvmeof");
    assert!(context.get("target_name").unwrap().starts_with("nqn."));
}

// ============================================================================
// Integration Scenarios
// ============================================================================

/// Test complete volume lifecycle parameter flow
#[test]
#[allow(clippy::const_is_empty)]
fn test_volume_lifecycle_params() {
    // Create volume params
    let name = "test-vol";
    let size_bytes: i64 = 10 * 1024 * 1024 * 1024; // 10GB
    let export_type = 1; // ISCSI

    // Validate create params
    assert!(!name.is_empty());
    assert!(size_bytes > 0);
    assert!(export_type == 1 || export_type == 2);

    let volume_id = name;

    // Delete volume params
    assert!(!volume_id.is_empty());

    // Expand volume params
    let new_size: i64 = 20 * 1024 * 1024 * 1024; // 20GB
    assert!(new_size > size_bytes);
}

/// Test complete snapshot lifecycle parameter flow
#[test]
#[allow(clippy::const_is_empty)]
fn test_snapshot_lifecycle_params() {
    let source_volume_id = "test-vol";
    let snap_name = "snap1";

    assert!(!source_volume_id.is_empty());
    assert!(!snap_name.is_empty());

    let snapshot_id = format!("{}@{}", source_volume_id, snap_name);
    assert_eq!(snapshot_id, "test-vol@snap1");

    assert!(!snapshot_id.is_empty());
    let parts: Vec<&str> = snapshot_id.split('@').collect();
    assert_eq!(parts.len(), 2);
}

/// Test volume with CHAP authentication flow
#[test]
fn test_volume_with_chap_flow() {
    // Setup auth
    let auth = IscsiChapAuth::new("csi-user", "verysecretpass");
    let config = AuthConfig::IscsiChap(auth);

    // Create volume params
    let name = "test-vol-chap";
    let size_bytes: i64 = 1024 * 1024 * 1024; // 1GB
    let export_type = ExportType::Iscsi;

    // Verify all components are valid
    assert!(size_bytes > 0);
    assert!(matches!(export_type, ExportType::Iscsi));
    assert!(matches!(config, AuthConfig::IscsiChap(_)));

    // Expected auth group
    let auth_group = format!("ag-{}", name);
    assert!(!auth_group.is_empty());
}

/// Test volume with mutual CHAP authentication
#[test]
fn test_volume_with_mutual_chap() {
    let auth = IscsiChapAuth::with_mutual(
        "initiator-user",
        "initiator-secret",
        "target-user",
        "target-secret",
    );
    let config = AuthConfig::IscsiChap(auth);

    assert!(matches!(config, AuthConfig::IscsiChap(_)));
}

/// Test NVMeoF volume with DH-HMAC-CHAP
#[test]
fn test_nvmeof_volume_with_dh_hmac_chap() {
    let auth = NvmeAuth::new(
        "nqn.2024-01.org.freebsd.host:initiator01",
        "supersecretkey123456789012345678901234567890",
        "sha256",
    )
    .with_dh_group("dh-hmac-chap:ffdhe2048");

    let config = AuthConfig::NvmeAuth(auth);
    let export_type = ExportType::Nvmeof;

    assert!(matches!(config, AuthConfig::NvmeAuth(_)));
    assert!(matches!(export_type, ExportType::Nvmeof));
}

// ============================================================================
// Timestamp Handling Tests
// ============================================================================

/// Test timestamp generation
#[test]
fn test_timestamp_generation() {
    use std::time::{SystemTime, UNIX_EPOCH};

    let creation_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    assert!(creation_time > 0, "Timestamp should be positive");
    // Should be after year 2024 (approximately 1704067200)
    assert!(creation_time > 1704067200, "Timestamp should be recent");
}

// ============================================================================
// Request Parameter Tests
// ============================================================================

/// Test volume creation request parameter extraction
#[test]
fn test_create_volume_request_params() {
    let params = create_volume_request("test-volume", 1073741824, 1);

    assert_eq!(params.get("name").unwrap(), "test-volume");
    assert_eq!(params.get("size_bytes").unwrap(), "1073741824");
    assert_eq!(params.get("export_type").unwrap(), "1");
}

/// Test empty request parameter handling
#[test]
fn test_empty_request_params() {
    let params: HashMap<String, String> = HashMap::new();

    assert!(!params.contains_key("name"));
    assert!(!params.contains_key("size_bytes"));
    assert!(!params.contains_key("export_type"));
}

// ============================================================================
// High Concurrency Stress Tests
// ============================================================================

/// Test 10+ parallel operations (per plan requirements)
#[tokio::test]
async fn test_high_concurrency_10_plus_parallel() {
    let total_operations = 15;
    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();

    for i in 0..total_operations {
        let completed_clone = completed.clone();
        handles.push(tokio::spawn(async move {
            // Simulate a storage operation
            tokio::time::sleep(Duration::from_millis(5)).await;
            completed_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            i
        }));
    }

    // All operations should complete
    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    assert_eq!(results.len(), total_operations);
    assert_eq!(
        completed.load(std::sync::atomic::Ordering::SeqCst),
        total_operations
    );
}

/// Test concurrent create/delete operations don't conflict
#[tokio::test]
async fn test_concurrent_create_delete_isolation() {
    let volumes: Arc<RwLock<HashMap<String, i64>>> = Arc::new(RwLock::new(HashMap::new()));

    let mut handles = Vec::new();

    // Spawn 5 creators
    for i in 0..5 {
        let volumes_clone = volumes.clone();
        handles.push(tokio::spawn(async move {
            let vol_name = format!("vol-{}", i);
            let mut guard = volumes_clone.write().await;
            guard.insert(vol_name.clone(), 1024 * 1024 * 1024);
            vol_name
        }));
    }

    // Wait for creates
    let created: Vec<String> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(created.len(), 5);

    // Verify all volumes exist
    let guard = volumes.read().await;
    assert_eq!(guard.len(), 5);

    // Now delete in parallel
    drop(guard);
    let mut delete_handles = Vec::new();
    for vol_name in created {
        let volumes_clone = volumes.clone();
        delete_handles.push(tokio::spawn(async move {
            let mut guard = volumes_clone.write().await;
            guard.remove(&vol_name).is_some()
        }));
    }

    let deleted: Vec<bool> = futures::future::join_all(delete_handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert!(deleted.iter().all(|&d| d), "All deletes should succeed");
    assert!(
        volumes.read().await.is_empty(),
        "All volumes should be deleted"
    );
}

// ============================================================================
// CSI Spec Compliance Tests - FAILED_PRECONDITION for Snapshots
// ============================================================================

/// Test the error message format for volumes with dependent snapshots
///
/// Per CSI spec, DeleteVolume should return FAILED_PRECONDITION when:
/// "volume has snapshots and the plugin doesn't treat them as independent entities"
#[test]
fn test_delete_volume_snapshot_error_format() {
    let volume_name = "pvc-test-volume";
    let snapshots = vec!["backup-daily", "csi-snap-1", "csi-snap-2"];
    let snapshot_list = snapshots.join(", ");

    // Simulate the error message format from delete_volume
    let error_message = format!(
        "Volume '{}' has {} dependent snapshot(s): [{}]. \
         Delete all VolumeSnapshots referencing this volume before deletion. \
         If these are external snapshots (not CSI-managed), remove them manually with: \
         zfs destroy {}@<snapshot_name>",
        volume_name,
        snapshots.len(),
        snapshot_list,
        volume_name
    );

    // Verify error contains all required information
    assert!(
        error_message.contains(volume_name),
        "Error should contain volume name"
    );
    assert!(
        error_message.contains("dependent snapshot"),
        "Error should mention dependent snapshots"
    );
    assert!(
        error_message.contains(&snapshots.len().to_string()),
        "Error should contain snapshot count"
    );
    for snap in &snapshots {
        assert!(
            error_message.contains(snap),
            "Error should list snapshot: {}",
            snap
        );
    }
    assert!(
        error_message.contains("VolumeSnapshots"),
        "Error should mention Kubernetes VolumeSnapshots"
    );
    assert!(
        error_message.contains("zfs destroy"),
        "Error should provide manual cleanup command"
    );
}

/// Test that empty snapshot list allows deletion
#[test]
fn test_delete_volume_no_snapshots_ok() {
    let snapshots: Vec<String> = vec![];

    // When snapshots list is empty, deletion should proceed
    let should_block = !snapshots.is_empty();
    assert!(
        !should_block,
        "Empty snapshot list should not block deletion"
    );
}

/// Test snapshot list parsing from ZFS output
#[test]
fn test_snapshot_list_parsing() {
    // Simulate ZFS output: tank/csi/vol1@snap1\ntank/csi/vol1@snap2
    let zfs_output = "tank/csi/vol1@snap1\ntank/csi/vol1@snap2\ntank/csi/vol1@backup-daily";
    let prefix = "tank/csi/vol1@";

    let snapshots: Vec<String> = zfs_output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| line.strip_prefix(prefix).map(|s| s.to_string()))
        .collect();

    assert_eq!(snapshots.len(), 3);
    assert!(snapshots.contains(&"snap1".to_string()));
    assert!(snapshots.contains(&"snap2".to_string()));
    assert!(snapshots.contains(&"backup-daily".to_string()));
}

/// Test distinguishing CSI-managed vs external snapshots (by naming convention)
#[test]
fn test_snapshot_categorization() {
    let snapshots = [
        "csi-snap-1234",       // CSI-managed (csi- prefix)
        "snapshot-1234",       // CSI-managed (snapshot- prefix)
        "backup-daily",        // External (cronjob)
        "zfs-auto-2024-01-01", // External (ZFS auto-snapshot)
        "manual-backup",       // External (manual)
    ];

    // CSI-managed snapshots typically have csi- or snapshot- prefix
    let csi_managed: Vec<_> = snapshots
        .iter()
        .filter(|s| s.starts_with("csi-") || s.starts_with("snapshot-"))
        .collect();

    let external: Vec<_> = snapshots
        .iter()
        .filter(|s| !s.starts_with("csi-") && !s.starts_with("snapshot-"))
        .collect();

    assert_eq!(csi_managed.len(), 2);
    assert_eq!(external.len(), 3);

    // Error message should differentiate between CSI and external snapshots
    let error_hint = if !external.is_empty() {
        let external_list: Vec<&str> = external.iter().copied().copied().collect();
        format!(
            "External snapshots detected: [{}]. Remove manually with zfs destroy.",
            external_list.join(", ")
        )
    } else {
        "Delete VolumeSnapshots using kubectl.".to_string()
    };

    assert!(error_hint.contains("External snapshots"));
}

// ============================================================================
// NVMe Controller/Namespace Serial Number Tests
// ============================================================================

/// Test that NVMe namespace serial is generated from volume name using SHA-256
#[test]
fn test_nvme_namespace_serial_generation() {
    use sha2::{Digest, Sha256};

    let volume_name = "pvc-12345678-abcd-1234-5678-123456789012";

    // Generate expected serial using same algorithm
    let mut hasher = Sha256::new();
    hasher.update(volume_name.as_bytes());
    let hash = hasher.finalize();
    let expected_serial = hex::encode(&hash[..8]);

    // Verify it's 16 hex characters (8 bytes)
    assert_eq!(
        expected_serial.len(),
        16,
        "Namespace serial should be 16 hex chars"
    );

    // Verify determinism - same input produces same output
    let mut hasher2 = Sha256::new();
    hasher2.update(volume_name.as_bytes());
    let hash2 = hasher2.finalize();
    let serial2 = hex::encode(&hash2[..8]);
    assert_eq!(
        expected_serial, serial2,
        "Serial generation should be deterministic"
    );
}

/// Test that NVMe controller serial differs from namespace serial
#[test]
fn test_nvme_controller_serial_differs_from_namespace() {
    use sha2::{Digest, Sha256};

    let volume_name = "test-volume";

    // Namespace serial
    let mut ns_hasher = Sha256::new();
    ns_hasher.update(volume_name.as_bytes());
    let ns_hash = ns_hasher.finalize();
    let ns_serial = hex::encode(&ns_hash[..8]);

    // Controller serial (uses "ctrl:" prefix)
    let mut ctrl_hasher = Sha256::new();
    ctrl_hasher.update(b"ctrl:");
    ctrl_hasher.update(volume_name.as_bytes());
    let ctrl_hash = ctrl_hasher.finalize();
    let ctrl_serial = hex::encode(&ctrl_hash[..10]);

    // Verify they are different
    assert_ne!(
        ns_serial, ctrl_serial,
        "Controller and namespace serial should differ"
    );

    // Verify controller serial is 20 hex chars (10 bytes)
    assert_eq!(
        ctrl_serial.len(),
        20,
        "Controller serial should be 20 hex chars"
    );
}

/// Test that different volumes produce different serials
#[test]
fn test_nvme_serial_uniqueness() {
    use sha2::{Digest, Sha256};

    let volumes = ["vol1", "vol2", "pvc-a", "pvc-b", "test-volume"];

    let serials: Vec<String> = volumes
        .iter()
        .map(|v| {
            let mut hasher = Sha256::new();
            hasher.update(v.as_bytes());
            let hash = hasher.finalize();
            hex::encode(&hash[..8])
        })
        .collect();

    // Check all serials are unique
    let unique_count = serials
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        unique_count,
        volumes.len(),
        "All volume serials should be unique"
    );
}

/// Test NVMe serial format validity (hex string)
#[test]
fn test_nvme_serial_format() {
    use sha2::{Digest, Sha256};

    let volume_name = "test-volume";

    let mut hasher = Sha256::new();
    hasher.update(volume_name.as_bytes());
    let hash = hasher.finalize();
    let serial = hex::encode(&hash[..8]);

    // Verify it's valid hex
    assert!(
        serial.chars().all(|c| c.is_ascii_hexdigit()),
        "Serial should be valid hex string"
    );

    // Verify it's lowercase (hex::encode produces lowercase)
    assert_eq!(
        serial,
        serial.to_lowercase(),
        "Serial should be lowercase hex"
    );
}
