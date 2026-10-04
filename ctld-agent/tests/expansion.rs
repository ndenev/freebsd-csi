//! Exercise the production service with an isolated, stateful ZFS command.
use std::sync::Arc;
use std::time::Duration;

use ctld_agent::proto::{ExpandVolumeRequest, storage_agent_server::StorageAgent};
use ctld_agent::{CtlManager, StorageService, ZfsManager};
use tokio::sync::RwLock;
use tonic::Request;

#[test]
fn expansion_preserves_capacity_and_serializes_cancelled_requests() {
    // PATH is set only in a child test process, never in a shared test runtime.
    if let Ok(directory) = std::env::var("CSI_EXPANSION_TEST_DIR") {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(exercise_service(directory));
        return;
    }
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let command = directory.path().join("zfs");
    std::fs::write(&command, r#"#!/bin/sh
set -eu
case "$*" in
  'list -H -o name audit/csi'|'list -H -o name audit/csi/vol') exit 0;;
  'list -H -r -t volume -o name,user:csi:metadata audit/csi')
    printf '%s\t%s\n' 'audit/csi/vol' '{"schema_version":3,"export_type":"ISCSI","target_name":"iqn.2024-01.org.freebsd.csi:vol","lun_id":0,"parameters":{},"created_at":0}'
    exit 0;;
  'list -H -p -o name,refer,volsize audit/csi/vol')
    read -r size < "$CSI_EXPANSION_TEST_DIR/size"
    printf 'audit/csi/vol\t0\t%s\n' "$size"
    exit 0;;
esac
if [ "$1" = set ] && [ "$3" = audit/csi/vol ]; then
    printf '%s\n' "$2" >> "$CSI_EXPANSION_TEST_DIR/sets"
    : > "$CSI_EXPANSION_TEST_DIR/started"
    while [ -e "$CSI_EXPANSION_TEST_DIR/blocked" ]; do sleep 0.01; done
    if [ -e "$CSI_EXPANSION_TEST_DIR/fail" ]; then exit 1; fi
    printf '%s\n' "${2#volsize=}" > "$CSI_EXPANSION_TEST_DIR/size"
    exit 0
fi
printf 'Unexpected ZFS command: %s\n' "$*" >&2
exit 99
"#).unwrap();
    std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o755)).unwrap();
    let path = std::env::join_paths(std::iter::once(directory.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))
    .unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "expansion_preserves_capacity_and_serializes_cancelled_requests",
            "--nocapture",
        ])
        .env("CSI_EXPANSION_TEST_DIR", directory.path())
        .env("PATH", path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn expand(service: &StorageService, size: i64) -> Result<i64, tonic::Status> {
    service
        .expand_volume(Request::new(ExpandVolumeRequest {
            volume_id: "vol".into(),
            new_size_bytes: size,
        }))
        .await
        .map(|response| response.into_inner().size_bytes)
}

async fn exercise_service(directory: String) {
    let directory = std::path::Path::new(&directory);
    let size = directory.join("size");
    let sets = directory.join("sets");
    std::fs::write(&size, "4096\n").unwrap();
    let zfs = ZfsManager::new("audit/csi".into()).await.unwrap();
    let ctl = CtlManager::new(
        "iqn.2024-01.org.freebsd.csi".into(),
        "nqn.2024-01.org.freebsd.csi".into(),
        "pg0".into(),
        "tg0".into(),
        "audit/csi".into(),
    )
    .unwrap();
    let service = Arc::new(StorageService::new(
        Arc::new(RwLock::new(zfs)),
        Arc::new(RwLock::new(ctl)),
    ));
    assert_eq!(service.restore_from_zfs().await.unwrap(), 1);
    assert_eq!(expand(&service, 4096).await.unwrap(), 4096);
    assert_eq!(expand(&service, 2048).await.unwrap(), 4096);
    assert!(
        !sets.exists(),
        "Equal/smaller requests must not call zfs set"
    );
    assert_eq!(expand(&service, 8192).await.unwrap(), 8192);
    assert_eq!(expand(&service, 8192).await.unwrap(), 8192);
    assert_eq!(std::fs::read_to_string(&sets).unwrap(), "volsize=8192\n");
    for invalid in [0, -1] {
        assert_eq!(
            expand(&service, invalid).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    for invalid in ["-\n", "0\n", "invalid\n"] {
        std::fs::write(&size, invalid).unwrap();
        assert_eq!(
            expand(&service, 16384).await.unwrap_err().code(),
            tonic::Code::Internal
        );
    }
    assert_eq!(std::fs::read_to_string(&sets).unwrap(), "volsize=8192\n");
    std::fs::write(&size, "8192\n").unwrap();
    std::fs::write(directory.join("fail"), "").unwrap();
    assert_eq!(
        expand(&service, 16384).await.unwrap_err().code(),
        tonic::Code::Internal
    );
    assert_eq!(std::fs::read_to_string(&size).unwrap(), "8192\n");
    std::fs::remove_file(directory.join("fail")).unwrap();

    // Force a pending smaller set, cancel its caller, then request a larger size.
    // Cancellation must not release the lock while the first command is alive.
    std::fs::write(&sets, "").unwrap();
    std::fs::remove_file(directory.join("started")).unwrap();
    std::fs::write(directory.join("blocked"), "").unwrap();
    let first_service = service.clone();
    let first = tokio::spawn(async move { expand(&first_service, 16384).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !directory.join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let second_service = service.clone();
    let second = tokio::spawn(async move { expand(&second_service, 32768).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(std::fs::read_to_string(&sets).unwrap(), "volsize=16384\n");
    std::fs::remove_file(directory.join("blocked")).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        32768
    );
    assert_eq!(expand(&service, 16384).await.unwrap(), 32768);
    assert_eq!(
        std::fs::read_to_string(&sets).unwrap(),
        "volsize=16384\nvolsize=32768\n"
    );
}
