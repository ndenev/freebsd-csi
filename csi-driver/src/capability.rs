//! Shared capability checks, performed before provisioning or device operations.
use tonic::Status;

use crate::csi::{
    VolumeCapability,
    volume_capability::{AccessType, access_mode::Mode},
};
use crate::platform;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("{0}")]
    Unsupported(String),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        Status::invalid_argument(error.to_string())
    }
}

/// Return the effective filesystem for mounts, or None for raw block access.
/// Explicit capability fs_type takes precedence over the StorageClass fallback.
pub(crate) fn validate(
    capability: Option<&VolumeCapability>,
    fallback_fs: Option<&str>,
) -> Result<Option<&'static str>, Error> {
    let capability = capability.ok_or(Error::Invalid("Volume capability is required"))?;
    let access = capability
        .access_type
        .as_ref()
        .ok_or(Error::Invalid("Volume capability must specify access type"))?;
    let mode = capability
        .access_mode
        .as_ref()
        .ok_or(Error::Invalid("Volume capability must specify access mode"))?;
    let mode =
        Mode::try_from(mode.mode).map_err(|_| Error::Invalid("Unknown volume access mode"))?;
    if mode == Mode::Unknown {
        return Err(Error::Invalid("Unknown volume access mode"));
    }
    match access {
        AccessType::Block(_) => Ok(None),
        AccessType::Mount(mount) => {
            if matches!(
                mode,
                Mode::MultiNodeSingleWriter | Mode::MultiNodeMultiWriter
            ) {
                return Err(Error::Unsupported(format!(
                    "{} is not supported for filesystem volumes",
                    mode.as_str_name()
                )));
            }
            let fs = if mount.fs_type.is_empty() {
                fallback_fs.unwrap_or("")
            } else {
                &mount.fs_type
            };
            platform::validate_fs_type(fs)
                .map(Some)
                .map_err(|e| Error::Unsupported(e.message().to_owned()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ControllerService, NodeService, csi};
    use csi::{controller_server::Controller, node_server::Node};
    use std::{collections::HashMap, path::Path, time::Duration};
    use tonic::Request;

    fn cap(mode: i32, fs: Option<&str>) -> VolumeCapability {
        VolumeCapability {
            access_type: Some(match fs {
                None => AccessType::Block(Default::default()),
                Some(fs) => AccessType::Mount(csi::volume_capability::MountVolume {
                    fs_type: fs.into(),
                    ..Default::default()
                }),
            }),
            access_mode: Some(csi::volume_capability::AccessMode { mode }),
        }
    }

    #[test]
    fn supported_modes_and_filesystems() {
        for mode in [
            Mode::SingleNodeWriter,
            Mode::SingleNodeReaderOnly,
            Mode::MultiNodeReaderOnly,
            Mode::MultiNodeSingleWriter,
            Mode::MultiNodeMultiWriter,
            Mode::SingleNodeSingleWriter,
            Mode::SingleNodeMultiWriter,
        ] {
            // Raw block keeps every known mode; filesystem multi-node writers are rejected.
            assert_eq!(
                validate(Some(&cap(mode as i32, None)), Some("ufs")).unwrap(),
                None
            );
            for fs in ["", "ext4", "EXT4", "xfs"] {
                let result = validate(Some(&cap(mode as i32, Some(fs))), None);
                if matches!(
                    mode,
                    Mode::MultiNodeSingleWriter | Mode::MultiNodeMultiWriter
                ) {
                    assert!(matches!(result, Err(Error::Unsupported(_))));
                } else {
                    assert_eq!(
                        result.unwrap(),
                        Some(if fs == "xfs" { "xfs" } else { "ext4" })
                    );
                }
            }
        }
        assert_eq!(
            validate(Some(&cap(1, Some(""))), Some("xfs")).unwrap(),
            Some("xfs")
        );
        assert_eq!(
            validate(Some(&cap(1, Some("ext4"))), Some("ufs")).unwrap(),
            Some("ext4")
        );
        assert!(validate(Some(&cap(1, Some(""))), Some("ufs")).is_err());
    }

    #[test]
    fn rejected_requests_have_no_storage_side_effects() {
        // Isolate PATH in a child process; no test can accidentally run host storage commands.
        if let Ok(root) = std::env::var("CSI_CAPABILITY_TEST_DIR") {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(exercise_services(Path::new(&root)));
            return;
        }
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("csi-capabilities-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        for name in [
            "mount",
            "findmnt",
            "mountpoint",
            "iscsiadm",
            "nvme",
            "blkid",
            "blockdev",
            "mkfs.ext4",
            "mkfs.xfs",
            "resize2fs",
            "xfs_growfs",
        ] {
            let path = root.join(name);
            std::fs::write(&path, "#!/bin/sh\nprintf '%s\\n' \"$0\" >> \"$CSI_CAPABILITY_TEST_DIR/commands\"\nexit 99\n").unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "capability::tests::rejected_requests_have_no_storage_side_effects",
                "--nocapture",
            ])
            .env("CSI_CAPABILITY_TEST_DIR", &root)
            .env("PATH", &root)
            .output()
            .unwrap();
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn exercise_services(root: &Path) {
        // A live listening socket detects even an attempted agent connection.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let controller =
            ControllerService::new(format!("http://{}", listener.local_addr().unwrap()));
        let node = NodeService::new("test-node".into());
        let stage = root.join("stage").to_str().unwrap().to_owned();
        let publish = root.join("publish").to_str().unwrap().to_owned();
        let invalid = [
            None,
            Some(VolumeCapability::default()),
            Some(VolumeCapability {
                access_mode: None,
                ..cap(1, None)
            }),
            Some(VolumeCapability {
                access_type: None,
                ..cap(1, None)
            }),
            Some(cap(0, None)),
            Some(cap(99, Some("ext4"))),
            Some(cap(4, Some("ext4"))),
            Some(cap(5, Some("xfs"))),
            Some(cap(1, Some("ufs"))),
            Some(cap(1, Some("ntfs"))),
        ];
        for protocol in ["iscsi", "nvmeof"] {
            let context = HashMap::from([
                ("exportType".into(), protocol.into()),
                (
                    "targetName".into(),
                    format!(
                        "{}n.2024-01.org.freebsd.csi:vol",
                        if protocol == "iscsi" { "iq" } else { "nq" }
                    ),
                ),
                (
                    "endpoints".into(),
                    if protocol == "iscsi" {
                        "127.0.0.1:3260"
                    } else {
                        "127.0.0.1:4420"
                    }
                    .into(),
                ),
            ]);
            for capability in &invalid {
                reject_all(
                    &controller,
                    &node,
                    capability.clone(),
                    context.clone(),
                    &stage,
                    &publish,
                )
                .await;
            }
            // Bad fsType fallback must be rejected before connecting too.
            let mut context = context.clone();
            context.insert("fsType".into(), "ufs".into());
            reject_all(
                &controller,
                &node,
                Some(cap(1, Some(""))),
                context,
                &stage,
                &publish,
            )
            .await;
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(
            !root.join("commands").exists(),
            "A rejected request executed a storage command"
        );
        assert!(!Path::new(&stage).exists() && !Path::new(&publish).exists());
    }

    async fn reject_all(
        controller: &ControllerService,
        node: &NodeService,
        capability: Option<VolumeCapability>,
        context: HashMap<String, String>,
        stage: &str,
        publish: &str,
    ) {
        // A timeout also makes a misplaced agent call fail instead of hanging the test.
        tokio::time::timeout(Duration::from_secs(2), async {
            let caps: Vec<_> = capability.clone().into_iter().collect();
            for volume_capabilities in [caps.clone(), {
                let mut mixed = vec![cap(1, Some("ext4"))];
                mixed.extend(caps.clone());
                if caps.is_empty() {
                    mixed.clear();
                }
                mixed
            }] {
                assert_eq!(
                    controller
                        .create_volume(Request::new(csi::CreateVolumeRequest {
                            name: "vol".into(),
                            volume_capabilities,
                            parameters: context.clone(),
                            ..Default::default()
                        }))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
            }
            assert_eq!(
                node.node_stage_volume(Request::new(csi::NodeStageVolumeRequest {
                    volume_id: "vol".into(),
                    staging_target_path: stage.into(),
                    volume_context: context.clone(),
                    volume_capability: capability.clone(),
                    ..Default::default()
                }))
                .await
                .unwrap_err()
                .code(),
                tonic::Code::InvalidArgument
            );
            assert_eq!(
                node.node_publish_volume(Request::new(csi::NodePublishVolumeRequest {
                    volume_id: "vol".into(),
                    staging_target_path: stage.into(),
                    target_path: publish.into(),
                    volume_context: context.clone(),
                    volume_capability: capability.clone(),
                    ..Default::default()
                }))
                .await
                .unwrap_err()
                .code(),
                tonic::Code::InvalidArgument
            );
            // The validation RPC returns malformed requests before its read-only existence lookup.
            if capability
                .as_ref()
                .is_none_or(|c| matches!(validate(Some(c), None), Err(Error::Invalid(_))))
            {
                assert_eq!(
                    controller
                        .validate_volume_capabilities(Request::new(
                            csi::ValidateVolumeCapabilitiesRequest {
                                volume_id: "vol".into(),
                                volume_capabilities: caps,
                                ..Default::default()
                            }
                        ))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
            }
            // Expansion capability is optional, but a supplied unsupported one is rejected.
            if capability.is_some() && validate(capability.as_ref(), None).is_err() {
                assert_eq!(
                    controller
                        .controller_expand_volume(Request::new(
                            csi::ControllerExpandVolumeRequest {
                                volume_id: "vol".into(),
                                volume_capability: capability.clone(),
                                capacity_range: Some(csi::CapacityRange {
                                    required_bytes: 4096,
                                    limit_bytes: 0
                                }),
                                ..Default::default()
                            }
                        ))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
                assert_eq!(
                    node.node_expand_volume(Request::new(csi::NodeExpandVolumeRequest {
                        volume_id: "vol".into(),
                        volume_path: stage.into(),
                        volume_capability: capability,
                        capacity_range: Some(csi::CapacityRange {
                            required_bytes: 4096,
                            limit_bytes: 0
                        }),
                        ..Default::default()
                    }))
                    .await
                    .unwrap_err()
                    .code(),
                    tonic::Code::InvalidArgument
                );
            }
        })
        .await
        .expect("Rejected request tried to contact the agent");
    }
}
