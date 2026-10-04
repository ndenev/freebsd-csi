//! Refresh the mounted volume's transport before growing its filesystem.
use std::path::{Path, PathBuf};

use tokio::{fs, process::Command};
use tonic::Status;

use crate::csi::{CapacityRange, NodeExpandVolumeRequest, volume_capability::AccessType};

async fn command(program: &str, args: &[&str]) -> Result<String, Status> {
    let output = Command::new(program)
        .args(args)
        .env("LC_ALL", "C")
        .output()
        .await
        .map_err(|e| Status::internal(format!("{program}: {e}")))?;
    if !output.status.success() {
        return Err(Status::internal(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| Status::internal(format!("{program} returned invalid UTF-8")))
}

async fn read(path: impl AsRef<Path>) -> Result<String, Status> {
    let path = path.as_ref();
    fs::read_to_string(path)
        .await
        .map(|s| s.trim().to_owned())
        .map_err(|e| Status::unavailable(format!("{}: {e}", path.display())))
}

fn positive(value: &str) -> Result<u64, Status> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|v| *v > 0)
        .ok_or_else(|| Status::internal("Invalid capacity or filesystem geometry"))
}

#[derive(Debug, PartialEq)]
struct Device {
    sysfs: PathBuf,
    path: String,
    fs_type: String,
    target: String,
    nvme: bool,
}

// Resolve the mount's major:minor, not a possibly stale /dev alias or a target
// guessed from a configurable IQN/NQN prefix. Partitions and device-mapper need
// their own resize handling; fail closed until that handling is implemented.
async fn resolve(req: &NodeExpandVolumeRequest, sys: &Path) -> Result<Device, Status> {
    let block = matches!(
        req.volume_capability
            .as_ref()
            .and_then(|c| c.access_type.as_ref()),
        Some(AccessType::Block(_))
    );
    let output = if block {
        command("lsblk", &["-dn", "-o", "MAJ:MIN", &req.volume_path]).await?
    } else {
        command(
            "findmnt",
            &[
                "-rn",
                "-o",
                "MAJ:MIN,FSTYPE",
                "--mountpoint",
                &req.volume_path,
            ],
        )
        .await?
    };
    let fields: Vec<_> = output.split_whitespace().collect();
    if fields.len() != if block { 1 } else { 2 } {
        return Err(Status::failed_precondition(
            "Expected one mounted block device",
        ));
    }
    let dev = fields[0];
    if !dev
        .split_once(':')
        .is_some_and(|(a, b)| a.parse::<u32>().is_ok() && b.parse::<u32>().is_ok())
    {
        return Err(Status::failed_precondition("Invalid block device identity"));
    }
    let sysfs = fs::canonicalize(sys.join("dev/block").join(dev))
        .await
        .map_err(|e| Status::unavailable(format!("Resolve block device {dev}: {e}")))?;
    if !sysfs.starts_with(sys)
        || fs::try_exists(sysfs.join("partition"))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
    {
        return Err(Status::failed_precondition(
            "Expansion requires a whole CSI block device",
        ));
    }
    let name = sysfs
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Status::internal("Missing block device name"))?;
    if name.starts_with("dm-") {
        return Err(Status::failed_precondition(
            "Device-mapper expansion is not supported",
        ));
    }
    let fs_type = if block { "" } else { fields[1] };
    if !matches!(fs_type, "" | "ext2" | "ext3" | "ext4" | "xfs") {
        return Err(Status::failed_precondition(format!(
            "Unsupported filesystem: {fs_type}"
        )));
    }
    let device = fs::canonicalize(sysfs.join("device"))
        .await
        .map_err(|e| Status::unavailable(format!("Resolve transport: {e}")))?;
    let nvme = name.starts_with("nvme");
    let target = if nvme {
        read(device.join("subsysnqn")).await?
    } else {
        let session = device
            .ancestors()
            .filter_map(|p| p.file_name())
            .filter_map(|n| n.to_str())
            .find(|n| {
                n.strip_prefix("session")
                    .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            })
            .ok_or_else(|| Status::failed_precondition("Block device is not an iSCSI LUN"))?;
        read(
            sys.join("class/iscsi_session")
                .join(session)
                .join("targetname"),
        )
        .await?
    };
    if target.rsplit_once(':').map(|(_, id)| id) != Some(req.volume_id.as_str()) {
        return Err(Status::failed_precondition(
            "Mounted device does not belong to the requested volume",
        ));
    }
    Ok(Device {
        path: format!("/dev/{name}"),
        sysfs,
        fs_type: fs_type.into(),
        target,
        nvme,
    })
}

async fn refresh(device: &Device, sys: &Path) -> Result<(), Status> {
    let (class, attribute) = if device.nvme {
        ("nvme", "subsysnqn")
    } else {
        ("iscsi_session", "targetname")
    };
    let mut entries = fs::read_dir(sys.join("class").join(class))
        .await
        .map_err(|e| Status::unavailable(format!("List transport paths: {e}")))?;
    let mut found = false;
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| Status::unavailable(e.to_string()))?
    {
        // A disappearing unrelated controller must not prevent this volume's resize.
        if read(entry.path().join(attribute)).await.ok().as_deref() != Some(&device.target) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if device.nvme {
            command("nvme", &["ns-rescan", &format!("/dev/{name}")]).await?;
        } else {
            let session = name
                .strip_prefix("session")
                .ok_or_else(|| Status::internal("Invalid iSCSI session"))?;
            command("iscsiadm", &["-m", "session", "-r", session, "--rescan"]).await?;
        }
        found = true;
    }
    if !found {
        return Err(Status::unavailable("No matching transport paths remain"));
    }
    Ok(())
}

// These are total filesystem blocks, including metadata. df reports usable
// blocks and therefore cannot establish whether a filesystem fills its device.
fn geometry(output: &str, xfs: bool) -> Result<(u64, u64), Status> {
    if xfs {
        let line = output
            .lines()
            .find(|l| l.starts_with("data "))
            .ok_or_else(|| Status::internal("Missing XFS data geometry"))?;
        let field = |key: &str| {
            line.split_whitespace()
                .find_map(|s| s.strip_prefix(key))
                .ok_or_else(|| Status::internal("Missing XFS geometry field"))
        };
        Ok((
            positive(field("blocks=")?.trim_end_matches(','))?,
            positive(field("bsize=")?.trim_end_matches(','))?,
        ))
    } else {
        let field = |key: &str| {
            output
                .lines()
                .find_map(|s| s.strip_prefix(key))
                .ok_or_else(|| Status::internal("Missing ext filesystem geometry"))
        };
        Ok((
            positive(field("Block count:")?)?,
            positive(field("Block size:")?)?,
        ))
    }
}

pub async fn expand(req: &NodeExpandVolumeRequest) -> Result<i64, Status> {
    expand_at(req, Path::new("/sys")).await
}

async fn expand_at(req: &NodeExpandVolumeRequest, sys: &Path) -> Result<i64, Status> {
    let CapacityRange {
        required_bytes,
        limit_bytes,
    } = req
        .capacity_range
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("Capacity range is required"))?;
    if *required_bytes < 0
        || *limit_bytes < 0
        || (*limit_bytes > 0 && required_bytes > limit_bytes)
        || (*required_bytes == 0 && *limit_bytes == 0)
    {
        return Err(Status::invalid_argument("Invalid capacity range"));
    }
    let requested = if *required_bytes > 0 {
        *required_bytes
    } else {
        *limit_bytes
    } as u64;
    let device = resolve(req, sys).await?;
    refresh(&device, sys).await?;
    let capacity = positive(&command("blockdev", &["--getsize64", &device.path]).await?)?;
    if capacity < requested {
        // Namespace scans are asynchronous. Let kubelet retry once the kernel
        // exposes the controller's new capacity, without touching the filesystem.
        return Err(Status::unavailable(format!(
            "Device capacity {capacity} has not reached {requested}"
        )));
    }
    if capacity > i64::MAX as u64 || (*limit_bytes > 0 && capacity > *limit_bytes as u64) {
        return Err(Status::out_of_range(
            "Device capacity exceeds requested limit",
        ));
    }
    if resolve(req, sys).await? != device {
        return Err(Status::aborted("Mounted device changed during expansion"));
    }
    let output = match device.fs_type.as_str() {
        "" => return Ok(capacity as i64),
        "xfs" => {
            command("xfs_growfs", &["-d", &req.volume_path]).await?;
            command("xfs_info", &[&req.volume_path]).await?
        }
        _ => {
            command("resize2fs", &[&device.path]).await?;
            command("dumpe2fs", &["-h", &device.path]).await?
        }
    };
    let (blocks, block_size) = geometry(&output, device.fs_type == "xfs")?;
    let mut required_blocks = capacity / block_size;
    if device.fs_type == "xfs" {
        let ag_blocks = output
            .split_whitespace()
            .find_map(|s| s.strip_prefix("agsize="))
            .ok_or_else(|| Status::internal("Missing XFS allocation group size"))?;
        let tail = required_blocks % positive(ag_blocks.trim_end_matches(','))?;
        // XFS cannot add an allocation group smaller than XFS_MIN_AG_BLOCKS
        // (64 filesystem blocks). Account only for that unallocatable tail.
        if tail < 64 {
            required_blocks -= tail;
        }
    }
    if blocks < required_blocks {
        return Err(Status::unavailable(format!(
            "Filesystem has {blocks} blocks of {block_size} bytes; device has {capacity} bytes"
        )));
    }
    if resolve(req, sys).await? != device {
        return Err(Status::aborted("Mounted device changed during expansion"));
    }
    Ok(capacity as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[test]
    fn expansion_checks_transport_and_filesystem_capacity() {
        if let Ok(directory) = std::env::var("CSI_NODE_EXPANSION_TEST_DIR") {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(exercise(Path::new(&directory)));
            return;
        }
        // Isolate PATH in a child process, as in the agent expansion test.
        let directory = std::env::temp_dir().join(format!("csi-expand-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let script = directory.as_path().join("command");
        std::fs::write(&script, r#"#!/bin/sh
set -eu
cd "$CSI_NODE_EXPANSION_TEST_DIR"
name=${0##*/}
printf '%s %s\n' "$name" "$*" >> calls
case "$name" in
  findmnt) cat mount;;
  lsblk) printf '259:1\n';;
  nvme|iscsiadm) if [ -f fail-scan ]; then exit 1; fi;;
  blockdev) cat capacity;;
  resize2fs|xfs_growfs)
    if [ -f fail-resize ]; then echo 'already at size; Nothing to do' >&2; exit 1; fi
    if [ ! -f no-op ]; then read -r size < capacity; echo "$((size / 4096))" > blocks; fi;;
  dumpe2fs) read -r blocks < blocks; printf 'Block count: %s\nBlock size: 4096\n' "$blocks";;
  xfs_info) read -r blocks < blocks; printf 'meta-data=/dev/test agcount=4, agsize=131072\ndata     = bsize=4096 blocks=%s, imaxpct=25\n' "$blocks";;
  *) exit 99;;
esac
"#).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        for name in [
            "findmnt",
            "lsblk",
            "nvme",
            "iscsiadm",
            "blockdev",
            "resize2fs",
            "xfs_growfs",
            "dumpe2fs",
            "xfs_info",
        ] {
            symlink(&script, directory.as_path().join(name)).unwrap();
        }
        let path = std::env::join_paths(std::iter::once(directory.as_path().to_path_buf()).chain(
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
        ))
        .unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "platform::expansion::tests::expansion_checks_transport_and_filesystem_capacity",
                "--nocapture",
            ])
            .env("CSI_NODE_EXPANSION_TEST_DIR", directory.as_path())
            .env("PATH", path)
            .output()
            .unwrap();
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    async fn exercise(directory: &Path) {
        let sys = directory.join("sys");
        let block = sys.join("devices/nvme0n1");
        for path in [
            sys.join("dev/block"),
            block.clone(),
            sys.join("devices/nvme-subsys0"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        symlink(&block, sys.join("dev/block/259:1")).unwrap();
        symlink(sys.join("devices/nvme-subsys0"), block.join("device")).unwrap();
        std::fs::write(sys.join("devices/nvme-subsys0/subsysnqn"), "nqn.custom:vol").unwrap();
        for (name, target) in [
            ("nvme0", "nqn.custom:vol"),
            ("nvme1", "nqn.custom:vol"),
            ("nvme2", "nqn.other:unrelated"),
        ] {
            let controller = sys.join("class/nvme").join(name);
            std::fs::create_dir_all(&controller).unwrap();
            std::fs::write(controller.join("subsysnqn"), target).unwrap();
        }
        let write = |name: &str, value: &str| std::fs::write(directory.join(name), value).unwrap();
        let remove = |name: &str| std::fs::remove_file(directory.join(name)).unwrap();
        let calls = || std::fs::read_to_string(directory.join("calls")).unwrap_or_default();
        write("mount", "259:1 ext4\n");
        write("capacity", "2147483648\n");
        write("blocks", "524288\n");
        let mut req = NodeExpandVolumeRequest {
            volume_id: "vol".into(),
            volume_path: "/staging/vol".into(),
            capacity_range: Some(CapacityRange {
                required_bytes: 3221225472,
                limit_bytes: 0,
            }),
            ..Default::default()
        };
        for (required_bytes, limit_bytes) in [(-1, 0), (0, 0), (1, -1), (2, 1)] {
            let invalid = NodeExpandVolumeRequest {
                capacity_range: Some(CapacityRange {
                    required_bytes,
                    limit_bytes,
                }),
                ..req.clone()
            };
            assert_eq!(
                expand_at(&invalid, &sys).await.unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
        assert!(calls().is_empty());
        // Regression: controller has grown, but Linux still sees 2 GiB.
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert!(calls().contains("nvme ns-rescan /dev/nvme0\n"));
        assert!(calls().contains("nvme ns-rescan /dev/nvme1\n"));
        assert!(!calls().contains("/dev/nvme2"));
        assert!(!calls().contains("resize2fs"));
        // Even a successful resizer cannot prove filesystem convergence.
        write("capacity", "3221225472\n");
        write("no-op", "");
        // Do not report the larger raw capacity when an older request arrives
        // and the filesystem still occupies only its original size.
        let smaller = NodeExpandVolumeRequest {
            capacity_range: Some(CapacityRange {
                required_bytes: 2147483648,
                limit_bytes: 0,
            }),
            ..req.clone()
        };
        assert_eq!(
            expand_at(&smaller, &sys).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        remove("no-op");
        assert_eq!(expand_at(&req, &sys).await.unwrap(), 3221225472);
        assert_eq!(expand_at(&req, &sys).await.unwrap(), 3221225472); // idempotent retry
        // A nonzero exit is a failure, even if stderr includes 'already'.
        write("fail-resize", "");
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::Internal
        );
        remove("fail-resize");
        write("calls", "");
        write("fail-scan", "");
        assert!(expand_at(&req, &sys).await.is_err());
        assert!(!calls().contains("resize2fs"));
        remove("fail-scan");
        write("calls", "");
        req.volume_id = "other".into();
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert!(!calls().contains("nvme ns-rescan"));
        req.volume_id = "vol".into();
        req.capacity_range.as_mut().unwrap().limit_bytes = 3221225472;
        write("capacity", "4294967296\n");
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::OutOfRange
        );
        assert!(!calls().contains("resize2fs"));
        write("capacity", "3221225472\n");
        // XFS uses data geometry, not df's metadata-adjusted usable blocks.
        write("mount", "259:1 xfs\n");
        write("blocks", "524288\n");
        write("no-op", "");
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        remove("no-op");
        assert_eq!(expand_at(&req, &sys).await.unwrap(), 3221225472);
        write("mount", "259:1 unknown\n");
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        // Raw block volumes refresh capacity without filesystem commands.
        req.volume_capability = Some(crate::csi::VolumeCapability {
            access_type: Some(AccessType::Block(Default::default())),
            ..Default::default()
        });
        write("calls", "");
        assert_eq!(expand_at(&req, &sys).await.unwrap(), 3221225472);
        assert!(!calls().contains("growfs") && !calls().contains("resize2fs"));
        // An iSCSI disk resolves through its owning session and rescans only
        // sessions for the exact IQN, including custom configured prefixes.
        let disk = sys.join("devices/session7/target0/block/sda");
        std::fs::create_dir_all(&disk).unwrap();
        symlink(sys.join("devices/session7/target0"), disk.join("device")).unwrap();
        std::fs::remove_file(sys.join("dev/block/259:1")).unwrap();
        symlink(&disk, sys.join("dev/block/259:1")).unwrap();
        for (name, target) in [
            ("session7", "iqn.custom:vol"),
            ("session8", "iqn.custom:vol"),
            ("session9", "iqn.custom:other"),
        ] {
            let session = sys.join("class/iscsi_session").join(name);
            std::fs::create_dir_all(&session).unwrap();
            std::fs::write(session.join("targetname"), target).unwrap();
        }
        req.volume_capability = None;
        write("mount", "259:1 ext4\n");
        write("calls", "");
        assert_eq!(expand_at(&req, &sys).await.unwrap(), 3221225472);
        assert!(calls().contains("iscsiadm -m session -r 7 --rescan\n"));
        assert!(calls().contains("iscsiadm -m session -r 8 --rescan\n"));
        assert!(!calls().contains("-r 9"));
        std::fs::write(disk.join("partition"), "1").unwrap();
        assert_eq!(
            expand_at(&req, &sys).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }
}
