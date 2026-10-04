//! Read-only filesystem probing. The blkid CLI collapses I/O failures and
//! "no signature" into the same exit status; the native API distinguishes them.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::ptr;

use super::PlatformResult;
use tonic::Status;

#[link(name = "blkid")]
unsafe extern "C" {
    fn blkid_new_probe_from_filename(filename: *const c_char) -> *mut c_void;
    fn blkid_free_probe(probe: *mut c_void);
    fn blkid_probe_get_size(probe: *mut c_void) -> i64;
    fn blkid_probe_enable_superblocks(probe: *mut c_void, enable: c_int) -> c_int;
    fn blkid_probe_set_superblocks_flags(probe: *mut c_void, flags: c_int) -> c_int;
    fn blkid_probe_enable_partitions(probe: *mut c_void, enable: c_int) -> c_int;
    fn blkid_do_safeprobe(probe: *mut c_void) -> c_int;
    fn blkid_probe_lookup_value(
        probe: *mut c_void,
        name: *const c_char,
        data: *mut *const c_char,
        len: *mut usize,
    ) -> c_int;
}

pub(super) fn needs_formatting(device: &str, fs_type: &str) -> PlatformResult<bool> {
    let path = CString::new(device)
        .map_err(|_| Status::invalid_argument("Device path contains a NUL byte"))?;

    // SAFETY: the path is NUL-terminated. Each probe is owned by this call and
    // freed after the closure, including error paths. Returned value pointers
    // are inspected only while the probe is alive and never retained.
    unsafe {
        let probe = blkid_new_probe_from_filename(path.as_ptr());
        if probe.is_null() {
            return Err(Status::internal(format!(
                "Cannot open device {device} for filesystem probing: {}",
                std::io::Error::last_os_error()
            )));
        }
        let result = (|| {
            if blkid_probe_get_size(probe) <= 0 {
                return Err(Status::failed_precondition("Device has no usable capacity"));
            }
            // BLKID_SUBLKS_TYPE | BLKID_SUBLKS_BADCSUM: even a damaged
            // filesystem signature must prevent automatic formatting.
            if blkid_probe_enable_superblocks(probe, 1) != 0
                || blkid_probe_set_superblocks_flags(probe, (1 << 5) | (1 << 10)) != 0
                || blkid_probe_enable_partitions(probe, 1) != 0
            {
                return Err(Status::internal("Cannot configure filesystem probe"));
            }

            let status = blkid_do_safeprobe(probe);
            let partitioned = blkid_probe_lookup_value(
                probe,
                c"PTTYPE".as_ptr(),
                ptr::null_mut(),
                ptr::null_mut(),
            ) == 0;
            let mut value = ptr::null();
            let filesystem =
                if blkid_probe_lookup_value(probe, c"TYPE".as_ptr(), &mut value, ptr::null_mut())
                    == 0
                    && !value.is_null()
                {
                    Some(CStr::from_ptr(value).to_str().map_err(|_| {
                        Status::internal("Filesystem probe returned an invalid type")
                    })?)
                } else {
                    None
                };
            classify_probe(status, partitioned, filesystem, fs_type)
        })();
        blkid_free_probe(probe);
        result
    }
}

fn classify_probe(
    status: c_int,
    partitioned: bool,
    filesystem: Option<&str>,
    requested: &str,
) -> PlatformResult<bool> {
    match status {
        1 if !partitioned && filesystem.is_none() => Ok(true),
        0 if !partitioned && filesystem == Some(requested) => Ok(false),
        -1 => Err(Status::internal(
            "Filesystem probe failed; refusing to format",
        )),
        _ => Err(Status::failed_precondition(format!(
            "Unsafe or incompatible device signature (probe={status}, partitioned={partitioned}, filesystem={filesystem:?}, requested={requested}); refusing to format"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_outcomes_fail_closed() {
        assert!(classify_probe(1, false, None, "ext4").unwrap());
        assert!(!classify_probe(0, false, Some("ext4"), "ext4").unwrap());
        for (status, partitioned, filesystem) in [
            (-1, false, None),         // I/O error, unlike the CLI's exit 2
            (-2, false, Some("ext4")), // ambiguous signatures
            (0, true, None),
            (0, true, Some("ext4")),
            (0, false, None),
            (0, false, Some("xfs")),
            (0, false, Some("crypto_LUKS")),
            (1, false, Some("ext4")),
            (4, false, None),
            (8, false, None),
        ] {
            assert!(classify_probe(status, partitioned, filesystem, "ext4").is_err());
        }
    }

    #[test]
    fn probe_real_images_without_mounting() {
        use std::fs::File;
        use std::io::{Seek, SeekFrom, Write};
        use std::process::Command;

        let path = std::env::temp_dir().join(format!("csi-probe-{}", uuid::Uuid::new_v4()));
        let device = path.to_str().unwrap();
        assert!(needs_formatting(device, "ext4").is_err());
        let mut file = File::create(&path).unwrap();
        assert!(needs_formatting(device, "ext4").is_err());
        file.set_len(16 * 1024 * 1024).unwrap();
        assert!(needs_formatting(device, "ext4").unwrap());

        // A partition table must not be mistaken for an empty device.
        let mut mbr = [0u8; 512];
        mbr[450] = 0x83;
        mbr[454..458].copy_from_slice(&1u32.to_le_bytes());
        mbr[458..462].copy_from_slice(&32767u32.to_le_bytes());
        mbr[510..512].copy_from_slice(&[0x55, 0xaa]);
        file.write_all(&mbr).unwrap();
        assert!(needs_formatting(device, "ext4").is_err());
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0u8; 512]).unwrap();

        let output = Command::new("mkfs.ext4")
            .args(["-F", device])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(!needs_formatting(device, "ext4").unwrap());
        assert!(needs_formatting(device, "xfs").is_err());
        drop(file);
        std::fs::remove_file(path).unwrap();
    }
}
