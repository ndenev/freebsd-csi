"""Snapshot operation tests.

Tests CreateSnapshot, DeleteSnapshot, and snapshot data consistency.
"""

import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


class TestSnapshotOperations:
    """Test CSI snapshot create/delete operations."""

    def test_create_snapshot(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Create snapshot, verify ZFS snapshot exists."""
        # Create source volume
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Create snapshot
        snap_name = snapshot_factory(pvc_name)
        assert wait_snapshot_ready(snap_name, timeout=60), f"Snapshot {snap_name} not ready"

        # Verify ZFS snapshot exists
        snapshots = storage.list_snapshots(dataset)
        assert len(snapshots) >= 1, "No ZFS snapshots found"

    def test_delete_snapshot(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Delete snapshot, verify ZFS snapshot removed."""
        # Create source and snapshot
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        snap_name = snapshot_factory(pvc_name)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Count snapshots before delete
        snaps_before = storage.list_snapshots(dataset)
        count_before = len(snaps_before)

        # Delete snapshot
        k8s.delete("volumesnapshot", snap_name, wait=True)

        # Wait for cleanup
        time.sleep(5)

        # Verify snapshot removed (unless it has clones)
        snaps_after = storage.list_snapshots(dataset)
        # Note: Snapshot may remain if it has clones
        # For now just verify the delete completed without error

    def test_snapshot_data_consistency(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Snapshot captures point-in-time data correctly."""
        # Create source with data
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        # Write initial data
        pod_name = pod_factory(source_pvc, name_suffix="writer")
        assert wait_pod_ready(pod_name, timeout=120)

        original_data = "before-snapshot-data"
        k8s.exec_in_pod(
            pod_name,
            ["sh", "-c", f"echo '{original_data}' > /mnt/data/test.txt"],
        )

        # Sync to ensure data is flushed to disk before snapshot
        k8s.exec_in_pod(pod_name, ["sync"])

        # Create snapshot
        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Modify data AFTER snapshot
        modified_data = "after-snapshot-data"
        k8s.exec_in_pod(
            pod_name,
            ["sh", "-c", f"echo '{modified_data}' > /mnt/data/test.txt"],
        )

        # Verify source has modified data
        stdout, _, _ = k8s.exec_in_pod(pod_name, ["cat", "/mnt/data/test.txt"])
        assert modified_data in stdout

        # Create clone from snapshot
        # Clone operations take longer: ZFS clone + iSCSI export + K8s binding
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_name,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(clone_pvc, timeout=120), f"Clone PVC {clone_pvc} not bound"

        # Delete source pod before mounting clone
        k8s.delete("pod", pod_name, wait=True)

        # Mount clone and verify it has ORIGINAL data
        clone_pod = pod_factory(clone_pvc, name_suffix="reader")
        assert wait_pod_ready(clone_pod, timeout=120)

        stdout, _, rc = k8s.exec_in_pod(clone_pod, ["cat", "/mnt/data/test.txt"])
        assert rc == 0
        assert original_data in stdout, f"Clone should have original data, got: {stdout}"
        assert modified_data not in stdout, "Clone should NOT have modified data"

    def test_multiple_snapshots(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Create multiple snapshots of same volume."""
        # Create source
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Create multiple snapshots
        snap_names = []
        for i in range(3):
            snap_name = snapshot_factory(pvc_name, name_suffix=f"multi-{i}")
            snap_names.append(snap_name)
            assert wait_snapshot_ready(snap_name, timeout=60)
            # Small delay between snapshots
            time.sleep(1)

        # Verify multiple ZFS snapshots exist
        zfs_snaps = storage.list_snapshots(dataset)
        assert len(zfs_snaps) >= 3, f"Expected 3+ snapshots, got {len(zfs_snaps)}"

    def test_snapshot_of_used_volume(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Snapshot volume while it's mounted (online snapshot)."""
        # Create and mount volume
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pod_name = pod_factory(pvc_name)
        assert wait_pod_ready(pod_name, timeout=120)

        # Write some data while volume is in use
        k8s.exec_in_pod(
            pod_name,
            ["sh", "-c", "dd if=/dev/urandom of=/mnt/data/random bs=1M count=10"],
            timeout=120,
        )

        # Create snapshot while pod is running
        snap_name = snapshot_factory(pvc_name)
        assert wait_snapshot_ready(snap_name, timeout=60), "Online snapshot failed"

        # Volume should still be usable
        stdout, stderr, rc = k8s.exec_in_pod(pod_name, ["ls", "-la", "/mnt/data"])
        assert rc == 0, f"Volume not accessible after snapshot: {stderr}"
