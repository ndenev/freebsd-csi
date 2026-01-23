"""LINKED clone mode tests.

Tests cloning from snapshots using zfs clone (fast, with dependency).
"""

import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


class TestLinkedCloneMode:
    """Test LINKED clone mode (zfs clone - fast, with dependency)."""

    def test_clone_from_snapshot_linked(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Clone from snapshot using LINKED mode, verify ZFS clone has origin."""
        # Create source and snapshot
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Create clone from snapshot (LINKED mode is default)
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
        assert wait_pvc_bound(clone_pvc, timeout=60)

        # Verify it's a ZFS clone (has origin)
        clone_pv = k8s.get_pvc_volume(clone_pvc)
        clone_dataset = f"{storage.csi_path}/{clone_pv}"

        origin = storage.get_origin(clone_dataset)
        assert origin is not None, "Clone should have origin (ZFS clone)"

    def test_linked_clone_data_matches_snapshot(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
        wait_snapshot_ready: Callable,
    ):
        """LINKED clone has correct data from snapshot."""
        # Create source with data
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        pod = pod_factory(source_pvc, name_suffix="writer")
        assert wait_pod_ready(pod, timeout=120)

        test_data = "linked-clone-test-data"
        k8s.exec_in_pod(pod, ["sh", "-c", f"echo '{test_data}' > /mnt/data/test.txt"])

        # Create snapshot
        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        k8s.delete("pod", pod, wait=True)

        # Create clone
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
        assert wait_pvc_bound(clone_pvc, timeout=60)

        # Verify data
        clone_pod = pod_factory(clone_pvc, name_suffix="reader")
        assert wait_pod_ready(clone_pod, timeout=120)

        stdout, _, rc = k8s.exec_in_pod(clone_pod, ["cat", "/mnt/data/test.txt"])
        assert rc == 0
        assert test_data in stdout

    def test_linked_clone_is_writable(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
        wait_snapshot_ready: Callable,
    ):
        """LINKED clone can be written to independently."""
        # Create source and snapshot
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Create clone
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
        assert wait_pvc_bound(clone_pvc, timeout=60)

        # Write to clone
        clone_pod = pod_factory(clone_pvc)
        assert wait_pod_ready(clone_pod, timeout=120)

        clone_data = "clone-specific-data"
        stdout, stderr, rc = k8s.exec_in_pod(
            clone_pod,
            ["sh", "-c", f"echo '{clone_data}' > /mnt/data/clone.txt"],
        )
        assert rc == 0, f"Failed to write to clone: {stderr}"

        # Verify write succeeded
        stdout, _, rc = k8s.exec_in_pod(clone_pod, ["cat", "/mnt/data/clone.txt"])
        assert rc == 0
        assert clone_data in stdout

    def test_delete_source_with_linked_clone(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Delete source volume - should auto-promote clone."""
        # Create source
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        source_pv = k8s.get_pvc_volume(source_pvc)

        # Create snapshot and clone
        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

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
        assert wait_pvc_bound(clone_pvc, timeout=60)

        clone_pv = k8s.get_pvc_volume(clone_pvc)
        clone_dataset = f"{storage.csi_path}/{clone_pv}"

        # Verify clone has origin before delete
        origin_before = storage.get_origin(clone_dataset)
        assert origin_before is not None

        # Delete snapshot first, then source
        k8s.delete("volumesnapshot", snap_name, wait=True)
        time.sleep(2)
        k8s.delete("pvc", source_pvc, wait=True)
        time.sleep(5)

        # Clone should still exist
        assert storage.verify_dataset_exists(clone_dataset), "Clone deleted unexpectedly"

        # After auto-promote, clone may no longer have origin
        # (or origin points to a local snapshot)
        # Main point: clone survives source deletion

    def test_multiple_clones_from_one_snapshot(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Create multiple clones from single snapshot."""
        # Create source and snapshot
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Create multiple clones
        clones = []
        for i in range(3):
            clone_pvc = pvc_factory(
                "freebsd-e2e-iscsi-linked",
                "1Gi",
                data_source={
                    "apiGroup": "snapshot.storage.k8s.io",
                    "kind": "VolumeSnapshot",
                    "name": snap_name,
                },
                name_suffix=f"clone-{i}",
            )
            clones.append(clone_pvc)
            assert wait_pvc_bound(clone_pvc, timeout=60)

        # Verify all clones exist with origins
        for clone_pvc in clones:
            clone_pv = k8s.get_pvc_volume(clone_pvc)
            clone_dataset = f"{storage.csi_path}/{clone_pv}"

            assert storage.verify_dataset_exists(clone_dataset)
            assert storage.get_origin(clone_dataset) is not None
