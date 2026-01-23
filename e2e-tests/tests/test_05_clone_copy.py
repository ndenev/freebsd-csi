"""COPY clone mode tests.

Tests cloning from snapshots using zfs send/recv (slow, independent).
"""

import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


class TestCopyCloneMode:
    """Test COPY clone mode (zfs send/recv - slow, independent)."""

    @pytest.mark.slow
    def test_clone_from_snapshot_copy(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Clone from snapshot using COPY mode, verify no ZFS origin."""
        # Create source and snapshot
        source_pvc = pvc_factory("freebsd-e2e-iscsi-copy", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Create clone with COPY mode StorageClass
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-copy",  # Uses cloneMode: copy
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_name,
            },
            name_suffix="clone",
        )
        # COPY mode is slower, give more time
        assert wait_pvc_bound(clone_pvc, timeout=300)

        # Verify it's independent (no origin)
        clone_pv = k8s.get_pvc_volume(clone_pvc)
        clone_dataset = f"{storage.csi_path}/{clone_pv}"

        origin = storage.get_origin(clone_dataset)
        assert origin is None, f"COPY clone should not have origin, got: {origin}"

    @pytest.mark.slow
    def test_copy_clone_data_matches_snapshot(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
        wait_snapshot_ready: Callable,
    ):
        """COPY clone has correct data despite being independent."""
        # Create source with data
        source_pvc = pvc_factory("freebsd-e2e-iscsi-copy", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        pod = pod_factory(source_pvc, name_suffix="writer")
        assert wait_pod_ready(pod, timeout=120)

        test_data = "copy-clone-test-data"
        k8s.exec_in_pod(pod, ["sh", "-c", f"echo '{test_data}' > /mnt/data/test.txt"])

        # Create snapshot
        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        k8s.delete("pod", pod, wait=True)

        # Create COPY clone
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-copy",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_name,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(clone_pvc, timeout=300)

        # Verify data
        clone_pod = pod_factory(clone_pvc, name_suffix="reader")
        assert wait_pod_ready(clone_pod, timeout=120)

        stdout, _, rc = k8s.exec_in_pod(clone_pod, ["cat", "/mnt/data/test.txt"])
        assert rc == 0
        assert test_data in stdout

    @pytest.mark.slow
    def test_copy_clone_source_immediately_deletable(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Source can be deleted immediately after COPY clone completes."""
        # Create source
        source_pvc = pvc_factory("freebsd-e2e-iscsi-copy", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        source_pv = k8s.get_pvc_volume(source_pvc)
        source_dataset = f"{storage.csi_path}/{source_pv}"

        # Create snapshot and COPY clone
        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-copy",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_name,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(clone_pvc, timeout=300)

        clone_pv = k8s.get_pvc_volume(clone_pvc)
        clone_dataset = f"{storage.csi_path}/{clone_pv}"

        # Delete snapshot and source immediately
        k8s.delete("volumesnapshot", snap_name, wait=True)
        k8s.delete("pvc", source_pvc, wait=True)

        time.sleep(5)

        # Source should be gone
        assert not storage.verify_dataset_exists(source_dataset)

        # Clone should still exist and be independent
        assert storage.verify_dataset_exists(clone_dataset)
        assert storage.get_origin(clone_dataset) is None

    @pytest.mark.slow
    def test_copy_clone_size_independent(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """COPY clone uses its own storage space (not shared with source)."""
        # Create source
        source_pvc = pvc_factory("freebsd-e2e-iscsi-copy", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Create COPY clone
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-copy",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_name,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(clone_pvc, timeout=300)

        clone_pv = k8s.get_pvc_volume(clone_pvc)
        clone_dataset = f"{storage.csi_path}/{clone_pv}"

        # COPY clone should have volsize set (it's a real zvol, not a clone)
        info = storage.get_dataset_info(clone_dataset)
        assert info is not None
        assert info.volsize is not None
        assert info.volsize >= 1024**3  # At least 1GB
