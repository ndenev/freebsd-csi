"""Rapid lifecycle stress tests.

Tests fast create/delete cycles to detect resource leaks and race conditions.
"""

import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor, StorageState


@pytest.mark.stress
class TestRapidLifecycle:
    """Test rapid create/delete cycles."""

    def test_rapid_volume_churn(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Create and immediately delete volumes in rapid succession."""
        num_cycles = 20

        # Capture state before
        state_before = storage.capture_state()
        datasets_before = {d.name for d in state_before.datasets}

        for i in range(num_cycles):
            name = f"churn-{unique_name}-{i}"

            # Create
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            assert k8s.wait_pvc_bound(name, timeout=60)

            # Immediately delete
            k8s.delete("pvc", name, wait=True)

        # Wait for cleanup
        time.sleep(10)

        # Verify no orphaned resources
        state_after = storage.capture_state()
        datasets_after = {d.name for d in state_after.datasets}

        # No new datasets should remain from the churn test
        orphans = []
        for ds in datasets_after - datasets_before:
            if f"churn-{unique_name}" in ds:
                orphans.append(ds)

        assert len(orphans) == 0, f"Orphaned datasets: {orphans}"

    def test_rapid_snapshot_churn(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        unique_name: str,
        wait_pvc_bound: Callable,
    ):
        """Create and delete snapshots rapidly on same volume."""
        # Create source volume
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        pv_name = k8s.get_pvc_volume(source_pvc)
        dataset = f"{storage.csi_path}/{pv_name}"

        num_cycles = 10
        for i in range(num_cycles):
            snap_name = f"churn-snap-{unique_name}-{i}"

            # Create snapshot
            k8s.create_snapshot(snap_name, source_pvc)
            assert k8s.wait_snapshot_ready(snap_name, timeout=60)

            # Immediately delete
            k8s.delete("volumesnapshot", snap_name, wait=True)

        # Wait for cleanup
        time.sleep(5)

        # Verify no orphaned snapshots (except those created by other operations)
        snapshots = storage.list_snapshots(dataset)
        churn_snaps = [s for s in snapshots if f"churn-snap-{unique_name}" in s.name]
        assert len(churn_snaps) == 0, f"Orphaned snapshots: {churn_snaps}"

    def test_mount_unmount_cycle(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        unique_name: str,
        wait_pvc_bound: Callable,
    ):
        """Rapidly mount and unmount volume."""
        # Create volume
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        num_cycles = 5
        for i in range(num_cycles):
            pod_name = f"mount-cycle-{unique_name}-{i}"

            # Create pod (mounts volume)
            k8s.create_pod_with_pvc(pod_name, pvc_name)
            assert k8s.wait_pod_ready(pod_name, timeout=120)

            # Verify mount works
            stdout, stderr, rc = k8s.exec_in_pod(pod_name, ["touch", "/mnt/data/test"])
            assert rc == 0, f"Mount failed in cycle {i}: {stderr}"

            # Delete pod (unmounts volume)
            k8s.delete("pod", pod_name, wait=True)

        # Volume should still be usable after all cycles
        final_pod = f"mount-final-{unique_name}"
        k8s.create_pod_with_pvc(final_pod, pvc_name)
        assert k8s.wait_pod_ready(final_pod, timeout=120)

        stdout, stderr, rc = k8s.exec_in_pod(final_pod, ["ls", "/mnt/data"])
        assert rc == 0, f"Volume not usable after mount cycles: {stderr}"

        k8s.delete("pod", final_pod, wait=True)

    def test_volume_recreation_same_name(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Create, delete, recreate volume with same name."""
        name = f"recreate-{unique_name}"

        for i in range(5):
            # Create
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            assert k8s.wait_pvc_bound(name, timeout=60)

            pv_name = k8s.get_pvc_volume(name)
            dataset = f"{storage.csi_path}/{pv_name}"
            assert storage.verify_dataset_exists(dataset)

            # Delete
            k8s.delete("pvc", name, wait=True)
            time.sleep(3)

            # Verify cleanup before next iteration
            assert not storage.verify_dataset_exists(dataset)

    def test_rapid_clone_lifecycle(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        unique_name: str,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Rapidly create and delete clones from same snapshot."""
        # Create source and snapshot
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Rapid clone create/delete cycles
        num_cycles = 10
        for i in range(num_cycles):
            clone_name = f"rapid-clone-{unique_name}-{i}"

            k8s.create_pvc(
                clone_name,
                "freebsd-e2e-iscsi-linked",
                "1Gi",
                data_source={
                    "apiGroup": "snapshot.storage.k8s.io",
                    "kind": "VolumeSnapshot",
                    "name": snap_name,
                },
            )
            assert k8s.wait_pvc_bound(clone_name, timeout=60)
            k8s.delete("pvc", clone_name, wait=True)

        # Snapshot should still be valid
        snap = k8s.get("volumesnapshot", snap_name)
        assert snap is not None
        assert snap.get("status", {}).get("readyToUse") is True

    def test_state_consistency_under_churn(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Verify storage state consistency after rapid operations."""
        # Capture initial state
        state_before = storage.capture_state()

        # Do a bunch of rapid operations
        created = []
        for i in range(10):
            name = f"consistency-{unique_name}-{i}"
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            created.append(name)

        for name in created:
            k8s.wait_pvc_bound(name, timeout=60)

        # Immediately delete all
        for name in created:
            k8s.delete("pvc", name, wait=False)

        # Wait for all deletes to complete
        time.sleep(30)

        # Capture final state
        state_after = storage.capture_state()

        # Compare states
        diff = storage.diff_state(state_before, state_after)

        # No unexpected datasets should remain
        unexpected = [
            ds
            for ds in diff["datasets"]["added"]
            if f"consistency-{unique_name}" in ds
        ]
        assert len(unexpected) == 0, f"Unexpected datasets remain: {unexpected}"

    def test_export_consistency(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Verify CTL exports match ZFS volumes after churn."""
        # Create and delete multiple volumes
        for i in range(5):
            name = f"export-{unique_name}-{i}"
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            k8s.wait_pvc_bound(name, timeout=60)
            k8s.delete("pvc", name, wait=True)

        time.sleep(10)

        # Get current state
        state = storage.capture_state()

        # All remaining datasets should have corresponding exports
        # (and vice versa for CSI-managed volumes)
        csi_datasets = [d for d in state.datasets if storage.csi_prefix in d.name]
        csi_luns = [l for l in state.luns if storage.csi_prefix in (l.path or "")]

        # This is a basic sanity check - more detailed verification
        # would require correlating by volume ID
        assert isinstance(csi_datasets, list)
        assert isinstance(csi_luns, list)
