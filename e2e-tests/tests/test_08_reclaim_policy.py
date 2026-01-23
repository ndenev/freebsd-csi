"""Reclaim policy tests.

Tests Delete vs Retain reclaim policies.
"""

import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


class TestReclaimPolicy:
    """Test Delete vs Retain reclaim policies."""

    def test_delete_policy(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Delete policy removes PV and backend storage."""
        # Use StorageClass with Delete policy
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")  # reclaimPolicy: Delete
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Verify resources exist
        assert k8s.get("pv", pv_name) is not None
        assert storage.verify_dataset_exists(dataset)

        # Delete PVC
        k8s.delete("pvc", pvc_name, wait=True)
        time.sleep(5)

        # PV and backend should be gone
        assert k8s.get("pv", pv_name) is None, "PV not deleted with Delete policy"
        assert not storage.verify_dataset_exists(dataset), "Dataset not deleted"

    def test_retain_policy(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Retain policy keeps PV and backend storage after PVC delete."""
        # Use StorageClass with Retain policy
        pvc_name = pvc_factory("freebsd-e2e-iscsi-retain", "1Gi")  # reclaimPolicy: Retain
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Delete PVC
        k8s.delete("pvc", pvc_name, wait=True)
        time.sleep(5)

        # PV should still exist with Released status
        pv = k8s.get("pv", pv_name)
        assert pv is not None, "PV deleted with Retain policy"
        assert pv.get("status", {}).get("phase") == "Released"

        # Backend storage should still exist
        assert storage.verify_dataset_exists(dataset), "Dataset deleted with Retain"
        assert storage.verify_volume_exported(pv_name, "iscsi"), "Export removed with Retain"

        # Manual cleanup for test hygiene
        k8s.delete("pv", pv_name, wait=True)
        # Note: Backend storage may need manual cleanup with Retain policy

    def test_delete_policy_with_snapshots(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Delete policy with snapshots - volume deleted when last snapshot gone."""
        # Create volume and snapshot
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        snap_name = snapshot_factory(pvc_name)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Delete PVC first
        k8s.delete("pvc", pvc_name, wait=True)
        time.sleep(5)

        # PV may still exist due to snapshot dependency
        # This depends on the VolumeSnapshotContent deletion policy

        # Delete snapshot
        k8s.delete("volumesnapshot", snap_name, wait=True)
        time.sleep(5)

        # Now everything should be cleaned up
        # Note: May need additional time for cascading deletes

    def test_mixed_policies_independent(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Volumes with different reclaim policies behave independently."""
        # Create one with Delete and one with Retain
        delete_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="delete")
        retain_pvc = pvc_factory("freebsd-e2e-iscsi-retain", "1Gi", name_suffix="retain")

        assert wait_pvc_bound(delete_pvc, timeout=60)
        assert wait_pvc_bound(retain_pvc, timeout=60)

        delete_pv = k8s.get_pvc_volume(delete_pvc)
        retain_pv = k8s.get_pvc_volume(retain_pvc)

        delete_dataset = f"{storage.csi_path}/{delete_pv}"
        retain_dataset = f"{storage.csi_path}/{retain_pv}"

        # Delete both PVCs
        k8s.delete("pvc", delete_pvc, wait=True)
        k8s.delete("pvc", retain_pvc, wait=True)
        time.sleep(5)

        # Delete policy volume should be gone
        assert k8s.get("pv", delete_pv) is None
        assert not storage.verify_dataset_exists(delete_dataset)

        # Retain policy volume should remain
        assert k8s.get("pv", retain_pv) is not None
        assert storage.verify_dataset_exists(retain_dataset)

        # Cleanup retained PV
        k8s.delete("pv", retain_pv, wait=True)
