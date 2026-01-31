"""Reclaim policy tests.

Tests Delete vs Retain reclaim policies.
"""

from typing import Callable, Generator

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


@pytest.fixture
def retain_cleanup(
    k8s: K8sClient, storage: StorageMonitor
) -> Generator[list[str], None, None]:
    """Track and clean up Retain policy resources.

    Retain policy volumes need special cleanup because Kubernetes doesn't
    call DeleteVolume when PVs are deleted. This fixture ensures cleanup
    happens even if the test fails.

    Yields:
        List to append PV names that need Retain policy cleanup
    """
    pvs_to_cleanup: list[str] = []
    yield pvs_to_cleanup

    # Cleanup runs even on test failure
    for pv_name in pvs_to_cleanup:
        try:
            # 1. Delete PV from Kubernetes (if it still exists)
            k8s.delete("pv", pv_name, wait=True, ignore_not_found=True)
            # 2. Clean up backend storage directly via ctld-agent
            storage.cleanup_volume(pv_name)
        except Exception as e:
            # Log but don't fail - we want to attempt all cleanups
            print(f"Warning: Failed to cleanup retain volume {pv_name}: {e}")


class TestReclaimPolicy:
    """Test Delete vs Retain reclaim policies."""

    def test_delete_policy(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pv_deleted: Callable,
    ):
        """Delete policy removes PV and backend storage."""
        # Use StorageClass with Delete policy
        pvc_name = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi"
        )  # reclaimPolicy: Delete
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Verify resources exist
        assert k8s.get("pv", pv_name) is not None
        assert storage.verify_dataset_exists(dataset)

        # Delete PVC
        k8s.delete("pvc", pvc_name, wait=True)

        # Wait for PV to be deleted (includes backend cleanup)
        assert wait_pv_deleted(pv_name, timeout=60), "PV not deleted with Delete policy"

        # Verify backend is gone
        assert not storage.verify_dataset_exists(dataset), "Dataset not deleted"

    def test_retain_policy(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
        retain_cleanup: list[str],
    ):
        """Retain policy keeps PV and backend storage after PVC delete."""
        # Use StorageClass with Retain policy
        pvc_name = pvc_factory(
            "freebsd-e2e-iscsi-retain", "1Gi"
        )  # reclaimPolicy: Retain
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Register for cleanup (runs even on test failure)
        retain_cleanup.append(pv_name)

        # Delete PVC and wait for PV to transition to Released
        k8s.delete("pvc", pvc_name, wait=True)

        # Wait for PV phase to become Released
        assert k8s.wait_for(
            "pv", pv_name, "jsonpath={.status.phase}=Released", timeout=30
        ), "PV did not transition to Released"

        # PV should still exist with Released status
        pv = k8s.get("pv", pv_name)
        assert pv is not None, "PV deleted with Retain policy"

        # Backend storage should still exist
        assert storage.verify_dataset_exists(dataset), "Dataset deleted with Retain"
        assert storage.verify_volume_exported(
            pv_name, "iscsi"
        ), "Export removed with Retain"

        # Cleanup is handled by retain_cleanup fixture

    def test_delete_policy_with_snapshots(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
        wait_pv_deleted: Callable,
    ):
        """Delete policy with snapshots - volume deleted when last snapshot gone."""
        # Create volume and snapshot
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)

        snap_name = snapshot_factory(pvc_name)
        assert wait_snapshot_ready(snap_name, timeout=60)

        # Delete PVC first
        k8s.delete("pvc", pvc_name, wait=True)

        # PV may still exist due to snapshot dependency
        # This depends on the VolumeSnapshotContent deletion policy

        # Delete snapshot and wait for cascading cleanup
        k8s.delete("volumesnapshot", snap_name, wait=True)

        # Wait for PV to be deleted (cascading from snapshot deletion)
        wait_pv_deleted(pv_name, timeout=60)

    def test_mixed_policies_independent(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pv_deleted: Callable,
        retain_cleanup: list[str],
    ):
        """Volumes with different reclaim policies behave independently."""
        # Create one with Delete and one with Retain
        delete_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi", name_suffix="delete"
        )
        retain_pvc = pvc_factory(
            "freebsd-e2e-iscsi-retain", "1Gi", name_suffix="retain"
        )

        assert wait_pvc_bound(delete_pvc, timeout=60)
        assert wait_pvc_bound(retain_pvc, timeout=60)

        delete_pv = k8s.get_pvc_volume(delete_pvc)
        retain_pv = k8s.get_pvc_volume(retain_pvc)

        # Register Retain policy PV for cleanup (runs even on test failure)
        retain_cleanup.append(retain_pv)

        delete_dataset = f"{storage.csi_path}/{delete_pv}"
        retain_dataset = f"{storage.csi_path}/{retain_pv}"

        # Delete both PVCs
        k8s.delete("pvc", delete_pvc, wait=True)
        k8s.delete("pvc", retain_pvc, wait=True)

        # Wait for Delete policy PV to be removed
        assert wait_pv_deleted(delete_pv, timeout=60), "Delete policy PV not deleted"
        assert not storage.verify_dataset_exists(delete_dataset)

        # Retain policy volume should remain
        assert k8s.get("pv", retain_pv) is not None
        assert storage.verify_dataset_exists(retain_dataset)

        # Cleanup is handled by retain_cleanup fixture
