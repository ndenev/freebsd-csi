"""Volume lifecycle tests.

Tests CreateVolume, DeleteVolume, ExpandVolume, and data persistence.
"""

from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


class TestVolumeLifecycle:
    """Test basic volume create/delete/expand operations."""

    def test_create_volume_iscsi(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Create iSCSI volume, verify ZFS dataset and CTL export."""
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60), f"PVC {pvc_name} not bound"

        # Get the PV name
        pv_name = k8s.get_pvc_volume(pvc_name)
        assert pv_name is not None, "PVC not bound to PV"

        # Verify ZFS dataset exists
        dataset = f"{storage.csi_path}/{pv_name}"
        assert storage.verify_dataset_exists(dataset), f"Dataset {dataset} not found"

        # Verify volume is exported
        assert storage.verify_volume_exported(
            pv_name, "iscsi"
        ), f"Volume {pv_name} not exported via iSCSI"

    @pytest.mark.nvmeof
    def test_create_volume_nvmeof(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Create NVMeoF volume, verify ZFS dataset and NVMe controller."""
        pvc_name = pvc_factory("freebsd-e2e-nvmeof-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60), f"PVC {pvc_name} not bound"

        pv_name = k8s.get_pvc_volume(pvc_name)
        assert pv_name is not None

        # Verify ZFS dataset
        dataset = f"{storage.csi_path}/{pv_name}"
        assert storage.verify_dataset_exists(dataset)

        # Verify NVMeoF export
        assert storage.verify_volume_exported(pv_name, "nvmeof")

    def test_delete_volume(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pv_deleted: Callable,
    ):
        """Delete volume, verify ZFS dataset and export removed."""
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Verify it exists before delete
        assert storage.verify_dataset_exists(dataset)

        # Delete PVC (with Delete reclaim policy, PV should also be deleted)
        k8s.delete("pvc", pvc_name, wait=True)

        # Wait for PV to be deleted (indicates backend cleanup is complete)
        assert wait_pv_deleted(pv_name, timeout=60), f"PV {pv_name} not deleted"

        # Verify ZFS dataset is gone
        assert not storage.verify_dataset_exists(dataset), "Dataset not cleaned up"

        # Verify export is gone
        assert storage.verify_volume_not_exported(pv_name), "Export not cleaned up"

    def test_expand_volume(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        pod_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
        wait_pvc_resized: Callable,
    ):
        """Expand volume online, verify new size in ZFS."""
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        # Get initial size
        info_before = storage.get_dataset_info(dataset)
        assert info_before is not None
        initial_size = info_before.volsize

        # Create pod to attach volume (needed for online expansion)
        pod_name = pod_factory(pvc_name)
        assert wait_pod_ready(pod_name, timeout=120)

        # Expand PVC to 2Gi
        k8s.expand_pvc(pvc_name, "2Gi")

        # Wait for PVC expansion to complete
        # Expansion can take time if resizer has a queue or needs multiple syncs
        assert wait_pvc_resized(pvc_name, "2Gi", timeout=180), "PVC expansion failed"

        # Verify new size in ZFS
        info_after = storage.get_dataset_info(dataset)
        assert info_after is not None
        new_size = info_after.volsize

        # New size should be at least 2GB
        assert new_size >= 2 * 1024**3, f"Volume not expanded: {new_size}"
        assert new_size > initial_size, "Volume size unchanged"

    def test_volume_data_persistence(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
    ):
        """Write data, delete pod, remount, verify data persists."""
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        # Create first pod and write data
        pod1_name = pod_factory(pvc_name, name_suffix="writer")
        assert wait_pod_ready(pod1_name, timeout=120)

        # Write test data
        test_data = "persistence-test-data-12345"
        stdout, stderr, rc = k8s.exec_in_pod(
            pod1_name,
            ["sh", "-c", f"echo '{test_data}' > /mnt/data/test.txt"],
        )
        assert rc == 0, f"Failed to write data: {stderr}"

        # Verify data was written
        stdout, stderr, rc = k8s.exec_in_pod(pod1_name, ["cat", "/mnt/data/test.txt"])
        assert rc == 0
        assert test_data in stdout

        # Delete the pod
        k8s.delete("pod", pod1_name, wait=True)

        # Create second pod and verify data
        pod2_name = pod_factory(pvc_name, name_suffix="reader")
        assert wait_pod_ready(pod2_name, timeout=120)

        stdout, stderr, rc = k8s.exec_in_pod(pod2_name, ["cat", "/mnt/data/test.txt"])
        assert rc == 0, f"Failed to read data: {stderr}"
        assert test_data in stdout, "Data not persisted"

    def test_multiple_volumes(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Create multiple volumes, verify independent datasets."""
        pvcs = []
        for i in range(3):
            pvc_name = pvc_factory(
                "freebsd-e2e-iscsi-linked", "1Gi", name_suffix=f"multi-{i}"
            )
            pvcs.append(pvc_name)

        # Wait for all to be bound
        for pvc_name in pvcs:
            assert wait_pvc_bound(pvc_name, timeout=60)

        # Verify each has its own dataset
        pv_names = [k8s.get_pvc_volume(pvc) for pvc in pvcs]
        assert len(set(pv_names)) == 3, "PVs not unique"

        for pv_name in pv_names:
            dataset = f"{storage.csi_path}/{pv_name}"
            assert storage.verify_dataset_exists(dataset)

    def test_volume_with_specific_size(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Create volume with specific size, verify ZFS reports correct volsize."""
        # Request 500Mi
        pvc_name = pvc_factory("freebsd-e2e-iscsi-linked", "500Mi")
        assert wait_pvc_bound(pvc_name, timeout=60)

        pv_name = k8s.get_pvc_volume(pvc_name)
        dataset = f"{storage.csi_path}/{pv_name}"

        info = storage.get_dataset_info(dataset)
        assert info is not None
        assert info.volsize is not None

        # Size should be at least 500MB (may be rounded up)
        expected_min = 500 * 1024 * 1024
        assert info.volsize >= expected_min, f"Volume too small: {info.volsize}"
