"""PVC-to-PVC cloning tests.

Tests cloning directly from PVC (not snapshot) as dataSource.
Internal temp snapshot (pvc-clone-*) should be created and cleaned up.
"""

from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


class TestPvcCloning:
    """Test cloning from PVC (not snapshot) as dataSource."""

    def test_clone_from_pvc(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        pod_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
    ):
        """Clone directly from PVC (creates temp snapshot internally)."""
        # Create source with data
        source_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source"
        )
        assert wait_pvc_bound(source_pvc, timeout=60)

        pod = pod_factory(source_pvc, name_suffix="writer")
        assert wait_pod_ready(pod, timeout=120)

        source_data = "pvc-clone-source-data"
        k8s.exec_in_pod(pod, ["sh", "-c", f"echo '{source_data}' > /mnt/data/test.txt"])

        k8s.delete("pod", pod, wait=True)

        # Clone from PVC (not snapshot)
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "kind": "PersistentVolumeClaim",
                "name": source_pvc,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(
            clone_pvc, timeout=120
        ), f"Clone PVC {clone_pvc} not bound"

        # Verify clone has data
        clone_pod = pod_factory(clone_pvc, name_suffix="reader")
        assert wait_pod_ready(clone_pod, timeout=120)

        stdout, _, rc = k8s.exec_in_pod(clone_pod, ["cat", "/mnt/data/test.txt"])
        assert rc == 0
        assert source_data in stdout

    def test_pvc_clone_creates_temp_snapshot(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """PVC cloning creates internal pvc-clone-* snapshot."""
        # Create source
        source_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source"
        )
        assert wait_pvc_bound(source_pvc, timeout=60)

        source_pv = k8s.get_pvc_volume(source_pvc)
        source_dataset = f"{storage.csi_path}/{source_pv}"

        # Count snapshots before
        snaps_before = storage.list_snapshots(source_dataset)
        pvc_clone_snaps_before = [
            s for s in snaps_before if "pvc-clone-" in s.snap_name
        ]

        # Create clone from PVC
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "kind": "PersistentVolumeClaim",
                "name": source_pvc,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(
            clone_pvc, timeout=120
        ), f"Clone PVC {clone_pvc} not bound"

        # Verify temp snapshot was created
        snaps_after = storage.list_snapshots(source_dataset)
        pvc_clone_snaps_after = [s for s in snaps_after if "pvc-clone-" in s.snap_name]

        assert len(pvc_clone_snaps_after) > len(
            pvc_clone_snaps_before
        ), "No pvc-clone-* snapshot created"

    def test_pvc_clone_temp_snapshot_cleanup(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pv_deleted: Callable,
    ):
        """Temp snapshot (pvc-clone-*) cleaned up when clone deleted."""
        # Create source
        source_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source"
        )
        assert wait_pvc_bound(source_pvc, timeout=60)

        source_pv = k8s.get_pvc_volume(source_pvc)
        source_dataset = f"{storage.csi_path}/{source_pv}"

        # Create clone from PVC
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "kind": "PersistentVolumeClaim",
                "name": source_pvc,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(
            clone_pvc, timeout=120
        ), f"Clone PVC {clone_pvc} not bound"

        clone_pv = k8s.get_pvc_volume(clone_pvc)

        # Verify temp snapshot exists
        snaps = storage.list_snapshots(source_dataset)
        pvc_clone_snaps = [s for s in snaps if "pvc-clone-" in s.snap_name]
        assert len(pvc_clone_snaps) >= 1

        # Delete clone and wait for PV cleanup
        k8s.delete("pvc", clone_pvc, wait=True)
        assert wait_pv_deleted(clone_pv, timeout=60), "Clone PV not deleted"

        # Temp snapshot should be cleaned up
        snaps_after = storage.list_snapshots(source_dataset)
        pvc_clone_snaps_after = [s for s in snaps_after if "pvc-clone-" in s.snap_name]

        # Should have fewer temp snapshots (or none if only one clone existed)
        assert len(pvc_clone_snaps_after) < len(
            pvc_clone_snaps
        ), "Temp snapshot not cleaned up"

    def test_pvc_clone_independence(
        self,
        k8s: K8sClient,
        pvc_factory: Callable,
        pod_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
    ):
        """Clone from PVC is independent - can modify without affecting source."""
        # Create source with data
        source_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source"
        )
        assert wait_pvc_bound(source_pvc, timeout=60)

        source_pod = pod_factory(source_pvc, name_suffix="source-pod")
        assert wait_pod_ready(source_pod, timeout=120)

        original_data = "original-source-data"
        k8s.exec_in_pod(
            source_pod,
            ["sh", "-c", f"echo '{original_data}' > /mnt/data/test.txt"],
        )

        k8s.delete("pod", source_pod, wait=True)

        # Create clone
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "kind": "PersistentVolumeClaim",
                "name": source_pvc,
            },
            name_suffix="clone",
        )
        assert wait_pvc_bound(
            clone_pvc, timeout=120
        ), f"Clone PVC {clone_pvc} not bound"

        # Modify clone
        clone_pod = pod_factory(clone_pvc, name_suffix="clone-pod")
        assert wait_pod_ready(clone_pod, timeout=120)

        clone_data = "modified-clone-data"
        k8s.exec_in_pod(
            clone_pod,
            ["sh", "-c", f"echo '{clone_data}' > /mnt/data/test.txt"],
        )

        # Verify clone has new data
        stdout, _, _ = k8s.exec_in_pod(clone_pod, ["cat", "/mnt/data/test.txt"])
        assert clone_data in stdout

        k8s.delete("pod", clone_pod, wait=True)

        # Verify source still has original data
        source_pod2 = pod_factory(source_pvc, name_suffix="source-verify")
        assert wait_pod_ready(source_pod2, timeout=120)

        stdout, _, _ = k8s.exec_in_pod(source_pod2, ["cat", "/mnt/data/test.txt"])
        assert original_data in stdout
        assert clone_data not in stdout

    def test_multiple_clones_from_same_pvc(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Create multiple clones from same PVC."""
        # Create source
        source_pvc = pvc_factory(
            "freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source"
        )
        assert wait_pvc_bound(source_pvc, timeout=60)

        source_pv = k8s.get_pvc_volume(source_pvc)
        source_dataset = f"{storage.csi_path}/{source_pv}"

        # Create multiple clones
        clones = []
        for i in range(3):
            clone_pvc = pvc_factory(
                "freebsd-e2e-iscsi-linked",
                "1Gi",
                data_source={
                    "kind": "PersistentVolumeClaim",
                    "name": source_pvc,
                },
                name_suffix=f"clone-{i}",
            )
            clones.append(clone_pvc)
            assert wait_pvc_bound(
                clone_pvc, timeout=120
            ), f"Clone PVC {clone_pvc} not bound"

        # Each clone should have its own temp snapshot (or share one)
        # Main verification: all clones exist
        for clone_pvc in clones:
            clone_pv = k8s.get_pvc_volume(clone_pvc)
            clone_dataset = f"{storage.csi_path}/{clone_pv}"
            assert storage.verify_dataset_exists(clone_dataset)

    def test_pvc_clone_with_copy_mode(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        wait_pvc_bound: Callable,
    ):
        """Clone from PVC using COPY mode creates independent volume."""
        # Create source
        source_pvc = pvc_factory("freebsd-e2e-iscsi-copy", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        # Create COPY clone from PVC
        clone_pvc = pvc_factory(
            "freebsd-e2e-iscsi-copy",
            "1Gi",
            data_source={
                "kind": "PersistentVolumeClaim",
                "name": source_pvc,
            },
            name_suffix="clone",
        )
        # COPY mode is slower
        assert wait_pvc_bound(clone_pvc, timeout=300)

        # Verify it's independent (no origin)
        clone_pv = k8s.get_pvc_volume(clone_pvc)
        clone_dataset = f"{storage.csi_path}/{clone_pv}"

        origin = storage.get_origin(clone_dataset)
        assert origin is None, "COPY clone should not have origin"
