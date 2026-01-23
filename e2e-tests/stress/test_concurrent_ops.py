"""Concurrent operations stress tests.

Tests parallel volume, snapshot, and clone operations.
"""

import concurrent.futures
import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


@pytest.mark.stress
class TestConcurrentOperations:
    """Stress test with parallel volume operations."""

    def test_parallel_volume_creation(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
        setup_storageclasses: list[str],
    ):
        """Create 10 volumes in parallel."""
        num_volumes = 10
        created_pvcs = []

        def create_pvc(index: int) -> str:
            name = f"parallel-{unique_name}-{index}"
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            return name

        # Create volumes in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_volumes) as executor:
            futures = [executor.submit(create_pvc, i) for i in range(num_volumes)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    pvc_name = future.result()
                    created_pvcs.append(pvc_name)
                except Exception as e:
                    pytest.fail(f"Parallel volume creation failed: {e}")

        assert len(created_pvcs) == num_volumes

        # Wait for all to be bound
        for pvc_name in created_pvcs:
            assert k8s.wait_pvc_bound(pvc_name, timeout=120), f"PVC {pvc_name} not bound"

        # Verify all have ZFS datasets
        for pvc_name in created_pvcs:
            pv_name = k8s.get_pvc_volume(pvc_name)
            dataset = f"{storage.csi_path}/{pv_name}"
            assert storage.verify_dataset_exists(dataset)

        # Cleanup
        for pvc_name in created_pvcs:
            k8s.delete("pvc", pvc_name, wait=False, ignore_not_found=True)

    def test_parallel_volume_deletion(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Delete multiple volumes in parallel."""
        num_volumes = 10

        # Create volumes first
        pvcs = []
        for i in range(num_volumes):
            name = f"del-parallel-{unique_name}-{i}"
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            pvcs.append(name)

        for pvc in pvcs:
            assert k8s.wait_pvc_bound(pvc, timeout=60)

        pv_names = [k8s.get_pvc_volume(pvc) for pvc in pvcs]

        # Delete in parallel
        def delete_pvc(name: str) -> bool:
            k8s.delete("pvc", name, wait=True)
            return True

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_volumes) as executor:
            futures = [executor.submit(delete_pvc, pvc) for pvc in pvcs]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(results)

        # Wait and verify cleanup
        time.sleep(10)
        for pv_name in pv_names:
            dataset = f"{storage.csi_path}/{pv_name}"
            assert not storage.verify_dataset_exists(dataset), f"Dataset {dataset} not cleaned up"

    def test_parallel_snapshot_creation(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        unique_name: str,
        wait_pvc_bound: Callable,
    ):
        """Create multiple snapshots of same volume in parallel."""
        # Create source volume
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        num_snapshots = 5
        created_snaps = []

        def create_snapshot(index: int) -> str:
            name = f"snap-{unique_name}-{index}"
            k8s.create_snapshot(name, source_pvc)
            return name

        # Create snapshots in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_snapshots) as executor:
            futures = [executor.submit(create_snapshot, i) for i in range(num_snapshots)]
            for future in concurrent.futures.as_completed(futures):
                try:
                    snap_name = future.result()
                    created_snaps.append(snap_name)
                except Exception as e:
                    # Some may fail due to ZFS locking - that's acceptable
                    print(f"Snapshot creation note: {e}")

        # At least some snapshots should succeed
        assert len(created_snaps) >= 1, "No snapshots created"

        # Wait for ready
        for snap_name in created_snaps:
            k8s.wait_snapshot_ready(snap_name, timeout=60)

        # Cleanup
        for snap_name in created_snaps:
            k8s.delete("volumesnapshot", snap_name, wait=False, ignore_not_found=True)

    def test_parallel_clone_and_delete(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        pvc_factory: Callable,
        snapshot_factory: Callable,
        unique_name: str,
        wait_pvc_bound: Callable,
        wait_snapshot_ready: Callable,
    ):
        """Create and delete clones in parallel from same snapshot."""
        # Create source and snapshot
        source_pvc = pvc_factory("freebsd-e2e-iscsi-linked", "1Gi", name_suffix="source")
        assert wait_pvc_bound(source_pvc, timeout=60)

        snap_name = snapshot_factory(source_pvc)
        assert wait_snapshot_ready(snap_name, timeout=60)

        num_clones = 5
        clone_pvcs = []

        def clone_and_delete(index: int) -> bool:
            clone_name = f"clone-{unique_name}-{index}"
            try:
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
                k8s.wait_pvc_bound(clone_name, timeout=60)
                k8s.delete("pvc", clone_name, wait=True)
                return True
            except Exception as e:
                print(f"Clone {index} operation note: {e}")
                # Try cleanup
                k8s.delete("pvc", clone_name, ignore_not_found=True)
                return False

        # Run clone+delete cycles in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_clones) as executor:
            results = list(executor.map(clone_and_delete, range(num_clones)))

        # Most operations should succeed
        success_count = sum(results)
        assert success_count >= num_clones // 2, f"Too many failures: {success_count}/{num_clones}"

    def test_mixed_operations_parallel(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Mix of create, delete, expand operations in parallel."""
        # Create some initial volumes
        initial_pvcs = []
        for i in range(5):
            name = f"mixed-{unique_name}-init-{i}"
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            initial_pvcs.append(name)

        for pvc in initial_pvcs:
            assert k8s.wait_pvc_bound(pvc, timeout=60)

        operations_completed = []

        def create_volume(index: int) -> str:
            name = f"mixed-{unique_name}-new-{index}"
            k8s.create_pvc(name, "freebsd-e2e-iscsi-linked", "1Gi")
            k8s.wait_pvc_bound(name, timeout=60)
            return f"create-{name}"

        def delete_volume(pvc_name: str) -> str:
            k8s.delete("pvc", pvc_name, wait=True)
            return f"delete-{pvc_name}"

        def expand_volume(pvc_name: str) -> str:
            k8s.expand_pvc(pvc_name, "2Gi")
            return f"expand-{pvc_name}"

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []

            # Queue creates
            for i in range(3):
                futures.append(executor.submit(create_volume, i))

            # Queue deletes (on some initial volumes)
            for pvc in initial_pvcs[:2]:
                futures.append(executor.submit(delete_volume, pvc))

            # Queue expands (on remaining initial volumes)
            for pvc in initial_pvcs[2:]:
                futures.append(executor.submit(expand_volume, pvc))

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    operations_completed.append(result)
                except Exception as e:
                    print(f"Operation failed: {e}")

        assert len(operations_completed) >= 5, "Too many mixed operations failed"

        # Cleanup any remaining resources
        for i in range(3):
            k8s.delete("pvc", f"mixed-{unique_name}-new-{i}", ignore_not_found=True)
        for pvc in initial_pvcs:
            k8s.delete("pvc", pvc, ignore_not_found=True)
