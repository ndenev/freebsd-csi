"""Clone chain stress tests.

Tests complex clone dependency chains and deletion ordering.
"""

import time
from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


@pytest.mark.stress
class TestCloneChains:
    """Test complex clone dependency chains."""

    def test_deep_clone_chain(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Create chain: A -> B -> C -> D, then delete from beginning.

        Tests auto-promote cascade when source is deleted before clones.
        """
        volumes = []
        snapshots = []

        # Create initial volume A
        a_name = f"chain-{unique_name}-a"
        k8s.create_pvc(a_name, "freebsd-e2e-iscsi-linked", "1Gi")
        assert k8s.wait_pvc_bound(a_name, timeout=60)
        volumes.append(a_name)

        # Create chain: A -> snap -> B -> snap -> C -> snap -> D
        prev_vol = a_name
        for letter in ["b", "c", "d"]:
            # Create snapshot of previous volume
            snap_name = f"snap-{unique_name}-{letter}"
            k8s.create_snapshot(snap_name, prev_vol)
            assert k8s.wait_snapshot_ready(snap_name, timeout=60)
            snapshots.append(snap_name)

            # Create clone from snapshot
            clone_name = f"chain-{unique_name}-{letter}"
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
            volumes.append(clone_name)
            prev_vol = clone_name

        # Verify chain exists
        assert len(volumes) == 4  # A, B, C, D
        for vol in volumes:
            pv = k8s.get_pvc_volume(vol)
            assert storage.verify_dataset_exists(f"{storage.csi_path}/{pv}")

        # Delete from beginning (A first)
        # This should trigger auto-promote cascade
        for i, vol in enumerate(volumes):
            print(f"Deleting volume {i}: {vol}")
            # Delete associated snapshot first (if any)
            if i < len(snapshots):
                k8s.delete("volumesnapshot", snapshots[i], wait=True, ignore_not_found=True)
                time.sleep(2)

            k8s.delete("pvc", vol, wait=True)
            time.sleep(3)  # Allow time for promotion

        # Verify all cleaned up
        time.sleep(5)
        for vol in volumes:
            pv = k8s.get_pvc_volume(vol)
            if pv:
                assert not storage.verify_dataset_exists(f"{storage.csi_path}/{pv}")

    def test_delete_middle_of_chain(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Create A -> B -> C, delete B (middle)."""
        volumes = []
        snapshots = []

        # Create A
        a_name = f"mid-{unique_name}-a"
        k8s.create_pvc(a_name, "freebsd-e2e-iscsi-linked", "1Gi")
        assert k8s.wait_pvc_bound(a_name, timeout=60)
        volumes.append(a_name)

        # Create A -> B
        snap_ab = f"snap-{unique_name}-ab"
        k8s.create_snapshot(snap_ab, a_name)
        assert k8s.wait_snapshot_ready(snap_ab, timeout=60)
        snapshots.append(snap_ab)

        b_name = f"mid-{unique_name}-b"
        k8s.create_pvc(
            b_name,
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_ab,
            },
        )
        assert k8s.wait_pvc_bound(b_name, timeout=60)
        volumes.append(b_name)

        # Create B -> C
        snap_bc = f"snap-{unique_name}-bc"
        k8s.create_snapshot(snap_bc, b_name)
        assert k8s.wait_snapshot_ready(snap_bc, timeout=60)
        snapshots.append(snap_bc)

        c_name = f"mid-{unique_name}-c"
        k8s.create_pvc(
            c_name,
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap_bc,
            },
        )
        assert k8s.wait_pvc_bound(c_name, timeout=60)
        volumes.append(c_name)

        c_pv = k8s.get_pvc_volume(c_name)
        c_dataset = f"{storage.csi_path}/{c_pv}"

        # Delete B (middle) - delete its snapshot first
        k8s.delete("volumesnapshot", snap_bc, wait=True)
        time.sleep(2)
        k8s.delete("pvc", b_name, wait=True)
        time.sleep(5)

        # C should still exist and be accessible
        assert storage.verify_dataset_exists(c_dataset), "C was deleted with B"

        # Cleanup
        k8s.delete("volumesnapshot", snap_ab, ignore_not_found=True)
        k8s.delete("pvc", c_name, ignore_not_found=True)
        k8s.delete("pvc", a_name, ignore_not_found=True)

    def test_multiple_clones_from_single_snapshot(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """Create A -> snap -> [B, C, D, E], delete in various orders."""
        # Create source and snapshot
        source = f"multi-{unique_name}-src"
        k8s.create_pvc(source, "freebsd-e2e-iscsi-linked", "1Gi")
        assert k8s.wait_pvc_bound(source, timeout=60)

        snap = f"snap-{unique_name}-multi"
        k8s.create_snapshot(snap, source)
        assert k8s.wait_snapshot_ready(snap, timeout=60)

        # Create 4 clones
        clones = []
        for letter in ["b", "c", "d", "e"]:
            clone_name = f"multi-{unique_name}-{letter}"
            k8s.create_pvc(
                clone_name,
                "freebsd-e2e-iscsi-linked",
                "1Gi",
                data_source={
                    "apiGroup": "snapshot.storage.k8s.io",
                    "kind": "VolumeSnapshot",
                    "name": snap,
                },
            )
            clones.append(clone_name)

        for clone in clones:
            assert k8s.wait_pvc_bound(clone, timeout=60)

        # Verify all clones exist
        for clone in clones:
            pv = k8s.get_pvc_volume(clone)
            assert storage.verify_dataset_exists(f"{storage.csi_path}/{pv}")

        # Delete in "random" order: D, B, E, C
        import random
        delete_order = clones.copy()
        random.shuffle(delete_order)

        for clone in delete_order:
            k8s.delete("pvc", clone, wait=True)
            time.sleep(2)

        # Now delete snapshot and source
        k8s.delete("volumesnapshot", snap, wait=True)
        time.sleep(2)
        k8s.delete("pvc", source, wait=True)

    def test_cross_clone_complexity(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
    ):
        """
        Complex scenario:
        A -> snap1 -> B
        A -> snap2 -> C
        B -> snap3 -> D
        Delete A first (should auto-promote B and C)
        """
        # Create A
        a = f"cross-{unique_name}-a"
        k8s.create_pvc(a, "freebsd-e2e-iscsi-linked", "1Gi")
        assert k8s.wait_pvc_bound(a, timeout=60)

        # A -> snap1 -> B
        snap1 = f"snap-{unique_name}-1"
        k8s.create_snapshot(snap1, a)
        assert k8s.wait_snapshot_ready(snap1, timeout=60)

        b = f"cross-{unique_name}-b"
        k8s.create_pvc(
            b,
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap1,
            },
        )
        assert k8s.wait_pvc_bound(b, timeout=60)

        # A -> snap2 -> C
        snap2 = f"snap-{unique_name}-2"
        k8s.create_snapshot(snap2, a)
        assert k8s.wait_snapshot_ready(snap2, timeout=60)

        c = f"cross-{unique_name}-c"
        k8s.create_pvc(
            c,
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap2,
            },
        )
        assert k8s.wait_pvc_bound(c, timeout=60)

        # B -> snap3 -> D
        snap3 = f"snap-{unique_name}-3"
        k8s.create_snapshot(snap3, b)
        assert k8s.wait_snapshot_ready(snap3, timeout=60)

        d = f"cross-{unique_name}-d"
        k8s.create_pvc(
            d,
            "freebsd-e2e-iscsi-linked",
            "1Gi",
            data_source={
                "apiGroup": "snapshot.storage.k8s.io",
                "kind": "VolumeSnapshot",
                "name": snap3,
            },
        )
        assert k8s.wait_pvc_bound(d, timeout=60)

        # Get dataset paths
        b_pv = k8s.get_pvc_volume(b)
        c_pv = k8s.get_pvc_volume(c)
        d_pv = k8s.get_pvc_volume(d)

        b_dataset = f"{storage.csi_path}/{b_pv}"
        c_dataset = f"{storage.csi_path}/{c_pv}"
        d_dataset = f"{storage.csi_path}/{d_pv}"

        # Delete A's snapshots, then A
        k8s.delete("volumesnapshot", snap1, wait=True)
        k8s.delete("volumesnapshot", snap2, wait=True)
        time.sleep(2)
        k8s.delete("pvc", a, wait=True)
        time.sleep(5)

        # B, C, D should still exist
        assert storage.verify_dataset_exists(b_dataset), "B deleted with A"
        assert storage.verify_dataset_exists(c_dataset), "C deleted with A"
        assert storage.verify_dataset_exists(d_dataset), "D deleted with A"

        # Cleanup remaining
        k8s.delete("volumesnapshot", snap3, ignore_not_found=True)
        k8s.delete("pvc", d, ignore_not_found=True)
        k8s.delete("pvc", c, ignore_not_found=True)
        k8s.delete("pvc", b, ignore_not_found=True)
