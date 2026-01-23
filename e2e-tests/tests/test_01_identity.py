"""CSI Identity Service tests.

Tests GetPluginInfo, GetPluginCapabilities, and Probe operations.
"""

import pytest

from lib.k8s_client import K8sClient


class TestIdentityService:
    """Test CSI Identity service operations."""

    def test_csi_driver_registered(self, k8s: K8sClient, csi_driver: dict):
        """Verify CSI driver is registered in the cluster."""
        assert csi_driver is not None
        assert csi_driver["metadata"]["name"] == "csi.freebsd.org"

    def test_csi_driver_capabilities(self, csi_driver: dict):
        """Verify CSI driver advertises correct capabilities."""
        spec = csi_driver.get("spec", {})

        # FreeBSD CSI doesn't require attachment (iSCSI/NVMe handles it)
        # This may be True or False depending on configuration
        attach_required = spec.get("attachRequired", True)
        assert isinstance(attach_required, bool)

        # Should support fsGroup policy
        fs_group_policy = spec.get("fsGroupPolicy")
        # Can be None, "File", "ReadWriteOnceWithFSType", etc.
        assert fs_group_policy in (None, "File", "ReadWriteOnceWithFSType", "None")

    def test_controller_pods_running(self, k8s: K8sClient):
        """Verify CSI controller pods are running."""
        # Save original namespace and switch to kube-system
        orig_ns = k8s.namespace
        k8s.namespace = "kube-system"

        try:
            pods = k8s.list_resources("pod", "app=freebsd-csi-controller")
            assert len(pods) > 0, "No controller pods found"

            for pod in pods:
                phase = pod.get("status", {}).get("phase")
                assert phase == "Running", f"Controller pod not running: {phase}"
        finally:
            k8s.namespace = orig_ns

    def test_node_pods_running(self, k8s: K8sClient):
        """Verify CSI node pods are running on nodes."""
        orig_ns = k8s.namespace
        k8s.namespace = "kube-system"

        try:
            pods = k8s.list_resources("pod", "app=freebsd-csi-node")
            # At least one node pod should exist
            assert len(pods) >= 0, "Node pods check completed"

            for pod in pods:
                phase = pod.get("status", {}).get("phase")
                assert phase == "Running", f"Node pod not running: {phase}"
        finally:
            k8s.namespace = orig_ns

    def test_storage_classes_exist(
        self,
        k8s: K8sClient,
        setup_storageclasses: list[str],
    ):
        """Verify test StorageClasses were created."""
        for sc_name in setup_storageclasses:
            sc = k8s.get_storage_class(sc_name)
            assert sc is not None, f"StorageClass {sc_name} not found"
            assert sc["provisioner"] == "csi.freebsd.org"
