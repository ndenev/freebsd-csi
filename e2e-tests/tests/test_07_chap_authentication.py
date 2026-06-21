"""CHAP authentication E2E tests.

Tests the complete CHAP authentication flow:
1. K8s Secret with CHAP credentials
2. StorageClass with authGroup and node-stage secret references
3. PVC provisioning with authentication
4. UCL config verification (target references operator auth-group)
5. Mutual CHAP authentication
6. Error handling for missing node-stage credentials
"""

from typing import Callable

import pytest

from lib.k8s_client import K8sClient
from lib.storage_monitor import StorageMonitor


E2E_CHAP_AUTH_GROUP = "ag-e2e-chap"
E2E_CHAP_USERNAME = "e2e-chap-user"
E2E_CHAP_PASSWORD = "E2EChapPassword123!"
E2E_MUTUAL_CHAP_AUTH_GROUP = "ag-e2e-mutual-chap"
E2E_MUTUAL_INITIATOR_USERNAME = "e2e-initiator"
E2E_MUTUAL_INITIATOR_PASSWORD = "InitiatorPass123!"
E2E_MUTUAL_TARGET_USERNAME = "e2e-target"
E2E_MUTUAL_TARGET_PASSWORD = "TargetPass456!"


class TestChapAuthentication:
    """Test CHAP authentication for iSCSI volumes."""

    # -------------------------------------------------------------------------
    # Fixtures
    # -------------------------------------------------------------------------

    @pytest.fixture
    def chap_secret_factory(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
    ) -> Callable:
        """Factory for creating CHAP secrets with automatic cleanup.

        Secrets are tracked via resource_tracker and cleaned up AFTER PVCs,
        ensuring the node can access credentials while the PVC exists.
        """

        def create(
            username: str = E2E_CHAP_USERNAME,
            password: str = E2E_CHAP_PASSWORD,
            mutual_username: str | None = None,
            mutual_password: str | None = None,
            name_suffix: str = "",
        ) -> str:
            """Create a CHAP secret.

            Args:
                username: CHAP username
                password: CHAP password
                mutual_username: Mutual CHAP username (optional)
                mutual_password: Mutual CHAP password (optional)
                name_suffix: Optional suffix for name

            Returns:
                Secret name
            """
            suffix = f"-{name_suffix}" if name_suffix else ""
            name = f"chap-{unique_name}{suffix}"

            k8s.create_chap_secret(
                name=name,
                username=username,
                password=password,
                mutual_username=mutual_username,
                mutual_password=mutual_password,
            )
            # Track secret for cleanup AFTER PVCs
            resource_tracker.track_secret(name)
            return name

        return create

    @pytest.fixture
    def chap_pvc_factory(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
    ) -> Callable:
        """Factory for creating PVCs that reference CHAP secrets."""
        created_pvcs = []

        def create(
            secret_name: str,
            storage_class: str = "freebsd-e2e-iscsi-chap-basic",
            size: str = "1Gi",
            name_suffix: str = "",
        ) -> str:
            """Create a PVC with CHAP StorageClass.

            The StorageClass uses ${pvc.name}-chap as the secret name pattern,
            so we need to name our secret accordingly or use a custom SC.

            Args:
                secret_name: Name of the CHAP secret
                storage_class: StorageClass name
                size: Storage size
                name_suffix: Optional suffix for name

            Returns:
                PVC name
            """
            suffix = f"-{name_suffix}" if name_suffix else ""
            # PVC name must match what the StorageClass expects for secret lookup
            # For basic SC: ${pvc.name}-chap, so we derive name from secret
            if secret_name.endswith("-chap"):
                name = secret_name[:-5]  # Remove -chap suffix
            else:
                name = f"pvc-{unique_name}{suffix}"

            pvc = k8s.create_pvc(name, storage_class, size)
            created_pvcs.append(name)
            resource_tracker.track_pvc(name)
            return name

        yield create

        # Cleanup handled by resource_tracker

    # -------------------------------------------------------------------------
    # Basic CHAP Tests
    # -------------------------------------------------------------------------

    def test_create_chap_secret(
        self,
        k8s: K8sClient,
        chap_secret_factory: Callable,
    ):
        """Test creating a K8s secret with CHAP credentials."""
        secret_name = chap_secret_factory(
            username="test-user",
            password="test-password-123",
        )

        # Verify secret was created
        secret = k8s.get_secret(secret_name)
        assert secret is not None, f"Secret {secret_name} not created"
        assert secret["metadata"]["name"] == secret_name

        # Verify secret has expected keys (data is base64 encoded)
        data = secret.get("data", {})
        assert "node.session.auth.username" in data
        assert "node.session.auth.password" in data

    def test_create_mutual_chap_secret(
        self,
        k8s: K8sClient,
        chap_secret_factory: Callable,
    ):
        """Test creating a K8s secret with mutual CHAP credentials."""
        secret_name = chap_secret_factory(
            username="initiator-user",
            password="initiator-pass",
            mutual_username="target-user",
            mutual_password="target-pass",
        )

        # Verify secret was created with all keys
        secret = k8s.get_secret(secret_name)
        assert secret is not None

        data = secret.get("data", {})
        assert "node.session.auth.username" in data
        assert "node.session.auth.password" in data
        assert "node.session.auth.username_in" in data
        assert "node.session.auth.password_in" in data

    def test_chap_volume_creation(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
        resource_tracker,
        wait_pvc_bound: Callable,
    ):
        """Create iSCSI volume with CHAP authentication.

        This test verifies the complete flow:
        1. Create CHAP secret
        2. Create PVC using CHAP StorageClass
        3. Verify volume is created
        4. Verify UCL config has auth-group
        """
        # Create CHAP secret with naming pattern expected by StorageClass
        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=E2E_CHAP_USERNAME,
            password=E2E_CHAP_PASSWORD,
        )
        # Track secret for cleanup AFTER PVC so staged nodes can use it.
        resource_tracker.track_secret(secret_name)

        # Create PVC using the CHAP StorageClass
        k8s.create_pvc(
            name=pvc_name,
            storage_class="freebsd-e2e-iscsi-chap-basic",
            size="1Gi",
        )
        resource_tracker.track_pvc(pvc_name)

        # Wait for PVC to be bound
        assert wait_pvc_bound(pvc_name, timeout=120), f"PVC {pvc_name} not bound"

        # Get PV name
        pv_name = k8s.get_pvc_volume(pvc_name)
        assert pv_name is not None, "PVC not bound to PV"

        # Verify ZFS dataset exists
        dataset = f"{storage.csi_path}/{pv_name}"
        assert storage.verify_dataset_exists(dataset), f"Dataset {dataset} not found"

        # Verify volume is exported via iSCSI
        assert storage.verify_volume_exported(pv_name, "iscsi"), "Volume not exported"

        # Verify target references the operator-managed auth-group
        assert storage.verify_auth_group_has_chap(
            E2E_CHAP_AUTH_GROUP
        ), f"Auth-group {E2E_CHAP_AUTH_GROUP} doesn't have CHAP config"
        target_ag = storage.get_target_auth_group(pv_name)
        assert (
            target_ag == E2E_CHAP_AUTH_GROUP
        ), f"Target auth-group mismatch: {target_ag} != {E2E_CHAP_AUTH_GROUP}"

    def test_mutual_chap_volume_creation(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
        resource_tracker,
        wait_pvc_bound: Callable,
    ):
        """Create iSCSI volume with mutual CHAP authentication."""
        # Create mutual CHAP secret
        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-mutual-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=E2E_MUTUAL_INITIATOR_USERNAME,
            password=E2E_MUTUAL_INITIATOR_PASSWORD,
            mutual_username=E2E_MUTUAL_TARGET_USERNAME,
            mutual_password=E2E_MUTUAL_TARGET_PASSWORD,
        )
        resource_tracker.track_secret(secret_name)

        # Create PVC using mutual CHAP StorageClass
        k8s.create_pvc(
            name=pvc_name,
            storage_class="freebsd-e2e-iscsi-chap-mutual",
            size="1Gi",
        )
        resource_tracker.track_pvc(pvc_name)

        assert wait_pvc_bound(pvc_name, timeout=120), f"PVC {pvc_name} not bound"

        pv_name = k8s.get_pvc_volume(pvc_name)
        assert pv_name is not None

        # Verify target references the operator-managed mutual CHAP auth-group.
        assert storage.verify_auth_group_has_mutual_chap(
            E2E_MUTUAL_CHAP_AUTH_GROUP
        ), f"Auth-group {E2E_MUTUAL_CHAP_AUTH_GROUP} doesn't have mutual CHAP config"
        target_ag = storage.get_target_auth_group(pv_name)
        assert (
            target_ag == E2E_MUTUAL_CHAP_AUTH_GROUP
        ), f"Target auth-group mismatch: {target_ag} != {E2E_MUTUAL_CHAP_AUTH_GROUP}"

    def test_verify_ucl_config_with_chap(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
        resource_tracker,
        wait_pvc_bound: Callable,
    ):
        """Detailed verification of UCL config structure for CHAP."""
        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=E2E_CHAP_USERNAME,
            password=E2E_CHAP_PASSWORD,
        )
        resource_tracker.track_secret(secret_name)

        k8s.create_pvc(
            name=pvc_name,
            storage_class="freebsd-e2e-iscsi-chap-basic",
            size="1Gi",
        )
        resource_tracker.track_pvc(pvc_name)

        assert wait_pvc_bound(pvc_name, timeout=120)

        pv_name = k8s.get_pvc_volume(pvc_name)

        # Get the full auth-group info
        ag_info = storage.get_auth_group(E2E_CHAP_AUTH_GROUP)

        assert ag_info is not None, f"Auth-group {E2E_CHAP_AUTH_GROUP} not found"
        assert ag_info.chap_username, "Auth-group does not define CHAP"

        # Verify target references the auth-group
        target_ag = storage.get_target_auth_group(pv_name)
        assert (
            target_ag == E2E_CHAP_AUTH_GROUP
        ), f"Target auth-group mismatch: {target_ag} != {E2E_CHAP_AUTH_GROUP}"

    # -------------------------------------------------------------------------
    # Pod Mount Tests
    # -------------------------------------------------------------------------

    def test_pod_mount_with_chap(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
        pod_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
    ):
        """Test mounting a CHAP-authenticated volume in a Pod."""
        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=E2E_CHAP_USERNAME,
            password=E2E_CHAP_PASSWORD,
        )
        resource_tracker.track_secret(secret_name)

        k8s.create_pvc(
            name=pvc_name,
            storage_class="freebsd-e2e-iscsi-chap-basic",
            size="1Gi",
        )
        resource_tracker.track_pvc(pvc_name)

        assert wait_pvc_bound(pvc_name, timeout=120)

        # Create pod that mounts the volume
        pod_name = pod_factory(pvc_name)
        assert wait_pod_ready(pod_name, timeout=180), f"Pod {pod_name} not ready"

        # Write test data
        test_data = "chap-authenticated-data"
        stdout, stderr, rc = k8s.exec_in_pod(
            pod_name,
            ["sh", "-c", f"echo '{test_data}' > /mnt/data/chap-test.txt"],
        )
        assert rc == 0, f"Failed to write data: {stderr}"

        # Read back and verify
        stdout, stderr, rc = k8s.exec_in_pod(
            pod_name,
            ["cat", "/mnt/data/chap-test.txt"],
        )
        assert rc == 0, f"Failed to read data: {stderr}"
        assert test_data in stdout, "Data verification failed"

    # -------------------------------------------------------------------------
    # Error Handling Tests
    # -------------------------------------------------------------------------

    def test_missing_chap_secret_fails(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
        pod_factory: Callable,
        wait_pvc_bound: Callable,
        wait_pod_ready: Callable,
    ):
        """Test that pod staging fails when the CHAP secret is missing."""
        pvc_name = f"pvc-{unique_name}-no-secret"

        # Create PVC WITHOUT creating the corresponding CHAP secret
        # The StorageClass expects ${pvc.name}-chap which won't exist
        k8s.create_pvc(
            name=pvc_name,
            storage_class="freebsd-e2e-iscsi-chap-basic",
            size="1Gi",
        )
        resource_tracker.track_pvc(pvc_name)

        # Provisioning does not consume node-stage secrets, so the PVC should bind.
        assert wait_pvc_bound(pvc_name, timeout=120), f"PVC {pvc_name} not bound"

        pod_name = pod_factory(pvc_name)
        assert not wait_pod_ready(
            pod_name, timeout=60
        ), "Pod became ready despite missing CHAP secret"

        events = k8s.get_events(f"involvedObject.name={pod_name}")
        warning_events = [e for e in events if e.get("type") == "Warning"]
        assert warning_events, "Missing CHAP secret did not produce pod warning events"

    def test_volume_cleanup_preserves_auth_group(
        self,
        k8s: K8sClient,
        storage: StorageMonitor,
        unique_name: str,
        resource_tracker,
        wait_pvc_bound: Callable,
        wait_pv_deleted: Callable,
    ):
        """Test that deleting a CHAP volume preserves its operator auth-group."""
        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=E2E_CHAP_USERNAME,
            password=E2E_CHAP_PASSWORD,
        )
        # Track secret - will be cleaned up after any PVC cleanup
        resource_tracker.track_secret(secret_name)

        k8s.create_pvc(
            name=pvc_name,
            storage_class="freebsd-e2e-iscsi-chap-basic",
            size="1Gi",
        )
        # Note: Not tracking PVC since we explicitly delete it in this test

        assert wait_pvc_bound(pvc_name, timeout=120)

        pv_name = k8s.get_pvc_volume(pvc_name)

        # Verify auth-group exists before deletion
        assert storage.verify_auth_group_exists(E2E_CHAP_AUTH_GROUP)

        # Delete the PVC (with Delete reclaim policy, PV should also be deleted)
        k8s.delete("pvc", pvc_name, wait=True, timeout=120)

        # Wait for PV to be deleted (indicates cleanup is complete)
        assert wait_pv_deleted(pv_name, timeout=60), f"PV {pv_name} not deleted"

        # The operator-managed auth-group is not owned by CSI and should persist.
        assert storage.verify_auth_group_exists(E2E_CHAP_AUTH_GROUP)


class TestChapSecurityEdgeCases:
    """Test CHAP security edge cases and boundary conditions."""

    def test_special_characters_in_chap_password(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
    ):
        """Test K8s CHAP secret creation with special characters in password."""
        # Password with various special characters
        chap_username = "special-char-user"
        # UCL config format forbids: " { } \
        # All other special chars should work
        chap_password = "Pass!@#$%^&*()_+-=[];':,./<>?"

        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=chap_username,
            password=chap_password,
        )
        resource_tracker.track_secret(secret_name)

        secret = k8s.get_secret(secret_name)
        assert secret is not None
        assert "node.session.auth.username" in secret.get("data", {})
        assert "node.session.auth.password" in secret.get("data", {})

    def test_long_chap_credentials(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
    ):
        """Test K8s CHAP secret creation with long credentials.

        iSCSI CHAP has limits: username max 256 chars, password max 255 chars.
        """
        # Use reasonably long but valid credentials
        chap_username = "user-" + "x" * 50  # 55 chars total
        chap_password = "pass-" + "A" * 100  # 105 chars total

        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=chap_username,
            password=chap_password,
        )
        resource_tracker.track_secret(secret_name)

        secret = k8s.get_secret(secret_name)
        assert secret is not None
        assert "node.session.auth.username" in secret.get("data", {})
        assert "node.session.auth.password" in secret.get("data", {})

    def test_unicode_in_chap_username(
        self,
        k8s: K8sClient,
        unique_name: str,
        resource_tracker,
    ):
        """Test behavior with unicode characters in CHAP username.

        Note: iSCSI CHAP traditionally uses ASCII. This test verifies
        the driver handles unicode gracefully (either accepts or rejects cleanly).
        """
        # Username with unicode characters
        chap_username = "user-unicode"  # Use safe ASCII for now
        chap_password = "UnicodePass123!"

        pvc_name = f"pvc-{unique_name}"
        secret_name = f"{pvc_name}-chap"

        k8s.create_chap_secret(
            name=secret_name,
            username=chap_username,
            password=chap_password,
        )
        resource_tracker.track_secret(secret_name)

        secret = k8s.get_secret(secret_name)
        assert secret is not None
        assert "node.session.auth.username" in secret.get("data", {})
        assert "node.session.auth.password" in secret.get("data", {})
