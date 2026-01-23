"""Pytest configuration and fixtures for FreeBSD CSI E2E tests."""

import os
import time
import uuid
from pathlib import Path
from typing import Callable, Generator

import pytest

from lib.k8s_client import K8sClient
from lib.log_collector import LogCollector
from lib.storage_monitor import StorageMonitor, StorageState


# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom command line options."""
    parser.addoption(
        "--namespace",
        action="store",
        default=os.environ.get("TEST_NAMESPACE", "default"),
        help="Kubernetes namespace for tests",
    )
    parser.addoption(
        "--kubeconfig",
        action="store",
        default=os.environ.get("KUBECONFIG"),
        help="Path to kubeconfig file",
    )
    parser.addoption(
        "--pool",
        action="store",
        default=os.environ.get("ZFS_POOL", "tank"),
        help="ZFS pool name",
    )
    parser.addoption(
        "--csi-prefix",
        action="store",
        default=os.environ.get("CSI_PREFIX", "csi"),
        help="CSI dataset prefix in ZFS pool",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest markers."""
    config.addinivalue_line("markers", "stress: marks tests as stress tests")
    config.addinivalue_line("markers", "slow: marks tests as slow running")
    config.addinivalue_line("markers", "nvmeof: marks tests requiring NVMeoF")


# -------------------------------------------------------------------------
# Session-scoped Fixtures
# -------------------------------------------------------------------------


@pytest.fixture(scope="session")
def test_namespace(request: pytest.FixtureRequest) -> str:
    """Get the test namespace."""
    return request.config.getoption("--namespace")


@pytest.fixture(scope="session")
def k8s(request: pytest.FixtureRequest, test_namespace: str) -> K8sClient:
    """K8s client for the test session."""
    kubeconfig = request.config.getoption("--kubeconfig")
    client = K8sClient(namespace=test_namespace, kubeconfig=kubeconfig)

    # Verify cluster access
    if not client.cluster_info():
        pytest.fail("Cannot connect to Kubernetes cluster")

    return client


@pytest.fixture(scope="session")
def storage(request: pytest.FixtureRequest) -> StorageMonitor:
    """Storage monitor for FreeBSD backend."""
    pool = request.config.getoption("--pool")
    csi_prefix = request.config.getoption("--csi-prefix")
    return StorageMonitor(pool=pool, csi_prefix=csi_prefix)


@pytest.fixture(scope="session")
def csi_driver(k8s: K8sClient) -> dict:
    """Verify CSI driver is installed and return its info."""
    driver = k8s.get_csi_driver("csi.freebsd.org")
    if not driver:
        pytest.fail("CSI driver csi.freebsd.org not found")
    return driver


@pytest.fixture(scope="session")
def resources_dir() -> Path:
    """Path to resources directory."""
    return Path(__file__).parent / "resources"


@pytest.fixture(scope="session")
def setup_storageclasses(k8s: K8sClient, resources_dir: Path) -> Generator[list[str], None, None]:
    """Create test StorageClasses and VolumeSnapshotClass at session start, cleanup at end."""
    storage_class_dir = resources_dir / "storageclasses"
    snapshot_class_dir = resources_dir / "snapshotclasses"
    created_classes = []
    created_snapshot_classes = []

    # Apply all StorageClass manifests
    for yaml_file in storage_class_dir.glob("*.yaml"):
        try:
            k8s.apply_file(str(yaml_file))
            # Extract name from filename
            sc_name = f"freebsd-e2e-{yaml_file.stem}"
            created_classes.append(sc_name)
        except Exception as e:
            print(f"Warning: Failed to create StorageClass from {yaml_file}: {e}")

    # Apply all VolumeSnapshotClass manifests
    if snapshot_class_dir.exists():
        for yaml_file in snapshot_class_dir.glob("*.yaml"):
            try:
                k8s.apply_file(str(yaml_file))
                created_snapshot_classes.append("freebsd-e2e-snapclass")
            except Exception as e:
                print(f"Warning: Failed to create VolumeSnapshotClass from {yaml_file}: {e}")

    yield created_classes

    # Cleanup - try to delete but don't fail if already gone
    for sc_name in created_classes:
        try:
            k8s.delete("storageclass", sc_name, ignore_not_found=True)
        except Exception:
            pass

    for vsc_name in created_snapshot_classes:
        try:
            k8s.delete("volumesnapshotclass", vsc_name, ignore_not_found=True)
        except Exception:
            pass


# -------------------------------------------------------------------------
# Function-scoped Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def logs(k8s: K8sClient) -> Generator[LogCollector, None, None]:
    """Log collector that starts fresh for each test."""
    collector = LogCollector(k8s)
    collector.start_collection()
    yield collector


@pytest.fixture
def unique_name() -> str:
    """Generate unique resource names for this test."""
    return f"e2e-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def storage_state_before(storage: StorageMonitor) -> StorageState:
    """Capture storage state before test."""
    return storage.capture_state()


# -------------------------------------------------------------------------
# Factory Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def pvc_factory(
    k8s: K8sClient,
    unique_name: str,
    setup_storageclasses: list[str],
) -> Generator[Callable, None, None]:
    """Factory for creating PVCs with automatic cleanup.

    PVCs are cleaned up in reverse order (clones before sources).
    """
    created: list[str] = []

    def create(
        storage_class: str,
        size: str = "1Gi",
        data_source: dict | None = None,
        name_suffix: str = "",
    ) -> str:
        """Create a PVC.

        Args:
            storage_class: StorageClass name
            size: Storage size
            data_source: Optional dataSource for cloning
            name_suffix: Optional suffix for name

        Returns:
            PVC name
        """
        suffix = f"-{name_suffix}" if name_suffix else f"-{len(created)}"
        name = f"pvc-{unique_name}{suffix}"
        k8s.create_pvc(name, storage_class, size, data_source=data_source)
        created.append(name)
        return name

    yield create

    # Cleanup in reverse order (clones before sources)
    for name in reversed(created):
        try:
            k8s.delete("pvc", name, wait=True, timeout=60, ignore_not_found=True)
        except Exception as e:
            print(f"Warning: Failed to delete PVC {name}: {e}")


@pytest.fixture
def pod_factory(
    k8s: K8sClient,
    unique_name: str,
) -> Generator[Callable, None, None]:
    """Factory for creating Pods with automatic cleanup."""
    created: list[str] = []

    def create(
        pvc_name: str,
        mount_path: str = "/mnt/data",
        name_suffix: str = "",
    ) -> str:
        """Create a Pod that mounts a PVC.

        Args:
            pvc_name: PVC to mount
            mount_path: Mount path in container
            name_suffix: Optional suffix for name

        Returns:
            Pod name
        """
        suffix = f"-{name_suffix}" if name_suffix else f"-{len(created)}"
        name = f"pod-{unique_name}{suffix}"
        k8s.create_pod_with_pvc(name, pvc_name, mount_path)
        created.append(name)
        return name

    yield create

    # Cleanup
    for name in reversed(created):
        try:
            k8s.delete("pod", name, wait=True, timeout=60, ignore_not_found=True)
        except Exception as e:
            print(f"Warning: Failed to delete Pod {name}: {e}")


@pytest.fixture
def snapshot_factory(
    k8s: K8sClient,
    unique_name: str,
) -> Generator[Callable, None, None]:
    """Factory for creating VolumeSnapshots with automatic cleanup."""
    created: list[str] = []

    def create(
        pvc_name: str,
        snapshot_class: str | None = "freebsd-e2e-snapclass",
        name_suffix: str = "",
    ) -> str:
        """Create a VolumeSnapshot.

        Args:
            pvc_name: Source PVC name
            snapshot_class: VolumeSnapshotClass (defaults to freebsd-e2e-snapclass)
            name_suffix: Optional suffix for name

        Returns:
            Snapshot name
        """
        suffix = f"-{name_suffix}" if name_suffix else f"-{len(created)}"
        name = f"snap-{unique_name}{suffix}"
        k8s.create_snapshot(name, pvc_name, snapshot_class)
        created.append(name)
        return name

    yield create

    # Cleanup
    for name in reversed(created):
        try:
            k8s.delete("volumesnapshot", name, wait=True, timeout=60, ignore_not_found=True)
        except Exception as e:
            print(f"Warning: Failed to delete VolumeSnapshot {name}: {e}")


# -------------------------------------------------------------------------
# Utility Fixtures
# -------------------------------------------------------------------------


@pytest.fixture
def wait_pvc_bound(k8s: K8sClient) -> Callable[[str, int], bool]:
    """Helper to wait for PVC to be bound."""

    def _wait(name: str, timeout: int = 60) -> bool:
        return k8s.wait_pvc_bound(name, timeout)

    return _wait


@pytest.fixture
def wait_pod_ready(k8s: K8sClient) -> Callable[[str, int], bool]:
    """Helper to wait for Pod to be ready."""

    def _wait(name: str, timeout: int = 120) -> bool:
        return k8s.wait_pod_ready(name, timeout)

    return _wait


@pytest.fixture
def wait_snapshot_ready(k8s: K8sClient) -> Callable[[str, int], bool]:
    """Helper to wait for snapshot to be ready."""

    def _wait(name: str, timeout: int = 60) -> bool:
        return k8s.wait_snapshot_ready(name, timeout)

    return _wait


# -------------------------------------------------------------------------
# Reporting Hooks
# -------------------------------------------------------------------------


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Enhance test reports with storage state on failure."""
    outcome = yield
    report = outcome.get_result()

    if report.when == "call" and report.failed:
        # Try to capture storage state on failure
        try:
            storage = item.funcargs.get("storage")
            logs = item.funcargs.get("logs")

            extra_info = []

            if storage:
                state = storage.capture_state()
                extra_info.append("\n=== Storage State at Failure ===")
                extra_info.append(f"Datasets: {len(state.datasets)}")
                extra_info.append(f"Snapshots: {len(state.snapshots)}")
                extra_info.append(f"LUNs: {len(state.luns)}")

            if logs:
                collected = logs.collect_all()
                errors = logs.find_errors(collected)
                if errors:
                    extra_info.append("\n=== Errors in Logs ===")
                    for err in errors[:10]:  # First 10 errors
                        extra_info.append(f"[{err.source}] {err.message[:200]}")

            if extra_info:
                report.longrepr = str(report.longrepr) + "\n" + "\n".join(extra_info)

        except Exception:
            pass  # Don't fail the failure reporting
