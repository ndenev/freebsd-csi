"""Resource tracker for coordinated E2E test cleanup.

Ensures resources are deleted in the correct dependency order:
1. Pods (release PVC usage)
2. Clone PVCs (depend on snapshots)
3. Snapshots (depend on source volumes)
4. Source PVCs (base volumes)
5. Secrets (deleted after PVCs so provisioner can access credentials)

This prevents "cannot delete snapshot with dependent clones" errors
and other dependency-related cleanup failures.
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.k8s_client import K8sClient


class ResourceType(IntEnum):
    """Resource types in cleanup priority order (lower = cleanup first)."""

    POD = 1
    CLONE_PVC = 2  # PVC created from snapshot or another PVC
    SNAPSHOT = 3
    SOURCE_PVC = 4  # Original PVC (no data source)
    SECRET = (
        5  # Secrets deleted LAST (after PVCs, so provisioner can access credentials)
    )


@dataclass
class TrackedResource:
    """A resource being tracked for cleanup."""

    kind: str  # K8s kind: "pod", "pvc", "volumesnapshot"
    name: str
    resource_type: ResourceType
    # For debugging dependency issues
    depends_on: str | None = None


@dataclass
class ResourceTracker:
    """Tracks test resources and coordinates cleanup in correct order.

    Usage:
        tracker = ResourceTracker(k8s)
        tracker.track_pvc("my-pvc", is_clone=False)
        tracker.track_snapshot("my-snap")
        tracker.track_pvc("my-clone", is_clone=True, depends_on="my-snap")
        tracker.track_pod("my-pod")

        # At test end:
        tracker.cleanup_all()  # Deletes in correct order
    """

    k8s: "K8sClient"
    resources: list[TrackedResource] = field(default_factory=list)

    def track_pod(self, name: str) -> None:
        """Track a pod for cleanup."""
        self.resources.append(
            TrackedResource(
                kind="pod",
                name=name,
                resource_type=ResourceType.POD,
            )
        )

    def track_pvc(
        self, name: str, is_clone: bool = False, depends_on: str | None = None
    ) -> None:
        """Track a PVC for cleanup.

        Args:
            name: PVC name
            is_clone: True if created from a snapshot/PVC (needs earlier cleanup)
            depends_on: Name of snapshot or PVC this was cloned from
        """
        resource_type = ResourceType.CLONE_PVC if is_clone else ResourceType.SOURCE_PVC
        self.resources.append(
            TrackedResource(
                kind="pvc",
                name=name,
                resource_type=resource_type,
                depends_on=depends_on,
            )
        )

    def track_snapshot(self, name: str, source_pvc: str | None = None) -> None:
        """Track a snapshot for cleanup.

        Args:
            name: Snapshot name
            source_pvc: Name of the source PVC (for dependency tracking)
        """
        self.resources.append(
            TrackedResource(
                kind="volumesnapshot",
                name=name,
                resource_type=ResourceType.SNAPSHOT,
                depends_on=source_pvc,
            )
        )

    def track_secret(self, name: str) -> None:
        """Track a secret for cleanup.

        Secrets are deleted AFTER PVCs to ensure the CSI provisioner can
        access credentials when deleting volumes.

        Args:
            name: Secret name
        """
        self.resources.append(
            TrackedResource(
                kind="secret",
                name=name,
                resource_type=ResourceType.SECRET,
            )
        )

    def cleanup_all(self, timeout: int = 60) -> list[str]:
        """Clean up all tracked resources in correct dependency order.

        Returns:
            List of warning messages for resources that failed to delete
        """
        warnings = []

        # Sort by resource type (pods first, source PVCs last)
        # Within same type, reverse creation order (LIFO)
        sorted_resources = sorted(
            enumerate(self.resources),
            key=lambda x: (x[1].resource_type, -x[0]),
        )

        for _, resource in sorted_resources:
            try:
                self.k8s.delete(
                    resource.kind,
                    resource.name,
                    wait=True,
                    timeout=timeout,
                    ignore_not_found=True,
                )
            except Exception as e:
                msg = f"Failed to delete {resource.kind} {resource.name}: {e}"
                warnings.append(msg)
                print(f"Warning: {msg}")

        # Clear tracked resources
        self.resources.clear()

        return warnings

    def clear(self) -> None:
        """Clear all tracked resources without deleting them."""
        self.resources.clear()

    def __len__(self) -> int:
        return len(self.resources)
