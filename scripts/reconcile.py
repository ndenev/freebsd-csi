#!/usr/bin/env python3
"""Reconcile ctld-agent volumes with Kubernetes PVs.

This script identifies and optionally cleans up orphaned volumes - volumes that
exist in ctld-agent but have no corresponding PersistentVolume in Kubernetes.

Orphaned volumes can occur when:
- Tests are interrupted before cleanup runs
- Retain policy volumes have their PVs deleted manually
- The CSI DeleteVolume call fails but the PV is removed
- Network partitions during delete operations

Usage:
    # Dry run - show orphans without deleting
    ./scripts/reconcile.py

    # Delete orphaned volumes
    ./scripts/reconcile.py --delete

    # Custom agent address
    ./scripts/reconcile.py --agent-address 192.168.1.10:50051

Requirements:
    pip install grpcio grpcio-tools

The script must be run from the repository root, or with PYTHONPATH set to
include the e2e-tests directory.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Add e2e-tests to path for imports
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root / "e2e-tests"))

try:
    from lib.agent_client import AgentClient
except ImportError as e:
    print(f"Error: {e}")
    print("Please install dependencies: pip install grpcio grpcio-tools")
    print("And ensure you're running from the repository root.")
    sys.exit(1)


def get_kubernetes_pvs(kubeconfig: str | None = None) -> set[str]:
    """Get all PV names from Kubernetes.

    Args:
        kubeconfig: Optional path to kubeconfig file

    Returns:
        Set of PV names
    """
    cmd = ["kubectl", "get", "pv", "-o", "json"]
    if kubeconfig:
        cmd.extend(["--kubeconfig", kubeconfig])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        return {item["metadata"]["name"] for item in data.get("items", [])}
    except subprocess.CalledProcessError as e:
        print(f"Error getting PVs: {e.stderr}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing kubectl output: {e}")
        sys.exit(1)


def get_agent_volumes(agent_address: str) -> list[dict]:
    """Get all volumes from ctld-agent.

    Args:
        agent_address: ctld-agent gRPC address (host:port)

    Returns:
        List of volume info dicts
    """
    try:
        with AgentClient(address=agent_address) as client:
            return client.list_volumes()
    except Exception as e:
        print(f"Error connecting to ctld-agent at {agent_address}: {e}")
        sys.exit(1)


def find_orphans(agent_volumes: list[dict], k8s_pvs: set[str]) -> list[dict]:
    """Find volumes that exist in agent but not in Kubernetes.

    Args:
        agent_volumes: Volumes from ctld-agent
        k8s_pvs: PV names from Kubernetes

    Returns:
        List of orphaned volume info dicts
    """
    orphans = []
    for vol in agent_volumes:
        # Volume ID should match the PV name
        if vol["id"] not in k8s_pvs:
            orphans.append(vol)
    return orphans


def delete_orphans(orphans: list[dict], agent_address: str, dry_run: bool = True) -> int:
    """Delete orphaned volumes.

    Args:
        orphans: List of orphaned volume info dicts
        agent_address: ctld-agent gRPC address
        dry_run: If True, only print what would be deleted

    Returns:
        Number of volumes deleted (or would be deleted in dry run)
    """
    if dry_run:
        print("\n[DRY RUN] Would delete the following volumes:")
        for vol in orphans:
            print(f"  - {vol['id']} ({vol.get('target_name', 'unknown target')})")
        return len(orphans)

    deleted = 0
    with AgentClient(address=agent_address) as client:
        for vol in orphans:
            volume_id = vol["id"]
            try:
                print(f"Deleting {volume_id}...", end=" ")
                if client.delete_volume(volume_id):
                    print("OK")
                    deleted += 1
                else:
                    print("NOT FOUND (already deleted)")
            except Exception as e:
                print(f"FAILED: {e}")

    return deleted


def main():
    parser = argparse.ArgumentParser(
        description="Reconcile ctld-agent volumes with Kubernetes PVs"
    )
    parser.add_argument(
        "--agent-address",
        default="10.0.0.10:50051",
        help="ctld-agent gRPC address (default: 10.0.0.10:50051)",
    )
    parser.add_argument(
        "--kubeconfig",
        help="Path to kubeconfig file (uses default if not specified)",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete orphaned volumes (default is dry run)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show all volumes, not just orphans",
    )

    args = parser.parse_args()

    print("Reconciling ctld-agent volumes with Kubernetes PVs\n")
    print(f"Agent address: {args.agent_address}")

    # Get data from both sources
    print("\nFetching Kubernetes PVs...")
    k8s_pvs = get_kubernetes_pvs(args.kubeconfig)
    print(f"  Found {len(k8s_pvs)} PVs")

    print("\nFetching ctld-agent volumes...")
    agent_volumes = get_agent_volumes(args.agent_address)
    print(f"  Found {len(agent_volumes)} volumes")

    if args.verbose:
        print("\nKubernetes PVs:")
        for pv in sorted(k8s_pvs):
            print(f"  - {pv}")

        print("\nctld-agent volumes:")
        for vol in agent_volumes:
            print(f"  - {vol['id']} ({vol.get('target_name', 'unknown')})")

    # Find orphans
    orphans = find_orphans(agent_volumes, k8s_pvs)

    if not orphans:
        print("\n✓ No orphaned volumes found - state is synchronized")
        return 0

    print(f"\n⚠ Found {len(orphans)} orphaned volume(s):")
    for vol in orphans:
        size_gb = vol.get("size_bytes", 0) / (1024**3)
        print(f"  - {vol['id']}")
        print(f"      Target: {vol.get('target_name', 'unknown')}")
        print(f"      Dataset: {vol.get('zfs_dataset', 'unknown')}")
        print(f"      Size: {size_gb:.2f} GiB")

    # Delete if requested
    if args.delete:
        print("\nDeleting orphaned volumes...")
        deleted = delete_orphans(orphans, args.agent_address, dry_run=False)
        print(f"\n✓ Deleted {deleted} volume(s)")
    else:
        delete_orphans(orphans, args.agent_address, dry_run=True)
        print("\nRun with --delete to remove these volumes")

    return 0 if args.delete else 1


if __name__ == "__main__":
    sys.exit(main())
