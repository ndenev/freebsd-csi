"""Kubernetes client wrapper using kubectl for E2E tests."""

import json
import os
import subprocess
import tempfile
import time
from typing import Any

import yaml


class K8sClient:
    """Wrapper for kubectl operations with proper error handling."""

    def __init__(self, namespace: str = "default", kubeconfig: str | None = None):
        """Initialize the K8s client.

        Args:
            namespace: Default namespace for operations
            kubeconfig: Path to kubeconfig file (uses KUBECONFIG env or default if None)
        """
        self.namespace = namespace
        self.kubeconfig = kubeconfig or os.environ.get("KUBECONFIG")

    def _kubectl(
        self,
        args: list[str],
        input_data: str | None = None,
        timeout: int = 60,
        check: bool = True,
    ) -> subprocess.CompletedProcess:
        """Run kubectl command.

        Args:
            args: kubectl arguments
            input_data: Optional stdin data
            timeout: Command timeout in seconds
            check: Whether to raise on non-zero exit

        Returns:
            CompletedProcess with stdout/stderr
        """
        cmd = ["kubectl"]
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", self.kubeconfig])
        cmd.extend(args)

        return subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check,
        )

    def _kubectl_json(self, args: list[str], timeout: int = 60) -> dict | list | None:
        """Run kubectl command and parse JSON output.

        Args:
            args: kubectl arguments (without -o json)
            timeout: Command timeout

        Returns:
            Parsed JSON or None if resource not found
        """
        try:
            result = self._kubectl(args + ["-o", "json"], timeout=timeout)
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            if "NotFound" in e.stderr or "not found" in e.stderr.lower():
                return None
            raise

    # -------------------------------------------------------------------------
    # Generic Resource Operations
    # -------------------------------------------------------------------------

    def apply(self, manifest: str | dict) -> dict:
        """Apply a manifest (create or update resource).

        Args:
            manifest: YAML string or dict to apply

        Returns:
            Applied resource as dict
        """
        if isinstance(manifest, dict):
            manifest = yaml.dump(manifest)

        result = self._kubectl(
            ["-n", self.namespace, "apply", "-f", "-", "-o", "json"],
            input_data=manifest,
        )
        return json.loads(result.stdout)

    def apply_file(self, path: str) -> dict:
        """Apply a manifest file.

        Args:
            path: Path to YAML file

        Returns:
            Applied resource as dict
        """
        result = self._kubectl(
            ["-n", self.namespace, "apply", "-f", path, "-o", "json"]
        )
        return json.loads(result.stdout)

    def delete(
        self,
        kind: str,
        name: str,
        wait: bool = True,
        timeout: int = 120,
        ignore_not_found: bool = True,
    ) -> bool:
        """Delete a resource.

        Args:
            kind: Resource kind (e.g., "pvc", "pod")
            name: Resource name
            wait: Whether to wait for deletion
            timeout: Wait timeout in seconds
            ignore_not_found: Don't error if resource doesn't exist

        Returns:
            True if deleted, False if not found
        """
        args = ["-n", self.namespace, "delete", kind, name]
        if wait:
            args.append("--wait=true")
            args.extend(["--timeout", f"{timeout}s"])
        if ignore_not_found:
            args.append("--ignore-not-found=true")

        try:
            self._kubectl(args, timeout=timeout + 10)
            return True
        except subprocess.CalledProcessError as e:
            if ignore_not_found and "not found" in e.stderr.lower():
                return False
            raise

    def get(self, kind: str, name: str) -> dict | None:
        """Get a resource by name.

        Args:
            kind: Resource kind
            name: Resource name

        Returns:
            Resource dict or None if not found
        """
        return self._kubectl_json(["-n", self.namespace, "get", kind, name])

    def list_resources(self, kind: str, label_selector: str | None = None) -> list[dict]:
        """List resources of a kind.

        Args:
            kind: Resource kind
            label_selector: Optional label selector

        Returns:
            List of resource dicts
        """
        args = ["-n", self.namespace, "get", kind]
        if label_selector:
            args.extend(["-l", label_selector])

        result = self._kubectl_json(args)
        if result and "items" in result:
            return result["items"]
        return []

    def patch(self, kind: str, name: str, patch: dict, patch_type: str = "merge") -> dict:
        """Patch a resource.

        Args:
            kind: Resource kind
            name: Resource name
            patch: Patch data
            patch_type: Patch type (merge, json, strategic)

        Returns:
            Patched resource
        """
        result = self._kubectl(
            [
                "-n",
                self.namespace,
                "patch",
                kind,
                name,
                "--type",
                patch_type,
                "-p",
                json.dumps(patch),
                "-o",
                "json",
            ]
        )
        return json.loads(result.stdout)

    def wait_for(
        self,
        kind: str,
        name: str,
        condition: str,
        timeout: int = 60,
    ) -> bool:
        """Wait for a resource condition.

        Args:
            kind: Resource kind
            name: Resource name
            condition: Condition to wait for (e.g., "condition=Ready")
            timeout: Wait timeout in seconds

        Returns:
            True if condition met, False on timeout
        """
        try:
            self._kubectl(
                [
                    "-n",
                    self.namespace,
                    "wait",
                    f"{kind}/{name}",
                    f"--for={condition}",
                    f"--timeout={timeout}s",
                ],
                timeout=timeout + 10,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    # -------------------------------------------------------------------------
    # PVC Operations
    # -------------------------------------------------------------------------

    def create_pvc(
        self,
        name: str,
        storage_class: str,
        size: str = "1Gi",
        access_mode: str = "ReadWriteOnce",
        data_source: dict | None = None,
    ) -> dict:
        """Create a PersistentVolumeClaim.

        Args:
            name: PVC name
            storage_class: StorageClass name
            size: Storage size (e.g., "1Gi")
            access_mode: Access mode
            data_source: Optional dataSource for cloning

        Returns:
            Created PVC resource
        """
        pvc = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": name, "namespace": self.namespace},
            "spec": {
                "accessModes": [access_mode],
                "storageClassName": storage_class,
                "resources": {"requests": {"storage": size}},
            },
        }

        if data_source:
            pvc["spec"]["dataSource"] = data_source

        return self.apply(pvc)

    def wait_pvc_bound(self, name: str, timeout: int = 60) -> bool:
        """Wait for PVC to be bound.

        Args:
            name: PVC name
            timeout: Wait timeout

        Returns:
            True if bound, False on timeout
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            pvc = self.get("pvc", name)
            if pvc and pvc.get("status", {}).get("phase") == "Bound":
                return True
            time.sleep(2)
        return False

    def get_pvc_volume(self, name: str) -> str | None:
        """Get the PV name bound to a PVC.

        Args:
            name: PVC name

        Returns:
            PV name or None if not bound
        """
        pvc = self.get("pvc", name)
        if pvc:
            return pvc.get("spec", {}).get("volumeName")
        return None

    def expand_pvc(self, name: str, new_size: str) -> dict:
        """Expand a PVC.

        Args:
            name: PVC name
            new_size: New size (e.g., "2Gi")

        Returns:
            Patched PVC
        """
        return self.patch(
            "pvc",
            name,
            {"spec": {"resources": {"requests": {"storage": new_size}}}},
        )

    # -------------------------------------------------------------------------
    # Pod Operations
    # -------------------------------------------------------------------------

    def create_pod_with_pvc(
        self,
        pod_name: str,
        pvc_name: str,
        mount_path: str = "/mnt/data",
        image: str = "busybox:latest",
        command: list[str] | None = None,
    ) -> dict:
        """Create a Pod that mounts a PVC.

        Args:
            pod_name: Pod name
            pvc_name: PVC to mount
            mount_path: Mount path in container
            image: Container image
            command: Container command (defaults to sleep)

        Returns:
            Created Pod resource
        """
        if command is None:
            command = ["sleep", "3600"]

        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": self.namespace},
            "spec": {
                "containers": [
                    {
                        "name": "test",
                        "image": image,
                        "command": command,
                        "volumeMounts": [{"name": "data", "mountPath": mount_path}],
                    }
                ],
                "volumes": [
                    {"name": "data", "persistentVolumeClaim": {"claimName": pvc_name}}
                ],
                "restartPolicy": "Never",
            },
        }
        return self.apply(pod)

    def wait_pod_ready(self, pod_name: str, timeout: int = 120) -> bool:
        """Wait for Pod to be ready/running.

        Args:
            pod_name: Pod name
            timeout: Wait timeout

        Returns:
            True if ready, False on timeout
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            pod = self.get("pod", pod_name)
            if pod:
                phase = pod.get("status", {}).get("phase")
                if phase == "Running":
                    # Check container ready
                    conditions = pod.get("status", {}).get("conditions", [])
                    for cond in conditions:
                        if cond.get("type") == "Ready" and cond.get("status") == "True":
                            return True
                elif phase in ("Failed", "Succeeded"):
                    # Pod terminated
                    return False
            time.sleep(2)
        return False

    def exec_in_pod(
        self,
        pod_name: str,
        command: list[str],
        container: str | None = None,
        timeout: int = 60,
    ) -> tuple[str, str, int]:
        """Execute command in a Pod.

        Args:
            pod_name: Pod name
            command: Command to execute
            container: Container name (optional)
            timeout: Execution timeout

        Returns:
            Tuple of (stdout, stderr, return_code)
        """
        args = ["-n", self.namespace, "exec", pod_name]
        if container:
            args.extend(["-c", container])
        args.append("--")
        args.extend(command)

        result = self._kubectl(args, timeout=timeout, check=False)
        return result.stdout, result.stderr, result.returncode

    # -------------------------------------------------------------------------
    # Snapshot Operations
    # -------------------------------------------------------------------------

    def create_snapshot(
        self,
        name: str,
        pvc_name: str,
        snapshot_class: str | None = None,
    ) -> dict:
        """Create a VolumeSnapshot.

        Args:
            name: Snapshot name
            pvc_name: Source PVC name
            snapshot_class: VolumeSnapshotClass name (optional)

        Returns:
            Created VolumeSnapshot resource
        """
        snapshot = {
            "apiVersion": "snapshot.storage.k8s.io/v1",
            "kind": "VolumeSnapshot",
            "metadata": {"name": name, "namespace": self.namespace},
            "spec": {
                "source": {"persistentVolumeClaimName": pvc_name},
            },
        }

        if snapshot_class:
            snapshot["spec"]["volumeSnapshotClassName"] = snapshot_class

        return self.apply(snapshot)

    def wait_snapshot_ready(self, name: str, timeout: int = 60) -> bool:
        """Wait for VolumeSnapshot to be ready.

        Args:
            name: Snapshot name
            timeout: Wait timeout

        Returns:
            True if ready, False on timeout
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            snap = self.get("volumesnapshot", name)
            if snap:
                status = snap.get("status", {})
                if status.get("readyToUse") is True:
                    return True
                # Check for error
                if status.get("error"):
                    return False
            time.sleep(2)
        return False

    def get_snapshot_content(self, name: str) -> str | None:
        """Get the VolumeSnapshotContent bound to a snapshot.

        Args:
            name: Snapshot name

        Returns:
            VolumeSnapshotContent name or None
        """
        snap = self.get("volumesnapshot", name)
        if snap:
            return snap.get("status", {}).get("boundVolumeSnapshotContentName")
        return None

    # -------------------------------------------------------------------------
    # Log Collection
    # -------------------------------------------------------------------------

    def get_pod_logs(
        self,
        pod_name: str,
        container: str | None = None,
        since: str | None = "5m",
        tail: int | None = None,
    ) -> str:
        """Get logs from a Pod.

        Args:
            pod_name: Pod name
            container: Container name (optional)
            since: Time duration (e.g., "5m")
            tail: Number of lines to return

        Returns:
            Log output
        """
        args = ["-n", self.namespace, "logs", pod_name]
        if container:
            args.extend(["-c", container])
        if since:
            args.extend(["--since", since])
        if tail:
            args.extend(["--tail", str(tail)])

        try:
            result = self._kubectl(args, check=False)
            return result.stdout
        except Exception:
            return ""

    def get_events(self, field_selector: str | None = None) -> list[dict]:
        """Get events in the namespace.

        Args:
            field_selector: Optional field selector

        Returns:
            List of events
        """
        args = ["-n", self.namespace, "get", "events", "--sort-by=.lastTimestamp"]
        if field_selector:
            args.extend(["--field-selector", field_selector])

        result = self._kubectl_json(args)
        if result and "items" in result:
            return result["items"]
        return []

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def cluster_info(self) -> bool:
        """Check if cluster is accessible.

        Returns:
            True if cluster is accessible
        """
        try:
            self._kubectl(["cluster-info"], timeout=10)
            return True
        except Exception:
            return False

    def get_csi_driver(self, name: str) -> dict | None:
        """Get a CSIDriver resource.

        Args:
            name: Driver name

        Returns:
            CSIDriver resource or None
        """
        return self._kubectl_json(["get", "csidriver", name])

    def get_storage_class(self, name: str) -> dict | None:
        """Get a StorageClass.

        Args:
            name: StorageClass name

        Returns:
            StorageClass resource or None
        """
        return self._kubectl_json(["get", "storageclass", name])
