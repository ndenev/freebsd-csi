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

        result = subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if check and result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode,
                cmd,
                output=result.stdout,
                stderr=result.stderr,
            )
        return result

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

        try:
            result = self._kubectl(
                ["-n", self.namespace, "apply", "-f", "-", "-o", "json"],
                input_data=manifest,
            )
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"kubectl apply failed: {e.stderr or e.output or 'unknown error'}"
            ) from e

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

    def list_resources(
        self, kind: str, label_selector: str | None = None
    ) -> list[dict]:
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

    def patch(
        self, kind: str, name: str, patch: dict, patch_type: str = "merge"
    ) -> dict:
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

    def wait_for_delete(
        self,
        kind: str,
        name: str,
        timeout: int = 60,
        cluster_scoped: bool = False,
    ) -> bool:
        """Wait for a resource to be deleted.

        Args:
            kind: Resource kind
            name: Resource name
            timeout: Wait timeout in seconds
            cluster_scoped: If True, don't use namespace (for PV, StorageClass, etc.)

        Returns:
            True if deleted (or already gone), False on timeout
        """
        args = ["wait", f"{kind}/{name}", "--for=delete", f"--timeout={timeout}s"]
        if not cluster_scoped:
            args = ["-n", self.namespace] + args

        try:
            self._kubectl(args, timeout=timeout + 10)
            return True
        except subprocess.CalledProcessError:
            # Check if resource is already gone
            if cluster_scoped:
                result = self._kubectl(["get", kind, name], check=False)
            else:
                result = self._kubectl(
                    ["-n", self.namespace, "get", kind, name], check=False
                )
            return result.returncode != 0

    def wait_pv_deleted(self, pv_name: str, timeout: int = 60) -> bool:
        """Wait for a PersistentVolume to be deleted.

        Used after deleting a PVC with Delete reclaim policy to verify
        the PV and backend storage are cleaned up.

        Args:
            pv_name: PV name
            timeout: Wait timeout in seconds

        Returns:
            True if deleted, False on timeout
        """
        return self.wait_for_delete("pv", pv_name, timeout, cluster_scoped=True)

    def wait_pvc_resized(
        self,
        name: str,
        expected_size: str,
        timeout: int = 60,
    ) -> bool:
        """Wait for PVC expansion to complete.

        Args:
            name: PVC name
            expected_size: Expected size after expansion (e.g., "2Gi")
            timeout: Wait timeout in seconds

        Returns:
            True if resized, False on timeout
        """
        expected_bytes = self._parse_size(expected_size)
        deadline = time.time() + timeout

        while time.time() < deadline:
            pvc = self.get("pvc", name)
            if pvc:
                # Check for resize conditions
                conditions = pvc.get("status", {}).get("conditions", [])
                for cond in conditions:
                    # FileSystemResizePending means resize is still in progress
                    if (
                        cond.get("type") == "FileSystemResizePending"
                        and cond.get("status") == "True"
                    ):
                        break
                else:
                    # No pending resize, check actual capacity
                    capacity = pvc.get("status", {}).get("capacity", {}).get("storage")
                    if capacity and self._parse_size(capacity) >= expected_bytes:
                        return True
            time.sleep(2)

        return False

    def _parse_size(self, size_str: str) -> int:
        """Parse Kubernetes size string to bytes.

        Args:
            size_str: Size string (e.g., "1Gi", "500Mi", "1000000")

        Returns:
            Size in bytes
        """
        if not size_str:
            return 0

        # Try parsing as plain integer (bytes)
        try:
            return int(size_str)
        except ValueError:
            pass

        # Binary units (Ki, Mi, Gi, Ti)
        binary_units = {
            "Ki": 1024,
            "Mi": 1024**2,
            "Gi": 1024**3,
            "Ti": 1024**4,
        }

        # Decimal units (K, M, G, T) - less common in K8s but supported
        decimal_units = {
            "K": 1000,
            "M": 1000**2,
            "G": 1000**3,
            "T": 1000**4,
        }

        for suffix, multiplier in binary_units.items():
            if size_str.endswith(suffix):
                try:
                    return int(float(size_str[: -len(suffix)]) * multiplier)
                except ValueError:
                    pass

        for suffix, multiplier in decimal_units.items():
            if size_str.endswith(suffix):
                try:
                    return int(float(size_str[: -len(suffix)]) * multiplier)
                except ValueError:
                    pass

        return 0

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
        """Wait for PVC to be bound using kubectl wait.

        Args:
            name: PVC name
            timeout: Wait timeout

        Returns:
            True if bound, False on timeout
        """
        return self.wait_for("pvc", name, "jsonpath={.status.phase}=Bound", timeout)

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
        """Wait for Pod to be ready using kubectl wait.

        Args:
            pod_name: Pod name
            timeout: Wait timeout

        Returns:
            True if ready, False on timeout
        """
        return self.wait_for("pod", pod_name, "condition=Ready", timeout)

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
        """Wait for VolumeSnapshot to be ready using kubectl wait.

        Args:
            name: Snapshot name
            timeout: Wait timeout

        Returns:
            True if ready, False on timeout
        """
        # kubectl wait with jsonpath for boolean true
        return self.wait_for(
            "volumesnapshot", name, "jsonpath={.status.readyToUse}=true", timeout
        )

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
    # Secret Operations
    # -------------------------------------------------------------------------

    def create_secret(
        self,
        name: str,
        data: dict[str, str],
        secret_type: str = "Opaque",
    ) -> dict:
        """Create a Kubernetes Secret.

        Args:
            name: Secret name
            data: String data (will be stored as stringData, not base64 encoded)
            secret_type: Secret type (default: Opaque)

        Returns:
            Created Secret resource
        """
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": name, "namespace": self.namespace},
            "type": secret_type,
            "stringData": data,
        }
        return self.apply(secret)

    def get_secret(self, name: str) -> dict | None:
        """Get a Secret by name.

        Args:
            name: Secret name

        Returns:
            Secret dict or None if not found
        """
        return self._kubectl_json(["-n", self.namespace, "get", "secret", name])

    def delete_secret(
        self,
        name: str,
        ignore_not_found: bool = True,
    ) -> bool:
        """Delete a Secret.

        Args:
            name: Secret name
            ignore_not_found: Don't error if secret doesn't exist

        Returns:
            True if deleted, False if not found
        """
        return self.delete(
            "secret", name, wait=False, ignore_not_found=ignore_not_found
        )

    def create_chap_secret(
        self,
        name: str,
        username: str,
        password: str,
        mutual_username: str | None = None,
        mutual_password: str | None = None,
    ) -> dict:
        """Create a CHAP authentication secret for iSCSI.

        Uses the standard CSI secret key names for CHAP credentials.

        Args:
            name: Secret name
            username: CHAP username (initiator authenticates to target)
            password: CHAP password
            mutual_username: Mutual CHAP username (target authenticates to initiator)
            mutual_password: Mutual CHAP password

        Returns:
            Created Secret resource
        """
        data = {
            "node.session.auth.username": username,
            "node.session.auth.password": password,
        }

        if mutual_username and mutual_password:
            data["node.session.auth.username_in"] = mutual_username
            data["node.session.auth.password_in"] = mutual_password

        return self.create_secret(name, data)

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
