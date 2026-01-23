"""FreeBSD storage monitoring for E2E tests.

Monitors ZFS datasets/snapshots, CTL LUNs/ports, and iSCSI targets.
"""

import json
import re
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DatasetInfo:
    """ZFS dataset information."""

    name: str
    type: str  # filesystem, volume, snapshot
    used: int  # bytes
    available: int  # bytes
    referenced: int  # bytes
    volsize: int | None = None  # For zvols only
    origin: str | None = None  # For clones only
    clones: list[str] = field(default_factory=list)


@dataclass
class SnapshotInfo:
    """ZFS snapshot information."""

    name: str  # full name including @
    dataset: str  # parent dataset
    snap_name: str  # just the snapshot part after @
    used: int
    referenced: int
    clones: list[str] = field(default_factory=list)


@dataclass
class LunInfo:
    """CTL LUN information."""

    lun_id: int
    backend: str
    serial: str
    device_id: str
    size: int  # bytes
    blocksize: int
    path: str | None = None  # zvol path for ZFS backend


@dataclass
class PortInfo:
    """CTL port information."""

    port_id: int
    port_type: str  # iscsi, nvmf, etc.
    target_name: str  # IQN or NQN
    online: bool


@dataclass
class StorageState:
    """Complete storage state snapshot."""

    datasets: list[DatasetInfo]
    snapshots: list[SnapshotInfo]
    luns: list[LunInfo]
    ports: list[PortInfo]
    ctld_config: str


class StorageMonitor:
    """Monitor FreeBSD storage state: ZFS, CTL, iSCSI."""

    def __init__(self, pool: str = "tank", csi_prefix: str = "csi", use_sudo: bool = True):
        """Initialize storage monitor.

        Args:
            pool: ZFS pool name
            csi_prefix: Dataset prefix used by CSI driver
            use_sudo: Whether to use sudo for privileged commands (default: True)
        """
        self.pool = pool
        self.csi_prefix = csi_prefix
        self.csi_path = f"{pool}/{csi_prefix}"
        self.use_sudo = use_sudo

    # Commands that require elevated privileges
    PRIVILEGED_COMMANDS = {"zfs", "ctladm"}

    def _run(self, cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
        """Run a command and return result.

        Automatically uses sudo for ZFS and CTL commands if use_sudo is enabled.

        Args:
            cmd: Command and arguments
            check: Whether to raise on non-zero exit
        """
        # Auto-detect privileged commands
        if self.use_sudo and cmd and cmd[0] in self.PRIVILEGED_COMMANDS:
            cmd = ["sudo"] + cmd
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=check,
        )

    def _parse_size(self, size_str: str) -> int:
        """Parse ZFS size string to bytes."""
        if not size_str or size_str == "-":
            return 0

        # Handle numeric (already in bytes)
        try:
            return int(size_str)
        except ValueError:
            pass

        # Handle suffixed sizes (K, M, G, T)
        multipliers = {
            "K": 1024,
            "M": 1024**2,
            "G": 1024**3,
            "T": 1024**4,
        }

        match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT])?$", size_str.upper())
        if match:
            value = float(match.group(1))
            suffix = match.group(2)
            if suffix:
                value *= multipliers[suffix]
            return int(value)

        return 0

    # -------------------------------------------------------------------------
    # ZFS Operations
    # -------------------------------------------------------------------------

    def list_datasets(self, path: str | None = None) -> list[DatasetInfo]:
        """List ZFS datasets.

        Args:
            path: Optional path to filter (defaults to CSI path)

        Returns:
            List of DatasetInfo
        """
        target = path or self.csi_path

        try:
            result = self._run(
                [
                    "zfs",
                    "list",
                    "-H",
                    "-p",  # Parseable (bytes)
                    "-o",
                    "name,type,used,avail,refer,volsize,origin,clones",
                    "-r",
                    target,
                ]
            )
        except subprocess.CalledProcessError:
            return []

        datasets = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 8:
                continue

            name, dtype, used, avail, refer, volsize, origin, clones = parts

            ds = DatasetInfo(
                name=name,
                type=dtype,
                used=int(used) if used != "-" else 0,
                available=int(avail) if avail != "-" else 0,
                referenced=int(refer) if refer != "-" else 0,
                volsize=int(volsize) if volsize and volsize != "-" else None,
                origin=origin if origin and origin != "-" else None,
                clones=clones.split(",") if clones and clones != "-" else [],
            )
            datasets.append(ds)

        return datasets

    def get_dataset_info(self, dataset: str) -> DatasetInfo | None:
        """Get detailed info about a specific dataset.

        Args:
            dataset: Full dataset path

        Returns:
            DatasetInfo or None if not found
        """
        try:
            result = self._run(
                [
                    "zfs",
                    "list",
                    "-H",
                    "-p",
                    "-o",
                    "name,type,used,avail,refer,volsize,origin,clones",
                    dataset,
                ]
            )
        except subprocess.CalledProcessError:
            return None

        line = result.stdout.strip()
        if not line:
            return None

        parts = line.split("\t")
        if len(parts) < 8:
            return None

        name, dtype, used, avail, refer, volsize, origin, clones = parts

        return DatasetInfo(
            name=name,
            type=dtype,
            used=int(used) if used != "-" else 0,
            available=int(avail) if avail != "-" else 0,
            referenced=int(refer) if refer != "-" else 0,
            volsize=int(volsize) if volsize and volsize != "-" else None,
            origin=origin if origin and origin != "-" else None,
            clones=clones.split(",") if clones and clones != "-" else [],
        )

    def list_snapshots(self, dataset: str | None = None) -> list[SnapshotInfo]:
        """List ZFS snapshots.

        Args:
            dataset: Optional dataset to filter (defaults to all CSI snapshots)

        Returns:
            List of SnapshotInfo
        """
        cmd = ["zfs", "list", "-H", "-p", "-t", "snapshot", "-o", "name,used,refer,clones"]
        if dataset:
            cmd.extend(["-r", dataset])
        else:
            cmd.extend(["-r", self.csi_path])

        try:
            result = self._run(cmd)
        except subprocess.CalledProcessError:
            return []

        snapshots = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 4:
                continue

            name, used, refer, clones = parts

            if "@" not in name:
                continue

            dataset_part, snap_name = name.rsplit("@", 1)

            snap = SnapshotInfo(
                name=name,
                dataset=dataset_part,
                snap_name=snap_name,
                used=int(used) if used != "-" else 0,
                referenced=int(refer) if refer != "-" else 0,
                clones=clones.split(",") if clones and clones != "-" else [],
            )
            snapshots.append(snap)

        return snapshots

    def verify_dataset_exists(self, dataset: str) -> bool:
        """Check if a ZFS dataset exists.

        Args:
            dataset: Full dataset path

        Returns:
            True if exists
        """
        result = self._run(["zfs", "list", "-H", dataset], check=False)
        return result.returncode == 0

    def verify_snapshot_exists(self, snapshot: str) -> bool:
        """Check if a ZFS snapshot exists.

        Args:
            snapshot: Full snapshot path (dataset@snap)

        Returns:
            True if exists
        """
        result = self._run(["zfs", "list", "-H", "-t", "snapshot", snapshot], check=False)
        return result.returncode == 0

    def get_origin(self, dataset: str) -> str | None:
        """Get the origin snapshot of a clone.

        Args:
            dataset: Dataset path

        Returns:
            Origin snapshot path or None
        """
        try:
            result = self._run(["zfs", "get", "-H", "-o", "value", "origin", dataset])
            origin = result.stdout.strip()
            return origin if origin and origin != "-" else None
        except subprocess.CalledProcessError:
            return None

    def get_clones(self, snapshot: str) -> list[str]:
        """Get clones of a snapshot.

        Args:
            snapshot: Snapshot path (dataset@snap)

        Returns:
            List of clone dataset paths
        """
        try:
            result = self._run(["zfs", "get", "-H", "-o", "value", "clones", snapshot])
            clones = result.stdout.strip()
            if clones and clones != "-":
                return clones.split(",")
            return []
        except subprocess.CalledProcessError:
            return []

    # -------------------------------------------------------------------------
    # CTL Operations
    # -------------------------------------------------------------------------

    def get_ctld_config(self) -> str:
        """Read the ctld configuration file.

        Returns:
            Contents of /etc/ctl.conf
        """
        try:
            with open("/etc/ctl.conf") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def list_ctl_luns(self) -> list[LunInfo]:
        """List CTL LUNs using XML output.

        Returns:
            List of LunInfo
        """
        try:
            result = self._run(["ctladm", "devlist", "-x"])
        except subprocess.CalledProcessError:
            return []

        luns = []

        try:
            root = ET.fromstring(result.stdout)
            for lun_elem in root.findall("lun"):
                lun_id = int(lun_elem.get("id", -1))

                # Get text content with defaults
                def get_text(tag: str, default: str = "") -> str:
                    elem = lun_elem.find(tag)
                    return elem.text if elem is not None and elem.text else default

                def get_int(tag: str, default: int = 0) -> int:
                    text = get_text(tag, str(default))
                    try:
                        return int(text)
                    except ValueError:
                        return default

                lun = LunInfo(
                    lun_id=lun_id,
                    backend=get_text("backend_type", "unknown"),
                    serial=get_text("serial_number"),
                    device_id=get_text("device_id"),
                    size=get_int("size") * get_int("blocksize", 512),  # Convert to bytes
                    blocksize=get_int("blocksize", 512),
                    path=get_text("file") or None,
                )
                luns.append(lun)
        except ET.ParseError:
            # Fall back to empty list on parse error
            return []

        return luns

    def list_ctl_ports(self) -> list[PortInfo]:
        """List CTL ports (iSCSI targets, NVMe controllers).

        Returns:
            List of PortInfo
        """
        try:
            result = self._run(["ctladm", "portlist", "-v"])
        except subprocess.CalledProcessError:
            return []

        ports = []
        current_port = None

        for line in result.stdout.split("\n"):
            line = line.strip()

            # Port header: "Port 0: ..."
            port_match = re.match(r"Port\s+(\d+):\s*(\w+)\s*(.*)$", line)
            if port_match:
                if current_port:
                    ports.append(current_port)

                port_id = int(port_match.group(1))
                port_type = port_match.group(2).lower()
                rest = port_match.group(3)

                current_port = PortInfo(
                    port_id=port_id,
                    port_type=port_type,
                    target_name="",
                    online="Online" in rest,
                )

            # Target name line
            elif current_port and ("target:" in line.lower() or "nqn:" in line.lower()):
                name_match = re.search(r"(?:target|nqn):\s*(.+)", line, re.IGNORECASE)
                if name_match:
                    current_port.target_name = name_match.group(1).strip()

        if current_port:
            ports.append(current_port)

        return ports

    def verify_volume_exported(self, volume_id: str, export_type: str = "iscsi") -> bool:
        """Check if a volume is exported via CTL.

        Args:
            volume_id: Volume ID (PV name)
            export_type: "iscsi" or "nvmeof"

        Returns:
            True if volume is exported
        """
        # Check if there's a LUN for this volume
        luns = self.list_ctl_luns()
        dataset_path = f"/dev/zvol/{self.csi_path}/{volume_id}"

        for lun in luns:
            if lun.path and dataset_path in lun.path:
                return True
            if volume_id in lun.device_id:
                return True

        return False

    def verify_volume_not_exported(self, volume_id: str) -> bool:
        """Check if a volume is NOT exported.

        Args:
            volume_id: Volume ID

        Returns:
            True if NOT exported
        """
        return not self.verify_volume_exported(volume_id)

    # -------------------------------------------------------------------------
    # iSCSI Operations
    # -------------------------------------------------------------------------

    def list_iscsi_targets(self) -> list[dict]:
        """List iSCSI targets from ctld perspective.

        Returns:
            List of target info dicts
        """
        # Parse ctl.conf for target definitions
        config = self.get_ctld_config()
        targets = []

        # Simple regex to find target blocks
        target_pattern = re.compile(
            r'target\s+"([^"]+)"\s*\{([^}]*)\}', re.DOTALL | re.MULTILINE
        )

        for match in target_pattern.finditer(config):
            target_name = match.group(1)
            target_body = match.group(2)

            target = {
                "name": target_name,
                "portal_group": None,
                "auth_group": None,
                "luns": [],
            }

            # Extract portal-group
            pg_match = re.search(r"portal-group\s+(\S+)", target_body)
            if pg_match:
                target["portal_group"] = pg_match.group(1)

            # Extract auth-group
            ag_match = re.search(r"auth-group\s+(\S+)", target_body)
            if ag_match:
                target["auth_group"] = ag_match.group(1)

            # Extract LUNs
            lun_pattern = re.compile(r'lun\s+(\d+)\s+"([^"]+)"')
            for lun_match in lun_pattern.finditer(target_body):
                target["luns"].append({
                    "id": int(lun_match.group(1)),
                    "name": lun_match.group(2),
                })

            targets.append(target)

        return targets

    # -------------------------------------------------------------------------
    # State Snapshots
    # -------------------------------------------------------------------------

    def capture_state(self) -> StorageState:
        """Capture complete storage state.

        Returns:
            StorageState with all current info
        """
        return StorageState(
            datasets=self.list_datasets(),
            snapshots=self.list_snapshots(),
            luns=self.list_ctl_luns(),
            ports=self.list_ctl_ports(),
            ctld_config=self.get_ctld_config(),
        )

    def diff_state(self, before: StorageState, after: StorageState) -> dict:
        """Compare two storage states.

        Args:
            before: State before operation
            after: State after operation

        Returns:
            Dict describing changes
        """
        before_datasets = {d.name for d in before.datasets}
        after_datasets = {d.name for d in after.datasets}

        before_snapshots = {s.name for s in before.snapshots}
        after_snapshots = {s.name for s in after.snapshots}

        before_luns = {l.lun_id for l in before.luns}
        after_luns = {l.lun_id for l in after.luns}

        return {
            "datasets": {
                "added": list(after_datasets - before_datasets),
                "removed": list(before_datasets - after_datasets),
            },
            "snapshots": {
                "added": list(after_snapshots - before_snapshots),
                "removed": list(before_snapshots - after_snapshots),
            },
            "luns": {
                "added": list(after_luns - before_luns),
                "removed": list(before_luns - after_luns),
            },
            "config_changed": before.ctld_config != after.ctld_config,
        }

    def state_to_dict(self, state: StorageState) -> dict:
        """Convert state to JSON-serializable dict.

        Args:
            state: StorageState to convert

        Returns:
            Dict representation
        """
        return {
            "datasets": [
                {
                    "name": d.name,
                    "type": d.type,
                    "used": d.used,
                    "available": d.available,
                    "volsize": d.volsize,
                    "origin": d.origin,
                    "clones": d.clones,
                }
                for d in state.datasets
            ],
            "snapshots": [
                {
                    "name": s.name,
                    "dataset": s.dataset,
                    "snap_name": s.snap_name,
                    "used": s.used,
                    "clones": s.clones,
                }
                for s in state.snapshots
            ],
            "luns": [
                {
                    "lun_id": l.lun_id,
                    "backend": l.backend,
                    "path": l.path,
                    "size": l.size,
                }
                for l in state.luns
            ],
            "ports": [
                {
                    "port_id": p.port_id,
                    "port_type": p.port_type,
                    "target_name": p.target_name,
                    "online": p.online,
                }
                for p in state.ports
            ],
        }
