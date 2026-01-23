"""Log collection from CSI components for E2E tests.

Collects and correlates logs from:
- CSI Controller pods
- CSI Node pods
- ctld-agent service
- System logs
"""

import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .k8s_client import K8sClient


@dataclass
class LogEntry:
    """A parsed log entry."""

    timestamp: datetime | None
    level: str
    message: str
    source: str
    raw: str


@dataclass
class CollectedLogs:
    """Collection of logs from all sources."""

    csi_controller: str
    csi_node: str
    ctld_agent: str
    system: str
    start_time: datetime
    end_time: datetime


class LogCollector:
    """Collect and correlate logs from CSI components."""

    # Common log patterns
    RUST_LOG_PATTERN = re.compile(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z?)\s+"  # timestamp
        r"(\w+)\s+"  # level
        r"(.+)"  # message
    )

    ERROR_PATTERNS = [
        re.compile(r"error", re.IGNORECASE),
        re.compile(r"failed", re.IGNORECASE),
        re.compile(r"panic", re.IGNORECASE),
        re.compile(r"exception", re.IGNORECASE),
    ]

    def __init__(
        self,
        k8s: K8sClient,
        controller_label: str = "app=freebsd-csi-controller",
        node_label: str = "app=freebsd-csi-node",
        csi_namespace: str = "kube-system",
    ):
        """Initialize log collector.

        Args:
            k8s: K8sClient instance
            controller_label: Label selector for controller pods
            node_label: Label selector for node pods
            csi_namespace: Namespace where CSI components run
        """
        self.k8s = k8s
        self.controller_label = controller_label
        self.node_label = node_label
        self.csi_namespace = csi_namespace
        self.start_time: datetime | None = None

    def start_collection(self) -> None:
        """Mark the start time for log collection."""
        self.start_time = datetime.utcnow()

    def _since_duration(self) -> str:
        """Calculate duration since start for kubectl --since flag."""
        if not self.start_time:
            return "5m"

        delta = datetime.utcnow() - self.start_time
        seconds = int(delta.total_seconds()) + 10  # Add buffer
        return f"{seconds}s"

    def _get_pod_names(self, label: str, namespace: str) -> list[str]:
        """Get pod names matching a label selector."""
        try:
            result = subprocess.run(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    "pods",
                    "-l",
                    label,
                    "-o",
                    "jsonpath={.items[*].metadata.name}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip().split() if result.stdout.strip() else []
        except subprocess.CalledProcessError:
            return []

    def _get_logs_for_pods(
        self,
        label: str,
        namespace: str,
        since: str | None = None,
        container: str | None = None,
    ) -> str:
        """Get logs from pods matching a label."""
        since = since or self._since_duration()
        pods = self._get_pod_names(label, namespace)

        all_logs = []
        for pod in pods:
            try:
                cmd = ["kubectl", "-n", namespace, "logs", pod, "--since", since]
                if container:
                    cmd.extend(["-c", container])

                result = subprocess.run(cmd, capture_output=True, text=True, check=False)
                if result.stdout:
                    all_logs.append(f"=== Pod: {pod} ===")
                    all_logs.append(result.stdout)
            except Exception:
                pass

        return "\n".join(all_logs)

    def get_controller_logs(self, since: str | None = None) -> str:
        """Get logs from CSI controller pods.

        Args:
            since: Duration (e.g., "5m") or uses time since start_collection

        Returns:
            Combined controller logs
        """
        return self._get_logs_for_pods(
            self.controller_label,
            self.csi_namespace,
            since,
            container="csi-driver",
        )

    def get_node_logs(self, since: str | None = None) -> str:
        """Get logs from CSI node pods.

        Args:
            since: Duration or uses time since start_collection

        Returns:
            Combined node logs
        """
        return self._get_logs_for_pods(
            self.node_label,
            self.csi_namespace,
            since,
            container="csi-driver",
        )

    def get_ctld_agent_logs(self, since: str | None = None) -> str:
        """Get logs from ctld-agent service (local FreeBSD).

        Checks both journalctl (if available) and log file.

        Args:
            since: Duration (not currently used for file logs)

        Returns:
            ctld-agent logs
        """
        logs = []

        # Try reading log file directly
        log_files = [
            "/var/log/ctld-agent.log",
            "/var/log/messages",
        ]

        for log_file in log_files:
            try:
                with open(log_file) as f:
                    content = f.read()
                    # Filter for ctld-agent entries
                    agent_lines = [
                        line
                        for line in content.split("\n")
                        if "ctld-agent" in line.lower() or "ctld_agent" in line.lower()
                    ]
                    if agent_lines:
                        logs.append(f"=== {log_file} ===")
                        # Get last 100 lines
                        logs.extend(agent_lines[-100:])
            except Exception:
                pass

        return "\n".join(logs)

    def get_system_logs(self, since: str | None = None) -> str:
        """Get relevant system logs from /var/log/messages.

        Args:
            since: Not currently used

        Returns:
            Relevant system log entries
        """
        patterns = ["ctld", "iscsi", "zfs", "kernel:.*cam", "nvme"]

        try:
            with open("/var/log/messages") as f:
                lines = f.readlines()

            relevant = []
            for line in lines[-500:]:  # Last 500 lines
                for pattern in patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        relevant.append(line.rstrip())
                        break

            return "\n".join(relevant)
        except Exception:
            return ""

    def collect_all(self, since: str | None = None) -> CollectedLogs:
        """Collect logs from all sources.

        Args:
            since: Duration to look back

        Returns:
            CollectedLogs with all log data
        """
        end_time = datetime.utcnow()
        start_time = self.start_time or end_time

        return CollectedLogs(
            csi_controller=self.get_controller_logs(since),
            csi_node=self.get_node_logs(since),
            ctld_agent=self.get_ctld_agent_logs(since),
            system=self.get_system_logs(since),
            start_time=start_time,
            end_time=end_time,
        )

    def parse_log_line(self, line: str, source: str) -> LogEntry | None:
        """Parse a single log line.

        Args:
            line: Raw log line
            source: Log source identifier

        Returns:
            LogEntry or None if unparseable
        """
        # Try Rust/tracing format
        match = self.RUST_LOG_PATTERN.match(line)
        if match:
            try:
                ts = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
            except ValueError:
                ts = None

            return LogEntry(
                timestamp=ts,
                level=match.group(2).upper(),
                message=match.group(3),
                source=source,
                raw=line,
            )

        # Return as-is with unknown format
        return LogEntry(
            timestamp=None,
            level="UNKNOWN",
            message=line,
            source=source,
            raw=line,
        )

    def find_errors(self, logs: CollectedLogs) -> list[LogEntry]:
        """Extract error entries from logs.

        Args:
            logs: Collected logs

        Returns:
            List of error LogEntry objects
        """
        errors = []

        sources = [
            ("controller", logs.csi_controller),
            ("node", logs.csi_node),
            ("ctld-agent", logs.ctld_agent),
            ("system", logs.system),
        ]

        for source, content in sources:
            for line in content.split("\n"):
                if not line.strip():
                    continue

                for pattern in self.ERROR_PATTERNS:
                    if pattern.search(line):
                        entry = self.parse_log_line(line, source)
                        if entry:
                            errors.append(entry)
                        break

        return errors

    def correlate_with_operation(
        self,
        logs: CollectedLogs,
        operation: str,
        resource: str | None = None,
    ) -> list[LogEntry]:
        """Find log entries related to a specific operation.

        Args:
            logs: Collected logs
            operation: Operation type (e.g., "CreateVolume", "DeleteVolume")
            resource: Optional resource name to filter

        Returns:
            List of related LogEntry objects
        """
        related = []
        patterns = [re.compile(re.escape(operation), re.IGNORECASE)]

        if resource:
            patterns.append(re.compile(re.escape(resource), re.IGNORECASE))

        sources = [
            ("controller", logs.csi_controller),
            ("node", logs.csi_node),
            ("ctld-agent", logs.ctld_agent),
        ]

        for source, content in sources:
            for line in content.split("\n"):
                if not line.strip():
                    continue

                # Check if any pattern matches
                if any(p.search(line) for p in patterns):
                    entry = self.parse_log_line(line, source)
                    if entry:
                        related.append(entry)

        # Sort by timestamp if available
        related.sort(key=lambda e: e.timestamp or datetime.min)
        return related

    def format_for_report(self, logs: CollectedLogs, max_lines: int = 50) -> str:
        """Format logs for inclusion in a test report.

        Args:
            logs: Collected logs
            max_lines: Maximum lines per source

        Returns:
            Formatted string for report
        """
        sections = []

        sources = [
            ("CSI Controller", logs.csi_controller),
            ("CSI Node", logs.csi_node),
            ("ctld-agent", logs.ctld_agent),
            ("System", logs.system),
        ]

        for name, content in sources:
            lines = content.strip().split("\n")
            if not lines or (len(lines) == 1 and not lines[0]):
                continue

            sections.append(f"\n{'=' * 60}")
            sections.append(f"{name} Logs")
            sections.append("=" * 60)

            if len(lines) > max_lines:
                sections.append(f"[Showing last {max_lines} of {len(lines)} lines]")
                lines = lines[-max_lines:]

            sections.extend(lines)

        return "\n".join(sections)

    def save_logs(self, logs: CollectedLogs, path: str) -> None:
        """Save collected logs to a file.

        Args:
            logs: Collected logs
            path: Output file path
        """
        with open(path, "w") as f:
            f.write(f"Log collection period: {logs.start_time} - {logs.end_time}\n")
            f.write("\n")
            f.write(self.format_for_report(logs, max_lines=1000))
