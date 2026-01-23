"""gRPC client for direct communication with ctld-agent.

This client allows E2E tests to directly call the ctld-agent's gRPC service,
bypassing Kubernetes. Primary use case is cleanup of Retain policy volumes
where Kubernetes doesn't call DeleteVolume.
"""

import grpc
from typing import Optional

# Import generated protobuf stubs
from lib.proto import ctld_agent_pb2
from lib.proto import ctld_agent_pb2_grpc


class AgentClient:
    """Client for ctld-agent gRPC service."""

    def __init__(
        self,
        address: str = "10.0.0.10:50051",
        timeout: float = 30.0,
    ):
        """Initialize the agent client.

        Args:
            address: ctld-agent gRPC address (host:port)
            timeout: Default timeout for RPC calls in seconds
        """
        self.address = address
        self.timeout = timeout
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[ctld_agent_pb2_grpc.StorageAgentStub] = None

    def _get_stub(self) -> ctld_agent_pb2_grpc.StorageAgentStub:
        """Get or create the gRPC stub."""
        if self._stub is None:
            self._channel = grpc.insecure_channel(self.address)
            self._stub = ctld_agent_pb2_grpc.StorageAgentStub(self._channel)
        return self._stub

    def close(self) -> None:
        """Close the gRPC channel."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None

    def delete_volume(self, volume_id: str) -> bool:
        """Delete a volume via ctld-agent.

        This properly unexports the iSCSI/NVMeoF target and deletes the ZFS dataset.
        Used for cleaning up Retain policy volumes where K8s doesn't call DeleteVolume.

        Args:
            volume_id: Volume ID (typically the PV name, e.g., pvc-xxx-xxx-xxx)

        Returns:
            True if deleted successfully, False if not found (already deleted)

        Raises:
            grpc.RpcError: On gRPC communication errors
        """
        stub = self._get_stub()
        request = ctld_agent_pb2.DeleteVolumeRequest(volume_id=volume_id)

        try:
            stub.DeleteVolume(request, timeout=self.timeout)
            return True
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                # Volume already deleted - this is fine
                return False
            raise

    def list_volumes(self) -> list[dict]:
        """List all volumes from ctld-agent.

        Useful for debugging and verifying cleanup.

        Returns:
            List of volume info dicts
        """
        stub = self._get_stub()
        request = ctld_agent_pb2.ListVolumesRequest()

        response = stub.ListVolumes(request, timeout=self.timeout)
        return [
            {
                "id": v.id,
                "name": v.name,
                "size_bytes": v.size_bytes,
                "zfs_dataset": v.zfs_dataset,
                "target_name": v.target_name,
            }
            for v in response.volumes
        ]

    def get_volume(self, volume_id: str) -> Optional[dict]:
        """Get volume info from ctld-agent.

        Args:
            volume_id: Volume ID

        Returns:
            Volume info dict or None if not found
        """
        stub = self._get_stub()
        request = ctld_agent_pb2.GetVolumeRequest(volume_id=volume_id)

        try:
            response = stub.GetVolume(request, timeout=self.timeout)
            v = response.volume
            return {
                "id": v.id,
                "name": v.name,
                "size_bytes": v.size_bytes,
                "zfs_dataset": v.zfs_dataset,
                "target_name": v.target_name,
            }
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise

    def __enter__(self) -> "AgentClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - close channel."""
        self.close()
