# Configuration Reference

This document provides a comprehensive reference for all configuration options in the FreeBSD CSI driver.

## Table of Contents

- [ctld-agent Configuration](#ctld-agent-configuration)
  - [Command-Line Arguments](#command-line-arguments)
  - [ZFS Dataset Requirements](#zfs-dataset-requirements)
  - [Network Configuration](#network-configuration)
- [CSI Driver Configuration](#csi-driver-configuration)
  - [Command-Line Arguments](#csi-driver-command-line-arguments)
  - [Environment Variables](#environment-variables)
  - [StorageClass Parameters](#storageclass-parameters)
- [Security Considerations](#security-considerations)
  - [RBAC Permissions](#rbac-permissions)
  - [Network Policies](#network-policies)
  - [Volume Access Control](#volume-access-control)

---

## ctld-agent Configuration

The ctld-agent is the FreeBSD daemon that manages ZFS volumes and CTL exports.

### Command-Line Arguments

| Argument | Default | Required | Description |
|----------|---------|----------|-------------|
| `--listen` | `[::1]:50051` | No | gRPC server listen address. Use `[::]:50051` to listen on all interfaces. |
| `--zfs-parent` | - | **Yes** | ZFS parent dataset where volumes will be created (e.g., `tank/csi`). |
| `--base-iqn` | `iqn.2024-01.org.freebsd.csi` | No | Base iSCSI Qualified Name for target naming. |
| `--base-nqn` | `nqn.2024-01.org.freebsd.csi` | No | Base NVMe Qualified Name for NVMeoF targets. |
| `--portal-group` | `1` | No | Portal group tag for iSCSI targets. |

#### Examples

**Minimal configuration:**
```bash
ctld-agent --zfs-parent tank/csi
```

**Production configuration with custom IQN:**
```bash
ctld-agent \
  --listen [::]:50051 \
  --zfs-parent tank/kubernetes/volumes \
  --base-iqn iqn.2024-01.com.example.storage \
  --base-nqn nqn.2024-01.com.example.storage \
  --portal-group 1
```

**Listen on specific interface:**
```bash
ctld-agent \
  --listen 192.168.1.100:50051 \
  --zfs-parent tank/csi
```

### ZFS Dataset Requirements

The parent dataset specified by `--zfs-parent` must:

1. **Exist before starting ctld-agent**
   ```bash
   zfs create tank/csi
   ```

2. **Have sufficient space** for volume creation
   ```bash
   zfs get available tank/csi
   ```

3. **Be writable** by the ctld-agent process (requires root privileges)

#### Recommended Dataset Properties

```bash
# Enable compression for space efficiency
zfs set compression=lz4 tank/csi

# Set a quota to prevent runaway usage
zfs set quota=500G tank/csi

# Set a reservation to guarantee minimum space
zfs set reservation=50G tank/csi

# Enable atime=off for performance
zfs set atime=off tank/csi
```

#### Volume Naming

Volumes are created as zvols under the parent dataset:
- Dataset path: `{zfs-parent}/{volume-name}`
- Example: `tank/csi/pvc-12345678-abcd-efgh-ijkl-123456789abc`

### Network Configuration

#### gRPC Listen Address

The `--listen` argument accepts:
- IPv4 address: `192.168.1.100:50051`
- IPv6 address: `[2001:db8::1]:50051`
- All IPv4: `0.0.0.0:50051`
- All IPv6: `[::]:50051`
- Localhost only: `127.0.0.1:50051` or `[::1]:50051`

#### Firewall Configuration

Allow gRPC traffic on the configured port:

```bash
# Using ipfw
ipfw add allow tcp from any to me 50051

# Using pf (add to /etc/pf.conf)
pass in on $ext_if proto tcp to port 50051
```

#### iSCSI Portal Configuration

For iSCSI targets, ensure the following ports are accessible:
- TCP 3260 (iSCSI)

#### NVMeoF Configuration

For NVMeoF targets, ensure:
- TCP 4420 (NVMe-oF/TCP) or
- RDMA port (for NVMe-oF/RDMA)

---

## CSI Driver Configuration

The csi-driver runs in Kubernetes and implements the Container Storage Interface.

### CSI Driver Command-Line Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--endpoint` | `unix:///var/run/csi/csi.sock` | CSI endpoint (Unix socket path) |
| `--node-id` | System hostname | Unique identifier for this CSI node |
| `--agent-endpoint` | `http://127.0.0.1:50051` | ctld-agent gRPC endpoint |
| `--controller` | `false` | Enable controller service |
| `--node` | `true` | Enable node service |
| `--driver-name` | `csi.freebsd.org` | CSI driver name |
| `--log-level` | `info` | Log level (trace, debug, info, warn, error) |

### Environment Variables

| Variable | Description |
|----------|-------------|
| `CSI_NODE_ID` | Alternative to `--node-id` argument |
| `AGENT_ENDPOINT` | Alternative to `--agent-endpoint` argument |
| `RUST_LOG` | Control logging verbosity (e.g., `debug`, `csi_driver=trace`) |

### StorageClass Parameters

StorageClass parameters control how volumes are provisioned:

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `exportType` | `iscsi`, `nvmeof` | `iscsi` | Protocol for exporting volumes |
| `fsType` | `ufs` | `ufs` | Filesystem type for formatting volumes |

#### Example StorageClasses

**iSCSI with UFS:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fsType: ufs
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**NVMeoF:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-nvmeof
provisioner: csi.freebsd.org
parameters:
  exportType: nvmeof
  fsType: ufs
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**Retain volumes on PVC deletion:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-retain
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fsType: ufs
allowVolumeExpansion: true
reclaimPolicy: Retain
volumeBindingMode: Immediate
```

---

## Security Considerations

### RBAC Permissions

The CSI driver controller requires specific Kubernetes RBAC permissions:

```yaml
rules:
  # PersistentVolume management
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "delete", "patch"]

  # PersistentVolumeClaim management
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "update"]

  # StorageClass access
  - apiGroups: ["storage.k8s.io"]
    resources: ["storageclasses"]
    verbs: ["get", "list", "watch"]

  # Event creation for status updates
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["list", "watch", "create", "update", "patch"]

  # Snapshot support
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshots"]
    verbs: ["get", "list"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshotcontents"]
    verbs: ["get", "list", "watch", "update", "patch"]

  # Node and CSINode access
  - apiGroups: ["storage.k8s.io"]
    resources: ["csinodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]

  # VolumeAttachment management
  - apiGroups: ["storage.k8s.io"]
    resources: ["volumeattachments"]
    verbs: ["get", "list", "watch", "patch"]
```

### Network Policies

Restrict access to the ctld-agent gRPC endpoint:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: ctld-agent-access
  namespace: storage
spec:
  podSelector:
    matchLabels:
      app: ctld-agent
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: kube-system
        - podSelector:
            matchLabels:
              app: freebsd-csi-controller
      ports:
        - protocol: TCP
          port: 50051
```

### Volume Access Control

#### iSCSI CHAP Authentication

For production deployments, configure CHAP authentication in your iSCSI targets:

1. Configure ctld with CHAP credentials
2. Configure Kubernetes nodes with initiator credentials
3. Reference credentials in your StorageClass

#### Network Segmentation

Best practices for network security:

1. **Dedicated storage network** - Use a separate VLAN for storage traffic
2. **Firewall rules** - Restrict access to storage ports (3260, 4420, 50051)
3. **TLS for gRPC** - Consider using TLS between CSI driver and ctld-agent

#### ZFS Permissions

The ctld-agent requires:
- Read/write access to the ZFS parent dataset
- Permission to create and destroy zvols
- Permission to manage CTL targets

This typically requires running as root or with appropriate ZFS delegations:

```bash
# Example: Delegate ZFS permissions (advanced)
zfs allow -u csi-user create,destroy,mount,snapshot tank/csi
```

---

## Troubleshooting

### Common Issues

**ctld-agent won't start:**
- Verify ZFS parent dataset exists: `zfs list tank/csi`
- Check CTL module is loaded: `kldstat | grep ctl`
- Verify port is available: `sockstat -4l | grep 50051`

**Volume creation fails:**
- Check ctld-agent logs for errors
- Verify network connectivity from Kubernetes to ctld-agent
- Ensure sufficient ZFS pool space

**Mount failures:**
- Verify iSCSI initiator is running on worker nodes
- Check target discovery: `iscsictl -L`
- Review CSI node pod logs

### Logging

Enable debug logging:

```bash
# ctld-agent
RUST_LOG=debug ctld-agent --zfs-parent tank/csi

# csi-driver
RUST_LOG=csi_driver=debug,tonic=debug
```

---

## Next Steps

- [Installation Guide](installation.md) - Detailed setup instructions
- [README](../README.md) - Project overview and quick start
