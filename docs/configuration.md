# Configuration Reference

This document provides a comprehensive reference for all configuration options in the FreeBSD CSI driver.

## Table of Contents

- [ctld-agent Configuration](#ctld-agent-configuration)
  - [Command-Line Arguments](#command-line-arguments)
  - [TLS Configuration](#tls-configuration)
  - [ZFS Dataset Requirements](#zfs-dataset-requirements)
  - [Network Configuration](#network-configuration)
  - [CTL Configuration (UCL)](#ctl-configuration-ucl)
- [CSI Driver Configuration](#csi-driver-configuration)
  - [Command-Line Arguments](#csi-driver-command-line-arguments)
  - [TLS Configuration](#csi-driver-tls-configuration)
  - [Environment Variables](#environment-variables)
  - [StorageClass Parameters](#storageclass-parameters)
- [Security Considerations](#security-considerations)
  - [mTLS Setup](#mtls-setup)
  - [RBAC Permissions](#rbac-permissions)
  - [Network Policies](#network-policies)
  - [Volume Access Control](#volume-access-control)
- [Platform Support](#platform-support)
  - [Linux Worker Nodes](#linux-worker-nodes)
  - [FreeBSD Worker Nodes](#freebsd-worker-nodes)

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
| `--tls-cert` | - | No | TLS certificate file (PEM format) for server identity. |
| `--tls-key` | - | No | TLS private key file (PEM format). |
| `--tls-client-ca` | - | No | CA certificate for client verification (enables mTLS). |
| `--ctl-config` | `/etc/ctl.ucl` | No | Path to ctld UCL configuration file. |
| `--auth-group` | `ag0` | No | Auth group name for iSCSI targets in UCL config. |
| `--portal-group-name` | `pg0` | No | Portal group name for iSCSI targets in UCL config. |

#### Examples

**Minimal configuration:**
```bash
ctld-agent --zfs-parent tank/csi
```

**Production configuration with mTLS:**
```bash
ctld-agent \
  --listen [::]:50051 \
  --zfs-parent tank/kubernetes/volumes \
  --base-iqn iqn.2024-01.com.example.storage \
  --ctl-config /etc/ctl.ucl \
  --auth-group ag0 \
  --portal-group-name pg0 \
  --tls-cert /etc/ctld-agent/server.crt \
  --tls-key /etc/ctld-agent/server.key \
  --tls-client-ca /etc/ctld-agent/ca.crt
```

### TLS Configuration

The ctld-agent supports optional TLS encryption and mutual TLS (mTLS) authentication.

| Mode | Arguments Required | Description |
|------|-------------------|-------------|
| Plaintext | None | No encryption (development only) |
| TLS (server-only) | `--tls-cert`, `--tls-key` | Encrypted connection, no client verification |
| mTLS | `--tls-cert`, `--tls-key`, `--tls-client-ca` | Encrypted connection with client certificate verification |

**Environment variables:**
- `TLS_CERT_PATH` - Alternative to `--tls-cert`
- `TLS_KEY_PATH` - Alternative to `--tls-key`
- `TLS_CLIENT_CA_PATH` - Alternative to `--tls-client-ca`
- `CTL_CONFIG_PATH` - Alternative to `--ctl-config`
- `CTL_AUTH_GROUP` - Alternative to `--auth-group`
- `CTL_PORTAL_GROUP_NAME` - Alternative to `--portal-group-name`

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

#### Volume Metadata

Volume metadata is automatically persisted in ZFS user properties:
- Property: `user:csi:metadata`
- Contains: export type, target name, LUN ID, creation time

This metadata survives ctld-agent restarts and is automatically restored on startup.

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

### CTL Configuration (UCL)

The ctld-agent manages iSCSI targets through FreeBSD's ctld daemon using UCL configuration files.

#### How It Works

1. CSI-managed targets are written to a marked section in the UCL config file
2. User-managed targets outside this section are preserved
3. After changes, ctld-agent reloads ctld to apply the configuration

#### Configuration File

By default, ctld-agent manages targets in `/etc/ctl.ucl`. The CSI-managed section is marked with:

```text
# BEGIN CSI-MANAGED TARGETS - DO NOT EDIT
target "iqn.2024-01.org.freebsd.csi:pvc-abc123" {
    auth-group = "ag0"
    portal-group = "pg0"
    lun 0 {
        path = "/dev/zvol/tank/csi/pvc-abc123"
        blocksize = 512
    }
}
# END CSI-MANAGED TARGETS
```

**Important:** Do not manually edit content between these markers. It will be overwritten.

#### Prerequisites

Ensure ctld is configured to use UCL format and the portal/auth groups exist:

```text
# /etc/ctl.ucl - Example base configuration

auth-group ag0 {
    auth-type = none
}

portal-group pg0 {
    discovery-auth-group = no-authentication
    listen = 0.0.0.0:3260
}

# CSI targets will be added below by ctld-agent
```

Start ctld with UCL format (typically via `/etc/rc.local`):
```bash
/usr/sbin/ctld -u -f /etc/ctl.ucl
```

#### NVMeoF Limitations

NVMeoF targets currently use `ctladm` directly and are ephemeral (not persisted across reboots).
FreeBSD 15.0+ ctld supports NVMeoF via UCL configuration using `controller` blocks, but this is
not yet implemented in ctld-agent. For persistent NVMeoF targets, configure them manually in the UCL file.

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
| `--tls-cert` | - | TLS certificate file for client identity |
| `--tls-key` | - | TLS private key file |
| `--tls-ca` | - | CA certificate for server verification |
| `--tls-domain` | `ctld-agent` | Domain name for TLS certificate verification |

### CSI Driver TLS Configuration

The CSI driver supports mTLS for secure communication with ctld-agent.

**All three options must be provided together for mTLS:**
- `--tls-cert` / `TLS_CERT_PATH`
- `--tls-key` / `TLS_KEY_PATH`
- `--tls-ca` / `TLS_CA_PATH`

**Example with mTLS:**
```bash
csi-driver \
  --controller \
  --agent-endpoint https://ctld-agent.storage.svc:50051 \
  --tls-cert /etc/csi/client.crt \
  --tls-key /etc/csi/client.key \
  --tls-ca /etc/csi/ca.crt \
  --tls-domain ctld-agent
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `CSI_NODE_ID` | Alternative to `--node-id` argument |
| `AGENT_ENDPOINT` | Alternative to `--agent-endpoint` argument |
| `TLS_CERT_PATH` | Alternative to `--tls-cert` argument |
| `TLS_KEY_PATH` | Alternative to `--tls-key` argument |
| `TLS_CA_PATH` | Alternative to `--tls-ca` argument |
| `TLS_DOMAIN` | Alternative to `--tls-domain` argument |
| `RUST_LOG` | Control logging verbosity (e.g., `debug`, `csi_driver=trace`) |

### StorageClass Parameters

StorageClass parameters control how volumes are provisioned:

#### Connection Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `exportType` | `iscsi`, `nvmeof` | `iscsi` | Protocol for exporting volumes |
| `fs_type` | `ext4`, `xfs` (Linux); `ufs` (FreeBSD) | `ext4` | Filesystem type for formatting volumes |
| `endpoints` | `<ip>:<port>[,<ip2>:<port2>...]` | - | **Required on Linux.** Comma-separated list of target endpoints. Default ports: iSCSI=3260, NVMeoF=4420 |

> **Multipath Support:** The `endpoints` parameter accepts comma-separated values for multipath configurations.
> For iSCSI, each portal will be discovered and logged into separately. For NVMeoF, each address will be connected separately.
> Native multipath (NVMe) or dm-multipath (iSCSI) will combine the paths automatically.

#### Block Device Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `blockSize` | `512`, `4096` | CTL default | Logical block size for the volume |
| `physicalBlockSize` | `512`, `4096`, etc. | - | Physical block size hint for storage optimization |
| `enableUnmap` | `true`, `false` | `false` | Enable TRIM/discard passthrough for SSD-backed storage |

> **Note:** These parameters also accept alternative naming conventions: `block_size`, `physical_block_size`/`pblocksize`, `enable_unmap`/`unmap`.

#### Filesystem Types by Platform

| Platform | Supported fs_type | Default |
|----------|-----------------|---------|
| Linux | `ext4`, `xfs` | `ext4` |
| FreeBSD | `ufs` | `ufs` |

> **Note:** `zfs` cannot be used as `fs_type` because ZFS manages its own storage layer and cannot format block devices.

#### Example StorageClasses

**iSCSI with ext4 (Linux workers):**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fs_type: ext4
  endpoints: "192.168.1.100:3260"  # REQUIRED for Linux (default port: 3260)
  blockSize: "4096"                # Optional: 4K block size
  enableUnmap: "true"              # Optional: Enable TRIM/discard
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**iSCSI with Multipath (high availability):**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi-ha
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fs_type: ext4
  # Multiple endpoints for multipath - dm-multipath combines paths automatically
  endpoints: "10.0.0.1:3260,10.0.0.2:3260"
  blockSize: "4096"
  enableUnmap: "true"
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
  fs_type: ext4
  endpoints: "192.168.1.100:4420"  # REQUIRED (default port: 4420)
  blockSize: "4096"
  enableUnmap: "true"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**NVMeoF with Multipath:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-nvmeof-ha
provisioner: csi.freebsd.org
parameters:
  exportType: nvmeof
  fs_type: ext4
  # Multiple endpoints for multipath - native NVMe multipath combines paths
  endpoints: "10.0.0.1:4420,10.0.0.2:4420"
  blockSize: "4096"
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
  fs_type: ext4
  endpoints: "192.168.1.100:3260"
allowVolumeExpansion: true
reclaimPolicy: Retain
volumeBindingMode: Immediate
```

---

## Security Considerations

### mTLS Setup

For production deployments, enable mTLS between the CSI driver and ctld-agent.

#### Generate Certificates

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt \
  -subj "/CN=FreeBSD-CSI-CA"

# Generate server certificate (ctld-agent)
openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr \
  -subj "/CN=ctld-agent"
openssl x509 -req -days 365 -in server.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt

# Generate client certificate (csi-driver)
openssl genrsa -out client.key 4096
openssl req -new -key client.key -out client.csr \
  -subj "/CN=csi-driver"
openssl x509 -req -days 365 -in client.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt
```

#### Deploy Certificates

**On FreeBSD (ctld-agent):**
```bash
mkdir -p /etc/ctld-agent
cp ca.crt server.crt server.key /etc/ctld-agent/
chmod 600 /etc/ctld-agent/*.key
```

**In Kubernetes (csi-driver):**
```bash
kubectl create secret generic csi-tls-certs \
  --from-file=ca.crt=ca.crt \
  --from-file=client.crt=client.crt \
  --from-file=client.key=client.key \
  -n kube-system
```

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
3. **mTLS for gRPC** - Enable mTLS between CSI driver and ctld-agent

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

## Platform Support

### Linux Worker Nodes

Linux worker nodes are the primary deployment target. Required packages:

**For iSCSI:**
```bash
# Debian/Ubuntu
apt-get install open-iscsi

# RHEL/CentOS/Fedora
dnf install iscsi-initiator-utils

# Start the iSCSI daemon
systemctl enable --now iscsid
```

**For NVMeoF:**
```bash
# Debian/Ubuntu
apt-get install nvme-cli

# RHEL/CentOS/Fedora
dnf install nvme-cli

# Load NVMe-oF kernel modules
modprobe nvme-tcp
```

**Filesystem tools:**
```bash
# ext4 (usually pre-installed)
apt-get install e2fsprogs

# XFS
apt-get install xfsprogs
```

### FreeBSD Worker Nodes

FreeBSD worker nodes are supported but experimental (Kubernetes on FreeBSD is limited).

**Required packages:**
- iSCSI initiator (built-in)
- NVMe support (built-in)

**Filesystem:** UFS (the default on FreeBSD)

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

**Mount failures on Linux:**
- Verify iSCSI initiator is running: `systemctl status iscsid`
- Check iSCSI sessions: `iscsiadm -m session`
- Verify portal is reachable: `nc -zv <portal-ip> 3260`
- Review CSI node pod logs

**mTLS connection fails:**
- Verify certificate paths are correct
- Check certificate validity: `openssl x509 -in cert.crt -text -noout`
- Ensure CA matches: certificates must be signed by the same CA
- Check domain name matches `--tls-domain`

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
