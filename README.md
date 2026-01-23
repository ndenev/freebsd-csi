# FreeBSD CSI Driver

A Kubernetes Container Storage Interface (CSI) driver for FreeBSD, enabling dynamic provisioning of persistent volumes backed by ZFS datasets exported via iSCSI or NVMeoF (NVMe over Fabrics).

## Overview

This project provides enterprise-grade block storage for Kubernetes clusters using FreeBSD's robust storage stack:

- **ZFS** - Advanced filesystem with snapshots, clones, compression, and data integrity
- **CTL** - FreeBSD's native SCSI target layer supporting iSCSI and NVMeoF protocols
- **CSI** - Kubernetes standard interface for storage provisioning

## Architecture

```
+------------------+       +------------------+       +------------------+
|   Kubernetes     |       |   CSI Driver     |       |  FreeBSD Node    |
|   Control Plane  |       |   (Controller)   |       |  (ctld-agent)    |
+------------------+       +------------------+       +------------------+
        |                          |                          |
        | PVC Create               | gRPC                     |
        +------------------------->+------------------------->|
        |                          |                          |
        |                          |    CreateVolume          |
        |                          |    - Create ZFS zvol     |
        |                          |    - Export via CTL      |
        |                          |    - Return target info  |
        |                          |<-------------------------+
        |                          |                          |
+------------------+       +------------------+                |
|   Kubernetes     |       |   CSI Driver     |                |
|   Worker Node    |       |   (Node)         |                |
+------------------+       +------------------+                |
        |                          |                          |
        | Mount Volume             |                          |
        +------------------------->|                          |
        |                          |  iSCSI/NVMeoF Login      |
        |                          +------------------------->|
        |                          |                          |
```

### Components

1. **ctld-agent** - FreeBSD daemon that manages:
   - ZFS zvol creation, deletion, expansion, and snapshots
   - CTL target configuration for iSCSI and NVMeoF exports
   - gRPC API for CSI driver communication

2. **csi-driver** - Kubernetes CSI driver with:
   - Controller service for volume lifecycle management
   - Node service for volume mounting/unmounting
   - Identity service for capability reporting

## Prerequisites

- **FreeBSD 13.0+** on storage node(s) (FreeBSD 15.0+ for NVMeoF)
- **ZFS** pool configured for storage
- **Kubernetes 1.25+** cluster
- **Helm 3.x** for deployment
- Network connectivity between Kubernetes nodes and FreeBSD storage node(s)

## Quick Start

### 1. FreeBSD Storage Node Setup

```bash
# Install ctld-agent from pkg (recommended)
pkg install ctld-agent

# Or build from source
git clone https://github.com/ndenev/freebsd-csi
cd freebsd-csi
cargo build --release -p ctld-agent
cp target/release/ctld-agent /usr/local/sbin/

# Create ZFS parent dataset
zfs create tank/csi

# Configure and start ctld-agent
sysrc ctld_agent_enable="YES"
sysrc ctld_agent_flags="--zfs-parent tank/csi --listen [::]:50051"
service ctld_agent start
```

### 2. Kubernetes Deployment (Helm)

```bash
# Install from OCI registry
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-IP>:50051 \
  --set storageClassIscsi.create=true \
  --set storageClassIscsi.parameters.portal=<FREEBSD-STORAGE-IP>:3260

# Or install from source
helm install freebsd-csi charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-IP>:50051
```

### 3. Create a PersistentVolumeClaim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-volume
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: freebsd-zfs-iscsi
  resources:
    requests:
      storage: 10Gi
```

## Documentation

- [Architecture Overview](docs/architecture.md) - System design and data flows
- [Installation Guide](docs/installation.md) - Detailed setup instructions
- [Configuration Reference](docs/configuration.md) - All configuration options
- [CHAP Authentication Setup](docs/chap-setup.md) - iSCSI/NVMeoF authentication guide
- [Metrics Reference](docs/metrics.md) - Prometheus metrics and Grafana dashboards
- [Operations Runbook](docs/runbook.md) - Troubleshooting and recovery procedures
- [Helm Chart README](charts/freebsd-csi/README.md) - Helm chart documentation

## Features

- Dynamic volume provisioning
- Volume expansion (online)
- Snapshots and clones
- iSCSI and NVMeoF export protocols
- **CHAP authentication** for iSCSI (one-way and mutual)
- Raw block volume support with ReadWriteMany
- mTLS support for secure communication
- Automatic recovery on restart
- Prometheus metrics and Grafana dashboards
- RBAC-secured Kubernetes integration

## CSI Driver Name

```
csi.freebsd.org
```

## StorageClasses

| Name | Export Protocol | Description |
|------|-----------------|-------------|
| `freebsd-zfs-iscsi` | iSCSI | ZFS volumes exported via iSCSI |
| `freebsd-zfs-nvmeof` | NVMeoF | ZFS volumes exported via NVMe over Fabrics (FreeBSD 15.0+) |

## Authentication

The CSI driver supports CHAP authentication for iSCSI to secure access to storage volumes. Credentials are passed via Kubernetes Secrets using the standard CSI secrets mechanism.

### Quick CHAP Setup

1. **Create a Secret with CHAP credentials:**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: iscsi-chap-secret
  namespace: default
type: Opaque
stringData:
  node.session.auth.username: "csi-initiator"
  node.session.auth.password: "MySecurePassword123!"
  # Optional: Mutual CHAP (target authenticates to initiator)
  node.session.auth.username_in: "csi-target"
  node.session.auth.password_in: "TargetPassword456!"
```

2. **Create a StorageClass referencing the secret:**

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi-chap
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  portal: "192.168.1.100:3260"
csi.storage.k8s.io/provisioner-secret-name: iscsi-chap-secret
csi.storage.k8s.io/provisioner-secret-namespace: default
csi.storage.k8s.io/node-stage-secret-name: iscsi-chap-secret
csi.storage.k8s.io/node-stage-secret-namespace: default
```

3. **Create a PVC using the authenticated StorageClass:**

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: secure-volume
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: freebsd-zfs-iscsi-chap
  resources:
    requests:
      storage: 10Gi
```

For detailed configuration options including mutual CHAP, see the [CHAP Authentication Setup Guide](docs/chap-setup.md).

## Monitoring

Enable Prometheus metrics by setting `metrics.enabled=true` in the Helm chart:

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --set metrics.enabled=true \
  --set metrics.port=9090
```

Key metrics include:
- `csi_operations_total{operation, status}` - CSI operation counts
- `csi_operation_duration_seconds{operation}` - Operation latency histogram
- `csi_agent_connected` - Agent connection status
- `ctld_volumes_total` - Number of managed volumes

For the complete metrics reference and Grafana dashboard examples, see [Metrics Reference](docs/metrics.md).

## Building from Source

```bash
# Clone the repository
git clone https://github.com/ndenev/freebsd-csi
cd freebsd-csi

# Build all components
cargo build --release

# Binaries are in target/release/
ls target/release/ctld-agent target/release/csi-driver
```

## License

BSD-3-Clause License. See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.
