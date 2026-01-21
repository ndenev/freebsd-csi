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

- [Installation Guide](docs/installation.md) - Detailed setup instructions
- [Configuration Reference](docs/configuration.md) - All configuration options
- [Helm Chart README](charts/freebsd-csi/README.md) - Helm chart documentation

## Features

- Dynamic volume provisioning
- Volume expansion (online)
- Snapshots and clones
- iSCSI and NVMeoF export protocols
- mTLS support for secure communication
- Automatic recovery on restart
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
