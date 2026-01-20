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

- **FreeBSD 13.0+** on storage node(s)
- **ZFS** pool configured for storage
- **Rust 1.75+** (for building from source)
- **Kubernetes 1.28+** cluster
- Network connectivity between Kubernetes nodes and FreeBSD storage node(s)

## Quick Start

### 1. FreeBSD Storage Node Setup

```bash
# Create ZFS pool and parent dataset
zfs create tank/csi

# Build and run ctld-agent
cargo build --release -p ctld-agent
./target/release/ctld-agent --zfs-parent tank/csi --listen [::]:50051
```

### 2. Kubernetes Deployment

```bash
# Deploy RBAC, CSI driver, and StorageClasses
kubectl apply -f deploy/kubernetes/rbac.yaml
kubectl apply -f deploy/kubernetes/csi-driver.yaml
kubectl apply -f deploy/kubernetes/storageclass.yaml
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

## Features

- Dynamic volume provisioning
- Volume expansion (online)
- Snapshots and clones
- iSCSI and NVMeoF export protocols
- UFS filesystem support
- RBAC-secured Kubernetes integration

## CSI Driver Name

```
csi.freebsd.org
```

## StorageClasses

| Name | Export Protocol | Description |
|------|-----------------|-------------|
| `freebsd-zfs-iscsi` | iSCSI | ZFS volumes exported via iSCSI |
| `freebsd-zfs-nvmeof` | NVMeoF | ZFS volumes exported via NVMe over Fabrics |

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
