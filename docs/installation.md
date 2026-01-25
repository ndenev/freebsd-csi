# Installation Guide

This guide covers the complete installation process for the FreeBSD CSI driver, including FreeBSD storage node setup and Kubernetes cluster deployment.

## Table of Contents

- [FreeBSD Storage Node Setup](#freebsd-storage-node-setup)
  - [Prerequisites](#prerequisites)
  - [Installing ctld-agent](#installing-ctld-agent)
  - [ZFS Pool Configuration](#zfs-pool-configuration)
  - [CTL Configuration](#ctl-configuration)
  - [Running ctld-agent](#running-ctld-agent)
- [Kubernetes Cluster Setup](#kubernetes-cluster-setup)
  - [Prerequisites](#kubernetes-prerequisites)
  - [Installing with Helm](#installing-with-helm)
  - [Creating StorageClasses](#creating-storageclasses)
  - [Verifying Installation](#verifying-installation)
- [Building from Source](#building-from-source)
  - [Cargo Build Instructions](#cargo-build-instructions)
  - [Docker Image Building](#docker-image-building)

---

## FreeBSD Storage Node Setup

### Prerequisites

Ensure your FreeBSD storage node meets the following requirements:

- **FreeBSD 13.0 or later** (FreeBSD 15.0+ for NVMeoF support)
- **ZFS** filesystem support (included by default)
- **Network connectivity** to Kubernetes nodes
- **Root access** for ZFS and CTL configuration

### Installing ctld-agent

**Option 1: Install from pkg (recommended)**

```bash
pkg install ctld-agent
```

**Option 2: Build from source**

```bash
# Install Rust toolchain
pkg install rust

# Clone and build
git clone https://github.com/ndenev/freebsd-csi
cd freebsd-csi
cargo build --release -p ctld-agent

# Install the binary
cp target/release/ctld-agent /usr/local/sbin/
chmod 755 /usr/local/sbin/ctld-agent
```

### ZFS Pool Configuration

1. **Create or identify your ZFS pool**

   If you need to create a new pool:

   ```bash
   # Single disk (not recommended for production)
   zpool create tank /dev/da0

   # Mirror (recommended for production)
   zpool create tank mirror /dev/da0 /dev/da1

   # RAIDZ (recommended for larger deployments)
   zpool create tank raidz /dev/da0 /dev/da1 /dev/da2
   ```

2. **Create a parent dataset for CSI volumes**

   ```bash
   zfs create tank/csi
   ```

3. **Optional: Configure dataset properties**

   ```bash
   # Enable compression
   zfs set compression=lz4 tank/csi

   # Set a reservation to guarantee space
   zfs set reservation=100G tank/csi

   # Set a quota to limit space usage
   zfs set quota=500G tank/csi
   ```

4. **Verify the configuration**

   ```bash
   zfs list tank/csi
   zpool status tank
   ```

### CTL Configuration

The ctld-agent manages CTL (CAM Target Layer) configuration automatically. However, you should ensure CTL is loaded and configured:

1. **Load the CTL kernel module**

   ```bash
   kldload ctl
   ```

2. **Enable CTL at boot** (add to `/boot/loader.conf`):

   ```
   ctl_load="YES"
   ```

3. **For iSCSI support**, enable the ctld service in `/etc/rc.conf`:

   ```bash
   sysrc ctld_enable="YES"
   ```

4. **Create base CTL configuration** (`/etc/ctl.ucl`):

   ```text
   auth-group ag0 {
       auth-type = none
   }

   portal-group pg0 {
       discovery-auth-group = no-authentication
       listen = 0.0.0.0:3260
   }

   # CSI targets will be added below by ctld-agent
   ```

5. **For NVMeoF support** (FreeBSD 15.0+), add transport group to `/etc/ctl.ucl`:

   ```text
   transport-group tg0 {
       transport-type = tcp
       listen = 0.0.0.0:4420
   }
   ```

   And load the nvmf kernel module:

   ```bash
   kldload nvmf
   ```

   Add to `/boot/loader.conf`:

   ```
   nvmf_load="YES"
   ```

### Running ctld-agent

1. **Configure the service** in `/etc/rc.conf`:

   ```bash
   sysrc ctld_agent_enable="YES"
   sysrc ctld_agent_flags="--zfs-parent tank/csi --listen [::]:50051"
   ```

2. **Start the service**

   ```bash
   service ctld_agent start
   ```

3. **Verify it's running**

   ```bash
   service ctld_agent status
   sockstat -4l | grep 50051
   ```

4. **Optional: Custom configuration**

   For custom IQN/NQN naming:

   ```bash
   sysrc ctld_agent_flags="--zfs-parent tank/csi --listen [::]:50051 --base-iqn iqn.2024-01.com.example.storage --base-nqn nqn.2024-01.com.example.storage"
   ```

---

## Kubernetes Cluster Setup

### Kubernetes Prerequisites

- **Kubernetes 1.25+** cluster
- **Helm 3.x** installed
- **kubectl** configured with cluster admin access
- **Network connectivity** from Kubernetes nodes to FreeBSD storage node(s)
- **iSCSI initiator** or **NVMeoF initiator** on worker nodes

For Linux worker nodes with iSCSI:
```bash
# Debian/Ubuntu
apt-get install open-iscsi
systemctl enable iscsid
systemctl start iscsid

# RHEL/CentOS
yum install iscsi-initiator-utils
systemctl enable iscsid
systemctl start iscsid
```

### Installing with Helm

**Option 1: Install from OCI Registry (recommended)**

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-IP>:50051
```

**Option 2: Install from source**

```bash
git clone https://github.com/ndenev/freebsd-csi
cd freebsd-csi

helm install freebsd-csi charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-IP>:50051
```

**With TLS enabled:**

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=https://<FREEBSD-STORAGE-IP>:50051 \
  --set tls.enabled=true \
  --set tls.existingSecret=ctld-agent-tls
```

### Creating StorageClasses

**Option 1: Create during Helm installation**

```bash
# iSCSI StorageClass
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-IP>:50051 \
  --set storageClassIscsi.create=true \
  --set storageClassIscsi.parameters.endpoints=<FREEBSD-STORAGE-IP>:3260

# NVMeoF StorageClass (FreeBSD 15.0+)
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-IP>:50051 \
  --set storageClassNvmeof.create=true \
  --set storageClassNvmeof.parameters.endpoints=<FREEBSD-STORAGE-IP>:4420
```

**Option 2: Create manually after installation**

iSCSI StorageClass:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fs_type: ext4
  endpoints: "<FREEBSD-STORAGE-IP>:3260"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

NVMeoF StorageClass:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-nvmeof
provisioner: csi.freebsd.org
parameters:
  exportType: nvmeof
  fs_type: ext4
  endpoints: "<FREEBSD-STORAGE-IP>:4420"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

### Verifying Installation

1. **Check CSI driver registration**

   ```bash
   kubectl get csidrivers
   ```

   Expected output:
   ```
   NAME              ATTACHREQUIRED   PODINFOONMOUNT   ...
   csi.freebsd.org   true             true             ...
   ```

2. **Check controller deployment**

   ```bash
   kubectl get pods -n freebsd-csi -l app.kubernetes.io/component=controller
   ```

3. **Check node DaemonSet**

   ```bash
   kubectl get pods -n freebsd-csi -l app.kubernetes.io/component=node
   ```

4. **Check StorageClasses**

   ```bash
   kubectl get storageclasses
   ```

5. **Test volume provisioning**

   ```bash
   cat <<EOF | kubectl apply -f -
   apiVersion: v1
   kind: PersistentVolumeClaim
   metadata:
     name: test-pvc
   spec:
     accessModes:
       - ReadWriteOnce
     storageClassName: freebsd-zfs-iscsi
     resources:
       requests:
         storage: 1Gi
   EOF
   ```

   Check the PVC status:
   ```bash
   kubectl get pvc test-pvc
   ```

   Clean up:
   ```bash
   kubectl delete pvc test-pvc
   ```

---

## Building from Source

### Cargo Build Instructions

1. **Prerequisites**

   - Rust 1.75 or later
   - Protocol Buffers compiler (`protoc`)

   On FreeBSD:
   ```bash
   pkg install rust protobuf
   ```

   On Linux:
   ```bash
   # Debian/Ubuntu
   apt-get install protobuf-compiler

   # Install Rust
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Clone and build**

   ```bash
   git clone https://github.com/ndenev/freebsd-csi
   cd freebsd-csi

   # Build all components
   cargo build --release

   # Build individual components
   cargo build --release -p ctld-agent
   cargo build --release -p csi-driver
   ```

3. **Run tests**

   ```bash
   cargo test
   ```

### Docker Image Building

Build container images for Kubernetes deployment:

```bash
# Build csi-driver image
docker build -t ghcr.io/ndenev/freebsd-csi-driver:latest \
  -f Dockerfile.csi-driver .

# Push to registry
docker push ghcr.io/ndenev/freebsd-csi-driver:latest
```

Example `Dockerfile.csi-driver`:

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release -p csi-driver

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y open-iscsi && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/csi-driver /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/csi-driver"]
```

---

## Next Steps

- [Configuration Reference](configuration.md) - Detailed configuration options
- [Helm Chart README](../charts/freebsd-csi/README.md) - Helm chart documentation
- Review the [README](../README.md) for usage examples
