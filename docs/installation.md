# Installation Guide

This guide covers the complete installation process for the FreeBSD CSI driver, including FreeBSD storage node setup and Kubernetes cluster deployment.

## Table of Contents

- [FreeBSD Storage Node Setup](#freebsd-storage-node-setup)
  - [Prerequisites](#prerequisites)
  - [ZFS Pool Configuration](#zfs-pool-configuration)
  - [CTL Configuration](#ctl-configuration)
  - [Building ctld-agent](#building-ctld-agent)
  - [Running ctld-agent](#running-ctld-agent)
- [Kubernetes Cluster Setup](#kubernetes-cluster-setup)
  - [Prerequisites](#kubernetes-prerequisites)
  - [Deploying RBAC](#deploying-rbac)
  - [Deploying the CSI Driver](#deploying-the-csi-driver)
  - [Creating StorageClasses](#creating-storageclasses)
  - [Verifying Installation](#verifying-installation)
- [Building from Source](#building-from-source)
  - [Cargo Build Instructions](#cargo-build-instructions)
  - [Docker Image Building](#docker-image-building)

---

## FreeBSD Storage Node Setup

### Prerequisites

Ensure your FreeBSD storage node meets the following requirements:

- **FreeBSD 13.0 or later**
- **ZFS** filesystem support (included by default)
- **Rust 1.75+** toolchain for building from source
- **Network connectivity** to Kubernetes nodes
- **Root access** for ZFS and CTL configuration

Install the Rust toolchain if not present:

```bash
pkg install rust
# or
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
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

4. **For NVMeoF support**, ensure the nvmf kernel module is loaded:

   ```bash
   kldload nvmf
   ```

   Add to `/boot/loader.conf`:

   ```
   nvmf_load="YES"
   ```

### Building ctld-agent

1. **Clone the repository**

   ```bash
   git clone https://github.com/ndenev/freebsd-csi
   cd freebsd-csi
   ```

2. **Build the ctld-agent**

   ```bash
   cargo build --release -p ctld-agent
   ```

3. **Install the binary**

   ```bash
   cp target/release/ctld-agent /usr/local/sbin/
   chmod 755 /usr/local/sbin/ctld-agent
   ```

### Running ctld-agent

1. **Basic usage**

   ```bash
   ctld-agent --zfs-parent tank/csi --listen [::]:50051
   ```

2. **With custom iSCSI/NVMeoF naming**

   ```bash
   ctld-agent \
     --zfs-parent tank/csi \
     --listen [::]:50051 \
     --base-iqn iqn.2024-01.com.example.storage \
     --base-nqn nqn.2024-01.com.example.storage \
     --portal-group 1
   ```

3. **Create a service script** (`/usr/local/etc/rc.d/ctld_agent`):

   ```sh
   #!/bin/sh

   # PROVIDE: ctld_agent
   # REQUIRE: LOGIN zfs
   # KEYWORD: shutdown

   . /etc/rc.subr

   name="ctld_agent"
   rcvar="${name}_enable"
   command="/usr/local/sbin/ctld-agent"
   command_args="--zfs-parent tank/csi --listen [::]:50051"
   pidfile="/var/run/${name}.pid"

   start_cmd="${name}_start"

   ctld_agent_start()
   {
       ${command} ${command_args} &
       echo $! > ${pidfile}
   }

   load_rc_config $name
   run_rc_command "$1"
   ```

4. **Enable and start the service**

   ```bash
   chmod +x /usr/local/etc/rc.d/ctld_agent
   sysrc ctld_agent_enable="YES"
   service ctld_agent start
   ```

---

## Kubernetes Cluster Setup

### Kubernetes Prerequisites

- **Kubernetes 1.28+** cluster
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

### Deploying RBAC

Deploy the required ServiceAccount, ClusterRole, and ClusterRoleBinding:

```bash
kubectl apply -f deploy/kubernetes/rbac.yaml
```

This creates:
- `freebsd-csi-controller` ServiceAccount in `kube-system`
- `freebsd-csi-controller` ClusterRole with permissions for:
  - PersistentVolumes and PersistentVolumeClaims
  - StorageClasses and CSINodes
  - VolumeSnapshots and VolumeSnapshotContents
  - Events and Nodes

### Deploying the CSI Driver

1. **Update the agent endpoint** in `deploy/kubernetes/csi-driver.yaml`:

   Edit the `AGENT_ENDPOINT` environment variable to point to your FreeBSD storage node:

   ```yaml
   env:
     - name: AGENT_ENDPOINT
       value: "your-freebsd-node.example.com:50051"
   ```

2. **Deploy the CSI driver**

   ```bash
   kubectl apply -f deploy/kubernetes/csi-driver.yaml
   ```

   This deploys:
   - CSIDriver object (`csi.freebsd.org`)
   - Controller Deployment (with sidecar containers)
   - Node DaemonSet (for volume mounting)

### Creating StorageClasses

Deploy the predefined StorageClasses:

```bash
kubectl apply -f deploy/kubernetes/storageclass.yaml
```

This creates two StorageClasses:
- `freebsd-zfs-iscsi` - Volumes exported via iSCSI
- `freebsd-zfs-nvmeof` - Volumes exported via NVMeoF

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
   kubectl get pods -n kube-system -l app=freebsd-csi-controller
   ```

3. **Check node DaemonSet**

   ```bash
   kubectl get pods -n kube-system -l app=freebsd-csi-node
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
- Review the [README](../README.md) for usage examples
