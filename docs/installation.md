# Installation Guide

This guide covers the complete installation process for the FreeBSD CSI driver, including FreeBSD storage node setup and Kubernetes cluster deployment.

## Table of Contents

- [FreeBSD Storage Node Setup](#freebsd-storage-node-setup)
  - [Prerequisites](#prerequisites)
  - [UCL Mode Requirement](#ucl-mode-requirement)
  - [CTL Configuration](#ctl-configuration)
  - [Include Directive Setup](#include-directive-setup)
  - [Directory and File Setup](#directory-and-file-setup)
  - [ZFS Pool Configuration](#zfs-pool-configuration)
  - [Installing ctld-agent](#installing-ctld-agent)
  - [Running ctld-agent](#running-ctld-agent)
- [Kubernetes Cluster Setup](#kubernetes-cluster-setup)
  - [Prerequisites](#kubernetes-prerequisites)
  - [Installing with Helm](#installing-with-helm)
  - [Creating StorageClasses](#creating-storageclasses)
  - [Verifying Installation](#verifying-installation)
- [Building from Source](#building-from-source)
  - [Cargo Build Instructions](#cargo-build-instructions)
  - [Docker Image Building](#docker-image-building)
- [Migration from Older Versions](#migration-from-older-versions)

---

## FreeBSD Storage Node Setup

### Prerequisites

Ensure your FreeBSD storage node meets the following requirements:

- **FreeBSD 13.0 or later** (FreeBSD 15.0+ for NVMeoF support)
- **ZFS** filesystem support (included by default)
- **Network connectivity** to Kubernetes nodes
- **Root access** for ZFS and CTL configuration

### UCL Mode Requirement

**IMPORTANT:** FreeBSD's ctld must run in UCL mode (using the `-u` flag) for the CSI driver to function correctly. UCL mode enables the `.include` directive which is essential for managing CSI targets in a separate configuration file.

1. **Configure ctld to run in UCL mode** by adding to `/etc/rc.conf`:

   ```bash
   sysrc ctld_flags="-u"
   ```

   Or manually edit `/etc/rc.conf`:
   ```
   ctld_flags="-u"
   ```

2. **Verify your configuration is in UCL format**

   If you have an existing `/etc/ctl.conf` in the old (non-UCL) format, you must convert it to UCL format. The key differences are:

   - UCL uses `=` for assignments and `{ }` blocks
   - Old format uses bare words without `=`

   See `ctl.conf(5)` for UCL format examples.

### CTL Configuration

The ctld-agent requires that portal groups (for iSCSI) and/or transport groups (for NVMeoF) are pre-configured by the user in `/etc/ctl.conf`. The agent validates these exist on startup.

1. **Load the CTL kernel module**

   ```bash
   kldload ctl
   ```

2. **Enable CTL at boot** (add to `/boot/loader.conf`):

   ```
   ctl_load="YES"
   ```

3. **Enable ctld service** in `/etc/rc.conf`:

   ```bash
   sysrc ctld_enable="YES"
   ```

4. **Create the CTL configuration** (`/etc/ctl.conf`) in UCL format:

   ```ucl
   # /etc/ctl.conf (UCL format)

   # Portal group for iSCSI (required for iSCSI volumes)
   portal-group pg0 {
       discovery-auth-group = no-authentication
       listen = "0.0.0.0:3260"
   }

   # Transport group for NVMeoF (required for NVMeoF volumes)
   # NOTE: NVMeoF requires FreeBSD 15.0+
   transport-group tg0 {
       listen {
           tcp = "0.0.0.0:4420"
       }
   }

   # CSI-managed targets (DO NOT EDIT - managed by ctld-agent)
   .include "/var/db/ctld-agent/csi-targets.conf"
   ```

   **Notes:**
   - The `portal-group` definition is required for iSCSI volumes
   - The `transport-group` definition is required for NVMeoF volumes (FreeBSD 15.0+)
   - The `.include` directive loads CSI-managed targets from a separate file
   - You can have your own manually-managed targets in this file as well

5. **For NVMeoF support** (FreeBSD 15.0+), load the nvmf kernel module:

   ```bash
   kldload nvmf
   ```

   Add to `/boot/loader.conf`:
   ```
   nvmf_load="YES"
   ```

### Include Directive Setup

The CSI driver manages targets in a separate file (`/var/db/ctld-agent/csi-targets.conf`) that is included into the main configuration via the `.include` directive. This approach provides:

- **Clean separation** between user-managed and CSI-managed targets
- **Atomic updates** - the CSI config file is regenerated completely each time
- **No corruption risk** - no marker parsing required
- **Easy troubleshooting** - CSI targets are isolated in their own file

Add this line to your `/etc/ctl.conf`:

```ucl
.include "/var/db/ctld-agent/csi-targets.conf"
```

**IMPORTANT:** The `.include` directive only works when ctld is running in UCL mode (`-u` flag). See [UCL Mode Requirement](#ucl-mode-requirement).

### Directory and File Setup

The ctld-agent stores its data in `/var/db/ctld-agent/`. Create this directory with appropriate permissions:

```bash
# Create the directory
mkdir -p /var/db/ctld-agent

# Set ownership and permissions
chown root:wheel /var/db/ctld-agent
chmod 0755 /var/db/ctld-agent

# Create an empty csi-targets.conf so ctld can start
touch /var/db/ctld-agent/csi-targets.conf
chmod 0644 /var/db/ctld-agent/csi-targets.conf
```

The ctld-agent will create and manage the following files in this directory:

| File | Permissions | Description |
|------|-------------|-------------|
| `csi-targets.conf` | 0644 | Generated UCL config with all CSI-managed targets |
| `auth.json` | 0600 | CHAP authentication credentials (if CHAP is enabled) |

**Security Note:** The `auth.json` file contains sensitive CHAP credentials and is readable only by root.

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

### Running ctld-agent

1. **Configure the service** in `/etc/rc.conf`:

   ```bash
   sysrc ctld_agent_enable="YES"
   sysrc ctld_agent_flags="--zfs-parent tank/csi --listen [::]:50051"
   ```

2. **Start ctld first** (if not already running):

   ```bash
   service ctld start
   ```

3. **Start the ctld-agent service**

   ```bash
   service ctld_agent start
   ```

4. **Verify it's running**

   ```bash
   service ctld_agent status
   sockstat -4l | grep 50051
   ```

5. **Optional: Custom configuration**

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

## Migration from Older Versions

If you are upgrading from an older version of the CSI driver that used marker-based configuration (`# BEGIN CSI-MANAGED TARGETS` / `# END CSI-MANAGED TARGETS`), follow these migration steps:

### Step 1: Enable UCL Mode

Ensure ctld runs in UCL mode:

```bash
sysrc ctld_flags="-u"
```

### Step 2: Create the Data Directory

```bash
mkdir -p /var/db/ctld-agent
chown root:wheel /var/db/ctld-agent
chmod 0755 /var/db/ctld-agent
touch /var/db/ctld-agent/csi-targets.conf
chmod 0644 /var/db/ctld-agent/csi-targets.conf
```

### Step 3: Update /etc/ctl.conf

1. **Remove the old marker-based section** from `/etc/ctl.conf`:

   Delete everything between (and including) these lines:
   ```
   # BEGIN CSI-MANAGED TARGETS - DO NOT EDIT
   ...
   # END CSI-MANAGED TARGETS
   ```

2. **Add the include directive** at the end of `/etc/ctl.conf`:

   ```ucl
   .include "/var/db/ctld-agent/csi-targets.conf"
   ```

3. **Ensure portal-group and/or transport-group are defined** (see [CTL Configuration](#ctl-configuration))

### Step 4: Restart Services

```bash
# Stop ctld-agent first
service ctld_agent stop

# Restart ctld to pick up config changes
service ctld restart

# Start the updated ctld-agent
service ctld_agent start
```

### Step 5: Verify Migration

Check that the agent starts without errors:

```bash
service ctld_agent status
```

Review the agent logs for any warnings about missing auth credentials (existing CHAP credentials from the old configuration are not automatically migrated).

**Note on CHAP Credentials:** If you had volumes with CHAP authentication enabled, the credentials were stored in the old UCL config and are not automatically migrated to the new `auth.json` format. You may need to recreate PVCs with CHAP or manually populate `auth.json`.

---

## Next Steps

- [Configuration Reference](configuration.md) - Detailed configuration options
- [CHAP Authentication Setup](chap-setup.md) - Configure iSCSI CHAP authentication
- [Helm Chart README](../charts/freebsd-csi/README.md) - Helm chart documentation
- Review the [README](../README.md) for usage examples
