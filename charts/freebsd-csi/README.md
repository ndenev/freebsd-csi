# FreeBSD CSI Driver Helm Chart

Helm chart for deploying the FreeBSD CSI driver to Kubernetes. This driver enables Kubernetes to provision persistent volumes on FreeBSD ZFS storage via iSCSI or NVMe-oF.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- A FreeBSD storage server running [ctld-agent](https://github.com/ndenev/freebsd-csi)
- Network connectivity from Kubernetes nodes to the storage server

## Installation

### From OCI Registry (recommended)

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-SERVER>:50051
```

### From Source

```bash
git clone https://github.com/ndenev/freebsd-csi.git
cd freebsd-csi

helm install freebsd-csi charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://<FREEBSD-STORAGE-SERVER>:50051
```

## Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `agent.endpoint` | **Required.** gRPC endpoint of ctld-agent on FreeBSD storage server | `""` |
| `image.repository` | CSI driver image repository | `ghcr.io/ndenev/freebsd-csi-driver` |
| `image.tag` | CSI driver image tag | `appVersion` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `driver.name` | CSI driver name | `csi.freebsd.org` |
| `driver.logLevel` | Log level (trace, debug, info, warn, error) | `info` |
| `controller.replicas` | Number of controller replicas | `1` |
| `controller.resources` | Controller resource requests/limits | See values.yaml |
| `node.resources` | Node DaemonSet resource requests/limits | See values.yaml |
| `node.tolerations` | Node DaemonSet tolerations | `[{operator: Exists}]` |
| `tls.enabled` | Enable mTLS for ctld-agent connection | `false` |
| `tls.existingSecret` | Use existing secret for TLS certs | `""` |
| `tls.caCert` | CA certificate (base64 encoded) | `""` |
| `tls.clientCert` | Client certificate (base64 encoded) | `""` |
| `tls.clientKey` | Client key (base64 encoded) | `""` |
| `tls.domain` | TLS domain for server verification | `ctld-agent` |
| `storageClassIscsi.create` | Create an iSCSI StorageClass | `false` |
| `storageClassIscsi.name` | iSCSI StorageClass name | `freebsd-zfs-iscsi` |
| `storageClassIscsi.default` | Set as default StorageClass | `false` |
| `storageClassIscsi.parameters.endpoints` | **Required if create=true.** Target endpoints (default port: 3260). Supports multipath: `10.0.0.1:3260,10.0.0.2:3260` | `""` |
| `storageClassIscsi.parameters.blockSize` | Logical block size (512 or 4096) | - |
| `storageClassIscsi.parameters.physicalBlockSize` | Physical block size hint | - |
| `storageClassIscsi.parameters.enableUnmap` | Enable TRIM/discard for SSD-backed storage | - |
| `storageClassIscsi.chapSecret.name` | Existing secret with CHAP credentials | `""` |
| `storageClassIscsi.chapSecret.namespace` | Namespace of CHAP secret | Release namespace |
| `storageClassIscsi.chapSecret.credentials.username` | CHAP username (creates secret if set) | `""` |
| `storageClassIscsi.chapSecret.credentials.password` | CHAP password | `""` |
| `storageClassIscsi.chapSecret.credentials.usernameIn` | Mutual CHAP username (optional) | `""` |
| `storageClassIscsi.chapSecret.credentials.passwordIn` | Mutual CHAP password (optional) | `""` |
| `storageClassNvmeof.create` | Create an NVMeoF StorageClass | `false` |
| `storageClassNvmeof.name` | NVMeoF StorageClass name | `freebsd-zfs-nvmeof` |
| `storageClassNvmeof.default` | Set as default StorageClass | `false` |
| `storageClassNvmeof.parameters.endpoints` | **Required if create=true.** Target endpoints (default port: 4420). Supports multipath: `10.0.0.1:4420,10.0.0.2:4420` | `""` |
| `storageClassNvmeof.parameters.blockSize` | Logical block size (512 or 4096) | - |
| `storageClassNvmeof.parameters.physicalBlockSize` | Physical block size hint | - |
| `storageClassNvmeof.parameters.enableUnmap` | Enable TRIM/discard for SSD-backed storage | - |
| `storageClassNvmeof.authSecret.name` | Existing secret with NVMeoF auth credentials | `""` |
| `storageClassNvmeof.authSecret.namespace` | Namespace of auth secret | Release namespace |
| `storageClassNvmeof.authSecret.credentials.hostNqn` | Host NQN for access control (creates secret if set) | `""` |
| `serviceAccount.create` | Create ServiceAccount | `true` |
| `rbac.create` | Create RBAC resources | `true` |

## Examples

### Basic Installation

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://192.168.1.100:50051
```

### With TLS Enabled

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=https://192.168.1.100:50051 \
  --set tls.enabled=true \
  --set tls.existingSecret=ctld-agent-tls
```

### With iSCSI StorageClass

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://192.168.1.100:50051 \
  --set storageClassIscsi.create=true \
  --set storageClassIscsi.parameters.endpoints=192.168.1.100:3260
```

### With iSCSI CHAP Authentication

Using an existing secret:
```bash
# First, create the CHAP secret
kubectl create secret generic iscsi-chap \
  --namespace freebsd-csi \
  --from-literal=node.session.auth.username=myuser \
  --from-literal=node.session.auth.password=mysecret

# Install with secret reference
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://192.168.1.100:50051 \
  --set storageClassIscsi.create=true \
  --set storageClassIscsi.parameters.endpoints=192.168.1.100:3260 \
  --set storageClassIscsi.chapSecret.name=iscsi-chap
```

Or create the secret inline (for testing/development):
```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://192.168.1.100:50051 \
  --set storageClassIscsi.create=true \
  --set storageClassIscsi.parameters.endpoints=192.168.1.100:3260 \
  --set storageClassIscsi.chapSecret.credentials.username=myuser \
  --set storageClassIscsi.chapSecret.credentials.password=mysecret
```

### With NVMeoF StorageClass (FreeBSD 15.0+)

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://192.168.1.100:50051 \
  --set storageClassNvmeof.create=true \
  --set storageClassNvmeof.parameters.endpoints=192.168.1.100:4420
```

### Using values.yaml

Create a `my-values.yaml`:

```yaml
agent:
  endpoint: "http://192.168.1.100:50051"

storageClassIscsi:
  create: true
  name: freebsd-zfs-iscsi
  default: true
  parameters:
    fsType: ext4
    endpoints: "192.168.1.100:3260"  # Required, default port: 3260
    blockSize: "4096"                # Optional: 4K block size
    enableUnmap: "true"              # Optional: Enable TRIM/discard

# High-availability example with multipath
# storageClassIscsi:
#   create: true
#   parameters:
#     endpoints: "10.0.0.1:3260,10.0.0.2:3260"  # Multiple endpoints for HA
#     blockSize: "4096"

# Optional: Also create NVMeoF StorageClass (FreeBSD 15.0+ required on storage server)
# storageClassNvmeof:
#   create: true
#   parameters:
#     endpoints: "192.168.1.100:4420"  # Required, default port: 4420
#     blockSize: "4096"
#     enableUnmap: "true"

controller:
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 512Mi
```

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  -f my-values.yaml
```

## Creating StorageClasses Manually

If you didn't create StorageClasses during installation, create them manually:

**iSCSI StorageClass:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fsType: ext4
  endpoints: "192.168.1.100:3260"  # Required, default port: 3260
  blockSize: "4096"                # Optional: 4K block size
  enableUnmap: "true"              # Optional: Enable TRIM/discard
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**iSCSI StorageClass with Multipath:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi-ha
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fsType: ext4
  # Multiple endpoints for multipath - dm-multipath combines paths automatically
  endpoints: "10.0.0.1:3260,10.0.0.2:3260"
  blockSize: "4096"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

**NVMeoF StorageClass (FreeBSD 15.0+ on storage server):**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-nvmeof
provisioner: csi.freebsd.org
parameters:
  exportType: nvmeof
  fsType: ext4
  endpoints: "192.168.1.100:4420"  # Required, default port: 4420
  blockSize: "4096"
  enableUnmap: "true"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

## Verifying Installation

```bash
# Check CSI driver is registered
kubectl get csidrivers

# Check pods are running
kubectl get pods -n freebsd-csi

# Check controller logs
kubectl logs -n freebsd-csi -l app.kubernetes.io/component=controller -c freebsd-csi-driver

# Check node logs
kubectl logs -n freebsd-csi -l app.kubernetes.io/component=node -c freebsd-csi-driver
```

## Uninstallation

```bash
helm uninstall freebsd-csi --namespace freebsd-csi
kubectl delete namespace freebsd-csi
```

## Architecture

The chart deploys:

- **Controller Deployment**: Handles volume provisioning, attachment, resizing, and snapshots. Communicates with ctld-agent on the FreeBSD storage server.
- **Node DaemonSet**: Runs on every node to handle volume mounting/unmounting via iSCSI or NVMe-oF.
- **CSI Sidecars**: Standard Kubernetes sig-storage containers (provisioner, attacher, resizer, snapshotter, node-driver-registrar).

## License

Apache 2.0
