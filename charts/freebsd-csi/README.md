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
| `storageClass.create` | Create a StorageClass | `false` |
| `storageClass.name` | StorageClass name | `freebsd-zfs` |
| `storageClass.default` | Set as default StorageClass | `false` |
| `storageClass.parameters` | StorageClass parameters | See values.yaml |
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

### With StorageClass

```bash
helm install freebsd-csi oci://ghcr.io/ndenev/charts/freebsd-csi \
  --namespace freebsd-csi \
  --create-namespace \
  --set agent.endpoint=http://192.168.1.100:50051 \
  --set storageClass.create=true \
  --set storageClass.parameters.portal=192.168.1.100:3260
```

### Using values.yaml

Create a `my-values.yaml`:

```yaml
agent:
  endpoint: "http://192.168.1.100:50051"

storageClass:
  create: true
  name: freebsd-zfs
  default: true
  parameters:
    exportType: iscsi
    fsType: ext4
    portal: "192.168.1.100:3260"

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

## Creating a StorageClass

If you didn't create a StorageClass during installation, create one manually:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  fsType: ext4
  portal: "192.168.1.100:3260"
allowVolumeExpansion: true
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

For NVMe-oF instead of iSCSI:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-nvme
provisioner: csi.freebsd.org
parameters:
  exportType: nvmeof
  fsType: ext4
  transportAddr: "192.168.1.100"
  transportPort: "4420"
allowVolumeExpansion: true
reclaimPolicy: Delete
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
