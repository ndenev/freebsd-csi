# CHAP Authentication Setup Guide

This guide explains how to configure CHAP (Challenge-Handshake Authentication Protocol) authentication for iSCSI volumes with the FreeBSD CSI driver.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Basic CHAP Setup](#basic-chap-setup)
- [Mutual CHAP Setup](#mutual-chap-setup)
- [Per-Volume Authentication](#per-volume-authentication)
- [Security Best Practices](#security-best-practices)
- [Troubleshooting](#troubleshooting)
- [NVMeoF Authentication (Not Yet Supported)](#nvmeof-authentication-not-yet-supported)

---

## Overview

CHAP provides authentication between iSCSI initiators (Kubernetes nodes) and targets (FreeBSD storage). The FreeBSD CSI driver supports:

- **One-way CHAP**: Target authenticates the initiator
- **Mutual CHAP**: Both sides authenticate each other (more secure)
- **Per-volume auth groups**: Each volume can have unique credentials

Authentication credentials are passed via Kubernetes Secrets using the standard CSI secrets mechanism.

### Authentication Flow

```
┌──────────────┐                      ┌──────────────┐
│  Kubernetes  │                      │   FreeBSD    │
│    Node      │                      │   Storage    │
│ (Initiator)  │                      │   (Target)   │
└──────┬───────┘                      └──────┬───────┘
       │                                     │
       │  1. iSCSI Login Request             │
       │────────────────────────────────────>│
       │                                     │
       │  2. CHAP Challenge                  │
       │<────────────────────────────────────│
       │                                     │
       │  3. CHAP Response (username+hash)   │
       │────────────────────────────────────>│
       │                                     │
       │  4. Authentication Success/Failure  │
       │<────────────────────────────────────│
       │                                     │
```

---

## Prerequisites

- FreeBSD CSI driver v0.1.14 or later
- Kubernetes 1.25 or later
- iSCSI initiator installed on worker nodes (`open-iscsi` on Linux)

---

## Basic CHAP Setup

### Step 1: Create a Kubernetes Secret

Create a secret containing CHAP credentials using the standard CSI key names:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: iscsi-chap-secret
  namespace: default
type: Opaque
stringData:
  # Initiator credentials (required for one-way CHAP)
  node.session.auth.username: "csi-initiator"
  node.session.auth.password: "MySecretPassword123!"
```

Apply the secret:

```bash
kubectl apply -f chap-secret.yaml
```

### Step 2: Create a StorageClass with Secret Reference

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi-chap
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
  portal: "192.168.1.100:3260"
# Reference the CHAP secret for volume provisioning
csi.storage.k8s.io/provisioner-secret-name: iscsi-chap-secret
csi.storage.k8s.io/provisioner-secret-namespace: default
# Reference the same secret for node operations (staging/publishing)
csi.storage.k8s.io/node-stage-secret-name: iscsi-chap-secret
csi.storage.k8s.io/node-stage-secret-namespace: default
```

Apply the StorageClass:

```bash
kubectl apply -f storageclass-chap.yaml
```

### Step 3: Create a PVC Using the StorageClass

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-secure-volume
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: freebsd-zfs-iscsi-chap
  resources:
    requests:
      storage: 10Gi
```

### Step 4: Verify the Configuration

Check that the volume was created with authentication:

```bash
# On FreeBSD storage node
grep -A10 "auth-group" /etc/ctl.conf
```

You should see an auth-group specific to your volume:

```
auth-group "ag-my-secure-volume" {
    chap "csi-initiator" "MySecretPassword123!"
}
```

---

## Mutual CHAP Setup

Mutual CHAP adds an additional layer of security where the initiator also authenticates the target.

### Create Secret with Mutual CHAP Credentials

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: iscsi-mutual-chap-secret
  namespace: default
type: Opaque
stringData:
  # Initiator credentials (target authenticates initiator)
  node.session.auth.username: "csi-initiator"
  node.session.auth.password: "InitiatorSecret123!"
  # Target credentials (initiator authenticates target)
  node.session.auth.username_in: "csi-target"
  node.session.auth.password_in: "TargetSecret456!"
```

The CSI driver automatically detects when mutual CHAP fields are present and configures the target accordingly.

### Verify Mutual CHAP Configuration

```bash
# On FreeBSD storage node
grep -A10 "auth-group" /etc/ctl.conf
```

You should see both `chap` and `chap-mutual` entries:

```
auth-group "ag-my-secure-volume" {
    chap "csi-initiator" "InitiatorSecret123!"
    chap-mutual "csi-target" "TargetSecret456!"
}
```

---

## Per-Volume Authentication

For enhanced security, you can use different credentials for each volume by creating separate secrets and StorageClasses.

### Example: Separate Secrets per Application

```yaml
# Secret for application A
apiVersion: v1
kind: Secret
metadata:
  name: app-a-chap
  namespace: app-a
type: Opaque
stringData:
  node.session.auth.username: "app-a-user"
  node.session.auth.password: "AppASecretPass123!"
---
# StorageClass for application A
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-app-a
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
csi.storage.k8s.io/provisioner-secret-name: app-a-chap
csi.storage.k8s.io/provisioner-secret-namespace: app-a
csi.storage.k8s.io/node-stage-secret-name: app-a-chap
csi.storage.k8s.io/node-stage-secret-namespace: app-a
```

---

## NVMeoF Authentication (Not Yet Supported)

> **⚠️ Important:** FreeBSD's ctld(8) does not currently support DH-HMAC-CHAP
> authentication for NVMeoF. This section documents the planned feature for
> when support is added.

NVMeoF (NVMe over Fabrics) uses DH-HMAC-CHAP for authentication, which is
different from iSCSI CHAP. As of FreeBSD 15.0, the ctld daemon does not
implement this authentication mechanism for NVMeoF controllers.

### Current Status

- **iSCSI CHAP**: ✅ Fully supported
- **NVMeoF DH-HMAC-CHAP**: ❌ Not supported by FreeBSD ctld
- **NVMeoF NQN Allowlists**: ⚠️ Available but has mobility issues (see below)

### NQN Allowlist Limitations

> **⚠️ Warning: NQN-based security has pod mobility issues**
>
> NVMeoF can restrict access using NQN (NVMe Qualified Name) allowlists,
> where only specific host NQNs can connect. However, this approach has
> significant limitations in Kubernetes environments:
>
> - **Pod mobility problem**: When a pod is rescheduled to a different node,
>   the new node's NQN won't be in the existing allowlist
> - **Connection denied**: The pod will fail to mount the volume on the new node
> - **Manual intervention required**: Allowlists must be updated with every
>   possible node NQN, or dynamically updated on pod migration
>
> **Recommendations:**
>
> 1. **For production NVMeoF**: Use network isolation (VLANs, firewall rules)
>    until DH-HMAC-CHAP is supported by FreeBSD ctld
> 2. **For testing only**: NQN allowlists can be used in static environments
>    where pods don't migrate between nodes
> 3. **Future improvement**: The CSI driver may implement dynamic NQN updates
>    at NodeStageVolume time (not yet available)

### Workarounds

For NVMeoF deployments requiring security:

1. **Network isolation**: Use dedicated VLANs for storage traffic
2. **Firewall rules**: Restrict NVMeoF port (4420) to known initiator IPs
3. **mTLS**: Use the CSI driver's mTLS support for agent communication

### Future Support

When FreeBSD adds DH-HMAC-CHAP support to ctld, the CSI driver has
placeholder code ready to enable it. The secret format will be:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: nvme-auth-secret
type: Opaque
stringData:
  nvme.host.nqn: "nqn.2024-01.org.kubernetes:node01"
  nvme.host.secret: "pre-shared-key"
  nvme.host.hash: "sha256"
  nvme.host.dhgroup: "ffdhe2048"
```

---

## Security Best Practices

### Credential Storage

The CSI driver stores CHAP credentials securely:

- **Credentials are stored in `/etc/ctl.conf`** (FreeBSD's ctld configuration)
  - File is root-owned with 0600 permissions
  - This is ctld's native credential store
  
- **ZFS metadata does NOT contain credentials**
  - Only the auth-group NAME is stored in ZFS user properties
  - This prevents credential exposure via `zfs get user:csi:metadata`
  
- **On ctld-agent restart**: Credentials persist in `/etc/ctl.conf`, and the
  agent references them by auth-group name from ZFS metadata

### 1. Use Strong Passwords

- Minimum 12 characters (16+ recommended)
- Mix of uppercase, lowercase, numbers, and symbols
- Unique password per volume or application

```bash
# Generate a strong password
openssl rand -base64 24
```

### 2. Rotate Credentials Regularly

To rotate CHAP credentials:

1. Create a new secret with updated credentials
2. Update the StorageClass to reference the new secret
3. Delete the old secret

Note: Existing volumes keep their original credentials. New volumes use the updated credentials.

### 3. Use Namespaced Secrets

Keep secrets in the same namespace as the application using them:

```yaml
csi.storage.k8s.io/provisioner-secret-namespace: ${pvc.namespace}
```

### 4. Enable RBAC for Secrets

Restrict access to CHAP secrets:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: chap-secret-reader
  namespace: default
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["iscsi-chap-secret"]
    verbs: ["get"]
```

### 5. Use Mutual CHAP in Production

Mutual CHAP prevents rogue targets from impersonating legitimate storage:

```yaml
# Always include both sets of credentials in production
node.session.auth.username: "initiator-user"
node.session.auth.password: "initiator-pass"
node.session.auth.username_in: "target-user"
node.session.auth.password_in: "target-pass"
```

### 6. Enable TLS for CSI Communication

Use mTLS between the CSI driver and ctld-agent:

```yaml
# In Helm values
tls:
  enabled: true
  certSecretName: csi-tls-secret
```

---

## Troubleshooting

### Authentication Failure During Volume Mount

**Symptoms:**
- Pod stuck in `ContainerCreating`
- Events show "NodeStageVolume failed"

**Debug steps:**

```bash
# Check node logs
kubectl -n freebsd-csi logs -l app.kubernetes.io/component=node -c freebsd-csi-driver

# On worker node, test iSCSI login
iscsiadm -m node -T <iqn> -p <portal> --login
```

**Common causes:**

1. **Username mismatch** - CHAP usernames are case-sensitive
2. **Password mismatch** - Check for trailing whitespace
3. **Secret not found** - Verify secret name and namespace

### Verify Secret Contents

```bash
# Decode and display secret (be careful with this in production)
kubectl get secret iscsi-chap-secret -o jsonpath='{.data}' | \
  jq -r 'to_entries[] | "\(.key): \(.value | @base64d)"'
```

### Check Target-Side Configuration

```bash
# On FreeBSD storage node
cat /etc/ctl.conf | grep -A5 "auth-group"

# Verify the target references the auth-group
cat /etc/ctl.conf | grep -B2 -A10 "target iqn"
```

### Secrets Not Being Passed

If authentication isn't working, verify the StorageClass has the correct secret references:

```bash
kubectl describe storageclass freebsd-zfs-iscsi-chap | grep -i secret
```

Expected output:
```
csi.storage.k8s.io/provisioner-secret-name: iscsi-chap-secret
csi.storage.k8s.io/provisioner-secret-namespace: default
csi.storage.k8s.io/node-stage-secret-name: iscsi-chap-secret
csi.storage.k8s.io/node-stage-secret-namespace: default
```

### Debug Logging

Enable debug logging to see authentication flow:

```yaml
# In Helm values.yaml
driver:
  logLevel: debug
```

Then check logs:
```bash
kubectl -n freebsd-csi logs deploy/freebsd-csi-controller -c freebsd-csi-driver | \
  grep -i "auth\|chap\|secret"
```

---

## Reference: CSI Secret Keys

### iSCSI CHAP (Supported)

| Key | Required | Description |
|-----|----------|-------------|
| `node.session.auth.username` | Yes | CHAP username for initiator |
| `node.session.auth.password` | Yes | CHAP password for initiator |
| `node.session.auth.username_in` | No | Mutual CHAP username for target |
| `node.session.auth.password_in` | No | Mutual CHAP password for target |

### NVMeoF DH-HMAC-CHAP (Not Yet Supported)

These keys are reserved for future use when FreeBSD ctld adds DH-HMAC-CHAP support:

| Key | Description |
|-----|-------------|
| `nvme.host.nqn` | Host NQN for NVMeoF auth |
| `nvme.host.secret` | Pre-shared key for DH-HMAC-CHAP |
| `nvme.host.hash` | Hash function (sha256/sha384/sha512) |
| `nvme.host.dhgroup` | DH group for key exchange |
