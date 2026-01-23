# FreeBSD CSI Driver Architecture

This document describes the architecture of the FreeBSD CSI driver, including component interactions, data flows, and authentication mechanisms.

## Table of Contents

- [System Overview](#system-overview)
- [Components](#components)
- [Data Flow](#data-flow)
- [Authentication Flow](#authentication-flow)
- [State Management](#state-management)
- [High Availability](#high-availability)

---

## System Overview

```
                                  Kubernetes Cluster
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  ┌─────────────────────┐         ┌─────────────────────────────────────────┐│
│  │   Control Plane     │         │         Worker Nodes                   ││
│  │                     │         │                                         ││
│  │ ┌─────────────────┐ │         │ ┌─────────────────┐ ┌─────────────────┐ ││
│  │ │ kube-controller │ │         │ │   CSI Node      │ │   Application   │ ││
│  │ │     manager     │ │         │ │   DaemonSet     │ │      Pod        │ ││
│  │ └────────┬────────┘ │         │ │                 │ │                 │ ││
│  │          │          │         │ │ ┌─────────────┐ │ │  ┌───────────┐  │ ││
│  │ ┌────────▼────────┐ │         │ │ │ node-driver │ │ │  │ Container │  │ ││
│  │ │ CSI Controller  │ │   gRPC  │ │ │  registrar  │ │ │  │           │  │ ││
│  │ │   Deployment    │◄─────────────►│             │ │ │  │  /mnt/vol │  │ ││
│  │ │                 │ │         │ │ └─────────────┘ │ │  └─────┬─────┘  │ ││
│  │ │ ┌─────────────┐ │ │         │ │ ┌─────────────┐ │ │        │        │ ││
│  │ │ │ csi-driver  │ │ │         │ │ │ csi-driver  │ │ │   bind mount    │ ││
│  │ │ │ (controller)│ │ │         │ │ │   (node)    │ │ │        │        │ ││
│  │ │ └──────┬──────┘ │ │         │ │ └──────┬──────┘ │ │        │        │ ││
│  │ │        │        │ │         │ │        │        │ └────────┼────────┘ ││
│  │ └────────┼────────┘ │         │ └────────┼────────┘          │          ││
│  │          │          │         │          │         ┌─────────▼─────────┐││
│  └──────────┼──────────┘         │          │         │  iSCSI / NVMeoF   │││
│             │                    │          │         │     Session       │││
│             │                    │          │         └─────────┬─────────┘││
│             │ gRPC (mTLS)        │          │                   │          ││
│             │                    │          │                   │          ││
└─────────────┼────────────────────┴──────────┼───────────────────┼──────────┘
              │                               │                   │
              │                               │ iSCSI:3260        │
              │                               │ NVMeoF:4420       │
              │                               │                   │
              ▼                               ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FreeBSD Storage Node                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                            ctld-agent                                   ││
│  │                                                                         ││
│  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              ││
│  │  │ gRPC Server  │    │  ZFS Manager │    │  CTL Manager │              ││
│  │  │  :50051      │───►│              │───►│              │              ││
│  │  │              │    │  - zvol ops  │    │  - targets   │              ││
│  │  │  - Auth      │    │  - snapshots │    │  - exports   │              ││
│  │  │  - Rate Limit│    │  - metadata  │    │  - auth      │              ││
│  │  └──────────────┘    └──────┬───────┘    └──────┬───────┘              ││
│  │                             │                   │                       ││
│  └─────────────────────────────┼───────────────────┼───────────────────────┘│
│                                │                   │                        │
│  ┌─────────────────────────────▼───────────────────▼───────────────────────┐│
│  │                          Operating System                               ││
│  │                                                                         ││
│  │  ┌──────────────────────────┐    ┌──────────────────────────┐          ││
│  │  │          ZFS             │    │          CTL             │          ││
│  │  │                          │    │   (CAM Target Layer)     │          ││
│  │  │  pool: tank              │    │                          │          ││
│  │  │    └── csi/              │    │  - iSCSI targets         │          ││
│  │  │         ├── vol1         │◄───│  - NVMeoF controllers    │          ││
│  │  │         ├── vol2         │    │  - LUNs/Namespaces       │          ││
│  │  │         └── vol3         │    │  - Auth groups           │          ││
│  │  │                          │    │                          │          ││
│  │  └──────────────────────────┘    └──────────────────────────┘          ││
│  │                                                                         ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Components

### CSI Controller (Kubernetes)

The controller runs as a Deployment in Kubernetes and handles:

| Responsibility | Description |
|----------------|-------------|
| Volume Lifecycle | Create, delete, expand volumes |
| Snapshot Management | Create, delete, list snapshots |
| Agent Communication | gRPC client to ctld-agent |
| Retry Logic | Exponential backoff for transient failures |
| Metrics | Operation counters and latency histograms |

**Key files:**
- `csi-driver/src/controller.rs` - Controller service implementation
- `csi-driver/src/agent_client.rs` - gRPC client with retry logic

### CSI Node (Kubernetes)

The node component runs as a DaemonSet on each worker:

| Responsibility | Description |
|----------------|-------------|
| Stage/Unstage | iSCSI/NVMeoF login and discovery |
| Publish/Unpublish | Bind mount to pod mount namespace |
| Filesystem Operations | Format and mount block devices |
| Volume Expansion | Online filesystem growth |

**Key files:**
- `csi-driver/src/node.rs` - Node service implementation
- `csi-driver/src/platform/` - FreeBSD-specific operations

### ctld-agent (FreeBSD)

The storage agent runs on FreeBSD and manages:

| Responsibility | Description |
|----------------|-------------|
| ZFS Operations | Create/delete zvols, snapshots, clones |
| CTL Configuration | Manage iSCSI/NVMeoF targets and exports |
| Authentication | Generate per-volume auth groups for CHAP |
| State Recovery | Restore volume metadata from ZFS properties |
| Rate Limiting | Semaphore-based concurrency control |

**Key files:**
- `ctld-agent/src/service/storage.rs` - gRPC service implementation
- `ctld-agent/src/ctl/` - CTL target management
- `ctld-agent/src/zfs/` - ZFS volume management

---

## Data Flow

### Volume Creation Flow

```
┌───────────┐    ┌────────────┐    ┌─────────────┐    ┌────────────┐
│   User    │    │ Kubernetes │    │ CSI Driver  │    │ ctld-agent │
│           │    │            │    │ (Controller)│    │            │
└─────┬─────┘    └──────┬─────┘    └──────┬──────┘    └──────┬─────┘
      │                 │                 │                  │
      │ kubectl apply   │                 │                  │
      │ PVC             │                 │                  │
      │────────────────>│                 │                  │
      │                 │                 │                  │
      │                 │ CreateVolume    │                  │
      │                 │ (with secrets)  │                  │
      │                 │────────────────>│                  │
      │                 │                 │                  │
      │                 │                 │ CreateVolume     │
      │                 │                 │ (gRPC + CHAP)    │
      │                 │                 │─────────────────>│
      │                 │                 │                  │
      │                 │                 │                  │──┐ 1. Create ZFS zvol
      │                 │                 │                  │  │ 2. Set metadata props
      │                 │                 │                  │  │ 3. Create auth-group
      │                 │                 │                  │  │ 4. Add CTL target
      │                 │                 │                  │  │ 5. Export LUN
      │                 │                 │                  │<─┘
      │                 │                 │                  │
      │                 │                 │ Volume info      │
      │                 │                 │ (IQN, LUN ID)    │
      │                 │                 │<─────────────────│
      │                 │                 │                  │
      │                 │ PV Created      │                  │
      │                 │<────────────────│                  │
      │                 │                 │                  │
      │ PVC Bound       │                 │                  │
      │<────────────────│                 │                  │
      │                 │                 │                  │
```

### Volume Mount Flow

```
┌───────────┐    ┌─────────────┐    ┌───────────────┐    ┌────────────┐
│   Pod     │    │   kubelet   │    │  CSI Driver   │    │   Target   │
│           │    │             │    │    (Node)     │    │  (FreeBSD) │
└─────┬─────┘    └──────┬──────┘    └───────┬───────┘    └──────┬─────┘
      │                 │                   │                   │
      │ Schedule Pod    │                   │                   │
      │────────────────>│                   │                   │
      │                 │                   │                   │
      │                 │ NodeStageVolume   │                   │
      │                 │ (target info)     │                   │
      │                 │──────────────────>│                   │
      │                 │                   │                   │
      │                 │                   │ iSCSI Discovery   │
      │                 │                   │──────────────────>│
      │                 │                   │                   │
      │                 │                   │ CHAP Challenge    │
      │                 │                   │<──────────────────│
      │                 │                   │                   │
      │                 │                   │ CHAP Response     │
      │                 │                   │──────────────────>│
      │                 │                   │                   │
      │                 │                   │ iSCSI Login OK    │
      │                 │                   │<──────────────────│
      │                 │                   │                   │
      │                 │                   │──┐ Format if needed
      │                 │                   │  │ Mount to staging
      │                 │                   │<─┘
      │                 │                   │                   │
      │                 │ Stage OK          │                   │
      │                 │<──────────────────│                   │
      │                 │                   │                   │
      │                 │ NodePublishVolume │                   │
      │                 │──────────────────>│                   │
      │                 │                   │                   │
      │                 │                   │──┐ Bind mount to
      │                 │                   │  │ pod directory
      │                 │                   │<─┘
      │                 │                   │                   │
      │                 │ Publish OK        │                   │
      │                 │<──────────────────│                   │
      │                 │                   │                   │
      │ Pod Running     │                   │                   │
      │<────────────────│                   │                   │
```

---

## Authentication Flow

### CHAP Authentication Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Kubernetes                                      │
│                                                                             │
│  ┌────────────────────┐                                                     │
│  │      Secret        │                                                     │
│  │  iscsi-chap-secret │                                                     │
│  │                    │                                                     │
│  │  username: user1   │                                                     │
│  │  password: pass1   │                                                     │
│  └─────────┬──────────┘                                                     │
│            │                                                                │
│            │ CSI Secret Reference                                           │
│            ▼                                                                │
│  ┌────────────────────┐         ┌────────────────────┐                     │
│  │    StorageClass    │         │    CSI Controller  │                     │
│  │                    │         │                    │                     │
│  │ provisioner-secret─┼────────►│  Extract CHAP      │                     │
│  │ node-stage-secret──┼─────┐   │  credentials       │                     │
│  └────────────────────┘     │   │  from secrets      │                     │
│                             │   └─────────┬──────────┘                     │
│                             │             │                                 │
│                             │             │ gRPC + AuthCredentials          │
│                             │             ▼                                 │
└─────────────────────────────┼─────────────────────────────────────────────┘
                              │             │
                              │             │
┌─────────────────────────────┼─────────────▼───────────────────────────────┐
│                         FreeBSD Storage Node                               │
│                              │                                             │
│  ┌───────────────────────────┼───────────────────────────────────────────┐│
│  │                     ctld-agent                                        ││
│  │                           │                                           ││
│  │  ┌────────────────────────▼────────────────────────────────────────┐  ││
│  │  │                   Auth Processing                               │  ││
│  │  │                                                                 │  ││
│  │  │  1. Receive CHAP credentials in CreateVolume request            │  ││
│  │  │  2. Generate unique auth-group name: "ag-<volume-id>"           │  ││
│  │  │  3. Create auth-group in UCL config                             │  ││
│  │  │  4. Associate auth-group with target                            │  ││
│  │  │                                                                 │  ││
│  │  └─────────────────────────────────────────────────────────────────┘  ││
│  │                                                                       ││
│  └───────────────────────────────────────────────────────────────────────┘│
│                                                                           │
│  ┌───────────────────────────────────────────────────────────────────────┐│
│  │                        /etc/ctl.conf                                  ││
│  │                                                                       ││
│  │  auth-group "ag-pvc-12345" {                                          ││
│  │      chap "user1" "pass1"                                             ││
│  │      chap-mutual "target-user" "target-pass"  # if mutual CHAP       ││
│  │  }                                                                    ││
│  │                                                                       ││
│  │  target "iqn.2024-01.org.freebsd.csi:pvc-12345" {                     ││
│  │      auth-group "ag-pvc-12345"                                        ││
│  │      portal-group "pg0"                                               ││
│  │      lun 0 {                                                          ││
│  │          path "/dev/zvol/tank/csi/pvc-12345"                          ││
│  │      }                                                                ││
│  │  }                                                                    ││
│  │                                                                       ││
│  └───────────────────────────────────────────────────────────────────────┘│
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
                              │
                              │ Node reads secret for login
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Kubernetes Worker Node                             │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        CSI Node Plugin                                │  │
│  │                                                                       │  │
│  │  NodeStageVolume:                                                     │  │
│  │    1. Receive CHAP credentials from secret                            │  │
│  │    2. Configure iscsid with CHAP auth                                 │  │
│  │    3. Perform iSCSI discovery                                         │  │
│  │    4. Login with CHAP authentication                                  │  │
│  │    5. Mount block device to staging path                              │  │
│  │                                                                       │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## State Management

### Source of Truth

The FreeBSD CSI driver uses a layered state management approach:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          State Hierarchy                                     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  Layer 1: ZFS User Properties (Primary Source of Truth)                ││
│  │                                                                         ││
│  │  tank/csi/vol1:                                                         ││
│  │    org.freebsd.csi:managed = true                                       ││
│  │    org.freebsd.csi:export_type = iscsi                                  ││
│  │    org.freebsd.csi:target_name = iqn.2024-01.org.freebsd.csi:vol1       ││
│  │    org.freebsd.csi:lun_id = 0                                           ││
│  │                                                                         ││
│  │  Benefits:                                                              ││
│  │    - Survives agent restart                                             ││
│  │    - Atomic with volume creation                                        ││
│  │    - Queryable via zfs command                                          ││
│  │                                                                         ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  Layer 2: CTL Configuration (/etc/ctl.conf)                            ││
│  │                                                                         ││
│  │  - Generated from ZFS metadata on agent startup                         ││
│  │  - Reconciled if targets are missing                                    ││
│  │  - UCL format for ctld daemon                                           ││
│  │                                                                         ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │  Layer 3: In-Memory State (ctld-agent)                                 ││
│  │                                                                         ││
│  │  - Volume metadata cache for fast lookups                               ││
│  │  - Rebuilt from ZFS on startup                                          ││
│  │  - Updated on operations                                                ││
│  │                                                                         ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Recovery Process

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Agent Startup Recovery                                 │
│                                                                             │
│  1. Load ZFS Metadata                                                       │
│     ┌─────────────────────────────────────────────────────────────────────┐│
│     │ zfs get -r org.freebsd.csi:managed tank/csi                         ││
│     │   → Find all CSI-managed volumes                                    ││
│     │   → Read export_type, target_name, lun_id properties                ││
│     └─────────────────────────────────────────────────────────────────────┘│
│                                      │                                      │
│                                      ▼                                      │
│  2. Rebuild In-Memory State                                                 │
│     ┌─────────────────────────────────────────────────────────────────────┐│
│     │ For each volume with org.freebsd.csi:managed=true:                  ││
│     │   → Create VolumeMetadata entry                                     ││
│     │   → Populate from ZFS properties                                    ││
│     └─────────────────────────────────────────────────────────────────────┘│
│                                      │                                      │
│                                      ▼                                      │
│  3. Reconcile CTL Exports                                                   │
│     ┌─────────────────────────────────────────────────────────────────────┐│
│     │ For each volume in memory:                                          ││
│     │   → Check if CTL target exists                                      ││
│     │   → If missing, recreate export from metadata                       ││
│     │   → Log reconciliation actions                                      ││
│     └─────────────────────────────────────────────────────────────────────┘│
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## High Availability

### Controller HA

The CSI controller supports running with multiple replicas:

```yaml
controller:
  replicas: 2
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            topologyKey: kubernetes.io/hostname
```

Leader election is handled by the CSI sidecar containers (csi-provisioner, csi-attacher).

### Storage Node HA

For storage node high availability:

1. **Active-Passive**: Run ctld-agent on multiple FreeBSD nodes with shared storage (e.g., HAST or DRBD)
2. **ZFS Replication**: Use `zfs send/recv` for asynchronous replication
3. **Multipath**: Configure dm-multipath on initiators for path redundancy

### Graceful Shutdown

Both components implement graceful shutdown:

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Graceful Shutdown Flow                            │
│                                                                          │
│  SIGTERM/SIGINT received                                                 │
│         │                                                                │
│         ▼                                                                │
│  ┌──────────────────────┐                                                │
│  │ Stop accepting new   │                                                │
│  │ gRPC requests        │                                                │
│  └──────────┬───────────┘                                                │
│             │                                                            │
│             ▼                                                            │
│  ┌──────────────────────┐                                                │
│  │ Wait for in-flight   │  (up to drain timeout)                         │
│  │ operations to finish │                                                │
│  └──────────┬───────────┘                                                │
│             │                                                            │
│             ▼                                                            │
│  ┌──────────────────────┐                                                │
│  │ Flush pending writes │                                                │
│  │ to CTL config        │                                                │
│  └──────────┬───────────┘                                                │
│             │                                                            │
│             ▼                                                            │
│  ┌──────────────────────┐                                                │
│  │ Exit cleanly         │                                                │
│  └──────────────────────┘                                                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```
