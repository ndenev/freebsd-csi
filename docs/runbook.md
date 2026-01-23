# FreeBSD CSI Driver Operations Runbook

This runbook provides troubleshooting procedures for common issues with the FreeBSD CSI driver.

## Table of Contents

- [Quick Diagnostics](#quick-diagnostics)
- [Volume Provisioning Issues](#volume-provisioning-issues)
- [Volume Mount Issues](#volume-mount-issues)
- [iSCSI Connection Issues](#iscsi-connection-issues)
- [NVMeoF Connection Issues](#nvmeof-connection-issues)
- [Authentication Issues](#authentication-issues)
- [Performance Issues](#performance-issues)
- [Recovery Procedures](#recovery-procedures)

---

## Quick Diagnostics

### Check CSI Driver Health

```bash
# Check CSI controller pod status
kubectl -n freebsd-csi get pods -l app.kubernetes.io/component=controller

# Check CSI node pods status
kubectl -n freebsd-csi get pods -l app.kubernetes.io/component=node

# View controller logs
kubectl -n freebsd-csi logs -l app.kubernetes.io/component=controller -c freebsd-csi-driver

# View node logs
kubectl -n freebsd-csi logs -l app.kubernetes.io/component=node -c freebsd-csi-driver
```

### Check ctld-agent Health

```bash
# On FreeBSD storage node
service ctld_agent status

# Check agent logs
tail -f /var/log/ctld-agent.log

# Verify gRPC endpoint is listening
sockstat -l | grep 50051
```

### Check ZFS Status

```bash
# On FreeBSD storage node
zpool status
zfs list -r tank/csi
```

### Check CTL Status

```bash
# List all CTL targets
ctladm portlist
ctladm lunlist

# View CTL configuration
cat /etc/ctl.conf
```

---

## Volume Provisioning Issues

### Symptom: PVC stuck in Pending

**Check provisioner logs:**
```bash
kubectl -n freebsd-csi logs -l app.kubernetes.io/component=controller -c csi-provisioner
```

**Common causes:**

1. **Agent unreachable**
   ```bash
   # Test connectivity from controller pod
   kubectl -n freebsd-csi exec -it deploy/freebsd-csi-controller -- \
     nc -zv <AGENT_IP> 50051
   ```

   **Resolution:** Verify network connectivity, firewall rules, and agent endpoint configuration.

2. **ZFS parent dataset doesn't exist**
   ```bash
   # On FreeBSD node
   zfs list tank/csi
   ```

   **Resolution:** Create the parent dataset:
   ```bash
   zfs create tank/csi
   ```

3. **Insufficient ZFS quota**
   ```bash
   zfs get quota,used tank/csi
   ```

   **Resolution:** Increase quota or free up space.

4. **Invalid StorageClass parameters**
   ```bash
   kubectl describe storageclass freebsd-zfs-iscsi
   ```

   **Resolution:** Verify `exportType` is `iscsi` or `nvmeof`.

### Symptom: Volume created but export fails

**Check CTL configuration:**
```bash
# On FreeBSD node
ctladm portlist
cat /etc/ctl.conf | grep -A10 "target iqn"
```

**Common causes:**

1. **CTL daemon not running**
   ```bash
   service ctld status
   ```

   **Resolution:**
   ```bash
   service ctld start
   ```

2. **Portal group misconfigured**
   ```bash
   grep -A5 "portal-group" /etc/ctl.conf
   ```

   **Resolution:** Verify portal group has correct IP addresses.

---

## Volume Mount Issues

### Symptom: Pod stuck in ContainerCreating

**Check node plugin logs:**
```bash
kubectl -n freebsd-csi logs -l app.kubernetes.io/component=node -c freebsd-csi-driver
```

**Check events:**
```bash
kubectl describe pod <pod-name> | grep -A20 Events
```

### Symptom: NodeStageVolume failed

**Common causes:**

1. **iSCSI initiator not installed**
   ```bash
   # On Kubernetes worker node
   which iscsiadm
   ```

   **Resolution:** Install open-iscsi:
   ```bash
   apt-get install open-iscsi  # Debian/Ubuntu
   yum install iscsi-initiator-utils  # RHEL/CentOS
   ```

2. **Target not discoverable**
   ```bash
   iscsiadm -m discovery -t sendtargets -p <PORTAL_IP>:3260
   ```

   **Resolution:** Check network connectivity and firewall rules for port 3260.

3. **Authentication failure** (if using CHAP)
   ```bash
   # Check CHAP credentials in secret
   kubectl get secret <chap-secret> -o yaml
   ```

   **Resolution:** Verify username/password match target configuration.

### Symptom: NodePublishVolume failed

**Common causes:**

1. **Mount point doesn't exist**
   ```bash
   ls -la /var/lib/kubelet/pods/<pod-uid>/volumes/kubernetes.io~csi/
   ```

2. **Filesystem not created on volume**
   ```bash
   # On worker node
   blkid /dev/sd<X>
   ```

   **Resolution:** The CSI driver should format the volume automatically. Check node logs for errors.

---

## iSCSI Connection Issues

### Symptom: Discovery succeeds but login fails

**Debug steps:**
```bash
# On worker node
iscsiadm -m node -T <target-iqn> -p <portal-ip> --login
```

**Check for:**
1. CHAP authentication mismatch
2. Initiator IQN not in allowed list
3. ACL restrictions on target

**Resolution:**
```bash
# Verify initiator IQN
cat /etc/iscsi/initiatorname.iscsi

# Check target allows this initiator (on FreeBSD)
ctladm portlist -v
```

### Symptom: Sessions disconnect randomly

**Check for:**
1. Network instability
2. iSCSI timeout settings
3. Multipath configuration issues

**Resolution:**
```bash
# Increase timeouts on initiator
iscsiadm -m node -T <target-iqn> -o update \
  -n node.session.timeo.replacement_timeout -v 120
```

---

## NVMeoF Connection Issues

### Symptom: NVMe discovery fails

**Debug steps:**
```bash
# On worker node (Linux)
nvme discover -t tcp -a <STORAGE_IP> -s 4420
```

**Common causes:**

1. **NVMeoF not enabled on FreeBSD**
   ```bash
   # On FreeBSD 15.0+
   kldstat | grep nvmf
   ```

   **Resolution:**
   ```bash
   kldload nvmf
   sysrc kld_list+="nvmf"
   ```

2. **Transport group misconfigured**
   ```bash
   grep -A5 "transport-group" /etc/ctl.conf
   ```

### Symptom: NVMe connect times out

**Check:**
1. Port 4420 accessibility
2. NQN format correctness
3. Host NQN in allowed list

---

## Authentication Issues

### Symptom: CHAP authentication fails

**Verify secret contents:**
```bash
kubectl get secret chap-secret -o jsonpath='{.data}' | \
  jq -r 'to_entries[] | "\(.key): \(.value | @base64d)"'
```

**Verify target-side configuration:**
```bash
# On FreeBSD
grep -A10 "auth-group" /etc/ctl.conf
```

**Common issues:**
1. Username mismatch (case-sensitive)
2. Password too short (minimum 12 characters recommended)
3. Mutual CHAP configured on one side only

### Symptom: Secrets not being passed to agent

**Check controller logs:**
```bash
kubectl -n freebsd-csi logs deploy/freebsd-csi-controller -c freebsd-csi-driver | \
  grep -i "chap\|auth\|secret"
```

**Verify StorageClass secret reference:**
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: freebsd-zfs-iscsi-chap
provisioner: csi.freebsd.org
parameters:
  exportType: iscsi
csi.storage.k8s.io/provisioner-secret-name: chap-secret
csi.storage.k8s.io/provisioner-secret-namespace: default
```

---

## Performance Issues

### Symptom: Slow I/O performance

**Check ZFS ARC:**
```bash
# On FreeBSD
sysctl kstat.zfs.misc.arcstats.size
sysctl vfs.zfs.arc_max
```

**Check CTL I/O stats:**
```bash
ctladm devlist -v
```

**Check network bandwidth:**
```bash
# On FreeBSD
netstat -I <interface> -w 1
```

**Tuning recommendations:**

1. **Increase ZFS ARC** (memory permitting):
   ```bash
   sysctl vfs.zfs.arc_max=8589934592  # 8GB
   ```

2. **Enable compression** on ZFS dataset:
   ```bash
   zfs set compression=lz4 tank/csi
   ```

3. **Tune iSCSI parameters**:
   ```bash
   # In /etc/ctl.conf portal-group
   option "MaxBurstLength" "16776192"
   option "FirstBurstLength" "16776192"
   ```

### Symptom: High latency spikes

**Check for:**
1. ZFS scrub/resilver operations
2. Snapshot operations
3. Network congestion

```bash
# Check for background ZFS operations
zpool status -v tank
```

---

## Recovery Procedures

### Recovering from ctld-agent crash

The ctld-agent automatically recovers state from ZFS metadata on startup:

```bash
# Restart the agent
service ctld_agent restart

# Verify volumes are restored
ctladm portlist
zfs get org.freebsd.csi:managed tank/csi -r
```

### Recovering orphaned volumes

If volumes exist in ZFS but are not exported:

```bash
# On FreeBSD, list CSI-managed volumes
zfs list -o name,org.freebsd.csi:managed -r tank/csi | grep true

# Restart agent to reconcile
service ctld_agent restart
```

### Forcing volume deletion

If a volume is stuck and cannot be deleted normally:

```bash
# 1. Remove from Kubernetes
kubectl delete pv <pv-name> --grace-period=0 --force

# 2. On FreeBSD, manually clean up
# Find the volume
zfs list -r tank/csi

# Remove CTL export
ctladm remove -b block -l <lun-id>

# Destroy ZFS volume
zfs destroy tank/csi/<volume-name>
```

### Recovering from split-brain

If Kubernetes and FreeBSD have inconsistent state:

1. **Stop all workloads using the affected volumes**
2. **Scale down the CSI controller**:
   ```bash
   kubectl -n freebsd-csi scale deploy/freebsd-csi-controller --replicas=0
   ```
3. **On FreeBSD, sync the CTL config**:
   ```bash
   service ctld_agent restart
   ```
4. **Scale up the controller**:
   ```bash
   kubectl -n freebsd-csi scale deploy/freebsd-csi-controller --replicas=1
   ```
5. **Verify volume states match**

---

## Monitoring Alerts

### Recommended alerting thresholds

| Metric | Warning | Critical |
|--------|---------|----------|
| `csi_operations_total{status="error"}` rate | > 1/min | > 10/min |
| `csi_operation_duration_seconds` p99 | > 30s | > 60s |
| ZFS pool capacity | > 80% | > 90% |
| iSCSI session count drop | > 10% | > 50% |

### Sample Prometheus alert rules

```yaml
groups:
  - name: freebsd-csi
    rules:
      - alert: CSIHighErrorRate
        expr: rate(csi_operations_total{status="error"}[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High CSI error rate"

      - alert: CSISlowOperations
        expr: histogram_quantile(0.99, csi_operation_duration_seconds_bucket) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "CSI operations taking too long"
```

---

## Getting Help

1. **Check logs** with increased verbosity:
   ```bash
   # Set log level to debug in Helm values
   driver:
     logLevel: debug
   ```

2. **Collect diagnostics**:
   ```bash
   kubectl -n freebsd-csi logs --all-containers=true -l app.kubernetes.io/name=freebsd-csi > csi-logs.txt
   ```

3. **Open an issue** with:
   - Kubernetes version
   - FreeBSD version
   - CSI driver version
   - Relevant logs
   - Steps to reproduce
