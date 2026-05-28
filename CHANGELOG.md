# Changelog

## Unreleased

- Target-side authentication now references operator-managed CTL auth-groups via
  the StorageClass `authGroup` parameter. The controller no longer receives CHAP
  or NVMe auth credentials in `CreateVolume`, and `ctld-agent` no longer stores
  credentials in `auth.json`.

### Upgrade Notes

Define any required `auth-group` entries in the main `ctl.conf`, set the
StorageClass `authGroup` parameter to that group name, and keep matching
initiator credentials in the node-stage Kubernetes Secret.

## 0.4.0

- Require CSI-managed volumes to have a versioned `user:csi:metadata` ZFS user
  property. Metadata without a `schema_version` field is no longer accepted as a
  CSI ownership marker.
- Keep explicit metadata migrations for older versioned schemas. Existing
  `schema_version: 1` metadata is still migrated to the current schema.

### Upgrade Notes

Before upgrading, check the configured ZFS parent dataset for unversioned CSI
metadata:

```sh
zfs list -H -r -t volume -o name,user:csi:metadata tank/csi
```

Every CSI-managed volume must have a non-empty JSON value with a
`schema_version` field. Volumes without `user:csi:metadata`, or with unversioned
metadata JSON, are treated as not CSI-managed by this version and will not be
deleted by `DeleteVolume`.
