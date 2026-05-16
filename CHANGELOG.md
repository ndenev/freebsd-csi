# Changelog

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
