# Whole-store backup

Stop the owning service cleanly and release embedded cursors and Arrow buffers
before copying a store. A live copy of only `catalog.sqlite` is not a backup:
the store also needs its identity, Parquet files, Iceberg metadata and permanent
operation outcomes.

Build and install the optional `glamdb-admin` with
`-DGLAMDB_BUILD_ADMIN=ON` as described in [installation](BUILDING.md).
Use the original store UUID and canonical absolute paths:

```sh
python3 tools/acceptance/backup.py \
  --admin /absolute/prefix/bin/glamdb-admin \
  --source /canonical/store --store-id ORIGINAL_STORE_UUID \
  --destination /canonical/new-copy
```

The destination must not exist or overlap the source. The helper opens and
recovers the source, closes SQLite through `quiesce-store`, and retains an
independent store ownership lease until copy and verification finish. Competing
owners are refused during the copy.

It copies all regular store files, including orphans and original successful
and failed outcomes, rejects links/special files, verifies SHA-256, synchronizes
files and directories, and publishes `store.ready` last. A failed partial
destination remains for inspection; it is not advertised as a successful copy.

Retain the source's engine limit configuration. Embedded callers must supply
a sufficient profile on reopen; core store identities do not persist those
limits. The bundled helper invokes `glamdb-admin` with default limits and
therefore requires a store that reopens under that profile. It does not forward
enlarged collection/history limits. Stores requiring an enlarged profile need
a quiescing owner configured with those limits before using the same copy and
verification procedure. Managed service registry state is separate from each database store.
For the organization gateway, ownership catalogs and identity services also
need the [SSO backup procedure](SSO_OPERATIONS.md#11-consistent-backup-and-restore).

Verify the copy by opening it at a different physical root with the same logical
store identity and with the original root unavailable. Compare latest,
historical and initial views, original successful/failed outcomes and exact
retries. [The installed acceptance tool](../tools/acceptance/README.md) exercises
this procedure on a dedicated test store.

This is a quiesced local-filesystem procedure. It does not provide online backup,
replication or physical power-loss qualification.
