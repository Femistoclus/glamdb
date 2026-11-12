# Physical owned-store inventory

Authenticated GET /databases/{database_id}/storage calls Engine::GetStorageFootprint.
Local storage advertises features.storage_footprint; custom backends return
DEPENDENCY_UNAVAILABLE. Legacy single-store mode supports /storage. The organization
gateway applies existing database metadata authorization. SDK call:
client.storage_footprint(store_id=original_store_id).

Version 1 returns the original store UUID, local/mock false, consistency
writer_quiescent, and exact nonnegative decimal-string counters: total file
lengths, allocated blocks, data, Iceberg metadata, catalog, other/shared files,
committed data, referenced metadata, unreferenced warehouse bytes, and cardinalities.
The service registry and client datasets/journals/reports are excluded. Current
physical store size is separate from historical selected-snapshot membership.
Abandoned attempt files are unreferenced bytes, not automatic deletion candidates.

The facade tries the existing writer gate and returns DATABASE_BUSY rather than
waiting behind publication. Recovery/closed stores do not return healthy inventories.
Native traversal verifies the pinned owned-root device/inode before/after, rejects
symlinks/special files/mount crossings, verifies committed lengths and required
closure presence, and deduplicates hardlinks. Limits: 100,000 entries, depth 16,
32 MiB reachable-path accounting, 64 MiB shared metadata reservation.

Internal writers are excluded. External changes are not an atomic snapshot;
external_writes_atomic is false. Run without external writers for independent
filesystem comparison. This bounded inventory is not a scrub, garbage collector,
power-loss qualification or host-wide disk usage endpoint.
