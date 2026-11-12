#pragma once

#include <cstdint>

namespace glamdb::detail {
// Conservative non-Arrow working sets for the fixed append profile. Pools
// charge Arrow capacities separately. These are admission units, not RSS.
inline constexpr uint64_t CatalogMemoryBytes = 64ULL << 20;
inline constexpr uint64_t RecoveryMemoryBytes = 768ULL << 20;
inline constexpr uint64_t WriteDataMemoryBytes = 256ULL << 20;
inline constexpr uint64_t MetadataWorkBytes = 768ULL << 20;
inline constexpr uint64_t ReadCacheMemoryBytes = 32ULL << 20;
inline constexpr uint64_t CompletionMemoryBytes = 64ULL << 10;
inline constexpr uint64_t ControlMemoryBytes = 16ULL << 10;
inline constexpr uint64_t AttemptDiskBytes = 256ULL << 20;
inline constexpr uint64_t WorkspaceDiskBytes = 256ULL << 20;

// The original eight-collection envelopes remain unchanged. Each additional
// slot covers bounded catalog rows, schema/collection/view copies and recovery
// descriptors; cached Iceberg roots and returned inventories charge separately.
// Call only after validating the finite collection limit.
inline constexpr uint64_t ExtraCollectionSlots(int64_t limit)
{
    return limit > 8 ? static_cast<uint64_t>(limit - 8) : 0;
}
// Full verification streams receipts and archive rows and keeps one compact
// receipt and ancestor per committed operation (a few hundred bytes; the
// verified generation's index is charged separately). The recovery reserve
// covers the actual operations, in chunks so that appends rarely reserve.
// The fixed RecoveryMemoryBytes envelope covers the first chunk.
inline constexpr uint64_t RecoveryBytesPerOperation = 1ULL << 10;
inline constexpr uint64_t RecoveryOperationChunk = 256;
// The catalog keeps room to double (journal) plus this growth; the recovery
// workspace keeps at least the former fixed reservation. Both grow with the
// actual catalog and retained state before an attempt binds.
inline constexpr uint64_t CatalogGrowthBytes = 64ULL << 20;
// Workspace units one attempt may add: a snapshot, eight manifests and 256
// data files.
inline constexpr uint64_t AttemptWorkspaceBytes = 8192ULL * (1 + 8 + 256);
inline constexpr uint64_t CollectionMetadataBytes = 1ULL << 20;
inline constexpr uint64_t CollectionRecoveryBytes = 1ULL << 20;
inline constexpr uint64_t CollectionControlBytes = 4ULL << 10;

// Root parsing/serialization is outside Arrow. Preserve the default envelope
// and grow it with the admitted root size; reject invalid internal contexts
// through reservation refusal rather than overflowing the multiplication.
inline constexpr uint64_t MetadataWorkForRoot(int64_t bytes)
{
    if (bytes <= 0 || bytes > (64LL << 20)) {
        return UINT64_MAX;
    }
    return bytes <= (16LL << 20) ? MetadataWorkBytes : static_cast<uint64_t>(bytes) * 48;
}
} // namespace glamdb::detail
