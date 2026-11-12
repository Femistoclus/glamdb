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
