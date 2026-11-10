#pragma once

#include "core/io/local_fs.h"

namespace NGlamDB::NIO {
class TStoreDirs;
}

namespace NGlamDB::NStorage {

struct TStorageUsage
{
    std::uint64_t RootBytes = 0;
    std::uint64_t Snapshots = 0;
    std::uint64_t SnapshotLogEntries = 0;
    std::uint64_t DataFiles = 0;
    std::uint64_t Manifests = 0;
    std::uint64_t ManifestBytes = 0;
    std::uint64_t ManifestListBytes = 0;
    std::uint64_t FooterBytes = 0;

    bool operator==(const TStorageUsage&) const = default;
};

struct TStorageLimits
{
    TStorageUsage Store{64ULL << 20, 4096, 4096, 16384, 4096, 256ULL << 20, 512ULL << 20, 256ULL << 20};
    std::uint64_t RootBytes = 16ULL << 20;
    std::uint64_t ManifestBytes = 8ULL << 20;
    std::uint64_t ManifestListBytes = 16ULL << 20;
    std::uint64_t MetadataLogEntries = 4096;
    std::uint64_t NewManifests = 8;
    std::uint64_t CommitBytes = 32ULL << 20;
    std::uint64_t WarehouseBytes = 1ULL << 40;
    std::uint64_t MinimumFreeBytes = 1ULL << 30;
    std::uint64_t AttemptReserveBytes = 256ULL << 20;
    std::uint64_t JournalReserveBytes = 256ULL << 20;
};

struct TUsageChange
{
    TStorageUsage Previous{};
    TStorageUsage Next{};
};

std::error_code CheckStorageCapacity(const TStorageUsage& usage, const TStorageLimits& limits);
NIO::TResult<TStorageUsage> ReplaceUsage(const TStorageUsage& total, const TUsageChange& change);
std::error_code
CheckDiskReserve(const NIO::TSpaceInfo& space, std::uint64_t warehouseBytes, const TStorageLimits& limits);
std::error_code CheckWriteCapacity(const NIO::TStoreDirs& directories, const TStorageLimits& limits = {});

}
