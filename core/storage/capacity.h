#pragma once

#include "core/io/local_fs.h"

namespace NGlamDB::NIO {
class TStoreDirs;
class TIOAttempt;
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
    std::uint64_t RecoveryWorkspaceBytes = 256ULL << 20;
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
// Measures the warehouse tree, including orphan files.
NIO::TResult<std::uint64_t> MeasureWarehouse(const NIO::TStoreDirs& directories, const TStorageLimits& limits = {});

// Allocated bytes of one collection's data and metadata directories, counted
// as MeasureWarehouse counts directories.
struct TCollectionDirBytes
{
    std::uint64_t Data = 0;
    std::uint64_t Metadata = 0;
};

NIO::TResult<TCollectionDirBytes> MeasureCollectionDirs(const NIO::TStoreDirs& directories,
    std::string_view collectionId);

// A conservative warehouse bound taken before an append attempt produced files,
// with that collection's directory sizes taken after it.
struct TWarehouseBaseline
{
    std::uint64_t Bytes = 0;
    TCollectionDirBytes Directories{};
};

// Warehouse bytes after a sealed attempt without walking the tree: the
// baseline plus the physical bytes of every durable file of the attempt plus
// the growth of the collection's directories. Fails with no_space_on_device
// above limits.WarehouseBytes, like MeasureWarehouse, and with
// state_not_recoverable when an attempt file is not durable.
NIO::TResult<std::uint64_t> EstimateAttemptWarehouse(const NIO::TStoreDirs& directories,
    const TWarehouseBaseline& baseline,
    const NIO::TIOAttempt& attempt,
    const TStorageLimits& limits);

}
