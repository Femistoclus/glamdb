#include "core/storage/capacity.h"
#include "core/io/io_attempt.h"
#include "core/io/store_dirs.h"
#include "core/io/store_uri.h"

#include <algorithm>
#include <array>
#include <limits>

namespace NGlamDB::NStorage {
namespace {

constexpr std::array UsageFields{&TStorageUsage::RootBytes,
    &TStorageUsage::Snapshots,
    &TStorageUsage::SnapshotLogEntries,
    &TStorageUsage::DataFiles,
    &TStorageUsage::Manifests,
    &TStorageUsage::ManifestBytes,
    &TStorageUsage::ManifestListBytes,
    &TStorageUsage::FooterBytes};

}

std::error_code CheckStorageCapacity(const TStorageUsage& usage, const TStorageLimits& limits)
{
    for (const auto field : UsageFields) {
        if (usage.*field > limits.Store.*field) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
    }
    // WITHOUT ROWID visited keys are at most 2 KiB. Budget two 4 KiB
    // pages per unique key, including B-tree split slack, plus the header.
    // This conservative envelope is checked before publication as well as
    // reopen; a state cannot outgrow its configured recovery workspace.
    auto remaining = limits.RecoveryWorkspaceBytes / 8192;
    if (remaining == 0) {
        return std::make_error_code(std::errc::no_space_on_device);
    }
    --remaining;
    for (const auto count : {usage.Snapshots, usage.Manifests, usage.DataFiles}) {
        if (count > remaining) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        remaining -= count;
    }
    return {};
}

NIO::TResult<TStorageUsage> ReplaceUsage(const TStorageUsage& total, const TUsageChange& change)
{
    TStorageUsage result;
    for (const auto field : UsageFields) {
        if (change.Previous.*field > total.*field) {
            return std::errc::invalid_argument;
        }
        const auto remaining = total.*field - change.Previous.*field;
        if (change.Next.*field > std::numeric_limits<std::uint64_t>::max() - remaining) {
            return std::errc::value_too_large;
        }
        result.*field = remaining + change.Next.*field;
    }
    return result;
}

std::error_code
CheckDiskReserve(const NIO::TSpaceInfo& space, std::uint64_t warehouseBytes, const TStorageLimits& limits)
{
    if (space.Available > space.Capacity) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    if (warehouseBytes > limits.WarehouseBytes || limits.AttemptReserveBytes > limits.WarehouseBytes - warehouseBytes) {
        return std::make_error_code(std::errc::no_space_on_device);
    }
    const auto fivePercent = space.Capacity / 20 + (space.Capacity % 20 != 0 ? 1 : 0);
    auto remaining = space.Available;
    for (const auto reserve :
        {std::max(fivePercent, limits.MinimumFreeBytes), limits.AttemptReserveBytes, limits.JournalReserveBytes})
    {
        if (reserve > remaining) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        remaining -= reserve;
    }
    return {};
}

std::error_code CheckWriteCapacity(const NIO::TStoreDirs& directories, const TStorageLimits& limits)
{
    if (const auto error = directories.CheckIdentity()) {
        return error;
    }
    const auto bytes = MeasureWarehouse(directories, limits);
    if (!bytes) {
        return bytes.Error();
    }
    const auto space = directories.Fs()->StatFs(directories.Root());
    return space ? CheckDiskReserve(space.Value(), bytes.Value(), limits) : space.Error();
}

NIO::TResult<std::uint64_t> MeasureWarehouse(const NIO::TStoreDirs& directories, const TStorageLimits& limits)
{
    return directories.Fs()->MeasureTree(directories.Warehouse(), limits.WarehouseBytes);
}

NIO::TResult<TCollectionDirBytes> MeasureCollectionDirs(const NIO::TStoreDirs& directories,
    std::string_view collectionId)
{
    const auto opened = directories.OpenCollection(collectionId);
    if (!opened) {
        return opened.Error();
    }
    TCollectionDirBytes result;
    for (const auto& [handle, bytes] :
        {std::pair{opened.Value().Data, &result.Data}, std::pair{opened.Value().Metadata, &result.Metadata}})
    {
        const auto info = directories.Fs()->Stat(handle);
        if (!info) {
            return info.Error();
        }
        if (info.Value().Type != NIO::EFileType::Directory) {
            return std::errc::not_a_directory;
        }
        *bytes = info.Value().Allocated;
    }
    return result;
}

NIO::TResult<std::uint64_t> EstimateAttemptWarehouse(const NIO::TStoreDirs& directories,
    const TWarehouseBaseline& baseline,
    const NIO::TIOAttempt& attempt,
    const TStorageLimits& limits)
{
    auto total = baseline.Bytes;
    const auto add = [&](std::uint64_t bytes) -> std::error_code {
        if (total > limits.WarehouseBytes || bytes > limits.WarehouseBytes - total) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        total += bytes;
        return {};
    };
    for (const auto& record : attempt.Records()) {
        const auto uri = NIO::TStoreUri::Parse(record.Path, attempt.StoreId());
        if (record.State != NIO::EFileState::DirSynced || !record.Receipt || record.FirstError || !uri ||
            uri.Value().CollectionId() != attempt.CollectionId() || record.PhysicalBytes < record.Length)
        {
            return std::errc::state_not_recoverable;
        }
        if (const auto error = add(record.PhysicalBytes)) {
            return error;
        }
    }
    // Directories only grow while entries are added; a shrunken directory
    // contributes nothing, which keeps the baseline bytes as an upper bound.
    const auto current = MeasureCollectionDirs(directories, attempt.CollectionId());
    if (!current) {
        return current.Error();
    }
    for (const auto& [before, after] : {std::pair{baseline.Directories.Data, current.Value().Data},
             std::pair{baseline.Directories.Metadata, current.Value().Metadata}})
    {
        if (const auto error = add(after > before ? after - before : 0)) {
            return error;
        }
    }
    if (total > limits.WarehouseBytes) {
        return std::errc::no_space_on_device;
    }
    return total;
}

}
