#include "core/storage/capacity.h"
#include "core/io/store_dirs.h"

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
    const auto bytes = directories.Fs()->MeasureTree(directories.Warehouse(), limits.WarehouseBytes);
    if (!bytes) {
        return bytes.Error();
    }
    const auto space = directories.Fs()->StatFs(directories.Root());
    return space ? CheckDiskReserve(space.Value(), bytes.Value(), limits) : space.Error();
}

}
