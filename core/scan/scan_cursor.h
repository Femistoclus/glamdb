#pragma once

#include "storage/scan.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

namespace arrow {
class MemoryPool;
} // namespace arrow

namespace iceberg {
class FileIO;
} // namespace iceberg

namespace NGlamDB::NStorage {
struct TBaseState;
}

namespace glamdb::detail {

struct TScanContext
{
    std::shared_ptr<ResourceOwner> Resources{};
    CollectionInfo Collection;
    // The committed view resolved once by the engine.
    BaseIdentity Base;
    std::shared_ptr<iceberg::FileIO> FileIO;
    // Aliases the engine's closing flag and retains the engine state, its
    // catalog and store lease until the cursor is released.
    std::shared_ptr<const std::atomic<bool>> Closing;
    // Retains engine resources and the store lock in returned Arrow buffers
    // until the last batch, extracted buffer, slice or C Data export is released.
    std::shared_ptr<const void> Lease;
    // Optional owned upstream allocator for decoded and filtered Arrow buffers.
    // A per-scan resource pool wraps it; empty selects Arrow's default pool.
    std::shared_ptr<arrow::MemoryPool> Pool{};
    std::function<void(const ErrorDetail&)> OnError;
    std::shared_ptr<const NGlamDB::NStorage::TBaseState> VerifiedState{};
};

Result<std::shared_ptr<ScanCursor>> OpenScan(TScanContext context, const ScanRequest& request);

// Data read work of one cursor beyond the public ScanProfile; internal, for
// tests and benchmarks. Reads are positioned reads of data files through the
// store FileIO (footers and column chunks); ScanProfile::data_bytes_read is
// their byte total.
struct TScanReadStats
{
    // Files skipped by manifest metrics and row groups skipped by Parquet
    // statistics of opened files; never files left unread by a limit or
    // cancellation. RowGroupsRead counts row groups selected for decoding.
    int64_t FilesPruned = 0;
    int64_t RowGroupsPruned = 0;
    int64_t RowGroupsRead = 0;
    int64_t ReadCalls = 0;
    // The largest single read, and the most read bytes held at once by the
    // decoder of one file.
    int64_t MaxReadBytes = 0;
    int64_t PeakHeldBytes = 0;
};

// nullopt for a cursor that OpenScan did not create.
std::optional<TScanReadStats> GetScanReadStats(const ScanCursor& cursor);

} // namespace glamdb::detail
