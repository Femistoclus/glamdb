#pragma once

#include "core/api/canonical.h"
#include "core/storage/base_state.h"
#include "core/storage/data_file_writer.h"

#include <filesystem>
#include <functional>

namespace arrow {
class MemoryPool;
}

namespace glamdb::detail {

struct BoundOperation;

struct TKvProduction
{
    std::vector<std::shared_ptr<iceberg::DataFile>> Added;
    std::vector<std::shared_ptr<iceberg::DataFile>> Removed;
    int64_t RowsAffected = 0;
    int64_t RowsWritten = 0;
    int64_t RowsRemoved = 0;
};

struct TKvProductionContext
{
    const CollectionInfo& Collection;
    const BoundOperation& Binding;
    const NormalizedInput& Input;
    const NGlamDB::NStorage::TBaseState& Base;
    std::shared_ptr<iceberg::FileIO> FileIO;
    std::shared_ptr<NGlamDB::NIO::TIOAttempt> Attempt;
    arrow::MemoryPool* Pool = nullptr;
    std::filesystem::path ScratchDirectory;
    NGlamDB::NStorage::TDataWriterLimits Writer;
    NGlamDB::NStorage::TParquetWriteSpec WriteSpec;
    std::chrono::steady_clock::time_point Deadline;
    std::function<Status()> CheckCapacity;
};

// Throws the same Failure used by backend admission and storage mapping. The
// caller owns the coordinator's bounded memory/device reservations and attempt.
TKvProduction ProduceKvMutation(const TKvProductionContext& context);

} // namespace glamdb::detail
