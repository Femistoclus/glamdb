#pragma once

#include "storage/scan.h"

namespace glamdb {

struct DeleteRequest
{
    std::string collection_id;
    std::string operation_id;
    SnapshotPrecondition expected_snapshot{};
    std::vector<std::string> keys;
    std::shared_ptr<const std::atomic<bool>> cancellation{};
    std::chrono::milliseconds timeout{60000};
};

struct CompactionRequest
{
    std::string collection_id;
    std::string operation_id;
    SnapshotPrecondition expected_snapshot{};
    int64_t max_files = 64;
    int64_t target_file_bytes = 8LL << 20;
    std::shared_ptr<const std::atomic<bool>> cancellation{};
    std::chrono::milliseconds timeout{60000};
};

struct LookupRequest
{
    std::string collection_id;
    std::vector<std::string> keys;
    SnapshotSelector snapshot{};
};

struct LookupResult
{
    std::shared_ptr<const void> resource_lease{};
    SnapshotSelector snapshot;
    // Input order, with repeated hits and explicit misses.
    std::vector<std::optional<Row>> rows;
};

struct MigrationRequest
{
    std::string source_collection_id;
    SnapshotSelector source_snapshot{};
    CreateCollectionRequest target;
    std::string operation_id;
    // Required explicitly; Unspecified is rejected before creating a target.
    DuplicatePolicy duplicate_policy = DuplicatePolicy::Unspecified;
    std::shared_ptr<const std::atomic<bool>> cancellation{};
    std::chrono::milliseconds timeout{600000};
};

// Creation and population are separate durable steps. A failed/unknown creation
// never starts population; each step retains its own authoritative outcome.
struct MigrationOutcome
{
    CreateOutcome creation;
    std::optional<AppendOutcome> mutation;
    // No population outcome was resolved while another publication/recovery
    // blocked its durable identity. Retry after that barrier is resolved.
    std::optional<ErrorDetail> resolution_error{};
};

} // namespace glamdb
