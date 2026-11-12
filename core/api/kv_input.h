#pragma once

#include "storage/engine.h"

namespace glamdb::detail {

struct TKvInput
{
    MutationKind Kind = MutationKind::Upsert;
    DuplicatePolicy Duplicates = DuplicatePolicy::LastWins;
    int64_t InputRows = 0;
    int64_t MaxFiles = 256;
    int64_t TargetFileBytes = 8LL << 20;
    std::shared_ptr<const std::atomic<bool>> Cancellation{};
    std::chrono::milliseconds Timeout{60000};
    // Fixed migration source identity; ordinary mutation requests leave it empty.
    std::string Identity{};
    // Catalog capability for the operation reserved by target creation.
    std::string MigrationOwner{};
};

Result<std::string> KvPayloadHash(const SchemaSpec& schema, std::string_view sourceHash, const TKvInput& input);

// Atomic, read-only replay classification while publication/recovery fences admission.
std::optional<MigrationOutcome> BlockedMigration(const std::shared_ptr<Engine::Impl>& impl,
    const MigrationRequest& request);
std::optional<MigrationOutcome>
ReplayMigration(const std::shared_ptr<Engine::Impl>& impl, const MigrationRequest& request, std::string_view identity);

} // namespace glamdb::detail
