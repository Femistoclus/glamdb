#pragma once

#include "core/catalog/durable_sqlite_catalog.h"
#include "core/engine/storage_backend.h"
#include "core/storage/base_state.h"
#include "core/storage/publication_guard.h"

#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace glamdb::detail {

// The catalog checkpoint of a verified append generation; nothing for KV
// collections, an unreadable root or values outside the catalog's range.
// The root digest comes from the publication's staged bytes when it has
// them, otherwise from the durable root file.
std::optional<CollectionCheckpoint> MakeCheckpoint(std::string_view collectionID,
    const NGlamDB::NStorage::TBaseState& state,
    const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::optional<NGlamDB::NStorage::TStagedRoot>& staged = std::nullopt);

// What OpenFromCheckpoints rebuilds a collection from.
NGlamDB::NStorage::TCheckpointedState CheckpointedState(const CollectionCheckpointView& view,
    std::span<const ExpiredSnapshotRecord> expired);

} // namespace glamdb::detail
