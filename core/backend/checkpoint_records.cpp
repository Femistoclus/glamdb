#include "core/backend/checkpoint_records.h"

#include "core/io/sha256.h"
#include "core/storage/kv_state.h"

#include <iceberg/file_io.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <limits>

namespace glamdb::detail {

std::optional<CollectionCheckpoint> MakeCheckpoint(std::string_view collectionID,
    const NGlamDB::NStorage::TBaseState& state,
    const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::optional<NGlamDB::NStorage::TStagedRoot>& staged)
{
    try {
        if (!state.Metadata || !state.ScanIndex || state.ScanIndex->KeyValue ||
            NGlamDB::NStorage::IsKVMetadata(*state.Metadata) || !fileIO)
        {
            return std::nullopt;
        }
        constexpr auto max = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        const auto& usage = state.Usage;
        const std::array<uint64_t, 8> values{usage.RootBytes,
            usage.Snapshots,
            usage.SnapshotLogEntries,
            usage.DataFiles,
            usage.Manifests,
            usage.ManifestBytes,
            usage.ManifestListBytes,
            usage.FooterBytes};
        CollectionCheckpoint checkpoint;
        for (size_t index = 0; index < values.size(); ++index) {
            if (values[index] > max) {
                return std::nullopt;
            }
            checkpoint.usage[index] = static_cast<int64_t>(values[index]);
        }
        std::string bytes;
        if (staged && staged->Bytes && staged->Bytes->size() == usage.RootBytes) {
            bytes = *staged->Bytes;
        } else {
            auto read = fileIO->ReadFile(state.MetadataLocation, static_cast<size_t>(usage.RootBytes));
            if (!read || read->size() != usage.RootBytes) {
                return std::nullopt;
            }
            bytes = std::move(*read);
        }
        NGlamDB::NIO::TSha256 hash;
        if (hash.Update(std::as_bytes(std::span(bytes.data(), bytes.size())))) {
            return std::nullopt;
        }
        checkpoint.collection_id = std::string(collectionID);
        const auto& metadata = *state.Metadata;
        if (!metadata.snapshots.empty()) {
            checkpoint.snapshot_id = metadata.current_snapshot_id;
        }
        checkpoint.sequence_number = metadata.last_sequence_number;
        checkpoint.metadata_location = state.MetadataLocation;
        checkpoint.root_bytes = static_cast<int64_t>(usage.RootBytes);
        checkpoint.root_sha256 = NGlamDB::NIO::HexDigest(hash.Finalize());
        return checkpoint;
    } catch (const std::exception&) {
        // Without a checkpoint the next open runs full verification.
        return std::nullopt;
    }
}

NGlamDB::NStorage::TCheckpointedState CheckpointedState(const CollectionCheckpointView& view,
    std::span<const ExpiredSnapshotRecord> expired)
{
    const auto& checkpoint = view.checkpoint;
    NGlamDB::NStorage::TCheckpointedState state;
    state.RootLocation = checkpoint.metadata_location;
    state.RootBytes = static_cast<uint64_t>(checkpoint.root_bytes);
    state.RootSha256 = checkpoint.root_sha256;
    state.SnapshotID = checkpoint.snapshot_id;
    state.Sequence = checkpoint.sequence_number;
    const auto& usage = checkpoint.usage;
    state.Usage = {static_cast<uint64_t>(usage[0]),
        static_cast<uint64_t>(usage[1]),
        static_cast<uint64_t>(usage[2]),
        static_cast<uint64_t>(usage[3]),
        static_cast<uint64_t>(usage[4]),
        static_cast<uint64_t>(usage[5]),
        static_cast<uint64_t>(usage[6]),
        static_cast<uint64_t>(usage[7])};
    state.ExpiredIDs.reserve(expired.size());
    for (const auto& record : expired) {
        state.ExpiredIDs.push_back(record.snapshot_id);
    }
    state.Archived = static_cast<uint64_t>(checkpoint.archived);
    if (view.newest_archived) {
        state.PrefixParent = view.newest_archived->snapshot_id;
        state.PrefixDataFiles = static_cast<uint64_t>(view.newest_archived->data_files);
    } else if (!expired.empty()) {
        state.PrefixParent = expired.back().snapshot_id;
    }
    return state;
}

} // namespace glamdb::detail
