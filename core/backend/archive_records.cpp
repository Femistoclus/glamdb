#include "core/backend/archive_records.h"

#include "core/api/binary.h"

#include <iceberg/snapshot.h>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

namespace glamdb::detail {
namespace {

constexpr std::string_view OperationKey = "glamdb.operation-id";
constexpr uint16_t EncodingVersion = 1;

bool SameSnapshot(const iceberg::Snapshot& left, const iceberg::Snapshot& right)
{
    // The pinned Iceberg equality ignores the summary and the list location.
    return left == right && left.snapshot_id == right.snapshot_id &&
        left.parent_snapshot_id == right.parent_snapshot_id && left.sequence_number == right.sequence_number &&
        left.timestamp_ms == right.timestamp_ms && left.manifest_list == right.manifest_list &&
        left.summary == right.summary && left.schema_id == right.schema_id && left.first_row_id == right.first_row_id &&
        left.added_rows == right.added_rows;
}

} // namespace

std::string EncodeArchivedSnapshot(const iceberg::Snapshot& snapshot)
{
    Encoder out;
    out.Number<uint16_t>(EncodingVersion);
    out.Number<int64_t>(snapshot.snapshot_id);
    out.OptionalNumber(snapshot.parent_snapshot_id);
    out.Number<int64_t>(snapshot.sequence_number);
    out.Number<int64_t>(snapshot.timestamp_ms.time_since_epoch().count());
    out.Text(snapshot.manifest_list);
    std::vector<std::pair<std::string_view, std::string_view>> summary(snapshot.summary.begin(),
        snapshot.summary.end());
    std::ranges::sort(summary);
    Require(summary.size() <= 1024, "INVALID_ARGUMENT", "snapshot summary is too large to archive");
    out.Number<uint32_t>(static_cast<uint32_t>(summary.size()));
    for (const auto& [key, value] : summary) {
        out.Text(key);
        out.Text(value);
    }
    out.OptionalNumber(snapshot.schema_id ? std::optional<int64_t>(*snapshot.schema_id) : std::nullopt);
    out.OptionalNumber(snapshot.first_row_id);
    out.OptionalNumber(snapshot.added_rows);
    return out.bytes;
}

std::shared_ptr<iceberg::Snapshot> DecodeArchivedSnapshot(const ArchivedSnapshotRecord& record)
{
    Require(!record.snapshot_blob.empty() && record.snapshot_blob.size() <= 65536,
        "CORRUPTION",
        "invalid archived snapshot entry");
    Decoder in{record.snapshot_blob};
    Require(in.Number<uint16_t>() == EncodingVersion, "UNSUPPORTED_STORE_VERSION", "unknown archived snapshot format");
    auto snapshot = std::make_shared<iceberg::Snapshot>();
    snapshot->snapshot_id = in.Number<int64_t>();
    snapshot->parent_snapshot_id = in.OptionalNumber();
    snapshot->sequence_number = in.Number<int64_t>();
    snapshot->timestamp_ms = iceberg::TimePointMs(std::chrono::milliseconds(in.Number<int64_t>()));
    snapshot->manifest_list = in.Text();
    const auto entries = in.Number<uint32_t>();
    Require(entries <= 1024, "CORRUPTION", "archived snapshot summary is too large");
    for (uint32_t index = 0; index < entries; ++index) {
        auto key = in.Text();
        auto value = in.Text();
        Require(snapshot->summary.emplace(std::move(key), std::move(value)).second,
            "CORRUPTION",
            "archived snapshot summary repeats a key");
    }
    if (const auto schema = in.OptionalNumber()) {
        Require(*schema >= std::numeric_limits<int32_t>::min() && *schema <= std::numeric_limits<int32_t>::max(),
            "CORRUPTION",
            "archived snapshot schema is out of range");
        snapshot->schema_id = static_cast<int32_t>(*schema);
    }
    snapshot->first_row_id = in.OptionalNumber();
    snapshot->added_rows = in.OptionalNumber();
    in.End();
    const auto operation = snapshot->summary.find(std::string(OperationKey));
    Require(EncodeArchivedSnapshot(*snapshot) == record.snapshot_blob && snapshot->snapshot_id == record.snapshot_id &&
            snapshot->sequence_number == record.sequence_number &&
            snapshot->parent_snapshot_id == record.parent_snapshot_id &&
            snapshot->timestamp_ms.time_since_epoch().count() == record.timestamp_ms &&
            snapshot->manifest_list == record.manifest_list && operation != snapshot->summary.end() &&
            operation->second == record.operation_id,
        "CORRUPTION",
        "archived snapshot row differs from its snapshot entry");
    return snapshot;
}

ArchivedSnapshotRecord MakeArchivedRecord(const NGlamDB::NStorage::TArchivedSnapshot& archived)
{
    Require(archived.Snapshot != nullptr && archived.ManifestListBytes > 0 &&
            archived.ManifestListBytes <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
            archived.DataFiles > 0 &&
            archived.DataFiles <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
            archived.RetiredManifests <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) &&
            archived.RetiredManifestBytes <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()),
        "INTERNAL",
        "invalid archived snapshot description");
    const auto& snapshot = *archived.Snapshot;
    const auto operation = snapshot.summary.find(std::string(OperationKey));
    Require(operation != snapshot.summary.end(), "CORRUPTION", "archived snapshot has no operation identity");
    ArchivedSnapshotRecord record{.snapshot_id = snapshot.snapshot_id,
        .sequence_number = snapshot.sequence_number,
        .parent_snapshot_id = snapshot.parent_snapshot_id,
        .timestamp_ms = snapshot.timestamp_ms.time_since_epoch().count(),
        .operation_id = operation->second,
        .manifest_list = snapshot.manifest_list,
        .manifest_list_bytes = static_cast<int64_t>(archived.ManifestListBytes),
        .manifest_list_sha256 = archived.ManifestListSha256,
        .data_files = static_cast<int64_t>(archived.DataFiles),
        .retired_manifests = static_cast<int64_t>(archived.RetiredManifests),
        .retired_manifest_bytes = static_cast<int64_t>(archived.RetiredManifestBytes),
        .snapshot_blob = EncodeArchivedSnapshot(snapshot)};
    // The archive must reproduce the root entry exactly.
    Require(SameSnapshot(*DecodeArchivedSnapshot(record), snapshot),
        "INTERNAL",
        "archived snapshot entry does not round-trip");
    return record;
}

NGlamDB::NStorage::TArchivedList ArchivedListIdentity(const ArchivedSnapshotRecord& record)
{
    return {.SnapshotID = record.snapshot_id,
        .Sequence = record.sequence_number,
        .Path = record.manifest_list,
        .Bytes = static_cast<uint64_t>(record.manifest_list_bytes),
        .Sha256 = record.manifest_list_sha256};
}

} // namespace glamdb::detail
