#include "core/storage/metadata_validation.h"
#include "core/io/store_uri.h"
#include "core/storage/iceberg_error_boundary.h"
#include "core/storage/table_profile.h"

#include <iceberg/constants.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>

#include <algorithm>
#include <limits>
#include <span>
#include <tuple>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

bool HasNullObjects(const iceberg::TableMetadata& metadata)
{
    const auto hasNull = [](const auto& items) {
        return std::ranges::any_of(items, [](const auto& item) {
            return !item;
        });
    };
    return hasNull(metadata.schemas) || hasNull(metadata.partition_specs) || hasNull(metadata.sort_orders) ||
        hasNull(metadata.snapshots) || hasNull(metadata.statistics) || hasNull(metadata.partition_statistics) ||
        std::ranges::any_of(metadata.refs, [](const auto& item) {
            return !item.second;
        });
}

bool EqualSchemas(const iceberg::TableMetadata& left, const iceberg::TableMetadata& right)
{
    return std::ranges::equal(left.schemas, right.schemas, [](const auto& first, const auto& second) {
        return *first == *second &&
            std::ranges::equal(first->fields(), second->fields(), [](const auto& field, const auto& other) {
                return field.doc() == other.doc();
            });
    });
}

iceberg::Status CheckNewSnapshot(const iceberg::TableMetadata& base,
    const iceberg::TableMetadata& candidate,
    const std::unordered_map<std::string, std::string>& expectedSummary)
{
    const auto& snapshot = *candidate.snapshots.back();
    ICEBERG_RETURN_UNEXPECTED(CheckSnapshotTimestamp(snapshot));
    std::optional<std::int64_t> parent{};
    if (base.current_snapshot_id != iceberg::kInvalidSnapshotId) {
        parent = base.current_snapshot_id;
    }
    if (snapshot.snapshot_id == iceberg::kInvalidSnapshotId || snapshot.parent_snapshot_id != parent ||
        snapshot.snapshot_id != candidate.current_snapshot_id ||
        snapshot.sequence_number != candidate.last_sequence_number ||
        snapshot.schema_id != candidate.current_schema_id || snapshot.first_row_id || snapshot.added_rows ||
        snapshot.manifest_list.empty())
    {
        return iceberg::ValidationFailed("Invalid append snapshot identity.");
    }
    if (std::ranges::any_of(base.snapshots, [&](const auto& previous) {
            return previous->snapshot_id == snapshot.snapshot_id;
        }))
    {
        return iceberg::ValidationFailed("Append reuses a snapshot ID.");
    }
    const auto operation = snapshot.summary.find("operation");
    if (operation == snapshot.summary.end() || operation->second != "append") {
        return iceberg::ValidationFailed("Snapshot is not an append.");
    }
    for (const auto& [key, value] : expectedSummary) {
        const auto actual = snapshot.summary.find(key);
        if (actual == snapshot.summary.end() || actual->second != value) {
            return iceberg::ValidationFailed("Snapshot summary differs at {}.", key);
        }
    }
    return {};
}

iceberg::Status RestoreMainRef(const iceberg::TableMetadata& base, iceberg::TableMetadata* normalized)
{
    const auto next = normalized->refs.find("main");
    if (next == normalized->refs.end() || next->second->snapshot_id != normalized->current_snapshot_id ||
        next->second->type() != iceberg::SnapshotRefType::kBranch)
    {
        return iceberg::ValidationFailed("Invalid main snapshot reference.");
    }
    const auto previous = base.refs.find("main");
    if (previous == base.refs.end()) {
        const iceberg::SnapshotRef empty{.snapshot_id = normalized->current_snapshot_id,
            .retention = iceberg::SnapshotRef::Branch{}};
        if (*next->second != empty) {
            return iceberg::ValidationFailed("Append changes snapshot retention.");
        }
        normalized->refs.erase(next);
    } else {
        auto restored = *next->second;
        restored.snapshot_id = previous->second->snapshot_id;
        if (restored != *previous->second) {
            return iceberg::ValidationFailed("Append changes snapshot retention.");
        }
        next->second = previous->second;
    }
    return {};
}

}

iceberg::Status CheckSnapshotTimestamp(const iceberg::Snapshot& snapshot)
{
    const auto milliseconds = snapshot.timestamp_ms.time_since_epoch().count();
    if (milliseconds < -62135596800000LL || milliseconds > 253402300799999LL) {
        return iceberg::ValidationFailed("Snapshot timestamp is outside UTC years 0001..9999.");
    }
    return {};
}

iceberg::Status
CheckCreateMetadata(const iceberg::TableMetadata& candidate, const TTableProfile& profile, const std::string& location)
{
    return CatchIcebergExceptions([&]() -> iceberg::Status {
        ICEBERG_RETURN_UNEXPECTED(CheckTableProfile(profile));
        if (HasNullObjects(candidate) || !NIO::IsCanonicalUuid(candidate.table_uuid)) {
            return iceberg::ValidationFailed("Invalid collection metadata.");
        }
        auto expected = iceberg::TableMetadata::Make(*profile.Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            location,
            profile.Properties);
        if (!expected) {
            return std::unexpected(std::move(expected.error()));
        }
        (*expected)->table_uuid = candidate.table_uuid;
        (*expected)->last_updated_ms = candidate.last_updated_ms;
        if (**expected != candidate || !EqualSchemas(**expected, candidate)) {
            return iceberg::ValidationFailed("Collection metadata differs from its profile.");
        }
        return {};
    });
}

iceberg::Status CheckAppendMetadata(const iceberg::TableMetadata& base,
    const iceberg::TableMetadata& candidate,
    const std::unordered_map<std::string, std::string>& expectedSummary)
{
    return CatchIcebergExceptions([&]() -> iceberg::Status {
        if (HasNullObjects(base) || HasNullObjects(candidate) || base.format_version != 2 ||
            candidate.snapshots.size() != base.snapshots.size() + 1 ||
            candidate.snapshot_log.size() != base.snapshot_log.size() + 1 ||
            base.last_sequence_number == std::numeric_limits<std::int64_t>::max() ||
            candidate.last_sequence_number != base.last_sequence_number + 1)
        {
            return iceberg::ValidationFailed("Invalid append metadata delta.");
        }
        if (!std::ranges::equal(base.snapshots,
                std::span(candidate.snapshots).first(base.snapshots.size()),
                [](const auto& previous, const auto& current) {
                    // A staged root shares the base's snapshot objects; the
                    // same object trivially compares equal to itself.
                    return previous == current ||
                        (*previous == *current && previous->summary == current->summary &&
                            previous->manifest_list == current->manifest_list);
                }))
        {
            return iceberg::ValidationFailed("Append changes a retained snapshot.");
        }
        auto status = CheckNewSnapshot(base, candidate, expectedSummary);
        if (!status) {
            return status;
        }
        const auto& log = candidate.snapshot_log.back();
        if (log.snapshot_id != candidate.current_snapshot_id ||
            log.timestamp_ms != candidate.snapshots.back()->timestamp_ms)
        {
            return iceberg::ValidationFailed("Append snapshot log differs from the snapshot.");
        }
        auto normalized = candidate;
        status = RestoreMainRef(base, &normalized);
        if (!status) {
            return status;
        }
        normalized.snapshots.pop_back();
        normalized.snapshot_log.pop_back();
        normalized.current_snapshot_id = base.current_snapshot_id;
        normalized.last_sequence_number = base.last_sequence_number;
        normalized.last_updated_ms = base.last_updated_ms;
        if (normalized != base || !EqualSchemas(normalized, base)) {
            return iceberg::ValidationFailed("Append changes immutable table metadata.");
        }
        return {};
    });
}

bool EqualManifest(const iceberg::ManifestFile& left, const iceberg::ManifestFile& right)
{
    const auto fields = [](const iceberg::ManifestFile& item) {
        return std::tie(item.manifest_path,
            item.manifest_length,
            item.partition_spec_id,
            item.content,
            item.sequence_number,
            item.min_sequence_number,
            item.added_snapshot_id,
            item.added_files_count,
            item.existing_files_count,
            item.deleted_files_count,
            item.added_rows_count,
            item.existing_rows_count,
            item.deleted_rows_count,
            item.partitions,
            item.key_metadata,
            item.first_row_id);
    };
    return fields(left) == fields(right);
}

}
