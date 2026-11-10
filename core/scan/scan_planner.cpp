#include "core/scan/scan_planner.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/storage/capacity.h"
#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"

#include <iceberg/constants.h>
#include <iceberg/file_format.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

namespace glamdb::detail {
namespace {

template <class T>
T Take(iceberg::Result<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        const auto kind = result.error().kind;
        const auto effective =
            (kind == iceberg::ErrorKind::kNotFound || kind == iceberg::ErrorKind::kInvalid) ? "CORRUPTION" : code;
        throw Failure({std::string(effective),
            BoundedDiagnostic(std::string(context) + ": " + result.error().message),
            RetryHint::OperatorAction});
    }
    return std::move(*result);
}

[[noreturn]] void Unsupported(std::string_view message)
{
    throw Failure({"UNSUPPORTED_TABLE_FEATURE", std::string(message), RetryHint::OperatorAction});
}

std::shared_ptr<iceberg::Snapshot> SelectSnapshot(const iceberg::TableMetadata& metadata,
    const SnapshotSelector& selector)
{
    switch (selector.kind) {
        case SnapshotSelectorKind::Latest:
            if (metadata.current_snapshot_id == iceberg::kInvalidSnapshotId) {
                return nullptr;
            }
            return Take(metadata.Snapshot(), "CORRUPTION", "current snapshot");
        case SnapshotSelectorKind::Id: {
            auto snapshot = metadata.SnapshotById(selector.snapshot_id);
            Require(snapshot.has_value(), "NOT_FOUND", "snapshot is not retained by this collection");
            return *snapshot;
        }
        case SnapshotSelectorKind::InitialEmpty:
            return nullptr;
    }
    throw Failure({"INVALID_ARGUMENT", "unknown snapshot selector", RetryHint::FixInput});
}

void CheckRoot(const iceberg::TableMetadata& metadata, const BaseIdentity& base)
{
    const auto current = metadata.current_snapshot_id == iceberg::kInvalidSnapshotId
        ? std::nullopt
        : std::optional(metadata.current_snapshot_id);
    Require(metadata.table_uuid == base.table_uuid && current == base.current_snapshot_id,
        "CORRUPTION",
        "committed root does not match the catalog identity");
    Require(metadata.format_version == 2 && metadata.current_schema_id == base.schema_id &&
            metadata.default_spec_id == base.spec_id,
        "UNSUPPORTED_TABLE_FEATURE",
        "only the initial unpartitioned Iceberg v2 schema is supported");
}

void AppendFiles(const iceberg::TableMetadata& metadata,
    const std::shared_ptr<iceberg::FileIO>& io,
    const iceberg::ManifestFile& manifest,
    std::vector<TScanFile>& files)
{
    if (manifest.content != iceberg::ManifestContent::kData) {
        Unsupported("delete manifests are not supported");
    }
    auto schema = Take(metadata.Schema(), "CORRUPTION", "table schema");
    auto spec = Take(metadata.PartitionSpecById(manifest.partition_spec_id), "CORRUPTION", "partition spec");
    if (!spec->IsUnpartitioned()) {
        Unsupported("partitioned manifests are not supported");
    }
    auto reader = Take(iceberg::ManifestReader::Make(manifest, io, schema, spec), "IO_ERROR", "open manifest");
    for (const auto& entry : Take(reader->LiveEntries(), "IO_ERROR", "read manifest")) {
        const auto& file = entry.data_file;
        Require(file != nullptr, "CORRUPTION", "manifest entry has no data file");
        if (file->content != iceberg::DataFile::Content::kData ||
            file->file_format != iceberg::FileFormatType::kParquet)
        {
            Unsupported("only Parquet data files are supported");
        }
        Require(file->file_size_in_bytes > 0 && file->record_count >= 0,
            "CORRUPTION",
            "manifest has invalid data file metrics");
        files.push_back({file->file_path, file->file_size_in_bytes, file->record_count});
    }
}

} // namespace

Result<TScanPlan>
PlanScan(const std::shared_ptr<iceberg::FileIO>& io, const BaseIdentity& base, const SnapshotSelector& selector)
{
    try {
        Require(io != nullptr, "DEPENDENCY_UNAVAILABLE", "storage reader is not installed");
        NGlamDB::NStorage::RegisterStorageFormats();
        const auto limit = static_cast<int64_t>(NGlamDB::NStorage::TStorageLimits{}.RootBytes);
        const auto metadata =
            Take(NGlamDB::NStorage::ReadTableMetadata(*io, base.metadata_location, std::nullopt, limit),
                "CORRUPTION",
                "read committed root");
        CheckRoot(*metadata, base);
        TScanPlan plan;
        const auto snapshot = SelectSnapshot(*metadata, selector);
        if (!snapshot) {
            return plan;
        }
        plan.SnapshotID = snapshot->snapshot_id;
        const auto list =
            Take(iceberg::ManifestListReader::Make(snapshot->manifest_list, io), "IO_ERROR", "open manifest list");
        for (const auto& manifest : Take(list->Files(), "IO_ERROR", "read manifest list")) {
            AppendFiles(*metadata, io, manifest, plan.Files);
        }
        return plan;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

} // namespace glamdb::detail
