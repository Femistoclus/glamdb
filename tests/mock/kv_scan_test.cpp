#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/scan_cursor.h"
#include "core/scan/scan_planner.h"
#include "core/storage/base_state.h"
#include "core/storage/format_registry.h"
#include "core/storage/table_profile.h"
#include "tests/support/memory_file_io.h"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gtest/gtest.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/file_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/metrics_config.h>
#include <iceberg/partition_spec.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/merge_append.h>

#include <algorithm>
#include <format>

namespace glamdb::detail {
namespace {

TEST(VerifiedKVScan, FixedViewsSelectFileIntervalsAfterRemoval)
{
    namespace storage = NGlamDB::NStorage;
    const std::string storeID = "11111111-1111-4111-8111-111111111111";
    const std::string collectionID = "22222222-2222-4222-8222-222222222222";
    const auto location = "store://" + storeID + "/warehouse/" + collectionID;
    const SchemaSpec schema{{{"key", FieldType::String, false}}};
    auto profile = storage::MakeKVTableProfile({{{"key", NGlamDB::EFieldType::String}}});
    ASSERT_TRUE(profile);
    auto created = iceberg::TableMetadata::Make(*profile->Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        location,
        profile->Properties);
    ASSERT_TRUE(created);
    auto metadata = std::shared_ptr<iceberg::TableMetadata>(std::move(*created));
    metadata->current_snapshot_id = 99;
    auto io = std::make_shared<test::TMemoryFileIO>();
    storage::RegisterStorageFormats();
    auto membership = std::make_shared<storage::TScanMembership>();
    membership->KeyValue = true;
    for (const std::string key : {"old", "new", "untouched"}) {
        auto path = location;
        path.append("/data/").append(key).append(".parquet");
        auto batch = MakeRecordBatch(schema, {Row{key}});
        ASSERT_TRUE(batch);
        iceberg::WriterOptions options;
        options.path = path;
        options.io = io;
        options.schema = profile->Schema;
        auto metrics = iceberg::MetricsConfig::Make(profile->Properties);
        ASSERT_TRUE(metrics);
        options.metrics_config = *metrics;
        auto writer = iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options);
        ASSERT_TRUE(writer);
        ArrowArray exported{};
        ASSERT_TRUE(arrow::ExportRecordBatch(**batch, &exported).ok());
        ASSERT_TRUE((*writer)->Write(&exported));
        if (exported.release) {
            exported.release(&exported);
        }
        ASSERT_TRUE((*writer)->Close());
        membership->Files.push_back({path,
            static_cast<int64_t>(io->Files.at(path).size()),
            1,
            {},
            key == "new" ? 2 : 1,
            key == "old"       ? std::optional<int64_t>(2)
                : key == "new" ? std::optional<int64_t>(3)
                               : std::nullopt});
    }
    // Opaque IDs intentionally have a different order from their sequences.
    membership->Snapshots = {{7, 2, 2}, {55, 2, 1}, {99, 1, 3}};
    BaseIdentity base{storeID, collectionID, metadata->table_uuid, location + "/metadata/current.json", 99, 0, 0};
    auto state = std::make_shared<storage::TBaseState>(
        storage::TBaseState{base.metadata_location, metadata, {}, {}, membership});
    TScanContext context{
        .Collection = {collectionID, "keys", metadata->table_uuid, schema, 0, 99, {}, 0, CollectionMode::KeyValue},
        .Base = base,
        .FileIO = io,
        .Closing = {},
        .Lease = {},
        .OnError = {},
        .VerifiedState = state};
    const std::vector<std::pair<SnapshotSelector, std::vector<std::string>>> cases{
        {SnapshotSelector::Latest(), {"untouched"}},
        {SnapshotSelector::Id(55), {"old", "untouched"}},
        {SnapshotSelector::Id(7), {"new", "untouched"}},
        {SnapshotSelector::InitialEmpty(), {}}};
    for (const auto& [selector, expected] : cases) {
        auto cursor = OpenScan(context, {.collection_id = collectionID, .selector = selector});
        ASSERT_TRUE(cursor) << cursor.error().message;
        std::vector<std::string> rows;
        while (true) {
            auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            if (!*batch) {
                break;
            }
            const auto& strings = static_cast<const arrow::StringArray&>(*(*batch)->column(0));
            for (int64_t row = 0; row < strings.length(); ++row) {
                rows.push_back(strings.GetString(row));
            }
        }
        std::ranges::sort(rows);
        EXPECT_EQ(rows, expected);
    }
    state->ExpiredSnapshotIDs = {55};
    std::size_t reads = 0;
    io->OnRead = [&](const auto&) {
        ++reads;
    };
    const auto invalid = OpenScan(context, {.collection_id = collectionID});
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, "CORRUPTION");
    EXPECT_EQ(reads, 0U);
}

void CheckAppendChronology(bool merge)
{
    namespace storage = NGlamDB::NStorage;
    const std::string storeID = "11111111-1111-4111-8111-111111111111";
    const std::string collectionID = "22222222-2222-4222-8222-222222222222";
    const auto location = "store://" + storeID + "/warehouse/" + collectionID;
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::String, false}}};
    auto profile =
        storage::MakeTableProfile({{{"key", NGlamDB::EFieldType::String}, {"value", NGlamDB::EFieldType::String}}});
    ASSERT_TRUE(profile);
    if (merge) {
        profile->Properties["commit.manifest.min-count-to-merge"] = "2";
    }
    auto created = iceberg::TableMetadata::Make(*profile->Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        location,
        profile->Properties);
    ASSERT_TRUE(created);
    auto metadata = std::shared_ptr<iceberg::TableMetadata>(std::move(*created));
    auto io = std::make_shared<test::TMemoryFileIO>();
    storage::RegisterStorageFormats();
    auto catalog = iceberg::InMemoryCatalog::Make("chronology", io, location, {});
    ASSERT_TRUE(catalog);
    auto membership = std::make_shared<storage::TScanMembership>();
    std::string root = location + "/metadata/00000.metadata.json";
    const std::vector<std::vector<std::vector<std::string>>> operations{
        {{"first-file-row-1", "first-file-row-2"}, {"second-file"}},
        {{"last-file-row-1", "last-file-row-2"}}};
    for (std::size_t operation = 0; operation < operations.size(); ++operation) {
        auto table = iceberg::Table::Make({.ns = {}, .name = "events"}, metadata, root, io, *catalog);
        ASSERT_TRUE(table);
        auto transaction = iceberg::Transaction::Make(*table, iceberg::TransactionKind::kUpdate);
        ASSERT_TRUE(transaction);
        std::shared_ptr<iceberg::FastAppend> fast;
        std::shared_ptr<iceberg::MergeAppend> merged;
        if (merge) {
            auto update = (*transaction)->NewMergeAppend();
            ASSERT_TRUE(update);
            merged = *update;
        } else {
            auto update = (*transaction)->NewFastAppend();
            ASSERT_TRUE(update);
            fast = *update;
        }
        for (std::size_t ordinal = 0; ordinal < operations[operation].size(); ++ordinal) {
            std::vector<Row> rows;
            for (const auto& key : operations[operation][ordinal]) {
                rows.push_back({"duplicate-key", key});
            }
            auto batch = MakeRecordBatch(schema, rows);
            ASSERT_TRUE(batch);
            auto file = std::make_shared<iceberg::DataFile>();
            file->file_path =
                std::format("{}/data/44444444-4444-4444-8444-{:012}-33333333-3333-7333-8333-{:012}-{:05}.parquet",
                    location,
                    operation,
                    operation,
                    ordinal);
            file->content = iceberg::DataFile::Content::kData;
            file->partition_spec_id = metadata->default_spec_id;
            file->sort_order_id = metadata->default_sort_order_id;
            file->file_format = iceberg::FileFormatType::kParquet;
            file->record_count = static_cast<int64_t>(rows.size());
            iceberg::WriterOptions options;
            options.path = file->file_path;
            options.io = io;
            options.schema = profile->Schema;
            auto writer = iceberg::WriterFactoryRegistry::Open(iceberg::FileFormatType::kParquet, options);
            ASSERT_TRUE(writer);
            ArrowArray exported{};
            ASSERT_TRUE(arrow::ExportRecordBatch(**batch, &exported).ok());
            ASSERT_TRUE((*writer)->Write(&exported));
            if (exported.release) {
                exported.release(&exported);
            }
            ASSERT_TRUE((*writer)->Close());
            file->file_size_in_bytes = static_cast<int64_t>(io->Files.at(file->file_path).size());
            if (merged) {
                merged->AppendFile(file);
            } else {
                fast->AppendFile(file);
            }
            membership->Files.push_back({file->file_path, file->file_size_in_bytes, file->record_count, file});
        }
        const auto committed = merged ? merged->Commit() : fast->Commit();
        ASSERT_TRUE(committed) << committed.error().message;
        metadata = std::make_shared<iceberg::TableMetadata>((*transaction)->current());
        root = location + "/metadata/0000" + std::to_string(operation + 1) + ".metadata.json";
        ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*io, root, *metadata));
        membership->Snapshots.push_back({metadata->current_snapshot_id, membership->Files.size()});
    }
    std::ranges::sort(membership->Snapshots, {}, &storage::TSnapshotBoundary::SnapshotID);
    const BaseIdentity base{storeID, collectionID, metadata->table_uuid, root, metadata->current_snapshot_id, 0, 0};
    auto state = std::make_shared<storage::TBaseState>(storage::TBaseState{root, metadata, {}, {}, membership});
    if (merge) {
        const auto plan = PlanManifests(io, base, SnapshotSelector::Latest());
        ASSERT_TRUE(plan) << plan.error().message;
        ASSERT_TRUE(std::ranges::any_of(plan->Manifests, [](const auto& manifest) {
            return manifest.existing_files_count > 0 && manifest.min_sequence_number < manifest.sequence_number;
        })) << "the regression must exercise an actual merged manifest";
    }
    const std::vector<std::string> expected{"first-file-row-1",
        "first-file-row-2",
        "second-file",
        "last-file-row-1",
        "last-file-row-2"};
    for (const bool verified : {false, true}) {
        TScanContext context{
            .Collection = {collectionID, "events", metadata->table_uuid, schema, 0, metadata->current_snapshot_id},
            .Base = base,
            .FileIO = io,
            .Closing = {},
            .Lease = {},
            .OnError = {},
            .VerifiedState = verified ? state : nullptr,
            .ChronologicalFiles = true};
        auto cursor = OpenScan(std::move(context), {.collection_id = collectionID, .batch_rows = 1});
        ASSERT_TRUE(cursor) << cursor.error().message;
        std::vector<std::string> actual;
        while (true) {
            auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            if (!*batch) {
                break;
            }
            const auto& keys = static_cast<const arrow::StringArray&>(*(*batch)->column(1));
            for (int64_t row = 0; row < keys.length(); ++row) {
                actual.push_back(keys.GetString(row));
            }
        }
        EXPECT_EQ(actual, expected) << "verified index: " << verified;
    }
}

TEST(MigrationScan, PreservesAppendChronologyAndWithinOperationFileAndRowOrder)
{
    CheckAppendChronology(false);
}

TEST(MigrationScan, PreservesChronologyAfterManifestMerging)
{
    CheckAppendChronology(true);
}

}
}
