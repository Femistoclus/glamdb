#include "core/storage/append_stager.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/metadata_validation.h"
#include "core/storage/publication_guard.h"
#include "core/storage/table_profile.h"
#include "tests/support/memory_file_io.h"
#include "tests/support/temp_dir.h"
#include <algorithm>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <format>
#include <functional>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/snapshot.h>
#include <iceberg/table.h>
#include <iceberg/transaction.h>
#include <iceberg/update/overwrite_files.h>
#include <iceberg/update/rewrite_files.h>

#include <gtest/gtest.h>
#include <iceberg/partition_spec.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

TEST(KVStorage, AcceptsExplicitKVProfileWithoutChangingAppendProfile)
{
    auto append = MakeTableProfile({{{"value", EFieldType::Int64, true}, {"key", EFieldType::String}}});
    ASSERT_TRUE(append);
    auto kv = *append;
    kv.Properties.emplace("glamdb.collection-mode", "kv1");
    const std::string location =
        "store://11111111-1111-4111-8111-111111111111/warehouse/22222222-2222-4222-8222-222222222222";
    auto metadata = iceberg::TableMetadata::Make(*kv.Schema,
        *iceberg::PartitionSpec::Unpartitioned(),
        *iceberg::SortOrder::Unsorted(),
        location,
        kv.Properties);
    ASSERT_TRUE(metadata);
    EXPECT_TRUE(CheckCreateMetadata(**metadata, kv, location));
    EXPECT_FALSE(CheckCreateMetadata(**metadata, *append, location));
}

class TKVClosureTest : public ::testing::Test
{
protected:
    glamdb::test::TempDir Temp_;
    std::shared_ptr<NIO::TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> IO_;
    TTableProfile Profile_;
    std::shared_ptr<const TBaseState> Empty_;
    int AttemptNumber_ = 1;
    static constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
    static constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
    const std::string Location_ = std::string("store://") + StoreID + "/warehouse/" + CollectionID;

    void SetUp() override
    {
        auto dirs = NIO::TStoreDirs::Create(std::make_shared<NIO::TPosixLocalFS>(),
            Temp_.path,
            std::make_shared<NIO::TWriteLatch>());
        ASSERT_TRUE(dirs);
        Dirs_ = std::move(dirs).Value();
        ASSERT_TRUE(Dirs_->CreateCollection(CollectionID));
        auto io = TDurableLocalFileIO::Make(Dirs_, StoreID);
        ASSERT_TRUE(io);
        IO_ = *io;
        auto profile = MakeKVTableProfile({{{"value", EFieldType::Int64, true}, {"key", EFieldType::String}}});
        ASSERT_TRUE(profile);
        Profile_ = *profile;
        auto metadata = iceberg::TableMetadata::Make(*Profile_.Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Location_,
            Profile_.Properties);
        ASSERT_TRUE(metadata);
        const std::string attemptID = "33333333-3333-7333-8333-333333333330";
        auto attempt =
            NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::CreateCollection, std::nullopt},
                Dirs_->Latch());
        ASSERT_TRUE(attempt);
        auto scope = IO_->BindAttempt(attempt.Value());
        ASSERT_TRUE(scope);
        const auto root = Location_ + "/metadata/00000-" + attemptID + ".metadata.json";
        ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*IO_, root, **metadata));
        TStoragePublicationGuard guard({.Directories = Dirs_,
            .FileIO = IO_,
            .Attempt = attempt.Value(),
            .Namespace = "test",
            .Name = "keys",
            .Profile = Profile_});
        auto status = guard.BeforePointerChange({EPointerOp::InsertTable, "test", "keys", {}, root});
        ASSERT_TRUE(status) << status.error().message;
        Empty_ = guard.GetCandidate();
    }

    iceberg::Result<std::shared_ptr<const TBaseState>> Change(const std::shared_ptr<const TBaseState>& base,
        const std::vector<std::vector<std::string>>& keyFiles,
        const std::vector<std::shared_ptr<iceberg::DataFile>>& removed = {},
        bool compact = false,
        bool rawAppend = false,
        std::optional<std::int64_t> valueOverride = {},
        std::string_view kindOverride = {},
        std::optional<std::int64_t> affectedOverride = {},
        const std::function<void(iceberg::DataFile&)>& mutateAdded = {},
        std::optional<std::int64_t> metadataBudgetSlack = {})
    {
        const auto ordinal = AttemptNumber_++;
        const auto attemptID = std::format("33333333-3333-7333-8333-{:012}", ordinal);
        const auto operationID = std::format("44444444-4444-4444-8444-{:012}", ordinal);
        auto attemptResult =
            NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, operationID},
                Dirs_->Latch(),
                NIO::TAttemptLimits::KeyValue());
        if (!attemptResult) {
            return iceberg::IOError(attemptResult.Error().message());
        }
        auto attempt = attemptResult.Value();
        ICEBERG_ASSIGN_OR_RAISE(auto scope, IO_->BindAttempt(attempt));
        std::vector<std::shared_ptr<iceberg::DataFile>> added;
        for (const auto& keys : keyFiles) {
            arrow::StringBuilder builder;
            arrow::Int64Builder values;
            for (const auto& key : keys) {
                const auto status = builder.Append(key);
                if (!status.ok()) {
                    return iceberg::IOError(status.ToString());
                }
                const auto valueStatus = valueOverride ? values.Append(*valueOverride)
                    : key == "b"                       ? values.AppendNull()
                                                       : values.Append(static_cast<std::int64_t>(key.size()));
                if (!valueStatus.ok()) {
                    return iceberg::IOError(valueStatus.ToString());
                }
            }
            auto array = builder.Finish();
            if (!array.ok()) {
                return iceberg::IOError(array.status().ToString());
            }
            auto valueArray = values.Finish();
            if (!valueArray.ok()) {
                return iceberg::IOError(valueArray.status().ToString());
            }
            auto batch = arrow::RecordBatch::Make(
                arrow::schema({arrow::field("value", arrow::int64()), arrow::field("key", arrow::utf8(), false)}),
                static_cast<int64_t>(keys.size()),
                {*valueArray, *array});
            const auto path =
                std::format("{}/data/{}-{}-{:05}.parquet", Location_, operationID, attemptID, added.size());
            ICEBERG_ASSIGN_OR_RAISE(auto data, WriteDataFile(IO_, attempt, *base->Metadata, path, {batch}));
            if (mutateAdded) {
                mutateAdded(*data.DataFile);
            }
            added.push_back(data.DataFile);
        }
        ICEBERG_ASSIGN_OR_RAISE(auto catalog, iceberg::InMemoryCatalog::Make("kv-test", IO_, Location_, {}));
        ICEBERG_ASSIGN_OR_RAISE(auto table,
            iceberg::Table::Make({.ns = {}, .name = "keys"},
                std::make_shared<iceberg::TableMetadata>(*base->Metadata),
                base->MetadataLocation,
                IO_,
                catalog));
        std::int64_t writtenRows = 0;
        std::int64_t removedRows = 0;
        for (const auto& file : added) {
            writtenRows += file->record_count;
        }
        for (const auto& file : removed) {
            removedRows += file->record_count;
        }
        const auto kind = !kindOverride.empty() ? std::string(kindOverride)
            : compact                           ? std::string("compact")
            : keyFiles.empty()                  ? std::string("delete")
                                                : std::string("upsert");
        const auto inputRows = compact ? 0 : kind == "delete" ? removedRows : writtenRows;
        const auto affected = affectedOverride.value_or(compact ? 0
                : kind == "delete"                              ? removedRows - writtenRows
                                                                : writtenRows);
        const std::unordered_map<std::string, std::string> summary{{"glamdb.operation-id", operationID},
            {"glamdb.content-hash", "fixture"},
            {"glamdb.kv-version", "1"},
            {"glamdb.kv-kind", kind},
            {"glamdb.kv-input-rows", std::to_string(inputRows)},
            {"glamdb.kv-affected-rows", std::to_string(affected)}};
        ICEBERG_ASSIGN_OR_RAISE(auto staged,
            rawAppend ? StageAppend(table, added.front(), summary, attempt)
                      : StageKVOverwrite(table, added, removed, summary, attempt, compact));
        const auto& transaction = staged.Transaction;
        const auto root = std::format("{}/metadata/{:05}-{}.metadata.json", Location_, ordinal, attemptID);
        ICEBERG_RETURN_UNEXPECTED(iceberg::TableMetadataUtil::Write(*IO_, root, transaction->current()));
        TStorageLimits limits;
        if (metadataBudgetSlack) {
            ICEBERG_ASSIGN_OR_RAISE(auto snapshot, transaction->current().Snapshot());
            ICEBERG_ASSIGN_OR_RAISE(auto listInput, IO_->NewInputFile(snapshot->manifest_list));
            ICEBERG_ASSIGN_OR_RAISE(auto listBytes, listInput->Size());
            ICEBERG_ASSIGN_OR_RAISE(auto list, iceberg::ManifestListReader::Make(snapshot->manifest_list, IO_));
            ICEBERG_ASSIGN_OR_RAISE(auto manifests, list->Files());
            auto budget = static_cast<std::int64_t>(attempt->Record(root).Value().Length) + listBytes;
            for (const auto& old : base->CurrentManifests) {
                if (std::ranges::none_of(manifests, [&](const auto& current) {
                        return current.manifest_path == old.manifest_path;
                    }))
                {
                    budget += old.manifest_length;
                }
            }
            for (const auto& current : manifests) {
                if (std::ranges::none_of(base->CurrentManifests, [&](const auto& old) {
                        return current.manifest_path == old.manifest_path;
                    }))
                {
                    budget += current.manifest_length;
                }
            }
            limits.CommitBytes = static_cast<std::uint64_t>(budget + *metadataBudgetSlack);
        }
        TStoragePublicationGuard guard({.Directories = Dirs_,
            .FileIO = IO_,
            .Attempt = attempt,
            .Namespace = "test",
            .Name = "keys",
            .Profile = Profile_,
            .Base = base,
            .ExpectedSummary = summary,
            .StoreUsage = base->Usage,
            .Limits = limits});
        ICEBERG_RETURN_UNEXPECTED(
            guard.BeforePointerChange({EPointerOp::UpdateLocation, "test", "keys", base->MetadataLocation, root}));
        return guard.GetCandidate();
    }
};

TEST_F(TKVClosureTest, MaintenanceGuardRejectsKVBeforeCandidateFileIO)
{
    auto attempt = NIO::TIOAttempt::Make(
        {StoreID, CollectionID, "33333333-3333-7333-8333-333333333399", NIO::EAttemptKind::Maintenance, {}},
        Dirs_->Latch());
    ASSERT_TRUE(attempt);
    auto io = std::make_shared<glamdb::test::TMemoryFileIO>();
    std::size_t reads = 0;
    io->OnRead = [&](const auto&) {
        ++reads;
    };
    TStoragePublicationGuard guard({.Directories = Dirs_,
        .FileIO = io,
        .Attempt = attempt.Value(),
        .Namespace = "test",
        .Name = "keys",
        .Profile = Profile_,
        .Base = Empty_,
        .RetentionPolicy = TRetentionPolicy{1, 0}});
    const auto status = guard.BeforePointerChange({EPointerOp::UpdateLocation,
        "test",
        "keys",
        Empty_->MetadataLocation,
        Location_ + "/metadata/00001-33333333-3333-7333-8333-333333333399.metadata.json"});
    ASSERT_FALSE(status);
    EXPECT_NE(status.error().message.find("KV"), std::string::npos);
    EXPECT_EQ(reads, 0U);
    EXPECT_FALSE(guard.GetCandidate());
}

TEST_F(TKVClosureTest, AcceptsOverwriteAndHistoricalRecovery)
{
    auto first = Change(Empty_, {{"a", "b", "c"}});
    ASSERT_TRUE(first) << first.error().message;
    auto oldFile = std::make_shared<iceberg::DataFile>(*(*first)->ScanIndex->Files.front().DataFile);
    auto second = Change(*first, {{"a", "c"}}, {oldFile}, false, false, {}, "delete");
    ASSERT_TRUE(second) << second.error().message;
    auto recovered = VerifyReachable(IO_, (*second)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage.DataFiles, 2U);
}

TEST_F(TKVClosureTest, RejectsUnknownMetricFieldsAndContradictoryColumnSizes)
{
    const auto unknown = Change(Empty_, {{"a", "b"}}, {}, false, false, {}, {}, {}, [](auto& file) {
        file.value_counts[999] = file.record_count;
    });
    ASSERT_FALSE(unknown);
    EXPECT_NE(unknown.error().message.find("unknown field ID"), std::string::npos);
    const auto sizes = Change(Empty_, {{"a", "b"}}, {}, false, false, {}, {}, {}, [](auto& file) {
        ++file.column_sizes[1];
    });
    ASSERT_FALSE(sizes);
    EXPECT_NE(sizes.error().message.find("column sizes differ"), std::string::npos);
}

TEST_F(TKVClosureTest, CountsRewrittenInputsAtTheExactMetadataVerificationBudget)
{
    auto first = Change(Empty_, {{"a", "b"}});
    ASSERT_TRUE(first) << first.error().message;
    auto old = std::make_shared<iceberg::DataFile>(*(*first)->ScanIndex->Files.front().DataFile);
    auto refused = Change(*first, {{"a", "b", "c"}}, {old}, false, false, {}, {}, {}, {}, -1);
    ASSERT_FALSE(refused);
    EXPECT_NE(refused.error().message.find("CAPACITY"), std::string::npos);
    auto exact = Change(*first, {{"a", "b", "c"}}, {old}, false, false, {}, {}, {}, {}, 0);
    ASSERT_TRUE(exact) << exact.error().message;
    EXPECT_GT((*exact)->Usage.ManifestBytes, (*first)->Usage.ManifestBytes);
    EXPECT_EQ((*exact)->Usage.DataFiles, 2U);
}

TEST_F(TKVClosureTest, RecoveryRetainsOldOriginsAndIntervalsAcrossSeveralManifestRewrites)
{
    auto state = Change(Empty_, {{"a", "b"}, {"z"}});
    ASSERT_TRUE(state) << state.error().message;
    const auto first = *state;
    for (int sequence = 2; sequence <= 12; ++sequence) {
        const auto current = std::ranges::find_if((*state)->ScanIndex->Files, [](const auto& file) {
            return !file.RemovedSequence && file.RecordCount == 2;
        });
        ASSERT_NE(current, (*state)->ScanIndex->Files.end());
        auto old = std::make_shared<iceberg::DataFile>(*current->DataFile);
        state = Change(*state, {{"a", "b"}}, {old}, false, false, sequence);
        ASSERT_TRUE(state) << state.error().message;
    }
    auto recovered = VerifyReachable(IO_, (*state)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    ASSERT_EQ(recovered->ScanIndex->Files.size(), 13U);
    EXPECT_EQ(recovered->Usage, (*state)->Usage);
    EXPECT_EQ(recovered->RetainedManifestPaths, (*state)->RetainedManifestPaths);
    EXPECT_EQ(recovered->RetainedManifestPaths.size(), recovered->Usage.Manifests);
    EXPECT_EQ(recovered->CurrentCommitMetadataBytes, (*state)->CurrentCommitMetadataBytes);
    EXPECT_GT(recovered->CurrentCommitMetadataBytes, 0U);
    EXPECT_TRUE(recovered->ExpiredSnapshotIDs.empty());
    ASSERT_EQ(recovered->ScanIndex->Snapshots.size(), 12U);
    for (std::size_t offset = 0; offset < recovered->ScanIndex->Files.size(); ++offset) {
        const auto& actual = recovered->ScanIndex->Files[offset];
        const auto& expected = (*state)->ScanIndex->Files[offset];
        EXPECT_EQ(actual.IntroducedSequence, expected.IntroducedSequence);
        EXPECT_EQ(actual.RemovedSequence, expected.RemovedSequence);
        ASSERT_TRUE(actual.DataFile);
        EXPECT_EQ(*actual.DataFile, *expected.DataFile);
    }
    EXPECT_FALSE(first->ScanIndex->Files.front().RemovedSequence);
    EXPECT_EQ(first->ScanIndex->Snapshots.size(), 1U);
    auto smallWorkspace = TStorageLimits{};
    smallWorkspace.RecoveryWorkspaceBytes = 8192;
    const auto refused = VerifyReachable(IO_, (*state)->MetadataLocation, StoreID, CollectionID, smallWorkspace);
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().kind, iceberg::ErrorKind::kValidationFailed);
    EXPECT_EQ(refused.error().message, "Recovery metadata exceeds storage capacity.");
}

TEST_F(TKVClosureTest, RejectsUnsortedAndDuplicatePhysicalKeys)
{
    EXPECT_FALSE(Change(Empty_, {{"b", "a"}}));
    EXPECT_FALSE(Change(Empty_, {{"a", "a"}}));
}

TEST_F(TKVClosureTest, RejectsCompactionThatChangesTheRows)
{
    auto first = Change(Empty_, {{"a", "b"}, {"c", "d"}, {"m", "n"}, {"y", "z"}});
    ASSERT_TRUE(first) << first.error().message;
    std::vector<std::shared_ptr<iceberg::DataFile>> removed;
    for (const auto& file : (*first)->ScanIndex->Files) {
        removed.push_back(std::make_shared<iceberg::DataFile>(*file.DataFile));
    }
    EXPECT_FALSE(Change(*first, {{"a", "b", "c", "e", "m", "n", "y", "z"}}, removed, true));
    // Identical keys and row counts still cannot conceal changed values or nulls.
    EXPECT_FALSE(Change(*first, {{"a", "b", "c", "d", "m", "n", "y", "z"}}, removed, true, false, 999));
}

TEST_F(TKVClosureTest, DeletingAllFilesRetainsOldIntervalsAndPhysicalUsage)
{
    auto first = Change(Empty_, {{"a", "b"}, {"m", "z"}});
    ASSERT_TRUE(first) << first.error().message;
    std::vector<std::shared_ptr<iceberg::DataFile>> removed;
    for (const auto& file : (*first)->ScanIndex->Files) {
        removed.push_back(std::make_shared<iceberg::DataFile>(*file.DataFile));
    }
    auto empty = Change(*first, {}, removed);
    ASSERT_TRUE(empty) << empty.error().message;
    EXPECT_TRUE((*empty)->ScanIndex->KeyValue);
    EXPECT_EQ((*empty)->ScanIndex->Files.size(), 2U);
    EXPECT_EQ((*empty)->Usage.DataFiles, 2U);
    EXPECT_EQ((*empty)->ScanIndex->Files[0].IntroducedSequence, 1);
    EXPECT_EQ((*empty)->ScanIndex->Files[0].RemovedSequence, 2);
    EXPECT_FALSE((*first)->ScanIndex->Files[0].RemovedSequence);
    auto recovered = VerifyReachable(IO_, (*empty)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->ScanIndex->Files[1].RemovedSequence, 2);
    EXPECT_EQ(recovered->Usage.DataFiles, 2U);
}

TEST_F(TKVClosureTest, RejectsOverlappingLiveRanges)
{
    EXPECT_FALSE(Change(Empty_, {{"a", "m"}, {"k", "z"}}));
}

TEST_F(TKVClosureTest, CompactsContiguousFilesWithOneValuePreservingSnapshot)
{
    auto first = Change(Empty_, {{"a", "b"}, {"c", "d"}, {"m", "n"}, {"y", "z"}});
    ASSERT_TRUE(first) << first.error().message;
    std::vector<std::shared_ptr<iceberg::DataFile>> removed;
    for (const auto& file : (*first)->ScanIndex->Files) {
        removed.push_back(std::make_shared<iceberg::DataFile>(*file.DataFile));
    }
    auto compacted = Change(*first, {{"a", "b", "c", "d", "m", "n", "y", "z"}}, removed, true);
    ASSERT_TRUE(compacted) << compacted.error().message;
    EXPECT_EQ((*compacted)->Usage.DataFiles, 5U);
    EXPECT_EQ((*compacted)->ScanIndex->Files[4].IntroducedSequence, 2);
    EXPECT_EQ((*compacted)->ScanIndex->Snapshots.size(), 2U);
    auto recovered = VerifyReachable(IO_, (*compacted)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage.DataFiles, 5U);
}

TEST_F(TKVClosureTest, RawAppendStagerRejectsKVCollections)
{
    EXPECT_FALSE(Change(Empty_, {{"a", "b"}}, {}, false, true));
}

TEST_F(TKVClosureTest, RejectsCompactionAcrossAnUntouchedRange)
{
    auto first = Change(Empty_, {{"a", "b"}, {"c", "d"}, {"m", "n"}, {"u", "v"}, {"y", "z"}});
    ASSERT_TRUE(first) << first.error().message;
    std::vector<std::shared_ptr<iceberg::DataFile>> removed;
    for (std::size_t offset = 0; offset < (*first)->ScanIndex->Files.size(); ++offset) {
        if (offset != 2) {
            removed.push_back(std::make_shared<iceberg::DataFile>(*(*first)->ScanIndex->Files[offset].DataFile));
        }
    }
    const auto rejected = Change(*first, {{"a", "b", "c", "d", "u", "v", "y", "z"}}, removed, true);
    ASSERT_FALSE(rejected);
    EXPECT_NE(rejected.error().message.find("untouched key range"), std::string::npos);
}

TEST_F(TKVClosureTest, UpsertCannotLoseRowsOrInventMoreInsertionsThanAffectedKeys)
{
    auto first = Change(Empty_, {{"a", "b", "c"}});
    ASSERT_TRUE(first) << first.error().message;
    auto file = std::make_shared<iceberg::DataFile>(*(*first)->ScanIndex->Files.front().DataFile);
    EXPECT_FALSE(Change(*first, {{"a", "c"}}, {file}));
    EXPECT_FALSE(Change(Empty_, {{"a", "b", "c"}}, {}, false, false, {}, {}, 1));
}

}
}
