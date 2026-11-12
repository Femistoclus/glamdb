#include "core/api/codec.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <format>

namespace glamdb {
namespace {
using namespace std::chrono_literals;
using namespace vertical;

class KVMigrationPolicy : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        const auto opened = CreateStore({test::store_id, Root_.string()});
        ASSERT_TRUE(opened) << opened.error().message;
        Engine_ = *opened;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "policy-source", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    MigrationRequest Request(int64_t snapshot, DuplicatePolicy policy)
    {
        MigrationRequest request;
        request.source_collection_id = test::collection_id;
        request.source_snapshot = SnapshotSelector::Id(snapshot);
        request.target = {SecondCollection, "policy-target", Schema};
        request.target.mode = CollectionMode::KeyValue;
        request.operation_id = FirstOperation;
        request.duplicate_policy = policy;
        return request;
    }

    void Reopen()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
        const auto opened = OpenExisting({Root_.string(), test::store_id});
        ASSERT_TRUE(opened) << opened.error().message;
        Engine_ = *opened;
    }

    void ExpectRows(std::string_view collection, SnapshotSelector snapshot, const std::vector<TInputRow>& expected)
    {
        const auto cursor = Engine_->Scan({.collection_id = std::string(collection), .selector = snapshot});
        ASSERT_TRUE(cursor) << cursor.error().message;
        std::vector<std::string> actual;
        for (;;) {
            const auto batch = (*cursor)->Next();
            ASSERT_TRUE(batch) << batch.error().message;
            if (!*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            actual.insert(actual.end(), rendered.begin(), rendered.end());
        }
        EXPECT_EQ(Sorted(std::move(actual)), Expected({&expected}));
    }

    void ExpectRejectedPopulation(const MigrationRequest& request, const MigrationOutcome& rejected)
    {
        ASSERT_EQ(rejected.creation.disposition, CommitDisposition::Committed);
        ASSERT_TRUE(rejected.creation.info);
        EXPECT_EQ(test::Present(rejected.creation.info).mode, CollectionMode::KeyValue);
        ASSERT_TRUE(rejected.mutation);
        ASSERT_EQ(test::Present(rejected.mutation).disposition, CommitDisposition::DefinitelyNotCommitted);
        EXPECT_FALSE(test::Present(rejected.mutation).result);
        ASSERT_TRUE(test::Present(rejected.mutation).error);
        EXPECT_EQ(test::Present(test::Present(rejected.mutation).error).code, "INVALID_ARGUMENT");
        ASSERT_TRUE(detail::ValidateOutcome(test::Present(rejected.mutation)));
        const auto recorded = Engine_->FindAppendOutcome(SecondCollection, FirstOperation);
        ASSERT_TRUE(recorded);
        ASSERT_TRUE(*recorded);
        EXPECT_EQ(test::Present(*recorded), test::Present(rejected.mutation));
        const auto target = Engine_->GetCollection(SecondCollection);
        ASSERT_TRUE(target);
        EXPECT_FALSE(target->current_snapshot_id);
        const auto history = Engine_->ListSnapshots(SecondCollection);
        ASSERT_TRUE(history);
        EXPECT_TRUE(history->snapshots.empty());
        const auto lookup = Engine_->Lookup({SecondCollection, {"duplicate", "other", "absent", "duplicate"}});
        ASSERT_TRUE(lookup);
        EXPECT_EQ(lookup->snapshot, SnapshotSelector::InitialEmpty());
        ASSERT_EQ(lookup->rows.size(), 4U);
        EXPECT_TRUE(std::ranges::all_of(lookup->rows, [](const auto& row) {
            return !row;
        }));
        const auto retry = Engine_->MigrateAppendToKV(request);
        EXPECT_EQ(retry.creation.disposition, CommitDisposition::Committed);
        EXPECT_EQ(retry.creation.info, rejected.creation.info);
        ASSERT_TRUE(retry.mutation);
        EXPECT_EQ(test::Present(retry.mutation), test::Present(rejected.mutation));
        EXPECT_FALSE(retry.resolution_error);
    }
};

TEST_F(KVMigrationPolicy, FirstWinsPreservesTheFirstFullRowAcrossChunksAndAppendSnapshots)
{
    const TInputRow first{"duplicate", 1, 1.5, true, 10};
    const TInputRow nullFirst{"null-first", std::nullopt, std::nullopt, std::nullopt, std::nullopt};
    std::vector<TInputRow> expected{first, nullFirst};
    for (int64_t index = 0; index < 1000; ++index) {
        expected.push_back({std::format("f{:04}", index), index, std::nullopt, std::nullopt, std::nullopt});
    }
    auto original = expected;
    original.push_back({"duplicate", 2, 2.5, false, 20});
    original.push_back({"null-first", 2, 2.5, false, 20});
    const auto firstAppend = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(original)}});
    ASSERT_EQ(firstAppend.disposition, CommitDisposition::Committed);
    const std::vector<TInputRow> later{{"duplicate", 3, 3.5, false, 30},
        {"null-first", 3, 3.5, false, 30},
        {"late", 4, 4.5, true, 40}};
    expected.push_back(later.back());
    const auto secondAppend = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(later)}});
    ASSERT_EQ(secondAppend.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(secondAppend.result);
    const auto request = Request(test::Present(secondAppend.result).snapshot_id, DuplicatePolicy::FirstWins);
    const auto newLatest = Engine_->AppendDurable({test::collection_id,
        ThirdOperation,
        {},
        {Batch({{"duplicate", 99, 99.5, false, 99}, {"excluded", 99, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(newLatest.disposition, CommitDisposition::Committed);
    const auto migrated = Engine_->MigrateAppendToKV(request);
    ASSERT_EQ(migrated.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(migrated.mutation);
    ASSERT_EQ(test::Present(migrated.mutation).disposition, CommitDisposition::Committed)
        << (test::Present(migrated.mutation).error ? test::Present(test::Present(migrated.mutation).error).message
                                                   : "");
    ASSERT_TRUE(test::Present(migrated.mutation).result);
    const auto& receipt = test::Present(test::Present(migrated.mutation).result);
    EXPECT_EQ(receipt.receipt_version, 3);
    EXPECT_EQ(receipt.mutation_kind, MutationKind::Upsert);
    EXPECT_TRUE(receipt.snapshot_created);
    EXPECT_FALSE(receipt.parent_snapshot_id);
    EXPECT_EQ(receipt.input_rows, static_cast<int64_t>(original.size() + later.size()));
    EXPECT_EQ(receipt.rows_affected, static_cast<int64_t>(expected.size()));
    EXPECT_EQ(receipt.rows_written, static_cast<int64_t>(expected.size()));
    EXPECT_EQ(receipt.rows_removed, 0);
    ASSERT_NO_FATAL_FAILURE(ExpectRows(SecondCollection, SnapshotSelector::Latest(), expected));
    const auto lookup = Engine_->Lookup({SecondCollection, {"duplicate", "null-first", "excluded", "duplicate"}});
    ASSERT_TRUE(lookup);
    ASSERT_EQ(lookup->rows.size(), 4U);
    const Row expectedFirst{std::string("duplicate"), int64_t{1}, 1.5, true, TimestampUs{10}};
    const Row expectedNull{std::string("null-first"),
        std::monostate{},
        std::monostate{},
        std::monostate{},
        std::monostate{}};
    EXPECT_EQ(lookup->rows[0], expectedFirst);
    EXPECT_EQ(lookup->rows[1], expectedNull);
    EXPECT_FALSE(lookup->rows[2]);
    EXPECT_EQ(lookup->rows[3], expectedFirst);
    auto source = original;
    source.insert(source.end(), later.begin(), later.end());
    ASSERT_NO_FATAL_FAILURE(ExpectRows(test::collection_id, request.source_snapshot, source));
    ASSERT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 3U);
    ASSERT_EQ(Engine_->ListSnapshots(SecondCollection)->snapshots.size(), 1U);
    ASSERT_NO_FATAL_FAILURE(Reopen());
    const auto retry = Engine_->MigrateAppendToKV(request);
    ASSERT_TRUE(retry.mutation);
    EXPECT_EQ(test::Present(retry.mutation), test::Present(migrated.mutation));
    EXPECT_FALSE(retry.resolution_error);
    ASSERT_NO_FATAL_FAILURE(ExpectRows(SecondCollection, SnapshotSelector::Latest(), expected));
    ASSERT_EQ(Engine_->ListSnapshots(SecondCollection)->snapshots.size(), 1U);
}

TEST_F(KVMigrationPolicy, RejectAcrossChunkBoundariesKeepsTheCreatedTargetEmptyAndFailurePermanent)
{
    std::vector<TInputRow> source{{"duplicate", 1, 1.5, true, 10}};
    for (int64_t index = 0; index < 1000; ++index) {
        source.push_back({std::format("f{:04}", index), index, std::nullopt, std::nullopt, std::nullopt});
    }
    source.push_back({"duplicate", 2, 2.5, false, 20});
    const auto appended = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(source)}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(appended.result);
    const auto request = Request(test::Present(appended.result).snapshot_id, DuplicatePolicy::Reject);
    const auto rejected = Engine_->MigrateAppendToKV(request);
    ASSERT_NO_FATAL_FAILURE(ExpectRejectedPopulation(request, rejected));
    ASSERT_NO_FATAL_FAILURE(ExpectRows(test::collection_id, request.source_snapshot, source));
    ASSERT_NO_FATAL_FAILURE(Reopen());
    ASSERT_NO_FATAL_FAILURE(ExpectRejectedPopulation(request, rejected));
    auto changedPolicy = request;
    changedPolicy.duplicate_policy = DuplicatePolicy::FirstWins;
    const auto conflict = Engine_->MigrateAppendToKV(changedPolicy);
    EXPECT_EQ(conflict.creation.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(conflict.mutation);
    ASSERT_TRUE(conflict.creation.error);
    EXPECT_EQ(test::Present(conflict.creation.error).code, "COLLECTION_EXISTS");
    ASSERT_NO_FATAL_FAILURE(ExpectRejectedPopulation(request, rejected));
}

TEST_F(KVMigrationPolicy, RejectDetectsDuplicatesInDifferentAppendSnapshots)
{
    const std::vector<TInputRow> first{{"duplicate", 1, 1.5, true, 10},
        {"other", 2, std::nullopt, std::nullopt, std::nullopt}};
    const auto inserted = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(first)}});
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed);
    const std::vector<TInputRow> duplicate{{"duplicate", 9, std::nullopt, std::nullopt, std::nullopt}};
    const auto appended = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(duplicate)}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(appended.result);
    const auto request = Request(test::Present(appended.result).snapshot_id, DuplicatePolicy::Reject);
    const auto rejected = Engine_->MigrateAppendToKV(request);
    ASSERT_NO_FATAL_FAILURE(ExpectRejectedPopulation(request, rejected));
    auto source = first;
    source.insert(source.end(), duplicate.begin(), duplicate.end());
    ASSERT_NO_FATAL_FAILURE(ExpectRows(test::collection_id, request.source_snapshot, source));
    ASSERT_NO_FATAL_FAILURE(Reopen());
    ASSERT_NO_FATAL_FAILURE(ExpectRejectedPopulation(request, rejected));
}

TEST_F(KVMigrationPolicy, RejectAcceptsAUniqueFixedSnapshotBeforeLaterDuplicates)
{
    const std::vector<TInputRow> unique{{"duplicate", 1, 1.5, true, 10},
        {"other", std::nullopt, std::nullopt, std::nullopt, std::nullopt}};
    const auto first = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(unique)}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.result);
    const auto request = Request(test::Present(first.result).snapshot_id, DuplicatePolicy::Reject);
    const auto later = Engine_->AppendDurable({test::collection_id,
        SecondOperation,
        {},
        {Batch({{"duplicate", 99, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(later.disposition, CommitDisposition::Committed);
    const auto migrated = Engine_->MigrateAppendToKV(request);
    ASSERT_EQ(migrated.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(migrated.mutation);
    ASSERT_EQ(test::Present(migrated.mutation).disposition, CommitDisposition::Committed)
        << (test::Present(migrated.mutation).error ? test::Present(test::Present(migrated.mutation).error).message
                                                   : "");
    ASSERT_TRUE(test::Present(migrated.mutation).result);
    EXPECT_EQ(test::Present(test::Present(migrated.mutation).result).input_rows, 2);
    EXPECT_EQ(test::Present(test::Present(migrated.mutation).result).rows_affected, 2);
    ASSERT_NO_FATAL_FAILURE(ExpectRows(SecondCollection, SnapshotSelector::Latest(), unique));
    ASSERT_NO_FATAL_FAILURE(Reopen());
    const auto retry = Engine_->MigrateAppendToKV(request);
    ASSERT_TRUE(retry.mutation);
    EXPECT_EQ(test::Present(retry.mutation), test::Present(migrated.mutation));
    ASSERT_NO_FATAL_FAILURE(ExpectRows(SecondCollection, SnapshotSelector::Latest(), unique));
    ASSERT_EQ(Engine_->ListSnapshots(SecondCollection)->snapshots.size(), 1U);
}

} // namespace
} // namespace glamdb
