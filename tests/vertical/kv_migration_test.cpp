#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

namespace glamdb {
namespace {
using namespace vertical;

TEST(KVMigration, RequiresAnExplicitPolicyAndFixedSourceBeforeCreatingTarget)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    const auto engine = *opened;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "source", Schema}).disposition,
        CommitDisposition::Committed);
    MigrationRequest request;
    request.source_collection_id = test::collection_id;
    request.target = {"11111111-1111-4111-8111-111111111112", "target", Schema};
    request.target.mode = CollectionMode::KeyValue;
    request.operation_id = FirstOperation;
    auto outcome = engine->MigrateAppendToKV(request);
    EXPECT_EQ(outcome.creation.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(outcome.mutation);
    EXPECT_FALSE(engine->GetCollection(request.target.collection_id));
    request.duplicate_policy = DuplicatePolicy::LastWins;
    outcome = engine->MigrateAppendToKV(request);
    EXPECT_EQ(outcome.creation.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(engine->GetCollection(request.target.collection_id));
}

TEST(KVMigration, BoundsTheTargetNameBeforeEncodingIdentityOrReadingTheSource)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    MigrationRequest request;
    request.source_collection_id = test::collection_id;
    request.source_snapshot = SnapshotSelector::InitialEmpty();
    request.target = {"11111111-1111-4111-8111-111111111112", std::string(129, 'x'), Schema};
    request.target.mode = CollectionMode::KeyValue;
    request.operation_id = FirstOperation;
    request.duplicate_policy = DuplicatePolicy::LastWins;
    const auto rejected = (*opened)->MigrateAppendToKV(request);
    ASSERT_TRUE(rejected.creation.error);
    EXPECT_EQ(test::Present(rejected.creation.error).code, "INVALID_ARGUMENT");
    EXPECT_FALSE(rejected.mutation);
    EXPECT_FALSE((*opened)->GetCollection(request.target.collection_id));
}

TEST(KVMigration, LastWinsUsesFixedSourceOrderAndReplayReturnsOriginalReceipt)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    const auto engine = *opened;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "source", Schema}).disposition,
        CommitDisposition::Committed);
    auto source = engine->AppendDurable({test::collection_id,
        FirstOperation,
        {},
        {Batch({{"a", 1, std::nullopt, std::nullopt, std::nullopt},
            {"b", 2, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(source.disposition, CommitDisposition::Committed);
    source = engine->AppendDurable(
        {test::collection_id, SecondOperation, {}, {Batch({{"a", 9, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(source.disposition, CommitDisposition::Committed);
    MigrationRequest request;
    request.source_collection_id = test::collection_id;
    request.source_snapshot = SnapshotSelector::Id(test::Present(source.result).snapshot_id);
    request.target = {"11111111-1111-4111-8111-111111111112", "target", Schema};
    request.target.mode = CollectionMode::KeyValue;
    request.operation_id = FirstOperation;
    request.duplicate_policy = DuplicatePolicy::LastWins;
    const auto migrated = engine->MigrateAppendToKV(request);
    ASSERT_EQ(migrated.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(migrated.mutation);
    ASSERT_EQ(test::Present(migrated.mutation).disposition, CommitDisposition::Committed)
        << (test::Present(migrated.mutation).error ? test::Present(test::Present(migrated.mutation).error).message
                                                   : "");
    const auto rows = engine->Lookup({request.target.collection_id, {"a", "b"}});
    ASSERT_TRUE(rows);
    EXPECT_EQ(std::get<int64_t>(test::Present(rows->rows[0])[1]), 9);
    EXPECT_EQ(std::get<int64_t>(test::Present(rows->rows[1])[1]), 2);
    const auto retried = engine->MigrateAppendToKV(request);
    ASSERT_TRUE(retried.mutation);
    EXPECT_EQ(test::Present(retried.mutation), test::Present(migrated.mutation));
    auto cancelled = std::make_shared<std::atomic<bool>>(true);
    request.cancellation = cancelled;
    request.timeout = std::chrono::milliseconds(1);
    const auto cancelledRetry = engine->MigrateAppendToKV(request);
    ASSERT_TRUE(cancelledRetry.mutation);
    EXPECT_EQ(test::Present(cancelledRetry.mutation), test::Present(migrated.mutation));
    auto owner = engine->GetResourceOwner();
    const auto data = static_cast<size_t>(ResourceClass::Data);
    auto pressure = owner->Reserve(ResourceClass::Data,
        owner->GetLimits().memory_bytes[data] - owner->GetUsage().memory_bytes[data]);
    ASSERT_TRUE(pressure);
    const auto pressureRetry = engine->MigrateAppendToKV(request);
    ASSERT_TRUE(pressureRetry.mutation);
    EXPECT_EQ(test::Present(pressureRetry.mutation), test::Present(migrated.mutation));

    EXPECT_EQ(engine->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
    EXPECT_EQ(engine->ListSnapshots(request.target.collection_id)->snapshots.size(), 1U);
}
} // namespace
} // namespace glamdb
