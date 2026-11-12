#include "core/api/codec.h"
#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"

#include "tests/vertical/vertical_support.h"
#include <thread>

namespace glamdb {
namespace {
using namespace vertical;

class KVMigrationFault : public ::testing::Test
{
protected:
    test::JournalFaultVfs Vfs_;
    test::TempDir Temporary_;
    std::shared_ptr<Engine> Engine_;
    MigrationRequest Request_;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [](const std::filesystem::path& path,
                const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
            return detail::TLocalStorageBackend::Open(path, store);
        };
        auto opened = detail::EngineFactory::CreateWith({test::store_id, (Temporary_.path / "store").string()},
            factory,
            Vfs_.Name());
        ASSERT_TRUE(opened);
        Engine_ = *opened;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "source", Schema}).disposition,
            CommitDisposition::Committed);
        const auto appended = Engine_->AppendDurable(
            {test::collection_id, FirstOperation, {}, {Batch({{"key", 7, std::nullopt, std::nullopt, std::nullopt}})}});
        ASSERT_EQ(appended.disposition, CommitDisposition::Committed);
        ASSERT_TRUE(appended.result);
        Request_.source_collection_id = test::collection_id;
        Request_.source_snapshot = SnapshotSelector::Id(test::Present(appended.result).snapshot_id);
        Request_.target = {"11111111-1111-4111-8111-111111111112", "target", Schema};
        Request_.target.mode = CollectionMode::KeyValue;
        Request_.operation_id = FirstOperation;
        Request_.duplicate_policy = DuplicatePolicy::LastWins;
    }
};

TEST_F(KVMigrationFault, TargetCreationLostReplyStaysUnknownOnExactRetryBeforeReconcile)
{
    Vfs_.Arm(1, true);
    const auto first = Engine_->MigrateAppendToKV(Request_);
    ASSERT_EQ(first.creation.disposition, CommitDisposition::CommitUnknown)
        << (first.creation.error ? test::Present(first.creation.error).message : "");
    ASSERT_TRUE(first.creation.recovery);
    EXPECT_FALSE(first.mutation);
    const auto retry = Engine_->MigrateAppendToKV(Request_);
    EXPECT_EQ(retry.creation.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(retry.creation.recovery);
    EXPECT_EQ(test::Present(retry.creation.recovery).attempt_id, test::Present(first.creation.recovery).attempt_id);
    EXPECT_EQ(test::Present(retry.creation.recovery).candidate_metadata_location,
        test::Present(first.creation.recovery).candidate_metadata_location);
    EXPECT_FALSE(retry.mutation);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto resumed = Engine_->MigrateAppendToKV(Request_);
    ASSERT_EQ(resumed.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(resumed.mutation);
    EXPECT_EQ(test::Present(resumed.mutation).disposition, CommitDisposition::Committed)
        << (test::Present(resumed.mutation).error ? test::Present(test::Present(resumed.mutation).error).message : "");
}

TEST_F(KVMigrationFault, PopulationLostReplyStaysUnknownOnExactRetryBeforeReconcile)
{
    Vfs_.Arm(3, true); // Target creation, operation binding, population COMMIT.
    const auto first = Engine_->MigrateAppendToKV(Request_);
    ASSERT_EQ(first.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.mutation);
    ASSERT_EQ(test::Present(first.mutation).disposition, CommitDisposition::CommitUnknown)
        << (test::Present(first.mutation).error ? test::Present(test::Present(first.mutation).error).message : "");
    ASSERT_TRUE(test::Present(first.mutation).recovery);
    const auto retry = Engine_->MigrateAppendToKV(Request_);
    EXPECT_EQ(retry.creation.disposition, CommitDisposition::Committed);
    EXPECT_EQ(retry.creation.info, first.creation.info);
    ASSERT_TRUE(retry.mutation);
    EXPECT_EQ(test::Present(retry.mutation).disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(test::Present(retry.mutation).recovery);
    EXPECT_EQ(test::Present(test::Present(retry.mutation).recovery).attempt_id,
        test::Present(test::Present(first.mutation).recovery).attempt_id);
    EXPECT_EQ(test::Present(test::Present(retry.mutation).recovery).candidate_snapshot_id,
        test::Present(test::Present(first.mutation).recovery).candidate_snapshot_id);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto replay = Engine_->MigrateAppendToKV(Request_);
    ASSERT_EQ(replay.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(replay.mutation);
    ASSERT_EQ(test::Present(replay.mutation).disposition, CommitDisposition::Committed);
    const auto stored = Engine_->FindAppendOutcome(Request_.target.collection_id, Request_.operation_id);
    ASSERT_TRUE(stored);
    ASSERT_TRUE(*stored);
    EXPECT_EQ(test::Present(replay.mutation), test::Present(*stored));
    const auto lookup = Engine_->Lookup({Request_.target.collection_id, {"key"}});
    ASSERT_TRUE(lookup);
    ASSERT_TRUE(lookup->rows[0]);
    EXPECT_EQ(std::get<int64_t>(test::Present(lookup->rows[0])[1]), 7);
}

TEST_F(KVMigrationFault, PreexistingEmptyTargetCannotBeClaimedByMigration)
{
    const auto created = Engine_->CreateCollection(Request_.target);
    ASSERT_EQ(created.disposition, CommitDisposition::Committed);
    const auto rejected = Engine_->MigrateAppendToKV(Request_);
    EXPECT_EQ(rejected.creation.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.creation.error);
    EXPECT_EQ(test::Present(rejected.creation.error).code, "COLLECTION_EXISTS");
    EXPECT_FALSE(rejected.mutation);
    const auto target = Engine_->GetCollection(Request_.target.collection_id);
    ASSERT_TRUE(target);
    EXPECT_FALSE(target->current_snapshot_id);
    const auto operation = Engine_->FindAppendOutcome(Request_.target.collection_id, Request_.operation_id);
    ASSERT_TRUE(operation);
    EXPECT_FALSE(*operation);
}

TEST_F(KVMigrationFault, PopulationOperationIdCannotBeClaimedByAnOrdinaryWrite)
{
    auto cancelled = std::make_shared<std::atomic<bool>>(true);
    Request_.cancellation = cancelled;
    const auto first = Engine_->MigrateAppendToKV(Request_);
    ASSERT_EQ(first.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.mutation);
    ASSERT_EQ(test::Present(first.mutation).disposition, CommitDisposition::DefinitelyNotCommitted);
    cancelled->store(false);
    const auto unrelated = Engine_->UpsertDurable({Request_.target.collection_id,
        Request_.operation_id,
        {},
        {Batch({{"wrong", 99, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(unrelated.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(unrelated.error);
    EXPECT_EQ(test::Present(unrelated.error).code, "OPERATION_CONFLICT");
    const auto migrated = Engine_->MigrateAppendToKV(Request_);
    ASSERT_TRUE(migrated.mutation);
    ASSERT_EQ(test::Present(migrated.mutation).disposition, CommitDisposition::Committed);
    const auto rows = Engine_->Lookup({Request_.target.collection_id, {"key", "wrong"}});
    ASSERT_TRUE(rows);
    ASSERT_TRUE(rows->rows[0]);
    EXPECT_EQ(std::get<int64_t>(test::Present(rows->rows[0])[1]), 7);
    EXPECT_FALSE(rows->rows[1]);
}

TEST_F(KVMigrationFault, BusyRetryDoesNotInventAPopulationRecoveryIdentity)
{
    const auto first = Engine_->MigrateAppendToKV(Request_);
    ASSERT_TRUE(first.mutation);
    ASSERT_EQ(test::Present(first.mutation).disposition, CommitDisposition::Committed);
    Vfs_.PauseBeforeDelete(1);
    std::thread writer([&] {
        Engine_->DeleteDurable({Request_.target.collection_id, SecondOperation, {}, {"missing"}});
    });
    const bool paused = Vfs_.WaitUntilPaused(std::chrono::seconds(5));
    if (paused) {
        const auto retry = Engine_->MigrateAppendToKV(Request_);
        EXPECT_EQ(retry.creation.disposition, CommitDisposition::Committed);
        if (retry.mutation) {
            EXPECT_TRUE(detail::ValidateOutcome(test::Present(retry.mutation)));
        }
        EXPECT_FALSE(retry.mutation);
        EXPECT_TRUE(retry.resolution_error);
    }
    Vfs_.Release();
    writer.join();
    ASSERT_TRUE(paused);
    const auto retry = Engine_->MigrateAppendToKV(Request_);
    ASSERT_TRUE(retry.mutation);
    EXPECT_EQ(test::Present(retry.mutation), test::Present(first.mutation));
}

TEST_F(KVMigrationFault, UnrelatedRecoveryBarrierDoesNotInventAPopulationIdentity)
{
    const auto first = Engine_->MigrateAppendToKV(Request_);
    ASSERT_TRUE(first.mutation);
    ASSERT_EQ(test::Present(first.mutation).disposition, CommitDisposition::Committed);
    Vfs_.Arm(2, true);
    const auto unrelated = Engine_->DeleteDurable({Request_.target.collection_id, SecondOperation, {}, {"missing"}});
    ASSERT_EQ(unrelated.disposition, CommitDisposition::CommitUnknown);
    const auto blocked = Engine_->MigrateAppendToKV(Request_);
    EXPECT_EQ(blocked.creation.disposition, CommitDisposition::Committed);
    if (blocked.mutation) {
        EXPECT_TRUE(detail::ValidateOutcome(test::Present(blocked.mutation)));
    }
    EXPECT_FALSE(blocked.mutation);
    EXPECT_TRUE(blocked.resolution_error);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto retry = Engine_->MigrateAppendToKV(Request_);
    ASSERT_TRUE(retry.mutation);
    EXPECT_EQ(test::Present(retry.mutation), test::Present(first.mutation));
}

} // namespace
} // namespace glamdb
