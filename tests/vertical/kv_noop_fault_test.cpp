#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

namespace glamdb {
namespace {
using namespace vertical;

TEST(KVNoOpFault, LostCommitReplyNeedsRecoveryDespiteHavingNoCandidateSnapshot)
{
    test::JournalFaultVfs vfs;
    test::TempDir temporary;
    const auto root = (temporary.path / "store").string();
    const detail::BackendFactory factory =
        [](const std::filesystem::path& path,
            const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
        return detail::TLocalStorageBackend::Open(path, store);
    };
    auto created = detail::EngineFactory::CreateWith({test::store_id, root}, factory, vfs.Name());
    ASSERT_TRUE(created);
    auto engine = *created;
    CreateCollectionRequest collection{test::collection_id, "keys", Schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    vfs.Arm(2, true); // Bind succeeds; durable receipt COMMIT loses its reply.
    const DeleteRequest request{test::collection_id, FirstOperation, {}, {"missing", "missing"}};
    const auto uncertain = engine->DeleteDurable(request);
    ASSERT_EQ(uncertain.disposition, CommitDisposition::CommitUnknown)
        << (uncertain.error ? test::Present(uncertain.error).message : "");
    ASSERT_TRUE(uncertain.recovery);
    EXPECT_FALSE(test::Present(uncertain.recovery).candidate_snapshot_id);
    EXPECT_FALSE(test::Present(uncertain.recovery).candidate_metadata_location);
    EXPECT_EQ(engine->GetHealth().state, StoreState::RecoveryRequired);
    ASSERT_TRUE(engine->Reconcile());
    const auto replay = engine->DeleteDurable(request);
    ASSERT_EQ(replay.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(replay.result);
    EXPECT_FALSE(test::Present(replay.result).snapshot_created);
    EXPECT_EQ(test::Present(replay.result).snapshot_id, 0);
    EXPECT_TRUE(engine->ListSnapshots(test::collection_id)->snapshots.empty());
    const auto inserted = engine->UpsertDurable({test::collection_id,
        SecondOperation,
        {},
        {Batch({{"missing", 5, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed);
    EXPECT_EQ(engine->DeleteDurable(request), replay);
    ASSERT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
    engine.reset();
    created->reset();
    auto reopened = OpenExisting({root, test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    EXPECT_EQ((*reopened)->DeleteDurable(request), replay);
    const auto history = (*reopened)->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    EXPECT_EQ(history->snapshots.size(), 1U);
    const auto lookup = (*reopened)->Lookup({test::collection_id, {"missing"}});
    ASSERT_TRUE(lookup);
    ASSERT_TRUE(lookup->rows[0]);
    EXPECT_EQ(std::get<int64_t>(test::Present(lookup->rows[0])[1]), 5);
}

TEST(KVNoOpFault, ReceiptSurvivesNoOpCommitRollbackWithoutMovingTheBase)
{
    test::JournalFaultVfs vfs;
    test::TempDir temporary;
    const detail::BackendFactory factory =
        [](const std::filesystem::path& path,
            const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
        return detail::TLocalStorageBackend::Open(path, store);
    };
    auto created =
        detail::EngineFactory::CreateWith({test::store_id, (temporary.path / "store").string()}, factory, vfs.Name());
    ASSERT_TRUE(created);
    auto engine = *created;
    CreateCollectionRequest collection{test::collection_id, "keys", Schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    vfs.Arm(2, false);
    const auto failed = engine->DeleteDurable({test::collection_id, FirstOperation, {}, {"absent"}});
    ASSERT_EQ(failed.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(engine->Reconcile());
    const auto recorded = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(recorded);
    ASSERT_TRUE(*recorded);
    EXPECT_EQ((test::Present(*recorded)).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_TRUE(engine->ListSnapshots(test::collection_id)->snapshots.empty());
}
TEST(KVNoOpFault, DeleteRetriesResolveBeforeTemporaryMemoryAdmission)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    CreateStoreRequest create{test::store_id, (temporary.path / "store").string()};
    create.resources = owner;
    auto opened = CreateStore(create);
    ASSERT_TRUE(opened);
    auto engine = *opened;
    CreateCollectionRequest collection{test::collection_id, "keys", Schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    const DeleteRequest request{test::collection_id, FirstOperation, {}, {"absent", "absent", "other"}};
    const auto original = engine->DeleteDurable(request);
    ASSERT_EQ(original.disposition, CommitDisposition::Committed);
    const auto used = owner->GetUsage().memory_bytes[static_cast<size_t>(ResourceClass::Data)];
    auto pressure = owner->Reserve(ResourceClass::Data,
        owner->GetLimits().memory_bytes[static_cast<size_t>(ResourceClass::Data)] - used);
    ASSERT_TRUE(pressure);
    EXPECT_EQ(engine->DeleteDurable(request), original);
    auto conflict = request;
    conflict.keys.pop_back();
    const auto refused = engine->DeleteDurable(conflict);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "OPERATION_CONFLICT");
}

} // namespace
} // namespace glamdb
