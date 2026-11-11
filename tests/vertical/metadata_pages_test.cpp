#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

namespace glamdb {
namespace {

using namespace vertical;

class TMetadataPagesTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        const auto created = CreateStore({test::store_id, (Temporary_.path / "store").string()});
        ASSERT_TRUE(created);
        Engine_ = *created;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        EXPECT_TRUE(Engine_->Shutdown(std::chrono::seconds(1)));
    }
};

void CheckFixedViews(Engine& engine)
{
    ASSERT_EQ(engine.CreateCollection({SecondCollection, "second", Schema}).disposition, CommitDisposition::Committed);
    const auto first = engine.ListCollectionsPage({.PageSize = 1, .AuthorizationContext = "reader"});
    ASSERT_TRUE(first);
    ASSERT_EQ(first->Collections.size(), 1U);
    EXPECT_EQ(first->Collections.front().collection_id, test::collection_id);
    EXPECT_EQ(first->CreationHighWaterMark, 2);
    ASSERT_FALSE(first->NextPageToken.empty());
    const auto a = engine.AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    const auto b = engine.AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_TRUE(a.result);
    ASSERT_TRUE(b.result);
    const auto history = engine.ListSnapshotsPage(test::collection_id, {.PageSize = 1});
    const auto files = engine.ListFilesPage(test::collection_id, {.PageSize = 1});
    ASSERT_TRUE(history);
    ASSERT_TRUE(files);
    ASSERT_EQ(history->snapshots.size(), 1U);
    EXPECT_EQ(history->snapshots.front().receipt, test::Present(a.result));
    ASSERT_FALSE(history->NextPageToken.empty());
    ASSERT_FALSE(files->NextPageToken.empty());
    const auto c = engine.AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(FirstRows)}});
    ASSERT_TRUE(c.result);
    ASSERT_EQ(engine.CreateCollection({"00000000-0000-4000-8000-0000000000b3", "third", Schema}).disposition,
        CommitDisposition::Committed);
    const auto second = engine.ListCollectionsPage(
        {.PageSize = 1, .PageToken = first->NextPageToken, .AuthorizationContext = "reader"});
    ASSERT_TRUE(second);
    ASSERT_EQ(second->Collections.size(), 1U);
    EXPECT_EQ(second->Collections.front().collection_id, SecondCollection);
    EXPECT_EQ(second->CreationHighWaterMark, 2);
    EXPECT_TRUE(second->NextPageToken.empty());
    const auto nextHistory =
        engine.ListSnapshotsPage(test::collection_id, {.PageSize = 1, .PageToken = history->NextPageToken});
    ASSERT_TRUE(nextHistory);
    EXPECT_EQ(nextHistory->identity, history->identity);
    ASSERT_EQ(nextHistory->snapshots.size(), 1U);
    EXPECT_EQ(nextHistory->snapshots.front().receipt, test::Present(b.result));
    EXPECT_TRUE(nextHistory->NextPageToken.empty());
    const auto nextFiles =
        engine.ListFilesPage(test::collection_id, {.PageSize = 1, .PageToken = files->NextPageToken});
    ASSERT_TRUE(nextFiles);
    EXPECT_EQ(nextFiles->Identity, files->Identity);
    EXPECT_EQ(nextFiles->SelectorResolved, SnapshotSelector::Id(test::Present(b.result).snapshot_id));
    ASSERT_EQ(files->Files.size(), 1U);
    ASSERT_EQ(nextFiles->Files.size(), 1U);
    EXPECT_LT(files->Files.front().FileID, nextFiles->Files.front().FileID);
    EXPECT_TRUE(nextFiles->NextPageToken.empty());
    const auto replay = engine.ListFilesPage(test::collection_id, {.PageSize = 1, .PageToken = files->NextPageToken});
    ASSERT_TRUE(replay);
    EXPECT_EQ(replay->Files, nextFiles->Files);
    EXPECT_EQ(replay->NextPageToken, nextFiles->NextPageToken);
}

TEST_F(TMetadataPagesTest, PinsCollectionsHistoryAndFilesAcrossWrites)
{
    CheckFixedViews(*Engine_);
}

TEST_F(TMetadataPagesTest, RejectsChangedForeignAndPreRestartTokens)
{
    ASSERT_TRUE(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).result);
    ASSERT_TRUE(Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).result);
    const auto files = Engine_->ListFilesPage(test::collection_id, {.PageSize = 1, .AuthorizationContext = "alice"});
    ASSERT_TRUE(files);
    const TMetadataPageRequest next{.PageSize = 1, .PageToken = files->NextPageToken, .AuthorizationContext = "alice"};
    ASSERT_FALSE(next.PageToken.empty());
    const auto expectCursor = [](const auto& result) {
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "INVALID_CURSOR");
    };
    auto changed = next;
    changed.PageSize = 2;
    expectCursor(Engine_->ListFilesPage(test::collection_id, changed));
    changed = next;
    changed.AuthorizationContext = "bob";
    expectCursor(Engine_->ListFilesPage(test::collection_id, changed));
    changed = next;
    changed.PageToken.front() = changed.PageToken.front() == 'a' ? 'b' : 'a';
    expectCursor(Engine_->ListFilesPage(test::collection_id, changed));
    expectCursor(Engine_->ListCollectionsPage(next));
    expectCursor(Engine_->ListSnapshotsPage(test::collection_id, next));
    expectCursor(Engine_->ListFilesPage(SecondCollection, next));
    expectCursor(Engine_->ListFilesPage(test::collection_id, next, SnapshotSelector::InitialEmpty()));
    const auto foreign = CreateStore({"00000000-0000-4000-8000-000000000099", (Temporary_.path / "foreign").string()});
    ASSERT_TRUE(foreign);
    expectCursor((*foreign)->ListFilesPage(test::collection_id, next));
    ASSERT_TRUE((*foreign)->Shutdown(std::chrono::seconds(1)));
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(1)));
    const auto closed = Engine_->ListFilesPage(test::collection_id, next);
    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().code, "CANCELLED");
    Engine_.reset();
    const auto reopened = OpenExisting({(Temporary_.path / "store").string(), test::store_id});
    ASSERT_TRUE(reopened);
    Engine_ = *reopened;
    expectCursor(Engine_->ListFilesPage(test::collection_id, next));
    const auto fresh = Engine_->ListFilesPage(test::collection_id, {.PageSize = 1});
    ASSERT_TRUE(fresh);
    ASSERT_FALSE(fresh->NextPageToken.empty());
    ASSERT_TRUE(Engine_->Reconcile());
    expectCursor(Engine_->ListFilesPage(test::collection_id, {.PageSize = 1, .PageToken = fresh->NextPageToken}));
}

TEST_F(TMetadataPagesTest, KeepsHistoricalAndInitialPagesSeparateFromLatest)
{
    const auto empty = Engine_->ListFilesPage(test::collection_id, {.PageSize = 1}, SnapshotSelector::InitialEmpty());
    const auto emptyHistory = Engine_->ListSnapshotsPage(test::collection_id, {.PageSize = 1});
    ASSERT_TRUE(empty);
    ASSERT_TRUE(emptyHistory);
    EXPECT_TRUE(empty->Files.empty());
    EXPECT_TRUE(empty->NextPageToken.empty());
    EXPECT_TRUE(emptyHistory->snapshots.empty());
    EXPECT_TRUE(emptyHistory->NextPageToken.empty());
    const auto a = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    const auto b = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_TRUE(a.result);
    ASSERT_TRUE(b.result);
    const auto old = Engine_->ListFilesPage(test::collection_id,
        {.PageSize = 1},
        SnapshotSelector::Id(test::Present(a.result).snapshot_id));
    ASSERT_TRUE(old);
    EXPECT_EQ(old->SelectorResolved, SnapshotSelector::Id(test::Present(a.result).snapshot_id));
    ASSERT_EQ(old->Files.size(), 1U);
    EXPECT_TRUE(old->NextPageToken.empty());
    EXPECT_TRUE(empty->Files.empty());
    EXPECT_EQ(empty->SelectorResolved, SnapshotSelector::InitialEmpty());
    const auto missing = Engine_->ListFilesPage(test::collection_id, {}, SnapshotSelector::Id(-7));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, "NOT_FOUND");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

} // namespace
} // namespace glamdb
