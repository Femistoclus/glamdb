#include "core/resources/envelopes.h"
#include "storage/resources.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

TEST(MetadataWorkingAdmissionTest, HistoryAndFilesShareOnePlanningEnvelope)
{
    for (const int64_t rootBytes : {16LL << 20, 32LL << 20}) {
        SCOPED_TRACE(rootBytes);
        test::TempDir temporary;
        auto owner = std::make_shared<ResourceOwner>();
        Limits limits;
        limits.history.max_root_file_bytes = rootBytes;
        auto created = CreateStore({.store_id = test::store_id,
            .absolute_root = (temporary.path / "store").string(),
            .limits = limits,
            .resources = owner});
        ASSERT_TRUE(created) << created.error().message;
        auto engine = std::move(*created);
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
        const auto second = engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
        ASSERT_TRUE(first.result);
        ASSERT_TRUE(second.result);

        // Enough for one complete root/manifest traversal plus retained DTOs,
        // deliberately insufficient for two nested full working leases.
        const auto available = owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1];
        auto pressure = owner->Reserve(ResourceClass::Metadata,
            available - detail::MetadataWorkForRoot(rootBytes) - (128ULL << 20));
        ASSERT_TRUE(pressure) << pressure.error().message;
        {
            const auto remaining = owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1];
            auto collectionPressure =
                owner->Reserve(ResourceClass::Metadata, remaining - detail::MetadataWorkBytes - (32ULL << 20));
            ASSERT_TRUE(collectionPressure);
            const auto collections = engine->ListCollectionsPage({.PageSize = 1});
            ASSERT_TRUE(collections) << collections.error().message;
            EXPECT_EQ(collections->Collections.size(), 1U);
        }
        auto history = engine->ListSnapshotsPage(test::collection_id, {.PageSize = 1});
        ASSERT_TRUE(history) << history.error().code << ": " << history.error().message;
        ASSERT_EQ(history->snapshots.size(), 1U);
        ASSERT_FALSE(history->NextPageToken.empty());
        EXPECT_EQ(history->snapshots.front().receipt.operation_id, FirstOperation);
        const TMetadataPageRequest historyRequest{.PageSize = 1, .PageToken = history->NextPageToken};
        auto remainingHistory = engine->ListSnapshotsPage(test::collection_id, historyRequest);
        ASSERT_TRUE(remainingHistory) << remainingHistory.error().message;
        ASSERT_EQ(remainingHistory->snapshots.size(), 1U);
        EXPECT_EQ(remainingHistory->snapshots.front().receipt.operation_id, SecondOperation);

        auto files = engine->ListFilesPage(test::collection_id, {.PageSize = 1});
        ASSERT_TRUE(files) << files.error().code << ": " << files.error().message;
        ASSERT_EQ(files->Files.size(), 1U);
        ASSERT_FALSE(files->NextPageToken.empty());
        const TMetadataPageRequest filesRequest{.PageSize = 1, .PageToken = files->NextPageToken};
        auto remainingFiles = engine->ListFilesPage(test::collection_id, filesRequest);
        ASSERT_TRUE(remainingFiles) << remainingFiles.error().message;
        ASSERT_EQ(remainingFiles->Files.size(), 1U);
        EXPECT_NE(files->Files.front().FileID, remainingFiles->Files.front().FileID);

        pressure->reset();
        const auto used = owner->GetUsage().memory_bytes[1];
        auto exhausted = owner->Reserve(ResourceClass::Metadata, owner->GetLimits().memory_bytes[1] - used);
        ASSERT_TRUE(exhausted);
        const auto refused = engine->ListFilesPage(test::collection_id, filesRequest);
        ASSERT_FALSE(refused);
        EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
        EXPECT_TRUE(engine->GetHealth().read_available);
        exhausted->reset();

        auto replay = engine->ListFilesPage(test::collection_id, filesRequest);
        ASSERT_TRUE(replay) << replay.error().message;
        EXPECT_EQ(replay->Files, remainingFiles->Files);
        EXPECT_EQ(replay->NextPageToken, remainingFiles->NextPageToken);
        EXPECT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}), first);
        ASSERT_TRUE(engine->Shutdown(1000ms));
        engine.reset();
        EXPECT_GT(owner->GetUsage().memory_bytes[1], 0U);
        history = SnapshotHistory{};
        remainingHistory = SnapshotHistory{};
        files = TFileListing{};
        remainingFiles = TFileListing{};
        replay = TFileListing{};
        EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0U);
    }
}

} // namespace
} // namespace glamdb
