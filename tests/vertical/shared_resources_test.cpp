#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include "core/resources/envelopes.h"
#include "storage/resources.h"

#include <arrow/c/bridge.h>

#include <chrono>

namespace glamdb {
namespace {
using namespace vertical;
using namespace std::chrono_literals;

TEST(SharedResources, TwoStoresRefuseNewWritesButKeepOriginalReceiptsAndRecovery)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto first = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "first").string(), .resources = owner});
    auto second = CreateStore(
        {.store_id = test::table_uuid, .absolute_root = (temporary.path / "second").string(), .resources = owner});
    ASSERT_TRUE(first) << first.error().message;
    ASSERT_TRUE(second) << second.error().message;
    for (const auto& engine : {*first, *second}) {
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }
    AppendRequest request{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    const auto committed = (*first)->AppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    auto saturated = owner->Reserve(ResourceClass::Data, owner->GetLimits().memory_bytes[0]);
    ASSERT_TRUE(saturated);
    EXPECT_EQ((*first)->AppendDurable(request), committed);
    request.operation_id = SecondOperation;
    for (const auto& engine : {*first, *second}) {
        auto refused = engine->AppendDurable(request);
        ASSERT_TRUE(refused.error);
        EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
        EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
        const auto lookup = engine->FindAppendOutcome(test::collection_id, SecondOperation);
        ASSERT_TRUE(lookup);
        EXPECT_FALSE(lookup->has_value());
        EXPECT_TRUE(engine->GetHealth().write_ready);
        EXPECT_TRUE(engine->Reconcile());
    }
    saturated->reset();
    EXPECT_EQ((*second)->AppendDurable(request).disposition, CommitDisposition::Committed);
}

TEST(SharedResources, RetainedSliceAndExportKeepAllocationChargesAfterClose)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    auto opened = engine->Scan({.collection_id = test::collection_id,
        .predicate = Predicate::And(
            {Predicate::In("key", {std::string("duplicate"), std::nullopt}), Predicate::IsNotNull("key")})});
    ASSERT_TRUE(opened) << opened.error().message;
    auto next = (*opened)->Next();
    ASSERT_TRUE(next) << next.error().message;
    ASSERT_NE(*next, nullptr);
    auto slice = (*next)->Slice(0, 1);
    ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportRecordBatch(*slice, &exported).ok());
    next->reset();
    opened->reset();
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();
    EXPECT_GT(owner->GetUsage().memory_bytes[0], 0);
    slice.reset();
    EXPECT_GT(owner->GetUsage().memory_bytes[0], 0);
    exported.release(&exported);
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, 0);
    EXPECT_EQ(owner->GetUsage().memory_bytes[4], 0);
}

TEST(SharedResources, MetadataPagesRetainIndependentChargesAfterClose)
{
    for (const int64_t pageSize : {1, 100}) {
        SCOPED_TRACE(pageSize);
        test::TempDir temporary;
        auto owner = std::make_shared<ResourceOwner>();
        auto created = CreateStore(
            {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
        ASSERT_TRUE(created) << created.error().message;
        auto engine = std::move(*created);
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(engine->CreateCollection({SecondCollection, "other", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_TRUE(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).result);
        ASSERT_TRUE(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).result);
        const TMetadataPageRequest request{.PageSize = pageSize};
        auto collections = engine->ListCollectionsPage(request);
        auto history = engine->ListSnapshotsPage(test::collection_id, request);
        auto files = engine->ListFilesPage(test::collection_id, request);
        ASSERT_TRUE(collections);
        ASSERT_TRUE(history);
        ASSERT_TRUE(files);
        ASSERT_EQ(collections->Collections.size(), pageSize == 1 ? 1U : 2U);
        ASSERT_EQ(history->snapshots.size(), pageSize == 1 ? 1U : 2U);
        ASSERT_EQ(files->Files.size(), pageSize == 1 ? 1U : 2U);
        ASSERT_TRUE(engine->Shutdown(1000ms));
        engine.reset();
        const auto allPages = owner->GetUsage().memory_bytes[1];
        EXPECT_GT(allPages, 0);
        collections = TCollectionPage{};
        const auto historyAndFiles = owner->GetUsage().memory_bytes[1];
        EXPECT_LT(historyAndFiles, allPages);
        history = SnapshotHistory{};
        EXPECT_LT(owner->GetUsage().memory_bytes[1], historyAndFiles);
        EXPECT_GT(owner->GetUsage().memory_bytes[1], 0);
        files = TFileListing{};
        EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
    }
}

TEST(SharedResources, MetadataPagePressurePreservesContinuationAndReplay)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->CreateCollection({SecondCollection, "other", Schema}).disposition, CommitDisposition::Committed);
    const auto first = engine->ListCollectionsPage({.PageSize = 1});
    ASSERT_TRUE(first);
    ASSERT_FALSE(first->NextPageToken.empty());
    const TMetadataPageRequest request{.PageSize = 1, .PageToken = first->NextPageToken};
    auto pressure =
        owner->Reserve(ResourceClass::Metadata, owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(pressure);
    const auto refused = engine->ListCollectionsPage(request);
    EXPECT_FALSE(refused);
    if (!refused) {
        EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    }
    EXPECT_TRUE(engine->GetHealth().read_available);
    pressure->reset();
    const auto second = engine->ListCollectionsPage(request);
    ASSERT_TRUE(second);
    ASSERT_EQ(second->Collections.size(), 1U);
    EXPECT_EQ(second->Collections.front().collection_id, SecondCollection);
    const auto beforeReplay = owner->GetUsage().memory_bytes[1];
    const auto replay = engine->ListCollectionsPage(request);
    ASSERT_TRUE(replay);
    EXPECT_GT(owner->GetUsage().memory_bytes[1], beforeReplay);
    EXPECT_EQ(replay->Collections, second->Collections);
    EXPECT_EQ(replay->NextPageToken, second->NextPageToken);
    ASSERT_TRUE(engine->Shutdown(1000ms));
}

TEST(SharedResources, CompoundScanLiteralsRequireRetainedMetadataAdmission)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"label", FieldType::String, true}}};
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", schema}).disposition,
        CommitDisposition::Committed);
    std::vector<OptionalLiteral> values;
    values.reserve(32);
    for (size_t index = 0; index < 32; ++index) {
        std::string text(1ULL << 20, 'x');
        text[0] = static_cast<char>('A' + index);
        values.emplace_back(std::move(text));
    }
    const ScanRequest request{.collection_id = test::collection_id,
        .predicate = Predicate::And({Predicate::In("label", std::move(values)), Predicate::IsNotNull("key")})};
    const auto baseline = owner->GetUsage().memory_bytes[1];
    auto cursor = (*created)->Scan(request);
    ASSERT_TRUE(cursor) << cursor.error().message;
    EXPECT_GE(owner->GetUsage().memory_bytes[1] - baseline, 32ULL << 20);
    cursor->reset();
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], baseline);
    // Leave room for planning and the old fixed cursor allowance, but not for
    // copying and retaining this predicate's string set.
    auto pressure = owner->Reserve(ResourceClass::Metadata,
        owner->GetLimits().memory_bytes[1] - baseline - detail::MetadataWorkBytes - (17ULL << 20));
    ASSERT_TRUE(pressure);
    auto refused = (*created)->Scan(request);
    EXPECT_FALSE(refused);
    if (!refused) {
        EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
        EXPECT_EQ(owner->GetUsage().memory_bytes[1], baseline + (*pressure)->GetBytes());
    }
    EXPECT_TRUE((*created)->GetHealth().read_available);
    pressure->reset();
    auto explanation = (*created)->ExplainScan(request);
    ASSERT_TRUE(explanation) << explanation.error().message;
    auto subtree = explanation->predicate.children().front();
    explanation = ScanExplanation{};
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    EXPECT_GT(owner->GetUsage().memory_bytes[1], 32ULL << 20);
    EXPECT_EQ(subtree.values().size(), 32);
    subtree = {};
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
}

TEST(SharedResources, UnknownRetainsCompletionAndDiskUntilReconciliation)
{
    test::TempDir temporary;
    test::JournalFaultVfs vfs;
    auto owner = std::make_shared<ResourceOwner>();
    std::shared_ptr<THookBackend> hooks;
    auto created = detail::EngineFactory::CreateWith(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner},
        THookBackend::Factory(hooks, {.Resources = owner}),
        vfs.Name());
    ASSERT_TRUE(created) << created.error().message;
    auto engine = *created;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    const auto baseline = owner->GetUsage();
    vfs.Arm(2, true);
    const auto unknown = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_GT(owner->GetUsage().memory_bytes[3], 0);
    EXPECT_GT(owner->GetUsage().device_reserved_bytes, baseline.device_reserved_bytes);
    vfs.Disarm();
    const auto used = owner->GetUsage().memory_bytes[0];
    auto saturated = owner->Reserve(ResourceClass::Data, owner->GetLimits().memory_bytes[0] - used);
    ASSERT_TRUE(saturated);
    const auto lookup = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(lookup);
    ASSERT_TRUE(lookup->has_value());
    EXPECT_EQ(test::Present(*lookup).disposition, CommitDisposition::CommitUnknown);
    const auto metadataUsed = owner->GetUsage().memory_bytes[1];
    auto metadataFull = owner->Reserve(ResourceClass::Metadata, owner->GetLimits().memory_bytes[1] - metadataUsed);
    ASSERT_TRUE(metadataFull);
    auto controlFull =
        owner->Reserve(ResourceClass::Control, owner->GetLimits().memory_bytes[2] - owner->GetUsage().memory_bytes[2]);
    ASSERT_TRUE(controlFull);
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(owner->GetUsage().memory_bytes[3], 0);
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, baseline.device_reserved_bytes);
    const auto resolved = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(resolved);
    ASSERT_TRUE(resolved->has_value());
    EXPECT_EQ(test::Present(*resolved).disposition, CommitDisposition::Committed);
}

TEST(SharedResources, UncertainCreationKeepsItsRootReservationUntilRecovery)
{
    test::TempDir temporary;
    test::JournalFaultVfs vfs;
    auto owner = std::make_shared<ResourceOwner>();
    std::shared_ptr<THookBackend> hooks;
    auto created = detail::EngineFactory::CreateWith(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner},
        THookBackend::Factory(hooks, {.Resources = owner}),
        vfs.Name());
    ASSERT_TRUE(created) << created.error().message;
    vfs.Arm(1, true);
    const auto unknown = (*created)->CreateCollection({test::collection_id, "events", Schema});
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    vfs.Disarm();
    auto full =
        owner->Reserve(ResourceClass::Metadata, owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(full);
    ASSERT_TRUE((*created)->Reconcile());
    EXPECT_TRUE((*created)->GetCollection(test::collection_id));
}

TEST(SharedResources, OriginalMetadataResultsKeepChargesAfterTheEngineCloses)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ((*created)->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    const auto baseline = owner->GetUsage().memory_bytes[1];
    auto pressure = owner->Reserve(ResourceClass::Metadata,
        owner->GetLimits().memory_bytes[1] - baseline - detail::MetadataWorkBytes - (32ULL << 10));
    ASSERT_TRUE(pressure);
    std::vector<SnapshotHistory> retained;
    bool refused = false;
    for (size_t count = 0; count < 64; ++count) {
        auto history = (*created)->ListSnapshots(test::collection_id);
        if (!history) {
            EXPECT_EQ(history.error().code, "RESOURCE_EXHAUSTED");
            refused = true;
            break;
        }
        ASSERT_EQ(history->snapshots.size(), 1);
        EXPECT_EQ(history->snapshots[0].receipt.rows_appended, 3);
        retained.push_back(std::move(*history));
    }
    EXPECT_TRUE(refused);
    EXPECT_GT(owner->GetUsage().memory_bytes[1], baseline + (*pressure)->GetBytes());
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    pressure->reset();
    EXPECT_GT(owner->GetUsage().memory_bytes[1], 0);
    retained.clear();
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
}

TEST(SharedResources, ReopenUsesTheSameFiniteParent)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    const auto root = (temporary.path / "store").string();
    auto created = CreateStore({.store_id = test::store_id, .absolute_root = root, .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    AppendRequest request{test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
    const auto committed = (*created)->AppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, 0);
    auto opened = OpenExisting({.absolute_root = root, .expected_store_id = test::store_id, .resources = owner});
    ASSERT_TRUE(opened) << opened.error().message;
    EXPECT_EQ((*opened)->AppendDurable(request), committed);
}

TEST(SharedResources, FileMetadataResultsKeepSeparateChargesAfterClose)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (temporary.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created) << created.error().message;
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ((*created)->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    auto listing = (*created)->ListFiles(test::collection_id);
    ASSERT_TRUE(listing) << listing.error().message;
    ASSERT_EQ(listing->Files.size(), 1);
    EXPECT_EQ(listing->Files.front().RecordCount, 3);
    auto details = (*created)->GetFileDetails(test::collection_id, listing->Files.front().FileID);
    ASSERT_TRUE(details) << details.error().message;
    ASSERT_EQ(details->RowGroups.size(), 1);
    EXPECT_EQ(details->RowGroups.front().RecordCount, 3);
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();
    const auto both = owner->GetUsage().memory_bytes[1];
    EXPECT_GT(both, 0);
    listing = TFileListing{};
    EXPECT_LT(owner->GetUsage().memory_bytes[1], both);
    EXPECT_GT(owner->GetUsage().memory_bytes[1], 0);
    details = TFileDetails{};
    EXPECT_EQ(owner->GetUsage().memory_bytes[1], 0);
}
} // namespace
} // namespace glamdb
