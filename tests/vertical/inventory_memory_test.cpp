#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include "storage/resources.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

TEST(InventoryMemory, OptionalTrackingCannotPreventReopenAndReceiptReads)
{
    test::TempDir temporary;
    const auto root = (temporary.path / "store").string();
    auto created = CreateStore({.store_id = test::store_id, .absolute_root = root});
    ASSERT_TRUE(created) << created.error().message;
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    const auto committed = (*created)->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    ASSERT_TRUE((*created)->Shutdown(1000ms));
    created->reset();

    ResourceLimits limits;
    limits.memory_bytes[static_cast<size_t>(ResourceClass::Metadata)] = 108ULL << 20;
    auto owner = std::make_shared<ResourceOwner>(limits);
    detail::TLocalStorageOptions options;
    options.Resources = owner;
    std::shared_ptr<THookBackend> hooks;
    auto opened = detail::EngineFactory::OpenWith(
        {.absolute_root = root, .expected_store_id = test::store_id, .resources = owner},
        THookBackend::Factory(hooks, options));
    ASSERT_TRUE(opened) << opened.error().message;
    EXPECT_LE(owner->GetUsage().memory_peak[static_cast<size_t>(ResourceClass::Metadata)], 108ULL << 20);
    EXPECT_TRUE((*opened)->GetHealth().read_available);
    const auto collection = (*opened)->GetCollection(test::collection_id);
    ASSERT_TRUE(collection) << collection.error().message;
    EXPECT_TRUE(collection->current_snapshot_id.has_value());
    const auto outcome = (*opened)->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(outcome) << outcome.error().message;
    ASSERT_TRUE(outcome->has_value());
    EXPECT_EQ(test::Present(*outcome), committed);
    ASSERT_TRUE((*opened)->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
