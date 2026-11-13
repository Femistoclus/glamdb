// The default profile of a new store: count limits do not bind, and storage
// reservations follow the retained state that actually exists.

#include "core/resources/envelopes.h"
#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <format>
#include <limits>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

constexpr auto RecoveryClass = static_cast<size_t>(ResourceClass::Recovery);

std::string DefaultOperation(int64_t index)
{
    return std::format("17000000-0000-4000-8000-{:012x}", index + 1);
}

AppendOutcome AppendOne(Engine& engine, int64_t index)
{
    return engine.AppendDurable(
        {test::collection_id, DefaultOperation(index), {}, {Batch({{"k", index, {}, {}, {}}})}});
}

TEST(DefaultProfile, NewStoresAreNotBoundByCounts)
{
    const Limits limits;
    EXPECT_GE(limits.max_operations, 1LL << 40);
    EXPECT_EQ(limits.max_catalog_bytes, DefaultWarehouseBytes / 16);
    EXPECT_GE(limits.history.max_data_files, 1LL << 40);
    EXPECT_GE(limits.history.max_manifests, 1LL << 40);
    // The hot window of every collection (hot_snapshots + 16 before a cut).
    EXPECT_GE(limits.history.max_snapshots, Limits::MaxSupportedCollections * (DefaultHotSnapshots + 16));
    EXPECT_EQ(limits.history.max_manifest_bytes, DefaultWarehouseBytes / 4);
    EXPECT_EQ(limits.history.max_manifest_list_bytes, DefaultWarehouseBytes / 4);
    EXPECT_EQ(limits.history.max_footer_bytes, DefaultWarehouseBytes / 16);
    EXPECT_EQ(limits.history.max_recovery_workspace_bytes, DefaultWarehouseBytes / 16);
    // Unchanged per-file and per-commit bounds.
    EXPECT_EQ(limits.history.max_root_file_bytes, 16LL << 20);
    EXPECT_EQ(limits.history.max_commit_bytes, 32LL << 20);
    EXPECT_EQ(limits.history.max_root_bytes, 64LL << 20);
}

// The recovery reserve grows in whole chunks with the bound operations, before
// the binding: a refusal consumes no operation and leaves the store writable.
// Device reservations follow the actual catalog and recovery workspace.
TEST(DefaultProfile, ReservationsFollowActualRetainedState)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    const auto chunk = detail::RecoveryOperationChunk * detail::RecoveryBytesPerOperation;
    ResourceLimits budget;
    budget.memory_bytes[RecoveryClass] = detail::RecoveryMemoryBytes + chunk;
    auto owner = std::make_shared<ResourceOwner>(budget);
    auto created = CreateStore({test::store_id, root.string(), "local-delete-extra-v1", {}, owner});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    // The fixed envelope covers the first chunk of operations.
    EXPECT_EQ(owner->GetUsage().memory_bytes[RecoveryClass], detail::RecoveryMemoryBytes);
    // Default limits (64 GiB catalog, 64 GiB workspace) reserve only what an
    // empty store needs on its device.
    const auto device = owner->GetUsage().device_reserved_bytes;
    EXPECT_LE(device, (8ULL << 20) + 2 * detail::CatalogGrowthBytes + detail::WorkspaceDiskBytes + (1ULL << 20));
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    const auto operations = 2 * static_cast<int64_t>(detail::RecoveryOperationChunk) - 1;
    for (int64_t index = 0; index < operations; ++index) {
        if (index == static_cast<int64_t>(detail::RecoveryOperationChunk) - 1) {
            EXPECT_EQ(owner->GetUsage().memory_bytes[RecoveryClass], detail::RecoveryMemoryBytes);
        }
        const auto outcome = AppendOne(*engine, index);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << index << ": " << test::Present(outcome.error).message;
    }
    const auto refused = AppendOne(*engine, operations);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    const auto absent = engine->FindAppendOutcome(test::collection_id, DefaultOperation(operations));
    ASSERT_TRUE(absent);
    EXPECT_FALSE(absent->has_value());
    EXPECT_TRUE(engine->GetHealth().write_ready);
    EXPECT_EQ(owner->GetUsage().memory_bytes[RecoveryClass], detail::RecoveryMemoryBytes + chunk);
    ASSERT_TRUE(engine->Shutdown(5s));
    engine.reset();
    EXPECT_EQ(owner->GetUsage().memory_bytes[RecoveryClass], 0U);
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, 0U);

    // A larger budget: reopening reserves for the existing operations only,
    // and the next binding grows the reserve by one chunk.
    budget.memory_bytes[RecoveryClass] = detail::RecoveryMemoryBytes + 2 * chunk;
    owner = std::make_shared<ResourceOwner>(budget);
    for (const bool full : {false, true}) {
        OpenStoreRequest request{root.string(), test::store_id, "local-delete-extra-v1", {}, owner};
        request.verify_full = full;
        auto opened = OpenExisting(request);
        ASSERT_TRUE(opened) << opened.error().message;
        EXPECT_EQ(owner->GetUsage().memory_bytes[RecoveryClass], detail::RecoveryMemoryBytes + chunk);
        if (full) {
            ASSERT_EQ(AppendOne(**opened, operations).disposition, CommitDisposition::Committed);
            EXPECT_EQ(owner->GetUsage().memory_bytes[RecoveryClass], detail::RecoveryMemoryBytes + 2 * chunk);
            EXPECT_GE(owner->GetUsage().device_reserved_bytes, device);
        }
        ASSERT_TRUE((*opened)->Shutdown(5s));
    }
}

// A store created with the defaults reopens under the same limits and is
// refused under lower limits that its retained state exceeds.
TEST(DefaultProfile, ReopenUnderSameAndLowerLimits)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    {
        auto created = CreateStore({test::store_id, root.string()});
        ASSERT_TRUE(created) << created.error().message;
        ASSERT_EQ((*created)->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        for (int64_t index = 0; index < 120; ++index) {
            ASSERT_EQ(AppendOne(**created, index).disposition, CommitDisposition::Committed);
        }
        ASSERT_TRUE((*created)->Shutdown(5s));
    }
    for (const bool full : {false, true}) {
        OpenStoreRequest request{root.string(), test::store_id};
        request.verify_full = full;
        auto opened = OpenExisting(request);
        ASSERT_TRUE(opened) << opened.error().message;
        EXPECT_EQ((*opened)->GetStatistics(test::collection_id)->rows.value, 120);
        ASSERT_TRUE((*opened)->Shutdown(5s));
    }
    const auto refused = [&](const std::function<void(Limits&)>& lower) {
        OpenStoreRequest request{root.string(), test::store_id};
        lower(request.limits);
        return OpenExisting(request);
    };
    const auto operations = refused([](Limits& limits) {
        limits.max_operations = 100;
    });
    ASSERT_FALSE(operations);
    EXPECT_EQ(operations.error().code, "CONFIG_TOO_SMALL");
    for (const auto& lower : std::vector<std::function<void(Limits&)>>{[](Limits& limits) {
                                                                           limits.history.max_data_files = 100;
                                                                       },
             [](Limits& limits) {
                 limits.history.max_manifests = 100;
             },
             [](Limits& limits) {
                 limits.history.max_manifest_list_bytes = 64 << 10;
             },
             [](Limits& limits) {
                 limits.history.max_snapshots = 10;
             }})
    {
        const auto result = refused(lower);
        EXPECT_FALSE(result);
    }
}

} // namespace
} // namespace glamdb
