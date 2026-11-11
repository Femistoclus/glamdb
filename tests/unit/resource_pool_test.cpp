#include "core/resources/arrow_pool.h"
#include "storage/resources.h"

#include <arrow/memory_pool.h>
#include <gtest/gtest.h>

namespace glamdb::detail {
namespace {
class TRefusingPool : public arrow::ProxyMemoryPool
{
public:
    using arrow::ProxyMemoryPool::ProxyMemoryPool;
    bool Refuse = false;
    arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override
    {
        return Refuse ? arrow::Status::OutOfMemory("test allocation refusal")
                      : arrow::ProxyMemoryPool::Allocate(size, alignment, out);
    }
};

TEST(ResourcePool, RefusalPreservesOldAllocationAndReleasesAfterFree)
{
    ResourceLimits limits;
    limits.memory_bytes[0] = 384;
    auto owner = std::make_shared<ResourceOwner>(limits);
    auto pool = MakeResourcePool(owner);
    uint8_t* data = nullptr;
    ASSERT_TRUE(pool->Allocate(128, &data).ok());
    data[0] = 42;
    EXPECT_TRUE(pool->Reallocate(128, 256, &data).IsOutOfMemory());
    EXPECT_EQ(data[0], 42);
    EXPECT_EQ(pool->bytes_allocated(), 128);
    pool->Free(data, 128);
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
}

TEST(ResourcePool, AllocatorRefusalRefundsTheParent)
{
    auto owner = std::make_shared<ResourceOwner>();
    auto failing = std::make_shared<arrow::CappedMemoryPool>(arrow::default_memory_pool(), 0);
    auto pool = MakeResourcePool(owner, failing);
    uint8_t* data = nullptr;
    EXPECT_TRUE(pool->Allocate(64, &data).IsOutOfMemory());
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
}

TEST(ResourcePool, ReallocationKeepsBothCapacitiesChargedUntilTheCopyFinishes)
{
    auto owner = std::make_shared<ResourceOwner>();
    auto pool = MakeResourcePool(owner);
    uint8_t* data = nullptr;
    ASSERT_TRUE(pool->Allocate(64, &data).ok());
    ASSERT_TRUE(pool->Reallocate(64, 128, &data).ok());
    EXPECT_GE(owner->GetUsage().memory_peak[0], 192);
    EXPECT_EQ(pool->bytes_allocated(), 128);
    pool->Free(data, 128);
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
}

TEST(ResourcePool, ZeroSizeRefusalPreservesTheOriginalAllocation)
{
    auto owner = std::make_shared<ResourceOwner>();
    auto upstream = std::make_shared<TRefusingPool>(arrow::default_memory_pool());
    auto pool = MakeResourcePool(owner, upstream);
    uint8_t* data = nullptr;
    ASSERT_TRUE(pool->Allocate(64, &data).ok());
    data[0] = 42;
    const auto charged = owner->GetUsage().memory_bytes[0];
    upstream->Refuse = true;
    EXPECT_TRUE(pool->Reallocate(64, 0, &data).IsOutOfMemory());
    EXPECT_EQ(pool->bytes_allocated(), 64);
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], charged);
    // Keep the failing version safe to run: it has already freed data.
    if (pool->bytes_allocated() == 64) {
        EXPECT_EQ(data[0], 42);
        upstream->Refuse = false;
        EXPECT_TRUE(pool->Reallocate(64, 0, &data).ok());
        EXPECT_EQ(pool->bytes_allocated(), 0);
        pool->Free(data, 0);
    }
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
}
} // namespace
} // namespace glamdb::detail
