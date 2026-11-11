#include "storage/resources.h"

#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

#include <barrier>
#include <future>
#include <limits>

namespace glamdb {
namespace {

TEST(ResourceOwner, ClassesRefuseWithoutBorrowingProtectedCapacity)
{
    ResourceLimits limits;
    limits.memory_bytes = {100, 100, 100, 40, 80};
    auto owner = std::make_shared<ResourceOwner>(limits);
    auto data = owner->Reserve(ResourceClass::Data, 100);
    ASSERT_TRUE(data);
    EXPECT_FALSE(owner->Reserve(ResourceClass::Data, 1));
    EXPECT_FALSE(owner->Reserve(ResourceClass::Data, std::numeric_limits<uint64_t>::max()));
    auto completion = owner->Reserve(ResourceClass::Completion, 40);
    auto recovery = owner->Reserve(ResourceClass::Recovery, 80);
    ASSERT_TRUE(completion);
    ASSERT_TRUE(recovery);
    EXPECT_EQ(owner->GetUsage().memory_peak[0], 100);
    data->reset();
    EXPECT_TRUE(owner->Reserve(ResourceClass::Data, 100));
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
}

TEST(ResourceOwner, SharedLeasesReleaseExactlyOnceAndRetainTheParentState)
{
    ResourceLimits limits;
    limits.memory_bytes[0] = 64;
    auto owner = std::make_shared<ResourceOwner>(limits);
    auto result = owner->Reserve(ResourceClass::Data, 64);
    ASSERT_TRUE(result);
    auto first = *result;
    auto second = first;
    result->reset();
    first.reset();
    EXPECT_FALSE(owner->Reserve(ResourceClass::Data, 1));
    second.reset();
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
    auto retained = owner->Reserve(ResourceClass::Data, 64);
    ASSERT_TRUE(retained);
    owner.reset();
    retained->reset();
}

TEST(ResourceOwner, ConcurrentChildrenCannotSpendTheSameBytes)
{
    ResourceLimits limits;
    limits.memory_bytes[0] = 100;
    auto owner = std::make_shared<ResourceOwner>(limits);
    std::barrier start(3), finish(3);
    auto compete = [&] {
        start.arrive_and_wait();
        auto reservation = owner->Reserve(ResourceClass::Data, 100);
        const bool admitted = reservation.has_value();
        finish.arrive_and_wait();
        return admitted;
    };
    auto first = std::async(std::launch::async, compete);
    auto second = std::async(std::launch::async, compete);
    start.arrive_and_wait();
    finish.arrive_and_wait();
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(owner->GetUsage().memory_bytes[0], 0);
}

TEST(ResourceOwner, StoresAndWorkspacesOnOneDeviceShareBytesAndInodes)
{
    test::TempDir temporary;
    std::filesystem::create_directory(temporary.path / "store");
    std::filesystem::create_directory(temporary.path / "workspace");
    ResourceLimits limits;
    limits.device_reserved_bytes = 4096;
    limits.device_reserved_inodes = 2;
    limits.device_free_bytes = 0;
    limits.device_free_inodes = 0;
    auto owner = std::make_shared<ResourceOwner>(limits);
    auto first = owner->ReserveDevice((temporary.path / "store").string(), 4096, 1);
    ASSERT_TRUE(first) << first.error().message;
    EXPECT_FALSE(owner->ReserveDevice((temporary.path / "workspace").string(), 1, 1));
    auto second = owner->ReserveDevice((temporary.path / "workspace").string(), 0, 1);
    ASSERT_TRUE(second);
    EXPECT_FALSE(owner->ReserveDevice(temporary.path.string(), 0, 1));
    first->reset();
    EXPECT_TRUE(owner->ReserveDevice(temporary.path.string(), 4096, 1));
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, 0);
    EXPECT_EQ(owner->GetUsage().device_reserved_inodes, 1);
}

TEST(ResourceOwner, DeviceOverflowAndMissingPathsRefuseWithoutChangingUsage)
{
    test::TempDir temporary;
    auto owner = std::make_shared<ResourceOwner>();
    EXPECT_FALSE(owner->ReserveDevice(temporary.path.string(), std::numeric_limits<uint64_t>::max(), 1));
    EXPECT_FALSE(owner->ReserveDevice("relative", 1, 1));
    EXPECT_FALSE(owner->ReserveDevice(std::string("/tmp\0ignored", 12), 1, 1));
    EXPECT_EQ(owner->GetUsage().device_reserved_bytes, 0);
}

} // namespace
} // namespace glamdb
