#include "apps/server/admission.h"

#include <gtest/gtest.h>

namespace glamdb::NServer {
TEST(FairAdmission, RotatesDatabasesAndKeepsEachFifo)
{
    TFairQueue<int> queue({.Active = 1, .PerDatabase = 1});
    ASSERT_TRUE(queue.Enqueue("a", 0, 1));
    ASSERT_TRUE(queue.Enqueue("a", 0, 2));
    ASSERT_TRUE(queue.Enqueue("b", 0, 3));
    ASSERT_TRUE(queue.Enqueue("c", 0, 4));
    EXPECT_EQ(queue.Take(), 1);
    EXPECT_FALSE(queue.Take());
    queue.Release("a");
    EXPECT_EQ(queue.Take(), 3);
    queue.Release("b");
    EXPECT_EQ(queue.Take(), 4);
    queue.Release("c");
    EXPECT_EQ(queue.Take(), 2);
    queue.Release("a");
    EXPECT_FALSE(queue.Take());
}
TEST(FairAdmission, BlockedPublishersLeaveOtherStoresAndReadersAvailable)
{
    TFairQueue<int> writers({.Active = 4, .PerDatabase = 1}), readers({.Active = 4, .PerDatabase = 2});
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(writers.Enqueue("a", 0, i));
        ASSERT_TRUE(writers.Enqueue("b", 0, i + 3));
    }
    EXPECT_EQ(writers.Take(), 0);
    EXPECT_EQ(writers.Take(), 3);
    EXPECT_FALSE(writers.Take());
    ASSERT_TRUE(writers.Enqueue("c", 0, 6));
    EXPECT_EQ(writers.Take(), 6);
    ASSERT_TRUE(readers.Enqueue("a", 0, 7));
    ASSERT_TRUE(readers.Enqueue("a", 0, 8));
    EXPECT_EQ(readers.Take(), 7);
    EXPECT_EQ(readers.Take(), 8);
}
TEST(FairAdmission, CancellationReleasesCallAndByteCapacity)
{
    TFairQueue<int> queue({.Active = 1, .PerDatabase = 1});
    constexpr size_t RequestBytes = size_t{16} << 20;
    ASSERT_TRUE(queue.Enqueue("a", RequestBytes, 1));
    ASSERT_TRUE(queue.Enqueue("a", RequestBytes, 2));
    EXPECT_FALSE(queue.Enqueue("a", 1, 3));
    ASSERT_TRUE(queue.Enqueue("b", RequestBytes, 4));
    ASSERT_TRUE(queue.Enqueue("c", RequestBytes, 5));
    EXPECT_FALSE(queue.Enqueue("d", 1, 6));
    queue.Remove("a", 1);
    EXPECT_TRUE(queue.Enqueue("d", RequestBytes, 6));
    EXPECT_EQ(queue.Take(), 2);
    queue.Release("a");
    queue.Remove("b", 4);
    queue.Remove("c", 5);
    EXPECT_EQ(queue.Take(), 6);
    queue.Release("d");
    for (int i = 0; i < 16; ++i) {
        ASSERT_TRUE(queue.Enqueue("a", 0, i));
    }
    EXPECT_FALSE(queue.Enqueue("a", 0, 17));
    queue.Remove("a", 7);
    EXPECT_TRUE(queue.Enqueue("a", 0, 17));
}
} // namespace glamdb::NServer
