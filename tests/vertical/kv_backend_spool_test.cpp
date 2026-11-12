#include "core/backend/kv_spool.h"
#include "tests/support/checked.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb::detail {
namespace {

TEST(KvBackendSpool, LastInputWinsAcrossOutOfOrderRoutesAndBinaryKeys)
{
    test::TempDir directory;
    auto spool = TKvSpool::Make(directory.path, "00112233445566778899aabbccddeeff");
    ASSERT_TRUE(spool) << spool.error().message;
    ASSERT_TRUE((*spool)->Insert(2, "z", "old", DuplicatePolicy::LastWins));
    ASSERT_TRUE((*spool)->Insert(0, "b", "bee", DuplicatePolicy::LastWins));
    ASSERT_TRUE((*spool)->Insert(0, std::string("a\0b", 3), "zero", DuplicatePolicy::LastWins));
    ASSERT_TRUE((*spool)->Insert(2, "z", "last", DuplicatePolicy::LastWins));
    ASSERT_TRUE((*spool)->Seal());
    auto cursor = (*spool)->ReadRoute(0);
    ASSERT_TRUE(cursor) << cursor.error().message;
    auto first = (*cursor)->Next();
    ASSERT_TRUE(first && first->has_value());
    EXPECT_EQ(test::Present(*first).Key, std::string("a\0b", 3));
    EXPECT_EQ(test::Present(*first).Payload, "zero");
    auto second = (*cursor)->Next();
    ASSERT_TRUE(second && second->has_value());
    EXPECT_EQ(test::Present(*second).Key, "b");
    EXPECT_FALSE((*cursor)->Next()->has_value());
    auto tail = (*spool)->ReadRoute(2);
    ASSERT_TRUE(tail);
    auto last = (*tail)->Next();
    ASSERT_TRUE(last && last->has_value());
    EXPECT_EQ(test::Present(*last).Payload, "last");
    EXPECT_GT((*spool)->IoBytes(), 0ULL);
    EXPECT_LE((*spool)->AllocatedBytes(), 4ULL << 30);
}

TEST(KvBackendSpool, MigrationPoliciesKeepFirstOrRejectDuplicate)
{
    test::TempDir directory;
    auto spool = TKvSpool::Make(directory.path, "10112233445566778899aabbccddeeff");
    ASSERT_TRUE(spool);
    ASSERT_TRUE((*spool)->Insert(0, "same", "first", DuplicatePolicy::FirstWins));
    ASSERT_TRUE((*spool)->Insert(0, "same", "later", DuplicatePolicy::FirstWins));
    ASSERT_TRUE((*spool)->Seal());
    auto cursor = (*spool)->ReadRoute(0);
    ASSERT_TRUE(cursor);
    auto row = (*cursor)->Next();
    ASSERT_TRUE(row && row->has_value());
    EXPECT_EQ(test::Present(*row).Payload, "first");
    auto reject = TKvSpool::Make(directory.path, "20112233445566778899aabbccddeeff");
    ASSERT_TRUE(reject);
    ASSERT_TRUE((*reject)->Insert(0, "same", "first", DuplicatePolicy::Reject));
    const auto failed = (*reject)->Insert(0, "same", "later", DuplicatePolicy::Reject);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "INVALID_ARGUMENT");
}

TEST(KvBackendSpool, PageLimitRefusesWithoutLeakingScratchFiles)
{
    test::TempDir directory;
    const auto path = directory.path / ".glamdb-kv-30112233445566778899aabbccddeeff.sqlite";
    {
        auto spool = TKvSpool::Make(directory.path, "30112233445566778899aabbccddeeff", 8192, 1ULL << 20);
        ASSERT_TRUE(spool);
        const auto failed = (*spool)->Insert(0, "large", std::string(32768, 'x'), DuplicatePolicy::LastWins);
        ASSERT_FALSE(failed);
        EXPECT_EQ(failed.error().code, "RESOURCE_EXHAUSTED");
    }
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_TRUE(std::filesystem::is_empty(directory.path));
}

TEST(KvBackendSpool, RequestedIoBudgetRefusesAndCleansUpBeforeAReadCanExceedIt)
{
    test::TempDir directory;
    const auto spool = TKvSpool::Make(directory.path, "40112233445566778899aabbccddeeff", 8192, 1);
    ASSERT_FALSE(spool);
    EXPECT_EQ(spool.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_TRUE(std::filesystem::is_empty(directory.path));
}

TEST(KvBackendSpool, ExclusiveCollisionDoesNotDeleteAnExistingOwnedSpool)
{
    test::TempDir directory;
    auto first = TKvSpool::Make(directory.path, "50112233445566778899aabbccddeeff");
    ASSERT_TRUE(first);
    ASSERT_TRUE((*first)->Insert(0, "keep", "original", DuplicatePolicy::LastWins));
    const auto collision = TKvSpool::Make(directory.path, "50112233445566778899aabbccddeeff");
    ASSERT_FALSE(collision);
    EXPECT_EQ(collision.error().code, "IO_ERROR");
    ASSERT_TRUE((*first)->Seal());
    auto cursor = (*first)->ReadRoute(0);
    ASSERT_TRUE(cursor);
    const auto row = (*cursor)->Next();
    ASSERT_TRUE(row && *row);
    EXPECT_EQ(test::Present(*row).Payload, "original");
}

} // namespace
} // namespace glamdb::detail
