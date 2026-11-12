#include "core/scan/read_budget.h"
#include "tests/support/checked.h"
#include "tests/support/memory_file_io.h"

#include <array>

#include <gtest/gtest.h>
TEST(KVReadBudget, RefusesBeforeExceedingTheSharedRequestedByteLimit)
{
    auto budget = std::make_shared<glamdb::detail::TReadBudget>(4);
    EXPECT_TRUE(budget->Charge(3));
    EXPECT_FALSE(budget->Charge(2));
    ASSERT_TRUE(budget->Error());
    EXPECT_EQ(glamdb::test::Present(budget->Error()).code, "RESOURCE_EXHAUSTED");
}

TEST(KVReadBudget, EveryReopenedStreamConsumesOneSharedBudget)
{
    auto backing = std::make_shared<glamdb::test::TMemoryFileIO>();
    backing->Files["data"] = "abcd";
    auto budget = std::make_shared<glamdb::detail::TReadBudget>(4);
    auto io = glamdb::detail::BudgetReadOnlyFileIO(backing, budget);
    auto firstFile = io->NewInputFile("data");
    ASSERT_TRUE(firstFile);
    auto first = (*firstFile)->Open();
    ASSERT_TRUE(first);
    std::array<std::byte, 3> bytes{};
    EXPECT_TRUE((*first)->ReadFully(0, bytes));
    auto secondFile = io->NewInputFile("data");
    ASSERT_TRUE(secondFile);
    auto second = (*secondFile)->Open();
    ASSERT_TRUE(second);
    EXPECT_FALSE((*second)->ReadFully(0, bytes));
    ASSERT_TRUE(budget->Error());
    EXPECT_EQ(glamdb::test::Present(budget->Error()).code, "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(io->NewOutputFile("forbidden"));
}

TEST(KVReadBudget, CancellationAndDeadlineAreLatchedBeforeReads)
{
    auto cancelled = std::make_shared<std::atomic<bool>>(true);
    glamdb::detail::TReadBudget budget(100, std::chrono::steady_clock::time_point::max(), cancelled);
    EXPECT_FALSE(budget.Charge(0));
    cancelled->store(false);
    EXPECT_FALSE(budget.Charge(0));
    EXPECT_EQ(glamdb::test::Present(budget.Error()).code, "CANCELLED");
    glamdb::detail::TReadBudget expired(100, std::chrono::steady_clock::now());
    EXPECT_FALSE(expired.Charge(0));
    EXPECT_EQ(glamdb::test::Present(expired.Error()).code, "DEADLINE_EXCEEDED");
}

TEST(KVReadBudget, PartialAndFailedReadsStillConsumeTheirFullRequestedBytes)
{
    auto backing = std::make_shared<glamdb::test::TMemoryFileIO>();
    backing->Files["data"] = "ab";
    for (const bool complete : {false, true}) {
        auto budget = std::make_shared<glamdb::detail::TReadBudget>(6);
        auto io = glamdb::detail::BudgetReadOnlyFileIO(backing, budget);
        auto file = io->NewInputFile("data");
        ASSERT_TRUE(file);
        auto stream = (*file)->Open();
        ASSERT_TRUE(stream);
        std::array<std::byte, 4> bytes{};
        if (complete) {
            EXPECT_FALSE((*stream)->ReadFully(0, bytes));
        } else {
            const auto partial = (*stream)->Read(bytes);
            ASSERT_TRUE(partial);
            EXPECT_EQ(*partial, 2);
        }
        EXPECT_FALSE(budget->Error());
        EXPECT_FALSE((*stream)->Read(std::span(bytes).first(3)));
        ASSERT_TRUE(budget->Error());
        EXPECT_EQ(glamdb::test::Present(budget->Error()).code, "RESOURCE_EXHAUSTED");
    }
}
