#include <gtest/gtest.h>

TEST(CoreScaffold, UsesCpp23)
{
    EXPECT_GT(__cplusplus, 202002L);
}
