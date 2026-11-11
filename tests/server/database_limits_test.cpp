#include "apps/server/databases.h"
#include "core/catalog/sqlite.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb::NServer {
using namespace NTransport;

TEST(DatabaseLimits, UncertainOpenIntentRetainsAuthoritativeConfiguration)
{
    for (const bool after : {false, true}) {
        SCOPED_TRACE(after ? "after journal unlink" : "before journal unlink");
        test::TempDir temporary;
        test::JournalFaultVfs vfs;
        const auto directory = temporary.path / "server";
        std::string id;
        {
            TDatabases databases(directory, vfs.Name());
            id = databases.Create({{"name", "limited"}, {"limits", {{"max_collections", 1}}}})
                     .at("database_id")
                     .get<std::string>();
            databases.Change(id, "close");
            vfs.Arm(1, after);
            EXPECT_THROW(databases.Change(id, "open", {{"limits", {{"max_collections", 2}}}}), detail::Failure);
            EXPECT_THROW(databases.Change(id, "open"), TFailure);
            EXPECT_THROW(databases.Create({{"name", "other"}}), TFailure);
            vfs.Disarm();
            databases.Shutdown();
        }
        TDatabases reopened(directory);
        const auto info = reopened.Get(id);
        EXPECT_EQ(info.at("limits").at("max_collections"), after ? 2 : 1);
        EXPECT_EQ(info.at("state"), after ? "open" : "closed");
        EXPECT_EQ(reopened.Create({{"name", "limited"}}).at("limits"), info.at("limits"));
        EXPECT_EQ(reopened.List().at("items").size(), 1U);
        reopened.Shutdown();
    }
}

TEST(DatabaseLimits, InterruptedInitializationPublishesOnlyCompleteRegistry)
{
    for (const bool after : {false, true}) {
        SCOPED_TRACE(after ? "after journal unlink" : "before journal unlink");
        test::TempDir temporary;
        test::JournalFaultVfs vfs;
        const auto directory = temporary.path / "server";
        vfs.Arm(1, after);
        EXPECT_THROW(TDatabases databases(directory, vfs.Name()), detail::Failure);
        EXPECT_FALSE(std::filesystem::exists(directory / "registry.sqlite"));
        vfs.Disarm();
        TDatabases reopened(directory);
        EXPECT_TRUE(reopened.List().at("items").empty());
        const auto info = reopened.Create({{"name", "after-failure"}});
        EXPECT_EQ(info.at("state"), "open");
        EXPECT_EQ(info.at("limits").at("max_collections"), 8);
        reopened.Shutdown();
    }
}
TEST(DatabaseLimits, ChangingLimitsWhileOpenRequiresClose)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    const auto id = databases.Create({{"name", "limited"}}).at("database_id").get<std::string>();
    try {
        databases.Change(id, "open", {{"limits", {{"max_collections", 1}}}});
        FAIL() << "changing limits requires a closed database";
    } catch (const TFailure& error) {
        EXPECT_EQ(error.Detail.code, "PRECONDITION_FAILED");
        EXPECT_EQ(error.Detail.retry_hint, RetryHint::FixInput);
    }
    EXPECT_EQ(databases.Get(id).at("limits").at("max_collections"), 8);
    databases.Change(id, "close");
    EXPECT_EQ(databases.Change(id, "open", {{"limits", {{"max_collections", 1}}}}).at("limits").at("max_collections"),
        1);
    databases.Shutdown();
}
} // namespace glamdb::NServer
