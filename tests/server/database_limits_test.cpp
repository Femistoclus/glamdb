#include "apps/server/databases.h"
#include "core/catalog/sqlite.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

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

// New databases get the current defaults; persisted rows keep theirs, and a
// legacy row without history budgets keeps the budgets it was created under.
TEST(DatabaseLimits, PersistedLimitsSurviveChangedDefaults)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    std::string fresh, persisted, legacy;
    {
        TDatabases databases(directory);
        const auto created = databases.Create({{"name", "fresh"}});
        fresh = created.at("database_id").get<std::string>();
        EXPECT_EQ(created.at("limits").at("max_operations"), Limits{}.max_operations);
        EXPECT_EQ(created.at("limits").at("history").at("max_data_files"), HistoryLimits{}.max_data_files);
        persisted = databases.Create({{"name", "persisted"}}).at("database_id").get<std::string>();
        legacy = databases.Create({{"name", "legacy"}}).at("database_id").get<std::string>();
        databases.Shutdown();
    }
    {
        // As written by binaries with the former defaults.
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, {});
        auto old = nlohmann::json::parse(registry.Text("SELECT limits FROM databases WHERE name='persisted'"));
        old["max_operations"] = 4096;
        old["max_catalog_bytes"] = 256LL << 20;
        old["history"]["max_snapshots"] = 4096;
        old["history"]["max_manifests"] = 4096;
        old["history"]["max_data_files"] = 16384;
        registry.Execute("UPDATE databases SET limits=? WHERE name='persisted'", {old.dump()});
        old.erase("history");
        registry.Execute("UPDATE databases SET limits=? WHERE name='legacy'", {old.dump()});
    }
    TDatabases reopened(directory);
    const auto kept = reopened.Get(persisted).at("limits");
    EXPECT_EQ(kept.at("max_operations"), 4096);
    EXPECT_EQ(kept.at("history").at("max_data_files"), 16384);
    const auto legacyLimits = reopened.Get(legacy).at("limits");
    EXPECT_EQ(legacyLimits.at("max_operations"), 4096);
    EXPECT_EQ(legacyLimits.at("history").at("max_snapshots"), LegacyHistoryLimits.max_snapshots);
    EXPECT_EQ(legacyLimits.at("history").at("max_data_files"), LegacyHistoryLimits.max_data_files);
    EXPECT_EQ(legacyLimits.at("history").at("max_manifest_bytes"), LegacyHistoryLimits.max_manifest_bytes);
    EXPECT_EQ(reopened.Get(fresh).at("limits").at("max_catalog_bytes"), Limits{}.max_catalog_bytes);
    reopened.Shutdown();
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
