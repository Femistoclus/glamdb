#include "apps/server/databases.h"
#include "core/api/binary.h"
#include "core/catalog/sqlite.h"
#include "core/engine/store_lease.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

#include <fstream>
#include <future>
#include <thread>

namespace glamdb::NServer {
using namespace NTransport;
using namespace std::chrono_literals;

TEST(Databases, RepeatedOpenAndCreateDoNotDrainAcceptedRequests)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    const auto info = databases.Create({{"name", "a"}});
    const auto id = info.at("database_id").get<std::string>();
    for (const bool create : {false, true}) {
        std::promise<void> entered, release;
        auto released = release.get_future();
        auto reader = std::async(std::launch::async, [&] {
            return databases.Use(id, [&](const auto&) {
                entered.set_value();
                released.wait();
                return TResponse{200, ""};
            });
        });
        entered.get_future().wait();
        auto repeated = std::async(std::launch::async, [&] {
            return create ? databases.Create({{"name", "a"}}) : databases.Change(id, "open");
        });
        EXPECT_EQ(repeated.wait_for(100ms), std::future_status::ready);
        EXPECT_NO_THROW(databases.Use(id, [](const auto&) {
            return TResponse{200, ""};
        }));
        release.set_value();
        reader.get();
        EXPECT_EQ(repeated.get(), info);
    }
    databases.Shutdown();
}

TEST(Databases, CloseDrainsOnlyItsOwnRequests)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    const auto a = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
    const auto b = databases.Create({{"name", "b"}}).at("database_id").get<std::string>();
    std::promise<void> entered, release;
    auto released = release.get_future();
    auto reader = std::async(std::launch::async, [&] {
        return databases.Use(a, [&](const auto&) {
            entered.set_value();
            released.wait();
            return TResponse{200, "done"};
        });
    });
    entered.get_future().wait();
    auto closer = std::async(std::launch::async, [&] {
        return databases.Change(a, "close");
    });
    bool rejected = false;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!rejected && std::chrono::steady_clock::now() < deadline) {
        try {
            databases.Use(a, [](const auto&) {
                return TResponse{200, ""};
            });
        } catch (const TFailure&) {
            rejected = true;
        }
        std::this_thread::yield();
    }
    EXPECT_TRUE(rejected);
    EXPECT_EQ(closer.wait_for(0ms), std::future_status::timeout);
    auto metadata = std::async(std::launch::async, [&] {
        const auto info = databases.Get(a);
        EXPECT_EQ(info.at("state"), "closing");
        return databases.List();
    });
    EXPECT_EQ(metadata.wait_for(100ms), std::future_status::ready);
    EXPECT_EQ(databases
                  .Use(b,
                      [](const auto&) {
                          return TResponse{200, ""};
                      })
                  .Status,
        200);
    release.set_value();
    EXPECT_EQ(reader.get().Status, 200);
    EXPECT_EQ(closer.get().at("state"), "closed");
    EXPECT_EQ(metadata.get().at("items").size(), 2U);
    EXPECT_EQ(databases.Change(a, "open").at("state"), "open");
    databases.Shutdown();
}

TEST(Databases, ConcurrentNameReservationHasOneIdentity)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    auto one = std::async(std::launch::async, [&] {
        return databases.Create({{"name", "same"}});
    });
    auto two = std::async(std::launch::async, [&] {
        return databases.Create({{"name", "same"}});
    });
    EXPECT_EQ(one.get(), two.get());
    EXPECT_EQ(databases.List().at("items").size(), 1U);
    EXPECT_THROW(TDatabases other(temporary.path / "server"), TFailure);
    databases.Shutdown();
}

TEST(Databases, FailedRecoveryCanCloseAndReleaseOwnership)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    const auto id = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
    databases.Use(id, [&](const auto& engine) {
        const auto created =
            engine->CreateCollection({test::collection_id, "c", SchemaSpec{{{"key", FieldType::String, false}}}});
        EXPECT_EQ(created.disposition, CommitDisposition::Committed);
        return TResponse{200, ""};
    });
    std::filesystem::path metadata;
    const auto root = temporary.path / "server/databases" / id;
    for (const auto& file : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        if (file.path().string().ends_with(".metadata.json")) {
            metadata = file.path();
            break;
        }
    }
    ASSERT_FALSE(metadata.empty());
    const auto saved = temporary.path / "saved-metadata";
    std::filesystem::rename(metadata, saved);
    EXPECT_THROW(databases.Change(id, "reconcile"), TFailure);
    const auto closed = databases.Change(id, "close");
    EXPECT_EQ(closed.at("state"), "closed");
    EXPECT_TRUE(closed.contains("error"));
    EXPECT_THROW(databases.Change(id, "reconcile"), TFailure);
    std::filesystem::rename(saved, metadata);
    {
        const auto competing = OpenExisting({(temporary.path / "server/databases" / id).string(), id});
        ASSERT_TRUE(competing);
        EXPECT_TRUE((*competing)->Shutdown(1s));
    }
    EXPECT_EQ(databases.Change(id, "open").at("state"), "open");
    databases.Shutdown();
}

TEST(Databases, RegistryCommitFailuresNeverRecreateStores)
{
    for (const int commit : {1, 2}) {
        for (const bool after : {false, true}) {
            SCOPED_TRACE(std::to_string(commit) + (after ? " after unlink" : " before unlink"));
            test::TempDir temporary;
            test::JournalFaultVfs vfs;
            const auto directory = temporary.path / "server";
            const auto root = directory / "databases" / test::store_id;
            {
                TDatabases databases(directory, vfs.Name());
                vfs.Arm(commit, after);
                EXPECT_THROW(databases.Create({{"name", "a"}, {"database_id", test::store_id}}), detail::Failure);
                // No store I/O follows an uncertain reservation; a failed final
                // registry update retains the complete store for restart.
                EXPECT_EQ(std::filesystem::exists(root), commit == 2);
                EXPECT_THROW(databases.Create({{"name", "other"}}), TFailure);
                vfs.Disarm();
                databases.Shutdown();
            }
            TDatabases reopened(directory);
            const auto value = reopened.Create({{"name", "a"}, {"database_id", test::store_id}});
            EXPECT_EQ(value.at("state"), "open");
            EXPECT_EQ(reopened.List().at("items").size(), 1U);
            reopened.Shutdown();
        }
    }
}

TEST(Databases, ReservationCrashAndIncompleteStoreRemainExplicit)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    {
        TDatabases databases(directory);
        databases.Shutdown();
    }
    {
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
        registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
            {test::store_id, "reserved"});
    }
    {
        TDatabases databases(directory);
        EXPECT_EQ(databases.Get(test::store_id).at("state"), "error");
        EXPECT_THROW(databases.Change(test::store_id, "open"), TFailure);
        EXPECT_EQ(databases.Create({{"name", "reserved"}}).at("state"), "open");
        databases.Shutdown();
    }
    const auto other = directory / "databases" / test::table_uuid;
    std::filesystem::create_directory(other);
    {
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
        registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
            {test::table_uuid, "incomplete"});
    }
    {
        TDatabases databases(directory);
        EXPECT_EQ(databases.Get(test::table_uuid).at("state"), "error");
        EXPECT_EQ(databases.Create({{"name", "incomplete"}}).at("state"), "open");
        EXPECT_TRUE(std::filesystem::exists(other / "store.ready"));
        size_t preserved = 0;
        for (const auto& item : std::filesystem::directory_iterator(directory / "databases")) {
            if (item.path().filename().string().starts_with("incomplete-")) {
                ++preserved;
                EXPECT_TRUE(std::filesystem::is_empty(item.path()));
            }
        }
        EXPECT_EQ(preserved, 0U);
        EXPECT_EQ(databases.Get(test::store_id).at("state"), "open");
        databases.Shutdown();
    }
    std::filesystem::remove(directory / "registry.sqlite");
    EXPECT_THROW(TDatabases databases(directory), TFailure);
}
TEST(Databases, DefiniteBusyRegistryFailureCanRetryWithoutRestart)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    const auto id = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
    detail::Database reader;
    reader.Open(temporary.path / "server/registry.sqlite", SQLITE_OPEN_READONLY, "");
    reader.Execute("BEGIN");
    EXPECT_EQ(reader.Integer("SELECT count(*) FROM databases"), 1);
    try {
        databases.Change(id, "close");
        ADD_FAILURE() << "reader must prevent registry commit";
    } catch (const detail::Failure& error) {
        EXPECT_EQ(error.native_code & 255, SQLITE_BUSY);
    }
    EXPECT_EQ(databases.Get(id).at("state"), "open");
    EXPECT_NO_THROW(databases.Use(id, [](const auto&) {
        return TResponse{200, ""};
    }));
    reader.Execute("ROLLBACK");
    EXPECT_EQ(databases.Change(id, "close").at("state"), "closed");
    EXPECT_EQ(databases.Create({{"name", "b"}}).at("state"), "open");
    databases.Shutdown();
}

TEST(Databases, StartupDoesNotRewriteAlreadyPersistedOpenIntent)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    std::string id;
    {
        TDatabases databases(directory);
        id = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
        databases.Shutdown();
    }
    test::JournalFaultVfs vfs;
    vfs.Arm(1, true);
    TDatabases reopened(directory, vfs.Name());
    EXPECT_EQ(reopened.Get(id).at("state"), "open");
    EXPECT_THROW(reopened.Create({{"name", "b"}}), detail::Failure);
    vfs.Disarm();
    EXPECT_EQ(reopened.Change(id, "reconcile").at("state"), "open");
    EXPECT_NO_THROW(reopened.Use(id, [](const auto&) {
        return TResponse{200, ""};
    }));
    EXPECT_EQ(reopened.Change(id, "close").at("state"), "closed");
    reopened.Shutdown();
}

TEST(Databases, UncertainCloseFencesCallsButAllowsOutcomeRecovery)
{
    test::TempDir temporary;
    test::JournalFaultVfs vfs;
    TDatabases databases(temporary.path / "server", vfs.Name());
    const auto id = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
    vfs.Arm(1, true);
    EXPECT_THROW(databases.Change(id, "close"), detail::Failure);
    vfs.Disarm();
    EXPECT_THROW(databases.Use(id,
                     [](const auto&) {
                         return TResponse{200, ""};
                     }),
        TFailure);
    EXPECT_NO_THROW(databases.Use(
        id,
        [](const auto&) {
            return TResponse{200, ""};
        },
        true));
    EXPECT_THROW(databases.Change(id, "reconcile"), TFailure);
    EXPECT_THROW(databases.Use(id,
                     [](const auto&) {
                         return TResponse{200, ""};
                     }),
        TFailure);
    EXPECT_EQ(databases.Change(id, "close").at("state"), "closed");
    EXPECT_THROW(databases.Change(id, "open"), TFailure);
    EXPECT_THROW(databases.Create({{"name", "a"}}), TFailure);
    databases.Shutdown();
}

TEST(Databases, UnknownDatabaseHasDistinctAbsenceCode)
{
    test::TempDir temporary;
    TDatabases databases(temporary.path / "server");
    try {
        databases.Get(test::store_id);
        ADD_FAILURE() << "unknown database must fail";
    } catch (const TFailure& error) {
        EXPECT_EQ(error.Detail.code, "DATABASE_NOT_FOUND");
        EXPECT_EQ(error.Detail.retry_hint, RetryHint::FixInput);
    }
    databases.Shutdown();
}

TEST(Databases, DefiniteRegistrationFailureStaysFencedUntilPersistedRetry)
{
    test::TempDir temporary;
    const auto root = temporary.path / "server";
    std::string id;
    {
        TDatabases databases(root);
        id = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
        databases.Shutdown();
    }
    detail::Database reader;
    reader.Open(root / "registry.sqlite", SQLITE_OPEN_READWRITE, {});
    reader.Execute("UPDATE databases SET initialized=0 WHERE id=?", {id});
    reader.Execute("BEGIN");
    EXPECT_EQ(reader.Integer("SELECT initialized FROM databases"), 0);
    TDatabases databases(root);
    EXPECT_EQ(databases.Get(id).at("state"), "error");
    EXPECT_THROW(databases.Change(id, "reconcile"), detail::Failure);
    EXPECT_THROW(databases.Use(id,
                     [](const auto&) {
                         return TResponse{200, ""};
                     }),
        TFailure);
    reader.Execute("ROLLBACK");
    EXPECT_EQ(databases.Change(id, "reconcile").at("state"), "open");
    EXPECT_EQ(reader.Integer("SELECT initialized FROM databases"), 1);
    databases.Shutdown();
}

TEST(Databases, StartupRegistrationFaultDoesNotHideOtherInitializedDatabases)
{
    test::TempDir temporary;
    test::JournalFaultVfs vfs;
    const auto root = temporary.path / "server";
    const std::string first = "00000000-0000-4000-8000-000000000001";
    const std::string second = "00000000-0000-4000-8000-000000000002";
    {
        TDatabases databases(root);
        databases.Create({{"name", "a"}, {"database_id", first}});
        databases.Create({{"name", "b"}, {"database_id", second}});
        databases.Shutdown();
    }
    {
        detail::Database registry;
        registry.Open(root / "registry.sqlite", SQLITE_OPEN_READWRITE, {});
        registry.Execute("UPDATE databases SET initialized=0 WHERE id=?", {first});
    }
    vfs.Arm(1, true);
    TDatabases databases(root, vfs.Name());
    vfs.Disarm();
    EXPECT_EQ(databases.Get(first).at("state"), "error");
    EXPECT_EQ(databases.Get(second).at("state"), "open");
    EXPECT_NO_THROW(databases.Use(second, [](const auto&) {
        return TResponse{200, ""};
    }));
    EXPECT_THROW(databases.Change(first, "reconcile"), TFailure);
    EXPECT_THROW(databases.Use(first,
                     [](const auto&) {
                         return TResponse{200, ""};
                     }),
        TFailure);
    EXPECT_NO_THROW(databases.Use(
        first,
        [](const auto&) {
            return TResponse{200, ""};
        },
        true));
    EXPECT_EQ(databases.Change(first, "close").at("state"), "closed");
    databases.Shutdown();
}

TEST(Databases, DiscardReservationReclaimsDisposableFilesAndReleasesNameAndCapacity)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    {
        TDatabases databases(directory);
    }
    {
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
        registry.Execute("BEGIN IMMEDIATE");
        for (int i = 1; i <= 64; ++i) {
            const auto number = std::to_string(i);
            const auto id = "00000000-0000-0000-0000-" + std::string(12 - number.size(), '0') + number;
            registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
                {id, "reserved-" + number});
        }
        registry.Execute("COMMIT");
    }
    const auto incomplete = directory / "databases" / test::store_id;
    std::filesystem::create_directory(incomplete);
    {
        std::ofstream(incomplete / "store.identity") << "partial initialization";
    }
    {
        TDatabases databases(directory);
        try {
            databases.Create({{"name", "overflow"}});
            FAIL() << "registry capacity must fail";
        } catch (const TFailure& error) {
            EXPECT_EQ(error.Detail.code, "RESOURCE_EXHAUSTED");
            EXPECT_EQ(error.Detail.retry_hint, RetryHint::OperatorAction);
        }
        EXPECT_EQ(databases.Change(test::store_id, "discard").at("discarded"), true);
        EXPECT_THROW(databases.Get(test::store_id), TFailure);
        EXPECT_EQ(databases.List().at("items").size(), 63U);
        EXPECT_FALSE(std::filesystem::exists(incomplete));
        size_t preserved = 0;
        for (const auto& item : std::filesystem::directory_iterator(directory / "databases")) {
            if (item.path().filename().string().starts_with("incomplete-")) {
                ++preserved;
            }
        }
        EXPECT_EQ(preserved, 0U);
        EXPECT_NE(databases.Create({{"name", "reserved-1"}}).at("database_id"), test::store_id);
        EXPECT_EQ(databases.List().at("items").size(), 64U);
        databases.Shutdown();
    }
    TDatabases reopened(directory);
    EXPECT_EQ(reopened.List().at("items").size(), 64U);
    EXPECT_THROW(reopened.Get(test::store_id), TFailure);
    reopened.Shutdown();
}

TEST(Databases, DiscardRefusesInitializedOrReadyStores)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    {
        TDatabases databases(directory);
        databases.Create({{"name", "initialized"}, {"database_id", test::store_id}});
        databases.Shutdown();
    }
    {
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
        registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,0)",
            {test::table_uuid, "ready-but-unregistered"});
    }
    const auto ready = directory / "databases" / test::table_uuid;
    {
        auto engine = CreateStore({test::table_uuid, ready.string()});
        ASSERT_TRUE(engine);
        ASSERT_TRUE((*engine)->Shutdown(1s));
    }
    TDatabases databases(directory);
    for (const auto* id : {test::store_id, test::table_uuid}) {
        try {
            databases.Change(id, "discard");
            FAIL() << "discard must refuse initialized or ready stores";
        } catch (const TFailure& error) {
            EXPECT_EQ(error.Detail.code, "PRECONDITION_FAILED");
        }
    }
    EXPECT_EQ(databases.List().at("items").size(), 2U);
    EXPECT_TRUE(std::filesystem::exists(ready / "store.ready"));
    EXPECT_EQ(databases.Get(test::store_id).at("state"), "open");
    databases.Shutdown();
}

TEST(Databases, UncertainDiscardRequiresRegistryRecoveryBeforeReuse)
{
    for (const bool after : {false, true}) {
        test::TempDir temporary;
        test::JournalFaultVfs vfs;
        const auto directory = temporary.path / "server";
        {
            TDatabases databases(directory);
        }
        {
            detail::Database registry;
            registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
            registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
                {test::store_id, "reserved"});
        }
        {
            TDatabases databases(directory, vfs.Name());
            vfs.Arm(1, after);
            EXPECT_THROW(databases.Change(test::store_id, "discard"), detail::Failure);
            EXPECT_THROW(databases.Create({{"name", "reserved"}}), TFailure);
            vfs.Disarm();
            databases.Shutdown();
        }
        TDatabases reopened(directory);
        EXPECT_EQ(reopened.List().at("items").size(), after ? 0U : 1U);
        if (!after) {
            EXPECT_EQ(reopened.Change(test::store_id, "discard").at("discarded"), true);
        }
        EXPECT_EQ(reopened.List().at("items").size(), 0U);
    }
}

TEST(Databases, InvalidReadyMarkerCanBeDiscardedOrRetriedWithoutHiddenDirectories)
{
    for (const bool discard : {false, true}) {
        for (const auto* marker : {"", "GLAMDB\n1\nwrong-id\nlocal-delete-extra-v1\n"}) {
            SCOPED_TRACE(discard ? "discard" : "retry create");
            test::TempDir temporary;
            const auto directory = temporary.path / "server";
            {
                TDatabases databases(directory);
            }
            {
                detail::Database registry;
                registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
                registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
                    {test::store_id, "incomplete"});
            }
            const auto root = directory / "databases" / test::store_id;
            std::filesystem::create_directory(root);
            std::filesystem::create_directory(root / "warehouse");
            std::ofstream(root / "store.identity") << "GLAMDB\n1\n" << test::store_id << "\nlocal-delete-extra-v1\n";
            std::ofstream(root / "store.ready") << marker;
            TDatabases databases(directory);
            ASSERT_EQ(databases.Get(test::store_id).at("state"), "error");
            if (discard) {
                EXPECT_EQ(databases.Change(test::store_id, "discard").at("discarded"), true);
                EXPECT_FALSE(std::filesystem::exists(root));
            } else {
                EXPECT_EQ(databases.Create({{"name", "incomplete"}}).at("state"), "open");
                EXPECT_EQ(databases.Get(test::store_id).at("database_id"), test::store_id);
            }
            for (const auto& item : std::filesystem::directory_iterator(directory / "databases")) {
                EXPECT_FALSE(item.path().filename().string().starts_with("incomplete-"));
            }
            databases.Shutdown();
        }
    }
}

TEST(Databases, IncompleteCleanupPreservesUnknownFilesAndSymlinks)
{
    for (const std::string kind : {"unknown", "marker-symlink", "root-symlink", "nonempty-warehouse"}) {
        SCOPED_TRACE(kind);
        test::TempDir temporary;
        const auto directory = temporary.path / "server";
        {
            TDatabases databases(directory);
        }
        {
            detail::Database registry;
            registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
            registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
                {test::store_id, "incomplete"});
        }
        const auto root = directory / "databases" / test::store_id;
        const auto evidence = temporary.path / "evidence";
        std::ofstream(evidence) << "retain";
        std::filesystem::path retained;
        if (kind == "root-symlink") {
            const auto target = temporary.path / "external-store";
            std::filesystem::create_directory(target);
            std::ofstream(target / "store.ready") << "retain";
            std::filesystem::create_directory_symlink(target, root);
            retained = target / "store.ready";
        } else {
            std::filesystem::create_directory(root);
            if (kind == "marker-symlink") {
                std::filesystem::create_symlink(evidence, root / "store.ready");
                retained = root / "store.ready";
            } else if (kind == "nonempty-warehouse") {
                std::filesystem::create_directory(root / "warehouse");
                retained = root / "warehouse/data";
                std::ofstream(retained) << "retain";
            } else {
                retained = root / "unknown";
                std::ofstream(retained) << "retain";
            }
        }
        TDatabases databases(directory);
        EXPECT_THROW(databases.Create({{"name", "incomplete"}}), TFailure);
        EXPECT_THROW(databases.Change(test::store_id, "discard"), TFailure);
        EXPECT_TRUE(std::filesystem::exists(root));
        EXPECT_TRUE(std::filesystem::exists(evidence));
        EXPECT_TRUE(std::filesystem::exists(retained));
        databases.Shutdown();
    }
}

TEST(Databases, ReadyStoreWithAnotherIdentityCannotBeReclaimed)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    {
        TDatabases databases(directory);
    }
    {
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
        registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,0)",
            {test::store_id, "reserved"});
    }
    const auto root = directory / "databases" / test::store_id;
    {
        auto engine = CreateStore({test::table_uuid, root.string()});
        ASSERT_TRUE(engine);
        ASSERT_TRUE((*engine)->Shutdown(1s));
    }
    const auto bytes = std::filesystem::file_size(root / "catalog.sqlite");
    TDatabases databases(directory);
    EXPECT_THROW(databases.Create({{"name", "reserved"}}), TFailure);
    EXPECT_THROW(databases.Change(test::store_id, "discard"), TFailure);
    EXPECT_EQ(std::filesystem::file_size(root / "catalog.sqlite"), bytes);
    auto engine = OpenExisting({root.string(), test::table_uuid});
    ASSERT_TRUE(engine);
    ASSERT_TRUE((*engine)->Shutdown(1s));
    databases.Shutdown();
}

TEST(Databases, IncompleteCleanupCannotRemoveOwnedDirectory)
{
    test::TempDir temporary;
    const auto directory = temporary.path / "server";
    {
        TDatabases databases(directory);
    }
    {
        detail::Database registry;
        registry.Open(directory / "registry.sqlite", SQLITE_OPEN_READWRITE, "");
        registry.Execute("INSERT INTO databases(id,name,initialized,auto_open) VALUES(?,?,0,1)",
            {test::store_id, "incomplete"});
    }
    const auto root = directory / "databases" / test::store_id;
    auto lease = detail::StoreLease::Create(root, {test::store_id, "local-delete-extra-v1", 1});
    TDatabases databases(directory);
    EXPECT_THROW(databases.Create({{"name", "incomplete"}}), TFailure);
    EXPECT_THROW(databases.Change(test::store_id, "discard"), TFailure);
    EXPECT_TRUE(std::filesystem::exists(root / "store.identity"));
    lease.reset();
    EXPECT_EQ(databases.Change(test::store_id, "discard").at("discarded"), true);
    databases.Shutdown();
}

TEST(Databases, RegistryRestartFenceRejectsReconcileWithOperatorAction)
{
    test::TempDir temporary;
    test::JournalFaultVfs vfs;
    TDatabases databases(temporary.path / "server", vfs.Name());
    const auto id = databases.Create({{"name", "a"}}).at("database_id").get<std::string>();
    vfs.Arm(1, true);
    EXPECT_THROW(databases.Change(id, "close"), detail::Failure);
    vfs.Disarm();
    EXPECT_EQ(databases.Get(id).at("error").at("retry_hint"), "OperatorAction");
    try {
        databases.Change(id, "reconcile");
        FAIL() << "reconcile cannot resolve an uncertain registry";
    } catch (const TFailure& error) {
        EXPECT_EQ(error.Detail.code, "RECOVERY_REQUIRED");
        EXPECT_EQ(error.Detail.retry_hint, RetryHint::OperatorAction);
    }
    EXPECT_EQ(databases.Get(id).at("state"), "error");
    databases.Shutdown();
}

} // namespace glamdb::NServer
