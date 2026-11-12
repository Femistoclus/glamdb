#include "core/catalog/durable_sqlite_catalog.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/codec.h"
#include "tests/support/checked.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

namespace glamdb::detail {
namespace {

using test::collection_id;
using test::operation_id;
using test::store_id;

class CatalogTest : public testing::Test
{
protected:
    test::TempDir temporary;
    StoreInfo info{store_id, "local-delete-extra-v1", 1};
    std::shared_ptr<DurableSqliteCatalogStore> catalog;

    void SetUp() override
    {
        catalog = DurableSqliteCatalogStore::Create(temporary.path / "catalog.sqlite", info, {});
    }
    CollectionInfo CreateCollection()
    {
        CollectionInfo collection{collection_id,
            "events",
            test::table_uuid,
            SchemaSpec{{{"n", FieldType::Int64, true}}},
            0,
            std::nullopt};
        return catalog->PublishCreate(
            collection,
            [](std::string_view) -> Status {
                return {};
            },
            [&] {
                return catalog->InsertTable("columnar", "events", Location("initial"));
            });
    }
    static std::string Location(std::string_view name)
    {
        return "store://" + std::string(store_id) + "/warehouse/" + std::string(name) + ".json";
    }
    BoundOperation Bind()
    {
        BoundOperation bound{collection_id,
            operation_id,
            std::string(64, 'a'),
            1,
            catalog->Base(collection_id),
            test::attempt_id};
        catalog->BindOperation(bound);
        return bound;
    }
    AppendResult Receipt()
    {
        return {collection_id,
            operation_id,
            std::string(64, 'a'),
            1,
            9007199254740993LL,
            std::nullopt,
            3,
            1,
            123,
            1700000000000000LL,
            info.profile_id,
            1};
    }
};

TEST_F(CatalogTest, WorkingConnectionSettingsAndMissingOpen)
{
    const auto settings = catalog->Profile();
    EXPECT_EQ(settings.journal_mode, "delete");
    EXPECT_EQ(settings.synchronous, 3);
    EXPECT_EQ(settings.foreign_keys, 1);
    EXPECT_EQ(settings.busy_timeout, 5000);
    EXPECT_EQ(settings.cache_size, -8192);
    EXPECT_EQ(settings.mmap_size, 0);
    EXPECT_EQ(settings.sqlite_version, "3.54.1");
    const auto missing = temporary.path / "missing.sqlite";
    EXPECT_THROW(DurableSqliteCatalogStore::Open(missing, info, {}), Failure);
    EXPECT_FALSE(std::filesystem::exists(missing));
    EXPECT_THROW(DurableSqliteCatalogStore::Open(":memory:", info, {}), Failure);
}

TEST_F(CatalogTest, GuardAndOuterCommitOwnPointerAndReceipt)
{
    CreateCollection();
    auto bound = Bind();
    bool guard_ran = false;
    catalog->PublishAppend(
        bound,
        Receipt(),
        [&](std::string_view location) -> Status {
            guard_ran = true;
            EXPECT_EQ(location, Location("next"));
            EXPECT_EQ(catalog->Base(collection_id), bound.base);
            return {};
        },
        [&]() -> iceberg::Status {
            EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).state, OperationState::Bound);
            const auto updated = catalog->UpdateTableMetadataLocation("columnar",
                "events",
                Location("next"),
                bound.base.metadata_location,
                bound.base.metadata_location);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            EXPECT_EQ(*updated, 1);
            return {};
        });
    EXPECT_TRUE(guard_ran);
    EXPECT_EQ(catalog->Base(collection_id).current_snapshot_id, Receipt().snapshot_id);
    const auto before = catalog->FindOperation(collection_id, operation_id);
    ASSERT_EQ(test::Present(before).state, OperationState::Committed);
    EXPECT_EQ(test::Present(test::Present(before).outcome).result, Receipt());
    catalog.reset();
    catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, {});
    EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).outcome_bytes,
        test::Present(before).outcome_bytes);
    EXPECT_EQ(catalog->Profile().synchronous, 3);
    EXPECT_FALSE(catalog->UpdateTableMetadataLocation("columnar",
        "events",
        Location("bad"),
        Location("next"),
        Location("next")));
}

TEST_F(CatalogTest, NestedFailurePoisonsOuterEvenIfCallerIgnoresIt)
{
    CreateCollection();
    const auto bound = Bind();
    EXPECT_THROW(catalog->PublishAppend(
                     bound,
                     Receipt(),
                     [](std::string_view) -> Status {
                         return {};
                     },
                     [&] {
                         const auto ignored = catalog->RunInTransaction([&]() -> iceberg::Status {
                             const auto changed = catalog->UpdateTableMetadataLocation("columnar",
                                 "events",
                                 Location("next"),
                                 bound.base.metadata_location,
                                 bound.base.metadata_location);
                             EXPECT_TRUE(changed);
                             return iceberg::IOError("abort nested scope");
                         });
                         EXPECT_FALSE(ignored);
                         return iceberg::Status{};
                     }),
        Failure);
    EXPECT_EQ(catalog->Base(collection_id), bound.base);
    EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).state, OperationState::Bound);
}

TEST_F(CatalogTest, ExactBaseRejectsMetadataOnlyChangeAndRequiresGuard)
{
    CreateCollection();
    auto bound = Bind();
    bound.base.metadata_location = Location("different-same-null-snapshot");
    bool body_ran = false;
    EXPECT_THROW(catalog->PublishAppend(
                     bound,
                     Receipt(),
                     [](std::string_view) -> Status {
                         return {};
                     },
                     [&] {
                         body_ran = true;
                         return iceberg::Status{};
                     }),
        Failure);
    EXPECT_FALSE(body_ran);
    bound.base = catalog->Base(collection_id);
    EXPECT_THROW(catalog->PublishAppend(bound,
                     Receipt(),
                     {},
                     [] {
                         return iceberg::Status{};
                     }),
        Failure);
}

TEST_F(CatalogTest, TransactionExceptionKeepsACompleteUtf8Diagnostic)
{
    CreateCollection();
    const auto bound = Bind();
    const std::string prefix(2047, 'x');
    std::optional<ErrorDetail> diagnostic;
    try {
        catalog->PublishAppend(
            bound,
            Receipt(),
            [](std::string_view) -> Status {
                return {};
            },
            [&]() -> iceberg::Status {
                throw std::runtime_error(prefix + "я");
            });
        FAIL() << "publication should have thrown";
    } catch (const Failure& error) {
        diagnostic = error.detail;
        EXPECT_FALSE(error.uncertain);
    }
    ASSERT_TRUE(diagnostic);
    EXPECT_EQ(test::Present(diagnostic).code, "IO_ERROR");
    EXPECT_EQ(test::Present(diagnostic).message, prefix);
    const AppendOutcome failure{operation_id,
        CommitDisposition::DefinitelyNotCommitted,
        CommitPhase::CatalogTransaction,
        std::nullopt,
        diagnostic,
        std::nullopt};
    ASSERT_TRUE(EncodeOutcome(failure));
    catalog->RecordFailure(bound, failure);
    catalog->Reopen();
    EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).outcome, failure);
}

TEST_F(CatalogTest, PermanentBindingConflictFailureCapacityAndFence)
{
    CreateCollection();
    const auto bound = Bind();
    EXPECT_THROW(catalog->BindOperation(bound), Failure);
    const AppendOutcome failure{operation_id,
        CommitDisposition::DefinitelyNotCommitted,
        CommitPhase::Data,
        std::nullopt,
        ErrorDetail{"IO_ERROR", "disk full", RetryHint::NewOperationId},
        std::nullopt};
    catalog->RecordFailure(bound, failure);
    EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).outcome, failure);
    EXPECT_THROW(catalog->RecordFailure(bound, failure), Failure);
    const auto initial = catalog->FenceValue();
    catalog->DurabilityFence();
    EXPECT_EQ(catalog->FenceValue(), initial + 1);
    catalog.reset();
    Limits limits;
    limits.max_operations = 1;
    catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, limits);
    auto second = bound;
    second.operation_id = "00000000-0000-0000-0000-000000000005";
    EXPECT_THROW(catalog->BindOperation(second), Failure);
    EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).outcome, failure);
}

TEST_F(CatalogTest, LargeOperationInventoryReopensAndRejectsLowerLimitsWithoutDroppingBindings)
{
    CreateCollection();
    const auto original = Bind();
    catalog.reset();
    Database raw;
    raw.Open(temporary.path / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    raw.Execute("WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<8192) "
                "INSERT INTO operations SELECT collection_id,printf('15600000-0000-4000-8000-%012x',n),"
                "payload_hash,hash_version,state,base_identity,attempt_id,receipt_version,outcome_blob "
                "FROM operations,numbers");
    raw.Close();
    Limits limits;
    limits.max_operations = 10000;
    ASSERT_NO_THROW(catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, limits));
    EXPECT_EQ(catalog->Operations().size(), 8193);
    EXPECT_EQ(test::Present(catalog->FindOperation(collection_id, operation_id)).binding, original);
    catalog.reset();
    limits.max_operations = 8192;
    try {
        catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, limits);
        FAIL() << "insufficient operation budget accepted";
    } catch (const Failure& error) {
        EXPECT_EQ(error.detail.code, "CONFIG_TOO_SMALL");
    }
    limits.max_operations = 10000;
    ASSERT_NO_THROW(catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, limits));
    EXPECT_EQ(catalog->Operations().size(), 8193);
}

TEST_F(CatalogTest, IncompatibleVersionOrMissingSchemaNeverReinitializes)
{
    catalog.reset();
    sqlite3* connection = nullptr;
    ASSERT_EQ(sqlite3_open((temporary.path / "catalog.sqlite").c_str(), &connection), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(connection, "PRAGMA user_version=2", nullptr, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_close(connection), SQLITE_OK);
    EXPECT_THROW(DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, {}), Failure);
}

TEST_F(CatalogTest, WideSchemaAndHashVersionsDoNotAliasSupportedInt32Values)
{
    CreateCollection();
    Bind();
    catalog.reset();
    sqlite3* connection = nullptr;
    ASSERT_EQ(sqlite3_open((temporary.path / "catalog.sqlite").c_str(), &connection), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(connection, "UPDATE operations SET hash_version=4294967297", nullptr, nullptr, nullptr),
        SQLITE_OK);
    ASSERT_EQ(sqlite3_close(connection), SQLITE_OK);
    EXPECT_THROW(DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, {}), Failure);
    ASSERT_EQ(sqlite3_open((temporary.path / "catalog.sqlite").c_str(), &connection), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(connection, "UPDATE operations SET hash_version=1", nullptr, nullptr, nullptr), SQLITE_OK);
    ASSERT_EQ(sqlite3_exec(connection,
                  "UPDATE collections SET schema_id=4294967296,spec_id=4294967296",
                  nullptr,
                  nullptr,
                  nullptr),
        SQLITE_OK);
    ASSERT_EQ(sqlite3_close(connection), SQLITE_OK);
    EXPECT_THROW(DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, {}), Failure);
}

} // namespace
} // namespace glamdb::detail
