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

TEST_F(CatalogTest, OperationCapacityStaysExactAcrossFailuresRollbacksAndReopens)
{
    CreateCollection();
    const std::string other = "00000000-0000-0000-0000-0000000000b2";
    catalog->PublishCreate(
        {other, "other", "00000000-0000-0000-0000-0000000000b3", SchemaSpec{{{"n", FieldType::Int64, true}}}, 0, {}},
        [](std::string_view) -> Status {
            return {};
        },
        [&] {
            return catalog->InsertTable("columnar", "other", Location("other"));
        });
    const auto path = temporary.path / "catalog.sqlite";
    Limits limits;
    limits.max_operations = 4;
    catalog.reset();
    catalog = DurableSqliteCatalogStore::Open(path, info, limits);
    const auto id = [](int index) {
        return "00000000-0000-4000-8000-00000000000" + std::to_string(index);
    };
    const auto bind = [&](const std::string& collection, int index) {
        BoundOperation bound{collection,
            id(index),
            std::string(64, 'a'),
            1,
            catalog->Base(collection),
            test::attempt_id};
        catalog->BindOperation(bound);
        return bound;
    };
    const auto refusal = [&](const std::string& collection, int index) -> std::string {
        try {
            bind(collection, index);
            return "BOUND";
        } catch (const Failure& error) {
            return error.detail.code;
        }
    };
    const auto first = Bind();
    const auto failed = bind(other, 2);
    // A failed bound operation keeps its permanent slot.
    catalog->RecordFailure(failed,
        {failed.operation_id,
            CommitDisposition::DefinitelyNotCommitted,
            CommitPhase::Data,
            std::nullopt,
            ErrorDetail{"IO_ERROR", "disk full", RetryHint::NewOperationId},
            std::nullopt});
    // Rolled-back transactions consume nothing.
    EXPECT_EQ(refusal(other, 2), "ALREADY_EXISTS");
    EXPECT_THROW(catalog->PublishAppend(
                     first,
                     Receipt(),
                     [](std::string_view) -> Status {
                         return {};
                     },
                     []() -> iceberg::Status {
                         return iceberg::IOError("abort publication");
                     }),
        Failure);
    bind(other, 3);
    bind(std::string(collection_id), 4);
    EXPECT_EQ(refusal(other, 5), "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(catalog->FindOperation(other, id(5)));
    catalog->Reopen();
    EXPECT_EQ(refusal(std::string(collection_id), 5), "RESOURCE_EXHAUSTED");
    // Reopen exactly at the limit, then below and above it.
    catalog.reset();
    ASSERT_NO_THROW(catalog = DurableSqliteCatalogStore::Open(path, info, limits));
    EXPECT_EQ(catalog->Operations().size(), 4U);
    EXPECT_EQ(refusal(other, 5), "RESOURCE_EXHAUSTED");
    catalog.reset();
    limits.max_operations = 3;
    try {
        catalog = DurableSqliteCatalogStore::Open(path, info, limits);
        FAIL() << "lower operation budget accepted";
    } catch (const Failure& error) {
        EXPECT_EQ(error.detail.code, "CONFIG_TOO_SMALL");
    }
    limits.max_operations = 6;
    catalog = DurableSqliteCatalogStore::Open(path, info, limits);
    bind(other, 5);
    catalog.reset();
    // Rows written by another connection while the catalog is closed count on open.
    Database raw;
    raw.Open(path, SQLITE_OPEN_READWRITE, {});
    raw.Execute("INSERT INTO operations SELECT collection_id,?,payload_hash,hash_version,state,base_identity,"
                "attempt_id,receipt_version,outcome_blob FROM operations WHERE operation_id=?",
        {id(6), id(5)});
    raw.Close();
    catalog = DurableSqliteCatalogStore::Open(path, info, limits);
    EXPECT_EQ(refusal(std::string(collection_id), 7), "RESOURCE_EXHAUSTED");
    EXPECT_EQ(catalog->Operations().size(), 6U);
    catalog->ValidateInventory();
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

TEST_F(CatalogTest, ExpirationInventoryPagesBeyondTheGeneralQueryRowLimit)
{
    CreateCollection();
    Bind();
    const auto base = catalog->Base(collection_id);
    catalog->PublishExpiration(
        base,
        {},
        {},
        [](std::string_view) -> Status {
            return {};
        },
        [&]() -> iceberg::Status {
            const auto updated = catalog->UpdateTableMetadataLocation("columnar",
                "events",
                Location("maintenance"),
                base.metadata_location,
                base.metadata_location);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            return {};
        });
    catalog.reset();
    Database raw;
    raw.Open(temporary.path / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    raw.Execute("WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<8192) "
                "INSERT INTO operations SELECT collection_id,printf('16800000-0000-4000-8000-%012x',n),"
                "payload_hash,hash_version,state,base_identity,attempt_id,receipt_version,outcome_blob "
                "FROM operations,numbers");
    raw.Execute("INSERT INTO expired_snapshots SELECT collection_id,row_number() OVER(ORDER BY operation_id),"
                "row_number() OVER(ORDER BY operation_id),operation_id FROM operations");
    raw.Close();
    Limits limits;
    limits.max_operations = 10000;
    catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, limits);
    const auto expired = catalog->ExpiredSnapshots(collection_id);
    ASSERT_EQ(expired.size(), 8193U);
    EXPECT_EQ(expired.front().sequence_number, 1);
    EXPECT_EQ(expired.back().sequence_number, 8193);
    EXPECT_EQ(expired.front().snapshot_id, 1);
    EXPECT_EQ(expired.back().snapshot_id, 8193);
}

TEST_F(CatalogTest, CleanupBacklogUsesBoundedBatchesAndAnExactCount)
{
    CreateCollection();
    const auto base = catalog->Base(collection_id);
    std::vector<std::string> garbage;
    garbage.reserve(8193);
    for (int i = 0; i < 8193; ++i) {
        garbage.push_back(Location("obsolete-" + std::to_string(i)));
    }
    catalog->PublishExpiration(
        base,
        {},
        garbage,
        [](std::string_view) -> Status {
            return {};
        },
        [&]() -> iceberg::Status {
            const auto updated = catalog->UpdateTableMetadataLocation("columnar",
                "events",
                Location("maintenance"),
                base.metadata_location,
                base.metadata_location);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            return {};
        });
    EXPECT_EQ(catalog->PendingMetadataCount(collection_id), 8193);
    const auto first = catalog->PendingMetadata(collection_id);
    ASSERT_EQ(first.size(), 8192U);
    catalog->AcknowledgeMetadata(collection_id, first);
    EXPECT_EQ(catalog->PendingMetadataCount(collection_id), 1);
    const auto last = catalog->PendingMetadata(collection_id);
    ASSERT_EQ(last.size(), 1U);
    catalog->AcknowledgeMetadata(collection_id, last);
    EXPECT_EQ(catalog->PendingMetadataCount(collection_id), 0);
    catalog.reset();
    catalog = DurableSqliteCatalogStore::Open(temporary.path / "catalog.sqlite", info, {});
    EXPECT_TRUE(catalog->PendingMetadata(collection_id).empty());
}

} // namespace
} // namespace glamdb::detail
