// The catalog's in-memory operation count stays exact across KV no-op receipts
// and migration ownership/population, including rolled-back and lost COMMITs.

#include "core/catalog/durable_sqlite_catalog.h"

#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <gtest/gtest.h>

namespace glamdb::detail {
namespace {

constexpr char target_id[] = "00000000-0000-4000-8000-0000000000b2";
constexpr char target_table[] = "00000000-0000-4000-8000-0000000000b3";

class CatalogCountFaultTest : public testing::Test
{
protected:
    test::JournalFaultVfs vfs;
    test::TempDir temporary;
    std::filesystem::path path = temporary.path / "catalog.sqlite";
    StoreInfo info{test::store_id, "local-delete-extra-v1", 1};
    Limits limits;
    std::shared_ptr<DurableSqliteCatalogStore> catalog;
    const std::string owner = std::string(64, 'b');

    void SetUp() override
    {
        limits.max_operations = 3;
        catalog = DurableSqliteCatalogStore::Create(path, info, limits, vfs.Name());
    }
    static std::string Id(int index)
    {
        return "00000000-0000-4000-8000-00000000000" + std::to_string(index);
    }
    static std::string Location(std::string_view name)
    {
        return "store://" + std::string(test::store_id) + "/warehouse/" + std::string(name) + ".json";
    }
    void CreateKV(const std::string& id,
        const std::string& name,
        const std::string& table,
        std::string_view identity = {},
        std::string_view population = {})
    {
        CollectionInfo definition{id, name, table, SchemaSpec{{{"key", FieldType::String, false}}}, 0, std::nullopt};
        definition.mode = CollectionMode::KeyValue;
        catalog->PublishCreate(
            definition,
            [](std::string_view) -> Status {
                return {};
            },
            [&] {
                return catalog->InsertTable("columnar", name, Location(name));
            },
            identity,
            population);
    }
    BoundOperation Binding(const std::string& collection, const std::string& operation)
    {
        return {collection, operation, std::string(64, 'a'), 1, catalog->Base(collection), test::attempt_id};
    }
    // Binds and returns "BOUND", or the error code of the refusal.
    std::string Bind(const std::string& collection, const std::string& operation, std::string_view migration = {})
    {
        try {
            catalog->BindOperation(Binding(collection, operation), migration);
            return "BOUND";
        } catch (const Failure& error) {
            return error.detail.code;
        }
    }
    AppendResult NoOpReceipt(const BoundOperation& binding) const
    {
        AppendResult receipt;
        receipt.collection_id = binding.collection_id;
        receipt.operation_id = binding.operation_id;
        receipt.payload_hash = binding.payload_hash;
        receipt.durability_profile = info.profile_id;
        receipt.receipt_version = 3;
        receipt.mutation_kind = MutationKind::Delete;
        receipt.snapshot_created = false;
        receipt.input_rows = 1;
        return receipt;
    }
    void PublishNoOp(const BoundOperation& binding, bool guardAccepts = true)
    {
        catalog->PublishNoOp(binding, NoOpReceipt(binding), [&](std::string_view) -> Status {
            if (!guardAccepts) {
                return std::unexpected(ErrorDetail{"IO_ERROR", "guard refused", RetryHint::OperatorAction});
            }
            return {};
        });
    }
    // Expects a lost COMMIT result, then reopens as recovery does.
    template <class F>
    void LoseCommit(bool applied, F&& action)
    {
        vfs.Arm(1, applied);
        try {
            action();
            ADD_FAILURE() << "lost COMMIT result was not reported";
        } catch (const Failure& error) {
            EXPECT_TRUE(error.uncertain);
        }
        vfs.Disarm();
        catalog->Reopen();
    }
    std::optional<OperationState> State(const std::string& collection, const std::string& operation)
    {
        const auto record = catalog->FindOperation(collection, operation);
        return record ? std::optional(record->state) : std::nullopt;
    }
    // Exactly at the limit: the next new operation is refused, also after a
    // fresh open; a lower limit is rejected and the full validation agrees.
    void ExpectFull(const std::string& collection)
    {
        EXPECT_EQ(Bind(collection, Id(9)), "RESOURCE_EXHAUSTED");
        EXPECT_EQ(State(collection, Id(9)), std::nullopt);
        catalog.reset();
        catalog = DurableSqliteCatalogStore::Open(path, info, limits);
        EXPECT_EQ(Bind(collection, Id(9)), "RESOURCE_EXHAUSTED");
        EXPECT_EQ(catalog->Operations().size(), 3U);
        catalog->ValidateInventory();
        catalog.reset();
        auto lower = limits;
        lower.max_operations = 2;
        try {
            catalog = DurableSqliteCatalogStore::Open(path, info, lower);
            ADD_FAILURE() << "lower operation budget accepted";
        } catch (const Failure& error) {
            EXPECT_EQ(error.detail.code, "CONFIG_TOO_SMALL");
        }
    }
};

TEST_F(CatalogCountFaultTest, NoOpReceiptsLeaveTheCountExact)
{
    CreateKV(test::collection_id, "keys", test::table_uuid);
    const std::string kv = test::collection_id;
    ASSERT_EQ(Bind(kv, Id(1)), "BOUND");
    PublishNoOp(Binding(kv, Id(1)));
    EXPECT_EQ(State(kv, Id(1)), OperationState::Committed);
    ASSERT_EQ(Bind(kv, Id(2)), "BOUND");
    EXPECT_THROW(PublishNoOp(Binding(kv, Id(2)), false), Failure);
    EXPECT_EQ(State(kv, Id(2)), OperationState::Bound);
    LoseCommit(true, [&] {
        PublishNoOp(Binding(kv, Id(2)));
    });
    EXPECT_EQ(State(kv, Id(2)), OperationState::Committed);
    ASSERT_EQ(Bind(kv, Id(3)), "BOUND");
    LoseCommit(false, [&] {
        PublishNoOp(Binding(kv, Id(3)));
    });
    EXPECT_EQ(State(kv, Id(3)), OperationState::Bound);
    ExpectFull(kv);
}

TEST_F(CatalogCountFaultTest, LostBindingCommitsOfANoOpCollectionAreCountedFromTheTable)
{
    CreateKV(test::collection_id, "keys", test::table_uuid);
    const std::string kv = test::collection_id;
    LoseCommit(false, [&] {
        catalog->BindOperation(Binding(kv, Id(1)));
    });
    EXPECT_EQ(State(kv, Id(1)), std::nullopt);
    LoseCommit(true, [&] {
        catalog->BindOperation(Binding(kv, Id(2)));
    });
    EXPECT_EQ(State(kv, Id(2)), OperationState::Bound);
    ASSERT_EQ(Bind(kv, Id(3)), "BOUND");
    ASSERT_EQ(Bind(kv, Id(4)), "BOUND");
    ExpectFull(kv);
}

TEST_F(CatalogCountFaultTest, MigrationCreationAndPopulationLeaveTheCountExact)
{
    // Target creation records ownership but no operation, even when its
    // COMMIT result is lost.
    LoseCommit(true, [&] {
        CreateKV(target_id, "target", target_table, owner, Id(1));
    });
    ASSERT_EQ(catalog->MigrationIdentity(target_id), owner);
    // Refused before any transaction.
    EXPECT_EQ(Bind(target_id, Id(1)), "OPERATION_CONFLICT");
    EXPECT_EQ(Bind(target_id, Id(1), std::string(64, 'c')), "OPERATION_CONFLICT");
    // Population whose COMMIT rolls back leaves ownership unbound.
    LoseCommit(false, [&] {
        catalog->BindOperation(Binding(target_id, Id(1)), owner);
    });
    EXPECT_EQ(State(target_id, Id(1)), std::nullopt);
    ASSERT_EQ(catalog->MigrationIdentity(target_id), owner);
    // Population whose COMMIT applied is counted once.
    LoseCommit(true, [&] {
        catalog->BindOperation(Binding(target_id, Id(1)), owner);
    });
    EXPECT_EQ(State(target_id, Id(1)), OperationState::Bound);
    // Rolled back inside the transaction: the population is already bound.
    EXPECT_EQ(Bind(target_id, Id(1), owner), "CORRUPTION");
    ASSERT_EQ(catalog->MigrationIdentity(target_id), owner);
    ASSERT_EQ(Bind(target_id, Id(2)), "BOUND");
    ASSERT_EQ(Bind(target_id, Id(3)), "BOUND");
    ExpectFull(target_id);
}

TEST_F(CatalogCountFaultTest, PopulationRefusedAtCapacityLeavesOwnershipUnbound)
{
    limits.max_operations = 1;
    catalog.reset();
    catalog = DurableSqliteCatalogStore::Open(path, info, limits, vfs.Name());
    CreateKV(target_id, "target", target_table, owner, Id(1));
    ASSERT_EQ(Bind(target_id, Id(2)), "BOUND");
    EXPECT_EQ(Bind(target_id, Id(1), owner), "RESOURCE_EXHAUSTED");
    EXPECT_EQ(State(target_id, Id(1)), std::nullopt);
    catalog.reset();
    catalog = DurableSqliteCatalogStore::Open(path, info, limits);
    EXPECT_EQ(catalog->MigrationIdentity(target_id), owner);
    EXPECT_EQ(Bind(target_id, Id(1), owner), "RESOURCE_EXHAUSTED");
    limits.max_operations = 2;
    catalog.reset();
    catalog = DurableSqliteCatalogStore::Open(path, info, limits);
    EXPECT_EQ(Bind(target_id, Id(1), owner), "BOUND");
    EXPECT_EQ(Bind(target_id, Id(3)), "RESOURCE_EXHAUSTED");
    catalog->ValidateInventory();
}

} // namespace
} // namespace glamdb::detail
