#include "core/api/codec.h"
#include "core/catalog/sqlite.h"
#include "tests/support/checked.h"
#include "tests/support/sql_trace.h"
#include "tests/vertical/vertical_support.h"

#include <cstdio>
#include <iostream>

namespace glamdb {
namespace {
using namespace vertical;

// Copies the store's single failed receipt `copies` times with fresh IDs and
// re-encoded outcomes while no engine owns the store.
void AddFailedReceipts(const std::filesystem::path& path, unsigned copies)
{
    detail::Database database;
    database.Open(path, SQLITE_OPEN_READWRITE, {});
    const auto rows = database.Query("SELECT collection_id,payload_hash,base_identity,attempt_id,outcome_blob FROM "
                                     "operations WHERE state=2");
    ASSERT_EQ(rows.size(), 1U);
    const auto& row = rows.front();
    const auto outcome = detail::DecodeOutcome(detail::Required(row, 4));
    ASSERT_TRUE(outcome);
    database.Execute("BEGIN IMMEDIATE");
    for (unsigned index = 1; index <= copies; ++index) {
        auto copy = *outcome;
        char id[37];
        std::snprintf(id, sizeof(id), "0000c10e-0000-4000-8000-%012x", index);
        copy.operation_id = id;
        const auto encoded = detail::EncodeOutcome(copy);
        ASSERT_TRUE(encoded);
        database.Execute("INSERT INTO operations(collection_id,operation_id,payload_hash,hash_version,state,"
                         "base_identity,attempt_id,receipt_version,outcome_blob) VALUES(?,?,?,1,2,?,?,1,?)",
            {detail::Required(row, 0),
                copy.operation_id,
                detail::Required(row, 1),
                detail::Blob{detail::Required(row, 2)},
                detail::Required(row, 3),
                detail::Blob{*encoded}});
    }
    database.Execute("COMMIT");
    database.Close();
}

struct KVWork
{
    std::vector<test::SqlStatement> no_op;
    std::vector<test::SqlStatement> upsert;
};

// Catalog statements of one KV no-op delete and one KV upsert in a freshly
// opened store that holds `receipts` permanent receipts.
KVWork MeasureKV(unsigned receipts)
{
    test::TempDir temporary;
    const auto root = (temporary.path / "store").string();
    Limits limits;
    limits.max_operations = 6000;
    {
        auto created = CreateStore({test::store_id, root, "local-delete-extra-v1", limits});
        EXPECT_TRUE(created);
        CreateCollectionRequest collection{test::collection_id, "keys", Schema};
        collection.mode = CollectionMode::KeyValue;
        EXPECT_EQ((*created)->CreateCollection(collection).disposition, CommitDisposition::Committed);
        const auto failed = (*created)->DeleteDurable(
            {test::collection_id, FirstOperation, {SnapshotPreconditionKind::Equals, 12345}, {"absent"}});
        EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
        EXPECT_TRUE((*created)->Shutdown(std::chrono::seconds(5)));
    }
    AddFailedReceipts(std::filesystem::path(root) / "catalog.sqlite", receipts - 1);
    KVWork work;
    test::SqlTrace trace;
    auto opened = OpenExisting({root, test::store_id, "local-delete-extra-v1", limits});
    EXPECT_TRUE(opened);
    (void)trace.Take();
    EXPECT_EQ((*opened)->DeleteDurable({test::collection_id, SecondOperation, {}, {"absent"}}).disposition,
        CommitDisposition::Committed);
    work.no_op = trace.Take();
    EXPECT_EQ((*opened)
                  ->UpsertDurable({test::collection_id,
                      ThirdOperation,
                      {},
                      {Batch({{"present", 5, std::nullopt, std::nullopt, std::nullopt}})}})
                  .disposition,
        CommitDisposition::Committed);
    work.upsert = trace.Take();
    EXPECT_TRUE((*opened)->Shutdown(std::chrono::seconds(5)));
    return work;
}

TEST(KVOperationCount, KVMutationCatalogWorkDoesNotDependOnOperationHistory)
{
    const auto small = MeasureKV(100);
    const auto large = MeasureKV(5000);
    for (const auto& [label, phase] : {std::pair{"no-op delete", &KVWork::no_op}, std::pair{"upsert", &KVWork::upsert}})
    {
        const auto a = test::Sum(small.*phase);
        const auto b = test::Sum(large.*phase);
        std::cout << label << " statements=" << a.statements << "/" << b.statements << " rows=" << a.rows << "/"
                  << b.rows << " vm_steps=" << a.vm_steps << "/" << b.vm_steps << " page_reads=" << a.page_reads << "/"
                  << b.page_reads << '\n';
        EXPECT_EQ(a.statements, b.statements);
        EXPECT_EQ(a.rows, b.rows);
        EXPECT_EQ(a.vm_steps, b.vm_steps);
        EXPECT_EQ(a.full_scan_steps, b.full_scan_steps);
        EXPECT_EQ(test::CountContaining(large.*phase, "COUNT(*) FROM operations"), 0);
        EXPECT_LE(b.page_reads, a.page_reads + 16);
    }
}

// Each new operation takes exactly one permanent slot: an append, a migration
// population (its target creation takes none) and KV no-op deletes; exact
// retries take none. The in-memory catalog count must refuse exactly at the
// limit, also after reopening.
TEST(KVOperationCount, NoOpsAndMigrationPopulationTakeExactlyOneSlot)
{
    test::TempDir temporary;
    const auto root = (temporary.path / "store").string();
    Limits limits;
    limits.max_operations = 4;
    auto created = CreateStore({test::store_id, root, "local-delete-extra-v1", limits});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "source", Schema}).disposition,
        CommitDisposition::Committed);
    const auto appended = engine->AppendDurable(
        {test::collection_id, FirstOperation, {}, {Batch({{"key", 7, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(appended.disposition, CommitDisposition::Committed);
    MigrationRequest migration;
    migration.source_collection_id = test::collection_id;
    migration.source_snapshot = SnapshotSelector::Id(test::Present(appended.result).snapshot_id);
    migration.target = {SecondCollection, "target", Schema};
    migration.target.mode = CollectionMode::KeyValue;
    migration.operation_id = SecondOperation;
    migration.duplicate_policy = DuplicatePolicy::LastWins;
    const auto migrated = engine->MigrateAppendToKV(migration);
    ASSERT_EQ(migrated.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(migrated.mutation);
    ASSERT_EQ(test::Present(migrated.mutation).disposition, CommitDisposition::Committed);
    const DeleteRequest noOp{SecondCollection, ThirdOperation, {}, {"absent"}};
    const auto deleted = engine->DeleteDurable(noOp);
    ASSERT_EQ(deleted.disposition, CommitDisposition::Committed);
    ASSERT_FALSE(test::Present(deleted.result).snapshot_created);
    // Exact retries resolve from their receipts and bind nothing.
    EXPECT_EQ(engine->DeleteDurable(noOp), deleted);
    EXPECT_EQ(test::Present(engine->MigrateAppendToKV(migration).mutation), test::Present(migrated.mutation));
    const DeleteRequest fourth{SecondCollection, "00000000-0000-4000-8000-0000000000a4", {}, {"absent"}};
    ASSERT_EQ(engine->DeleteDurable(fourth).disposition, CommitDisposition::Committed);
    const DeleteRequest fifth{SecondCollection, "00000000-0000-4000-8000-0000000000a5", {}, {"absent"}};
    const auto refused = engine->DeleteDurable(fifth);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(engine->FindAppendOutcome(SecondCollection, fifth.operation_id)->has_value());
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    auto reopened = OpenExisting({root, test::store_id, "local-delete-extra-v1", limits});
    ASSERT_TRUE(reopened) << reopened.error().message;
    EXPECT_EQ(test::Present((*reopened)->DeleteDurable(fifth).error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ((*reopened)->DeleteDurable(noOp), deleted);
    ASSERT_TRUE((*reopened)->Shutdown(std::chrono::seconds(5)));
    reopened->reset();
    limits.max_operations = 3;
    const auto smaller = OpenExisting({root, test::store_id, "local-delete-extra-v1", limits});
    ASSERT_FALSE(smaller);
    EXPECT_EQ(smaller.error().code, "CONFIG_TOO_SMALL");
}

} // namespace
} // namespace glamdb
