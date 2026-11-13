// Catalog work that must not grow with permanent operation history (#174).
// SqlTrace observes every statement on the store's catalog connections through
// an SQLite auto extension, so production code carries no test hooks.

#include "storage/engine.h"

#include "core/api/codec.h"
#include "core/catalog/durable_sqlite_catalog.h"
#include "core/engine/engine_factory.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/fake_storage.h"
#include "tests/support/sql_trace.h"
#include "tests/support/temp_dir.h"

#include <array>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <map>

#include <gtest/gtest.h>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using detail::Blob;
using detail::Database;
using detail::DurableSqliteCatalogStore;

std::string Uuid(unsigned group, unsigned value)
{
    char text[37];
    std::snprintf(text, sizeof(text), "%08x-0000-4000-8000-%012x", group, value);
    return text;
}

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw std::runtime_error(result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}

void Print(std::string_view label, const std::vector<test::SqlStatement>& statements)
{
    const auto totals = test::Sum(statements);
    std::cout << label << " statements=" << totals.statements << " rows=" << totals.rows
              << " vm_steps=" << totals.vm_steps << " full_scan_steps=" << totals.full_scan_steps
              << " page_reads=" << totals.page_reads
              << " operation_counts=" << test::CountContaining(statements, "COUNT(*) FROM operations")
              << " integrity_checks=" << test::CountContaining(statements, "PRAGMA integrity_check") << '\n';
}

void ExpectSameWork(const std::vector<test::SqlStatement>& small, const std::vector<test::SqlStatement>& large)
{
    const auto a = test::Sum(small);
    const auto b = test::Sum(large);
    EXPECT_EQ(a.statements, b.statements);
    EXPECT_EQ(a.rows, b.rows);
    EXPECT_EQ(a.vm_steps, b.vm_steps);
    EXPECT_EQ(a.full_scan_steps, b.full_scan_steps);
    ASSERT_EQ(small.size(), large.size());
    for (size_t index = 0; index < small.size(); ++index) {
        EXPECT_EQ(small[index].sql, large[index].sql) << index;
    }
}

// Copies the store's single Failed operation `copies` times with fresh IDs and
// re-encoded receipts, in one transaction, while no engine owns the store.
void AddFailedOperations(const std::filesystem::path& path, unsigned copies)
{
    Database database;
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
        copy.operation_id = Uuid(0xc10e, index);
        const auto encoded = detail::EncodeOutcome(copy);
        ASSERT_TRUE(encoded);
        database.Execute("INSERT INTO operations(collection_id,operation_id,payload_hash,hash_version,state,"
                         "base_identity,attempt_id,receipt_version,outcome_blob) VALUES(?,?,?,1,2,?,?,1,?)",
            {detail::Required(row, 0),
                copy.operation_id,
                detail::Required(row, 1),
                Blob{detail::Required(row, 2)},
                detail::Required(row, 3),
                Blob{*encoded}});
    }
    database.Execute("COMMIT");
    database.Close();
}

struct StoreWork
{
    std::vector<test::SqlStatement> open;
    std::vector<test::SqlStatement> committed;
    std::vector<test::SqlStatement> failed;
};

// Opens a store holding `operations` permanent receipts in a fresh connection,
// then performs one committed and one failed append.
StoreWork MeasureStore(unsigned operations)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto storage = std::make_shared<test::FakeStorage>(root);
    Limits limits;
    limits.max_operations = 6000;
    const auto append = [](Engine& engine, unsigned index) {
        return engine.AppendDurable({test::collection_id, Uuid(0xa, index), {}, {test::Batch()}});
    };
    {
        auto engine = Take(
            detail::EngineFactory::Create({test::store_id, root.string(), "local-delete-extra-v1", limits}, storage));
        EXPECT_EQ(engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                      .disposition,
            CommitDisposition::Committed);
        EXPECT_EQ(append(*engine, 1).disposition, CommitDisposition::Committed);
        storage->fail_prepare = true;
        EXPECT_EQ(append(*engine, 2).disposition, CommitDisposition::DefinitelyNotCommitted);
        storage->fail_prepare = false;
        EXPECT_TRUE(engine->Shutdown(5s));
    }
    AddFailedOperations(root / "catalog.sqlite", operations - 2);
    StoreWork work;
    test::SqlTrace trace;
    auto engine =
        Take(detail::EngineFactory::Open({root.string(), test::store_id, "local-delete-extra-v1", limits}, storage));
    work.open = trace.Take();
    EXPECT_EQ(append(*engine, 3).disposition, CommitDisposition::Committed);
    work.committed = trace.Take();
    storage->fail_prepare = true;
    EXPECT_EQ(append(*engine, 4).disposition, CommitDisposition::DefinitelyNotCommitted);
    work.failed = trace.Take();
    EXPECT_TRUE(engine->Shutdown(5s));
    return work;
}

TEST(CatalogHistoryCost, AppendCatalogWorkDoesNotDependOnOperationHistory)
{
    const auto small = MeasureStore(100);
    const auto large = MeasureStore(5000);
    for (const auto& [label, phase] :
        {std::pair{"committed append", &StoreWork::committed}, std::pair{"failed append", &StoreWork::failed}})
    {
        Print(std::string(label) + " operations=100", small.*phase);
        Print(std::string(label) + " operations=5000", large.*phase);
        ExpectSameWork(small.*phase, large.*phase);
        EXPECT_EQ(test::CountContaining(large.*phase, "FROM operations WHERE (collection_id,operation_id)>"), 0);
        EXPECT_EQ(test::CountContaining(large.*phase, "COUNT(*) FROM operations"), 0);
        // The B-tree descent of indexed point reads and inserts may deepen;
        // nothing may visit a page per stored operation.
        EXPECT_LE(test::Sum(large.*phase).page_reads, test::Sum(small.*phase).page_reads + 16);
    }
    Print("open operations=100", small.open);
    Print("open operations=5000", large.open);
    for (const auto* open : {&small.open, &large.open}) {
        // When the catalog connection opens and after the durability fence;
        // storage verification in between reuses the first validation.
        EXPECT_EQ(test::CountContaining(*open, "PRAGMA integrity_check"), 2);
        // One count derives the exact operation counter for the connection.
        EXPECT_EQ(test::CountContaining(*open, "COUNT(*) FROM operations"), 1);
    }
}

// ---- Collection-scoped operation pages ----

struct CatalogCollection
{
    std::string id;
    std::string name;
    std::string table_uuid;
};

const std::array<CatalogCollection, 3>& CatalogCollections()
{
    static const std::array<CatalogCollection, 3> collections{CatalogCollection{Uuid(0x10, 1), "first", Uuid(0x10, 2)},
        CatalogCollection{Uuid(0x20, 1), "middle", Uuid(0x20, 2)},
        CatalogCollection{Uuid(0x30, 1), "last", Uuid(0x30, 2)}};
    return collections;
}

std::string Location(std::string_view name)
{
    return "store://" + std::string(test::store_id) + "/warehouse/" + std::string(name) + ".json";
}

// Binds one operation per collection, then copies it within that collection
// until each has `counts[i]` operations.
std::shared_ptr<DurableSqliteCatalogStore> MakeCatalog(const std::filesystem::path& path,
    const std::array<unsigned, 3>& counts)
{
    const StoreInfo info{test::store_id, "local-delete-extra-v1", 1};
    Limits limits;
    limits.max_operations = 8192;
    auto catalog = DurableSqliteCatalogStore::Create(path, info, limits);
    for (size_t index = 0; index < CatalogCollections().size(); ++index) {
        const auto& collection = CatalogCollections()[index];
        catalog->PublishCreate(
            {collection.id, collection.name, collection.table_uuid, SchemaSpec{{{"n", FieldType::Int64, true}}}, 0, {}},
            [](std::string_view) -> Status {
                return {};
            },
            [&] {
                return catalog->InsertTable("columnar", collection.name, Location(collection.name));
            });
        catalog->BindOperation({collection.id,
            Uuid(0x100 + static_cast<unsigned>(index), 0),
            std::string(64, 'a'),
            1,
            catalog->Base(collection.id),
            test::attempt_id});
    }
    catalog->Close();
    Database raw;
    raw.Open(path, SQLITE_OPEN_READWRITE, {});
    for (size_t index = 0; index < CatalogCollections().size(); ++index) {
        if (counts[index] < 2) {
            continue;
        }
        raw.Execute("WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM numbers WHERE n<" +
                std::to_string(counts[index] - 1) +
                ") INSERT INTO operations SELECT collection_id,printf('%08x-0000-4000-8000-%012x'," +
                std::to_string(0x100 + index) +
                ",n),payload_hash,hash_version,state,base_identity,attempt_id,receipt_version,outcome_blob FROM "
                "(SELECT * FROM operations WHERE collection_id=?),numbers",
            {CatalogCollections()[index].id});
    }
    raw.Close();
    return DurableSqliteCatalogStore::Open(path, info, limits);
}

// Operation rows exactly as the pre-#174 keyset query returned them.
std::vector<detail::SqlRow> LegacyOperationPages(const std::filesystem::path& path, std::string_view collection)
{
    Database raw;
    raw.Open(path, SQLITE_OPEN_READONLY, {});
    std::vector<detail::SqlRow> result;
    std::string lastCollection;
    std::string lastOperation;
    while (true) {
        const auto rows = raw.Query("SELECT collection_id,operation_id,payload_hash,hash_version,base_identity,"
                                    "attempt_id,state,receipt_version,outcome_blob FROM operations WHERE "
                                    "(collection_id,operation_id)>(?,?) AND (?='' OR collection_id=?) ORDER BY "
                                    "collection_id,operation_id LIMIT 256",
            {lastCollection, lastOperation, std::string(collection), std::string(collection)});
        if (rows.empty()) {
            break;
        }
        result.insert(result.end(), rows.begin(), rows.end());
        lastCollection = detail::Required(rows.back(), 0);
        lastOperation = detail::Required(rows.back(), 1);
    }
    raw.Close();
    return result;
}

void ExpectLegacyRecords(const std::vector<detail::OperationRecord>& records, const std::vector<detail::SqlRow>& legacy)
{
    ASSERT_EQ(records.size(), legacy.size());
    for (size_t index = 0; index < records.size(); ++index) {
        const auto& record = records[index];
        const auto& row = legacy[index];
        EXPECT_EQ(record.binding.collection_id, detail::Required(row, 0));
        EXPECT_EQ(record.binding.operation_id, detail::Required(row, 1));
        EXPECT_EQ(record.binding.payload_hash, detail::Required(row, 2));
        EXPECT_EQ(detail::EncodeBase(record.binding.base), detail::Required(row, 4));
        EXPECT_EQ(record.binding.attempt_id, detail::Required(row, 5));
        EXPECT_EQ(static_cast<int>(record.state), std::stoi(detail::Required(row, 6)));
        EXPECT_EQ(record.outcome_bytes, row[8].value_or(""));
    }
}

TEST(CatalogHistoryCost, CollectionPagesReadOnlyThatCollectionAndMatchTheLegacyQuery)
{
    std::vector<std::vector<test::SqlStatement>> middle;
    for (const auto others : {1U, 2000U}) {
        test::TempDir temporary;
        const auto path = temporary.path / "catalog.sqlite";
        auto catalog = MakeCatalog(path, {others, 300, others});
        catalog->Close();
        std::map<std::string, std::vector<detail::SqlRow>, std::less<>> legacy;
        legacy[""] = LegacyOperationPages(path, {});
        for (const auto& collection : CatalogCollections()) {
            legacy[collection.id] = LegacyOperationPages(path, collection.id);
        }
        Limits limits;
        limits.max_operations = 8192;
        test::SqlTrace trace;
        catalog = DurableSqliteCatalogStore::Open(path, {test::store_id, "local-delete-extra-v1", 1}, limits);
        (void)trace.Take();
        for (const auto& collection : CatalogCollections()) {
            std::vector<detail::OperationRecord> records;
            catalog->ForEachOperation(
                [&](const auto& record) {
                    records.push_back(record);
                },
                collection.id);
            const auto statements = trace.Take();
            Print("collection " + collection.name + " others=" + std::to_string(others), statements);
            if (collection.name == "middle") {
                ASSERT_EQ(records.size(), 300U);
                middle.push_back(statements);
            }
            ExpectLegacyRecords(records, legacy.at(collection.id));
        }
        ExpectLegacyRecords(catalog->Operations(), legacy.at(""));
        catalog->Close();
    }
    ASSERT_EQ(middle.size(), 2U);
    // A full page, a partial page and the empty page that ends the keyset scan.
    EXPECT_EQ(middle[0].size(), 3U);
    EXPECT_EQ(test::Sum(middle[0]).rows, 300);
    ExpectSameWork(middle[0], middle[1]);
}

} // namespace
} // namespace glamdb
