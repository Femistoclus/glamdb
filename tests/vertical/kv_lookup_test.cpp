#include "core/engine/storage_mapping.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

namespace glamdb {
namespace {
using namespace vertical;

TEST(KVLookup, FixedSnapshotPreservesRequestOrderDuplicateHitsAndMisses)
{
    test::TempDir temporary;
    auto created = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(created);
    auto engine = *created;
    CreateCollectionRequest collection{test::collection_id, "keys", Schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    const auto first = engine->UpsertDurable({test::collection_id,
        FirstOperation,
        {},
        {Batch({{"a", 1, std::nullopt, std::nullopt, std::nullopt},
            {"b", 2, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.result);
    const auto old = SnapshotSelector::Id(test::Present(first.result).snapshot_id);
    const auto second = engine->UpsertDurable(
        {test::collection_id, SecondOperation, {}, {Batch({{"a", 9, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(second.disposition, CommitDisposition::Committed);
    const auto result = engine->Lookup({test::collection_id, {"b", "missing", "a", "b"}, old});
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->snapshot, old);
    ASSERT_EQ(result->rows.size(), 4U);
    ASSERT_TRUE(result->rows[0]);
    EXPECT_EQ(std::get<int64_t>(test::Present(result->rows[0])[1]), 2);
    EXPECT_FALSE(result->rows[1]);
    ASSERT_TRUE(result->rows[2]);
    EXPECT_EQ(std::get<int64_t>(test::Present(result->rows[2])[1]), 1);
    EXPECT_EQ(result->rows[0], result->rows[3]);
    const auto empty = engine->Lookup({test::collection_id, {"a"}, SnapshotSelector::InitialEmpty()});
    ASSERT_TRUE(empty);
    ASSERT_EQ(empty->rows.size(), 1U);
    EXPECT_FALSE(empty->rows[0]);
    EXPECT_TRUE(engine->Shutdown(std::chrono::milliseconds(1000)));
    EXPECT_EQ(std::get<int64_t>(test::Present(result->rows[2])[1]), 1);
}

TEST(KVLookup, AppendCollectionsDoNotOfferUniquenessLookup)
{
    test::TempDir temporary;
    auto created = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(created);
    ASSERT_EQ((*created)->CreateCollection({test::collection_id, "append", Schema}).disposition,
        CommitDisposition::Committed);
    const auto result = (*created)->Lookup({test::collection_id, {"a"}});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "COLLECTION_MODE_MISMATCH");
}

TEST(KVLookup, NonFirstKeyPreservesEveryScalarTypeAndNull)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    SchemaSpec schema{{{"number", FieldType::Int64, true},
        {"key", FieldType::String, false},
        {"flag", FieldType::Bool, true},
        {"score", FieldType::Float64, true},
        {"time", FieldType::TimestampUsUtc, true},
        {"amount", FieldType::Decimal, true, 18, 4},
        {"date", FieldType::Date, true},
        {"text", FieldType::String, true}}};
    CreateCollectionRequest collection{test::collection_id, "all types", schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    const Row row{int64_t{9223372036854775807LL},
        std::string("ключ"),
        true,
        -1.25,
        TimestampUs{-1234567},
        Decimal{-1, UINT64_MAX - 1233, 18, 4},
        Date{-1},
        std::monostate{}};
    auto batch = detail::MakeRecordBatch(schema, {row});
    ASSERT_TRUE(batch);
    const auto written = engine->UpsertDurable({test::collection_id, FirstOperation, {}, {*batch}});
    ASSERT_EQ(written.disposition, CommitDisposition::Committed) << (written.error ? written.error->message : "");
    const auto lookup = engine->Lookup({test::collection_id, {"ключ", "absent", "ключ"}});
    ASSERT_TRUE(lookup) << lookup.error().message;
    ASSERT_EQ(lookup->rows.size(), 3U);
    EXPECT_EQ(lookup->rows[0], row);
    EXPECT_FALSE(lookup->rows[1]);
    EXPECT_EQ(lookup->rows[2], row);
}

TEST(KVLookup, DuplicateOutputPositionsCannotExceedTheResultEnvelope)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::String, true}}};
    CreateCollectionRequest collection{test::collection_id, "bounded", schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    auto batch = detail::MakeRecordBatch(schema, {{std::string("a"), std::string(size_t{256} * 1024, 'x')}});
    ASSERT_TRUE(batch);
    ASSERT_EQ(engine->UpsertDurable({test::collection_id, FirstOperation, {}, {*batch}}).disposition,
        CommitDisposition::Committed);
    const auto refused = engine->Lookup({test::collection_id, std::vector<std::string>(100, "a")});
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    const auto accepted = engine->Lookup({test::collection_id, {"a"}});
    ASSERT_TRUE(accepted);
    EXPECT_EQ(std::get<std::string>(test::Present(accepted->rows[0])[1]).size(), 256U * 1024);
}

} // namespace
} // namespace glamdb
