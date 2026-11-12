#include "core/engine/storage_mapping.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <cstring>

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
    EXPECT_TRUE(engine->GetHealth().read_available);
    EXPECT_TRUE(engine->GetHealth().write_ready);
    const auto accepted = engine->Lookup({test::collection_id, {"a"}});
    ASSERT_TRUE(accepted);
    EXPECT_EQ(std::get<std::string>(test::Present(accepted->rows[0])[1]).size(), 256U * 1024);
}

TEST(KVLookup, InvalidKeysDoNotCloseHealthyAdmission)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    CreateCollectionRequest collection{test::collection_id, "keys", Schema};
    collection.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    for (const auto& keys : {std::vector<std::string>{std::string(1025, 'a')},
             std::vector<std::string>{std::string(1, '\xFF')},
             std::vector<std::string>(4097, "a")})
    {
        const auto lookup = engine->Lookup({test::collection_id, keys});
        ASSERT_FALSE(lookup);
        EXPECT_EQ(lookup.error().code, "INVALID_ARGUMENT");
        EXPECT_TRUE(engine->GetHealth().read_available);
        EXPECT_TRUE(engine->GetHealth().write_ready);
    }
    EXPECT_EQ(engine->DeleteDurable({test::collection_id, FirstOperation, {}, {"missing"}}).disposition,
        CommitDisposition::Committed);
}

void ReplaceWithDuplicateKeys(const std::filesystem::path& file, const std::string& original)
{
    auto input = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(original));
    auto reader = parquet::arrow::OpenFile(input, arrow::default_memory_pool());
    ASSERT_TRUE(reader.ok());
    auto table = (*reader)->ReadTable();
    ASSERT_TRUE(table.ok());
    arrow::StringBuilder keys;
    ASSERT_TRUE(keys.AppendValues({"a", "a", "c"}).ok());
    auto duplicateKeys = keys.Finish().ValueOrDie();
    auto crafted =
        (*table)
            ->SetColumn(0, (*table)->schema()->field(0), std::make_shared<arrow::ChunkedArray>(duplicateKeys))
            .ValueOrDie();
    arrow::StringBuilder values;
    ASSERT_TRUE(values.AppendValues(std::vector<std::string>(3, std::string(9000, 'x'))).ok());
    crafted = crafted
                  ->SetColumn(1,
                      crafted->schema()->field(1),
                      std::make_shared<arrow::ChunkedArray>(values.Finish().ValueOrDie()))
                  .ValueOrDie();
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    const auto properties = parquet::WriterProperties::Builder()
                                .compression(parquet::Compression::UNCOMPRESSED)
                                ->disable_statistics()
                                ->build();
    ASSERT_TRUE(parquet::arrow::WriteTable(*crafted, arrow::default_memory_pool(), sink, 1024, properties).ok());
    auto bytes = sink->Finish().ValueOrDie()->ToString();
    ASSERT_LE(bytes.size(), original.size());
    // Preserve the manifest length and readable schema; only the decoded KV
    // uniqueness invariant must fail, after the lower-level scan succeeds.
    uint32_t footerBytes = 0;
    std::memcpy(&footerBytes, bytes.data() + bytes.size() - 8, sizeof(footerBytes));
    bytes.insert(bytes.size() - 8 - footerBytes, original.size() - bytes.size(), '\0');
    WriteBytes(file, bytes);
}

class TKvLookupIntegrityTest : public ::testing::TestWithParam<bool>
{ };

TEST_P(TKvLookupIntegrityTest, DuplicateKeysFenceAdmissionUntilRepair)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto opened = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::String, false}}};
    CreateCollectionRequest collection{test::collection_id, "keys", schema};
    collection.mode = CollectionMode::KeyValue;
    collection.write_settings.compression = ParquetCompression{ParquetCodec::Uncompressed, {}};
    collection.write_settings.dictionary = DictionaryPolicy::Disabled;
    ASSERT_EQ(engine->CreateCollection(collection).disposition, CommitDisposition::Committed);
    const std::vector<Row> rows{{std::string("a"), std::string(10000, 'a')},
        {std::string("b"), std::string(10000, 'b')},
        {std::string("c"), std::string(10000, 'c')}};
    const auto batch = detail::MakeRecordBatch(schema, rows);
    ASSERT_TRUE(batch);
    const auto first = engine->UpsertDurable({test::collection_id, FirstOperation, {}, {*batch}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.result);
    const auto files = FilesIn(root / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(files.size(), 1U);
    const auto original = ReadBytes(files.front());
    auto selected = SnapshotSelector::Latest();
    if (GetParam()) {
        selected = SnapshotSelector::Id(test::Present(first.result).snapshot_id);
        const auto replacement = detail::MakeRecordBatch(schema, {{std::string("a"), std::string("new value")}});
        ASSERT_TRUE(replacement);
        ASSERT_EQ(engine->UpsertDurable({test::collection_id, SecondOperation, {}, {*replacement}}).disposition,
            CommitDisposition::Committed);
    }
    ASSERT_NO_FATAL_FAILURE(ReplaceWithDuplicateKeys(files.front(), original));
    const auto lookup = engine->Lookup({test::collection_id, {"a"}, selected});
    ASSERT_FALSE(lookup);
    EXPECT_EQ(lookup.error().code, "CORRUPTION");
    const auto health = engine->GetHealth();
    EXPECT_EQ(health.state, StoreState::RecoveryRequired);
    EXPECT_FALSE(health.read_available);
    EXPECT_FALSE(health.write_ready);
    const auto read = engine->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error().code, "RECOVERY_REQUIRED");
    const auto receipt = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_FALSE(receipt);
    EXPECT_EQ(receipt.error().code, "RECOVERY_REQUIRED");
    const auto refused = engine->DeleteDurable({test::collection_id, ThirdOperation, {}, {"missing"}});
    EXPECT_NE(refused.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RECOVERY_REQUIRED");
    EXPECT_FALSE(engine->Reconcile());
    EXPECT_FALSE(engine->GetHealth().write_ready);

    WriteBytes(files.front(), original);
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_TRUE(engine->GetHealth().read_available);
    EXPECT_TRUE(engine->GetHealth().write_ready);
    const auto repaired = engine->Lookup({test::collection_id, {"a"}, selected});
    ASSERT_TRUE(repaired);
    ASSERT_EQ(repaired->rows.size(), 1U);
    EXPECT_EQ(repaired->rows[0], rows[0]);
    const auto replay = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(replay);
    ASSERT_TRUE(*replay);
    EXPECT_EQ(test::Present(*replay).result, first.result);
    EXPECT_EQ(engine->DeleteDurable({test::collection_id, ThirdOperation, {}, {"missing"}}).disposition,
        CommitDisposition::Committed);
}

INSTANTIATE_TEST_SUITE_P(CurrentAndHistorical, TKvLookupIntegrityTest, ::testing::Bool());

} // namespace
} // namespace glamdb
