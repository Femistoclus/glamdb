#include "core/catalog/sqlite.h"
#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <sqlite3.h>

namespace glamdb {
namespace {
using namespace vertical;
using namespace std::chrono_literals;

constexpr auto OtherCollection = "22222222-2222-4222-8222-222222222223";
constexpr auto LastCollection = "22222222-2222-4222-8222-222222222224";

int64_t CatalogVersion(const std::filesystem::path& root)
{
    detail::Database database;
    database.Open(root / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
    return database.Integer("PRAGMA user_version");
}

TEST(KVRetentionIntegration, OfficialVersionThreeAppendStoreUpgradesLazilyAndReopensMixedModes)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto opened = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    opened->reset();
    // An official version 3 retention store: created without the archive tier.
    ASSERT_EQ(engine
                  ->CreateCollection(
                      {test::collection_id, "append", Schema, {}, CollectionMode::Append, KeepAllSnapshotsInRoot})
                  .disposition,
        CommitDisposition::Committed);
    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    const auto second = engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_TRUE(first.result);
    ASSERT_TRUE(second.result);
    const auto expired = engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 1);
    ASSERT_TRUE(engine->Shutdown(5s));
    engine.reset();
    EXPECT_EQ(CatalogVersion(root), 3);

    opened = OpenExisting({root.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    engine = *opened;
    opened->reset();
    const auto appendInfo = engine->GetCollection(test::collection_id);
    ASSERT_TRUE(appendInfo);
    EXPECT_EQ(appendInfo->mode, CollectionMode::Append);
    auto policy = engine->GetSnapshotRetention(test::collection_id);
    ASSERT_TRUE(policy);
    EXPECT_EQ(policy->keep_last, 1);
    EXPECT_EQ(CatalogVersion(root), 3);
    CreateCollectionRequest definition{OtherCollection, "kv", Schema};
    definition.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(definition).disposition, CommitDisposition::Committed);
    const auto upsert = engine->UpsertDurable({OtherCollection, ThirdOperation, {}, {Batch(FirstRows)}});
    ASSERT_TRUE(upsert.result);
    EXPECT_EQ(CatalogVersion(root), 4);
    const auto retained = engine->GetSnapshotRetention(OtherCollection);
    ASSERT_FALSE(retained);
    EXPECT_EQ(retained.error().code, "COLLECTION_MODE_MISMATCH");
    for (const auto& requested : {std::optional<SnapshotRetentionPolicy>{},
             std::optional<SnapshotRetentionPolicy>{SnapshotRetentionPolicy{}},
             std::optional<SnapshotRetentionPolicy>{SnapshotRetentionPolicy{1}}})
    {
        const auto refused = engine->ExpireSnapshots({OtherCollection, requested});
        ASSERT_FALSE(refused);
        EXPECT_EQ(refused.error().code, "COLLECTION_MODE_MISMATCH");
    }
    const auto more = engine->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(FirstRows)}});
    ASSERT_TRUE(more.result);
    ASSERT_TRUE(engine->ExpireSnapshots({test::collection_id}));
    ASSERT_TRUE(engine->Shutdown(5s));
    engine.reset();
    opened = OpenExisting({root.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    engine = *opened;
    const auto kvInfo = engine->GetCollection(OtherCollection);
    ASSERT_TRUE(kvInfo);
    EXPECT_EQ(kvInfo->mode, CollectionMode::KeyValue);
    const auto replay = engine->UpsertDurable({OtherCollection, ThirdOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(replay.result, upsert.result);
    const auto original = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(original);
    ASSERT_TRUE(*original);
    EXPECT_EQ(test::Present(*original).result, first.result);
    ScanRequest old{test::collection_id};
    old.selector = SnapshotSelector::Id(test::Present(first.result).snapshot_id);
    const auto absent = engine->Scan(old);
    ASSERT_FALSE(absent);
    EXPECT_EQ(absent.error().code, "SNAPSHOT_EXPIRED");
}

TEST(KVRetentionIntegration, TerminalMigrationReplaysAfterSourceExpiryButFreshMigrationCreatesNoTarget)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto opened = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "source", Schema}).disposition,
        CommitDisposition::Committed);
    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_TRUE(first.result);
    CreateCollectionRequest target{OtherCollection, "migrated", Schema};
    target.mode = CollectionMode::KeyValue;
    MigrationRequest request{test::collection_id,
        SnapshotSelector::Id(test::Present(first.result).snapshot_id),
        target,
        ThirdOperation,
        DuplicatePolicy::LastWins};
    const auto migrated = engine->MigrateAppendToKV(request);
    ASSERT_TRUE(migrated.mutation);
    ASSERT_TRUE(test::Present(migrated.mutation).result);
    ASSERT_TRUE(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).result);
    ASSERT_TRUE(engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    request.cancellation = std::make_shared<std::atomic_bool>(true);
    request.timeout = 1ms;
    const auto replay = engine->MigrateAppendToKV(request);
    ASSERT_TRUE(replay.mutation);
    EXPECT_EQ(test::Present(replay.mutation).result, test::Present(migrated.mutation).result);
    request.cancellation.reset();
    request.timeout = 60s;
    request.target.collection_id = LastCollection;
    request.target.exact_name = "must-not-exist";
    request.operation_id = "44444444-4444-4444-8444-444444444445";
    const auto refused = engine->MigrateAppendToKV(request);
    EXPECT_EQ(refused.creation.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(refused.creation.error);
    EXPECT_EQ(test::Present(refused.creation.error).code, "SNAPSHOT_EXPIRED");
    EXPECT_FALSE(refused.mutation);
    const auto missing = engine->GetCollection(LastCollection);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, "COLLECTION_NOT_FOUND");
}

TEST(KVRetentionIntegration, UnboundPopulationKeepsCommittedCreationWhenItsSourceExpires)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "source", Schema}).disposition,
        CommitDisposition::Committed);
    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_TRUE(first.result);
    CreateCollectionRequest target{OtherCollection, "partial-target", Schema};
    target.mode = CollectionMode::KeyValue;
    MigrationRequest request{test::collection_id,
        SnapshotSelector::Id(test::Present(first.result).snapshot_id),
        target,
        ThirdOperation,
        DuplicatePolicy::LastWins};
    request.cancellation = std::make_shared<std::atomic_bool>(true);
    const auto partial = engine->MigrateAppendToKV(request);
    ASSERT_EQ(partial.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(partial.mutation);
    ASSERT_TRUE(test::Present(partial.mutation).error);
    EXPECT_EQ(test::Present(test::Present(partial.mutation).error).code, "CANCELLED");
    const auto unbound = engine->FindAppendOutcome(OtherCollection, ThirdOperation);
    ASSERT_TRUE(unbound);
    EXPECT_FALSE(*unbound);
    ASSERT_TRUE(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).result);
    ASSERT_TRUE(engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    request.cancellation.reset();
    const auto refused = engine->MigrateAppendToKV(request);
    EXPECT_EQ(refused.creation.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(refused.creation.info);
    EXPECT_EQ(test::Present(refused.creation.info).collection_id, OtherCollection);
    ASSERT_TRUE(refused.mutation);
    EXPECT_EQ(test::Present(refused.mutation).disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(test::Present(refused.mutation).error);
    EXPECT_EQ(test::Present(test::Present(refused.mutation).error).code, "SNAPSHOT_EXPIRED");
    const auto statistics = engine->GetStatistics(OtherCollection);
    ASSERT_TRUE(statistics);
    EXPECT_EQ(statistics->rows.value, 0);
}

TEST(KVRetentionIntegration, FullWidthSchemaPreservesEveryFieldAndANonFirstKey)
{
    test::TempDir temporary;
    auto opened = CreateStore({test::store_id, (temporary.path / "store").string()});
    ASSERT_TRUE(opened);
    auto engine = *opened;
    SchemaSpec schema;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (size_t position = 0; position < MaxSchemaFields; ++position) {
        const auto key = position == MaxSchemaFields / 2;
        const auto name = key ? std::string("key") : "field_" + std::to_string(position);
        schema.fields.push_back({name, key ? FieldType::String : FieldType::Int64, false});
        fields.push_back(arrow::field(name, key ? arrow::utf8() : arrow::int64(), false));
        if (key) {
            arrow::StringBuilder builder;
            ASSERT_TRUE(builder.Append("same-key").ok());
            ASSERT_TRUE(builder.Append("same-key").ok());
            auto array = builder.Finish();
            ASSERT_TRUE(array.ok());
            columns.push_back(*array);
        } else {
            arrow::Int64Builder builder;
            ASSERT_TRUE(builder.Append(static_cast<int64_t>(position)).ok());
            ASSERT_TRUE(builder.Append(static_cast<int64_t>(position + 1000)).ok());
            auto array = builder.Finish();
            ASSERT_TRUE(array.ok());
            columns.push_back(*array);
        }
    }
    CreateCollectionRequest definition{test::collection_id, "wide-kv", schema};
    definition.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(definition).disposition, CommitDisposition::Committed);
    const auto batch = arrow::RecordBatch::Make(arrow::schema(fields), 2, columns);
    const auto written = engine->UpsertDurable({test::collection_id, FirstOperation, {}, {batch}});
    ASSERT_TRUE(written.result);
    EXPECT_EQ(test::Present(written.result).rows_affected, 1);
    const auto lookup = engine->Lookup({test::collection_id, {"same-key"}});
    ASSERT_TRUE(lookup) << lookup.error().message;
    ASSERT_EQ(lookup->rows.size(), 1U);
    ASSERT_TRUE(lookup->rows.front());
    const auto& row = test::Present(lookup->rows.front());
    ASSERT_EQ(row.size(), MaxSchemaFields);
    for (size_t position = 0; position < MaxSchemaFields; ++position) {
        if (position == MaxSchemaFields / 2) {
            EXPECT_EQ(std::get<std::string>(row[position]), "same-key");
        } else {
            EXPECT_EQ(std::get<int64_t>(row[position]), static_cast<int64_t>(position + 1000));
        }
    }
}

} // namespace
} // namespace glamdb
