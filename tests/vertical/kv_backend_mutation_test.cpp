#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include "core/api/codec.h"
#include "core/api/kv_input.h"
#include "core/catalog/sqlite.h"
#include "core/engine/storage_mapping.h"

#include <chrono>
#include <format>
#include <map>
#include <set>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace vertical;

class TChunkedKvSource final : public BulkAppendSource
{
public:
    std::vector<std::vector<TInputRow>> Chunks;
    std::function<void(int)> OnNext;
    std::function<void(int)> OnRewind;
    bool ChangeProduction = false;
    int Passes = 0;

    Status Rewind() override
    {
        Position_ = 0;
        ++Passes;
        if (OnRewind) {
            OnRewind(Passes);
        }
        return {};
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (OnNext) {
            OnNext(Passes);
        }
        if (Position_ == Chunks.size()) {
            return nullptr;
        }
        auto rows = Chunks[Position_++];
        if (ChangeProduction && Passes > 1) {
            rows.front().Value = test::Present(rows.front().Value) + 1;
        }
        return Batch(rows);
    }

private:
    size_t Position_ = 0;
};

class TKvBackendTest : public testing::Test
{
protected:
    test::TempDir Temp_;
    std::filesystem::path Root_ = Temp_.path / "store";
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, Root_.string()});
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        CreateCollectionRequest request{test::collection_id, "kv", Schema};
        request.mode = CollectionMode::KeyValue;
        const auto collection = Engine_->CreateCollection(request);
        ASSERT_EQ(collection.disposition, CommitDisposition::Committed)
            << (collection.error ? test::Present(collection.error).message : std::string());
        ASSERT_TRUE(collection.info);
        EXPECT_EQ(test::Present(collection.info).mode, CollectionMode::KeyValue);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
        Engine_.reset();
        std::filesystem::remove_all(Root_);
    }

    AppendOutcome Upsert(const std::vector<TInputRow>& rows, std::string_view operation)
    {
        return Engine_->UpsertDurable({test::collection_id, std::string(operation), {}, {Batch(rows)}});
    }

    std::vector<std::string> Read(SnapshotSelector selector = SnapshotSelector::Latest())
    {
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id, .selector = selector});
        EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
        if (!cursor) {
            return {};
        }
        std::vector<std::string> rows;
        while (true) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return Sorted(std::move(rows));
    }
};

TEST_F(TKvBackendTest, FullReplacementUsesLastInputValueAndPreservesOldSnapshot)
{
    const std::vector<TInputRow> first{{"b", 1, 3.0, true, 10}, {"a", 4, 1.0, false, 20}};
    const auto inserted = Upsert(first, FirstOperation);
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed)
        << (inserted.error ? test::Present(inserted.error).message : std::string());
    ASSERT_TRUE(inserted.result);
    const auto old = test::Present(inserted.result).snapshot_id;
    const std::vector<TInputRow> changed{{"b", 8, 9.0, true, 30},
        {"c", 5, std::nullopt, std::nullopt, std::nullopt},
        {"b", 9, std::nullopt, std::nullopt, std::nullopt}};
    const auto replaced = Upsert(changed, SecondOperation);
    ASSERT_EQ(replaced.disposition, CommitDisposition::Committed)
        << (replaced.error ? test::Present(replaced.error).message : std::string());
    ASSERT_TRUE(replaced.result);
    EXPECT_EQ(test::Present(replaced.result).receipt_version, 3);
    EXPECT_EQ(test::Present(replaced.result).rows_appended, 0);
    EXPECT_EQ(test::Present(replaced.result).input_rows, 3);
    EXPECT_EQ(test::Present(replaced.result).rows_affected, 2);
    const std::vector<TInputRow> expected{first[1], changed[1], changed[2]};
    EXPECT_EQ(Read(), Expected({&expected}));
    EXPECT_EQ(Read(SnapshotSelector::Id(old)), Expected({&first}));
    EXPECT_EQ(Engine_->UpsertDurable({test::collection_id, SecondOperation, {}, {Batch(changed)}}), replaced);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_EQ(Read(), Expected({&expected}));
    EXPECT_EQ(Read(SnapshotSelector::Id(old)), Expected({&first}));
}

TEST_F(TKvBackendTest, UnchangedUpsertAndMissingDeleteKeepSnapshotAndDurableReceipts)
{
    const std::vector<TInputRow> rows{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    const auto inserted = Upsert(rows, FirstOperation);
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed);
    const auto same = Upsert(rows, SecondOperation);
    ASSERT_EQ(same.disposition, CommitDisposition::Committed)
        << (same.error ? test::Present(same.error).message : std::string());
    ASSERT_TRUE(same.result);
    EXPECT_FALSE(test::Present(same.result).snapshot_created);
    EXPECT_EQ(test::Present(same.result).snapshot_id, test::Present(inserted.result).snapshot_id);
    EXPECT_EQ(test::Present(same.result).rows_affected, 0);
    EXPECT_EQ(test::Present(same.result).data_files_added, 0);
    const DeleteRequest missing{test::collection_id, ThirdOperation, {}, {"absent", "absent"}};
    const auto removed = Engine_->DeleteDurable(missing);
    ASSERT_EQ(removed.disposition, CommitDisposition::Committed)
        << (removed.error ? test::Present(removed.error).message : std::string());
    ASSERT_TRUE(removed.result);
    EXPECT_FALSE(test::Present(removed.result).snapshot_created);
    EXPECT_EQ(test::Present(removed.result).input_rows, 2);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 1);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_EQ(Upsert(rows, SecondOperation), same);
    EXPECT_EQ(Engine_->DeleteDurable(missing), removed);
    EXPECT_EQ(Read(), Expected({&rows}));
}

TEST_F(TKvBackendTest, DeleteDeduplicatesKeysAndCanRemoveEveryLiveFile)
{
    const std::vector<TInputRow> rows{{"a", 1, std::nullopt, std::nullopt, std::nullopt},
        {"b", 2, std::nullopt, std::nullopt, std::nullopt}};
    const auto inserted = Upsert(rows, FirstOperation);
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed);
    const auto removed = Engine_->DeleteDurable({test::collection_id, SecondOperation, {}, {"b", "a", "b", "missing"}});
    ASSERT_EQ(removed.disposition, CommitDisposition::Committed)
        << (removed.error ? test::Present(removed.error).message : std::string());
    ASSERT_TRUE(removed.result);
    EXPECT_TRUE(test::Present(removed.result).snapshot_created);
    EXPECT_EQ(test::Present(removed.result).rows_affected, 2);
    EXPECT_EQ(test::Present(removed.result).rows_written, 0);
    EXPECT_EQ(test::Present(removed.result).data_files_added, 0);
    EXPECT_TRUE(Read().empty());
    EXPECT_EQ(Read(SnapshotSelector::Id(test::Present(inserted.result).snapshot_id)), Expected({&rows}));
}

TEST_F(TKvBackendTest, NoOpRetryRetainsItsOriginalRootAfterLaterMutationAndReopen)
{
    const std::vector<TInputRow> first{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    const auto inserted = Upsert(first, FirstOperation);
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed);
    const auto noOp = Upsert(first, SecondOperation);
    ASSERT_EQ(noOp.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(noOp.result);
    ASSERT_FALSE(test::Present(noOp.result).snapshot_created);
    const std::vector<TInputRow> changed{{"a", 2, std::nullopt, std::nullopt, std::nullopt}};
    const auto later = Upsert(changed, ThirdOperation);
    ASSERT_EQ(later.disposition, CommitDisposition::Committed)
        << (later.error ? test::Present(later.error).message : std::string());
    EXPECT_EQ(Upsert(first, SecondOperation), noOp);
    EXPECT_EQ(Read(), Expected({&changed}));
    EXPECT_EQ(FilesIn(Root_ / "warehouse" / test::collection_id / "metadata",
                  [](const auto& path) {
                      return path.filename().string().ends_with(".metadata.json");
                  })
                  .size(),
        2U);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_EQ(Upsert(first, SecondOperation), noOp);
    EXPECT_EQ(Read(), Expected({&changed}));
    EXPECT_EQ(Read(SnapshotSelector::Id(test::Present(inserted.result).snapshot_id)), Expected({&first}));
}

TEST_F(TKvBackendTest, RecoveryRejectsNoOpBindingToAnAbsentHistoricalRoot)
{
    const std::vector<TInputRow> first{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    ASSERT_EQ(Upsert(first, FirstOperation).disposition, CommitDisposition::Committed);
    const auto noOp = Upsert(first, SecondOperation);
    ASSERT_EQ(noOp.disposition, CommitDisposition::Committed);
    const std::vector<TInputRow> changed{{"a", 2, std::nullopt, std::nullopt, std::nullopt}};
    ASSERT_EQ(Upsert(changed, ThirdOperation).disposition, CommitDisposition::Committed);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    detail::Database database;
    database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    const auto rows = database.Query("SELECT base_identity FROM operations WHERE collection_id=? AND operation_id=?",
        {std::string(test::collection_id), std::string(SecondOperation)});
    ASSERT_EQ(rows.size(), 1U);
    auto base = detail::DecodeBase(detail::Required(rows.front(), 0));
    ASSERT_TRUE(base);
    base->metadata_location = base->metadata_location.substr(0, base->metadata_location.rfind('/') + 1) +
        "00001-00000000-0000-7000-8000-000000000099.metadata.json";
    database.Execute("UPDATE operations SET base_identity=? WHERE collection_id=? AND operation_id=?",
        {detail::Blob{detail::EncodeBase(*base)}, std::string(test::collection_id), std::string(SecondOperation)});
    database.Close();
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(reopened);
    EXPECT_EQ(reopened.error().code, "CORRUPTION");
}

TEST_F(TKvBackendTest, RecoveryRejectsOldNoOpSnapshotPairedWithANewerRealRoot)
{
    const std::vector<TInputRow> first{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    ASSERT_EQ(Upsert(first, FirstOperation).disposition, CommitDisposition::Committed);
    const auto noOp = Upsert(first, SecondOperation);
    ASSERT_EQ(noOp.disposition, CommitDisposition::Committed);
    const std::vector<TInputRow> changed{{"a", 2, std::nullopt, std::nullopt, std::nullopt}};
    ASSERT_EQ(Upsert(changed, ThirdOperation).disposition, CommitDisposition::Committed);
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history);
    const auto newerRoot = history->identity.metadata_location;
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    detail::Database database;
    database.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    const auto rows = database.Query("SELECT base_identity FROM operations WHERE collection_id=? AND operation_id=?",
        {std::string(test::collection_id), std::string(SecondOperation)});
    ASSERT_EQ(rows.size(), 1U);
    auto base = detail::DecodeBase(detail::Required(rows.front(), 0));
    ASSERT_TRUE(base);
    base->metadata_location = newerRoot;
    database.Execute("UPDATE operations SET base_identity=? WHERE collection_id=? AND operation_id=?",
        {detail::Blob{detail::EncodeBase(*base)}, std::string(test::collection_id), std::string(SecondOperation)});
    database.Close();
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(reopened);
    EXPECT_EQ(reopened.error().code, "CORRUPTION");
}

TEST_F(TKvBackendTest, ModeIsImmutableAndRawAppendConsumesNoOperationIdentity)
{
    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    EXPECT_EQ(collection->mode, CollectionMode::KeyValue);
    auto repeated = CreateCollectionRequest{test::collection_id, "kv", Schema};
    repeated.mode = CollectionMode::KeyValue;
    EXPECT_EQ(Engine_->CreateCollection(repeated).disposition, CommitDisposition::Committed);
    repeated.mode = CollectionMode::Append;
    EXPECT_EQ(Engine_->CreateCollection(repeated).disposition, CommitDisposition::DefinitelyNotCommitted);
    const auto append = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(append.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(append.error);
    EXPECT_EQ(test::Present(append.error).code, "COLLECTION_MODE_MISMATCH");
    const auto absent = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(absent);
    EXPECT_FALSE(absent->has_value());
    EXPECT_TRUE(Read().empty());
}

TEST_F(TKvBackendTest, EmptyMutationsHaveDurableInitialEmptyReceipts)
{
    const AppendRequest empty{test::collection_id, FirstOperation, {}, {}};
    const auto upsert = Engine_->UpsertDurable(empty);
    ASSERT_EQ(upsert.disposition, CommitDisposition::Committed)
        << (upsert.error ? test::Present(upsert.error).message : std::string());
    ASSERT_TRUE(upsert.result);
    EXPECT_FALSE(test::Present(upsert.result).snapshot_created);
    EXPECT_EQ(test::Present(upsert.result).snapshot_id, 0);
    EXPECT_FALSE(test::Present(upsert.result).parent_snapshot_id);
    const auto removed = Engine_->DeleteDurable({test::collection_id, SecondOperation, {}, {}});
    ASSERT_EQ(removed.disposition, CommitDisposition::Committed)
        << (removed.error ? test::Present(removed.error).message : std::string());
    ASSERT_FALSE(test::Present(removed.result).snapshot_created);
    const std::vector<TInputRow> rows{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    ASSERT_EQ(Upsert(rows, ThirdOperation).disposition, CommitDisposition::Committed);
    EXPECT_EQ(Engine_->UpsertDurable(empty), upsert);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    const auto reopened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = *reopened;
    EXPECT_EQ(Engine_->UpsertDurable(empty), upsert);
    EXPECT_EQ(Read(), Expected({&rows}));
}

TEST_F(TKvBackendTest, NonFirstUtf8KeyAndEveryScalarSurviveReplacementAndNullableOmission)
{
    const SchemaSpec schema{{{"text", FieldType::String, true},
        {"flag", FieldType::Bool, true},
        {"number", FieldType::Int64, true},
        {"key", FieldType::String, false},
        {"score", FieldType::Float64, true},
        {"at", FieldType::TimestampUsUtc, true},
        {"amount", FieldType::Decimal, true, 18, 4},
        {"day", FieldType::Date, true}}};
    auto create = CreateCollectionRequest{SecondCollection, "scalar-kv", schema};
    create.mode = CollectionMode::KeyValue;
    ASSERT_EQ(Engine_->CreateCollection(create).disposition, CommitDisposition::Committed);
    const std::string key = "\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87";
    const std::vector<Row> first{{std::string("first"),
        true,
        int64_t{9007199254740993LL},
        key,
        1.25,
        TimestampUs{253402300799999999LL},
        Decimal{0, 123456789, 18, 4},
        Date{-719162}}};
    const auto batch = detail::MakeRecordBatch(schema, first);
    ASSERT_TRUE(batch);
    const auto inserted = Engine_->UpsertDurable({SecondCollection, FirstOperation, {}, {*batch}});
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed)
        << (inserted.error ? test::Present(inserted.error).message : std::string());
    const SchemaSpec partial{{{"number", FieldType::Int64, true}, {"key", FieldType::String, false}}};
    const auto replacement = detail::MakeRecordBatch(partial, {{int64_t{-9007199254740993LL}, key}});
    ASSERT_TRUE(replacement);
    const auto updated = Engine_->UpsertDurable({SecondCollection, SecondOperation, {}, {*replacement}});
    ASSERT_EQ(updated.disposition, CommitDisposition::Committed)
        << (updated.error ? test::Present(updated.error).message : std::string());
    const std::vector<Row> expected{{std::monostate{},
        std::monostate{},
        int64_t{-9007199254740993LL},
        key,
        std::monostate{},
        std::monostate{},
        std::monostate{},
        std::monostate{}}};
    const auto expectedBatch = detail::MakeRecordBatch(schema, expected);
    ASSERT_TRUE(expectedBatch);
    const auto cursor = Engine_->Scan({.collection_id = SecondCollection});
    ASSERT_TRUE(cursor);
    const auto latest = (*cursor)->Next();
    ASSERT_TRUE(latest && *latest);
    EXPECT_TRUE((*latest)->Equals(**expectedBatch));
    const auto historical = Engine_->Scan({.collection_id = SecondCollection,
        .selector = SnapshotSelector::Id(test::Present(inserted.result).snapshot_id)});
    ASSERT_TRUE(historical);
    const auto old = (*historical)->Next();
    ASSERT_TRUE(old && *old);
    EXPECT_TRUE((*old)->Equals(**batch));
}

TEST_F(TKvBackendTest, CompactionAcceptsThePublicSixteenMiBTargetAndRejectsLargerTargets)
{
    CompactionRequest request{test::collection_id, FirstOperation};
    request.target_file_bytes = 16LL << 20;
    const auto noOp = Engine_->CompactDurable(request);
    ASSERT_EQ(noOp.disposition, CommitDisposition::Committed)
        << (noOp.error ? test::Present(noOp.error).message : std::string());
    ASSERT_TRUE(noOp.result);
    EXPECT_FALSE(test::Present(noOp.result).snapshot_created);
    EXPECT_EQ(Engine_->CompactDurable(request), noOp);
    auto changedTarget = request;
    changedTarget.target_file_bytes = 8LL << 20;
    const auto conflict = Engine_->CompactDurable(changedTarget);
    EXPECT_EQ(conflict.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(conflict.error);
    EXPECT_EQ(test::Present(conflict.error).code, "OPERATION_CONFLICT");
    request.operation_id = SecondOperation;
    request.target_file_bytes = (16LL << 20) + 1;
    const auto rejected = Engine_->CompactDurable(request);
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(Engine_->FindAppendOutcome(test::collection_id, SecondOperation)->has_value());
}

TEST_F(TKvBackendTest, BulkDuplicatesAcrossChunksUseTheLastFullInputRow)
{
    auto source = std::make_shared<TChunkedKvSource>();
    source->Chunks = {{{"c", 1, 1.0, true, 1}, {"a", 2, 2.0, true, 2}},
        {{"b", 3, 3.0, true, 3}, {"a", 4, std::nullopt, std::nullopt, std::nullopt}},
        {{"c", 5, std::nullopt, std::nullopt, std::nullopt}, {"b", 6, std::nullopt, std::nullopt, std::nullopt}}};
    const BulkAppendRequest request{test::collection_id, FirstOperation, {}, 6, source};
    const auto inserted = Engine_->BulkUpsertDurable(request);
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed)
        << (inserted.error ? test::Present(inserted.error).message : std::string());
    ASSERT_TRUE(inserted.result);
    EXPECT_EQ(test::Present(inserted.result).input_rows, 6);
    EXPECT_EQ(test::Present(inserted.result).rows_affected, 3);
    EXPECT_EQ(test::Present(inserted.result).rows_written, 3);
    const std::vector<TInputRow> expected{source->Chunks[1][1], source->Chunks[2][0], source->Chunks[2][1]};
    EXPECT_EQ(Read(), Expected({&expected}));
    EXPECT_EQ(source->Passes, 2);
    EXPECT_EQ(Engine_->BulkUpsertDurable(request), inserted);
}

TEST_F(TKvBackendTest, ChangedBulkProductionIsPermanentAndPublishesNoPartialRows)
{
    auto source = std::make_shared<TChunkedKvSource>();
    source->Chunks = {{{"a", 1, std::nullopt, std::nullopt, std::nullopt}},
        {{"b", 2, std::nullopt, std::nullopt, std::nullopt}}};
    source->ChangeProduction = true;
    const BulkAppendRequest request{test::collection_id, FirstOperation, {}, 2, source};
    const auto failed = Engine_->BulkUpsertDurable(request);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "INVALID_ARGUMENT");
    EXPECT_TRUE(Read().empty());
    EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id)->snapshots.empty());
    source->ChangeProduction = false;
    EXPECT_EQ(Engine_->BulkUpsertDurable(request), failed);
}

TEST_F(TKvBackendTest, CancellationDuringBulkProductionKeepsAPermanentFailure)
{
    auto source = std::make_shared<TChunkedKvSource>();
    source->Chunks = {{{"a", 1, std::nullopt, std::nullopt, std::nullopt}},
        {{"b", 2, std::nullopt, std::nullopt, std::nullopt}}};
    auto stop = std::make_shared<std::atomic<bool>>(false);
    source->OnNext = [stop](int pass) {
        if (pass == 2) {
            stop->store(true);
        }
    };
    BulkAppendRequest request{test::collection_id, FirstOperation, {}, 2, source};
    request.cancellation = stop;
    const auto failed = Engine_->BulkUpsertDurable(request);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "CANCELLED");
    EXPECT_TRUE(Read().empty());
    EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id)->snapshots.empty());
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, FirstOperation), failed);
}

TEST_F(TKvBackendTest, MissingDeleteWithAStaleExactSnapshotFailsBeforeANoOpCanCommit)
{
    const auto first = Upsert({{"a", 1, std::nullopt, std::nullopt, std::nullopt}}, FirstOperation);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.result);
    const auto second = Upsert({{"b", 2, std::nullopt, std::nullopt, std::nullopt}}, SecondOperation);
    ASSERT_EQ(second.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(second.result);
    const auto before = Read();
    const DeleteRequest request{test::collection_id,
        ThirdOperation,
        {SnapshotPreconditionKind::Equals, test::Present(first.result).snapshot_id},
        {"missing"}};
    const auto rejected = Engine_->DeleteDurable(request);
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "PRECONDITION_FAILED");
    EXPECT_EQ(Read(), before);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
    EXPECT_EQ(*Engine_->FindAppendOutcome(test::collection_id, ThirdOperation), rejected);
}

TEST(KvBackendCapacity, AnUnchangedNoOpCommitsAtTheRetainedFileLimitWhileGrowthFailsAtomically)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    detail::TLocalStorageOptions options;
    options.Storage.Store.DataFiles = 1;
    std::shared_ptr<THookBackend> backend;
    auto opened =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(backend, options));
    ASSERT_TRUE(opened) << opened.error().message;
    auto engine = std::move(*opened);
    CreateCollectionRequest create{test::collection_id, "bounded-kv", Schema};
    create.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(create).disposition, CommitDisposition::Committed);
    const std::vector<TInputRow> row{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    const auto first = engine->UpsertDurable({test::collection_id, FirstOperation, {}, {Batch(row)}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(first.result);
    const auto same = engine->UpsertDurable({test::collection_id, SecondOperation, {}, {Batch(row)}});
    ASSERT_EQ(same.disposition, CommitDisposition::Committed)
        << (same.error ? test::Present(same.error).message : std::string());
    ASSERT_TRUE(same.result);
    EXPECT_FALSE(test::Present(same.result).snapshot_created);
    const AppendRequest change{test::collection_id,
        ThirdOperation,
        {},
        {Batch({{"a", 9, std::nullopt, std::nullopt, std::nullopt}})}};
    const auto failed = engine->UpsertDurable(change);
    EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    EXPECT_EQ(test::Present(failed.error).code, "RESOURCE_EXHAUSTED");
    const auto snapshots = engine->ListSnapshots(test::collection_id);
    ASSERT_TRUE(snapshots);
    ASSERT_EQ(snapshots->snapshots.size(), 1U);
    EXPECT_EQ(snapshots->snapshots.front().snapshot_id, test::Present(first.result).snapshot_id);
    {
        const auto found = engine->Lookup({test::collection_id, {"a"}});
        ASSERT_TRUE(found);
        ASSERT_TRUE(found->rows.front());
        EXPECT_EQ(std::get<int64_t>(test::Present(found->rows.front())[1]), 1);
    }
    ASSERT_TRUE(engine->Shutdown(1000ms));
    engine.reset();
    backend.reset();
    opened = detail::EngineFactory::OpenWith({root.string(), test::store_id}, THookBackend::Factory(backend, options));
    ASSERT_TRUE(opened) << opened.error().message;
    engine = std::move(*opened);
    EXPECT_EQ(engine->UpsertDurable(change), failed);
    EXPECT_EQ(engine->UpsertDurable({test::collection_id, SecondOperation, {}, {Batch(row)}}), same);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

TEST(KvBackendInventory, ChangedAndUnchangedMutationsReuseTheWarehouseInventory)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<NGlamDB::NTesting::TFaultFS>(posix);
    detail::TLocalStorageOptions options;
    options.Fs = faults;
    options.VerifyStagedRoot = false;
    std::shared_ptr<THookBackend> backend;
    auto created =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(backend, options));
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    CreateCollectionRequest create{test::collection_id, "inventory-kv", Schema};
    create.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(create).disposition, CommitDisposition::Committed);
    const auto batch = Batch({{"a", 1, std::nullopt, std::nullopt, std::nullopt}});
    ASSERT_EQ(engine->UpsertDurable({test::collection_id, FirstOperation, {}, {batch}}).disposition,
        CommitDisposition::Committed);
    const auto warehouse = posix.OpenDir(root / "warehouse");
    ASSERT_TRUE(warehouse);
    const bool incremental = static_cast<bool>(posix.TrackTree(warehouse.Value()));
    for (const auto& request : std::vector<AppendRequest>{{test::collection_id, SecondOperation, {}, {batch}},
             {test::collection_id, ThirdOperation, {}, {Batch({{"a", 2, std::nullopt, std::nullopt, std::nullopt}})}}})
    {
        const auto mark = faults->Events().size();
        const auto outcome = engine->UpsertDurable(request);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? test::Present(outcome.error).message : std::string());
        size_t walks = 0;
        size_t statfs = 0;
        const auto events = faults->Events();
        for (size_t index = mark; index < events.size(); ++index) {
            walks += events[index].Op == NGlamDB::NTesting::EIOOp::MeasureTree ? 1 : 0;
            statfs += events[index].Op == NGlamDB::NTesting::EIOOp::StatFs ? 1 : 0;
        }
        EXPECT_EQ(walks, incremental ? 0U : 1U);
        EXPECT_GE(statfs, 1U);
    }
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

TEST(KvBackendCapacity, RoutingCopiesAreAdmittedBeforeAnyNewDataFileIsProduced)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    auto opened = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(opened) << opened.error().message;
    auto engine = std::move(*opened);
    CreateCollectionRequest create{test::collection_id, "route-budget-kv", Schema};
    create.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(create).disposition, CommitDisposition::Committed);
    ASSERT_EQ(engine
                  ->UpsertDurable({test::collection_id,
                      FirstOperation,
                      {},
                      {Batch({{"a", 1, std::nullopt, std::nullopt, std::nullopt}})}})
                  .disposition,
        CommitDisposition::Committed);
    const auto dataDirectory = root / "warehouse" / test::collection_id / "data";
    const auto before = FilesIn(dataDirectory);
    auto source = std::make_shared<TChunkedKvSource>();
    source->Chunks = {{{"a", 2, std::nullopt, std::nullopt, std::nullopt}}};
    const auto owner = engine->GetResourceOwner();
    std::shared_ptr<ResourceReservation> pressure;
    source->OnRewind = [&](int pass) {
        if (pass == 2) {
            const auto metadata = static_cast<size_t>(ResourceClass::Metadata);
            const auto available = owner->GetLimits().memory_bytes[metadata] - owner->GetUsage().memory_bytes[metadata];
            if (available < (128ULL << 20)) {
                throw std::runtime_error("test requires the temporary KV envelope to remain available");
            }
            auto reserved = owner->Reserve(ResourceClass::Metadata, available - (128ULL << 20));
            if (!reserved) {
                throw std::runtime_error(reserved.error().message);
            }
            pressure = std::move(*reserved);
        }
    };
    const BulkAppendRequest request{test::collection_id, SecondOperation, {}, 1, source};
    const auto refused = engine->BulkUpsertDurable(request);
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(FilesIn(dataDirectory), before);
    pressure.reset();
    EXPECT_EQ(engine->BulkUpsertDurable(request), refused);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

TEST(KvBackendRouting, ClusteredEndsPreserveUntouchedMiddleFilesUnderTightByteBounds)
{
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    detail::TLocalStorageOptions options;
    options.Writer.MaxInputBytes = 1024;
    std::shared_ptr<THookBackend> backend;
    const auto opened =
        detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(backend, options));
    ASSERT_TRUE(opened) << opened.error().message;
    const auto& engine = *opened;
    CreateCollectionRequest create{test::collection_id, "tight-kv", Schema};
    create.mode = CollectionMode::KeyValue;
    ASSERT_EQ(engine->CreateCollection(create).disposition, CommitDisposition::Committed);
    auto source = std::make_shared<TChunkedKvSource>();
    for (int chunk = 0; chunk < 20; ++chunk) {
        std::vector<TInputRow> rows;
        for (int offset = 0; offset < 5; ++offset) {
            const auto ordinal = chunk * 5 + offset;
            rows.push_back({std::format("k{:04}", ordinal), ordinal, std::nullopt, std::nullopt, std::nullopt});
        }
        source->Chunks.push_back(std::move(rows));
    }
    const auto inserted = engine->BulkUpsertDurable({test::collection_id, FirstOperation, {}, 100, source});
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed)
        << (inserted.error ? test::Present(inserted.error).message : std::string());
    const auto before = engine->ListFiles(test::collection_id);
    ASSERT_TRUE(before);
    ASSERT_GE(before->Files.size(), 3U);
    const auto changed = engine->UpsertDurable({test::collection_id,
        SecondOperation,
        {},
        {Batch({{"k0000", 999, std::nullopt, std::nullopt, std::nullopt},
            {"k0099", 999, std::nullopt, std::nullopt, std::nullopt}})}});
    ASSERT_EQ(changed.disposition, CommitDisposition::Committed)
        << (changed.error ? test::Present(changed.error).message : std::string());
    ASSERT_TRUE(changed.result);
    EXPECT_EQ(test::Present(changed.result).rows_affected, 2);
    EXPECT_EQ(test::Present(changed.result).data_files_removed, 2);
    const auto after = engine->ListFiles(test::collection_id);
    ASSERT_TRUE(after);
    std::set<std::string> newIDs;
    for (const auto& file : after->Files) {
        newIDs.insert(file.FileID);
    }
    const auto preserved = std::ranges::count_if(before->Files, [&](const auto& file) {
        return newIDs.contains(file.FileID);
    });
    EXPECT_EQ(static_cast<size_t>(preserved), before->Files.size() - 2);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

} // namespace
} // namespace glamdb
