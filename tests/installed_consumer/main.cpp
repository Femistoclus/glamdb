#include <storage/contract.h>
#include <storage/engine.h>
#include <storage/read_view.h>
#include <storage/scan.h>

#include <arrow/api.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

void CheckNegotiation(glamdb::Engine& engine, bool write);

namespace {

constexpr char StoreId[] = "00000000-0000-0000-0000-000000000001";
constexpr char CollectionId[] = "00112233-4455-6677-8899-aabbccddeeff";
constexpr char OperationA[] = "00000000-0000-0000-0000-000000000004";
constexpr char OperationB[] = "00000000-0000-0000-0000-000000000005";
constexpr char UnknownOperation[] = "00000000-0000-0000-0000-000000000006";

using TRow = std::pair<std::string, std::optional<int64_t>>;
const std::vector<TRow> RowsA{{"dup", 1}, {"dup", 1}, {"null", std::nullopt}};
const std::vector<TRow> RowsB{{"b", 2}, {"dup", 3}};
const std::vector<TRow> RowsAB{{"dup", 1}, {"dup", 1}, {"null", std::nullopt}, {"b", 2}, {"dup", 3}};
const glamdb::SchemaSpec Schema{{{"key", glamdb::FieldType::String, false}, {"n", glamdb::FieldType::Int64, true}}};

void Check(bool condition, std::string_view message)
{
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

glamdb::Predicate CompoundDupPredicate()
{
    const auto predicate = glamdb::Predicate::And({
        glamdb::Predicate::Compare("key", glamdb::CompareOp::Eq, std::string("dup")),
        glamdb::Predicate::Or({
            glamdb::Predicate::In("n", {int64_t{1}, std::nullopt}),
            glamdb::Predicate::Not(glamdb::Predicate::IsNotNull("n")),
            glamdb::Predicate::CompareNull("n", glamdb::CompareOp::Eq),
        }),
    });
    Check(predicate.kind() == glamdb::Predicate::Kind::And && predicate.children().size() == 2U,
        "compound predicate AST accessors");
    return predicate;
}

template <class T>
T Take(glamdb::Result<T> result, std::string_view context)
{
    if (!result) {
        throw std::runtime_error(std::string(context) + ": " + result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}

void CheckContract(glamdb::Engine& engine, bool storage)
{
    const auto legacy = Take(engine.NegotiateStorageContract(), "negotiate default 1.0 contract");
    Check(legacy.Version == glamdb::TStorageContractVersion{1, 0} &&
            !legacy.HasFeature(glamdb::EStorageFeature::IndependentReadView) && !legacy.EffectiveLimits.ReadViews,
        "default negotiation preserves 1.0 without read views");
    glamdb::TStorageContractRequest request;
    request.MaxVersion = {1, 9};
    if (storage) {
        request.RequiredOperations = {glamdb::EStorageOperation::Scan,
            glamdb::EStorageOperation::Append,
            glamdb::EStorageOperation::FindOutcome};
        request.RequiredFeatures = {glamdb::EStorageFeature::FixedSnapshotScan,
            glamdb::EStorageFeature::TypedNulls,
            glamdb::EStorageFeature::PreserveAppendDuplicates};
    }
    const auto contract = Take(engine.NegotiateStorageContract(request), "negotiate native contract");
    Check(contract.Version == glamdb::TStorageContractVersion{1, 3} &&
            contract.EffectiveLimits.Engine.max_collections == engine.GetLimits().max_collections &&
            contract.EffectiveLimits.SchemaFields == 128 && contract.EffectiveLimits.AppendRows == 10000 &&
            contract.HasOperation(glamdb::EStorageOperation::Scan) == storage &&
            !contract.HasOperation(glamdb::EStorageOperation::Upsert),
        "negotiated append support and effective limits");
    Check(contract.HasFeature(glamdb::EStorageFeature::IndependentReadView) == storage &&
            contract.EffectiveLimits.ReadViews.has_value() == storage,
        "native read-view feature and quotas are gated together");
    Check(contract.HasFeature(glamdb::EStorageFeature::ScanSplits) == storage &&
            contract.HasOperation(glamdb::EStorageOperation::ReadSplit) == storage &&
            contract.EffectiveLimits.ScanSplits.has_value() == storage,
        "native split discovery and limits are gated together");
    Check(contract.HasFeature(glamdb::EStorageFeature::ScanNegotiation) == storage &&
            contract.HasOperation(glamdb::EStorageOperation::NegotiateScan) == storage,
        "native scan negotiation is gated by storage support");
    auto version12 = request;
    version12.MaxVersion = {1, 2};
    const auto oldScan = Take(engine.NegotiateStorageContract(version12), "negotiate version 1.2");
    Check(!oldScan.HasFeature(glamdb::EStorageFeature::ScanNegotiation) &&
            !oldScan.HasOperation(glamdb::EStorageOperation::NegotiateScan),
        "version 1.2 excludes negotiation");
    auto version11 = request;
    version11.MaxVersion = {1, 1};
    const auto previous = Take(engine.NegotiateStorageContract(version11), "negotiate version 1.1");
    Check(!previous.HasFeature(glamdb::EStorageFeature::ScanSplits) && !previous.EffectiveLimits.ScanSplits,
        "version 1.1 does not expose splits");
    request.Mode = glamdb::CollectionMode::KeyValue;
    request.RequiredFeatures.clear();
    request.RequiredOperations.clear();
    if (storage) {
        request.RequiredOperations = {glamdb::EStorageOperation::Upsert,
            glamdb::EStorageOperation::Delete,
            glamdb::EStorageOperation::Lookup,
            glamdb::EStorageOperation::Compact};
        request.RequiredFeatures = {glamdb::EStorageFeature::DurableNoOp};
    }
    const auto kv = Take(engine.NegotiateStorageContract(request), "negotiate KV contract");
    Check(!kv.HasOperation(glamdb::EStorageOperation::Append) &&
            !kv.HasOperation(glamdb::EStorageOperation::ExpireSnapshots),
        "KV contract refuses raw append and retention");
    request.RequiredOperations = {glamdb::EStorageOperation::Append};
    const auto wrongMode = engine.NegotiateStorageContract(request);
    Check(!wrongMode && wrongMode.error().code == "UNSUPPORTED_OPERATION", "unsupported operation requirement");
    request.RequiredOperations.clear();
    request.RequiredFeatures = {glamdb::EStorageFeature::StagedWrites};
    const auto future = engine.NegotiateStorageContract(request);
    Check(!future && future.error().code == "UNSUPPORTED_FEATURE", "future features fail explicitly");
}

std::shared_ptr<arrow::Schema> InputSchema()
{
    return arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("n", arrow::int64())});
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(const std::vector<TRow>& rows)
{
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    for (const auto& [key, value] : rows) {
        Check(keys.Append(key).ok(), "build keys");
        Check((value ? values.Append(*value) : values.AppendNull()).ok(), "build values");
    }
    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> valueArray;
    Check(keys.Finish(&keyArray).ok() && values.Finish(&valueArray).ok(), "finish input arrays");
    return arrow::RecordBatch::Make(InputSchema(), static_cast<int64_t>(rows.size()), {keyArray, valueArray});
}

void CheckRows(glamdb::ScanCursor& cursor, std::vector<TRow> expected, glamdb::SnapshotSelector resolved)
{
    Check(cursor.Info().selector_resolved == resolved, "resolved scan selector");
    Check(cursor.Info().output_schema && cursor.Info().output_schema->Equals(*InputSchema()), "scan schema");
    std::vector<TRow> rows;
    while (const auto batch = Take(cursor.Next(), "scan next")) {
        Check(batch->ValidateFull().ok() && batch->schema()->Equals(*InputSchema()), "scan batch schema and values");
        const auto& keys = static_cast<const arrow::StringArray&>(*batch->column(0));
        const auto& values = static_cast<const arrow::Int64Array&>(*batch->column(1));
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            Check(!keys.IsNull(row), "required key");
            rows.emplace_back(keys.GetString(row),
                values.IsNull(row) ? std::nullopt : std::optional<int64_t>(values.Value(row)));
        }
    }
    std::ranges::sort(rows);
    std::ranges::sort(expected);
    Check(rows == expected, "scan rows must match the literal multiset, including duplicates and NULL");
}

void CheckScan(glamdb::Engine& engine,
    glamdb::SnapshotSelector selector,
    const std::vector<TRow>& expected,
    glamdb::SnapshotSelector resolved)
{
    auto cursor = Take(engine.Scan({.collection_id = CollectionId, .selector = selector, .batch_rows = 1}), "scan");
    CheckRows(*cursor, expected, resolved);
}

const glamdb::AppendResult& CheckReceipt(const glamdb::AppendOutcome& outcome, std::string_view operation, int64_t rows)
{
    if (!outcome.result) {
        throw std::runtime_error("committed append receipt is missing");
    }
    Check(outcome.disposition == glamdb::CommitDisposition::Committed &&
            outcome.phase == glamdb::CommitPhase::Complete && outcome.operation_id == operation && !outcome.error &&
            !outcome.recovery,
        "committed append outcome");
    const auto& receipt = *outcome.result;
    Check(receipt.collection_id == CollectionId && receipt.operation_id == operation && receipt.rows_appended == rows &&
            receipt.data_files_added == 1 && receipt.compressed_data_bytes_added > 0 && receipt.snapshot_id != 0 &&
            receipt.snapshot_created_at_us > 0 && !receipt.payload_hash.empty() && receipt.hash_version == 1 &&
            receipt.receipt_version == 1 && receipt.durability_profile == "local-delete-extra-v1",
        "complete append receipt");
    return receipt;
}

std::string RenderReceipt(const glamdb::AppendResult& receipt)
{
    std::ostringstream output;
    output << std::quoted(receipt.collection_id) << ' ' << std::quoted(receipt.operation_id) << ' '
           << std::quoted(receipt.payload_hash) << ' ' << receipt.hash_version << ' ' << receipt.snapshot_id << ' '
           << (receipt.parent_snapshot_id ? std::to_string(*receipt.parent_snapshot_id) : "none") << ' '
           << receipt.rows_appended << ' ' << receipt.data_files_added << ' ' << receipt.compressed_data_bytes_added
           << ' ' << receipt.snapshot_created_at_us << ' ' << std::quoted(receipt.durability_profile) << ' '
           << receipt.receipt_version;
    return output.str();
}

void CheckViews(glamdb::Engine& engine, int64_t snapshotA, int64_t snapshotB)
{
    CheckScan(engine, glamdb::SnapshotSelector::Latest(), RowsAB, glamdb::SnapshotSelector::Id(snapshotB));
    CheckScan(engine, glamdb::SnapshotSelector::Id(snapshotA), RowsA, glamdb::SnapshotSelector::Id(snapshotA));
    CheckScan(engine, glamdb::SnapshotSelector::InitialEmpty(), {}, glamdb::SnapshotSelector::InitialEmpty());
    auto filtered = Take(engine.Scan({.collection_id = CollectionId,
                             .selector = glamdb::SnapshotSelector::Id(snapshotB),
                             .predicate = CompoundDupPredicate(),
                             .batch_rows = 1}),
        "compound filtered scan");
    CheckRows(*filtered, {{"dup", 1}, {"dup", 1}}, glamdb::SnapshotSelector::Id(snapshotB));
    auto countOnly = Take(engine.Scan({.collection_id = CollectionId,
                              .projection = std::vector<std::string>{},
                              .predicate = CompoundDupPredicate(),
                              .batch_rows = 1}),
        "zero-column filtered scan");
    Check(countOnly->Info().output_schema->num_fields() == 0 &&
            countOnly->Info().selector_resolved == glamdb::SnapshotSelector::Id(snapshotB),
        "zero-column schema and fixed view before Next");
    int64_t count = 0;
    while (const auto batch = Take(countOnly->Next(), "zero-column next")) {
        Check(batch->num_columns() == 0 && batch->num_rows() == 1 && batch->ValidateFull().ok(),
            "zero-column batches preserve cardinality");
        count += batch->num_rows();
    }
    Check(count == 2, "zero-column scan preserves duplicate matches");
    const auto capabilities = engine.GetCapabilities();
    Check(capabilities.backend == glamdb::BackendKind::LocalStorage && capabilities.metadata && capabilities.scan &&
            capabilities.append && capabilities.file_pruning && capabilities.row_group_pruning &&
            !capabilities.durability_qualified,
        "native capabilities report pruning without claiming qualified durability");
    const auto history = Take(engine.ListSnapshots(CollectionId), "snapshot history");
    Check(history.snapshots.size() == 2 && history.snapshots[0].snapshot_id == snapshotA &&
            history.snapshots[1].snapshot_id == snapshotB && history.snapshots[1].parent_snapshot_id == snapshotA,
        "retained history and parent identity");
    const auto a = Take(engine.FindAppendOutcome(CollectionId, OperationA), "metadata receipt A");
    const auto b = Take(engine.FindAppendOutcome(CollectionId, OperationB), "metadata receipt B");
    Check(a && b && a->result && b->result && history.snapshots[0].receipt == *a->result &&
            history.snapshots[1].receipt == *b->result,
        "history includes original durable receipts");
    const auto firstPage = Take(engine.ListSnapshotsPage(CollectionId, {.PageSize = 1}), "history page");
    const auto secondPage =
        Take(engine.ListSnapshotsPage(CollectionId, {.PageSize = 1, .PageToken = firstPage.NextPageToken}),
            "history continuation");
    Check(firstPage.snapshots.size() == 1 && secondPage.snapshots.size() == 1 &&
            firstPage.snapshots[0].snapshot_id == snapshotA && secondPage.snapshots[0].snapshot_id == snapshotB &&
            firstPage.identity == secondPage.identity && secondPage.NextPageToken.empty(),
        "paged history preserves ancestry and root identity");
    const auto collectionPage = Take(engine.ListCollectionsPage({.PageSize = 1}), "collection page");
    const auto collectionCount = Take(engine.ListCollections(), "collection count").size();
    Check(collectionPage.CreationHighWaterMark == static_cast<int64_t>(collectionCount) &&
            collectionPage.Collections.size() == 1 && collectionPage.NextPageToken.empty() == (collectionCount == 1),
        "collection page reports its creation high-water mark");
    for (const auto& [selector, rows, files] : {std::tuple{glamdb::SnapshotSelector::Latest(), 5, 2},
             std::tuple{glamdb::SnapshotSelector::Id(snapshotA), 3, 1},
             std::tuple{glamdb::SnapshotSelector::InitialEmpty(), 0, 0}})
    {
        const auto stats = Take(engine.GetStatistics(CollectionId, selector), "selected statistics");
        Check(stats.rows.value == rows && stats.data_files.value == files && !stats.logical_bytes.value &&
                stats.rows.kind == glamdb::MetricKind::Metadata && stats.identity == history.identity,
            "view-specific metadata with unavailable logical size");
        const auto listing = Take(engine.ListFiles(CollectionId, selector), "selected files");
        Check(listing.Identity == stats.identity && listing.SelectorResolved == stats.selector_resolved &&
                static_cast<int64_t>(listing.Files.size()) == files,
            "manifest file membership uses the selected view");
        int64_t fileRows = 0;
        for (const auto& file : listing.Files) {
            const auto details = Take(engine.GetFileDetails(CollectionId, file.FileID, selector), "file details");
            Check(details.File == file && details.SelectorResolved == listing.SelectorResolved &&
                    !details.RowGroups.empty() && file.FileID.size() == 64,
                "opaque file IDs resolve only through manifest membership");
            int64_t groupRows = 0;
            for (const auto& group : details.RowGroups) {
                groupRows += group.RecordCount;
                Check(!group.Columns.empty() && group.Columns.front().FieldID == 1,
                    "Parquet row groups retain stable field IDs");
            }
            Check(groupRows == file.RecordCount, "row-group counts match the manifest");
            fileRows += file.RecordCount;
        }
        Check(fileRows == rows, "listed files match selected rows");
        const auto explanation =
            Take(engine.ExplainScan({.collection_id = CollectionId, .selector = selector}), "explain");
        Check(explanation.info.selector_resolved == stats.selector_resolved &&
                explanation.candidates.rows.value == rows && explanation.file_pruning &&
                explanation.row_group_pruning && explanation.estimated_output_rows.value == rows,
            "explain uses the selected fixed view");
    }
    auto zero = Take(engine.Scan({.collection_id = CollectionId, .limit = 0}), "zero limit");
    Check(!Take(zero->Next(), "zero next") && zero->Profile().data_files_read.value == 0 &&
            zero->Profile().rows_returned.value == 0 && zero->Profile().state == glamdb::ScanState::Complete,
        "zero-limit execution reads no data files");
}

void Write(glamdb::Engine& engine, const std::string& receiptPath)
{
    const auto created = engine.CreateCollection({CollectionId, "events", Schema});
    Check(created.disposition == glamdb::CommitDisposition::Committed && created.info && !created.error &&
            !created.info->current_snapshot_id,
        "create collection with the default production backend");
    const auto wrongMode = engine.UpsertDurable({CollectionId, UnknownOperation, {}, {MakeBatch(RowsA)}});
    Check(wrongMode.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted && wrongMode.error &&
            wrongMode.error->code == "COLLECTION_MODE_MISMATCH",
        "append collection refuses KV mutation");
    const auto valid = MakeBatch({{"bad", 7}});
    const auto wrongType = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::int64(), false), arrow::field("n", arrow::int64())}),
        1,
        {valid->column(1), valid->column(1)});
    const auto badInput = engine.AppendDurable({CollectionId, UnknownOperation, {}, {wrongType}});
    Check(badInput.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted && badInput.error &&
            badInput.error->code == "INVALID_ARGUMENT" &&
            !Take(engine.FindAppendOutcome(CollectionId, UnknownOperation), "unbound invalid input"),
        "wrong Arrow types fail before binding an outcome");
    CheckScan(engine, glamdb::SnapshotSelector::Latest(), {}, glamdb::SnapshotSelector::InitialEmpty());
    const auto a = engine.AppendDurable(
        {CollectionId, OperationA, {glamdb::SnapshotPreconditionKind::NoSnapshot, 0}, {MakeBatch(RowsA)}});
    const auto& receiptA = CheckReceipt(a, OperationA, 3);
    auto pinned = Take(engine.AcquireReadView({{{CollectionId}}, "installed-consumer"}), "acquire read view");
    auto splitPlan = Take(engine.PlanScan(pinned, 0, {}, "installed-consumer", {.SplitsPerPage = 1, .TargetBytes = 1}),
        "plan pinned split");
    Check(!receiptA.parent_snapshot_id, "first snapshot has no parent");
    auto fixedLatest = Take(engine.Scan({.collection_id = CollectionId, .batch_rows = 1}), "fixed latest scan");
    const auto b = engine.AppendDurable({CollectionId,
        OperationB,
        {glamdb::SnapshotPreconditionKind::Equals, receiptA.snapshot_id},
        {MakeBatch(RowsB)}});
    const auto& receiptB = CheckReceipt(b, OperationB, 2);
    auto delayed = Take(engine.ScanReadView(*pinned, 0, {}, "installed-consumer"), "delayed pinned scan");
    CheckRows(*delayed, RowsA, glamdb::SnapshotSelector::Id(receiptA.snapshot_id));
    const auto page = Take(engine.NextScanSplits(*splitPlan, "installed-consumer"), "page pinned splits after append");
    Check(page.Done && page.Splits.size() == 1 && page.Splits[0]->FileCount() == 1 &&
            page.Splits[0]->EstimatedBytes() > 0,
        "bounded physical split covers original file");
    splitPlan.reset();
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto split = Take(engine.ReadSplit(*page.Splits[0], "installed-consumer"), "replay split");
        CheckRows(*split, RowsA, glamdb::SnapshotSelector::Id(receiptA.snapshot_id));
    }
    Check(!engine.ReadSplit(*page.Splits[0], "wrong-scope"), "split rejects wrong authorization scope");
    auto admittedSplit = Take(engine.ReadSplit(*page.Splits[0], "installed-consumer"), "admit split before release");
    Check(engine.ReleaseReadView(*pinned, "installed-consumer").has_value(), "release read view");
    Check(!engine.ReadSplit(*page.Splits[0], "installed-consumer"), "released view rejects split replay");
    CheckRows(*admittedSplit, RowsA, glamdb::SnapshotSelector::Id(receiptA.snapshot_id));
    Check(receiptA.snapshot_id != receiptB.snapshot_id && receiptB.parent_snapshot_id == receiptA.snapshot_id,
        "second snapshot extends the first");
    CheckRows(*fixedLatest, RowsA, glamdb::SnapshotSelector::Id(receiptA.snapshot_id));
    CheckViews(engine, receiptA.snapshot_id, receiptB.snapshot_id);
    std::ofstream output(receiptPath);
    output << RenderReceipt(receiptA) << '\n' << RenderReceipt(receiptB) << '\n';
    output.close();
    Check(!output.fail(), "save original receipts for the next process");
    const auto files = Take(engine.ListFilesPage(CollectionId, {.PageSize = 1}), "file page before restart");
    Check(!files.NextPageToken.empty(), "multi-file listing has a continuation");
    std::ofstream cursorOutput(receiptPath + ".cursor");
    cursorOutput << files.NextPageToken << '\n';
    cursorOutput.close();
    Check(!cursorOutput.fail(), "save cursor for restart rejection");
}

void Reopen(glamdb::Engine& engine, const std::string& receiptPath)
{
    std::ifstream cursorInput(receiptPath + ".cursor");
    std::string oldCursor;
    Check(static_cast<bool>(std::getline(cursorInput, oldCursor)) && !oldCursor.empty(), "read pre-restart cursor");
    const auto stale = engine.ListFilesPage(CollectionId, {.PageSize = 1, .PageToken = oldCursor});
    Check(!stale && stale.error().code == "INVALID_CURSOR", "restart explicitly invalidates metadata cursors");
    const auto collection = Take(engine.GetCollection(CollectionId), "get reopened collection");
    Check(collection.schema == Schema && collection.name == "events", "persisted collection schema and name");
    Check(Take(engine.ListCollections(), "list reopened collections").size() == 3, "persisted collection count");
    const auto a = Take(engine.FindAppendOutcome(CollectionId, OperationA), "find receipt A");
    const auto b = Take(engine.FindAppendOutcome(CollectionId, OperationB), "find receipt B");
    if (!a || !b) {
        throw std::runtime_error("both append outcomes must survive reopen");
    }
    const auto& receiptA = CheckReceipt(*a, OperationA, 3);
    const auto& receiptB = CheckReceipt(*b, OperationB, 2);
    std::ifstream input(receiptPath);
    std::string expectedA;
    std::string expectedB;
    Check(static_cast<bool>(std::getline(input, expectedA)) && static_cast<bool>(std::getline(input, expectedB)),
        "read original receipts");
    Check(RenderReceipt(receiptA) == expectedA && RenderReceipt(receiptB) == expectedB,
        "every receipt field must survive reopen unchanged");
    Check(collection.current_snapshot_id == receiptB.snapshot_id, "persisted latest snapshot");
    Check(!Take(engine.FindAppendOutcome(CollectionId, UnknownOperation), "find unknown operation"),
        "unknown operation has no receipt");
    CheckViews(engine, receiptA.snapshot_id, receiptB.snapshot_id);
    const auto retryA = engine.AppendDurable(
        {CollectionId, OperationA, {glamdb::SnapshotPreconditionKind::NoSnapshot, 0}, {MakeBatch(RowsA)}});
    const auto retryB = engine.AppendDurable(
        {CollectionId, OperationB, {glamdb::SnapshotPreconditionKind::NoSnapshot, 0}, {MakeBatch(RowsB)}});
    Check(retryA == *a && retryB == *b, "retries return original outcomes despite stale preconditions");
    CheckViews(engine, receiptA.snapshot_id, receiptB.snapshot_id);
}

constexpr char KVCollectionId[] = "00112233-4455-6677-8899-aabbccddee00";
void WriteKV(glamdb::Engine& engine)
{
    glamdb::CreateCollectionRequest request{KVCollectionId, "keys", Schema};
    request.mode = glamdb::CollectionMode::KeyValue;
    Check(engine.GetCapabilities().key_value, "installed KV capability");
    Check(engine.CreateCollection(request).disposition == glamdb::CommitDisposition::Committed, "create installed KV");
    const auto wrongMode = engine.AppendDurable({KVCollectionId, UnknownOperation, {}, {MakeBatch(RowsA)}});
    Check(wrongMode.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted && wrongMode.error &&
            wrongMode.error->code == "COLLECTION_MODE_MISMATCH",
        "KV collection refuses raw append");
    const auto upsert =
        engine.UpsertDurable({KVCollectionId, OperationA, {}, {MakeBatch({{"a", 1}, {"a", 9}, {"b", 2}})}});
    Check(upsert.disposition == glamdb::CommitDisposition::Committed && upsert.result &&
            upsert.result->receipt_version == 3 && upsert.result->rows_affected == 2,
        "installed full-row upsert");
    const auto removed = engine.DeleteDurable({KVCollectionId, OperationB, {}, {"b", "b", "missing"}});
    Check(removed.disposition == glamdb::CommitDisposition::Committed && removed.result &&
            removed.result->rows_affected == 1,
        "installed safe delete");
    const auto noOp = engine.DeleteDurable({KVCollectionId, UnknownOperation, {}, {"absent"}});
    Check(noOp.disposition == glamdb::CommitDisposition::Committed && noOp.result && !noOp.result->snapshot_created,
        "installed durable no-op");
}

void ReopenKV(glamdb::Engine& engine)
{
    const auto collection = Take(engine.GetCollection(KVCollectionId), "installed KV collection");
    Check(collection.mode == glamdb::CollectionMode::KeyValue, "KV mode survives fresh-process reopen");
    const auto upsert = Take(engine.FindAppendOutcome(KVCollectionId, OperationA), "find KV upsert");
    if (!upsert || !upsert->result) {
        throw std::runtime_error("KV receipt missing after reopen");
    }
    Check(upsert->result->receipt_version == 3, "KV receipt survives reopen");
    const auto latest = Take(engine.Lookup({KVCollectionId, {"a", "b", "a"}}), "installed latest lookup");
    Check(latest.rows.size() == 3, "installed lookup result count");
    const auto& firstHit = latest.rows.at(0);
    Check(firstHit && !latest.rows[1] && latest.rows[2] == firstHit && std::get<int64_t>((*firstHit)[1]) == 9,
        "installed ordered hits and misses");
    const auto historical =
        Take(engine.Lookup({KVCollectionId, {"b"}, glamdb::SnapshotSelector::Id(upsert->result->snapshot_id)}),
            "installed historical lookup");
    const auto& oldHit = historical.rows.at(0);
    Check(oldHit && std::get<int64_t>((*oldHit)[1]) == 2, "installed retained row survives delete");
    const auto noOp = engine.DeleteDurable({KVCollectionId, UnknownOperation, {}, {"absent"}});
    Check(noOp.disposition == glamdb::CommitDisposition::Committed && noOp.result && !noOp.result->snapshot_created,
        "installed original no-op retry");
    Check(Take(engine.ListSnapshots(KVCollectionId), "installed KV history").snapshots.size() == 2,
        "no-op excluded from installed history");
}

void CheckUnavailable(glamdb::Engine& engine)
{
    const auto capabilities = engine.GetCapabilities();
    Check(capabilities.backend == glamdb::BackendKind::Unavailable && !capabilities.metadata && !capabilities.scan &&
            !capabilities.append && !capabilities.file_pruning && !capabilities.row_group_pruning &&
            !capabilities.durability_qualified,
        "storage-disabled capabilities");
    const auto view = engine.AcquireReadView({{{CollectionId}}, "installed-consumer"});
    Check(!view && view.error().code == "UNSUPPORTED_FEATURE", "storage-disabled read views are unavailable");
    const auto stats = engine.GetStatistics(CollectionId);
    const auto history = engine.ListSnapshots(CollectionId);
    const auto explain = engine.ExplainScan({.collection_id = CollectionId});
    const auto files = engine.ListFiles(CollectionId);
    const auto details = engine.GetFileDetails(CollectionId, std::string(64, '0'));
    const auto historyPage = engine.ListSnapshotsPage(CollectionId);
    const auto filePage = engine.ListFilesPage(CollectionId);
    Check(!stats && stats.error().code == "DEPENDENCY_UNAVAILABLE" && !history &&
            history.error().code == "DEPENDENCY_UNAVAILABLE" && !explain &&
            explain.error().code == "DEPENDENCY_UNAVAILABLE" && !files &&
            files.error().code == "DEPENDENCY_UNAVAILABLE" && !details &&
            details.error().code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled metadata reports dependency errors");
    Check(!historyPage && historyPage.error().code == "DEPENDENCY_UNAVAILABLE" && !filePage &&
            filePage.error().code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled pages report dependency errors");
    const auto created = engine.CreateCollection({CollectionId, "events", Schema});
    Check(created.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted && !created.info && created.error &&
            created.error->code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled create must report DEPENDENCY_UNAVAILABLE");
    const auto appended = engine.AppendDurable({CollectionId, OperationA, {}, {MakeBatch(RowsA)}});
    Check(appended.disposition == glamdb::CommitDisposition::DefinitelyNotCommitted &&
            appended.phase == glamdb::CommitPhase::Validation && !appended.result && !appended.recovery &&
            appended.error && appended.error->code == "DEPENDENCY_UNAVAILABLE",
        "storage-disabled append must report DEPENDENCY_UNAVAILABLE before binding");
    const auto scan = engine.Scan({.collection_id = CollectionId, .predicate = CompoundDupPredicate()});
    Check(!scan && scan.error().code == "DEPENDENCY_UNAVAILABLE", "storage-disabled scan dependency error");
    Check(Take(engine.ListCollections(), "list storage-disabled collections").empty(), "no fabricated collection");
    const auto outcome = engine.FindAppendOutcome(CollectionId, OperationA);
    Check(!outcome && outcome.error().code == "COLLECTION_NOT_FOUND", "no fabricated append outcome");
}

} // namespace

int main(int argc, char** argv)
try {
    if (argc != 5) {
        std::cerr << "usage: consumer <write|reopen> <store-root> <receipt-file> <storage|nostorage>\n";
        return 2;
    }
    const std::string mode = argv[1];
    const std::string storage = argv[4];
    Check(mode == "write" || mode == "reopen", "unknown consumer phase");
    Check(storage == "storage" || storage == "nostorage", "explicit storage expectation required");
    auto engine = mode == "write" ? Take(glamdb::CreateStore({StoreId, argv[2]}), "create store")
                                  : Take(glamdb::OpenExisting({argv[2], StoreId}), "reopen store");
    Check(engine->GetStoreInfo().store_id == StoreId, "store identity");
    CheckContract(*engine, storage == "storage");
    if (storage == "nostorage") {
        CheckUnavailable(*engine);
    } else if (mode == "write") {
        Check(Take(engine->ListCollections(), "list initial collections").empty(), "new store is empty");
        Write(*engine, argv[3]);
        WriteKV(*engine);
        CheckNegotiation(*engine, true);
    } else {
        Reopen(*engine, argv[3]);
        ReopenKV(*engine);
        CheckNegotiation(*engine, false);
    }
    const auto shutdown = engine->Shutdown(std::chrono::milliseconds(1000));
    Check(shutdown.has_value(), "shutdown");
    std::cout << "Installed consumer " << storage << ' ' << mode << ": PASS\n";
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
