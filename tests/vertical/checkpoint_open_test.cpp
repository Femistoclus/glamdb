// Verified checkpoints (catalog version 6): a fresh open of an archive-tier
// store rebuilds every collection from its checkpoint, current root and
// current manifests. Evidence: the rebuilt in-memory state and every read
// equal what full verification produces (differential), the open reads no
// footer and the same catalog work at two history sizes (counters), and any
// tampering either falls back to full verification or is refused.

#include "core/catalog/sqlite.h"
#include "core/storage/base_state.h"
#include "core/storage/format_verifier.h"
#include "core/storage/metadata_validation.h"
#include "core/storage/scan_index.h"
#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/sql_trace.h"
#include "tests/vertical/vertical_support.h"

#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/table_metadata.h>

#include <chrono>
#include <format>
#include <iostream>
#include <map>
#include <random>

namespace glamdb {
namespace {
using namespace vertical;
using NGlamDB::NStorage::TBaseState;

std::string Op(int64_t index)
{
    return std::format("00000000-0000-4000-8000-{:012x}", index + 1);
}

std::vector<TInputRow> RowsFor(int64_t index, int64_t count = 2)
{
    std::vector<TInputRow> rows;
    for (int64_t row = 0; row < count; ++row) {
        rows.push_back({std::format("k{}-{}", index, row),
            index * 10 + row,
            static_cast<double>(index) / 4.0,
            (index + row) % 2 == 0,
            index * 1000 + row});
    }
    return rows;
}

class TBatchesSource final : public BulkAppendSource
{
public:
    explicit TBatchesSource(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
        : Batches_(std::move(batches))
    { }
    Status Rewind() override
    {
        Position_ = 0;
        return {};
    }
    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (Position_ < Batches_.size()) {
            return Batches_[Position_++];
        }
        return std::shared_ptr<arrow::RecordBatch>{};
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> Batches_;
    size_t Position_ = 0;
};

using TStates = std::map<std::string, std::shared_ptr<const TBaseState>, std::less<>>;

// Field by field: everything recovery builds and later reads or charges use.
void ExpectSameState(const TBaseState& fast, const TBaseState& full, const std::string& where)
{
    SCOPED_TRACE(where);
    EXPECT_EQ(fast.MetadataLocation, full.MetadataLocation);
    ASSERT_TRUE(fast.Metadata && full.Metadata);
    EXPECT_TRUE(*fast.Metadata == *full.Metadata);
    EXPECT_EQ(fast.Usage, full.Usage);
    EXPECT_EQ(fast.CurrentCommitMetadataBytes, full.CurrentCommitMetadataBytes);
    ASSERT_EQ(fast.CurrentManifests.size(), full.CurrentManifests.size());
    for (size_t index = 0; index < fast.CurrentManifests.size(); ++index) {
        EXPECT_TRUE(NGlamDB::NStorage::EqualManifest(fast.CurrentManifests[index], full.CurrentManifests[index]))
            << index;
    }
    EXPECT_TRUE(fast.ExpiredSnapshotIDs == full.ExpiredSnapshotIDs);
    EXPECT_TRUE(fast.RetainedManifestPaths == full.RetainedManifestPaths);
    EXPECT_EQ(fast.RetainedManifestPaths.StringBytes(), full.RetainedManifestPaths.StringBytes());
    ASSERT_TRUE(fast.ScanIndex && full.ScanIndex);
    const auto& a = *fast.ScanIndex;
    const auto& b = *full.ScanIndex;
    EXPECT_EQ(a.KeyValue, b.KeyValue);
    ASSERT_EQ(a.Files.size(), b.Files.size());
    size_t position = 0;
    auto other = b.Files.begin();
    for (const auto& file : a.Files) {
        ASSERT_EQ(file.Path, other->Path) << position;
        ASSERT_EQ(file.SizeBytes, other->SizeBytes) << position;
        ASSERT_EQ(file.RecordCount, other->RecordCount) << position;
        ASSERT_EQ(file.IntroducedSequence, other->IntroducedSequence) << position;
        ASSERT_EQ(file.RemovedSequence, other->RemovedSequence) << position;
        ASSERT_TRUE(file.DataFile && other->DataFile && *file.DataFile == *other->DataFile) << position;
        ++other;
        ++position;
    }
    ASSERT_EQ(a.Snapshots.size(), b.Snapshots.size());
    for (size_t index = 0; index < a.Snapshots.size(); ++index) {
        EXPECT_EQ(a.Snapshots[index].SnapshotID, b.Snapshots[index].SnapshotID) << index;
        EXPECT_EQ(a.Snapshots[index].FileCount, b.Snapshots[index].FileCount) << index;
        EXPECT_EQ(a.Snapshots[index].SequenceNumber, b.Snapshots[index].SequenceNumber) << index;
    }
    EXPECT_EQ(a.Files.DescriptorBytes(), b.Files.DescriptorBytes());
    EXPECT_EQ(NGlamDB::NStorage::ScanIndexBytes(a, fast.ExpiredSnapshotIDs, fast.RetainedManifestPaths),
        NGlamDB::NStorage::ScanIndexBytes(b, full.ExpiredSnapshotIDs, full.RetainedManifestPaths));
}

// Answers of the public API that depend on the rebuilt state.
std::string ReadDigest(const Engine& engine,
    const std::vector<std::string>& collections,
    const std::vector<std::pair<std::string, int64_t>>& operations,
    std::mt19937_64& random)
{
    std::string text;
    for (const auto& collection : collections) {
        const auto history = engine.ListSnapshots(collection);
        EXPECT_TRUE(history) << history.error().message;
        if (!history) {
            continue;
        }
        text += std::format("{}:{}|", collection, history->snapshots.size());
        for (const auto& snapshot : history->snapshots) {
            text += std::format("{}:{};", snapshot.snapshot_id, snapshot.receipt.operation_id);
        }
        // A sample of snapshots (latest, oldest and random): rows in output
        // order, statistics and file listings.
        std::vector<int64_t> sample;
        if (!history->snapshots.empty()) {
            sample.push_back(history->snapshots.front().snapshot_id);
            sample.push_back(history->snapshots.back().snapshot_id);
            for (int probe = 0; probe < 4; ++probe) {
                sample.push_back(history->snapshots[random() % history->snapshots.size()].snapshot_id);
            }
        }
        for (const auto snapshot : sample) {
            ScanRequest request{collection};
            request.selector = SnapshotSelector::Id(snapshot);
            auto cursor = engine.Scan(request);
            EXPECT_TRUE(cursor) << cursor.error().code << ": " << cursor.error().message;
            if (cursor) {
                while (auto next = (*cursor)->Next()) {
                    if (!*next) {
                        break;
                    }
                    for (const auto& row : Render(**next)) {
                        text += row + ";";
                    }
                }
            }
            const auto statistics = engine.GetStatistics(collection, request.selector);
            EXPECT_TRUE(statistics) << statistics.error().message;
            if (statistics) {
                text += std::format("s{}/{}/{};",
                    statistics->rows.value.value_or(-1),
                    statistics->data_files.value.value_or(-1),
                    statistics->compressed_data_bytes.value.value_or(-1));
            }
            const auto files = engine.ListFiles(collection, request.selector);
            EXPECT_TRUE(files) << files.error().message;
            if (files) {
                for (const auto& file : files->Files) {
                    text += file.FileID + ",";
                }
            }
        }
        const auto archive = engine.GetSnapshotArchive(collection);
        if (archive) {
            text += std::format("a{}/{}/{};",
                archive->root_snapshots,
                archive->archived_snapshots,
                archive->expired_snapshots);
        }
    }
    for (const auto& [collection, index] : operations) {
        const auto outcome = engine.FindAppendOutcome(collection, Op(index));
        EXPECT_TRUE(outcome) << outcome.error().message;
        if (outcome && *outcome) {
            const auto& found = **outcome;
            text += std::format("o{}:{}:{};",
                index,
                static_cast<int>(found.disposition),
                found.result ? found.result->snapshot_id : 0);
        }
    }
    return text;
}

class TCheckpointOpenTest : public ::testing::Test
{
protected:
    test::TempDir Directory_;
    std::filesystem::path Root_ = Directory_.path / "store";
    test::JournalFaultVfs Vfs_;
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;
    std::map<std::string, std::vector<int64_t>> Committed_;
    std::vector<std::pair<std::string, int64_t>> Operations_;
    int64_t Next_ = 0;

    // As the default factory: the backend checks the engine's history budgets.
    detail::BackendFactory Factory(const Limits& limits)
    {
        return [this, history = limits.history](const std::filesystem::path& root,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.ApplyHistoryLimits(history);
            auto backend = detail::TLocalStorageBackend::Open(root, info, std::move(options));
            if (!backend) {
                return std::unexpected(backend.error());
            }
            Backend_ = *backend;
            return std::shared_ptr<detail::StorageBackend>(*backend);
        };
    }

    static Limits TestLimits()
    {
        Limits limits;
        limits.max_operations = 8192;
        limits.history.max_manifests = 16384;
        return limits;
    }

    void Create(const std::vector<std::pair<std::string, std::optional<int64_t>>>& collections)
    {
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string(), "local-delete-extra-v1", TestLimits()},
                Factory(TestLimits()),
                Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        for (const auto& [id, hot] : collections) {
            CreateCollectionRequest request{id, "c" + id.substr(id.size() - 4), Schema};
            request.hot_snapshots = hot;
            const auto outcome = Engine_->CreateCollection(request);
            ASSERT_EQ(outcome.disposition, CommitDisposition::Committed) << test::Present(outcome.error).message;
        }
    }

    void TearDown() override
    {
        Vfs_.Disarm();
        if (Engine_) {
            [[maybe_unused]] auto stopped = Engine_->Shutdown(std::chrono::seconds(5));
        }
    }

    // Opens in this process (the backend instance is fresh; only the shared
    // process caches of SQLite and the allocator remain).
    Result<std::shared_ptr<Engine>> Open(bool full, Limits limits = TestLimits())
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
            Engine_.reset();
        }
        Backend_.reset();
        OpenStoreRequest request{Root_.string(), test::store_id, "local-delete-extra-v1", limits};
        request.verify_full = full;
        return detail::EngineFactory::OpenWith(request, Factory(limits), Vfs_.Name());
    }

    void Reopen(bool full)
    {
        auto opened = Open(full);
        ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
        Engine_ = std::move(*opened);
    }

    AppendOutcome Append(const std::string& collection, SnapshotPrecondition precondition = {})
    {
        const auto index = Next_++;
        auto outcome = Engine_->AppendDurable({collection, Op(index), precondition, {Batch(RowsFor(index))}});
        Operations_.emplace_back(collection, index);
        if (outcome.disposition == CommitDisposition::Committed) {
            Committed_[collection].push_back(outcome.result->snapshot_id);
        }
        return outcome;
    }

    AppendOutcome Bulk(const std::string& collection, int64_t batches)
    {
        const auto index = Next_++;
        std::vector<std::shared_ptr<arrow::RecordBatch>> source;
        int64_t rows = 0;
        for (int64_t batch = 0; batch < batches; ++batch) {
            source.push_back(Batch(RowsFor(index * 1000 + batch, 2 + batch)));
            rows += 2 + batch;
        }
        auto outcome = Engine_->BulkAppendDurable({.collection_id = collection,
            .operation_id = Op(index),
            .rows = rows,
            .source = std::make_shared<TBatchesSource>(std::move(source))});
        Operations_.emplace_back(collection, index);
        if (outcome.disposition == CommitDisposition::Committed) {
            Committed_[collection].push_back(outcome.result->snapshot_id);
        }
        return outcome;
    }

    int64_t CatalogVersion() const
    {
        detail::Database catalog;
        catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
        return catalog.Integer("PRAGMA user_version");
    }

    void Tamper(const std::string& sql)
    {
        detail::Database catalog;
        catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        catalog.Execute(sql);
        ASSERT_GE(catalog.Changes(), 0);
    }

    // Opens from checkpoints and with full verification, in that order, and
    // requires identical in-memory states and read answers.
    void ExpectCheckpointOpenMatchesFullVerification(const std::vector<std::string>& collections, int step)
    {
        Reopen(false);
        ASSERT_TRUE(Backend_->OpenedFromCheckpoints()) << "step " << step;
        const auto fast = Backend_->ConfirmedStates();
        std::mt19937_64 sampleA(static_cast<uint64_t>(step));
        const auto fastReads = ReadDigest(*Engine_, collections, Operations_, sampleA);
        Reopen(true);
        ASSERT_FALSE(Backend_->OpenedFromCheckpoints());
        const auto full = Backend_->ConfirmedStates();
        std::mt19937_64 sampleB(static_cast<uint64_t>(step));
        const auto fullReads = ReadDigest(*Engine_, collections, Operations_, sampleB);
        ASSERT_EQ(fast.size(), full.size());
        for (const auto& [id, state] : fast) {
            ASSERT_TRUE(full.contains(id));
            ExpectSameState(*state, *full.at(id), std::format("step {} collection {}", step, id));
        }
        EXPECT_EQ(fastReads, fullReads) << "step " << step;
    }
};

// Randomized histories over two collections: ordinary and bulk appends (bulk
// files share commits and manifests), failed appends, archive maintenance,
// setting changes, expiration of archived and root snapshots, and more than
// 100 manifests per collection so that manifests merge.
TEST_F(TCheckpointOpenTest, RandomizedHistoriesOpenIdenticallyFromCheckpointsAndByFullVerification)
{
    const std::string first = test::collection_id;
    const std::string second = SecondCollection;
    Create({{first, 3}, {second, std::nullopt}});
    // The second collection gets checkpoints too once the store has the tier.
    ASSERT_EQ(CatalogVersion(), 6);
    std::mt19937_64 random(0xC4EC4);
    const std::vector<std::string> collections{first, second};
    for (int step = 1; step <= 330; ++step) {
        const auto& collection = collections[random() % 3 == 0 ? 1 : 0];
        const auto action = random() % 100;
        if (action < 70) {
            ASSERT_EQ(Append(collection).disposition, CommitDisposition::Committed) << step;
        } else if (action < 82) {
            ASSERT_EQ(Bulk(collection, 2 + static_cast<int64_t>(random() % 3)).disposition,
                CommitDisposition::Committed)
                << step;
        } else if (action < 86 && !Committed_[collection].empty()) {
            // A stale precondition fails permanently and still takes a slot.
            const auto stale = Committed_[collection].front();
            const auto outcome = Append(collection,
                SnapshotPrecondition{.kind = SnapshotPreconditionKind::Equals, .snapshot_id = stale});
            ASSERT_EQ(outcome.disposition,
                Committed_[collection].size() == 1 ? CommitDisposition::Committed
                                                   : CommitDisposition::DefinitelyNotCommitted)
                << step;
        } else if (action < 90) {
            ASSERT_TRUE(Engine_->ArchiveSnapshots(collection)) << step;
        } else if (action < 94) {
            const auto hot = std::vector<std::optional<int64_t>>{2, 5, std::nullopt}[random() % 3];
            const auto updated = Engine_->UpdateSnapshotArchive({collection, hot});
            // hot_snapshots may not exceed an expiration keep_last.
            ASSERT_TRUE(updated || updated.error().code == "INVALID_ARGUMENT") << updated.error().message;
        } else if (action < 97) {
            const auto expired = Engine_->ExpireSnapshots(
                {collection, SnapshotRetentionPolicy{.keep_last = static_cast<int64_t>(20 + random() % 20)}});
            ASSERT_TRUE(expired || expired.error().code == "INVALID_ARGUMENT") << expired.error().message;
        }
        if (step % 55 == 0) {
            ExpectCheckpointOpenMatchesFullVerification(collections, step);
            if (HasFatalFailure()) {
                return;
            }
        }
    }
    // Expiration keeps the root short; merges must still have happened.
    EXPECT_GT(Committed_[first].size(), 150U);
    const auto states = Backend_->ConfirmedStates();
    EXPECT_TRUE(std::ranges::any_of(states.at(first)->CurrentManifests, [](const auto& manifest) {
        return manifest.existing_files_count.value_or(0) > 0;
    }));
}

// What a fresh open reads does not depend on how much history exists: no
// data-file footer, the same SQL statements and rows, and a bounded number of
// Avro containers (root lists plus current manifests). Full verification
// grows with history at the same points.
TEST_F(TCheckpointOpenTest, OpenReadsNoFooterAndTheSameCatalogWorkAtTwoHistorySizes)
{
    constexpr int64_t hot = 8;
    Create({{test::collection_id, hot}});
    struct TOpenCost
    {
        uint64_t Footers = 0;
        uint64_t Avro = 0;
        uint64_t RootParses = 0;
        uint64_t Statements = 0;
        uint64_t Rows = 0;
        int64_t IntegrityChecks = 0;
        int64_t StoreOperationPages = 0;
    };
    const auto measure = [&](bool full) {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
            Engine_.reset();
        }
        test::SqlTrace trace;
        const auto footers = NGlamDB::NStorage::ParquetFooterReads();
        const auto avro = NGlamDB::NStorage::AvroContainerChecks();
        const auto parses = NGlamDB::NStorage::TableMetadataParses();
        const auto work = detail::ReadCatalogWork();
        auto opened = Open(full);
        EXPECT_TRUE(opened) << opened.error().message;
        const auto after = detail::ReadCatalogWork();
        TOpenCost cost{.Footers = NGlamDB::NStorage::ParquetFooterReads() - footers,
            .Avro = NGlamDB::NStorage::AvroContainerChecks() - avro,
            .RootParses = NGlamDB::NStorage::TableMetadataParses() - parses,
            .Statements = after.statements - work.statements,
            .Rows = after.rows_returned - work.rows_returned};
        const auto statements = trace.Take();
        cost.IntegrityChecks = test::CountContaining(statements, "integrity_check");
        cost.StoreOperationPages =
            test::CountContaining(statements, "FROM operations WHERE (collection_id,operation_id)>");
        EXPECT_EQ(Backend_->OpenedFromCheckpoints(), !full);
        Engine_ = std::move(*opened);
        return cost;
    };
    std::vector<std::pair<int64_t, TOpenCost>> fast, full;
    // Both sizes end at the same phase of the archive cycle (hot + slack + 1).
    for (const int64_t history : {153, 459}) {
        while (Next_ < history) {
            ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
        }
        full.emplace_back(history, measure(true));
        fast.emplace_back(history, measure(false));
        const auto status = Engine_->GetSnapshotArchive(test::collection_id);
        ASSERT_TRUE(status);
        for (const auto& [label, costs] : {std::pair{"checkpoint", &fast}, std::pair{"full", &full}}) {
            const auto& cost = costs->back().second;
            std::cout << std::format("checkpoint_open mode={} history={} root_snapshots={} archived={} footers={} "
                                     "avro={} root_parses={} statements={} rows={} integrity_checks={} "
                                     "store_operation_pages={}\n",
                label,
                history,
                status->root_snapshots,
                status->archived_snapshots,
                cost.Footers,
                cost.Avro,
                cost.RootParses,
                cost.Statements,
                cost.Rows,
                cost.IntegrityChecks,
                cost.StoreOperationPages);
        }
    }
    for (const auto& [history, cost] : fast) {
        EXPECT_EQ(cost.Footers, 0U) << history;
        EXPECT_EQ(cost.IntegrityChecks, 0) << history;
        EXPECT_EQ(cost.StoreOperationPages, 0) << history;
        EXPECT_EQ(cost.Statements, fast.front().second.Statements) << history;
        EXPECT_EQ(cost.Rows, fast.front().second.Rows) << history;
        EXPECT_EQ(cost.RootParses, fast.front().second.RootParses) << history;
        // Root lists (at most hot + slack) plus current manifests (< 101).
        EXPECT_LE(cost.Avro, static_cast<uint64_t>(2 * hot + 101)) << history;
    }
    EXPECT_GT(full.back().second.Footers, full.front().second.Footers);
    EXPECT_GT(full.back().second.Rows, full.front().second.Rows);
}

// A tampered checkpoint never makes open fail wrongly or succeed with other
// state: the open falls back to full verification, which re-establishes the
// checkpoint for the next open.
class TCheckpointTamperingTest
    : public TCheckpointOpenTest
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TCheckpointTamperingTest, FallsBackToFullVerificationAndReseeds)
{
    Create({{test::collection_id, 2}});
    for (int step = 0; step < 12; ++step) {
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    }
    Reopen(true);
    std::mt19937_64 sample(1);
    const auto expected = ReadDigest(*Engine_, {test::collection_id}, Operations_, sample);
    const auto states = Backend_->ConfirmedStates();
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    Tamper(GetParam());
    Reopen(false);
    EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
    std::mt19937_64 again(1);
    EXPECT_EQ(ReadDigest(*Engine_, {test::collection_id}, Operations_, again), expected);
    ExpectSameState(*Backend_->ConfirmedStates().at(test::collection_id), *states.at(test::collection_id), "fallback");
    Reopen(false);
    EXPECT_TRUE(Backend_->OpenedFromCheckpoints());
    ExpectSameState(*Backend_->ConfirmedStates().at(test::collection_id), *states.at(test::collection_id), "reseeded");
}

INSTANTIATE_TEST_SUITE_P(Checkpoint,
    TCheckpointTamperingTest,
    ::testing::Values("UPDATE collection_checkpoints SET "
                      "root_sha256='0000000000000000000000000000000000000000000000000000000000000000'",
        "UPDATE collection_checkpoints SET root_bytes=root_bytes+1",
        "UPDATE collection_checkpoints SET usage_data_files=usage_data_files+1",
        "UPDATE collection_checkpoints SET usage_manifest_list_bytes=1099511627776",
        "UPDATE collection_checkpoints SET operations=operations+1",
        "UPDATE collection_checkpoints SET archived=archived-1",
        "UPDATE collection_checkpoints SET expired=expired+1",
        "UPDATE collection_checkpoints SET sequence_number=sequence_number-1",
        "UPDATE collection_checkpoints SET snapshot_id=snapshot_id+1",
        "DELETE FROM collection_checkpoints"));

// Cumulative byte totals (footers, manifests, lists) cannot be re-derived
// without reading every historical file: like receipt totals and archive row
// byte fields they are trusted catalog state on the checkpoint path, within
// the configured budgets. Full verification recomputes them and re-seeds.
TEST_F(TCheckpointOpenTest, ByteTotalsAreTrustedUntilFullVerificationRecomputesThem)
{
    Create({{test::collection_id, 2}});
    for (int step = 0; step < 6; ++step) {
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    }
    Reopen(true);
    const auto usage = Backend_->ConfirmedStates().at(test::collection_id)->Usage;
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    Tamper("UPDATE collection_checkpoints SET usage_footer_bytes=usage_footer_bytes+1");
    Reopen(false);
    ASSERT_TRUE(Backend_->OpenedFromCheckpoints());
    EXPECT_EQ(Backend_->ConfirmedStates().at(test::collection_id)->Usage.FooterBytes, usage.FooterBytes + 1);
    ASSERT_TRUE(Engine_->VerifyStoreFull());
    EXPECT_EQ(Backend_->ConfirmedStates().at(test::collection_id)->Usage, usage);
    Reopen(false);
    ASSERT_TRUE(Backend_->OpenedFromCheckpoints());
    EXPECT_EQ(Backend_->ConfirmedStates().at(test::collection_id)->Usage, usage);
}

// Damaged files: the checkpoint path detects the current root and current
// manifests; full verification then refuses with CORRUPTION.
TEST_F(TCheckpointOpenTest, DamagedRootOrCurrentManifestIsRefused)
{
    Create({{test::collection_id, 2}});
    for (int step = 0; step < 8; ++step) {
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    }
    Reopen(true);
    const auto state = Backend_->ConfirmedStates().at(test::collection_id);
    const auto local = [&](const std::string& uri) {
        return Root_ / "warehouse" / uri.substr(uri.find("/warehouse/") + 11);
    };
    const auto root = local(state->MetadataLocation);
    const auto manifest = local(state->CurrentManifests.front().manifest_path);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    for (const auto& path : {root, manifest}) {
        const auto original = ReadBytes(path);
        auto bytes = original;
        if (path == root) {
            bytes[bytes.size() / 2] = static_cast<char>(bytes[bytes.size() / 2] ^ 0x20);
        } else {
            // Not every flipped Avro byte is detectable without a checksum; a
            // length change always is.
            bytes.pop_back();
        }
        WriteBytes(path, bytes);
        auto opened = Open(false);
        ASSERT_FALSE(opened) << path;
        EXPECT_EQ(opened.error().code, "CORRUPTION") << opened.error().message;
        WriteBytes(path, original);
        // The detected corruption removed the checkpoints: the restored store
        // is verified fully once, then opens from re-established checkpoints.
        auto restored = Open(false);
        ASSERT_TRUE(restored) << restored.error().message;
        EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
        Engine_ = std::move(*restored);
        Reopen(false);
        EXPECT_TRUE(Backend_->OpenedFromCheckpoints());
        ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
        Engine_.reset();
    }
}

// Lower budgets are refused exactly as by full verification.
TEST_F(TCheckpointOpenTest, LowerLimitsAreRefusedLikeFullVerification)
{
    Create({{test::collection_id, 2}});
    for (int step = 0; step < 12; ++step) {
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    }
    Reopen(true);
    const auto usage = Backend_->ConfirmedStates().at(test::collection_id)->Usage;
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    std::vector<std::pair<std::string, Limits>> lowered;
    auto limits = TestLimits();
    limits.max_operations = static_cast<int64_t>(Operations_.size()) - 1;
    lowered.emplace_back("operations", limits);
    limits = TestLimits();
    limits.history.max_data_files = static_cast<int64_t>(usage.DataFiles) - 1;
    lowered.emplace_back("data files", limits);
    limits = TestLimits();
    limits.history.max_manifest_list_bytes = static_cast<int64_t>(usage.ManifestListBytes) - 1;
    lowered.emplace_back("list bytes", limits);
    limits = TestLimits();
    limits.history.max_footer_bytes = static_cast<int64_t>(usage.FooterBytes) - 1;
    lowered.emplace_back("footer bytes", limits);
    for (const auto& [name, lower] : lowered) {
        auto checkpointed = Open(false, lower);
        auto verified = Open(true, lower);
        ASSERT_FALSE(checkpointed) << name;
        ASSERT_FALSE(verified) << name;
        EXPECT_EQ(checkpointed.error().code, verified.error().code) << name;
    }
    // A refusal for CORRUPTION removed the checkpoints; full verification
    // under the original limits re-establishes them.
    Reopen(false);
    Reopen(false);
    EXPECT_TRUE(Backend_->OpenedFromCheckpoints());
}

// A version 5 store (archive tier without checkpoints) opens by full
// verification and keeps its version until a publication upgrades it; then
// the remaining collection gets its checkpoint at the next full open.
TEST_F(TCheckpointOpenTest, Version5StoresOpenFullyAndUpgradeOnTheFirstPublication)
{
    const std::string first = test::collection_id;
    const std::string second = SecondCollection;
    Create({{first, 2}, {second, 2}});
    for (int step = 0; step < 6; ++step) {
        ASSERT_EQ(Append(step % 2 == 0 ? first : second).disposition, CommitDisposition::Committed);
    }
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    Tamper("DROP INDEX operations_bound");
    Tamper("DROP TABLE collection_checkpoints");
    Tamper("DROP TABLE collection_hot_defaults");
    Tamper("PRAGMA user_version=5");
    Reopen(false);
    EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    EXPECT_EQ(CatalogVersion(), 5);
    Reopen(false);
    ASSERT_EQ(Append(first).disposition, CommitDisposition::Committed);
    EXPECT_EQ(CatalogVersion(), 6);
    // Only `first` has a checkpoint: full verification, which seeds `second`.
    Reopen(false);
    EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
    Reopen(false);
    EXPECT_TRUE(Backend_->OpenedFromCheckpoints());
    ExpectCheckpointOpenMatchesFullVerification({first, second}, 0);
}

// KV collections keep their full history in the root and never use the
// checkpoint path; a store with one opens by full verification.
TEST_F(TCheckpointOpenTest, StoresWithAKVCollectionOpenByFullVerification)
{
    Create({{test::collection_id, 2}});
    ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    SchemaSpec kv = Schema;
    CreateCollectionRequest request{SecondCollection, "kv", kv};
    request.mode = CollectionMode::KeyValue;
    ASSERT_EQ(Engine_->CreateCollection(request).disposition, CommitDisposition::Committed);
    Reopen(false);
    EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
}

// A lost outer COMMIT of an archiving append: the engine requires
// reconciliation (full verification); afterwards a fresh open uses the
// checkpoint and matches full verification.
TEST_F(TCheckpointOpenTest, LostCommitIsReconciledByFullVerificationThenOpensFromCheckpoints)
{
    for (const bool applied : {false, true}) {
        SCOPED_TRACE(applied);
        Root_ = Directory_.path / std::format("store-{}", applied);
        Engine_.reset();
        Next_ = 0;
        Operations_.clear();
        Committed_.clear();
        Create({{test::collection_id, 2}});
        for (int step = 0; step < 5; ++step) {
            ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
        }
        Vfs_.Arm(2, applied);
        const auto lost = Append(test::collection_id);
        EXPECT_EQ(lost.disposition, CommitDisposition::CommitUnknown);
        Vfs_.Disarm();
        const auto reconciled = Engine_->Reconcile();
        ASSERT_TRUE(reconciled) << reconciled.error().message;
        EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
        const auto retry = Engine_->AppendDurable(
            {test::collection_id, Op(Operations_.back().second), {}, {Batch(RowsFor(Operations_.back().second))}});
        EXPECT_EQ(retry.disposition,
            applied ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
        ExpectCheckpointOpenMatchesFullVerification({test::collection_id}, applied ? 1 : 0);
        ASSERT_TRUE(Engine_->VerifyStoreFull());
        EXPECT_FALSE(Backend_->OpenedFromCheckpoints());
        ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
        Engine_.reset();
    }
}

// The physical inventory counts archived lists, and the manifests only
// archived snapshots reference, as referenced metadata: the classification
// of one history is the same before and after archiving it, and after an
// open from checkpoints or by full verification.
TEST_F(TCheckpointOpenTest, FootprintClassifiesArchivedMetadataLikeRootMetadata)
{
    const auto footprint = [&] {
        const auto measured = Engine_->GetStorageFootprint();
        EXPECT_TRUE(measured) << measured.error().code << ": " << measured.error().message;
        return measured.value_or(StorageFootprint{});
    };
    const auto rootBytes = [&] {
        return Backend_->ConfirmedStates().at(test::collection_id)->Usage.RootBytes;
    };
    Create({{test::collection_id, std::nullopt}});
    for (int step = 0; step < 40; ++step) {
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    }
    const auto before = footprint();
    const auto beforeRoot = rootBytes();
    EXPECT_EQ(before.UnreferencedWarehouseFileBytes, 0U);
    ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, 2}));
    const auto moved = Engine_->ArchiveSnapshots(test::collection_id);
    ASSERT_TRUE(moved) << moved.error().message;
    ASSERT_EQ(Engine_->GetSnapshotArchive(test::collection_id)->archived_snapshots, 38);
    const auto after = footprint();
    const auto afterRoot = rootBytes();
    EXPECT_EQ(after.UnreferencedWarehouseFileBytes, 0U);
    EXPECT_EQ(after.Files, before.Files);
    EXPECT_EQ(after.CommittedDataFileBytes, before.CommittedDataFileBytes);
    EXPECT_EQ(after.DataFileBytes, before.DataFileBytes);
    // Only the root changed: it lists 2 instead of 40 snapshots.
    EXPECT_EQ(after.ReferencedMetadataFileBytes - afterRoot, before.ReferencedMetadataFileBytes - beforeRoot);
    for (const bool full : {false, true}) {
        Reopen(full);
        EXPECT_EQ(Backend_->OpenedFromCheckpoints(), !full);
        const auto reopened = footprint();
        EXPECT_EQ(reopened.UnreferencedWarehouseFileBytes, 0U) << full;
        EXPECT_EQ(reopened.ReferencedMetadataFileBytes, after.ReferencedMetadataFileBytes) << full;
        EXPECT_EQ(reopened.CommittedDataFileBytes, after.CommittedDataFileBytes) << full;
    }
    // Archiving while appending (hot_snapshots from creation) as well.
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    Root_ = Directory_.path / "hot";
    Create({{test::collection_id, 2}});
    for (int step = 0; step < 40; ++step) {
        ASSERT_EQ(Append(test::collection_id).disposition, CommitDisposition::Committed);
    }
    ASSERT_GT(Engine_->GetSnapshotArchive(test::collection_id)->archived_snapshots, 30);
    EXPECT_EQ(footprint().UnreferencedWarehouseFileBytes, 0U);
}

} // namespace
} // namespace glamdb
