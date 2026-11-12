#include "core/api/binary.h"
#include "core/api/codec.h"
#include "core/catalog/sqlite.h"
#include "core/storage/format_verifier.h"
#include "core/storage/snapshot_archive.h"
#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <format>
#include <iostream>
#include <map>
#include <random>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace glamdb {
namespace {
using namespace vertical;

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

// Every read answer of one snapshot; FileIDs only compare within one store.
struct TView
{
    std::vector<std::string> Rows;
    std::optional<int64_t> StatisticsRows, StatisticsFiles, StatisticsBytes, ExplainRows;
    std::vector<std::pair<int64_t, int64_t>> Files;
    std::vector<std::string> FileIDs;
    int64_t FooterBytes = 0;
    bool operator==(const TView&) const = default;
};

void PrintTo(const TView& view, std::ostream* out)
{
    *out << std::format("rows={} stats={}/{}/{} explain={} files={} ids={} footer={}",
        view.Rows.size(),
        view.StatisticsRows.value_or(-1),
        view.StatisticsFiles.value_or(-1),
        view.StatisticsBytes.value_or(-1),
        view.ExplainRows.value_or(-1),
        view.Files.size(),
        view.FileIDs.size(),
        view.FooterBytes);
    for (const auto& [rows, bytes] : view.Files) {
        *out << std::format(" ({},{})", rows, bytes);
    }
}

TView Capture(const Engine& engine, const std::string& collection, int64_t snapshot, bool sortRows = true)
{
    TView view;
    ScanRequest request{collection};
    request.selector = SnapshotSelector::Id(snapshot);
    auto cursor = engine.Scan(request);
    EXPECT_TRUE(cursor) << snapshot << ": " << cursor.error().code << ": " << cursor.error().message;
    if (!cursor) {
        return view;
    }
    while (true) {
        auto next = (*cursor)->Next();
        EXPECT_TRUE(next.has_value());
        if (!next || !*next) {
            break;
        }
        auto rows = Render(**next);
        view.Rows.insert(view.Rows.end(), rows.begin(), rows.end());
    }
    if (sortRows) {
        view.Rows = Sorted(std::move(view.Rows));
    }
    const auto statistics = engine.GetStatistics(collection, request.selector);
    EXPECT_TRUE(statistics) << statistics.error().code << ": " << statistics.error().message;
    if (statistics) {
        view.StatisticsRows = statistics->rows.value;
        view.StatisticsFiles = statistics->data_files.value;
        view.StatisticsBytes = statistics->compressed_data_bytes.value;
    }
    const auto files = engine.ListFiles(collection, request.selector);
    EXPECT_TRUE(files) << files.error().code << ": " << files.error().message;
    if (files) {
        for (const auto& file : files->Files) {
            view.Files.emplace_back(file.RecordCount, file.CompressedBytes);
            view.FileIDs.push_back(file.FileID);
        }
        std::ranges::sort(view.Files);
        std::ranges::sort(view.FileIDs);
        if (!files->Files.empty()) {
            const auto details = engine.GetFileDetails(collection, files->Files.back().FileID, request.selector);
            EXPECT_TRUE(details) << details.error().code << ": " << details.error().message;
            view.FooterBytes = details ? details->FooterBytes : -1;
        }
    }
    const auto explained = engine.ExplainScan(request);
    EXPECT_TRUE(explained) << explained.error().code << ": " << explained.error().message;
    if (explained) {
        view.ExplainRows = explained->candidates.rows.value;
    }
    return view;
}

detail::BackendFactory ArchiveBackend(detail::TLocalStorageOptions options = {})
{
    return [options = std::move(options)](const std::filesystem::path& root,
               const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        const auto backend = detail::TLocalStorageBackend::Open(root, info, options);
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return std::shared_ptr<detail::StorageBackend>(*backend);
    };
}

int64_t CatalogVersion(const std::filesystem::path& root)
{
    detail::Database catalog;
    catalog.Open(root / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
    return catalog.Integer("PRAGMA user_version");
}

int64_t ArchiveRows(const std::filesystem::path& root, std::string_view collection)
{
    detail::Database catalog;
    catalog.Open(root / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
    if (catalog.Integer("PRAGMA user_version") < 5) {
        return 0;
    }
    return detail::ParseInteger(detail::Required(
        catalog.Query("SELECT COUNT(*) FROM archived_snapshots WHERE collection_id=?", {std::string(collection)}).at(0),
        0));
}

std::filesystem::path CurrentRoot(const std::filesystem::path& root, std::string_view collection)
{
    const auto roots = FilesIn(root / "warehouse" / std::string(collection) / "metadata", [](const auto& path) {
        return path.filename().string().ends_with(".metadata.json");
    });
    return roots.empty() ? std::filesystem::path{} : roots.back();
}

class TSnapshotArchiveTest : public ::testing::Test
{
protected:
    test::TempDir Directory_;
    std::filesystem::path Root_ = Directory_.path / "store";
    test::JournalFaultVfs Vfs_;
    std::shared_ptr<Engine> Engine_;
    std::map<int64_t, AppendResult> Receipts_;
    int64_t Next_ = 0;

    void Create(std::optional<int64_t> hot, Limits limits = {})
    {
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string(), "local-delete-extra-v1", limits},
                ArchiveBackend(),
                Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        CreateCollectionRequest request{test::collection_id, "events", Schema};
        request.hot_snapshots = hot;
        const auto outcome = Engine_->CreateCollection(request);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed) << test::Present(outcome.error).message;
    }
    void TearDown() override
    {
        Vfs_.Disarm();
        if (Engine_) {
            [[maybe_unused]] auto stopped = Engine_->Shutdown(std::chrono::seconds(5));
        }
    }
    void Reopen(Limits limits = {})
    {
        if (Engine_) {
            ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
            Engine_.reset();
        }
        auto opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id, "local-delete-extra-v1", limits},
            ArchiveBackend(),
            Vfs_.Name());
        ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
        Engine_ = std::move(*opened);
    }
    AppendOutcome Append(int64_t index, SnapshotPrecondition precondition = {})
    {
        auto outcome = Engine_->AppendDurable({test::collection_id, Op(index), precondition, {Batch(RowsFor(index))}});
        if (outcome.result) {
            Receipts_[index] = *outcome.result;
        }
        return outcome;
    }
    AppendOutcome Bulk(int64_t index)
    {
        auto outcome = Engine_->BulkAppendDurable({.collection_id = test::collection_id,
            .operation_id = Op(index),
            .rows = 5,
            .source = std::make_shared<TBatchesSource>(
                std::vector{Batch(RowsFor(index, 2)), Batch(RowsFor(index + 100000, 3))})});
        if (outcome.result) {
            Receipts_[index] = *outcome.result;
        }
        return outcome;
    }
    SnapshotArchiveStatus Status() const
    {
        const auto status = Engine_->GetSnapshotArchive(test::collection_id);
        EXPECT_TRUE(status) << status.error().message;
        return status.value_or(SnapshotArchiveStatus{});
    }
};

TEST_F(TSnapshotArchiveTest, ArchivedSnapshotsAnswerExactlyAsWhileHot)
{
    Create(2);
    EXPECT_EQ(CatalogVersion(Root_), 5);
    std::map<int64_t, TView> hot;
    std::vector<int64_t> order;
    for (int64_t index = 0; index < 14; ++index) {
        const auto outcome = index % 5 == 3 ? Bulk(index) : Append(index);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed) << test::Present(outcome.error).message;
        const auto id = test::Present(outcome.result).snapshot_id;
        order.push_back(id);
        // Unsorted rows: an archived snapshot must also keep the scan order.
        hot[id] = Capture(*Engine_, test::collection_id, id, false);
        const auto status = Status();
        EXPECT_LE(status.root_snapshots, 4);
        EXPECT_EQ(status.root_snapshots + status.archived_snapshots, index + 1);
        EXPECT_EQ(ArchiveRows(Root_, test::collection_id), status.archived_snapshots);
    }
    const auto status = Status();
    EXPECT_EQ(status.hot_snapshots, 2);
    EXPECT_GE(status.archived_snapshots, 10);
    for (const auto& [id, view] : hot) {
        EXPECT_EQ(Capture(*Engine_, test::collection_id, id, false), view) << id;
    }
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    ASSERT_EQ(history->snapshots.size(), order.size());
    for (size_t index = 0; index < order.size(); ++index) {
        EXPECT_EQ(history->snapshots[index].snapshot_id, order[index]);
        EXPECT_EQ(history->snapshots[index].receipt, Receipts_.at(static_cast<int64_t>(index)));
    }
    std::vector<int64_t> paged;
    TMetadataPageRequest page{.PageSize = 4};
    while (true) {
        const auto listing = Engine_->ListSnapshotsPage(test::collection_id, page);
        ASSERT_TRUE(listing) << listing.error().message;
        for (const auto& snapshot : listing->snapshots) {
            paged.push_back(snapshot.snapshot_id);
        }
        if (listing->NextPageToken.empty()) {
            break;
        }
        page.PageToken = listing->NextPageToken;
    }
    EXPECT_EQ(paged, order);
    // A precondition on an archived snapshot fails like one on a hot non-current snapshot.
    const auto stale = Append(100, {SnapshotPreconditionKind::Equals, order.front()});
    ASSERT_TRUE(stale.error);
    EXPECT_EQ(test::Present(stale.error).code, "PRECONDITION_FAILED");
    ScanRequest unknown{test::collection_id};
    unknown.selector = SnapshotSelector::Id(-42);
    EXPECT_EQ(Engine_->Scan(unknown).error().code, "NOT_FOUND");
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id, unknown.selector).error().code, "NOT_FOUND");
    EXPECT_EQ(test::Present(Append(0).result), Receipts_.at(0));
    Reopen();
    EXPECT_EQ(Status().archived_snapshots, status.archived_snapshots);
    for (const auto& [id, view] : hot) {
        auto expected = view;
        expected.Rows = Sorted(std::move(expected.Rows));
        EXPECT_EQ(Capture(*Engine_, test::collection_id, id), expected) << id;
    }
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), order.size());
    for (const auto& [index, receipt] : Receipts_) {
        EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, Op(index)).value()).result, receipt);
    }
}

TEST_F(TSnapshotArchiveTest, StoresWithoutTheSettingStayUnchanged)
{
    Create(std::nullopt);
    for (int64_t index = 0; index < 6; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    EXPECT_EQ(CatalogVersion(Root_), 1);
    const auto root = ReadBytes(CurrentRoot(Root_, test::collection_id));
    EXPECT_EQ(root.find("glamdb.retention"), std::string::npos);
    EXPECT_EQ(Status().root_snapshots, 6);
    EXPECT_EQ(Status().archived_snapshots, 0);
    const auto archived = Engine_->ArchiveSnapshots(test::collection_id);
    ASSERT_TRUE(archived) << archived.error().message;
    EXPECT_EQ(archived->snapshots_archived, 0);
    // Unsetting a setting that was never stored changes nothing either.
    ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, std::nullopt}));
    EXPECT_EQ(CatalogVersion(Root_), 1);
    Reopen();
    EXPECT_EQ(CatalogVersion(Root_), 1);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 6U);
}

TEST_F(TSnapshotArchiveTest, SettingIsValidatedPersistedAndPartOfCreation)
{
    Create(3);
    CreateCollectionRequest retry{test::collection_id, "events", Schema};
    retry.hot_snapshots = 3;
    EXPECT_EQ(Engine_->CreateCollection(retry).disposition, CommitDisposition::Committed);
    retry.hot_snapshots = 4;
    const auto mismatch = Engine_->CreateCollection(retry);
    EXPECT_EQ(mismatch.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(mismatch.error).code, "SNAPSHOT_ARCHIVE_MISMATCH");
    for (const auto value : {int64_t{0}, int64_t{-1}, (int64_t{1} << 40) + 1}) {
        const auto invalid = Engine_->UpdateSnapshotArchive({test::collection_id, value});
        ASSERT_FALSE(invalid);
        EXPECT_EQ(invalid.error().code, "INVALID_ARGUMENT");
        CreateCollectionRequest bad{SecondCollection, "bad", Schema};
        bad.hot_snapshots = value;
        EXPECT_EQ(test::Present(Engine_->CreateCollection(bad).error).code, "INVALID_ARGUMENT");
    }
    ASSERT_TRUE(Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{5}}));
    const auto tooMany = Engine_->UpdateSnapshotArchive({test::collection_id, 6});
    ASSERT_FALSE(tooMany);
    EXPECT_EQ(tooMany.error().code, "INVALID_ARGUMENT");
    const auto keepTooFew = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{2}});
    ASSERT_FALSE(keepTooFew);
    EXPECT_EQ(keepTooFew.error().code, "INVALID_ARGUMENT");
    const auto updated = Engine_->UpdateSnapshotArchive({test::collection_id, 5});
    ASSERT_TRUE(updated) << updated.error().message;
    EXPECT_EQ(updated->hot_snapshots, 5);
    // A repeated request is applied once.
    EXPECT_EQ(Engine_->UpdateSnapshotArchive({test::collection_id, 5})->hot_snapshots, 5);
    Reopen();
    EXPECT_EQ(Engine_->GetCollection(test::collection_id)->hot_snapshots, 5);
    // The creation value still decides a repeated creation.
    retry.hot_snapshots = 3;
    EXPECT_EQ(Engine_->CreateCollection(retry).disposition, CommitDisposition::Committed);
    CreateCollectionRequest kv{SecondCollection, "kv", Schema};
    kv.mode = CollectionMode::KeyValue;
    kv.hot_snapshots = 2;
    EXPECT_EQ(test::Present(Engine_->CreateCollection(kv).error).code, "COLLECTION_MODE_MISMATCH");
    kv.hot_snapshots.reset();
    ASSERT_EQ(Engine_->CreateCollection(kv).disposition, CommitDisposition::Committed);
    EXPECT_EQ(Engine_->UpdateSnapshotArchive({SecondCollection, 2}).error().code, "COLLECTION_MODE_MISMATCH");
    EXPECT_EQ(Engine_->ArchiveSnapshots(SecondCollection).error().code, "COLLECTION_MODE_MISMATCH");
    EXPECT_EQ(Engine_->GetSnapshotArchive(SecondCollection)->archived_snapshots, 0);
}

TEST_F(TSnapshotArchiveTest, MaintenanceArchivesAnExistingHistoryInBoundedCalls)
{
    Create(std::nullopt);
    for (int64_t index = 0; index < 30; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    const auto before = std::filesystem::file_size(CurrentRoot(Root_, test::collection_id));
    std::map<int64_t, TView> views;
    for (const auto& [index, receipt] : Receipts_) {
        views[receipt.snapshot_id] = Capture(*Engine_, test::collection_id, receipt.snapshot_id);
    }
    ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, 4}));
    // The setting itself moves nothing.
    EXPECT_EQ(Status().root_snapshots, 30);
    EXPECT_EQ(Status().archive_pending, 26);
    const auto archived = Engine_->ArchiveSnapshots(test::collection_id);
    ASSERT_TRUE(archived) << archived.error().message;
    EXPECT_EQ(archived->snapshots_archived, 26);
    EXPECT_EQ(archived->root_snapshots, 4);
    EXPECT_EQ(archived->archived_snapshots, 26);
    EXPECT_EQ(archived->archive_pending, 0);
    EXPECT_LT(std::filesystem::file_size(CurrentRoot(Root_, test::collection_id)), before / 4);
    EXPECT_EQ(Engine_->ArchiveSnapshots(test::collection_id)->snapshots_archived, 0);
    for (const auto& [id, view] : views) {
        EXPECT_EQ(Capture(*Engine_, test::collection_id, id), view) << id;
    }
    // Raising the setting does not move archived snapshots back.
    ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, 10}));
    ASSERT_EQ(Append(30).disposition, CommitDisposition::Committed);
    EXPECT_EQ(Status().root_snapshots, 5);
    EXPECT_EQ(Status().archived_snapshots, 26);
    Reopen();
    for (const auto& [id, view] : views) {
        EXPECT_EQ(Capture(*Engine_, test::collection_id, id), view) << id;
    }
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 31U);
}

TEST_F(TSnapshotArchiveTest, AppendsShrinkALongBacklogByABoundedCut)
{
    Create(std::nullopt);
    for (int64_t index = 0; index < 80; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, 4}));
    ASSERT_EQ(Append(80).disposition, CommitDisposition::Committed);
    // 81 root snapshots after the append, at most 64 move per append.
    EXPECT_EQ(Status().root_snapshots, 17);
    EXPECT_EQ(Status().archived_snapshots, 64);
    ASSERT_EQ(Append(81).disposition, CommitDisposition::Committed);
    EXPECT_EQ(Status().root_snapshots, 4);
    EXPECT_EQ(Status().archived_snapshots, 78);
    Reopen();
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 82U);
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id)->rows.value, 164);
}

std::uintmax_t ManifestListBytes(const std::filesystem::path& root)
{
    std::uintmax_t size = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        if (entry.is_regular_file() && entry.path().filename().string().starts_with("snap-")) {
            size += entry.file_size();
        }
    }
    return size;
}

TEST_F(TSnapshotArchiveTest, RetentionExpiresArchivedSnapshotsFirstAndKeepsTiersContiguous)
{
    Create(2);
    for (int64_t index = 0; index < 12; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    const auto before = Status();
    ASSERT_GE(before.archived_snapshots, 8);
    std::map<int64_t, TView> views;
    for (const auto& [index, receipt] : Receipts_) {
        views[receipt.snapshot_id] = Capture(*Engine_, test::collection_id, receipt.snapshot_id);
    }
    // A cursor planned on the oldest snapshot keeps reading after it expires.
    ScanRequest oldest{test::collection_id};
    oldest.selector = SnapshotSelector::Id(Receipts_.at(0).snapshot_id);
    auto cursor = Engine_->Scan(oldest);
    ASSERT_TRUE(cursor);
    const auto listBytes = ManifestListBytes(Root_);
    const auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{9}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 3);
    EXPECT_EQ(expired->cleanup_pending, 0);
    EXPECT_LT(ManifestListBytes(Root_), listBytes);
    const auto after = Status();
    EXPECT_EQ(after.expired_snapshots, 3);
    EXPECT_EQ(after.archived_snapshots, before.archived_snapshots - 3);
    EXPECT_EQ(after.root_snapshots, before.root_snapshots);
    EXPECT_EQ(ArchiveRows(Root_, test::collection_id), after.archived_snapshots);
    std::vector<std::string> rows;
    while (auto next = (*cursor)->Next()) {
        if (!*next) {
            break;
        }
        auto piece = Render(**next);
        rows.insert(rows.end(), piece.begin(), piece.end());
    }
    EXPECT_EQ(Sorted(rows), views.at(Receipts_.at(0).snapshot_id).Rows);
    cursor->reset();
    for (int64_t index = 0; index < 12; ++index) {
        const auto id = Receipts_.at(index).snapshot_id;
        if (index < 3) {
            EXPECT_EQ(Engine_->GetStatistics(test::collection_id, SnapshotSelector::Id(id)).error().code,
                "SNAPSHOT_EXPIRED");
            const auto precondition = Append(200 + index, {SnapshotPreconditionKind::Equals, id});
            EXPECT_EQ(test::Present(precondition.error).code, "SNAPSHOT_EXPIRED");
        } else {
            EXPECT_EQ(Capture(*Engine_, test::collection_id, id), views.at(id)) << index;
        }
    }
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 9U);
    // Later appends keep archiving; retention keeps applying across both tiers.
    for (int64_t index = 12; index < 16; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    ASSERT_TRUE(Engine_->ExpireSnapshots({test::collection_id}));
    EXPECT_EQ(Status().expired_snapshots, 7);
    Reopen();
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 9U);
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id)->rows.value, 32);
    for (int64_t index = 7; index < 12; ++index) {
        const auto id = Receipts_.at(index).snapshot_id;
        EXPECT_EQ(Capture(*Engine_, test::collection_id, id), views.at(id)) << index;
    }
    for (const auto& [index, receipt] : Receipts_) {
        EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, Op(index)).value()).result, receipt);
    }
}

TEST_F(TSnapshotArchiveTest, ExpirationCanReachRootSnapshotsOnlyAfterTheWholeArchive)
{
    Create(2);
    for (int64_t index = 0; index < 8; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, std::nullopt}));
    for (int64_t index = 8; index < 12; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    const auto before = Status();
    ASSERT_GT(before.archived_snapshots, 0);
    const auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{2}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 10);
    EXPECT_EQ(Status().archived_snapshots, 0);
    EXPECT_EQ(Status().root_snapshots, 2);
    EXPECT_EQ(ArchiveRows(Root_, test::collection_id), 0);
    Reopen();
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id)->rows.value, 24);
}

TEST_F(TSnapshotArchiveTest, PlannedCursorKeepsItsViewWhileItsSnapshotIsArchived)
{
    Create(1);
    ASSERT_EQ(Append(0).disposition, CommitDisposition::Committed);
    ScanRequest request{test::collection_id};
    request.selector = SnapshotSelector::Id(Receipts_.at(0).snapshot_id);
    auto cursor = Engine_->Scan(request);
    ASSERT_TRUE(cursor);
    for (int64_t index = 1; index < 6; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    EXPECT_GE(Status().archived_snapshots, 3);
    std::vector<std::string> rows;
    while (auto next = (*cursor)->Next()) {
        if (!*next) {
            break;
        }
        auto piece = Render(**next);
        rows.insert(rows.end(), piece.begin(), piece.end());
    }
    const auto first = RowsFor(0);
    EXPECT_EQ(Sorted(rows), Expected({&first}));
}

class TArchiveRowTamperingTest
    : public TSnapshotArchiveTest
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TArchiveRowTamperingTest, OpenFailsClosed)
{
    Create(2);
    for (int64_t index = 0; index < 8; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    ASSERT_GE(Status().archived_snapshots, 4);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    {
        detail::Database catalog;
        catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
        catalog.Execute(GetParam());
        ASSERT_EQ(catalog.Changes(), 1);
    }
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION") << opened.error().message;
}

INSTANTIATE_TEST_SUITE_P(SnapshotArchive,
    TArchiveRowTamperingTest,
    ::testing::Values("UPDATE archived_snapshots SET data_files=data_files+1 WHERE sequence_number=2",
        "UPDATE archived_snapshots SET parent_snapshot_id=NULL WHERE sequence_number=2",
        "UPDATE archived_snapshots SET timestamp_ms=timestamp_ms+1 WHERE sequence_number=1",
        "UPDATE archived_snapshots SET snapshot_blob=X'0100' WHERE sequence_number=1",
        "UPDATE archived_snapshots SET snapshot_id=snapshot_id+1 WHERE sequence_number=3",
        "DELETE FROM archived_snapshots WHERE sequence_number=2",
        "DELETE FROM archived_snapshots WHERE sequence_number=(SELECT MAX(sequence_number) FROM archived_snapshots)",
        "UPDATE archived_snapshots SET manifest_list='store://00000000-0000-0000-0000-000000000001/warehouse/"
        "00112233-4455-6677-8899-aabbccddeeff/data/x.avro' WHERE sequence_number=1"));

TEST_F(TSnapshotArchiveTest, AlteredOrMissingArchivedListIsAnExplicitErrorNeverWrongData)
{
    Create(2);
    for (int64_t index = 0; index < 8; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    const auto first = Receipts_.at(0).snapshot_id;
    const auto second = Receipts_.at(1).snapshot_id;
    const auto expected = Capture(*Engine_, test::collection_id, first);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    std::string firstList, secondList;
    {
        detail::Database catalog;
        catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
        firstList = detail::Required(
            catalog.Query("SELECT manifest_list FROM archived_snapshots WHERE snapshot_id=?", {first}).at(0),
            0);
        secondList = detail::Required(
            catalog.Query("SELECT manifest_list FROM archived_snapshots WHERE snapshot_id=?", {second}).at(0),
            0);
    }
    const auto local = [&](const std::string& uri) {
        return Root_ / "warehouse" / uri.substr(uri.find("/warehouse/") + 11);
    };
    auto bytes = ReadBytes(local(firstList));
    bytes[bytes.size() / 2] = static_cast<char>(bytes[bytes.size() / 2] ^ 1);
    WriteBytes(local(firstList), bytes);
    std::filesystem::remove(local(secondList));
    // Open verifies the archive against receipts and the root, not every list.
    Reopen();
    ScanRequest request{test::collection_id};
    request.selector = SnapshotSelector::Id(first);
    auto cursor = Engine_->Scan(request);
    ASSERT_TRUE(cursor) << cursor.error().message;
    std::vector<std::string> rows;
    while (auto next = (*cursor)->Next()) {
        if (!*next) {
            break;
        }
        auto piece = Render(**next);
        rows.insert(rows.end(), piece.begin(), piece.end());
    }
    EXPECT_EQ(Sorted(rows), expected.Rows);
    cursor->reset();
    const auto altered = Engine_->GetStatistics(test::collection_id, request.selector);
    ASSERT_FALSE(altered);
    EXPECT_EQ(altered.error().code, "CORRUPTION");
    Reopen();
    const auto missing = Engine_->ListFiles(test::collection_id, SnapshotSelector::Id(second));
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, "CORRUPTION");
    Reopen();
    // Expiration needs the lists to find last references and refuses too.
    const auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{3}});
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, "CORRUPTION");
    Reopen();
    EXPECT_EQ(Status().expired_snapshots, 0);
}

TEST_F(TSnapshotArchiveTest, ReopenRejectsLowerLimitsThatArchivedMetadataStillNeeds)
{
    Limits limits;
    limits.history.max_snapshots = 8;
    Create(2, limits);
    for (int64_t index = 0; index < 20; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed) << index;
    }
    // history.max_snapshots bounds the root, not the archive.
    EXPECT_LE(Status().root_snapshots, 4);
    Reopen(limits);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    const auto listBytes = static_cast<int64_t>(ManifestListBytes(Root_));
    Limits lower = limits;
    lower.history.max_manifest_list_bytes = listBytes - 1;
    const auto refused = OpenExisting({Root_.string(), test::store_id, "local-delete-extra-v1", lower});
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "CORRUPTION") << refused.error().message;
    lower.history.max_manifest_list_bytes = listBytes;
    const auto accepted = OpenExisting({Root_.string(), test::store_id, "local-delete-extra-v1", lower});
    EXPECT_TRUE(accepted) << accepted.error().message;
}

// The rows of the snapshot written by append `last` when appends 0..last all committed.
std::vector<std::string> CumulativeRows(int64_t last)
{
    std::vector<std::string> rows;
    for (int64_t index = 0; index <= last; ++index) {
        const auto rendered = Render(*Batch(RowsFor(index)));
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
    return Sorted(std::move(rows));
}

// Every snapshot's answers, independent of its tier and of the reader process.
std::string HistoryDigest(const Engine& engine, const std::string& collection)
{
    std::string text;
    const auto history = engine.ListSnapshots(collection);
    EXPECT_TRUE(history) << history.error().message;
    if (!history) {
        return {};
    }
    for (const auto& snapshot : history->snapshots) {
        const auto view = Capture(engine, collection, snapshot.snapshot_id);
        text += std::format("{}:{}:{}:{}|",
            snapshot.snapshot_id,
            snapshot.receipt.operation_id,
            view.StatisticsRows.value_or(-1),
            view.StatisticsBytes.value_or(-1));
        for (const auto& row : view.Rows) {
            text += row + ";";
        }
        for (const auto& id : view.FileIDs) {
            text += id + ",";
        }
    }
    return detail::Sha256(text);
}

enum class EChildExit : uint8_t { Success = 0, Interrupted = 77 };

void RunArchiveChild(const std::filesystem::path& root,
    std::string_view mode,
    int64_t operation,
    EChildExit expected = EChildExit::Success)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        if (!std::string_view(*entry).starts_with("GLAMDB_ARCHIVE_")) {
            environment.emplace_back(*entry);
        }
    }
    environment.push_back("GLAMDB_ARCHIVE_ROOT=" + root.string());
    environment.push_back("GLAMDB_ARCHIVE_MODE=" + std::string(mode));
    environment.push_back("GLAMDB_ARCHIVE_OPERATION=" + std::to_string(operation));
    std::vector<char*> variables;
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=SnapshotArchiveChild.Run";
    char* arguments[]{executable.data(), filter.data(), nullptr};
    pid_t child = 0;
    ASSERT_EQ(::posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments, variables.data()), 0);
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), static_cast<int>(expected)) << mode;
#else
    (void)root;
    (void)mode;
    (void)operation;
    (void)expected;
    GTEST_SKIP() << "Fresh-process evidence requires Linux.";
#endif
}

TEST(SnapshotArchiveChild, Run)
{
    const auto* root = std::getenv("GLAMDB_ARCHIVE_ROOT");
    const auto* mode = std::getenv("GLAMDB_ARCHIVE_MODE");
    const auto* operation = std::getenv("GLAMDB_ARCHIVE_OPERATION");
    if (!root || !mode || !operation) {
        GTEST_SKIP() << "Invoked by snapshot archive tests in a fresh process.";
    }
    const auto start = std::chrono::steady_clock::now();
    test::JournalFaultVfs vfs;
    auto opened = detail::EngineFactory::OpenWith({root, test::store_id}, ArchiveBackend(), vfs.Name());
    ASSERT_TRUE(opened) << opened.error().code << ": " << opened.error().message;
    auto engine = std::move(*opened);
    const auto openMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    const std::string_view selected(mode);
    const auto index = std::stoll(operation);
    if (selected.starts_with("crash-append-")) {
        vfs.Arm(2, selected == "crash-append-after");
        const auto outcome = engine->AppendDurable({test::collection_id, Op(index), {}, {Batch(RowsFor(index))}});
        ::_exit(outcome.disposition == CommitDisposition::CommitUnknown ? 77 : 78);
    }
    if (selected.starts_with("crash-archive-")) {
        vfs.Arm(1, selected == "crash-archive-after");
        const auto result = engine->ArchiveSnapshots(test::collection_id);
        ::_exit(!result && result.error().code == "RECOVERY_REQUIRED" ? 77 : 78);
    }
    WriteBytes(std::filesystem::path(root).parent_path() / "digest-child", HistoryDigest(*engine, test::collection_id));
    std::cout << "archive fresh_process_open_ms=" << openMs << '\n';
}

class TArchiveCrashTest
    : public TSnapshotArchiveTest
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TArchiveCrashTest, ProcessExitAroundTheArchivingCommitReopensConsistently)
{
    const std::string mode = GetParam();
    const bool append = mode.starts_with("crash-append-");
    Create(append ? std::optional<int64_t>(2) : std::nullopt);
    for (int64_t index = 0; index < (append ? 4 : 6); ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    if (!append) {
        ASSERT_TRUE(Engine_->UpdateSnapshotArchive({test::collection_id, 2}));
    }
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    RunArchiveChild(Root_, mode, 50, EChildExit::Interrupted);
    RunArchiveChild(Root_, "verify", 0);
    Reopen();
    const auto status = Status();
    EXPECT_EQ(ArchiveRows(Root_, test::collection_id), status.archived_snapshots);
    const bool after = mode.ends_with("-after");
    if (append) {
        const auto outcome = test::Present(Engine_->FindAppendOutcome(test::collection_id, Op(50)).value());
        EXPECT_EQ(outcome.disposition,
            after ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
        EXPECT_EQ(status.archived_snapshots, after ? 3 : 0);
        EXPECT_EQ(status.root_snapshots, after ? 2 : 4);
    } else {
        EXPECT_EQ(status.archived_snapshots, after ? 4 : 0);
        EXPECT_EQ(status.root_snapshots, after ? 2 : 6);
    }
    EXPECT_EQ(ReadBytes(Directory_.path / "digest-child"), HistoryDigest(*Engine_, test::collection_id));
    // The interrupted work can be completed afterwards.
    if (!append) {
        ASSERT_TRUE(Engine_->ArchiveSnapshots(test::collection_id));
        EXPECT_EQ(Status().root_snapshots, 2);
    }
    ASSERT_EQ(Append(60).disposition, CommitDisposition::Committed);
    const auto digest = HistoryDigest(*Engine_, test::collection_id);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
    Engine_.reset();
    RunArchiveChild(Root_, "verify", 0);
    EXPECT_EQ(ReadBytes(Directory_.path / "digest-child"), digest);
}

INSTANTIATE_TEST_SUITE_P(SnapshotArchive,
    TArchiveCrashTest,
    ::testing::Values("crash-append-before", "crash-append-after", "crash-archive-before", "crash-archive-after"));

class TArchiveLostCommitTest
    : public TSnapshotArchiveTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TArchiveLostCommitTest, UncertainArchivingCommitRequiresReconcileAndPreservesReceipts)
{
    const bool afterUnlink = GetParam();
    Create(2);
    for (int64_t index = 0; index < 4; ++index) {
        ASSERT_EQ(Append(index).disposition, CommitDisposition::Committed);
    }
    Vfs_.Arm(2, afterUnlink);
    const auto lost = Engine_->AppendDurable({test::collection_id, Op(4), {}, {Batch(RowsFor(4))}});
    EXPECT_EQ(lost.disposition, CommitDisposition::CommitUnknown);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    Vfs_.Disarm();
    const auto reconciled = Engine_->Reconcile();
    ASSERT_TRUE(reconciled) << reconciled.error().message;
    const auto retry = Engine_->AppendDurable({test::collection_id, Op(4), {}, {Batch(RowsFor(4))}});
    EXPECT_EQ(retry.disposition,
        afterUnlink ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(Status().archived_snapshots, afterUnlink ? 3 : 0);
    EXPECT_EQ(ArchiveRows(Root_, test::collection_id), Status().archived_snapshots);
    for (int64_t index = 0; index < 4; ++index) {
        const auto id = Receipts_.at(index).snapshot_id;
        EXPECT_EQ(Capture(*Engine_, test::collection_id, id).Rows, CumulativeRows(index)) << index;
    }
    Reopen();
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, Op(4)).value()).disposition,
        retry.disposition);
}

INSTANTIATE_TEST_SUITE_P(SnapshotArchive, TArchiveLostCommitTest, ::testing::Bool());

// Two stores receive the same randomized history: `Archived` with archive
// settings (changed mid-way), `Reference` keeping every snapshot in its root.
// Store-specific snapshot IDs and times are compared through operation IDs.
class TArchiveDifferentialTest : public ::testing::Test
{
protected:
    struct TStore
    {
        test::TempDir Directory;
        std::shared_ptr<Engine> Engine;
        std::map<std::string, int64_t> SnapshotOf;
        std::map<int64_t, std::string> OperationOf;
    };
    TStore Archived_, Reference_;
    // Operation index -> (collection, bulk).
    std::map<int64_t, std::pair<std::string, bool>> Operations_;
    std::map<std::string, TView> HotViews_;
    const std::array<std::string, 2> Collections_{test::collection_id, SecondCollection};

    static std::filesystem::path RootOf(const TStore& store)
    {
        return store.Directory.path / "store";
    }
    void SetUp() override
    {
        Limits limits;
        limits.max_operations = 2048;
        for (auto* store : {&Archived_, &Reference_}) {
            auto created = CreateStore({test::store_id, RootOf(*store).string(), "local-delete-extra-v1", limits});
            ASSERT_TRUE(created) << created.error().message;
            store->Engine = std::move(*created);
            for (const auto& collection : Collections_) {
                CreateCollectionRequest request{collection, collection, Schema};
                if (store == &Archived_ && collection == Collections_[0]) {
                    request.hot_snapshots = 3;
                }
                ASSERT_EQ(store->Engine->CreateCollection(request).disposition, CommitDisposition::Committed);
            }
        }
    }
    void TearDown() override
    {
        for (auto* store : {&Archived_, &Reference_}) {
            if (store->Engine) {
                [[maybe_unused]] auto stopped = store->Engine->Shutdown(std::chrono::seconds(5));
            }
        }
    }
    AppendOutcome Run(TStore& store, int64_t index, SnapshotPrecondition precondition)
    {
        const auto& [collection, bulk] = Operations_.at(index);
        auto outcome = bulk ? store.Engine->BulkAppendDurable({.collection_id = collection,
                                  .operation_id = Op(index),
                                  .expected_snapshot = precondition,
                                  .rows = 5,
                                  .source = std::make_shared<TBatchesSource>(
                                      std::vector{Batch(RowsFor(index, 2)), Batch(RowsFor(index + 100000, 3))})})
                            : store.Engine->AppendDurable(
                                  {collection, Op(index), precondition, {Batch(RowsFor(index, 1 + index % 3))}});
        if (outcome.result) {
            store.SnapshotOf[Op(index)] = outcome.result->snapshot_id;
            store.OperationOf[outcome.result->snapshot_id] = Op(index);
        }
        return outcome;
    }
    // Store-independent form of an outcome.
    static std::string Normalize(const TStore& store, const AppendOutcome& outcome)
    {
        auto text = std::format("{}:{}:{}",
            outcome.operation_id,
            static_cast<int>(outcome.disposition),
            outcome.error ? outcome.error->code : std::string("-"));
        if (outcome.result) {
            const auto& receipt = *outcome.result;
            const auto parent = receipt.parent_snapshot_id ? store.OperationOf.at(*receipt.parent_snapshot_id) : "-";
            text += std::format(":{}:{}:{}:{}:{}:{}:{}",
                receipt.collection_id,
                receipt.payload_hash,
                receipt.rows_appended,
                receipt.data_files_added,
                receipt.compressed_data_bytes_added,
                receipt.receipt_version,
                parent);
        }
        return text;
    }
    // File IDs bind the table UUID; Parquet footers may differ by encoded offsets.
    static TView Portable(TView view)
    {
        view.FileIDs.clear();
        view.FooterBytes = 0;
        return view;
    }
    void CompareEverySnapshot(bool againstHot)
    {
        for (const auto& [index, operation] : Operations_) {
            const auto& collection = operation.first;
            const auto archived = Archived_.SnapshotOf.find(Op(index));
            const auto reference = Reference_.SnapshotOf.find(Op(index));
            ASSERT_EQ(archived == Archived_.SnapshotOf.end(), reference == Reference_.SnapshotOf.end()) << index;
            if (archived == Archived_.SnapshotOf.end()) {
                continue;
            }
            const auto a = Archived_.Engine->GetStatistics(collection, SnapshotSelector::Id(archived->second));
            const auto b = Reference_.Engine->GetStatistics(collection, SnapshotSelector::Id(reference->second));
            ASSERT_EQ(a.has_value(), b.has_value()) << index;
            if (!a) {
                EXPECT_EQ(a.error().code, b.error().code) << index;
                continue;
            }
            const auto view = Capture(*Archived_.Engine, collection, archived->second);
            EXPECT_EQ(Portable(view), Portable(Capture(*Reference_.Engine, collection, reference->second))) << index;
            if (againstHot) {
                EXPECT_EQ(view, HotViews_.at(Op(index))) << index;
            }
        }
        for (const auto& collection : Collections_) {
            std::array<std::vector<std::string>, 2> listings;
            for (size_t which = 0; which < 2; ++which) {
                const auto& store = which == 0 ? Archived_ : Reference_;
                const auto history = store.Engine->ListSnapshots(collection);
                ASSERT_TRUE(history) << history.error().message;
                for (const auto& snapshot : history->snapshots) {
                    listings[which].push_back(Normalize(store,
                        {snapshot.receipt.operation_id,
                            CommitDisposition::Committed,
                            CommitPhase::Complete,
                            snapshot.receipt,
                            std::nullopt,
                            std::nullopt}));
                }
            }
            EXPECT_EQ(listings[0], listings[1]) << collection;
        }
    }
};

TEST_F(TArchiveDifferentialTest, RandomizedHistoryAnswersIdenticallyWithAndWithoutArchive)
{
    std::mt19937 random(174);
    int64_t next = 0;
    for (int64_t step = 0; step < 90; ++step) {
        if (step == 40) {
            ASSERT_TRUE(Archived_.Engine->UpdateSnapshotArchive({Collections_[0], 5}));
            ASSERT_TRUE(Archived_.Engine->UpdateSnapshotArchive({Collections_[1], 2}));
        }
        if (step == 60) {
            for (auto* store : {&Archived_, &Reference_}) {
                const auto expired = store->Engine->ExpireSnapshots({Collections_[0], SnapshotRetentionPolicy{12}});
                ASSERT_TRUE(expired) << expired.error().message;
            }
        }
        if (step == 75) {
            ASSERT_TRUE(Archived_.Engine->ArchiveSnapshots(Collections_[0]));
        }
        const auto choice = random() % 20;
        const auto& collection = Collections_[random() % 2];
        if (choice >= 18 && next > 0) {
            // An exact retry of an earlier operation, committed or failed.
            const auto index = static_cast<int64_t>(random() % static_cast<uint64_t>(next));
            const auto a = Run(Archived_, index, {});
            const auto b = Run(Reference_, index, {});
            EXPECT_EQ(Normalize(Archived_, a), Normalize(Reference_, b)) << index;
            continue;
        }
        const auto index = next++;
        Operations_[index] = {collection, choice >= 14 && choice < 16};
        SnapshotPrecondition aPrecondition, bPrecondition;
        if (choice >= 16) {
            // Usually stale: an earlier snapshot of this collection, possibly archived or expired.
            std::vector<int64_t> earlier;
            for (const auto& [prior, operation] : Operations_) {
                if (operation.first == collection && Archived_.SnapshotOf.contains(Op(prior))) {
                    earlier.push_back(prior);
                }
            }
            if (!earlier.empty()) {
                const auto prior = earlier[random() % earlier.size()];
                aPrecondition = {SnapshotPreconditionKind::Equals, Archived_.SnapshotOf.at(Op(prior))};
                bPrecondition = {SnapshotPreconditionKind::Equals, Reference_.SnapshotOf.at(Op(prior))};
            }
        }
        const auto a = Run(Archived_, index, aPrecondition);
        const auto b = Run(Reference_, index, bPrecondition);
        ASSERT_EQ(Normalize(Archived_, a), Normalize(Reference_, b)) << index;
        if (a.result) {
            HotViews_[Op(index)] = Capture(*Archived_.Engine, collection, a.result->snapshot_id);
        }
    }
    for (const auto& collection : Collections_) {
        const auto status = Archived_.Engine->GetSnapshotArchive(collection);
        ASSERT_TRUE(status);
        EXPECT_GT(status->archived_snapshots, 0) << collection;
        EXPECT_EQ(Reference_.Engine->GetSnapshotArchive(collection)->archived_snapshots, 0);
    }
    CompareEverySnapshot(true);
    for (auto* store : {&Archived_, &Reference_}) {
        ASSERT_TRUE(store->Engine->Shutdown(std::chrono::seconds(5)));
        store->Engine.reset();
        Limits limits;
        limits.max_operations = 2048;
        auto opened = OpenExisting({RootOf(*store).string(), test::store_id, "local-delete-extra-v1", limits});
        ASSERT_TRUE(opened) << opened.error().message;
        store->Engine = std::move(*opened);
    }
    // FileIDs and footer details stay identical; reopened scans compare as multisets.
    CompareEverySnapshot(true);
    for (const auto& [index, operation] : Operations_) {
        const auto a = Archived_.Engine->FindAppendOutcome(operation.first, Op(index));
        const auto b = Reference_.Engine->FindAppendOutcome(operation.first, Op(index));
        ASSERT_TRUE(a && b);
        ASSERT_EQ(a->has_value(), b->has_value());
        if (a->has_value()) {
            EXPECT_EQ(Normalize(Archived_, **a), Normalize(Reference_, **b)) << index;
        }
    }
}

struct TWindowCost
{
    std::uintmax_t RootBytes = 0;
    int64_t RootSnapshots = 0;
    int64_t ArchivedSnapshots = 0;
    uint64_t RowsChanged = 0;
    uint64_t RowsReturned = 0;
    uint64_t Statements = 0;
    uint64_t FullscanSteps = 0;
    uint64_t VmSteps = 0;
    uint64_t RootParses = 0;
    double MeanMs = 0;
};

// Appends until `history` committed operations, then measures `window`
// appends: one complete archive cycle when window == hot + slack + 1.
TWindowCost
MeasureWindow(Engine& engine, const std::filesystem::path& root, int64_t& next, int64_t history, int64_t window)
{
    const auto batch = Batch(RowsFor(0, 3));
    const auto append = [&] {
        const auto outcome = engine.AppendDurable({test::collection_id, Op(next++), {}, {batch}});
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed) << test::Present(outcome.error).message;
    };
    while (next < history) {
        append();
    }
    const auto work = detail::ReadCatalogWork();
    const auto parses = NGlamDB::NStorage::TableMetadataParses();
    const auto start = std::chrono::steady_clock::now();
    for (int64_t index = 0; index < window; ++index) {
        append();
    }
    const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
    const auto after = detail::ReadCatalogWork();
    const auto status = engine.GetSnapshotArchive(test::collection_id);
    TWindowCost cost{.RootBytes = std::filesystem::file_size(CurrentRoot(root, test::collection_id)),
        .RootSnapshots = status ? status->root_snapshots : -1,
        .ArchivedSnapshots = status ? status->archived_snapshots : -1,
        .RowsChanged = after.rows_changed - work.rows_changed,
        .RowsReturned = after.rows_returned - work.rows_returned,
        .Statements = after.statements - work.statements,
        .FullscanSteps = after.fullscan_steps - work.fullscan_steps,
        .VmSteps = after.vm_steps - work.vm_steps,
        .RootParses = NGlamDB::NStorage::TableMetadataParses() - parses,
        .MeanMs = elapsed / static_cast<double>(window)};
    std::cout << std::format("archive_cost history={} window={} root_bytes={} root_snapshots={} archived={} "
                             "rows_changed={} rows_returned={} statements={} fullscan_steps={} vm_steps={} "
                             "root_parses={} mean_append_ms={:.2f}\n",
        history,
        window,
        cost.RootBytes,
        cost.RootSnapshots,
        cost.ArchivedSnapshots,
        cost.RowsChanged,
        cost.RowsReturned,
        cost.Statements,
        cost.FullscanSteps,
        cost.VmSteps,
        cost.RootParses,
        cost.MeanMs);
    return cost;
}

std::vector<int64_t> CostHistories()
{
    // Defaults fit the default CTest timeout; the campaign sets e.g. "1008,5004".
    std::vector<int64_t> histories{180, 540};
    if (const auto* text = std::getenv("GLAMDB_ARCHIVE_COST")) {
        histories.clear();
        std::string_view rest(text);
        while (!rest.empty()) {
            const auto comma = rest.find(',');
            histories.push_back(std::stoll(std::string(rest.substr(0, comma))));
            rest = comma == std::string_view::npos ? std::string_view{} : rest.substr(comma + 1);
        }
    }
    return histories;
}

Limits CostLimits()
{
    Limits limits;
    limits.max_operations = 8192;
    limits.history.max_manifests = 16384;
    return limits;
}

TEST(SnapshotArchiveCost, RootAndCatalogWorkPerAppendDoNotDependOnArchivedHistory)
{
    constexpr int64_t hot = 8;
    // One archive cycle: the root grows from hot to hot + slack, the next append cuts.
    const int64_t window = static_cast<int64_t>(NGlamDB::NStorage::ArchiveSlack(hot)) + 1;
    test::TempDir directory;
    const auto root = directory.path / "store";
    auto created = CreateStore({test::store_id, root.string(), "local-delete-extra-v1", CostLimits()});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    CreateCollectionRequest request{test::collection_id, "events", Schema};
    request.hot_snapshots = hot;
    ASSERT_EQ(engine->CreateCollection(request).disposition, CommitDisposition::Committed);
    int64_t next = 0;
    std::vector<TWindowCost> costs;
    for (const auto history : CostHistories()) {
        ASSERT_EQ(history % window, 0) << "histories must align with the archive cycle";
        costs.push_back(MeasureWindow(*engine, root, next, history, window));
    }
    for (const auto& cost : costs) {
        EXPECT_EQ(cost.RootSnapshots, costs.front().RootSnapshots);
        EXPECT_LE(cost.RootSnapshots, hot + static_cast<int64_t>(NGlamDB::NStorage::ArchiveSlack(hot)));
        // Random snapshot IDs and sequence digits change the root by a few bytes.
        EXPECT_LE(std::max(cost.RootBytes, costs.front().RootBytes) - std::min(cost.RootBytes, costs.front().RootBytes),
            64U);
        EXPECT_EQ(cost.RowsChanged, costs.front().RowsChanged);
        EXPECT_EQ(cost.RowsReturned, costs.front().RowsReturned);
        EXPECT_EQ(cost.Statements, costs.front().Statements);
        EXPECT_EQ(cost.RootParses, costs.front().RootParses);
    }
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    const auto start = std::chrono::steady_clock::now();
    auto opened = OpenExisting({root.string(), test::store_id, "local-delete-extra-v1", CostLimits()});
    ASSERT_TRUE(opened) << opened.error().message;
    std::cout << std::format("archive_cost reopen_ms={} archived={}\n",
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count(),
        (*opened)->GetSnapshotArchive(test::collection_id)->archived_snapshots);
}

// The same measurement without the setting: the root grows with history.
TEST(SnapshotArchiveCost, DISABLED_BaselineWithoutArchiveGrowsWithHistory)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    auto created = CreateStore({test::store_id, root.string(), "local-delete-extra-v1", CostLimits()});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    int64_t next = 0;
    for (const auto history : CostHistories()) {
        (void)MeasureWindow(*engine, root, next, history, 17);
    }
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    const auto start = std::chrono::steady_clock::now();
    auto opened = OpenExisting({root.string(), test::store_id, "local-delete-extra-v1", CostLimits()});
    ASSERT_TRUE(opened) << opened.error().message;
    std::cout << std::format("archive_cost baseline reopen_ms={}\n",
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
}

} // namespace
} // namespace glamdb
