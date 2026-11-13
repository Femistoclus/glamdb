#include "core/api/binary.h"
#include "core/api/codec.h"
#include "core/resources/envelopes.h"
#include "storage/resources.h"
#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <cerrno>
#include <cstdlib>
#include <format>
#include <iostream>
#include <limits>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace glamdb {
namespace {
using namespace vertical;

std::vector<std::string> Collect(ScanCursor& cursor)
{
    std::vector<std::string> rows;
    while (true) {
        auto next = cursor.Next();
        EXPECT_TRUE(next.has_value());
        if (!next || !*next) {
            break;
        }
        auto piece = Render(**next);
        rows.insert(rows.end(), piece.begin(), piece.end());
    }
    return Sorted(std::move(rows));
}

TEST(SnapshotRetention, ExplicitPolicyPreservesCurrentDataReceiptsAndOpenCursor)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    auto created = CreateStore({test::store_id, root.string()});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = *created;
    created->reset();
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    const auto first = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    const auto second = engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    ASSERT_TRUE(first.result);
    ASSERT_TRUE(second.result);
    ScanRequest request{test::collection_id};
    request.selector = SnapshotSelector::Id(test::Present(first.result).snapshot_id);
    auto cursor = engine->Scan(request);
    ASSERT_TRUE(cursor);
    auto unchanged = engine->ExpireSnapshots({test::collection_id});
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(unchanged->snapshots_expired, 0);
    const auto before = CountRegularFiles(root / "warehouse");
    auto expired = engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 1);
    EXPECT_LT(CountRegularFiles(root / "warehouse"), before);
    auto missing = engine->Scan(request);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error().code, "SNAPSHOT_EXPIRED");
    EXPECT_EQ(Collect(**cursor), Expected({&FirstRows}));
    cursor->reset();
    const auto retry = engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(retry.result, first.result);
    auto precondition = engine->AppendDurable({test::collection_id,
        ThirdOperation,
        {SnapshotPreconditionKind::Equals, test::Present(first.result).snapshot_id},
        {Batch(FirstRows)}});
    ASSERT_TRUE(precondition.error);
    EXPECT_EQ(test::Present(precondition.error).code, "SNAPSHOT_EXPIRED");
    EXPECT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    auto reopened = OpenExisting({root.string(), test::store_id});
    ASSERT_TRUE(reopened) << reopened.error().message;
    auto saved = (*reopened)->GetSnapshotRetention(test::collection_id);
    ASSERT_TRUE(saved);
    EXPECT_EQ(saved->keep_last, 1);
    auto latest = (*reopened)->Scan(ScanRequest{test::collection_id});
    ASSERT_TRUE(latest);
    EXPECT_EQ(Collect(**latest), Expected({&FirstRows, &SecondRows}));
    EXPECT_EQ(test::Present((*reopened)->FindAppendOutcome(test::collection_id, FirstOperation).value()).result,
        first.result);
}

detail::BackendFactory RetentionBackend(detail::TLocalStorageOptions options)
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

class TSnapshotRetentionTest : public ::testing::Test
{
protected:
    test::TempDir Directory_;
    std::filesystem::path Root_ = Directory_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<NGlamDB::NTesting::TFaultFS> Fs_ = std::make_shared<NGlamDB::NTesting::TFaultFS>(Posix_);
    test::JournalFaultVfs Vfs_;
    std::shared_ptr<Engine> Engine_;
    AppendResult First_, Second_;

    void SetUp() override
    {
        detail::TLocalStorageOptions options;
        options.Fs = Fs_;
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, RetentionBackend(options), Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        // Retention (#184) without the archive tier is this suite's subject.
        ASSERT_EQ(Engine_
                      ->CreateCollection(
                          {test::collection_id, "events", Schema, {}, CollectionMode::Append, KeepAllSnapshotsInRoot})
                      .disposition,
            CommitDisposition::Committed);
        const auto a = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
        const auto b = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
        ASSERT_TRUE(a.result);
        ASSERT_TRUE(b.result);
        First_ = test::Present(a.result);
        Second_ = test::Present(b.result);
    }
    void TearDown() override
    {
        Vfs_.Disarm();
        if (Engine_) {
            [[maybe_unused]] auto stopped = Engine_->Shutdown(std::chrono::seconds(5));
        }
    }
    void Close()
    {
        ASSERT_TRUE(Engine_->Shutdown(std::chrono::seconds(5)));
        Engine_.reset();
    }
    void Reopen()
    {
        Close();
        auto opened = OpenExisting({Root_.string(), test::store_id});
        if (!opened) {
            throw std::runtime_error("reopen failed: " + opened.error().code + ": " + opened.error().message);
        }
        Engine_ = std::move(*opened);
    }
    std::uintmax_t ManifestListBytes() const
    {
        std::uintmax_t size = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(Root_ / "warehouse")) {
            if (entry.is_regular_file() && entry.path().filename().string().starts_with("snap-")) {
                size += entry.file_size();
            }
        }
        return size;
    }
};

TEST_F(TSnapshotRetentionTest, DefaultDoesNotMigrateOrDeleteHistory)
{
    const auto before = ManifestListBytes();
    const auto result = Engine_->ExpireSnapshots({test::collection_id});
    ASSERT_TRUE(result);
    EXPECT_EQ(result->snapshots_expired, 0);
    EXPECT_EQ(ManifestListBytes(), before);
    detail::Database catalog;
    catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READONLY, {});
    EXPECT_EQ(catalog.Integer("PRAGMA user_version"), 1);
    Reopen();
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
}

TEST_F(TSnapshotRetentionTest, MinimumAgeProtectsSnapshotsBeyondKeepLastAndPolicyCanBeDisabled)
{
    const SnapshotRetentionPolicy policy{1, std::chrono::hours(24)};
    auto result = Engine_->ExpireSnapshots({test::collection_id, policy});
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->snapshots_expired, 0);
    Reopen();
    EXPECT_EQ(*Engine_->GetSnapshotRetention(test::collection_id), policy);
    result = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->snapshots_expired, 1);
    result = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{}});
    ASSERT_TRUE(result);
    const auto append = Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(FirstRows)}});
    ASSERT_TRUE(append.result) << test::Present(append.error).message;
    EXPECT_EQ(test::Present(append.result).parent_snapshot_id, Second_.snapshot_id);
    EXPECT_EQ(Engine_->ExpireSnapshots({test::collection_id})->snapshots_expired, 0);
    Reopen();
    auto scan = Engine_->Scan(ScanRequest{test::collection_id});
    ASSERT_TRUE(scan);
    EXPECT_EQ(Collect(**scan), Expected({&FirstRows, &SecondRows, &FirstRows}));
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
}

TEST_F(TSnapshotRetentionTest, InvalidPolicyDoesNotPublishOrRemoveAnything)
{
    const auto before = CountRegularFiles(Root_ / "warehouse");
    for (const auto& policy : {SnapshotRetentionPolicy{0},
             SnapshotRetentionPolicy{(1LL << 40) + 1},
             SnapshotRetentionPolicy{1, std::chrono::milliseconds(-1)}})
    {
        const auto result = Engine_->ExpireSnapshots({test::collection_id, policy});
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
    }
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    EXPECT_EQ(*Engine_->GetSnapshotRetention(test::collection_id), SnapshotRetentionPolicy{});
}

TEST_F(TSnapshotRetentionTest, ExpiredMetadataErrorsRemainDistinctAndInitialViewStaysEmpty)
{
    const auto selector = SnapshotSelector::Id(First_.snapshot_id);
    const auto previousFiles = Engine_->ListFiles(test::collection_id, selector);
    ASSERT_TRUE(previousFiles);
    ASSERT_FALSE(previousFiles->Files.empty());
    ASSERT_TRUE(Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    const auto details = Engine_->GetFileDetails(test::collection_id, previousFiles->Files.front().FileID, selector);
    ASSERT_FALSE(details);
    EXPECT_EQ(details.error().code, "SNAPSHOT_EXPIRED");
    const auto stats = Engine_->GetStatistics(test::collection_id, selector);
    ASSERT_FALSE(stats);
    EXPECT_EQ(stats.error().code, "SNAPSHOT_EXPIRED");
    const auto files = Engine_->ListFiles(test::collection_id, selector);
    ASSERT_FALSE(files);
    EXPECT_EQ(files.error().code, "SNAPSHOT_EXPIRED");
    ScanRequest request{test::collection_id};
    request.selector = selector;
    const auto explain = Engine_->ExplainScan(request);
    ASSERT_FALSE(explain);
    EXPECT_EQ(explain.error().code, "SNAPSHOT_EXPIRED");
    request.selector = SnapshotSelector::Id(-42);
    const auto unknown = Engine_->Scan(request);
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "NOT_FOUND");
    request.selector = SnapshotSelector::InitialEmpty();
    auto empty = Engine_->Scan(request);
    ASSERT_TRUE(empty);
    EXPECT_TRUE(Collect(**empty).empty());
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    ASSERT_EQ(history->snapshots.size(), 1U);
    EXPECT_EQ(history->snapshots[0].receipt, Second_);
}

TEST_F(TSnapshotRetentionTest, FailedRootSyncCannotAuthorizeExpiration)
{
    const auto before = ManifestListBytes();
    Fs_->AddRule({.Op = NGlamDB::NTesting::EIOOp::SyncFile,
        .PathGlob = "*.metadata.json",
        .Error = std::make_error_code(std::errc::no_space_on_device),
        .MaxBytes = std::nullopt});
    const auto result = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_FALSE(result);
    EXPECT_EQ(ManifestListBytes(), before);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
    Reopen();
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
    EXPECT_EQ(*Engine_->GetSnapshotRetention(test::collection_id), SnapshotRetentionPolicy{});
}

TEST_F(TSnapshotRetentionTest, FailedUnlinkRemainsQueuedAndRecoveryReclaimsIt)
{
    const auto before = ManifestListBytes();
    Fs_->AddRule({.Op = NGlamDB::NTesting::EIOOp::Unlink,
        .PathGlob = "*snap-*",
        .Repeat = 100,
        .Error = std::make_error_code(std::errc::permission_denied),
        .MaxBytes = std::nullopt});
    const auto result = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->snapshots_expired, 1);
    EXPECT_EQ(result->cleanup_pending, 1);
    EXPECT_EQ(ManifestListBytes(), before);
    Reopen();
    EXPECT_LT(ManifestListBytes(), before);
    EXPECT_EQ(Engine_->ExpireSnapshots({test::collection_id})->cleanup_pending, 0);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, FirstOperation).value()).result, First_);
}

TEST_F(TSnapshotRetentionTest, UncertainOuterCommitDefersCleanupUntilReconciliation)
{
    const auto before = ManifestListBytes();
    Vfs_.Arm(1, true);
    const auto result = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "RECOVERY_REQUIRED");
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(ManifestListBytes(), before);
    Vfs_.Disarm();
    const auto reconciled = Engine_->Reconcile();
    ASSERT_TRUE(reconciled) << reconciled.error().message;
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 1U);
    EXPECT_LT(ManifestListBytes(), before);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, FirstOperation).value()).result, First_);
}

TEST_F(TSnapshotRetentionTest, MissingExpirationLedgerIsCorruption)
{
    ASSERT_TRUE(Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    Close();
    detail::Database catalog;
    catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    catalog.Execute("DELETE FROM expired_snapshots");
    catalog.Close();
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
}

TEST_F(TSnapshotRetentionTest, ExpiredDataFilesRemainVerified)
{
    ASSERT_TRUE(Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    Close();
    auto files = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(files.size(), 2U);
    std::filesystem::remove(files.front());
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
}

std::string RetentionOperation(int64_t index)
{
    return std::format("00000000-0000-4000-8000-{:012x}", index + 1);
}

enum class ERetentionChildExit : uint8_t { Success = 0, Interrupted = 77 };

void RunRetentionChild(const std::filesystem::path& root,
    std::string_view mode,
    int64_t rows,
    ERetentionChildExit expectedExit = ERetentionChildExit::Success)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        if (!std::string_view(*entry).starts_with("GLAMDB_RETENTION_")) {
            environment.emplace_back(*entry);
        }
    }
    environment.push_back("GLAMDB_RETENTION_ROOT=" + root.string());
    environment.push_back("GLAMDB_RETENTION_MODE=" + std::string(mode));
    environment.push_back("GLAMDB_RETENTION_ROWS=" + std::to_string(rows));
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=SnapshotRetentionChild.Run";
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
    EXPECT_EQ(WEXITSTATUS(status), static_cast<int>(expectedExit));
#else
    (void)root;
    (void)mode;
    (void)rows;
    (void)expectedExit;
    GTEST_SKIP() << "Fresh-process evidence requires Linux.";
#endif
}

TEST(SnapshotRetentionChild, Run)
{
    const auto* root = std::getenv("GLAMDB_RETENTION_ROOT");
    const auto* mode = std::getenv("GLAMDB_RETENTION_MODE");
    const auto* rowText = std::getenv("GLAMDB_RETENTION_ROWS");
    if (!root || !mode || !rowText) {
        GTEST_SKIP() << "Invoked by retention tests in a fresh process.";
    }
    const auto start = std::chrono::steady_clock::now();
    Limits limits;
    limits.max_operations = 8192;
    test::JournalFaultVfs vfs;
    auto opened = detail::EngineFactory::OpenWith({root, test::store_id, "local-delete-extra-v1", limits},
        RetentionBackend({}),
        vfs.Name());
    ASSERT_TRUE(opened) << opened.error().message;
    auto engine = std::move(*opened);
    const auto reopenMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    if (std::string_view(mode).starts_with("crash-")) {
        vfs.Arm(1, std::string_view(mode) == "crash-after");
        const auto result = engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
        ::_exit(!result && result.error().code == "RECOVERY_REQUIRED" ? 77 : 78);
    }
    const auto rows = std::stoll(rowText);
    auto cursor = engine->Scan(ScanRequest{test::collection_id});
    ASSERT_TRUE(cursor);
    EXPECT_EQ(Collect(**cursor).size(), static_cast<size_t>(rows));
    if (rows > 4096) {
        std::string receipts;
        for (int64_t index = 0; index < rows / 3; ++index) {
            const auto outcome = engine->FindAppendOutcome(test::collection_id, RetentionOperation(index));
            ASSERT_TRUE(outcome && *outcome);
            ASSERT_EQ(test::Present(outcome.value()).disposition, CommitDisposition::Committed);
            const auto encoded = detail::EncodeOutcome(test::Present(outcome.value()));
            ASSERT_TRUE(encoded);
            receipts += *encoded;
        }
        EXPECT_EQ(detail::Sha256(receipts), ReadBytes(std::filesystem::path(root).parent_path() / "receipts.sha256"));
    }
    std::cout << "retention fresh_process_reopen_ms=" << reopenMs << " rows=" << rows << '\n';
}

TEST_F(TSnapshotRetentionTest, ProcessExitAcrossOuterCommitKeepsAuthoritativeRoot)
{
    Close();
    RunRetentionChild(Root_, "crash-before", 5, ERetentionChildExit::Interrupted);
    auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = std::move(*opened);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 2U);
    Close();
    RunRetentionChild(Root_, "crash-after", 5, ERetentionChildExit::Interrupted);
    RunRetentionChild(Root_, "verify", 5);
    opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = std::move(*opened);
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 1U);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, FirstOperation).value()).result, First_);
}

TEST(SnapshotRetention, DISABLED_MoreThan4096AppendsBoundRootAndVerifyAllReceiptsInFreshProcess)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    Limits limits;
    limits.max_operations = 8192;
    auto created = CreateStore({test::store_id, root.string(), "local-delete-extra-v1", limits});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_TRUE(engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{8}}));
    const auto batch = Batch(FirstRows);
    std::string receipts;
    std::uintmax_t maximumRoot = 0;
    const auto start = std::chrono::steady_clock::now();
    for (int64_t index = 0; index < 4100; ++index) {
        const auto appended = engine->AppendDurable({test::collection_id, RetentionOperation(index), {}, {batch}});
        ASSERT_EQ(appended.disposition, CommitDisposition::Committed)
            << index << ": " << test::Present(appended.error).message;
        const auto encoded = detail::EncodeOutcome(appended);
        ASSERT_TRUE(encoded);
        receipts += *encoded;
        if (index % 16 == 15 || index == 4099) {
            const auto expired = engine->ExpireSnapshots({test::collection_id});
            ASSERT_TRUE(expired) << index << ": " << expired.error().message;
            const auto roots = FilesIn(root / "warehouse" / test::collection_id / "metadata", [](const auto& path) {
                return path.filename().string().ends_with(".metadata.json");
            });
            ASSERT_EQ(roots.size(), 1U);
            maximumRoot = std::max(maximumRoot, std::filesystem::file_size(roots[0]));
            EXPECT_LT(std::filesystem::file_size(roots[0]), 16384U);
        }
        if (index % 512 == 511) {
            std::cout << "retention appends=" << index + 1 << " max_root_bytes=" << maximumRoot << '\n' << std::flush;
        }
    }
    const auto history = engine->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    EXPECT_EQ(history->snapshots.size(), 8U);
    WriteBytes(directory.path / "receipts.sha256", detail::Sha256(receipts));
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    std::cout << "retention appends=4100 max_root_bytes=" << maximumRoot << " elapsed_ms=" << elapsed << '\n'
              << std::flush;
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    RunRetentionChild(root, "verify", 12300);
}

TEST(SnapshotRetention, VeryLargeOperationLimitAndKeepLastDoNotOverflowMaintenance)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    Limits limits;
    limits.max_operations = std::numeric_limits<int64_t>::max();
    auto created = CreateStore({test::store_id, root.string(), "local-delete-extra-v1", limits});
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    const auto protectedHistory = engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{8193}});
    ASSERT_TRUE(protectedHistory) << protectedHistory.error().message;
    EXPECT_EQ(protectedHistory->snapshots_expired, 0);
    EXPECT_EQ(protectedHistory->cleanup_pending, 0);
    const auto expired = engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 1);
    EXPECT_EQ(expired->cleanup_pending, 0);
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    const auto opened = OpenExisting({root.string(), test::store_id, "local-delete-extra-v1", limits});
    ASSERT_TRUE(opened) << opened.error().message;
    EXPECT_EQ((*opened)->GetStatistics(test::collection_id)->rows.value, 5);
    EXPECT_EQ((*opened)->ListSnapshots(test::collection_id)->snapshots.size(), 1U);
}

// Configured maxima reserve nothing: a huge operation profile is admitted, and
// the recovery reserve follows the operations that actually exist.
TEST(SnapshotRetention, HugeOperationProfilesReserveOnlyForActualOperations)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    ResourceLimits budget;
    budget.memory_bytes[static_cast<size_t>(ResourceClass::Recovery)] = 1ULL << 30;
    const auto owner = std::make_shared<ResourceOwner>(budget);
    Limits limits;
    limits.max_operations = std::numeric_limits<int64_t>::max();
    limits.history.max_data_files = 1LL << 40;
    const auto admitted = CreateStore({test::store_id, root.string(), "local-delete-extra-v1", limits, owner});
    ASSERT_TRUE(admitted) << admitted.error().message;
    EXPECT_TRUE((*admitted)->GetHealth().write_ready);
    EXPECT_EQ(owner->GetUsage().memory_bytes[static_cast<size_t>(ResourceClass::Recovery)],
        detail::RecoveryMemoryBytes);
}

TEST(SnapshotRetention, OptInDoesNotRelaxOtherCollectionsManifestProfile)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    detail::TLocalStorageOptions options;
    options.Storage.Store.Manifests = 1;
    auto created = detail::EngineFactory::CreateWith({test::store_id, root.string()}, RetentionBackend(options));
    ASSERT_TRUE(created);
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "retained", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_TRUE(engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    ASSERT_EQ(engine->CreateCollection({SecondCollection, "default", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(engine->AppendDurable({SecondCollection, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    const auto refused = engine->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(SecondRows)}});
    EXPECT_EQ(refused.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(engine->FindAppendOutcome(SecondCollection, SecondOperation)->has_value());
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    const auto reopened = detail::EngineFactory::OpenWith({root.string(), test::store_id}, RetentionBackend(options));
    ASSERT_TRUE(reopened) << reopened.error().message;
}

TEST(SnapshotRetention, ReconcileReusesExpiredIdentityReservationAtFullMetadataCapacity)
{
    test::TempDir directory;
    auto owner = std::make_shared<ResourceOwner>();
    auto created = CreateStore(
        {.store_id = test::store_id, .absolute_root = (directory.path / "store").string(), .resources = owner});
    ASSERT_TRUE(created);
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    for (int64_t index = 0; index < 4; ++index) {
        ASSERT_EQ(
            engine->AppendDurable({test::collection_id, RetentionOperation(index), {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
    }
    const auto expired = engine->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired);
    ASSERT_EQ(expired->snapshots_expired, 3);
    ASSERT_EQ(engine->AppendDurable({test::collection_id, RetentionOperation(4), {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    auto occupied =
        owner->Reserve(ResourceClass::Metadata, owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(occupied);
    const auto recovered = engine->Reconcile();
    EXPECT_TRUE(recovered) << recovered.error().message;
}

TEST(SnapshotRetention, AggregateReopenIsIndependentOfCollectionCreationOrder)
{
    test::TempDir directory;
    const auto root = directory.path / "store";
    detail::TLocalStorageOptions options;
    options.Storage.Store.Manifests = 3;
    auto created = detail::EngineFactory::CreateWith({test::store_id, root.string()}, RetentionBackend(options));
    ASSERT_TRUE(created);
    auto engine = std::move(*created);
    constexpr char third[] = "00000000-0000-4000-8000-0000000000b3";
    for (const auto* id : {test::collection_id, SecondCollection, third}) {
        ASSERT_EQ(engine->CreateCollection({id, id, Schema}).disposition, CommitDisposition::Committed);
    }
    ASSERT_TRUE(engine->ExpireSnapshots({third, SnapshotRetentionPolicy{1}}));
    for (const auto* id : {test::collection_id, SecondCollection, third}) {
        ASSERT_EQ(engine->AppendDurable({id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
    }
    ASSERT_TRUE(engine->Shutdown(std::chrono::seconds(5)));
    engine.reset();
    const auto opened = detail::EngineFactory::OpenWith({root.string(), test::store_id}, RetentionBackend(options));
    ASSERT_TRUE(opened) << opened.error().message;
}

class TRetentionBulkSource final : public BulkAppendSource
{
public:
    Status Rewind() override
    {
        Position_ = 0;
        return {};
    }
    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (Position_ == 0) {
            ++Position_;
            return Batch(FirstRows);
        }
        if (Position_ == 1) {
            ++Position_;
            return Batch(SecondRows);
        }
        return std::shared_ptr<arrow::RecordBatch>{};
    }

private:
    int Position_ = 0;
};

TEST_F(TSnapshotRetentionTest, RepeatedExpirationPreservesBulkAncestryAndEveryReceipt)
{
    const auto bulk = Engine_->BulkAppendDurable({.collection_id = test::collection_id,
        .operation_id = ThirdOperation,
        .rows = 5,
        .source = std::make_shared<TRetentionBulkSource>()});
    ASSERT_TRUE(bulk.result) << test::Present(bulk.error).message;
    ASSERT_EQ(test::Present(bulk.result).receipt_version, 2);
    auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 2);
    constexpr char fourth[] = "00000000-0000-4000-8000-0000000000a4";
    ASSERT_TRUE(Engine_->AppendDurable({test::collection_id, fourth, {}, {Batch(FirstRows)}}).result);
    expired = Engine_->ExpireSnapshots({test::collection_id});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 1);
    Reopen();
    auto scan = Engine_->Scan(ScanRequest{test::collection_id});
    ASSERT_TRUE(scan);
    EXPECT_EQ(Collect(**scan), Expected({&FirstRows, &SecondRows, &FirstRows, &SecondRows, &FirstRows}));
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, FirstOperation).value()).result, First_);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, SecondOperation).value()).result, Second_);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, ThirdOperation).value()).result,
        bulk.result);
}

TEST_F(TSnapshotRetentionTest, SameCountLedgerIdentityTamperingIsCorruption)
{
    ASSERT_TRUE(Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}}));
    Close();
    detail::Database catalog;
    catalog.Open(Root_ / "catalog.sqlite", SQLITE_OPEN_READWRITE, {});
    catalog.Execute("UPDATE expired_snapshots SET snapshot_id=?", {Second_.snapshot_id});
    catalog.Close();
    const auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, "CORRUPTION");
}

TEST_F(TSnapshotRetentionTest, UnlinkWithoutDirectoryFenceRemainsQueuedAndRetryFencesMissingPath)
{
    const auto before = ManifestListBytes();
    Fs_->AddRule({.Op = NGlamDB::NTesting::EIOOp::SyncDir,
        .PathGlob = "*/metadata",
        .Occurrence = 2,
        .Repeat = 2,
        .Error = std::make_error_code(std::errc::no_space_on_device),
        .MaxBytes = std::nullopt});
    const auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->cleanup_pending, 1);
    EXPECT_LT(ManifestListBytes(), before);
    const auto retried = Engine_->ExpireSnapshots({test::collection_id});
    ASSERT_TRUE(retried) << retried.error().message;
    EXPECT_EQ(retried->snapshots_expired, 0);
    EXPECT_EQ(retried->cleanup_pending, 0);
    Reopen();
    EXPECT_EQ(Engine_->ListSnapshots(test::collection_id)->snapshots.size(), 1U);
}

TEST_F(TSnapshotRetentionTest, LastRetainedReferenceProtectsMergedInputsUntilTheNextExpiration)
{
    for (int64_t index = 2; index < 100; ++index) {
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, RetentionOperation(index), {}, {Batch(FirstRows)}})
                      .disposition,
            CommitDisposition::Committed);
    }
    const auto metadata = Root_ / "warehouse" / test::collection_id / "metadata";
    auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{2}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 98);
    EXPECT_EQ(expired->cleanup_pending, 0);
    const auto protectedFiles = CountRegularFiles(metadata);
    EXPECT_GT(protectedFiles, 100U);
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history);
    ASSERT_EQ(history->snapshots.size(), 2U);
    const auto older = history->snapshots.front().receipt.snapshot_id;
    Reopen();
    auto scan = Engine_->Scan(ScanRequest{test::collection_id, SnapshotSelector::Id(older)});
    ASSERT_TRUE(scan);
    EXPECT_EQ(Collect(**scan).size(), 296U);
    scan->reset();
    ASSERT_EQ(
        Engine_->AppendDurable({test::collection_id, RetentionOperation(100), {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    expired = Engine_->ExpireSnapshots({test::collection_id});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 1);
    EXPECT_EQ(expired->cleanup_pending, 0);
    EXPECT_LT(CountRegularFiles(metadata), 8U);
    Reopen();
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id)->rows.value, 302);
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id, SnapshotSelector::Id(older)).error().code,
        "SNAPSHOT_EXPIRED");
}

TEST_F(TSnapshotRetentionTest, MergedManifestsReclaimObsoleteInputsAndReopenExpiredAncestry)
{
    for (int64_t index = 2; index < 105; ++index) {
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, RetentionOperation(index), {}, {Batch(FirstRows)}})
                      .disposition,
            CommitDisposition::Committed);
    }
    const auto metadata = Root_ / "warehouse" / test::collection_id / "metadata";
    const auto before = CountRegularFiles(metadata);
    ASSERT_GT(before, 200U);
    const auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 104);
    EXPECT_EQ(expired->cleanup_pending, 0);
    EXPECT_LT(CountRegularFiles(metadata), 12U);
    Close();
    RunRetentionChild(Root_, "verify", 314);
    auto opened = OpenExisting({Root_.string(), test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = std::move(*opened);
    EXPECT_EQ(Engine_->GetStatistics(test::collection_id)->rows.value, 314);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, FirstOperation).value()).result, First_);
    EXPECT_EQ(test::Present(Engine_->FindAppendOutcome(test::collection_id, SecondOperation).value()).result, Second_);
}

} // namespace
} // namespace glamdb
