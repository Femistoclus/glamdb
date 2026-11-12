#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <spawn.h>
#include <sys/wait.h>
#include <tuple>

extern char** environ;

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

AppendRequest SecondRequest()
{
    return {test::collection_id, SecondOperation, {}, {Batch(SecondRows)}};
}

void ExpectRows(const std::shared_ptr<Engine>& engine, bool secondCommitted)
{
    const auto cursor = engine->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor) << cursor.error().message;
    std::vector<std::string> rows;
    for (;;) {
        const auto batch = (*cursor)->Next();
        ASSERT_TRUE(batch) << batch.error().message;
        if (!*batch) {
            break;
        }
        const auto rendered = Render(**batch);
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
    EXPECT_EQ(Sorted(std::move(rows)), secondCommitted ? Expected({&FirstRows, &SecondRows}) : Expected({&FirstRows}));
}

TEST(StorageFaultRecoveryChild, ReopensWithProductionDefaults)
{
    const auto* root = std::getenv("GLAMDB_STORAGE_FAULT_ROOT");
    const auto* committed = std::getenv("GLAMDB_STORAGE_FAULT_COMMITTED");
    if (!root || !committed) {
        GTEST_SKIP() << "Invoked by the storage fault test in a fresh process.";
    }
    const bool secondCommitted = std::string_view(committed) == "1";
    const auto before = CountRegularFiles(std::filesystem::path(root) / "warehouse");
    const auto opened = OpenExisting({root, test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    EXPECT_EQ(CountRegularFiles(std::filesystem::path(root) / "warehouse"), before);
    const auto& engine = *opened;
    EXPECT_TRUE(engine->GetHealth().write_ready);
    ExpectRows(engine, secondCommitted);
    const auto outcome = engine->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(outcome) << outcome.error().message;
    ASSERT_TRUE(outcome->has_value());
    EXPECT_EQ(test::Present(*outcome).disposition,
        secondCommitted ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(engine->AppendDurable(SecondRequest()), test::Present(*outcome));
    EXPECT_EQ(CountRegularFiles(std::filesystem::path(root) / "warehouse"), before);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

void ExpectRecoveryInFreshProcess(const std::filesystem::path& root, bool committed)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        const std::string_view value(*entry);
        if (!value.starts_with("GLAMDB_STORAGE_FAULT_ROOT=") && !value.starts_with("GLAMDB_STORAGE_FAULT_COMMITTED=")) {
            environment.emplace_back(value);
        }
    }
    environment.push_back("GLAMDB_STORAGE_FAULT_ROOT=" + root.string());
    environment.push_back(committed ? "GLAMDB_STORAGE_FAULT_COMMITTED=1" : "GLAMDB_STORAGE_FAULT_COMMITTED=0");
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=StorageFaultRecoveryChild.ReopensWithProductionDefaults";
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
    EXPECT_EQ(WEXITSTATUS(status), 0);
#else
    static_cast<void>(root);
    static_cast<void>(committed);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

class TStorageFaultTest : public ::testing::Test
{
protected:
    test::JournalFaultVfs Vfs_;
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<TFaultFS> Faults_ = std::make_shared<TFaultFS>(Posix_);
    std::shared_ptr<Engine> Engine_;
    std::size_t BaselineStart_ = 0;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [fs = Faults_](const std::filesystem::path& root,
                const StoreInfo& store) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = fs;
            return detail::TLocalStorageBackend::Open(root, store, options);
        };
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, factory, Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        BaselineStart_ = Faults_->Events().size();
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
            Engine_.reset();
        }
    }

    void Close()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
    }

    void ExpectRealFilesSince(std::size_t start)
    {
        const auto events = Faults_->Events();
        for (const auto kind : {NGlamDB::NIO::EFileKind::Data,
                 NGlamDB::NIO::EFileKind::Manifest,
                 NGlamDB::NIO::EFileKind::ManifestList,
                 NGlamDB::NIO::EFileKind::Metadata})
        {
            const auto found = std::find_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                events.end(),
                [kind](const auto& event) {
                    return event.Op == EIOOp::SyncFile && event.Tag.Kind == kind;
                });
            ASSERT_NE(found, events.end()) << static_cast<int>(kind);
            const auto bytes = ReadBytes(found->Path);
            if (kind == NGlamDB::NIO::EFileKind::Data) {
                EXPECT_TRUE(bytes.starts_with("PAR1"));
                EXPECT_TRUE(bytes.ends_with("PAR1"));
            } else if (kind == NGlamDB::NIO::EFileKind::Metadata) {
                EXPECT_NE(bytes.find("\"format-version\""), std::string::npos);
            } else {
                EXPECT_TRUE(bytes.starts_with("Obj\x01"));
            }
        }
    }
};

struct TFileFault
{
    const char* Name;
    EIOOp Operation;
    const char* Path;
    std::uint64_t Occurrence;
};

class TCombinedStorageFaultTest
    : public TStorageFaultTest
    , public ::testing::WithParamInterface<std::tuple<TFileFault, bool>>
{ };

TEST_P(TCombinedStorageFaultTest, PreservesFileFailureWhenFailureReceiptResultIsLost)
{
    const auto& [fault, afterUnlink] = GetParam();
    // Baseline files and rows come from the production backend, including duplicates.
    ExpectRealFilesSince(BaselineStart_);
    const auto start = Faults_->Events().size();
    Faults_->AddRule(
        {fault.Operation, fault.Path, fault.Occurrence, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    Vfs_.Arm(2, afterUnlink); // Bind succeeds; the failed-outcome COMMIT loses its result.
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::RecordingFailure);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_EQ(test::Present(unknown.recovery).first_error.code, "IO_ERROR");
    const auto events = Faults_->Events();
    EXPECT_EQ(std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                  events.end(),
                  [operation = fault.Operation](const auto& event) {
                      return event.Op == operation && event.Error == std::errc::io_error;
                  }),
        1);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()).disposition, CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_TRUE(Engine_->GetHealth().storage_failure_latched);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    const auto failed = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(failed) << failed.error().message;
    ASSERT_TRUE(failed->has_value());
    EXPECT_EQ(test::Present(*failed).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(test::Present(*failed).error), test::Present(unknown.recovery).first_error);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()), test::Present(*failed));
    ExpectRows(Engine_, false);
    Close();
    ExpectRecoveryInFreshProcess(Root_, false);
}

INSTANTIATE_TEST_SUITE_P(Filesystem,
    TCombinedStorageFaultTest,
    ::testing::Combine(::testing::Values(TFileFault{"Parquet", EIOOp::SyncFile, "*.parquet", 1},
                           TFileFault{"Manifest", EIOOp::SyncFile, "*-m0.avro", 1},
                           TFileFault{"ManifestList", EIOOp::SyncFile, "*snap-*.avro", 1},
                           TFileFault{"Metadata", EIOOp::SyncFile, "*.metadata.json", 1},
                           TFileFault{"Directory", EIOOp::SyncDir, "*/metadata", 3}),
        ::testing::Bool()),
    [](const ::testing::TestParamInfo<std::tuple<TFileFault, bool>>& info) {
        return std::string(std::get<0>(info.param).Name) + (std::get<1>(info.param) ? "AfterUnlink" : "BeforeUnlink");
    });

class TCatalogStorageFaultTest
    : public TStorageFaultTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TCatalogStorageFaultTest, RecoversUncertainPublicationWithoutReplayingFiles)
{
    const auto start = Faults_->Events().size();
    Vfs_.Arm(2, GetParam()); // Bind succeeds; publication fails before or after unlink.
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::CatalogCommit);
    ExpectRealFilesSince(start);
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    Vfs_.Disarm();
    // Shutdown reconciles. Destruction only closes SQLite, leaving Open to recover.
    Engine_.reset();
    ExpectRecoveryInFreshProcess(Root_, GetParam());
}

INSTANTIATE_TEST_SUITE_P(Journal, TCatalogStorageFaultTest, ::testing::Bool());

} // namespace
} // namespace glamdb
