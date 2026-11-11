#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <spawn.h>
#include <sys/wait.h>

extern char** environ;

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

TEST(CapacityRecoveryChild, RejectsOverBudgetStore)
{
    const auto* root = std::getenv("GLAMDB_CAPACITY_ROOT");
    const auto* physical = std::getenv("GLAMDB_CAPACITY_PHYSICAL");
    if (!root || !physical) {
        GTEST_SKIP() << "Invoked by the capacity test in a fresh process.";
    }
    detail::TLocalStorageOptions options;
    if (std::string_view(physical) == "1") {
        options.Storage.WarehouseBytes = 1U << 20;
    } else {
        options.Storage.Store.Snapshots = 2;
    }
    std::shared_ptr<THookBackend> hooks;
    const auto opened = detail::EngineFactory::OpenWith({root, test::store_id}, THookBackend::Factory(hooks, options));
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().code, std::string_view(physical) == "1" ? "RESOURCE_EXHAUSTED" : "CORRUPTION");
}

void ExpectRejectedInFreshProcess(const std::filesystem::path& root, bool physical)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        const std::string_view value(*entry);
        if (!value.starts_with("GLAMDB_CAPACITY_ROOT=") && !value.starts_with("GLAMDB_CAPACITY_PHYSICAL=")) {
            environment.emplace_back(value);
        }
    }
    environment.push_back("GLAMDB_CAPACITY_ROOT=" + root.string());
    environment.push_back(physical ? "GLAMDB_CAPACITY_PHYSICAL=1" : "GLAMDB_CAPACITY_PHYSICAL=0");
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=CapacityRecoveryChild.RejectsOverBudgetStore";
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
    static_cast<void>(physical);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

class TAggregateCapacityTest : public ::testing::Test
{
protected:
    test::JournalFaultVfs Vfs_;
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;

    void TearDown() override
    {
        if (Engine_) {
            [[maybe_unused]] const auto status = Engine_->Shutdown(1000ms);
        }
    }

    void Create(const detail::TLocalStorageOptions& options = {})
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()},
            THookBackend::Factory(Hooks_, options),
            Vfs_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "a", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "b", Schema}).disposition, CommitDisposition::Committed);
    }

    void Close()
    {
        ASSERT_TRUE(Engine_->Shutdown(1000ms));
        Engine_.reset();
    }
};

TEST_F(TAggregateCapacityTest, ReopenCountsOrphansAgainstPhysicalWarehouseLimit)
{
    Create();
    Close();
    const auto orphan = Root_ / "warehouse" / "orphan.parquet";
    WriteBytes(orphan, std::string(2U << 20, 'x'));
    detail::TLocalStorageOptions options;
    options.Storage.WarehouseBytes = 1U << 20;
    const auto rejected =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    EXPECT_FALSE(rejected);
    if (!rejected) {
        EXPECT_EQ(rejected.error().code, "RESOURCE_EXHAUSTED");
    }
    EXPECT_TRUE(std::filesystem::exists(orphan));
    ExpectRejectedInFreshProcess(Root_, /*physical*/ true);
    std::filesystem::remove(orphan);
    // Reading an admitted store does not require room for another attempt.
    auto reopened =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = std::move(*reopened);
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

TEST_F(TAggregateCapacityTest, PublicationWalkCatchesPhysicalGrowthAfterAdmission)
{
    detail::TLocalStorageOptions options;
    options.Storage.WarehouseBytes = 1U << 20;
    options.Storage.AttemptReserveBytes = 0;
    options.WalkWarehouseAtPublication = true;
    Create(options);
    Hooks_->AfterPrepareAppend = [&](const detail::BoundOperation&) {
        WriteBytes(Root_ / "warehouse" / "orphan.parquet", std::string(2U << 20, 'x'));
    };
    const auto rejected = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    EXPECT_FALSE(collection->current_snapshot_id);
    const auto outcome = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(outcome);
    ASSERT_TRUE(outcome->has_value());
    EXPECT_EQ(test::Present(*outcome).disposition, CommitDisposition::DefinitelyNotCommitted);
}

// By default publication adds the attempt's durable files to the admission
// measurement: growth by anything else after admission is first rejected by
// the next admission, before that append binds an operation.
TEST_F(TAggregateCapacityTest, NextAdmissionRejectsPhysicalGrowthAfterAnEarlierAdmission)
{
    detail::TLocalStorageOptions options;
    options.Storage.WarehouseBytes = 1U << 20;
    options.Storage.AttemptReserveBytes = 0;
    Create(options);
    const auto orphan = Root_ / "warehouse" / "orphan.parquet";
    Hooks_->AfterPrepareAppend = [&](const detail::BoundOperation&) {
        WriteBytes(orphan, std::string(2U << 20, 'x'));
    };
    ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    Hooks_->AfterPrepareAppend = {};
    const auto rejected = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    const auto outcome = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(outcome);
    EXPECT_FALSE(outcome->has_value()) << "admission rejection binds no operation";
    std::filesystem::remove(orphan);
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
}

// Without a second walk, the attempt's own files still cannot publish above
// the physical cap: admission passes at exactly the measured warehouse and
// the guard rejects the produced files before the pointer changes.
TEST_F(TAggregateCapacityTest, AttemptFilesAboveThePhysicalCapCannotPublish)
{
    Create();
    Close();
    const auto warehouse = Posix_.OpenDir(Root_ / "warehouse");
    ASSERT_TRUE(warehouse);
    const auto measured = Posix_.MeasureTree(warehouse.Value(), std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(measured);
    auto faults = std::make_shared<NGlamDB::NTesting::TFaultFS>(Posix_);
    detail::TLocalStorageOptions options;
    options.Fs = faults;
    options.Storage.WarehouseBytes = measured.Value() + 1;
    options.Storage.AttemptReserveBytes = 0;
    auto reopened =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = std::move(*reopened);
    const auto mark = faults->Events().size();
    const auto rejected = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    std::size_t walks = 0;
    const auto events = faults->Events();
    for (std::size_t index = mark; index < events.size(); ++index) {
        walks += events[index].Op == NGlamDB::NTesting::EIOOp::MeasureTree ? 1 : 0;
    }
    EXPECT_EQ(walks, 1U) << "only admission walks the warehouse";
    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    EXPECT_FALSE(collection->current_snapshot_id);
}

TEST_F(TAggregateCapacityTest, PhysicalCapacityFailureKeepsReconcileNotReadyUntilSpaceIsReleased)
{
    detail::TLocalStorageOptions options;
    options.Storage.WarehouseBytes = 1U << 20;
    options.Storage.AttemptReserveBytes = 0;
    Create(options);
    const auto orphan = Root_ / "warehouse" / "orphan.parquet";
    WriteBytes(orphan, std::string(2U << 20, 'x'));
    const auto rejected = Engine_->Reconcile();
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_TRUE(std::filesystem::exists(orphan));
    std::filesystem::remove(orphan);
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetHealth().write_ready);
}

class TPublicationMeasurementFailureTest
    : public TAggregateCapacityTest
    , public ::testing::WithParamInterface<bool>
{ };

// With the publication walk, its failure rejects the candidate. Without it,
// a failed directory measurement falls back to the walk, whose failure
// rejects it the same way.
TEST_P(TPublicationMeasurementFailureTest, DoesNotPublishCandidate)
{
    auto faults = std::make_shared<NGlamDB::NTesting::TFaultFS>(Posix_);
    detail::TLocalStorageOptions options;
    options.Fs = faults;
    options.WalkWarehouseAtPublication = GetParam();
    Create(options);
    Hooks_->AfterPrepareAppend = [&](const detail::BoundOperation&) {
        if (!GetParam()) {
            faults->AddRule({NGlamDB::NTesting::EIOOp::Stat,
                "*/data",
                1,
                1,
                std::make_error_code(std::errc::io_error),
                std::nullopt});
        }
        faults->AddRule({NGlamDB::NTesting::EIOOp::MeasureTree,
            "*",
            1,
            1,
            std::make_error_code(std::errc::io_error),
            std::nullopt});
    };
    const auto rejected = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "IO_ERROR");
    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    EXPECT_FALSE(collection->current_snapshot_id);
}

INSTANTIATE_TEST_SUITE_P(PublicationWalk, TPublicationMeasurementFailureTest, ::testing::Bool());

TEST_F(TAggregateCapacityTest, AlternatingCollectionsShareSnapshotCapacityAfterLostCommitResult)
{
    detail::TLocalStorageOptions options;
    options.Storage.Store.Snapshots = 3;
    Create(options);
    ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
    Vfs_.Arm(2, true);
    const auto unknown = Engine_->AppendDurable({SecondCollection, FirstOperation, {}, {Batch(SecondRows)}});
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    const auto recovered = Engine_->FindAppendOutcome(SecondCollection, FirstOperation);
    ASSERT_TRUE(recovered);
    ASSERT_TRUE(recovered->has_value());
    ASSERT_EQ(test::Present(*recovered).disposition, CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(SecondRows)}}).disposition,
        CommitDisposition::Committed);
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto rejected = Engine_->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    Close();
    auto reopened =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = std::move(*reopened);
    EXPECT_TRUE(Engine_->GetHealth().write_ready);
    Close();
    ExpectRejectedInFreshProcess(Root_, /*physical*/ false);
    options.Storage.Store.Snapshots = 2;
    const auto tooSmall =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_FALSE(tooSmall);
    EXPECT_EQ(tooSmall.error().code, "CORRUPTION");
}

TEST_F(TAggregateCapacityTest, RolledBackUncertainPublicationDoesNotConsumeLogicalCapacity)
{
    detail::TLocalStorageOptions options;
    options.Storage.Store.Snapshots = 1;
    Create(options);
    Vfs_.Arm(2, false);
    const auto unknown = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}});
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    const auto failed = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(failed);
    ASSERT_TRUE(failed->has_value());
    EXPECT_EQ(test::Present(*failed).disposition, CommitDisposition::DefinitelyNotCommitted);
    const auto committed = Engine_->AppendDurable({SecondCollection, FirstOperation, {}, {Batch(SecondRows)}});
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    const auto rejected = Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(FirstRows)}});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    Close();
    auto reopened =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = std::move(*reopened);
    EXPECT_FALSE(Engine_->GetCollection(test::collection_id)->current_snapshot_id);
    EXPECT_EQ(Engine_->GetCollection(SecondCollection)->current_snapshot_id,
        test::Present(committed.result).snapshot_id);
}

TEST_F(TAggregateCapacityTest, EmptyCollectionsShareRootCapacityBeforePublicationAndReopen)
{
    Create();
    std::uint64_t roots = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(Root_ / "warehouse")) {
        if (entry.path().extension() == ".json") {
            roots += entry.file_size();
        }
    }
    ASSERT_GT(roots, 0U);
    Close();
    detail::TLocalStorageOptions options;
    options.Storage.Store.RootBytes = roots + 1;
    auto opened =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = std::move(*opened);
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto rejected = Engine_->CreateCollection({"00000000-0000-4000-8000-0000000000b3", "c", Schema});
    EXPECT_EQ(rejected.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(rejected.error);
    EXPECT_EQ(test::Present(rejected.error).code, "RESOURCE_EXHAUSTED");
    // The one-byte preflight fits, but the actual new root must not publish.
    EXPECT_GT(CountRegularFiles(Root_ / "warehouse"), before);
    const auto collections = Engine_->ListCollections();
    ASSERT_TRUE(collections);
    EXPECT_EQ(collections->size(), 2U);
    Close();
    options.Storage.Store.RootBytes = roots;
    auto exact =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_TRUE(exact) << exact.error().message;
    Engine_ = std::move(*exact);
    Close();
    --options.Storage.Store.RootBytes;
    const auto tooSmall =
        detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, THookBackend::Factory(Hooks_, options));
    ASSERT_FALSE(tooSmall);
    EXPECT_EQ(tooSmall.error().code, "CORRUPTION");
}

} // namespace
} // namespace glamdb
