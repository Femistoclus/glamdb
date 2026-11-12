#include "tests/vertical/storage_fault_support.h"

#include "tests/support/checked.h"

#include <limits>
#include <ostream>
#include <tuple>

namespace glamdb {
namespace {

using namespace glamdb::vertical;
using NGlamDB::NIO::EFileKind;
using NGlamDB::NTesting::TFaultRule;

struct TFileFault
{
    std::string Name;
    TFaultRule Rule;
    bool ShortFirst = false;
};

struct TFileClass
{
    const char* Name;
    const char* Path;
    EFileKind Kind;
};

void PrintTo(const TFileFault& fault, std::ostream* output)
{
    *output << fault.Name;
}

void PrintTo(const TFileClass& file, std::ostream* output)
{
    *output << file.Name;
}

const TFileClass FileClasses[]{
    {"Parquet", "*.parquet", EFileKind::Data},
    {"Manifest", "*-m0.avro", EFileKind::Manifest},
    {"ManifestList", "*snap-*.avro", EFileKind::ManifestList},
    {"Metadata", "*.metadata.json", EFileKind::Metadata},
};

std::vector<TFileFault> FileFaults()
{
    std::vector<TFileFault> faults;
    for (const auto& file : FileClasses) {
        for (const auto& [name, op, error] : {std::tuple{"WriteEio", EIOOp::Write, std::errc::io_error},
                 std::tuple{"WriteEnospc", EIOOp::Write, std::errc::no_space_on_device},
                 std::tuple{"Sync", EIOOp::SyncFile, std::errc::io_error},
                 std::tuple{"Close", EIOOp::Close, std::errc::io_error}})
        {
            faults.push_back({std::string(file.Name) + name, {op, file.Path, 1, 1, std::make_error_code(error), {}}});
        }
        faults.push_back({std::string(file.Name) + "ShortThenEnospc",
            {EIOOp::Write, file.Path, 2, 1, std::make_error_code(std::errc::no_space_on_device), {}},
            true});
        faults.push_back({std::string(file.Name) + "ZeroWrite", {EIOOp::Write, file.Path, 1, 1, {}, 0}});
    }
    faults.push_back(
        {"ParquetDirectory", {EIOOp::SyncDir, "*/data", 1, 1, std::make_error_code(std::errc::io_error), {}}});
    for (std::uint64_t i = 1; i <= 3; ++i) {
        faults.push_back({std::string(FileClasses[i].Name) + "Directory",
            {EIOOp::SyncDir, "*/metadata", i, 1, std::make_error_code(std::errc::io_error), {}}});
    }
    return faults;
}

class TCombinedStorageFaultTest
    : public TStorageFaultTest
    , public ::testing::WithParamInterface<std::tuple<TFileFault, bool, bool>>
{ };

TEST_P(TCombinedStorageFaultTest, PreservesFileFailureWhenFailureReceiptResultIsLost)
{
    const auto [fault, afterUnlink, freshRecovery] = GetParam();
    const bool latched = fault.Rule.Error == std::errc::io_error;
    // Baseline files and rows come from the production backend, including duplicates.
    ExpectRealFilesSince(BaselineStart_);
    const auto start = Faults_->Events().size();
    if (fault.ShortFirst) {
        Faults_->AddRule({EIOOp::Write, fault.Rule.PathGlob, 1, 1, {}, 1});
    }
    Faults_->AddRule(fault.Rule);
    Vfs_.Arm(2, afterUnlink); // Bind succeeds; the failed-outcome COMMIT loses its result.
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::RecordingFailure);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_EQ(test::Present(unknown.recovery).first_error.code, "IO_ERROR");
    const auto events = Faults_->Events();
    if (fault.ShortFirst) {
        const auto failedWrite =
            std::find_if(events.begin() + static_cast<std::ptrdiff_t>(start), events.end(), [fault](const auto& event) {
                return event.Op == EIOOp::Write && event.Error == fault.Rule.Error;
            });
        ASSERT_NE(failedWrite, events.end());
        EXPECT_EQ(std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                      failedWrite,
                      [&failedWrite](const auto& event) {
                          return event.Op == EIOOp::Write && event.Path == failedWrite->Path && event.Bytes == 1;
                      }),
            1);
    }
    EXPECT_EQ(std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                  events.end(),
                  [operation = fault.Rule.Op, error = fault.Rule.Error, zeroProgress = fault.Rule.MaxBytes.has_value()](
                      const auto& event) {
                      return event.Op == operation &&
                          (zeroProgress ? event.Op == EIOOp::Write && event.Bytes == 0 && !event.Error
                                        : event.Error == error);
                  }),
        1);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()).disposition, CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    if (freshRecovery) {
        // No Shutdown/Reconcile: only the fresh process resolves the bound ID.
        Engine_.reset();
        auto expected = unknown;
        if (afterUnlink) {
            auto error = test::Present(unknown.recovery).first_error;
            error.retry_hint = latched ? RetryHint::OperatorAction : RetryHint::NewOperationId;
            expected = {SecondOperation,
                CommitDisposition::DefinitelyNotCommitted,
                CommitPhase::Data,
                {},
                std::move(error),
                {}};
        }
        ExpectRecoveryInFreshProcess(Root_, ESecondOutcome::Failed, First_, expected);
        return;
    }
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_EQ(Engine_->GetHealth().storage_failure_latched, latched);
    EXPECT_EQ(Engine_->GetHealth().write_ready, !latched);
    const auto failed = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(failed) << failed.error().message;
    ASSERT_TRUE(failed->has_value());
    EXPECT_EQ(test::Present(*failed).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(test::Present(test::Present(*failed).error), test::Present(unknown.recovery).first_error);
    const auto retryStart = Faults_->Events().size();
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()), test::Present(*failed));
    if (latched) {
        const auto refused = Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(SecondRows)}});
        ASSERT_TRUE(refused.error);
        EXPECT_EQ(test::Present(refused.error).code, "IO_ERROR");
        const auto absent = Engine_->FindAppendOutcome(test::collection_id, ThirdOperation);
        ASSERT_TRUE(absent);
        EXPECT_FALSE(absent->has_value());
    }
    const auto retryEvents = Faults_->Events();
    EXPECT_EQ(std::count_if(retryEvents.begin() + static_cast<std::ptrdiff_t>(retryStart),
                  retryEvents.end(),
                  [](const auto& event) {
                      return event.Op == EIOOp::CreateExclusive || event.Op == EIOOp::Write;
                  }),
        0);
    ExpectRows(Engine_, false);
    ExpectHistory(Engine_, First_, false);
    ASSERT_NO_FATAL_FAILURE(Close());
    ExpectRecoveryInFreshProcess(Root_, ESecondOutcome::Failed, First_, test::Present(*failed));
}

INSTANTIATE_TEST_SUITE_P(Filesystem,
    TCombinedStorageFaultTest,
    ::testing::Combine(::testing::ValuesIn(FileFaults()), ::testing::Bool(), ::testing::Bool()),
    [](const ::testing::TestParamInfo<std::tuple<TFileFault, bool, bool>>& info) {
        return std::get<0>(info.param).Name + (std::get<1>(info.param) ? "AfterUnlink" : "BeforeUnlink") +
            (std::get<2>(info.param) ? "Fresh" : "Reconciled");
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
    ExpectRecoveryInFreshProcess(Root_,
        GetParam() ? ESecondOutcome::Committed : ESecondOutcome::Failed,
        First_,
        unknown);
}

INSTANTIATE_TEST_SUITE_P(Journal, TCatalogStorageFaultTest, ::testing::Bool());

TEST_P(TCatalogStorageFaultTest, ParquetFinalizationWriteFailureCannotPublishOrLoseItsFirstError)
{
    const auto start = Faults_->Events().size();
    ASSERT_EQ(Engine_->AppendDurable(SecondRequest()).disposition, CommitDisposition::Committed);
    const auto events = Faults_->Events();
    const auto finalWrite =
        std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start), events.end(), [](const auto& event) {
            return event.Op == EIOOp::Write && event.Tag.Kind == EFileKind::Data;
        });
    ASSERT_GT(finalWrite, 0);
    // Locate the final footer/flush write with the exact input, not a guessed
    // syscall count. Recreate only this fixture's disposable store for the fault.
    ASSERT_NO_FATAL_FAILURE(Close());
    std::filesystem::remove_all(Root_);
    ASSERT_NO_FATAL_FAILURE(SetUp());
    const auto faultStart = Faults_->Events().size();
    Faults_->AddRule({EIOOp::Write,
        "*.parquet",
        static_cast<std::uint64_t>(finalWrite),
        1,
        std::make_error_code(std::errc::io_error),
        {}});
    Vfs_.Arm(2, GetParam());
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::RecordingFailure);
    const auto failedEvents = Faults_->Events();
    EXPECT_EQ(std::count_if(failedEvents.begin() + static_cast<std::ptrdiff_t>(faultStart),
                  failedEvents.end(),
                  [](const auto& event) {
                      return event.Op == EIOOp::Write && event.Tag.Kind == EFileKind::Data;
                  }),
        finalWrite);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto failed = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(failed);
    ASSERT_TRUE(failed->has_value());
    EXPECT_EQ(test::Present(test::Present(*failed).error), test::Present(unknown.recovery).first_error);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()), test::Present(*failed));
    ASSERT_NO_FATAL_FAILURE(Close());
    ExpectRecoveryInFreshProcess(Root_, ESecondOutcome::Failed, First_, test::Present(*failed));
}

TEST_P(TCatalogStorageFaultTest, BindingUncertaintyCreatesNoFilesAndRecoversWithoutPayloadReplay)
{
    const auto start = Faults_->Events().size();
    const auto before = CountRegularFiles(Root_ / "warehouse");
    Vfs_.Arm(1, GetParam());
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::Binding);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_FALSE(test::Present(unknown.recovery).publication_started);
    const auto events = Faults_->Events();
    EXPECT_EQ(std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                  events.end(),
                  [](const auto& event) {
                      return event.Op == EIOOp::CreateExclusive || event.Op == EIOOp::Write;
                  }),
        0);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    Vfs_.Disarm();
    Engine_.reset();
    ExpectRecoveryInFreshProcess(Root_, GetParam() ? ESecondOutcome::Failed : ESecondOutcome::Absent, First_, unknown);
}

TEST_P(TCatalogStorageFaultTest, FailedRecoveryFenceCannotReopenReadOrWriteAdmission)
{
    Vfs_.Arm(2, true);
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto failures = Vfs_.FailureCount();
    Vfs_.Arm(1, GetParam());
    EXPECT_FALSE(Engine_->Reconcile());
    EXPECT_EQ(Vfs_.FailureCount(), failures + 1);
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()).disposition, CommitDisposition::CommitUnknown);
    EXPECT_FALSE(Engine_->Scan({.collection_id = test::collection_id}));
    const auto refused = Engine_->AppendDurable({test::collection_id, ThirdOperation, {}, {Batch(SecondRows)}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RECOVERY_REQUIRED");
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    Vfs_.Disarm();
    Engine_.reset();
    ExpectRecoveryInFreshProcess(Root_, ESecondOutcome::Committed, First_, unknown);
}

TEST_P(TCatalogStorageFaultTest, RecoveryFailureRecordingResultCannotOpenAdmissionOrReplayPayload)
{
    Vfs_.Arm(1, true);
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    ASSERT_EQ(unknown.phase, CommitPhase::Binding);
    const auto before = CountRegularFiles(Root_ / "warehouse");
    const auto failures = Vfs_.FailureCount();
    Vfs_.Arm(2, GetParam()); // The fence commits, then recording the interrupted ID loses its result.
    EXPECT_FALSE(Engine_->Reconcile());
    EXPECT_EQ(Vfs_.FailureCount(), failures + 1);
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()).disposition, CommitDisposition::CommitUnknown);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_TRUE(Engine_->GetHealth().write_ready);
    const auto failed = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(failed);
    ASSERT_TRUE(failed->has_value());
    EXPECT_EQ(test::Present(*failed).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()), test::Present(*failed));
    ExpectRows(Engine_, false);
    ExpectHistory(Engine_, First_, false);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    ASSERT_NO_FATAL_FAILURE(Close());
    ExpectRecoveryInFreshProcess(Root_, ESecondOutcome::Failed, First_, test::Present(*failed));
}

class TShortWriteStorageFaultTest
    : public TStorageFaultTest
    , public ::testing::WithParamInterface<std::tuple<TFileClass, bool>>
{ };

TEST_P(TShortWriteStorageFaultTest, PositiveShortWritesAreCompletedBeforeUncertainPublication)
{
    const auto [file, afterUnlink] = GetParam();
    const auto start = Faults_->Events().size();
    Faults_->AddRule({EIOOp::Write, file.Path, 1, std::numeric_limits<std::uint64_t>::max(), {}, 7});
    Vfs_.Arm(2, afterUnlink);
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::CatalogCommit);
    EXPECT_FALSE(Engine_->GetHealth().storage_failure_latched);
    ExpectRealFilesSince(start);
    const auto events = Faults_->Events();
    EXPECT_GT(std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                  events.end(),
                  [kind = file.Kind](const auto& event) {
                      return event.Op == EIOOp::Write && event.Tag.Kind == kind && event.Bytes == 7 && !event.Error;
                  }),
        1);
    Vfs_.Disarm();
    Engine_.reset();
    ExpectRecoveryInFreshProcess(Root_,
        afterUnlink ? ESecondOutcome::Committed : ESecondOutcome::Failed,
        First_,
        unknown);
}

TEST_P(TShortWriteStorageFaultTest, RecoveryReadFailureAndLostFenceResultBothKeepAdmissionClosed)
{
    const auto [file, afterUnlink] = GetParam();
    Vfs_.Arm(2, true);
    const auto unknown = Engine_->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    const auto start = Faults_->Events().size();
    const auto before = CountRegularFiles(Root_ / "warehouse");
    Faults_->AddRule({EIOOp::OpenRead, file.Path, 1, 1, std::make_error_code(std::errc::io_error), {}});
    const auto failures = Vfs_.FailureCount();
    Vfs_.Arm(1, afterUnlink);
    EXPECT_FALSE(Engine_->Reconcile()); // Closure verification fails before the fence.
    EXPECT_EQ(Vfs_.FailureCount(), failures);
    const auto events = Faults_->Events();
    EXPECT_EQ(std::count_if(events.begin() + static_cast<std::ptrdiff_t>(start),
                  events.end(),
                  [](const auto& event) {
                      return event.Op == EIOOp::OpenRead && event.Error == std::errc::io_error;
                  }),
        1);
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_FALSE(Engine_->Reconcile()); // The still-armed SQLite fence now loses its result.
    EXPECT_EQ(Vfs_.FailureCount(), failures + 1);
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    EXPECT_FALSE(Engine_->GetHealth().write_ready);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    Vfs_.Disarm();
    ASSERT_TRUE(Engine_->Reconcile());
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    EXPECT_TRUE(Engine_->GetHealth().write_ready);
    ExpectRows(Engine_, true);
    ExpectHistory(Engine_, First_, true);
    const auto committed = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(committed);
    ASSERT_TRUE(committed->has_value());
    EXPECT_EQ(Engine_->AppendDurable(SecondRequest()), test::Present(*committed));
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), before);
    Engine_.reset();
    ExpectRecoveryInFreshProcess(Root_, ESecondOutcome::Committed, First_, test::Present(*committed));
}

INSTANTIATE_TEST_SUITE_P(Filesystem,
    TShortWriteStorageFaultTest,
    ::testing::Combine(::testing::ValuesIn(FileClasses), ::testing::Bool()),
    [](const ::testing::TestParamInfo<std::tuple<TFileClass, bool>>& info) {
        return std::string(std::get<0>(info.param).Name) + (std::get<1>(info.param) ? "AfterUnlink" : "BeforeUnlink");
    });

} // namespace
} // namespace glamdb
