#include "tests/vertical/storage_fault_support.h"

#include "tests/support/checked.h"

#include <cstdlib>

namespace glamdb {
namespace {

using namespace glamdb::vertical;

TEST(StorageFaultCrashChild, ExitsWithoutShutdownOrReconciliation)
{
    const auto* root = std::getenv("GLAMDB_STORAGE_FAULT_ROOT");
    const auto* setting = std::getenv("GLAMDB_STORAGE_FAULT_COMMITTED");
    if (!root || !setting) {
        GTEST_SKIP() << "Invoked by the crash parent against a disposable store.";
    }
    const std::string_view mode(setting);
    const bool afterUnlink = mode.ends_with("After");
    test::JournalFaultVfs vfs;
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<TFaultFS>(posix);
    const auto factory = [faults](const std::filesystem::path& path, const StoreInfo& store) {
        detail::TLocalStorageOptions options;
        options.Fs = faults;
        return detail::TLocalStorageBackend::Open(path, store, options);
    };
    const auto opened = detail::EngineFactory::OpenWith({root, test::store_id}, factory, vfs.Name());
    ASSERT_TRUE(opened) << opened.error().message;
    const auto& engine = *opened;
    if (mode.starts_with("Failure")) {
        faults->AddRule({EIOOp::SyncFile, "*.metadata.json", 1, 1, std::make_error_code(std::errc::io_error), {}});
    }
    vfs.Arm(mode.starts_with("Binding") ? 1 : 2, mode.starts_with("Fence") || afterUnlink);
    const auto unknown = engine->AppendDurable(SecondRequest());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(unknown.recovery);
    if (mode.starts_with("Fence")) {
        vfs.Arm(1, afterUnlink);
        EXPECT_FALSE(engine->Reconcile());
    }
    EXPECT_FALSE(engine->GetHealth().read_available);
    EXPECT_FALSE(engine->GetHealth().write_ready);
    const auto parent = std::filesystem::path(root).parent_path();
    WriteBytes(parent / "second.outcome", "");
    const auto& candidate = test::Present(unknown.recovery).candidate_snapshot_id;
    WriteBytes(parent / "candidate.snapshot", candidate ? std::to_string(test::Present(candidate)) : "");
    if (mode.starts_with("Failure") && afterUnlink) {
        auto error = test::Present(unknown.recovery).first_error;
        error.retry_hint = RetryHint::OperatorAction;
        AppendOutcome failed{SecondOperation,
            CommitDisposition::DefinitelyNotCommitted,
            CommitPhase::Data,
            {},
            std::move(error),
            {}};
        ASSERT_NO_FATAL_FAILURE(SaveOutcome(parent / "second.outcome", failed));
    }
    std::_Exit(::testing::Test::HasFailure() ? 74 : 73);
}

class TProcessCrashStorageFaultTest
    : public TStorageFaultTest
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TProcessCrashStorageFaultTest, FreshProcessResolvesAbruptExitAtUncertainBoundary)
{
    const std::string_view mode(GetParam());
    ASSERT_NO_FATAL_FAILURE(Close());
    ASSERT_NO_FATAL_FAILURE(SaveOutcome(Root_.parent_path() / "first.outcome", First_));
    ASSERT_NO_FATAL_FAILURE(RunChild(Root_,
        {.Test = "StorageFaultCrashChild.ExitsWithoutShutdownOrReconciliation", .State = mode, .ExpectedExit = 73}));
    const bool committed = mode.starts_with("Fence") || (mode.starts_with("Publication") && mode.ends_with("After"));
    const bool absent = mode == "BindingBefore";
    RunChild(Root_,
        {.Test = "StorageFaultRecoveryChild.ReopensWithProductionDefaults",
            .State = absent ? "absent"
                : committed ? "1"
                            : "0"});
}

INSTANTIATE_TEST_SUITE_P(Process,
    TProcessCrashStorageFaultTest,
    ::testing::Values("BindingBefore",
        "BindingAfter",
        "FailureBefore",
        "FailureAfter",
        "PublicationBefore",
        "PublicationAfter",
        "FenceBefore",
        "FenceAfter"),
    [](const ::testing::TestParamInfo<const char*>& info) {
        return info.param;
    });

} // namespace
} // namespace glamdb
