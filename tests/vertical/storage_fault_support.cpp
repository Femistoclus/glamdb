#include "tests/vertical/storage_fault_support.h"

#include "core/api/codec.h"
#include "tests/support/checked.h"

#include <cerrno>
#include <cstdlib>
#include <map>
#include <spawn.h>
#include <sys/wait.h>

extern char** environ;

namespace glamdb::vertical {

using namespace std::chrono_literals;

AppendRequest FirstRequest()
{
    return {test::collection_id, FirstOperation, {}, {Batch(FirstRows)}};
}

AppendRequest SecondRequest()
{
    return {test::collection_id, SecondOperation, {}, {Batch(SecondRows)}};
}

const std::vector<std::string> FirstExpected{
    "duplicate|1|0.500000|true|ts:10",
    "duplicate|1|0.500000|true|ts:10",
    "other|null|null|null|null",
};

std::vector<std::string> ExpectedRows(bool secondCommitted)
{
    auto rows = FirstExpected;
    if (secondCommitted) {
        rows.emplace_back("duplicate|2|-1.500000|false|ts:20");
        rows.emplace_back("ключ|3|2.500000|true|ts:30");
    }
    return Sorted(std::move(rows));
}

void ExpectScan(const std::shared_ptr<Engine>& engine,
    SnapshotSelector selector,
    const std::vector<std::string>& expected)
{
    const auto cursor = engine->Scan({.collection_id = test::collection_id, .selector = selector});
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
    EXPECT_EQ(Sorted(std::move(rows)), expected);
}

void ExpectRows(const std::shared_ptr<Engine>& engine, bool secondCommitted)
{
    ExpectScan(engine, SnapshotSelector::Latest(), ExpectedRows(secondCommitted));
}

void ExpectHistory(const std::shared_ptr<Engine>& engine, const AppendOutcome& first, bool secondCommitted)
{
    const auto history = engine->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    ASSERT_EQ(history->snapshots.size(), secondCommitted ? 2 : 1);
    ASSERT_TRUE(first.result);
    const auto& firstReceipt = test::Present(first.result);
    EXPECT_EQ(history->snapshots.front().receipt, firstReceipt);
    EXPECT_EQ(history->snapshots.front().snapshot_id, firstReceipt.snapshot_id);
    EXPECT_FALSE(history->snapshots.front().parent_snapshot_id);
    ExpectScan(engine, SnapshotSelector::Id(firstReceipt.snapshot_id), FirstExpected);
    ExpectScan(engine, SnapshotSelector::InitialEmpty(), {});
    if (secondCommitted) {
        const auto second = engine->FindAppendOutcome(test::collection_id, SecondOperation);
        ASSERT_TRUE(second);
        ASSERT_TRUE(second->has_value());
        const auto& receipt = test::Present(test::Present(*second).result);
        EXPECT_EQ(receipt.operation_id, SecondOperation);
        EXPECT_EQ(receipt.collection_id, test::collection_id);
        EXPECT_EQ(receipt.rows_appended, 2);
        EXPECT_EQ(receipt.data_files_added, 1);
        EXPECT_GT(receipt.compressed_data_bytes_added, 0);
        EXPECT_EQ(receipt.parent_snapshot_id, firstReceipt.snapshot_id);
        EXPECT_EQ(history->snapshots.back().receipt, receipt);
        EXPECT_EQ(history->snapshots.back().snapshot_id, receipt.snapshot_id);
        EXPECT_EQ(history->snapshots.back().parent_snapshot_id, firstReceipt.snapshot_id);
        EXPECT_EQ(history->snapshots.back().created_at_us, receipt.snapshot_created_at_us);
        ExpectScan(engine, SnapshotSelector::Id(receipt.snapshot_id), ExpectedRows(true));
    }
}

void SaveOutcome(const std::filesystem::path& path, const AppendOutcome& outcome)
{
    const auto encoded = detail::EncodeOutcome(outcome);
    ASSERT_TRUE(encoded) << encoded.error().message;
    WriteBytes(path, *encoded);
    EXPECT_EQ(ReadBytes(path), *encoded);
}

std::map<std::filesystem::path, std::string> CaptureWarehouse(const std::filesystem::path& root)
{
    std::map<std::filesystem::path, std::string> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        if (entry.is_regular_file()) {
            files.emplace(entry.path().lexically_relative(root), ReadBytes(entry.path()));
        }
    }
    return files;
}

void ExpectRetries(const std::shared_ptr<Engine>& engine, const AppendOutcome& expected)
{
    EXPECT_EQ(engine->AppendDurable(SecondRequest()), expected);
    auto conflicting = SecondRequest();
    conflicting.batches = {Batch(FirstRows)};
    const auto conflict = engine->AppendDurable(conflicting);
    ASSERT_TRUE(conflict.error);
    EXPECT_EQ(test::Present(conflict.error).code, "OPERATION_CONFLICT");
}

TEST(StorageFaultRecoveryChild, ReopensWithProductionDefaults)
{
    const auto* root = std::getenv("GLAMDB_STORAGE_FAULT_ROOT");
    const auto* committed = std::getenv("GLAMDB_STORAGE_FAULT_COMMITTED");
    if (!root || !committed) {
        GTEST_SKIP() << "Invoked by the storage fault test in a fresh process.";
    }
    const bool secondCommitted = std::string_view(committed) == "1";
    const bool secondAbsent = std::string_view(committed) == "absent";
    const auto first = detail::DecodeOutcome(ReadBytes(std::filesystem::path(root).parent_path() / "first.outcome"));
    ASSERT_TRUE(first) << first.error().message;
    const auto before = CaptureWarehouse(root);
    const auto opened = OpenExisting({root, test::store_id});
    ASSERT_TRUE(opened) << opened.error().message;
    EXPECT_EQ(CaptureWarehouse(root), before);
    const auto& engine = *opened;
    EXPECT_TRUE(engine->GetHealth().write_ready);
    ExpectRows(engine, secondCommitted);
    ExpectHistory(engine, *first, secondCommitted);
    const auto acknowledged = engine->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(acknowledged);
    ASSERT_TRUE(acknowledged->has_value());
    EXPECT_EQ(test::Present(*acknowledged), *first);
    EXPECT_EQ(engine->AppendDurable(FirstRequest()), *first);
    const auto outcome = engine->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(outcome) << outcome.error().message;
    if (secondAbsent) {
        EXPECT_FALSE(outcome->has_value());
        EXPECT_EQ(CaptureWarehouse(root), before);
        const auto retried = engine->AppendDurable(SecondRequest());
        ASSERT_EQ(retried.disposition, CommitDisposition::Committed);
        const auto afterAppend = CaptureWarehouse(root);
        ExpectRetries(engine, retried);
        EXPECT_EQ(CaptureWarehouse(root), afterAppend);
        ExpectRows(engine, true);
        ExpectHistory(engine, *first, true);
        EXPECT_TRUE(engine->Shutdown(1000ms));
        return;
    }
    ASSERT_TRUE(outcome->has_value());
    EXPECT_EQ(test::Present(*outcome).disposition,
        secondCommitted ? CommitDisposition::Committed : CommitDisposition::DefinitelyNotCommitted);
    const auto expectedPath = std::filesystem::path(root).parent_path() / "second.outcome";
    const auto expectedBytes = ReadBytes(expectedPath);
    if (!expectedBytes.empty()) {
        const auto expected = detail::DecodeOutcome(expectedBytes);
        ASSERT_TRUE(expected) << expected.error().message;
        EXPECT_EQ(test::Present(*outcome), *expected);
    } else if (secondCommitted) {
        const auto snapshot = ReadBytes(std::filesystem::path(root).parent_path() / "candidate.snapshot");
        ASSERT_FALSE(snapshot.empty());
        EXPECT_EQ(test::Present(test::Present(*outcome).result).snapshot_id, std::stoll(snapshot));
    } else {
        EXPECT_EQ(test::Present(test::Present(*outcome).error).code, "INTERRUPTED");
    }
    ExpectRetries(engine, test::Present(*outcome));
    ExpectRows(engine, secondCommitted);
    ExpectHistory(engine, *first, secondCommitted);
    EXPECT_EQ(CaptureWarehouse(root), before);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

void RunChild(const std::filesystem::path& root, TChildInvocation invocation)
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
    environment.push_back("GLAMDB_STORAGE_FAULT_COMMITTED=" + std::string(invocation.State));
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=" + std::string(invocation.Test);
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
    ASSERT_EQ(WEXITSTATUS(status), invocation.ExpectedExit);
#else
    static_cast<void>(root);
    static_cast<void>(invocation);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

void ExpectRecoveryInFreshProcess(const std::filesystem::path& root,
    ESecondOutcome state,
    const AppendOutcome& first,
    const AppendOutcome& second)
{
    ASSERT_NO_FATAL_FAILURE(SaveOutcome(root.parent_path() / "first.outcome", first));
    if (second.disposition == CommitDisposition::CommitUnknown) {
        WriteBytes(root.parent_path() / "second.outcome", "");
        ASSERT_TRUE(second.recovery);
        const auto& candidate = test::Present(second.recovery).candidate_snapshot_id;
        WriteBytes(root.parent_path() / "candidate.snapshot",
            candidate ? std::to_string(test::Present(candidate)) : "");
    } else {
        ASSERT_NO_FATAL_FAILURE(SaveOutcome(root.parent_path() / "second.outcome", second));
    }
    RunChild(root,
        {.Test = "StorageFaultRecoveryChild.ReopensWithProductionDefaults",
            .State = state == ESecondOutcome::Absent ? "absent"
                : state == ESecondOutcome::Committed ? "1"
                                                     : "0"});
}

} // namespace glamdb::vertical
