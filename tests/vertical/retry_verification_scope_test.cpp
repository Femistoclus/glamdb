// An idempotent retry verifies only what its own outcome depends on: a
// committed append checks its collection's root and that snapshot's files,
// not other collections or every retained manifest list; a failed outcome
// references no storage and reads none.

#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <format>
#include <set>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
namespace NT = NGlamDB::NTesting;

std::string B1Operation(int index)
{
    return std::format("00000000-0000-4000-8000-{:012x}", 0xb100 + index);
}

class BackendRetryScopeTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    // Declared before the engine so the pass-through recorder outlives it.
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<NT::TFaultFS> Faults_ = std::make_shared<NT::TFaultFS>(Posix_);
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        detail::TLocalStorageOptions options;
        options.Fs = Faults_;
        auto created =
            detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, THookBackend::Factory(Hooks_, options));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events-a", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "events-b", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
            Engine_.reset();
        }
    }

    AppendOutcome Append(std::string_view collection,
        const std::string& operation,
        const std::vector<TInputRow>& rows,
        SnapshotPrecondition expected = {})
    {
        return Engine_->AppendDurable({std::string(collection), operation, expected, {Batch(rows)}});
    }

    // Warehouse files opened for reading since event index `mark`.
    std::vector<std::string> OpenedSince(std::size_t mark) const
    {
        const auto events = Faults_->Events();
        std::vector<std::string> paths;
        for (std::size_t index = mark; index < events.size(); ++index) {
            if (events[index].Op == NT::EIOOp::OpenRead && events[index].Path.find("/warehouse/") != std::string::npos)
            {
                paths.push_back(events[index].Path);
            }
        }
        return paths;
    }
};

// A retry in collection A returns A's receipt; collection B's files are irrelevant
// to that outcome, yet VerifyRetained reopens B's root, manifest list, manifest and
// Parquet footer (local_storage_backend.cpp:720-735 loops over every collection).
TEST_F(BackendRetryScopeTest, RetryOfCommittedAppendDoesNotVerifyOtherCollections)
{
    const auto first = Append(test::collection_id, B1Operation(0), FirstRows);
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    ASSERT_EQ(Append(SecondCollection, B1Operation(1), SecondRows).disposition, CommitDisposition::Committed);

    const auto mark = Faults_->Events().size();
    const auto retry = Append(test::collection_id, B1Operation(0), FirstRows);
    ASSERT_EQ(retry.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(retry.result && first.result);
    EXPECT_EQ(test::Present(retry.result).snapshot_id, test::Present(first.result).snapshot_id);

    const auto foreign = "/warehouse/" + std::string(SecondCollection) + "/";
    std::size_t foreignReads = 0;
    for (const auto& path : OpenedSince(mark)) {
        foreignReads += path.find(foreign) != std::string::npos ? 1 : 0;
    }
    EXPECT_EQ(foreignReads, 0U);
}

// Within one collection, VerifyReachable reads every retained manifest list
// (base_state.cpp:317-344 -> 204-213), i.e. sum_{i<=n} i records, twice.
TEST_F(BackendRetryScopeTest, RetryOfCommittedAppendDoesNotRereadEveryManifestList)
{
    constexpr int Appends = 8;
    for (int index = 0; index < Appends; ++index) {
        ASSERT_EQ(Append(test::collection_id, B1Operation(index), FirstRows).disposition, CommitDisposition::Committed);
    }

    const auto mark = Faults_->Events().size();
    ASSERT_EQ(Append(test::collection_id, B1Operation(0), FirstRows).disposition, CommitDisposition::Committed);

    std::set<std::string> lists;
    for (const auto& path : OpenedSince(mark)) {
        if (std::filesystem::path(path).filename().string().starts_with("snap-")) {
            lists.insert(path);
        }
    }
    // Today: all `Appends` lists. The retried snapshot's list (plus, at most, the
    // current one) is all a committed receipt depends on.
    EXPECT_LE(lists.size(), 2U);
}

// A failed terminal outcome references no storage file, yet its retry runs the
// same full verification (engine.cpp:910-921 does not distinguish outcomes).
TEST_F(BackendRetryScopeTest, RetryOfFailedAppendReadsNoWarehouseFile)
{
    ASSERT_EQ(Append(test::collection_id, B1Operation(0), FirstRows).disposition, CommitDisposition::Committed);
    ASSERT_EQ(Append(SecondCollection, B1Operation(1), SecondRows).disposition, CommitDisposition::Committed);

    // Bound first, then rejected by the precondition: a permanent failed outcome.
    const SnapshotPrecondition stale{SnapshotPreconditionKind::Equals, 1};
    const auto failed = Append(test::collection_id, B1Operation(2), SecondRows, stale);
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(failed.error);
    ASSERT_EQ(test::Present(failed.error).code, "PRECONDITION_FAILED");

    const auto mark = Faults_->Events().size();
    const auto retry = Append(test::collection_id, B1Operation(2), SecondRows, stale);
    ASSERT_EQ(retry.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(retry.error);
    EXPECT_EQ(test::Present(retry.error).code, "PRECONDITION_FAILED");
    EXPECT_TRUE(OpenedSince(mark).empty());
}

} // namespace
} // namespace glamdb
