// The call that owns the writer is counted in the pending-call budget before
// its outcome lookup. Pending-call exhaustion must never produce a definite
// failure for an operation that a queued call with the same ID and content
// can still publish (COORDINATOR.md: it returns commit_unknown).

#include "core/api/binary.h"
#include "core/engine/engine_factory.h"
#include "tests/support/access_gate_vfs.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/fake_storage.h"
#include "tests/support/temp_dir.h"

#include <sqlite3.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <string_view>
#include <thread>

#include <gtest/gtest.h>

namespace glamdb {
namespace {

using namespace std::chrono_literals;

class PendingBudgetOwnerTest : public testing::Test
{
protected:
    // Declared first: destroyed after the engine closes its SQLite connection.
    AccessGateVfs vfs;
    test::TempDir temporary;
    std::filesystem::path root = temporary.path / "store";
    std::shared_ptr<test::FakeStorage> backend = std::make_shared<test::FakeStorage>(root);
    std::shared_ptr<Engine> engine;

    void SetUp() override
    {
        CreateStoreRequest request{test::store_id, root.string()};
        request.limits.max_pending_calls = 2;
        request.limits.writer_wait = 5000ms;
        auto created = detail::EngineFactory::Create(request, backend, vfs.Name());
        ASSERT_TRUE(created) << created.error().code << ": " << created.error().message;
        engine = *created;
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                      .disposition,
            CommitDisposition::Committed);
    }

    static AppendRequest Request(std::string id)
    {
        return {test::collection_id, std::move(id), {}, {test::Batch()}};
    }
};

TEST_F(PendingBudgetOwnerTest, OwnerIsNotDefinitelyRejectedWhileQueuedSameIdCommits)
{
    const std::string unrelated_id = "00000000-0000-0000-0000-000000000006";
    std::future<AppendOutcome> owner;
    std::future<AppendOutcome> retry;
    std::future<AppendOutcome> unrelated;
    ReleaseOnExit release_on_exit{vfs};

    // 1. The owner wins the free writer on the fast path (engine.cpp:850) and
    //    pauses inside its authoritative lookup (engine.cpp:898).
    vfs.Arm();
    owner = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request(test::operation_id));
    });
    ASSERT_EQ(vfs.Entered().wait_for(10s), std::future_status::ready);

    // 2. A same-ID, same-content retry and an unrelated call arrive while the
    //    owner holds the writer; together they exceed the budget of two.
    //    The retry queues first, so it holds a slot whichever way the owner
    //    is counted; only then does the unrelated call arrive.
    retry = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request(test::operation_id));
    });
    ASSERT_EQ(retry.wait_for(300ms), std::future_status::timeout);
    unrelated = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request(unrelated_id));
    });
    std::this_thread::sleep_for(100ms);

    // 3. The owner resumes and reaches the pending-budget check.
    vfs.Release();
    const auto first = owner.get();
    const auto second = retry.get();
    (void)unrelated.get();

    // Exactly one of the two same-ID calls publishes the operation.
    const auto final_outcome = engine->AppendDurable(Request(test::operation_id));
    ASSERT_EQ(final_outcome.disposition, CommitDisposition::Committed);
    EXPECT_TRUE(
        second.disposition == CommitDisposition::Committed || second.disposition == CommitDisposition::CommitUnknown);

    // Contract: pending-call exhaustion never produces a new definite failure,
    // in particular not for the owner while a same-ID call can still publish.
    EXPECT_NE(first.disposition, CommitDisposition::DefinitelyNotCommitted)
        << (first.error ? first.error->code + ": " + first.error->message : std::string());
    if (first.disposition == CommitDisposition::CommitUnknown) {
        ASSERT_TRUE(first.error.has_value());
        EXPECT_EQ(first.error.value_or(ErrorDetail{}).retry_hint, RetryHint::SameIdAfterReconcile);
    }
    if (first.disposition == CommitDisposition::Committed) {
        EXPECT_EQ(first, final_outcome);
    }
}

} // namespace
} // namespace glamdb
