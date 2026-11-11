// The call that owns the writer is counted in the pending-call budget before
// its outcome lookup. Pending-call exhaustion must never produce a definite
// failure for an operation that a queued call with the same ID and content
// can still publish (COORDINATOR.md: it returns commit_unknown).

#include "core/api/binary.h"
#include "core/engine/engine_factory.h"
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

// Real SQLite VFS delegation. When armed, the next existence check of a
// rollback journal blocks until released. In DELETE journal mode SQLite makes
// that check at the start of every read transaction (hot-journal probe), so
// arming it right before an append pauses the writer inside its
// authoritative FindOperation, after it acquired the writer lock.
class AccessGateVfs
{
public:
    AccessGateVfs()
        : parent_(sqlite3_vfs_find(nullptr))
    {
        detail::Require(parent_ != nullptr, "INTERNAL", "default VFS is missing");
        // Retain the parent's pAppData: its other callbacks use it.
        vfs_ = *parent_;
        vfs_.pNext = nullptr;
        vfs_.zName = name_.c_str();
        vfs_.xAccess = &AccessGateVfs::Access;
        instance_.store(this);
        detail::Require(sqlite3_vfs_register(&vfs_, 0) == SQLITE_OK, "INTERNAL", "cannot register gate VFS");
    }

    ~AccessGateVfs()
    {
        Release();
        sqlite3_vfs_unregister(&vfs_);
        instance_.store(nullptr);
    }

    AccessGateVfs(const AccessGateVfs&) = delete;
    AccessGateVfs& operator=(const AccessGateVfs&) = delete;

    const std::string& Name() const
    {
        return name_;
    }

    void Arm()
    {
        armed_.store(true);
    }

    std::shared_future<void> Entered() const
    {
        return entered_future_;
    }

    void Release()
    {
        if (!released_.exchange(true)) {
            release_.set_value();
        }
    }

private:
    static int Access(sqlite3_vfs*, const char* path, int flags, int* result)
    {
        auto* self = instance_.load();
        if (self != nullptr && path != nullptr && std::string_view(path).ends_with("-journal") &&
            self->armed_.exchange(false))
        {
            self->entered_.set_value();
            self->release_future_.wait();
        }
        sqlite3_vfs* parent = self != nullptr ? self->parent_ : sqlite3_vfs_find(nullptr);
        return parent->xAccess(parent, path, flags, result);
    }

    static inline std::atomic<AccessGateVfs*> instance_{nullptr};

    std::string name_ = "glamdb-access-gate";
    sqlite3_vfs* parent_ = nullptr;
    sqlite3_vfs vfs_{};
    std::atomic<bool> armed_{false};
    std::atomic<bool> released_{false};
    std::promise<void> entered_;
    std::shared_future<void> entered_future_ = entered_.get_future().share();
    std::promise<void> release_;
    std::shared_future<void> release_future_ = release_.get_future().share();
};

// Releases the gate before any pending future is joined, so a failed
// assertion cannot deadlock the test on a paused writer.
struct ReleaseOnExit
{
    AccessGateVfs& vfs;
    ~ReleaseOnExit()
    {
        vfs.Release();
    }
};

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
