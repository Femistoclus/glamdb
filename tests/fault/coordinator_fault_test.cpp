#include "core/api/binary.h"
#include "core/api/codec.h"
#include "core/engine/engine_factory.h"
#include "tests/support/arrow_batch.h"
#include "tests/support/checked.h"
#include "tests/support/fake_storage.h"
#include "tests/support/journal_fault_vfs.h"
#include "tests/support/temp_dir.h"

#include <future>
#include <gtest/gtest.h>

namespace glamdb {
namespace {

class CoordinatorFaultTest : public testing::Test
{
protected:
    test::JournalFaultVfs vfs;
    test::TempDir temporary;
    std::filesystem::path root = temporary.path / "store";
    std::shared_ptr<test::FakeStorage> backend = std::make_shared<test::FakeStorage>(root);
    std::shared_ptr<Engine> engine;
    void SetUp() override
    {
        const auto created = detail::EngineFactory::Create({test::store_id, root.string()}, backend, vfs.Name());
        ASSERT_TRUE(created);
        engine = *created;
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", SchemaSpec{{{"n", FieldType::Int64, true}}}})
                      .disposition,
            CommitDisposition::Committed);
    }
    AppendRequest Request(std::string id = test::operation_id) const
    {
        return {test::collection_id, std::move(id), {}, {test::Batch()}};
    }
};

TEST_F(CoordinatorFaultTest, LostOuterCommitResultBlocksWritersAndFenceFailureKeepsBarrier)
{
    vfs.Arm(2, true); // binding succeeds; final publication unlinks journal,
                      // then reports I/O failure.
    const auto unknown = engine->AppendDurable(Request());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::CatalogCommit);
    EXPECT_EQ(engine->GetHealth().state, StoreState::RecoveryRequired);
    EXPECT_FALSE(engine->GetHealth().read_available);
    EXPECT_FALSE(engine->GetHealth().write_ready);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_TRUE(test::Present(unknown.recovery).publication_started);
    EXPECT_EQ(test::Present(engine->FindAppendOutcome(test::collection_id, test::operation_id).value()).disposition,
        CommitDisposition::CommitUnknown);
    EXPECT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::CommitUnknown);
    auto different = Request();
    different.batches = {test::Batch({5})};
    EXPECT_EQ(test::Present(engine->AppendDurable(different).error).code, "OPERATION_CONFLICT");
    EXPECT_EQ(test::Present(engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006")).error).code,
        "RECOVERY_REQUIRED");
    EXPECT_EQ(backend->prepares, 1);
    vfs.Arm(1, true); // Even an apparently committed fence with a lost result
                      // cannot open readiness.
    EXPECT_FALSE(engine->Reconcile());
    EXPECT_EQ(test::Present(engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006")).error).code,
        "RECOVERY_REQUIRED");
    vfs.Disarm();
    ASSERT_TRUE(engine->Reconcile());
    const auto outcome = engine->FindAppendOutcome(test::collection_id, test::operation_id);
    ASSERT_TRUE(outcome);
    ASSERT_TRUE(outcome->has_value());
    EXPECT_EQ(test::Present(outcome.value()).disposition, CommitDisposition::Committed);
    EXPECT_EQ(engine->AppendDurable(Request()), test::Present(outcome.value()));
    EXPECT_EQ(backend->prepares, 1);
}

TEST_F(CoordinatorFaultTest, OutcomeLookupDoesNotWaitForAnotherPublication)
{
    const auto first = engine->AppendDurable(Request());
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    vfs.PauseBeforeDelete(2);
    auto writer = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006"));
    });
    const auto entered = vfs.WaitUntilPaused(std::chrono::seconds(5));
    auto lookup = std::async(std::launch::async, [&] {
        return engine->FindAppendOutcome(test::collection_id, test::operation_id);
    });
    const auto ready = lookup.wait_for(std::chrono::milliseconds(250));
    const auto active = engine->FindAppendOutcome(test::collection_id, "00000000-0000-0000-0000-000000000006");
    vfs.Release();
    const auto result = lookup.get();
    EXPECT_TRUE(entered);
    EXPECT_EQ(ready, std::future_status::ready);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "OPERATION_IN_PROGRESS");
    ASSERT_FALSE(active);
    EXPECT_EQ(active.error().retry_hint, RetryHint::SameIdAfterReconcile);
    EXPECT_EQ(writer.get().disposition, CommitDisposition::Committed);
    EXPECT_EQ(test::Present(engine->FindAppendOutcome(test::collection_id, test::operation_id).value()), first);
}

TEST_F(CoordinatorFaultTest, UnknownBindingHasNoDataPublicationAndResolvesWithoutReplay)
{
    vfs.Arm(1, true);
    const auto unknown = engine->AppendDurable(Request());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::Binding);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_FALSE(test::Present(unknown.recovery).publication_started);
    EXPECT_EQ(backend->prepares, 0);
    ASSERT_TRUE(engine->Reconcile());
    const auto failed = engine->FindAppendOutcome(test::collection_id, test::operation_id);
    ASSERT_TRUE(failed);
    ASSERT_TRUE(failed->has_value());
    EXPECT_EQ(test::Present(failed.value()).disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(engine->AppendDurable(Request()), test::Present(failed.value()));
    EXPECT_EQ(backend->prepares, 0);
}

TEST_F(CoordinatorFaultTest, BindingRolledBackBeforeJournalDeleteLeavesIdReusable)
{
    vfs.Arm(1, false);
    const auto unknown = engine->AppendDurable(Request());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(engine->Reconcile());
    ASSERT_FALSE(engine->FindAppendOutcome(test::collection_id, test::operation_id)->has_value());
    EXPECT_EQ(engine->AppendDurable(Request()).disposition, CommitDisposition::Committed);
    EXPECT_EQ(backend->prepares, 1);
}

TEST_F(CoordinatorFaultTest, LostFailureRecordingResultRetainsOriginalProducerFailure)
{
    backend->fail_prepare = true;
    vfs.Arm(2, true);
    const auto unknown = engine->AppendDurable(Request());
    ASSERT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(unknown.phase, CommitPhase::RecordingFailure);
    ASSERT_TRUE(unknown.recovery);
    EXPECT_FALSE(test::Present(unknown.recovery).publication_started);
    EXPECT_EQ(test::Present(unknown.recovery).first_error.message, "first producer error");
    ASSERT_TRUE(engine->Reconcile());
    backend->fail_prepare = false;
    const auto failed = engine->AppendDurable(Request());
    ASSERT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_EQ(failed.phase, CommitPhase::Data);
    EXPECT_EQ(test::Present(failed.error).message, "first producer error");
    EXPECT_EQ(backend->prepares, 1);
}

TEST_F(CoordinatorFaultTest, LongUtf8FailureSurvivesLostRecordingResultAndLiveReconcile)
{
    const std::string prefix(2047, 'x');
    for (const bool committed : {false, true}) {
        const auto request = Request(committed ? "00000000-0000-0000-0000-000000000006" : test::operation_id);
        backend->on_prepare = [&] {
            throw detail::Failure({"IO_ERROR", prefix + "я", RetryHint::OperatorAction});
        };
        vfs.Arm(2, committed);
        const auto unknown = engine->AppendDurable(request);
        backend->on_prepare = {};
        vfs.Disarm();
        EXPECT_EQ(unknown.disposition, CommitDisposition::CommitUnknown);
        EXPECT_EQ(unknown.phase, CommitPhase::RecordingFailure);
        EXPECT_TRUE(detail::ValidateOutcome(unknown));
        EXPECT_EQ(test::Present(unknown.recovery).first_error.message, prefix);
        ASSERT_TRUE(engine->Reconcile());
        const auto failed = engine->AppendDurable(request);
        EXPECT_EQ(failed.disposition, CommitDisposition::DefinitelyNotCommitted);
        EXPECT_EQ(failed.phase, CommitPhase::Data);
        EXPECT_EQ(test::Present(failed.error).code, "IO_ERROR");
        EXPECT_EQ(test::Present(failed.error).message, prefix);
        EXPECT_TRUE(detail::ValidateOutcome(failed));
        EXPECT_EQ(engine->AppendDurable(request), failed);
    }
    EXPECT_EQ(backend->prepares, 2);
}

TEST_F(CoordinatorFaultTest, RecoveryBarrierCannotReportAnOldCommittedIdAsUncommitted)
{
    const auto first = engine->AppendDurable(Request());
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    vfs.Arm(2, true);
    const auto second = engine->AppendDurable(Request("00000000-0000-0000-0000-000000000006"));
    ASSERT_EQ(second.disposition, CommitDisposition::CommitUnknown);
    const auto retry = engine->AppendDurable(Request());
    EXPECT_EQ(retry.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(retry.phase, CommitPhase::Reconciling);
    EXPECT_EQ(retry.operation_id, test::operation_id);
    EXPECT_EQ(backend->prepares, 2);
    ASSERT_TRUE(engine->Reconcile());
    EXPECT_EQ(engine->AppendDurable(Request()), first);
}

TEST_F(CoordinatorFaultTest, WaitingRetryObservesBarrierClosedByAnotherWriter)
{
    const auto first = engine->AppendDurable(Request());
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    backend->on_prepare = [&] {
        entered.set_value();
        released.wait();
    };
    vfs.Arm(2, true);
    const auto second_id = "00000000-0000-0000-0000-000000000006";
    auto write = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request(second_id));
    });
    entered.get_future().wait();
    auto retry = std::async(std::launch::async, [&] {
        return engine->AppendDurable(Request());
    });
    EXPECT_EQ(retry.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout);
    release.set_value();
    const auto second = write.get();
    const auto blocked = retry.get();
    EXPECT_EQ(second.disposition, CommitDisposition::CommitUnknown);
    EXPECT_EQ(blocked.disposition, CommitDisposition::CommitUnknown);
    ASSERT_TRUE(blocked.recovery);
    EXPECT_TRUE(test::Present(blocked.recovery).lookup_blocked);
    EXPECT_EQ(blocked.operation_id, test::operation_id);
    EXPECT_EQ(test::Present(test::Present(engine->FindAppendOutcome(test::collection_id, second_id).value()).recovery)
                  .operation_id,
        second_id);
    ASSERT_TRUE(engine->Reconcile());
    backend->on_prepare = {};
    EXPECT_EQ(engine->AppendDurable(Request()), first);
    EXPECT_EQ(backend->prepares, 2);
}

TEST_F(CoordinatorFaultTest, UncertainCreateRetainsItsIdentityAndOutcomeUntilReconcile)
{
    for (const bool committed : {false, true}) {
        const CreateCollectionRequest request{committed ? "00000000-0000-0000-0000-000000000007"
                                                        : "00000000-0000-0000-0000-000000000006",
            committed ? "after-unlink" : "before-unlink",
            SchemaSpec{{{"n", FieldType::Int64, true}}}};
        vfs.Arm(1, committed);
        const auto first = engine->CreateCollection(request);
        ASSERT_EQ(first.disposition, CommitDisposition::CommitUnknown);
        const auto schema = engine->SchemaForAppend(request.collection_id);
        ASSERT_FALSE(schema);
        EXPECT_EQ(schema.error().code, "RECOVERY_REQUIRED");
        const auto repeated = engine->CreateCollection(request);
        EXPECT_EQ(repeated.disposition, CommitDisposition::CommitUnknown);
        EXPECT_EQ(repeated.phase, first.phase);
        EXPECT_EQ(repeated.error, first.error);
        EXPECT_EQ(test::Present(repeated.error).retry_hint, RetryHint::SameIdAfterReconcile);
        const auto& initial = test::Present(first.recovery);
        const auto& retry = test::Present(repeated.recovery);
        EXPECT_EQ(retry.store_id, initial.store_id);
        EXPECT_EQ(retry.collection_id, initial.collection_id);
        EXPECT_EQ(retry.exact_name, initial.exact_name);
        EXPECT_EQ(retry.schema_hash, initial.schema_hash);
        EXPECT_EQ(retry.attempt_id, initial.attempt_id);
        EXPECT_EQ(retry.candidate_table_uuid, initial.candidate_table_uuid);
        EXPECT_EQ(retry.candidate_metadata_location, initial.candidate_metadata_location);
        EXPECT_EQ(retry.publication_started, initial.publication_started);
        auto different = request;
        different.schema.fields[0].nullable = false;
        EXPECT_EQ(test::Present(engine->CreateCollection(different).error).code, "SCHEMA_MISMATCH");
        EXPECT_EQ(test::Present(engine->CreateCollection(request).recovery).attempt_id, initial.attempt_id);
        const CreateCollectionRequest unrelated{"00000000-0000-0000-0000-000000000009", "unrelated", request.schema};
        const auto blocked_other = engine->CreateCollection(unrelated);
        EXPECT_EQ(blocked_other.disposition, CommitDisposition::CommitUnknown);
        EXPECT_EQ(test::Present(blocked_other.recovery).collection_id, unrelated.collection_id);
        EXPECT_EQ(test::Present(engine->CreateCollection(request).recovery).attempt_id, initial.attempt_id);
        vfs.Disarm();
        ASSERT_TRUE(engine->Reconcile());
        if (!committed) {
            EXPECT_FALSE(engine->GetCollection(request.collection_id));
        }
        const auto resolved = engine->CreateCollection(request);
        EXPECT_EQ(resolved.disposition, CommitDisposition::Committed);
        EXPECT_EQ(test::Present(resolved.info).collection_id, request.collection_id);
        EXPECT_EQ(test::Present(resolved.info).schema, request.schema);
    }
}

} // namespace
} // namespace glamdb
