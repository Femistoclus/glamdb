#include "core/catalog/sqlite.h"
#include "core/engine/storage_mapping.h"
#include "storage/resources.h"
#include "storage/write_session.h"
#include "tests/support/staging_fault_vfs.h"
#include "tests/vertical/vertical_support.h"

namespace glamdb {
namespace {
using namespace vertical;
TEST(WriteSessions, DurablePartsReplayAndPublishExactlyOnce)
{
    test::TempDir temp;
    auto engine = CreateStore({test::store_id, (temp.path / "store").string()});
    ASSERT_TRUE(engine);
    ASSERT_EQ((*engine)->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);
    auto opened = TWriteSessions::Open(*engine, (temp.path / "stage").string(), "alice");
    ASSERT_TRUE(opened);
    auto sessions = *opened;
    const TWriteIdentity id{test::collection_id, FirstOperation};
    ASSERT_TRUE(sessions->BeginWrite({id}));
    int calls = 0;
    auto put = sessions->PutPart(id, {2, 1, 0}, [&]() -> Result<std::shared_ptr<arrow::RecordBatch>> {
        if (calls++ == 0) {
            return Batch({{"duplicate", 2, -1.5, false, 20}});
        }
        return nullptr;
    });
    ASSERT_TRUE(put);
    EXPECT_EQ(calls, 2);
    ASSERT_TRUE(
        sessions->PutPart(id, {1, 1, 0}, [done = false]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
            if (std::exchange(done, true)) {
                return nullptr;
            }
            return Batch({{"duplicate", 1, 0.5, true, 10}, {"duplicate", 1, 0.5, true, 10}});
        }));
    auto sealed = sessions->SealWrite(id, {{2, 1, 0}, {1, 1, 0}});
    ASSERT_TRUE(sealed);
    EXPECT_EQ(sealed->Rows, 3);
    auto committed = sessions->CommitWrite(id);
    ASSERT_TRUE(committed);
    ASSERT_EQ(committed->disposition, CommitDisposition::Committed);
    auto again = sessions->CommitWrite(id);
    ASSERT_TRUE(again);
    EXPECT_EQ(*again, *committed);
    const auto originalReplay = (*engine)->AppendDurable({test::collection_id,
        FirstOperation,
        {},
        {Batch(
            {{"duplicate", 1, 0.5, true, 10}, {"duplicate", 1, 0.5, true, 10}, {"duplicate", 2, -1.5, false, 20}})}});
    EXPECT_EQ(originalReplay, *committed);
    EXPECT_EQ((*engine)->GetStatistics(test::collection_id)->rows.value, 3);
}
}
}

namespace glamdb {
namespace {
using namespace vertical;
TWriteSessions::TNext Once(std::vector<TInputRow> rows)
{
    return [rows = std::move(rows), done = false]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
        if (std::exchange(done, true)) {
            return nullptr;
        }
        return Batch(rows);
    };
}
class WriteSession : public ::testing::Test
{
protected:
    test::TempDir Temp;
    std::shared_ptr<Engine> Store;
    std::shared_ptr<TWriteSessions> Sessions;
    TWriteIdentity ID{test::collection_id, FirstOperation};
    void SetUp() override
    {
        auto engine = CreateStore({test::store_id, (Temp.path / "store").string()});
        ASSERT_TRUE(engine);
        Store = *engine;
        ASSERT_EQ(Store->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        auto session = TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice");
        ASSERT_TRUE(session) << (session ? "" : session.error().message);
        Sessions = *session;
    }
    void Seal()
    {
        ASSERT_TRUE(Sessions->BeginWrite({ID}));
        ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"a", 1, 0.5, true, 10}})));
        ASSERT_TRUE(Sessions->SealWrite(ID, {{0, 0, 0}}));
    }
    std::vector<std::string> Rows()
    {
        auto cursor = Store->Scan({.collection_id = test::collection_id});
        EXPECT_TRUE(cursor);
        std::vector<std::string> result;
        if (!cursor) {
            return result;
        }
        for (;;) {
            auto next = (*cursor)->Next();
            EXPECT_TRUE(next);
            if (!next || !*next) {
                break;
            }
            auto rows = Render(**next);
            result.insert(result.end(), rows.begin(), rows.end());
        }
        return Sorted(std::move(result));
    }
};
TEST_F(WriteSession, CanonicalPartRetryIgnoresBatchBoundariesAndRejectsChanges)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    const std::vector<TInputRow> rows{{"key", INT64_MAX, -0.0, true, INT64_C(9007199254740993)},
        {"null", {}, {}, {}, {}}};
    auto original = Sessions->PutPart(ID, {3, 2, 1}, Once(rows));
    ASSERT_TRUE(original);
    auto split = Sessions->PutPart(ID,
        {3, 2, 1},
        [rows, i = size_t{0}]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
            if (i == rows.size()) {
                return nullptr;
            }
            return Batch({rows[i++]});
        });
    ASSERT_TRUE(split);
    EXPECT_EQ(split->CanonicalHash, original->CanonicalHash);
    auto conflict = Sessions->PutPart(ID, {3, 2, 1}, Once({{"changed", 1, {}, {}, {}}}));
    ASSERT_FALSE(conflict);
    EXPECT_EQ(conflict.error().code, "IDEMPOTENCY_CONFLICT");
    ASSERT_TRUE(Sessions->SealWrite(ID, {{3, 2, 1}}));
    ASSERT_TRUE(Sessions->CommitWrite(ID));
    EXPECT_EQ(Rows(),
        (std::vector<std::string>{"key|9223372036854775807|0.000000|true|ts:9007199254740993",
            "null|null|null|null|null"}));
}
TEST_F(WriteSession, SelectionRejectsMixedAttemptsAndIncludesOnlyChosenParts)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"loser", 0, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 1, 0}, Once({{"winner", 1, {}, {}, {}}})));
    EXPECT_FALSE(Sessions->SealWrite(ID, {{0, 0, 0}, {0, 1, 0}}));
    EXPECT_FALSE(Sessions->SealWrite(ID, {{0, 1, 0}, {0, 1, 0}}));
    ASSERT_TRUE(Sessions->SealWrite(ID, {{0, 1, 0}}));
    EXPECT_FALSE(Sessions->SealWrite(ID, {{0, 0, 0}}));
    ASSERT_TRUE(Sessions->CommitWrite(ID));
    EXPECT_EQ(Rows(), (std::vector<std::string>{"winner|1|null|null|null"}));
}
TEST_F(WriteSession, KvLastWinsUsesTaskPartAndRowOrderAcrossParts)
{
    const TWriteIdentity id{SecondCollection, SecondOperation};
    CreateCollectionRequest create{SecondCollection, "kv", Schema};
    create.mode = CollectionMode::KeyValue;
    ASSERT_EQ(Store->CreateCollection(create).disposition, CommitDisposition::Committed);
    ASSERT_TRUE(Sessions->BeginWrite({id, MutationKind::Upsert}));
    ASSERT_TRUE(Sessions->PutPart(id, {8, 0, 0}, Once({{"key", 4, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->PutPart(id, {1, 2, 1}, Once({{"key", 2, {}, {}, {}}, {"key", 3, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->PutPart(id, {1, 2, 0}, Once({{"key", 1, {}, {}, {}}, {"other", {}, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->SealWrite(id, {{8, 0, 0}, {1, 2, 1}, {1, 2, 0}}));
    auto outcome = Sessions->CommitWrite(id);
    ASSERT_TRUE(outcome);
    ASSERT_TRUE(outcome->result);
    EXPECT_EQ(outcome->result->input_rows, 5);
    auto cursor = Store->Scan({.collection_id = SecondCollection});
    ASSERT_TRUE(cursor);
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    ASSERT_TRUE(*next);
    EXPECT_EQ(Sorted(Render(**next)), (std::vector<std::string>{"key|4|null|null|null", "other|null|null|null|null"}));
}
TEST_F(WriteSession, CancelMalformedAndQuotaRollbackWholePart)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    auto cancel = std::make_shared<std::atomic<bool>>(false);
    auto cancelled = Sessions->PutPart(
        ID,
        {0, 0, 0},
        [cancel, first = true]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
            if (std::exchange(first, false)) {
                return Batch({{"unaccepted", 1, {}, {}, {}}});
            }
            cancel->store(true);
            return nullptr;
        },
        cancel);
    ASSERT_FALSE(cancelled);
    EXPECT_EQ(cancelled.error().code, "CANCELLED");
    auto malformed =
        Sessions->PutPart(ID, {0, 0, 0}, [first = true]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
            if (std::exchange(first, false)) {
                return Batch({{"unaccepted", 1, {}, {}, {}}});
            }
            return arrow::RecordBatch::Make(arrow::schema({}), 1, std::vector<std::shared_ptr<arrow::Array>>{});
        });
    EXPECT_FALSE(malformed);
    EXPECT_FALSE(Sessions->SealWrite(ID, {{0, 0, 0}}));
    EXPECT_FALSE(Store->GetCollection(test::collection_id)->current_snapshot_id);
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"accepted", 2, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->SealWrite(ID, {{0, 0, 0}}));
    ASSERT_TRUE(Sessions->CommitWrite(ID));
    EXPECT_EQ(Rows(), (std::vector<std::string>{"accepted|2|null|null|null"}));
}
TEST_F(WriteSession, StalePreconditionIsPermanentAndCleanupPreservesReceipt)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID, MutationKind::None, {SnapshotPreconditionKind::NoSnapshot}}));
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"stale", 1, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->SealWrite(ID, {{0, 0, 0}}));
    ASSERT_EQ(Store->AppendDurable({test::collection_id, SecondOperation, {}, {Batch({{"current", 2, {}, {}, {}}})}})
                  .disposition,
        CommitDisposition::Committed);
    auto outcome = Sessions->CommitWrite(ID);
    ASSERT_TRUE(outcome);
    EXPECT_EQ(outcome->disposition, CommitDisposition::DefinitelyNotCommitted);
    auto receipt = Sessions->GetWriteOutcome(ID);
    ASSERT_TRUE(receipt);
    ASSERT_TRUE(*receipt);
    EXPECT_EQ(**receipt, *outcome);
    ASSERT_TRUE(Sessions->Cleanup());
    auto retry = Sessions->CommitWrite(ID);
    ASSERT_TRUE(retry);
    EXPECT_EQ(*retry, *outcome);
    EXPECT_EQ(Rows(), (std::vector<std::string>{"current|2|null|null|null"}));
}
TEST_F(WriteSession, WrongBoundPayloadCannotBeMistakenForTerminalSessionOutcome)
{
    Seal();
    auto other = Store->AppendDurable({test::collection_id,
        FirstOperation,
        {SnapshotPreconditionKind::Equals, 42},
        {Batch({{"other", 2, {}, {}, {}}})}});
    ASSERT_EQ(other.disposition, CommitDisposition::DefinitelyNotCommitted);
    auto outcome = Sessions->GetWriteOutcome(ID);
    ASSERT_FALSE(outcome);
    EXPECT_EQ(outcome.error().code, "IDEMPOTENCY_CONFLICT");
    EXPECT_FALSE(Sessions->CommitWrite(ID));
    ASSERT_TRUE(Sessions->Cleanup());
    EXPECT_EQ(Sessions->GetWriteStatus(ID)->State, EWriteState::Sealed);
}
TEST_F(WriteSession, CommitCancellationRetainsUnboundSealedReplay)
{
    Seal();
    auto cancel = std::make_shared<std::atomic<bool>>(true);
    auto outcome = Sessions->CommitWrite(ID, cancel);
    ASSERT_TRUE(outcome);
    EXPECT_EQ(outcome->disposition, CommitDisposition::DefinitelyNotCommitted);
    EXPECT_FALSE(Sessions->AbortWrite(ID));
    ASSERT_TRUE(Sessions->Cleanup());
    EXPECT_EQ(Sessions->GetWriteStatus(ID)->State, EWriteState::CommitStarted);
    auto retry = Sessions->CommitWrite(ID);
    ASSERT_TRUE(retry);
    EXPECT_EQ(retry->disposition, CommitDisposition::Committed);
}
TEST_F(WriteSession, OwnershipScopePathAndUnsupportedMutationFailBeforeStaging)
{
    EXPECT_FALSE(TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice"));
    EXPECT_FALSE(TWriteSessions::Open(Store, (Temp.path / "store" / "stage").string(), "alice"));
    EXPECT_FALSE(Sessions->BeginWrite({ID, MutationKind::Delete}));
    EXPECT_FALSE(Sessions->GetWriteStatus(ID));
    Sessions.reset();
    auto wrong = TWriteSessions::Open(Store, (Temp.path / "stage").string(), "bob");
    ASSERT_FALSE(wrong);
    EXPECT_EQ(wrong.error().code, "IDENTITY_MISMATCH");
    EXPECT_TRUE(TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice"));
}
TEST_F(WriteSession, ActiveCallbackCannotBlockControlAdmission)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    bool called = false;
    auto put = Sessions->PutPart(ID, {0, 0, 0}, [&]() -> Result<std::shared_ptr<arrow::RecordBatch>> {
        if (std::exchange(called, true)) {
            return nullptr;
        }
        auto status = Sessions->GetWriteStatus(ID);
        EXPECT_FALSE(status);
        EXPECT_EQ(status.error().code, "OPERATION_IN_PROGRESS");
        auto abort = Sessions->AbortWrite(ID);
        EXPECT_FALSE(abort);
        EXPECT_EQ(abort.error().code, "OPERATION_IN_PROGRESS");
        return Batch({{"a", 1, {}, {}, {}}});
    });
    ASSERT_TRUE(put);
}
}
}

namespace glamdb {
namespace {
TEST_F(WriteSession, NativeSqliteDiskFullRollsBackUploadAndReserveProtectsCleanup)
{
    Sessions.reset();
    test::TStagingFaultVfs fault;
    auto opened = TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice");
    ASSERT_TRUE(opened);
    Sessions = *opened;
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    fault.FullOnNextWrite();
    auto part = Sessions->PutPart(ID, {0, 0, 0}, Once({{"full", 1, {}, {}, {}}}));
    ASSERT_FALSE(part);
    EXPECT_EQ(part.error().code, "CAPACITY_EXCEEDED");
    EXPECT_EQ(fault.Failures, 1);
    auto absent = Sessions->GetPartReceipt(ID, {0, 0, 0});
    ASSERT_TRUE(absent);
    EXPECT_FALSE(*absent);
    EXPECT_TRUE(std::filesystem::exists(Temp.path / "stage" / "completion.reserve"));
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"accepted", 1, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->AbortWrite(ID));
    fault.FullOnNextWrite();
    ASSERT_TRUE(Sessions->Cleanup());
    EXPECT_EQ(fault.Failures, 2);
    EXPECT_FALSE(std::filesystem::exists(Temp.path / "stage" / "completion.reserve"));
    auto unavailable = Sessions->BeginWrite({{test::collection_id, SecondOperation}});
    ASSERT_FALSE(unavailable);
    EXPECT_EQ(unavailable.error().code, "RESOURCE_EXHAUSTED");
    Sessions.reset();
    opened->reset();
}
TEST_F(WriteSession, CorruptedAcknowledgedPayloadCannotBeSealed)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"accepted", 1, {}, {}, {}}})));
    Sessions.reset();
    {
        detail::Database db;
        db.Open(Temp.path / "stage" / "staging.sqlite", SQLITE_OPEN_READWRITE, {});
        db.Execute("DELETE FROM batches");
    }
    auto opened = TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice");
    ASSERT_TRUE(opened);
    Sessions = *opened;
    auto sealed = Sessions->SealWrite(ID, {{0, 0, 0}});
    ASSERT_FALSE(sealed);
    EXPECT_EQ(sealed.error().code, "CORRUPTION");
}
TEST_F(WriteSession, TinySpoolRefusesWholePartAndReclaimedPagesAreReusable)
{
    Sessions.reset();
    TWriteSessionLimits limits;
    limits.MaxSpoolBytes = 4096;
    limits.MaxParts = 1;
    limits.MaxBatches = 2;
    auto opened = TWriteSessions::Open(Store, (Temp.path / "tiny").string(), "alice", limits);
    ASSERT_TRUE(opened);
    Sessions = *opened;
    ASSERT_TRUE(Sessions->BeginWrite({ID}));
    auto large =
        Sessions->PutPart(ID, {0, 0, 0}, Once(std::vector<TInputRow>(100, {std::string(128, 'a'), 1, {}, {}, {}})));
    ASSERT_FALSE(large);
    EXPECT_EQ(large.error().code, "RESOURCE_EXHAUSTED");
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"accepted", 1, {}, {}, {}}})));
    EXPECT_FALSE(Sessions->PutPart(ID, {1, 0, 0}, Once({{"full", 1, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->AbortWrite(ID));
    ASSERT_TRUE(Sessions->Cleanup());
    const TWriteIdentity next{test::collection_id, SecondOperation};
    ASSERT_TRUE(Sessions->BeginWrite({next}));
    ASSERT_TRUE(Sessions->PutPart(next, {0, 0, 0}, Once({{"reuse", 1, {}, {}, {}}})));
    ASSERT_TRUE(Sessions->SealWrite(next, {{0, 0, 0}}));
    ASSERT_TRUE(Sessions->CommitWrite(next));
}
}
}

namespace glamdb {
namespace {
TEST_F(WriteSession, ExpiredUnstartedSessionAbortsAndRetainsBeginIdentity)
{
    ASSERT_TRUE(Sessions->BeginWrite({ID, MutationKind::None, {}, std::chrono::seconds(1)}));
    ASSERT_TRUE(Sessions->PutPart(ID, {0, 0, 0}, Once({{"expired", 1, {}, {}, {}}})));
    // Adjust persisted wall-clock expiry as a deterministic expired-state fixture.
    Sessions.reset();
    {
        detail::Database db;
        db.Open(Temp.path / "stage" / "staging.sqlite", SQLITE_OPEN_READWRITE, {});
        db.Execute("UPDATE sessions SET expires=1");
    }
    auto opened = TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice");
    ASSERT_TRUE(opened);
    Sessions = *opened;
    ASSERT_TRUE(Sessions->Cleanup());
    EXPECT_EQ(Sessions->GetWriteStatus(ID)->State, EWriteState::Aborted);
    auto part = Sessions->GetPartReceipt(ID, {0, 0, 0});
    ASSERT_TRUE(part);
    EXPECT_FALSE(*part);
    auto begin = Sessions->BeginWrite({ID, MutationKind::None, {}, std::chrono::seconds(1)});
    ASSERT_TRUE(begin);
    EXPECT_EQ(begin->State, EWriteState::Aborted);
    EXPECT_FALSE(Sessions->BeginWrite({ID}));
    EXPECT_FALSE(Store->GetCollection(test::collection_id)->current_snapshot_id);
}
TEST_F(WriteSession, ManagerRetainsExactEngineAndReleasesSharedReservations)
{
    Sessions.reset();
    auto resources = Store->GetResourceOwner();
    const auto before = resources->GetUsage();
    auto opened = TWriteSessions::Open(Store, (Temp.path / "stage").string(), "alice");
    ASSERT_TRUE(opened);
    auto engineWeak = std::weak_ptr<Engine>(Store);
    Store.reset();
    EXPECT_FALSE(engineWeak.expired());
    opened->reset();
    EXPECT_TRUE(engineWeak.expired());
    const auto after = resources->GetUsage();
    EXPECT_LE(after.device_reserved_bytes, before.device_reserved_bytes);
    EXPECT_LE(after.memory_bytes[static_cast<int>(ResourceClass::Metadata)],
        before.memory_bytes[static_cast<int>(ResourceClass::Metadata)]);
}
}
}

namespace glamdb {
namespace {
TEST_F(WriteSession, DecimalAndDateRemainExactThroughStagingAndPublication)
{
    const SchemaSpec schema{{{"key", FieldType::String, false},
        {"decimal", FieldType::Decimal, true, 38, 4},
        {"date", FieldType::Date, true}}};
    const TWriteIdentity id{SecondCollection, SecondOperation};
    ASSERT_EQ(Store->CreateCollection({SecondCollection, "extended", schema}).disposition,
        CommitDisposition::Committed);
    const Decimal value{1, UINT64_C(123456789), 38, 4};
    auto batch =
        detail::MakeRecordBatch(schema, {{std::string("precise"), value, Date{-12345}}, {std::string("null"), {}, {}}});
    ASSERT_TRUE(batch);
    ASSERT_TRUE(Sessions->BeginWrite({id}));
    ASSERT_TRUE(Sessions->PutPart(id,
        {0, 0, 0},
        [data = *batch, done = false]() mutable -> Result<std::shared_ptr<arrow::RecordBatch>> {
            if (std::exchange(done, true)) {
                return nullptr;
            }
            return data;
        }));
    ASSERT_TRUE(Sessions->SealWrite(id, {{0, 0, 0}}));
    auto outcome = Sessions->CommitWrite(id);
    ASSERT_TRUE(outcome);
    ASSERT_EQ(outcome->disposition, CommitDisposition::Committed);
    auto cursor = Store->Scan({.collection_id = SecondCollection});
    ASSERT_TRUE(cursor);
    auto read = (*cursor)->Next();
    ASSERT_TRUE(read);
    ASSERT_TRUE(*read);
    const auto& decimals = static_cast<const arrow::Decimal128Array&>(*(*read)->column(1));
    const auto& dates = static_cast<const arrow::Date32Array&>(*(*read)->column(2));
    EXPECT_EQ(decimals.FormatValue(0), "1844674407383300.8405");
    EXPECT_EQ(dates.Value(0), -12345);
    EXPECT_TRUE(decimals.IsNull(1));
    EXPECT_TRUE(dates.IsNull(1));
}
}
}

namespace glamdb {
namespace {
TEST_F(WriteSession, ReturnedStatusesAndPartReceiptsKeepIndependentCharges)
{
    Seal();
    auto owner = Store->GetResourceOwner();
    const auto before = owner->GetUsage();
    {
        auto first = Sessions->GetWriteStatus(ID);
        ASSERT_TRUE(first);
        auto second = Sessions->GetWriteStatus(ID);
        ASSERT_TRUE(second);
        auto part = Sessions->GetPartReceipt(ID, {0, 0, 0});
        ASSERT_TRUE(part);
        ASSERT_TRUE(*part);
        const auto retained = owner->GetUsage();
        EXPECT_GE(retained.memory_bytes[static_cast<int>(ResourceClass::Metadata)],
            before.memory_bytes[static_cast<int>(ResourceClass::Metadata)] + 2 * (128ULL << 10));
        EXPECT_GE(retained.memory_bytes[static_cast<int>(ResourceClass::Control)],
            before.memory_bytes[static_cast<int>(ResourceClass::Control)] + 512);
    }
    EXPECT_EQ(owner->GetUsage().memory_bytes, before.memory_bytes);
}
}
}

namespace glamdb {
namespace {
TEST_F(WriteSession, OutcomeAbortAndCleanupUseAlreadyAdmittedControlSpace)
{
    Seal();
    auto owner = Store->GetResourceOwner();
    const auto index = static_cast<size_t>(ResourceClass::Metadata);
    auto pressure = owner->Reserve(ResourceClass::Metadata,
        owner->GetLimits().memory_bytes[index] - owner->GetUsage().memory_bytes[index]);
    ASSERT_TRUE(pressure);
    EXPECT_FALSE(Sessions->GetWriteStatus(ID));
    auto outcome = Sessions->GetWriteOutcome(ID);
    ASSERT_TRUE(outcome);
    EXPECT_FALSE(*outcome);
    ASSERT_TRUE(Sessions->AbortWrite(ID));
    EXPECT_TRUE(Sessions->Cleanup());
}
}
}
