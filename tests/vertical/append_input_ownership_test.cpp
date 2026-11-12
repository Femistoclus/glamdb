// Caller-owned Arrow buffers and what an append commits: the committed rows
// and bound hash are those of the owned copy taken during the call, and input
// changed between fingerprinting and the owned copy is rejected before binding.

#include "core/api/canonical.h"
#include "tests/support/access_gate_vfs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>
#include <cstring>
#include <future>
#include <limits>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

class AppendInputOwnershipTest : public ::testing::Test
{
protected:
    // Declared first: destroyed after the engine closes its SQLite connection.
    AccessGateVfs Gate_;
    test::TempDir Temporary_;
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, (Temporary_.path / "store").string()},
            THookBackend::Factory(Hooks_),
            Gate_.Name());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(5000ms));
        }
    }

    std::vector<std::string> Read()
    {
        auto cursor = Engine_->Scan({.collection_id = test::collection_id});
        EXPECT_TRUE(cursor);
        std::vector<std::string> rows;
        while (cursor) {
            const auto batch = (*cursor)->Next();
            EXPECT_TRUE(batch);
            if (!batch || !*batch) {
                break;
            }
            const auto rendered = Render(**batch);
            rows.insert(rows.end(), rendered.begin(), rendered.end());
        }
        return Sorted(std::move(rows));
    }
};

uint8_t* Mutable(const arrow::RecordBatch& batch, int column, int buffer)
{
    return const_cast<uint8_t*>(batch.column_data(column)->buffers[buffer]->data());
}

void SetValue(const arrow::RecordBatch& batch, int64_t row, int64_t value)
{
    const auto& data = *batch.column_data(1);
    std::memcpy(Mutable(batch, 1, 1) + (data.offset + row) * sizeof(int64_t), &value, sizeof(value));
}

TEST_F(AppendInputOwnershipTest, MutatingCallerBuffersAfterReturnChangesNothingCommitted)
{
    const auto batch = Batch(FirstRows);
    const auto expectedHash = detail::FingerprintBatches(Schema, test::collection_id, {batch});
    ASSERT_TRUE(expectedHash);
    const auto first = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {batch}});
    ASSERT_EQ(first.disposition, CommitDisposition::Committed);

    SetValue(*batch, 0, 777);
    std::memset(Mutable(*batch, 0, 2), 'Z', 3);

    EXPECT_EQ(Read(), Expected({&FirstRows}));
    const auto found = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(found && *found);
    EXPECT_EQ(**found, first);
    // The operation stays bound to the original content: the mutated buffers
    // are different content, the original content is the same operation.
    const auto conflict = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {batch}});
    EXPECT_EQ(conflict.error ? conflict.error->code : "", "OPERATION_CONFLICT");
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}), first);
    EXPECT_NE(*detail::FingerprintBatches(Schema, test::collection_id, {batch}), *expectedHash);
}

// Owned columns of sliced and chunked input store exactly the input values.
TEST_F(AppendInputOwnershipTest, SlicedAndChunkedInputStoresExactValues)
{
    const std::vector<TInputRow> rows{
        {"dup", 1, 0.5, true, 10},
        {"dup", 1, 0.5, true, 10},
        {"", std::nullopt, std::nullopt, std::nullopt, std::nullopt},
        {"\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87", std::numeric_limits<int64_t>::min(), -0.0, false, -62135596800000000LL},
        {std::string(1024, 'k'),
            std::numeric_limits<int64_t>::max(),
            std::numeric_limits<double>::max(),
            true,
            253402300799999999LL},
        {"tail", std::nullopt, -1.5, std::nullopt, 0},
    };
    auto stored = rows;
    stored[3].Score = 0.0;
    const auto batch = Batch(rows);
    const std::vector<std::shared_ptr<arrow::RecordBatch>> input{batch->Slice(0, 1),
        batch->Slice(1, 3),
        batch->Slice(4, 0),
        batch->Slice(4)};
    const auto hash = detail::FingerprintBatches(Schema, test::collection_id, {batch});
    ASSERT_TRUE(hash);
    const auto outcome = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, input});
    ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
        << (outcome.error ? outcome.error->message : std::string());
    EXPECT_EQ(outcome.result->payload_hash, *hash);
    EXPECT_EQ(outcome.result->rows_appended, 6);
    EXPECT_EQ(Read(), Expected({&stored}));
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {batch}}), outcome);
}

// A call fingerprints the caller's buffers before admission and copies them
// after its authoritative outcome lookup. The gate pauses that lookup (the
// first catalog read after the writer is acquired), so the buffers change
// deterministically after fingerprinting and before the owned copy. The owned
// copy then no longer matches the fingerprint (value change) or its
// reservation (shape change) and the call fails before binding.
class TInputChangedAfterFingerprintTest
    : public AppendInputOwnershipTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TInputChangedAfterFingerprintTest, IsRejectedBeforeBinding)
{
    const bool changeShape = GetParam();
    const auto batch = Batch(FirstRows);
    std::future<AppendOutcome> append;
    // Destroyed before `append`, so a failed assertion cannot leave the call
    // paused and the future's destructor waiting for it.
    ReleaseOnExit releaseOnExit{Gate_};
    Gate_.Arm();
    append = std::async(std::launch::async, [&] {
        return Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {batch}});
    });
    ASSERT_EQ(Gate_.Entered().wait_for(10s), std::future_status::ready);
    if (changeShape) {
        // Keys "duplicate","duplicate","other" become one byte shorter.
        const auto& keys = *batch->column_data(0);
        auto* offsets = reinterpret_cast<int32_t*>(Mutable(*batch, 0, 1)) + keys.offset;
        offsets[3] -= 1;
    } else {
        SetValue(*batch, 0, 42);
    }
    Gate_.Release();
    const auto outcome = append.get();
    EXPECT_EQ(outcome.disposition, CommitDisposition::DefinitelyNotCommitted);
    ASSERT_TRUE(outcome.error);
    EXPECT_EQ(outcome.error->code, "INVALID_ARGUMENT");
    EXPECT_EQ(outcome.error->message, "Arrow input changed during append");
    const auto found = Engine_->FindAppendOutcome(test::collection_id, SecondOperation);
    ASSERT_TRUE(found);
    EXPECT_FALSE(*found) << "a rejected input must not bind its operation ID";
    EXPECT_TRUE(Read().empty());
    // The unbound ID remains usable for new content.
    EXPECT_EQ(Engine_->AppendDurable({test::collection_id, SecondOperation, {}, {Batch(FirstRows)}}).disposition,
        CommitDisposition::Committed);
}

INSTANTIATE_TEST_SUITE_P(ValueOrShape, TInputChangedAfterFingerprintTest, ::testing::Bool());

} // namespace
} // namespace glamdb
