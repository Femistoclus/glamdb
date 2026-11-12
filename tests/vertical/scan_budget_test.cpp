#include "core/resources/envelopes.h"
#include "storage/resources.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/c/bridge.h>

#include <barrier>
#include <chrono>
#include <thread>

namespace glamdb {
namespace {
using namespace vertical;

class ScanBudget : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::shared_ptr<ResourceOwner> Owner_ = std::make_shared<ResourceOwner>();
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = CreateStore(
            {.store_id = test::store_id, .absolute_root = (Temporary_.path / "store").string(), .resources = Owner_});
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
    }
};

TEST_F(ScanBudget, RetainedExportsBlockOtherScansUntilFinalRelease)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id,
        .predicate = Predicate::Compare("key", CompareOp::Eq, std::string("duplicate"))});
    ASSERT_TRUE(cursor);
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    ASSERT_NE(*next, nullptr);
    auto slice = (*next)->Slice(0, 1);
    ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportRecordBatch(*slice, &exported).ok());
    next->reset();
    cursor->reset();
    auto pressure = Owner_->Reserve(ResourceClass::Data, Owner_->GetLimits().memory_bytes[0] - (1ULL << 20));
    ASSERT_TRUE(pressure);
    auto refused = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(std::static_pointer_cast<arrow::StringArray>(slice->column(0))->GetString(0), "duplicate");
    slice.reset();
    EXPECT_FALSE(Engine_->Scan({.collection_id = test::collection_id}));
    exported.release(&exported);
    auto resumed = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(resumed) << resumed.error().message;
    pressure->reset();
    auto batch = (*resumed)->Next();
    ASSERT_TRUE(batch);
    ASSERT_NE(*batch, nullptr);
    EXPECT_EQ((*batch)->num_rows(), 3);
}

TEST_F(ScanBudget, ProfilesRetainedMemoryAndSeparatesStopsFromPruning)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id,
        .predicate = Predicate::Compare("key", CompareOp::Eq, std::string("duplicate")),
        .limit = 1});
    ASSERT_TRUE(cursor);
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    ASSERT_NE(*next, nullptr);
    auto profile = (*cursor)->Profile();
    EXPECT_EQ(profile.rows_decoded.value, 3);
    EXPECT_EQ(profile.rows_filtered.value, 1);
    EXPECT_EQ(profile.rows_limited.value, 1);
    EXPECT_EQ(profile.rows_returned.value, 1);
    EXPECT_EQ(profile.limit_stops.value, 1);
    EXPECT_EQ(profile.cancellation_stops.value, 0);
    EXPECT_EQ(profile.data_files_pruned.value, 0);
    EXPECT_EQ(profile.row_groups_pruned.value, 0);
    EXPECT_GT(profile.arrow_bytes.value.value_or(0), 0);
    EXPECT_GT(profile.output_metadata_bytes.value.value_or(0), 0);
    const auto outputBytes = profile.output_metadata_bytes.value;
    ASSERT_TRUE((*cursor)->Next());
    profile = (*cursor)->Profile();
    EXPECT_EQ(profile.state, ScanState::Complete);
    EXPECT_EQ(profile.output_metadata_bytes.value, outputBytes);
    EXPECT_GE(profile.peak_arrow_bytes.value, profile.arrow_bytes.value);
    next->reset();
    profile = (*cursor)->Profile();
    EXPECT_EQ(profile.arrow_bytes.value, 0);
    EXPECT_EQ(profile.output_metadata_bytes.value, 0);
    EXPECT_EQ(profile.peak_output_metadata_bytes.value, outputBytes);
    EXPECT_GT(profile.peak_arrow_bytes.value.value_or(0), 0);
    ASSERT_TRUE((*cursor)->Next());
    EXPECT_EQ((*cursor)->Profile().limit_stops.value, 1);

    auto zero = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
    ASSERT_TRUE(zero);
    ASSERT_TRUE((*zero)->Next());
    EXPECT_EQ((*zero)->Profile().limit_stops.value, 1);
    EXPECT_EQ((*zero)->Profile().data_files_pruned.value, 0);
    auto cancelled = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cancelled);
    (*cancelled)->Cancel();
    EXPECT_FALSE((*cancelled)->Next());
    EXPECT_FALSE((*cancelled)->Next());
    EXPECT_EQ((*cancelled)->Profile().cancellation_stops.value, 1);
    EXPECT_EQ((*cancelled)->Profile().limit_stops.value, 0);
    EXPECT_EQ((*cancelled)->Profile().data_files_pruned.value, 0);
}

TEST_F(ScanBudget, ConcurrentScansShareControlAdmission)
{
    auto pressure = Owner_->Reserve(ResourceClass::Control,
        Owner_->GetLimits().memory_bytes[2] - Owner_->GetUsage().memory_bytes[2] - detail::ControlMemoryBytes);
    ASSERT_TRUE(pressure);
    std::barrier start(3);
    std::array<Result<std::shared_ptr<ScanCursor>>, 2> scans;
    std::array<std::thread, 2> threads;
    for (size_t index = 0; index < threads.size(); ++index) {
        threads[index] = std::thread([&, index] {
            start.arrive_and_wait();
            scans[index] = Engine_->Scan({.collection_id = test::collection_id});
        });
    }
    start.arrive_and_wait();
    for (auto& thread : threads) {
        thread.join();
    }
    int admitted = 0;
    for (auto& scan : scans) {
        if (scan) {
            ++admitted;
            auto next = (*scan)->Next();
            ASSERT_TRUE(next) << next.error().message;
            ASSERT_NE(*next, nullptr);
            EXPECT_EQ((*next)->num_rows(), 3);
        } else {
            EXPECT_EQ(scan.error().code, "RESOURCE_EXHAUSTED");
        }
    }
    EXPECT_EQ(admitted, 1);
    for (auto& scan : scans) {
        if (scan) {
            scan->reset();
        }
    }
    EXPECT_TRUE(Engine_->Scan({.collection_id = test::collection_id}));
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

TEST_F(ScanBudget, EveryRetainedOutputKeepsMetadataIncludingZeroColumnSlices)
{
    for (const auto& projection : {std::vector<std::string>{"key"}, std::vector<std::string>{}}) {
        auto cursor = Engine_->Scan({.collection_id = test::collection_id, .projection = projection, .batch_rows = 1});
        ASSERT_TRUE(cursor);
        auto first = (*cursor)->Next();
        ASSERT_TRUE(first);
        ASSERT_NE(*first, nullptr);
        const auto firstUsage = Owner_->GetUsage().memory_bytes[1];
        auto second = (*cursor)->Next();
        ASSERT_TRUE(second);
        ASSERT_NE(*second, nullptr);
        EXPECT_GT(Owner_->GetUsage().memory_bytes[1], firstUsage);
        auto slice = (*second)->Slice(0, 1);
        second->reset();
        const auto retained = Owner_->GetUsage().memory_bytes[1];
        slice.reset();
        EXPECT_LT(Owner_->GetUsage().memory_bytes[1], retained);
        EXPECT_EQ((*first)->num_rows(), 1);
    }
}

TEST_F(ScanBudget, RetainedInfoSchemaKeepsItsChargeAfterShutdown)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
    ASSERT_TRUE(cursor);
    auto schema = (*cursor)->Info().output_schema;
    cursor->reset();
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
    Engine_.reset();
    EXPECT_GT(Owner_->GetUsage().memory_bytes[1], 0);
    EXPECT_EQ(schema->num_fields(), 5);
    schema.reset();
    EXPECT_EQ(Owner_->GetUsage().memory_bytes[1], 0);
    EXPECT_EQ(Owner_->GetUsage().device_reserved_bytes, 0);
}

TEST_F(ScanBudget, ZeroColumnSliceKeepsItsChargeAfterShutdown)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id, .projection = std::vector<std::string>{}});
    ASSERT_TRUE(cursor);
    auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    ASSERT_NE(*next, nullptr);
    auto slice = (*next)->Slice(0, 1);
    next->reset();
    cursor->reset();
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
    Engine_.reset();
    EXPECT_GT(Owner_->GetUsage().memory_bytes[1], 0);
    EXPECT_EQ(slice->num_rows(), 1);
    slice.reset();
    EXPECT_EQ(Owner_->GetUsage().memory_bytes[1], 0);
    EXPECT_EQ(Owner_->GetUsage().device_reserved_bytes, 0);
}

TEST_F(ScanBudget, OutputMetadataPressurePreservesEarlierRows)
{
    auto cursor = Engine_->Scan({.collection_id = test::collection_id, .batch_rows = 1});
    ASSERT_TRUE(cursor);
    auto first = (*cursor)->Next();
    ASSERT_TRUE(first);
    ASSERT_NE(*first, nullptr);
    const auto key = std::static_pointer_cast<arrow::StringArray>((*first)->column(0));
    const auto expected = key->GetString(0);
    auto pressure = Owner_->Reserve(ResourceClass::Metadata,
        Owner_->GetLimits().memory_bytes[1] - Owner_->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(pressure);
    const auto refused = (*cursor)->Next();
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(key->GetString(0), expected);
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    pressure->reset();
    EXPECT_TRUE(Engine_->Scan({.collection_id = test::collection_id}));
}

} // namespace
} // namespace glamdb
