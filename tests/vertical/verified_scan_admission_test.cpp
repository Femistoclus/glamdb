#include "core/scan/scan_cursor.h"
#include "core/storage/base_state.h"
#include "storage/resources.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

class VerifiedScanAdmissionTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::shared_ptr<ResourceOwner> Owner_ = std::make_shared<ResourceOwner>();
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;
    BaseIdentity Base_;

    void SetUp() override
    {
        const detail::BackendFactory factory =
            [this](const std::filesystem::path& path,
                const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Resources = Owner_;
            auto opened = detail::TLocalStorageBackend::Open(path, info, options);
            if (!opened) {
                return std::unexpected(opened.error());
            }
            Backend_ = *opened;
            return Backend_;
        };
        auto created = detail::EngineFactory::CreateWith(
            {.store_id = test::store_id, .absolute_root = (Temporary_.path / "store").string(), .resources = Owner_},
            factory);
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
        ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {Batch(FirstRows)}}).disposition,
            CommitDisposition::Committed);
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
        ASSERT_TRUE(cursor) << cursor.error().message;
        Base_ = (*cursor)->Info().identity;
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    Result<std::shared_ptr<ResourceReservation>> HoldMetadataLeaving(uint64_t bytes)
    {
        return Owner_->Reserve(ResourceClass::Metadata,
            Owner_->GetLimits().memory_bytes[1] - Owner_->GetUsage().memory_bytes[1] - bytes);
    }

    detail::TScanContext MakeContext(bool verified = true)
    {
        const auto collection = Engine_->GetCollection(test::collection_id);
        EXPECT_TRUE(collection);
        const auto io = Backend_->ReadOnlyFileIO();
        EXPECT_TRUE(io);
        const auto state = Backend_->VerifiedScanState(Base_);
        EXPECT_TRUE(state);
        if (!collection || !io || !state) {
            return {};
        }
        return {.Resources = Owner_,
            .Collection = *collection,
            .Base = Base_,
            .FileIO = *io,
            .Closing = nullptr,
            .Lease = Engine_,
            .OnError = {},
            .VerifiedState = verified ? *state : nullptr};
    }
};

TEST_F(VerifiedScanAdmissionTest, VerifiedPlanningProgressesUnderMetadataPressureAndRetainsOutput)
{
    auto pressure = HoldMetadataLeaving(512ULL << 20);
    ASSERT_TRUE(pressure) << pressure.error().message;
    auto cursor = Engine_->Scan({.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key", "value"},
        .predicate = Predicate::Compare("key", CompareOp::Eq, std::string("duplicate")),
        .batch_rows = 1});
    ASSERT_TRUE(cursor) << cursor.error().code << ": " << cursor.error().message;
    auto schema = (*cursor)->Info().output_schema;
    std::shared_ptr<arrow::RecordBatch> retained;
    std::vector<std::string> rows;
    while (true) {
        auto batch = (*cursor)->Next();
        ASSERT_TRUE(batch) << batch.error().message;
        if (!*batch) {
            break;
        }
        const auto rendered = Render(**batch);
        rows.insert(rows.end(), rendered.begin(), rendered.end());
        if (!retained) {
            retained = (*batch)->Slice(0, 1);
        }
    }
    EXPECT_EQ(rows, (std::vector<std::string>{"duplicate|1", "duplicate|1"}));
    EXPECT_EQ((*cursor)->Profile().manifests_read.value, 0);
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    cursor->reset();
    pressure->reset();
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    Backend_.reset();
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(Render(*retained), (std::vector<std::string>{"duplicate|1"}));
    EXPECT_EQ(schema->num_fields(), 2);
    EXPECT_GT(Owner_->GetUsage().memory_bytes[1], 0);
    retained.reset();
    EXPECT_GT(Owner_->GetUsage().memory_bytes[1], 0);
    schema.reset();
    for (const auto bytes : Owner_->GetUsage().memory_bytes) {
        EXPECT_EQ(bytes, 0);
    }
    EXPECT_EQ(Owner_->GetUsage().device_reserved_bytes, 0);
}

TEST_F(VerifiedScanAdmissionTest, CallerPlanningLeaseAvoidsAnotherWorkingReservation)
{
    auto context = MakeContext();
    auto planning = Owner_->Reserve(ResourceClass::Metadata, 768ULL << 20);
    ASSERT_TRUE(planning) << planning.error().message;
    context.PlanningMemory = *planning;
    auto pressure = HoldMetadataLeaving(24ULL << 20);
    ASSERT_TRUE(pressure) << pressure.error().message;
    auto cursor = detail::OpenScan(context, {.collection_id = test::collection_id, .limit = 0});
    ASSERT_TRUE(cursor) << cursor.error().message;
    const auto end = (*cursor)->Next();
    ASSERT_TRUE(end) << end.error().message;
    EXPECT_EQ(*end, nullptr);
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

TEST_F(VerifiedScanAdmissionTest, UncachedPlanningStillRequiresItsFullWorkingEnvelope)
{
    auto context = MakeContext(false);
    auto pressure = HoldMetadataLeaving(512ULL << 20);
    ASSERT_TRUE(pressure) << pressure.error().message;
    const ScanRequest request{.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key", "value"},
        .batch_rows = 1};
    const auto refused = detail::OpenScan(context, request);
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "RESOURCE_EXHAUSTED");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
    pressure->reset();
    auto cursor = detail::OpenScan(std::move(context), request);
    ASSERT_TRUE(cursor) << cursor.error().message;
    std::vector<std::string> rows;
    while (true) {
        const auto batch = (*cursor)->Next();
        ASSERT_TRUE(batch) << batch.error().message;
        if (!*batch) {
            break;
        }
        const auto rendered = Render(**batch);
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
    EXPECT_EQ(Sorted(std::move(rows)), (std::vector<std::string>{"duplicate|1", "duplicate|1", "other|null"}));
}

TEST_F(VerifiedScanAdmissionTest, MetadataPressureDoesNotBypassVerifiedMembershipValidation)
{
    auto context = MakeContext();
    ASSERT_NE(context.VerifiedState, nullptr);
    auto state = std::make_shared<NGlamDB::NStorage::TBaseState>(*context.VerifiedState);
    ASSERT_NE(state->ScanIndex, nullptr);
    auto index = std::make_shared<NGlamDB::NStorage::TScanMembership>(*state->ScanIndex);
    ASSERT_EQ(index->Snapshots.size(), 1U);
    index->Snapshots.front().FileCount = index->Files.size() + 1;
    state->ScanIndex = std::move(index);
    context.VerifiedState = std::move(state);
    auto pressure = HoldMetadataLeaving(512ULL << 20);
    ASSERT_TRUE(pressure) << pressure.error().message;
    const auto refused = detail::OpenScan(std::move(context), {.collection_id = test::collection_id});
    ASSERT_FALSE(refused);
    EXPECT_EQ(refused.error().code, "CORRUPTION");
}

} // namespace
} // namespace glamdb
