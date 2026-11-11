#include "core/scan/scan_cursor.h"
#include "core/scan/scan_planner.h"
#include "core/storage/base_state.h"
#include "storage/resources.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include <chrono>

namespace glamdb {
namespace {

using namespace vertical;
using namespace std::chrono_literals;

std::vector<std::string> Drain(ScanCursor& cursor)
{
    std::vector<std::string> rows;
    while (true) {
        const auto batch = cursor.Next();
        EXPECT_TRUE(batch) << (batch ? "" : batch.error().message);
        if (!batch || !*batch) {
            return Sorted(std::move(rows));
        }
        auto rendered = Render(**batch);
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
}

class ScanIndexTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<NGlamDB::NTesting::TFaultFS> Faults_ = std::make_shared<NGlamDB::NTesting::TFaultFS>(Posix_);
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    detail::BackendFactory Factory()
    {
        return [this](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = Faults_;
            auto opened = detail::TLocalStorageBackend::Open(path, info, options);
            if (!opened) {
                return std::unexpected(opened.error());
            }
            Backend_ = *opened;
            return Backend_;
        };
    }

    void SetUp() override
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, Factory());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    AppendResult Append(const char* operation, const std::vector<TInputRow>& rows)
    {
        const auto outcome = Engine_->AppendDurable({test::collection_id, operation, {}, {Batch(rows)}});
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed) << (outcome.error ? outcome.error->message : "");
        return outcome.result.value_or(AppendResult{});
    }

    BaseIdentity Base()
    {
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
        EXPECT_TRUE(cursor) << (cursor ? "" : cursor.error().message);
        return cursor ? (*cursor)->Info().identity : BaseIdentity{};
    }

    size_t MetadataOpens() const
    {
        size_t count = 0;
        for (const auto& event : Faults_->Events()) {
            if (event.Op == NGlamDB::NTesting::EIOOp::OpenRead &&
                (event.Path.ends_with(".metadata.json") || event.Path.ends_with(".avro")))
            {
                ++count;
            }
        }
        return count;
    }
};

TEST_F(ScanIndexTest, MatchesAuthoritativeMembershipAndRowsForEverySelector)
{
    const auto empty = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(empty);
    const auto first = Append(FirstOperation, FirstRows);
    const auto second = Append(SecondOperation, SecondRows);
    const auto base = Base();
    const auto state = Backend_->VerifiedScanState(base);
    ASSERT_TRUE(state);
    ASSERT_NE(*state, nullptr);
    ASSERT_NE((*state)->ScanIndex, nullptr);
    EXPECT_EQ((*state)->ScanIndex->Files.size(), 2U);
    EXPECT_EQ((*state)->ScanIndex->Snapshots.size(), 2U);

    const auto files = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(files.size(), 2U);
    const auto orphan = files.front().parent_path() / "orphan.parquet";
    std::filesystem::copy_file(files.front(), orphan);
    const auto io = Backend_->ReadOnlyFileIO();
    ASSERT_TRUE(io);
    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    const std::pair<SnapshotSelector, std::vector<std::string>> cases[] = {
        {SnapshotSelector::Latest(), Expected({&FirstRows, &SecondRows})},
        {SnapshotSelector::Id(first.snapshot_id), Expected({&FirstRows})},
        {SnapshotSelector::Id(second.snapshot_id), Expected({&FirstRows, &SecondRows})},
        {SnapshotSelector::InitialEmpty(), {}}};
    for (const auto& [selector, expected] : cases) {
        const auto reference = detail::PlanScan(*io, base, selector);
        ASSERT_TRUE(reference) << reference.error().message;
        const auto indexed = detail::PlanVerifiedScan(**state, base, selector);
        ASSERT_TRUE(indexed) << indexed.error().message;
        EXPECT_EQ(indexed->SnapshotID, reference->SnapshotID);
        std::vector<detail::TScanFile> selected;
        for (size_t index = indexed->FileCount; index > 0; --index) {
            selected.push_back(indexed->Index->Files[index - 1]);
        }
        EXPECT_EQ(selected, reference->Files);

        detail::TScanContext context{.Collection = *collection,
            .Base = base,
            .FileIO = *io,
            .Closing = nullptr,
            .Lease = nullptr,
            .OnError = {}};
        const ScanRequest request{.collection_id = test::collection_id, .selector = selector};
        const auto uncached = detail::OpenScan(context, request);
        ASSERT_TRUE(uncached);
        context.VerifiedState = *state;
        const auto cached = detail::OpenScan(std::move(context), request);
        ASSERT_TRUE(cached);
        EXPECT_EQ(Drain(**uncached), expected);
        EXPECT_EQ(Drain(**cached), expected);
        EXPECT_EQ((*cached)->Profile().manifests_read.value, 0);
    }
    EXPECT_TRUE(Drain(**empty).empty());
    EXPECT_TRUE(std::filesystem::exists(orphan));
    const auto unknown = Engine_->Scan({.collection_id = test::collection_id, .selector = SnapshotSelector::Id(-9)});
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "NOT_FOUND");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

TEST_F(ScanIndexTest, ReopenRebuildsMembershipAndWarmScansReadNoMetadata)
{
    const auto first = Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    const auto before = Backend_->VerifiedScanState(Base());
    ASSERT_TRUE(before);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    Backend_.reset();
    const auto opens = MetadataOpens();
    auto opened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, Factory());
    ASSERT_TRUE(opened) << opened.error().message;
    Engine_ = std::move(*opened);
    EXPECT_GT(MetadataOpens(), opens);
    const auto after = Backend_->VerifiedScanState(Base());
    ASSERT_TRUE(after);
    EXPECT_NE(*before, *after);
    EXPECT_EQ((*before)->ScanIndex->Files, (*after)->ScanIndex->Files);
    const auto rebuiltOpens = MetadataOpens();
    for (int repeat = 0; repeat < 3; ++repeat) {
        const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
        ASSERT_TRUE(cursor);
        EXPECT_EQ(Drain(**cursor), Expected({&FirstRows, &SecondRows}));
        const auto historical =
            Engine_->Scan({.collection_id = test::collection_id, .selector = SnapshotSelector::Id(first.snapshot_id)});
        ASSERT_TRUE(historical);
        EXPECT_EQ(Drain(**historical), Expected({&FirstRows}));
        const auto empty =
            Engine_->Scan({.collection_id = test::collection_id, .selector = SnapshotSelector::InitialEmpty()});
        ASSERT_TRUE(empty);
        EXPECT_TRUE(Drain(**empty).empty());
        const auto zero = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
        ASSERT_TRUE(zero);
        EXPECT_TRUE(Drain(**zero).empty());
    }
    EXPECT_EQ(MetadataOpens(), rebuiltOpens);
}

TEST_F(ScanIndexTest, ReconciliationReusesRootReservationUnderMetadataPressure)
{
    Append(FirstOperation, FirstRows);
    const auto owner = Engine_->GetResourceOwner();
    auto pressure = owner->Reserve(ResourceClass::Metadata,
        owner->GetLimits().memory_bytes[1] - owner->GetUsage().memory_bytes[1]);
    ASSERT_TRUE(pressure) << pressure.error().message;
    const auto recovered = Engine_->Reconcile();
    ASSERT_TRUE(recovered) << recovered.error().code << ": " << recovered.error().message;
    pressure->reset();
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor) << cursor.error().message;
    EXPECT_EQ(Drain(**cursor), Expected({&FirstRows}));
    EXPECT_EQ((*cursor)->Profile().manifests_read.value, 0);
}

TEST_F(ScanIndexTest, ObservedCorruptionRevokesMembershipUntilSuccessfulReconciliation)
{
    Append(FirstOperation, FirstRows);
    const auto base = Base();
    const auto before = Backend_->VerifiedScanState(base);
    ASSERT_TRUE(before);
    const auto data = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
    ASSERT_EQ(data.size(), 1U);
    const auto saved = ReadBytes(data.front());
    std::filesystem::remove(data.front());
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cursor);
    const auto failed = (*cursor)->Next();
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code, "CORRUPTION");
    EXPECT_FALSE(Backend_->VerifiedScanState(base));
    EXPECT_FALSE(Engine_->Scan({.collection_id = test::collection_id}));
    EXPECT_FALSE(Engine_->Reconcile());
    EXPECT_FALSE(Backend_->VerifiedScanState(base));
    WriteBytes(data.front(), saved);
    ASSERT_TRUE(Engine_->Reconcile());
    const auto after = Backend_->VerifiedScanState(base);
    ASSERT_TRUE(after);
    EXPECT_NE(*before, *after);
    const auto restored = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(restored);
    EXPECT_EQ(Drain(**restored), Expected({&FirstRows}));
}

} // namespace
} // namespace glamdb
