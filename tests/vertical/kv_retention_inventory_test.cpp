#include "tests/support/checked.h"
#include "tests/support/fault_fs.h"
#include "tests/vertical/vertical_support.h"

#include "core/io/local_fs.h"
#include "core/storage/base_state.h"

#include <iceberg/table_metadata.h>

#include <chrono>

namespace glamdb {
namespace {
using namespace std::chrono_literals;
using namespace vertical;

constexpr char FourthOperation[] = "00000000-0000-4000-8000-0000000000a4";
constexpr char FifthOperation[] = "00000000-0000-4000-8000-0000000000a5";

class TKvRetentionInventoryTest : public testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    NGlamDB::NIO::TPosixLocalFS Posix_;
    std::shared_ptr<NGlamDB::NTesting::TFaultFS> Fs_ = std::make_shared<NGlamDB::NTesting::TFaultFS>(Posix_);
    std::shared_ptr<detail::TLocalStorageBackend> Backend_;
    std::shared_ptr<Engine> Engine_;

    detail::BackendFactory Factory()
    {
        return [this](const std::filesystem::path& root,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = Fs_;
            auto backend = detail::TLocalStorageBackend::Open(root, info, options);
            if (!backend) {
                return std::unexpected(backend.error());
            }
            Backend_ = std::move(*backend);
            return Backend_;
        };
    }

    void SetUp() override
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()}, Factory());
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        CreateCollectionRequest request{test::collection_id, "retained-kv", Schema};
        request.mode = CollectionMode::KeyValue;
        const auto collection = Engine_->CreateCollection(request);
        ASSERT_EQ(collection.disposition, CommitDisposition::Committed);
    }

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(5s));
        }
        Engine_.reset();
        Backend_.reset();
    }

    AppendOutcome Upsert(const std::vector<TInputRow>& rows, std::string_view operation)
    {
        return Engine_->UpsertDurable({test::collection_id, std::string(operation), {}, {Batch(rows)}});
    }

    void CheckPhysicalReferences()
    {
        uint64_t data = 0;
        uint64_t metadata = 0;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(Root_ / "warehouse")) {
            if (!entry.is_regular_file()) {
                continue;
            }
            if (entry.path().extension() == ".parquet") {
                data += entry.file_size();
            } else {
                metadata += entry.file_size();
            }
        }
        const auto measured = Engine_->GetStorageFootprint();
        ASSERT_TRUE(measured) << measured.error().message;
        EXPECT_GT(data, 0U);
        EXPECT_EQ(measured->DataFileBytes, data);
        EXPECT_EQ(measured->CommittedDataFileBytes, data);
        EXPECT_EQ(measured->IcebergMetadataFileBytes, metadata);
        EXPECT_EQ(measured->ReferencedMetadataFileBytes, metadata);
        EXPECT_EQ(measured->UnreferencedWarehouseFileBytes, 0U);
    }
};

TEST_F(TKvRetentionInventoryTest, KvExpirationRefusesBeforeStorageIoAndPreservesHistory)
{
    const std::vector<TInputRow> first{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    const std::vector<TInputRow> changed{{"a", 2, std::nullopt, std::nullopt, std::nullopt}};
    ASSERT_EQ(Upsert(first, FirstOperation).disposition, CommitDisposition::Committed);
    const auto noOp = Upsert(first, SecondOperation);
    ASSERT_EQ(noOp.disposition, CommitDisposition::Committed);
    ASSERT_EQ(Upsert(changed, ThirdOperation).disposition, CommitDisposition::Committed);
    const auto collection = Engine_->GetCollection(test::collection_id);
    ASSERT_TRUE(collection);
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history);
    ASSERT_EQ(history->snapshots.size(), 2U);
    const auto filesBefore = CountRegularFiles(Root_ / "warehouse");
    const auto eventsBefore = Fs_->Events().size();
    const auto direct = Backend_->PrepareExpiration(Engine_->GetStoreInfo(),
        *collection,
        history->identity,
        SnapshotRetentionPolicy{1},
        {});
    ASSERT_FALSE(direct);
    EXPECT_EQ(direct.error().code, "COLLECTION_MODE_MISMATCH");
    EXPECT_EQ(Fs_->Events().size(), eventsBefore);
    const auto expired = Engine_->ExpireSnapshots({test::collection_id, SnapshotRetentionPolicy{1}});
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().code, "COLLECTION_MODE_MISMATCH");
    EXPECT_EQ(Fs_->Events().size(), eventsBefore);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), filesBefore);
    const auto after = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(after);
    ASSERT_EQ(after->snapshots.size(), history->snapshots.size());
    for (size_t index = 0; index < history->snapshots.size(); ++index) {
        EXPECT_EQ(after->snapshots[index].snapshot_id, history->snapshots[index].snapshot_id);
    }
    EXPECT_EQ(Upsert(first, SecondOperation), noOp);
    CheckPhysicalReferences();
}

TEST_F(TKvRetentionInventoryTest, CleanupCannotDeleteALiveKvManifest)
{
    ASSERT_EQ(Upsert({{"a", 1, std::nullopt, std::nullopt, std::nullopt}}, FirstOperation).disposition,
        CommitDisposition::Committed);
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history);
    const auto state = Backend_->VerifiedScanState(history->identity);
    ASSERT_TRUE(state);
    ASSERT_FALSE((*state)->CurrentManifests.empty());
    const auto& liveManifest = (*state)->CurrentManifests.front().manifest_path;
    const auto filesBefore = CountRegularFiles(Root_ / "warehouse");
    const auto eventsBefore = Fs_->Events().size();
    const auto removed = Backend_->RemoveExpiredMetadata(test::collection_id, liveManifest);
    ASSERT_FALSE(removed);
    EXPECT_EQ(removed.error().code, "CORRUPTION");
    EXPECT_EQ(Fs_->Events().size(), eventsBefore);
    EXPECT_EQ(CountRegularFiles(Root_ / "warehouse"), filesBefore);
    CheckPhysicalReferences();
}

TEST_F(TKvRetentionInventoryTest, FootprintRetainsHistoricalDataAndNoOpRootsAcrossAppendCleanupAndReopen)
{
    const DeleteRequest empty{test::collection_id, FirstOperation, {}, {}};
    const auto emptyNoOp = Engine_->DeleteDurable(empty);
    ASSERT_EQ(emptyNoOp.disposition, CommitDisposition::Committed);
    const std::vector<TInputRow> first{{"a", 1, std::nullopt, std::nullopt, std::nullopt}};
    const std::vector<TInputRow> changed{{"a", 2, std::nullopt, std::nullopt, std::nullopt}};
    const auto inserted = Upsert(first, SecondOperation);
    ASSERT_EQ(inserted.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(inserted.result);
    const auto same = Upsert(first, ThirdOperation);
    ASSERT_EQ(same.disposition, CommitDisposition::Committed);
    ASSERT_TRUE(same.result);
    ASSERT_FALSE(test::Present(same.result).snapshot_created);
    ASSERT_EQ(Upsert(changed, FourthOperation).disposition, CommitDisposition::Committed);
    ASSERT_EQ(Engine_->DeleteDurable({test::collection_id, FifthOperation, {}, {"a"}}).disposition,
        CommitDisposition::Committed);

    ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "append-retention", Schema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({SecondCollection, FirstOperation, {}, {Batch(first)}}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({SecondCollection, SecondOperation, {}, {Batch(changed)}}).disposition,
        CommitDisposition::Committed);
    CheckPhysicalReferences();
    const auto expired = Engine_->ExpireSnapshots({SecondCollection, SnapshotRetentionPolicy{1}});
    ASSERT_TRUE(expired) << expired.error().message;
    EXPECT_EQ(expired->snapshots_expired, 1);
    EXPECT_EQ(expired->cleanup_pending, 0);
    CheckPhysicalReferences();

    const auto checkKv = [&] {
        EXPECT_EQ(Engine_->DeleteDurable(empty), emptyNoOp);
        EXPECT_EQ(Upsert(first, ThirdOperation), same);
        const auto latest = Engine_->Lookup({test::collection_id, {"a"}});
        ASSERT_TRUE(latest) << latest.error().message;
        ASSERT_EQ(latest->rows.size(), 1U);
        EXPECT_FALSE(latest->rows.front());
        const auto old = Engine_->Lookup(
            {test::collection_id, {"a"}, SnapshotSelector::Id(test::Present(inserted.result).snapshot_id)});
        ASSERT_TRUE(old) << old.error().message;
        ASSERT_EQ(old->rows.size(), 1U);
        ASSERT_TRUE(old->rows.front());
        EXPECT_EQ(std::get<int64_t>(test::Present(old->rows.front()).at(1)), 1);
        const auto history = Engine_->ListSnapshots(test::collection_id);
        ASSERT_TRUE(history);
        EXPECT_EQ(history->snapshots.size(), 3U);
    };
    checkKv();
    ASSERT_TRUE(Engine_->Shutdown(5s));
    Engine_.reset();
    Backend_.reset();
    auto reopened = detail::EngineFactory::OpenWith({Root_.string(), test::store_id}, Factory());
    ASSERT_TRUE(reopened) << reopened.error().message;
    Engine_ = std::move(*reopened);
    CheckPhysicalReferences();
    checkKv();
}

} // namespace
} // namespace glamdb
