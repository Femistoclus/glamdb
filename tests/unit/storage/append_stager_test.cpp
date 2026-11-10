#include "core/storage/append_stager.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/table_profile.h"
#include "tests/support/fault_fs.h"

#include <gtest/gtest.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <span>

namespace NGlamDB::NStorage {
namespace {

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
constexpr auto OperationID = "44444444-4444-4444-8444-444444444444";
constexpr auto AppendID = "33333333-3333-7333-8333-333333333333";

class TAppendStagerTest : public ::testing::Test
{
protected:
    std::filesystem::path Root_;
    std::shared_ptr<NIO::TPosixLocalFS> Posix_;
    std::shared_ptr<NGlamDB::NTesting::TFaultFS> FS_;
    std::shared_ptr<NIO::TWriteLatch> Latch_;
    std::shared_ptr<NIO::TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> IO_;
    std::shared_ptr<iceberg::InMemoryCatalog> Catalog_;
    std::shared_ptr<iceberg::Table> Table_;
    std::shared_ptr<NIO::TIOAttempt> Attempt_;
    std::optional<TAttemptScope> Scope_;
    std::shared_ptr<iceberg::DataFile> Data_;

    void SetUp() override
    {
        auto root = (std::filesystem::temp_directory_path() / "glamdb-stage-XXXXXX").string();
        ASSERT_NE(::mkdtemp(root.data()), nullptr);
        Root_ = std::filesystem::canonical(root);
        Posix_ = std::make_shared<NIO::TPosixLocalFS>();
        FS_ = std::make_shared<NGlamDB::NTesting::TFaultFS>(*Posix_);
        Latch_ = std::make_shared<NIO::TWriteLatch>();
        auto directories = NIO::TStoreDirs::Create(FS_, Root_, Latch_);
        ASSERT_TRUE(directories);
        Dirs_ = std::move(directories).Value();
        ASSERT_TRUE(Dirs_->CreateCollection(CollectionID));
        auto fileIO = TDurableLocalFileIO::Make(Dirs_, StoreID);
        ASSERT_TRUE(fileIO) << fileIO.error().message;
        IO_ = std::move(*fileIO);
        const auto catalog = iceberg::InMemoryCatalog::Make("staging-test-double", IO_, Location(), {});
        ASSERT_TRUE(catalog);
        Catalog_ = *catalog;
        const auto profile = MakeTableProfile({{{"key", EFieldType::String}}});
        ASSERT_TRUE(profile);
        const auto attempt = NIO::TIOAttempt::Make({StoreID,
                                                       CollectionID,
                                                       "33333333-3333-7333-8333-333333333331",
                                                       NIO::EAttemptKind::CreateCollection,
                                                       std::nullopt},
            Latch_);
        ASSERT_TRUE(attempt);
        {
            auto scope = IO_->BindAttempt(attempt.Value());
            ASSERT_TRUE(scope);
            auto table = Catalog_->CreateTable({.ns = {}, .name = "events"},
                profile->Schema,
                iceberg::PartitionSpec::Unpartitioned(),
                iceberg::SortOrder::Unsorted(),
                Location(),
                profile->Properties);
            ASSERT_TRUE(table) << table.error().message;
            Table_ = std::move(*table);
        }
    }

    void TearDown() override
    {
        Scope_.reset();
        std::error_code error;
        std::filesystem::remove_all(Root_, error);
        EXPECT_FALSE(error);
    }

    static std::string Location()
    {
        return std::string("store://") + StoreID + "/warehouse/" + CollectionID;
    }

    void PrepareData(bool verified = true, const std::string& attemptID = AppendID)
    {
        Scope_.reset();
        auto attempt =
            NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, OperationID}, Latch_);
        ASSERT_TRUE(attempt);
        Attempt_ = std::move(attempt).Value();
        auto scope = IO_->BindAttempt(Attempt_);
        ASSERT_TRUE(scope) << scope.error().message;
        Scope_.emplace(std::move(*scope));
        Data_ = std::make_shared<iceberg::DataFile>();
        Data_->file_path = Location() + "/data/" + OperationID + "-" + attemptID + "-00000.parquet";
        Data_->record_count = 3;
        Data_->partition_spec_id = 0;
        constexpr std::string_view bytes = "test-data";
        Data_->file_size_in_bytes = bytes.size();
        ASSERT_FALSE(Attempt_->DeclareDataFile(Data_->file_path));
        auto output = IO_->NewOutputFile(Data_->file_path);
        ASSERT_TRUE(output);
        auto stream = (*output)->Create();
        ASSERT_TRUE(stream);
        ASSERT_TRUE((*stream)->Write(std::as_bytes(std::span(bytes.data(), bytes.size()))));
        ASSERT_TRUE((*stream)->Close());
        // The data-format verifier is a test double; manifests use real Avro IO.
        if (verified) {
            ASSERT_FALSE(Attempt_->MarkFormatVerified(Data_->file_path));
        }
    }

    void CheckUnpublished()
    {
        if (Attempt_->FirstError()) {
            Scope_.reset();
        }
        const auto loaded = Catalog_->LoadTable(Table_->name());
        ASSERT_TRUE(loaded);
        EXPECT_EQ((*loaded)->metadata_file_location(), Table_->metadata_file_location());
        EXPECT_EQ((*loaded)->metadata()->current_snapshot_id, Table_->metadata()->current_snapshot_id);
        for (const auto& record : Attempt_->Records()) {
            EXPECT_NE(record.Kind, NIO::EFileKind::Metadata);
        }
    }
};

TEST_F(TAppendStagerTest, StagesRealManifestsWithoutPublishingTheTable)
{
    PrepareData();
    const std::unordered_map<std::string, std::string> summary{{"glamdb.operation-id", OperationID},
        {"glamdb.content-hash", "test-hash"}};
    auto staged = StageAppend(Table_, Data_, summary, Attempt_);
    ASSERT_TRUE(staged) << staged.error().message;
    EXPECT_FALSE(staged->ParentID);
    EXPECT_EQ(staged->Transaction->state(), iceberg::TransactionState::kReady);
    EXPECT_EQ(staged->Transaction->base()->table_uuid, Table_->uuid());
    CheckUnpublished();
    auto snapshot = staged->Transaction->current().Snapshot();
    ASSERT_TRUE(snapshot);
    EXPECT_EQ((*snapshot)->snapshot_id, staged->SnapshotID);
    EXPECT_EQ((*snapshot)->manifest_list, staged->ManifestList);
    for (const auto& [key, value] : summary) {
        EXPECT_EQ((*snapshot)->summary.at(key), value);
    }
    EXPECT_EQ((*snapshot)->Operation(), "append");
    iceberg::SnapshotReader snapshotReader(snapshot->get());
    auto manifests = snapshotReader.Manifests(IO_);
    ASSERT_TRUE(manifests) << manifests.error().message;
    ASSERT_EQ(manifests->size(), 1);
    ASSERT_EQ(staged->NewManifestPaths.size(), 1);
    const auto& manifest = manifests->front();
    EXPECT_EQ(manifest.manifest_path, staged->NewManifestPaths.front());
    auto reader = iceberg::ManifestReader::Make(manifest, IO_, Table_->schema().value(), Table_->spec().value());
    ASSERT_TRUE(reader) << reader.error().message;
    auto entries = (*reader)->Entries();
    ASSERT_TRUE(entries) << entries.error().message;
    ASSERT_EQ(entries->size(), 1);
    EXPECT_EQ(entries->front().status, iceberg::ManifestStatus::kAdded);
    EXPECT_EQ(entries->front().data_file->file_path, Data_->file_path);
    EXPECT_EQ(entries->front().data_file->record_count, 3);
    EXPECT_EQ(entries->front().data_file->file_size_in_bytes, Data_->file_size_in_bytes);
    EXPECT_EQ(entries->front().snapshot_id, staged->SnapshotID);
    EXPECT_EQ(Attempt_->Receipts().size(), 3);
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TAppendStagerTest, CallerPublishesAndNextStageKeepsTheParentManifest)
{
    PrepareData();
    auto first = StageAppend(Table_, Data_, {}, Attempt_);
    ASSERT_TRUE(first) << first.error().message;
    CheckUnpublished();
    auto published = first->Transaction->Commit();
    ASSERT_TRUE(published) << published.error().message;
    Table_ = *published;
    EXPECT_EQ(Table_->metadata()->current_snapshot_id, first->SnapshotID);
    PrepareData(true, "33333333-3333-7333-8333-333333333334");
    auto second = StageAppend(Table_, Data_, {}, Attempt_);
    ASSERT_TRUE(second) << second.error().message;
    EXPECT_EQ(second->ParentID, first->SnapshotID);
    EXPECT_NE(second->SnapshotID, first->SnapshotID);
    CheckUnpublished();
    auto snapshot = second->Transaction->current().Snapshot();
    ASSERT_TRUE(snapshot);
    iceberg::SnapshotReader reader(snapshot->get());
    const auto manifests = reader.Manifests(IO_);
    ASSERT_TRUE(manifests);
    ASSERT_EQ(manifests->size(), 2);
    ASSERT_EQ(second->NewManifestPaths.size(), 1);
    EXPECT_EQ((*manifests)[0].manifest_path, second->NewManifestPaths.front());
    EXPECT_EQ((*manifests)[1].manifest_path, first->NewManifestPaths.front());
}

TEST_F(TAppendStagerTest, DroppingTheStageNeitherPublishesNorDeletesFiles)
{
    PrepareData();
    const auto deletes = IO_->DeniedDeleteCount();
    {
        auto staged = StageAppend(Table_, Data_, {}, Attempt_);
        ASSERT_TRUE(staged) << staged.error().message;
    }
    CheckUnpublished();
    EXPECT_EQ(IO_->DeniedDeleteCount(), deletes);
    for (const auto& receipt : Attempt_->Receipts()) {
        const auto file = IO_->NewInputFile(receipt.Path);
        ASSERT_TRUE(file);
        EXPECT_EQ((*file)->Size().value(), receipt.Length);
    }
}

TEST_F(TAppendStagerTest, RequiresIndependentDataFormatVerification)
{
    PrepareData(false);
    const auto eventCount = FS_->Events().size();
    EXPECT_FALSE(StageAppend(Table_, Data_, {}, Attempt_));
    EXPECT_EQ(FS_->Events().size(), eventCount);
    EXPECT_TRUE(Attempt_->FirstError());
    CheckUnpublished();
}

class TInvalidAppendTest
    : public TAppendStagerTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TInvalidAppendTest, RejectsInvalidDataBeforeMetadataIO)
{
    PrepareData();
    auto table = Table_;
    auto data = Data_;
    auto attempt = Attempt_;
    switch (GetParam()) {
        case 0:
            table.reset();
            break;
        case 1:
            data.reset();
            break;
        case 2:
            attempt.reset();
            break;
        case 3:
            ++data->file_size_in_bytes;
            break;
        case 4:
            data->file_size_in_bytes = -1;
            break;
        case 5:
            data->record_count = 0;
            break;
        case 6:
            data->record_count = -1;
            break;
        case 7:
            data->content = iceberg::DataFile::Content::kEqualityDeletes;
            break;
        case 8:
            data->file_format = iceberg::FileFormatType::kAvro;
            break;
        case 9:
            data->partition_spec_id = 42;
            break;
        case 10:
            data->file_path += "-foreign";
            break;
        case 11:
            ASSERT_FALSE(attempt->Seal());
            break;
        case 12:
            attempt->Deactivate();
            break;
        default:
            FAIL();
    }
    const auto eventCount = FS_->Events().size();
    EXPECT_FALSE(StageAppend(table, data, {}, attempt));
    EXPECT_EQ(FS_->Events().size(), eventCount);
    CheckUnpublished();
}

INSTANTIATE_TEST_SUITE_P(InvalidInputs, TInvalidAppendTest, ::testing::Range(0, 13));

TEST_F(TAppendStagerTest, ManifestSyncFailureCannotProduceAStageOrPublish)
{
    PrepareData();
    FS_->AddRule({NGlamDB::NTesting::EIOOp::SyncFile,
        "*.avro",
        1,
        1,
        std::make_error_code(std::errc::no_space_on_device),
        std::nullopt});
    const auto staged = StageAppend(Table_, Data_, {}, Attempt_);
    EXPECT_FALSE(staged);
    EXPECT_EQ(Attempt_->FirstError(), std::errc::no_space_on_device);
    EXPECT_FALSE(Latch_->IsSet());
    EXPECT_EQ(Attempt_->Receipts().size(), 1);
    CheckUnpublished();
}

}
}
