#include "core/storage/append_stager.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/publication_guard.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <gtest/gtest.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <parquet/arrow/reader.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <spawn.h>
#include <sys/wait.h>
#include <thread>

extern char** environ;

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
constexpr auto AttemptID = "33333333-3333-7333-8333-333333333333";

void ExpectFreshProcess(const std::filesystem::path& root, const std::string& location)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        const std::string_view value(*entry);
        if (!value.starts_with("GLAMDB_RECOVERY_ROOT=") && !value.starts_with("GLAMDB_RECOVERY_METADATA=")) {
            environment.emplace_back(value);
        }
    }
    environment.push_back("GLAMDB_RECOVERY_ROOT=" + root.string());
    environment.push_back("GLAMDB_RECOVERY_METADATA=" + location);
    std::vector<char*> variables;
    variables.reserve(environment.size() + 1);
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=RecoveryChildProcess.ReopensPublishedRoot";
    char* arguments[]{executable.data(), filter.data(), nullptr};
    pid_t child = 0;
    ASSERT_EQ(::posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments, variables.data()), 0);
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
#else
    static_cast<void>(root);
    static_cast<void>(location);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

TEST(RecoveryChildProcess, ReopensPublishedRoot)
{
    const auto* root = std::getenv("GLAMDB_RECOVERY_ROOT");
    const auto* location = std::getenv("GLAMDB_RECOVERY_METADATA");
    if (!root || !location) {
        GTEST_SKIP() << "Invoked by the storage pipeline test in a fresh process.";
    }
    auto directories =
        NIO::TStoreDirs::Open(std::make_shared<NIO::TPosixLocalFS>(), root, std::make_shared<NIO::TWriteLatch>());
    ASSERT_TRUE(directories);
    auto fileIO = TDurableLocalFileIO::Make(directories.Value(), StoreID);
    ASSERT_TRUE(fileIO);
    const auto recovered = VerifyReachable(*fileIO, location, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage.Snapshots, 2);
    EXPECT_EQ(recovered->Usage.DataFiles, 2);
}

class TPublicationGuardTest : public ::testing::Test
{
protected:
    std::filesystem::path Root_;
    std::shared_ptr<NIO::TStoreDirs> Dirs_;
    std::shared_ptr<TDurableLocalFileIO> IO_;
    std::shared_ptr<NIO::TIOAttempt> Attempt_;
    std::optional<TAttemptScope> Scope_;
    TTableProfile Profile_;
    std::unique_ptr<iceberg::TableMetadata> Metadata_;
    std::string Location_;
    std::string RootLocation_;
    std::vector<std::string> DataLocations_;

    void SetUp() override
    {
        auto path = (std::filesystem::temp_directory_path() / "glamdb-guard-XXXXXX").string();
        ASSERT_NE(::mkdtemp(path.data()), nullptr);
        Root_ = std::filesystem::canonical(path);
        auto dirs = NIO::TStoreDirs::Create(std::make_shared<NIO::TPosixLocalFS>(),
            Root_,
            std::make_shared<NIO::TWriteLatch>());
        ASSERT_TRUE(dirs);
        Dirs_ = std::move(dirs).Value();
        ASSERT_TRUE(Dirs_->CreateCollection(CollectionID));
        auto fileIO = TDurableLocalFileIO::Make(Dirs_, StoreID);
        ASSERT_TRUE(fileIO);
        IO_ = std::move(*fileIO);
        auto attempt =
            NIO::TIOAttempt::Make({StoreID, CollectionID, AttemptID, NIO::EAttemptKind::CreateCollection, std::nullopt},
                Dirs_->Latch());
        ASSERT_TRUE(attempt);
        Attempt_ = std::move(attempt).Value();
        auto scope = IO_->BindAttempt(Attempt_);
        ASSERT_TRUE(scope);
        Scope_.emplace(std::move(*scope));
        auto profile = MakeTableProfile({{{"key", EFieldType::String}}});
        ASSERT_TRUE(profile);
        Profile_ = std::move(*profile);
        Location_ = std::string("store://") + StoreID + "/warehouse/" + CollectionID;
        RootLocation_ = Location_ + "/metadata/00000-" + AttemptID + ".metadata.json";
        auto metadata = iceberg::TableMetadata::Make(*Profile_.Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Location_,
            Profile_.Properties);
        ASSERT_TRUE(metadata);
        Metadata_ = std::move(*metadata);
    }

    void TearDown() override
    {
        Scope_.reset();
        std::error_code error;
        std::filesystem::remove_all(Root_, error);
        EXPECT_FALSE(error);
    }

    TPublicationContext Context() const
    {
        return {.Directories = Dirs_,
            .FileIO = IO_,
            .Attempt = Attempt_,
            .Namespace = "test",
            .Name = "events",
            .Profile = Profile_};
    }

    TPointerChange Change() const
    {
        return {EPointerOp::InsertTable, "test", "events", std::nullopt, RootLocation_};
    }

    void WriteRoot()
    {
        const auto status = iceberg::TableMetadataUtil::Write(*IO_, RootLocation_, *Metadata_);
        ASSERT_TRUE(status) << status.error().message;
    }

    iceberg::Result<std::shared_ptr<const TBaseState>> Append(int index,
        const std::shared_ptr<const TBaseState>& base,
        int rows,
        const std::function<void(TPublicationContext*, TPointerChange*)>& mutate = {})
    {
        Scope_.reset();
        const auto attemptID = std::string("33333333-3333-7333-8333-33333333333") + std::to_string(index + 3);
        const auto operationID = std::string("44444444-4444-4444-8444-44444444444") + std::to_string(index);
        auto attempt = NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, operationID},
            Dirs_->Latch());
        if (!attempt) {
            return iceberg::IOError(attempt.Error().message());
        }
        Attempt_ = std::move(attempt).Value();
        ICEBERG_ASSIGN_OR_RAISE(auto scope, IO_->BindAttempt(Attempt_));
        arrow::StringBuilder builder;
        for (int row = 0; row < rows; ++row) {
            const auto status = builder.Append("duplicate");
            if (!status.ok()) {
                return iceberg::UnknownError(status.ToString());
            }
        }
        auto array = builder.Finish();
        if (!array.ok()) {
            return iceberg::UnknownError(array.status().ToString());
        }
        auto batch =
            arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false)}), rows, {*array});
        const auto dataPath = Location_ + "/data/" + operationID + "-" + attemptID + "-00000.parquet";
        ICEBERG_ASSIGN_OR_RAISE(auto data, WriteDataFile(IO_, Attempt_, *base->Metadata, dataPath, {batch}));
        DataLocations_.push_back(dataPath);
        ICEBERG_ASSIGN_OR_RAISE(auto catalog, iceberg::InMemoryCatalog::Make("guard-test-double", IO_, Location_, {}));
        ICEBERG_ASSIGN_OR_RAISE(auto table,
            iceberg::Table::Make({.ns = {}, .name = "events"},
                std::make_shared<iceberg::TableMetadata>(*base->Metadata),
                base->MetadataLocation,
                IO_,
                catalog));
        const std::unordered_map<std::string, std::string> summary{{"glamdb.operation-id", operationID},
            {"glamdb.content-hash", "test-fixture-hash"}};
        ICEBERG_ASSIGN_OR_RAISE(auto staged, StageAppend(table, data.DataFile, summary, Attempt_));
        const auto location = Location_ + "/metadata/0000" + std::to_string(index) + "-" + attemptID + ".metadata.json";
        ICEBERG_RETURN_UNEXPECTED(iceberg::TableMetadataUtil::Write(*IO_, location, staged.Transaction->current()));
        auto context = Context();
        context.Base = base;
        context.StoreUsage = base->Usage;
        context.ExpectedSummary = summary;
        TPointerChange change{EPointerOp::UpdateLocation, "test", "events", base->MetadataLocation, location};
        if (mutate) {
            mutate(&context, &change);
        }
        TStoragePublicationGuard guard(std::move(context));
        ICEBERG_RETURN_UNEXPECTED(guard.BeforePointerChange(change));
        return guard.GetCandidate();
    }

    void CheckRows(const std::string& path, std::int64_t count)
    {
        auto bytes = IO_->ReadFile(path, std::nullopt);
        ASSERT_TRUE(bytes) << bytes.error().message;
        auto source = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(*bytes)));
        auto reader = parquet::arrow::OpenFile(source, arrow::default_memory_pool());
        ASSERT_TRUE(reader.ok()) << reader.status();
        auto table = (*reader)->ReadTable();
        ASSERT_TRUE(table.ok()) << table.status();
        EXPECT_EQ((*table)->num_rows(), count);
        ASSERT_EQ((*table)->num_columns(), 1);
        for (const auto& chunk : (*table)->column(0)->chunks()) {
            const auto strings = std::dynamic_pointer_cast<arrow::StringArray>(chunk);
            ASSERT_NE(strings, nullptr);
            for (std::int64_t row = 0; row < strings->length(); ++row) {
                EXPECT_EQ(strings->GetView(row), "duplicate");
            }
        }
    }
};

TEST_F(TPublicationGuardTest, VerifiesCreateWithoutAdvancingCallerState)
{
    WriteRoot();
    TStoragePublicationGuard guard(Context());
    EXPECT_EQ(guard.GetCandidate(), nullptr);
    const auto status = guard.BeforePointerChange(Change());
    ASSERT_TRUE(status) << status.error().message;
    const auto state = guard.GetCandidate();
    ASSERT_NE(state, nullptr);
    EXPECT_EQ(state->MetadataLocation, RootLocation_);
    EXPECT_EQ(state->Metadata->table_uuid, Metadata_->table_uuid);
    EXPECT_GT(state->Usage.RootBytes, 0);
    EXPECT_EQ(state->Usage.Snapshots, 0);
    EXPECT_TRUE(Attempt_->Record(RootLocation_).Value().FormatVerified);
    Scope_.reset();
    const auto recovered = VerifyReachable(IO_, RootLocation_, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage, state->Usage);
    EXPECT_EQ(*recovered->Metadata, *state->Metadata);
}

TEST_F(TPublicationGuardTest, RejectsMissingReceiptAndInvalidJson)
{
    TStoragePublicationGuard missing(Context());
    EXPECT_FALSE(missing.BeforePointerChange(Change()));
    EXPECT_EQ(missing.GetCandidate(), nullptr);
}

TEST_F(TPublicationGuardTest, RejectsMalformedDurableMetadata)
{
    ASSERT_TRUE(IO_->WriteFile(RootLocation_, "{invalid"));
    TStoragePublicationGuard guard(Context());
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
    EXPECT_EQ(guard.GetCandidate(), nullptr);
    EXPECT_TRUE(Attempt_->FirstError());
}

TEST_F(TPublicationGuardTest, RejectsChangedFileSizeAfterDurableClose)
{
    WriteRoot();
    const auto path =
        Root_ / "warehouse" / CollectionID / "metadata" / (std::string("00000-") + AttemptID + ".metadata.json");
    std::filesystem::resize_file(path, 1);
    TStoragePublicationGuard guard(Context());
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
}

TEST_F(TPublicationGuardTest, RejectsProfileMutationAndRecoveryWithDifferentIdentity)
{
    Metadata_->properties = iceberg::TableProperties::FromMap({});
    WriteRoot();
    TStoragePublicationGuard guard(Context());
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
    Scope_.reset();
    EXPECT_FALSE(VerifyReachable(IO_, RootLocation_, StoreID, CollectionID));
    EXPECT_FALSE(VerifyReachable(IO_, RootLocation_, CollectionID, StoreID));
}

TEST_F(TPublicationGuardTest, RejectsUnsupportedProfileBeforeAuthorizingPublication)
{
    Profile_.Properties.emplace("glamdb.collection-id", CollectionID);
    Metadata_->properties = iceberg::TableProperties::FromMap(Profile_.Properties);
    WriteRoot();
    TStoragePublicationGuard guard(Context());
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
    EXPECT_EQ(guard.GetCandidate(), nullptr);
    EXPECT_TRUE(Attempt_->FirstError());
    Scope_.reset();
    EXPECT_FALSE(VerifyReachable(IO_, RootLocation_, StoreID, CollectionID));
}

class TPublicationIdentityTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TPublicationIdentityTest, RejectsUnpreparedPointerChanges)
{
    WriteRoot();
    auto change = Change();
    switch (GetParam()) {
        case 0:
            change.Op = EPointerOp::UpdateLocation;
            break;
        case 1:
            change.Namespace = "other";
            break;
        case 2:
            change.Name = "other";
            break;
        case 3:
            change.ExpectedCurrentLocation = RootLocation_;
            break;
        case 4:
            change.NewLocation = Location_ + "/data/file.parquet";
            break;
        default:
            change.NewLocation = "store://invalid/metadata/root.json";
            break;
    }
    TStoragePublicationGuard guard(Context());
    EXPECT_FALSE(guard.BeforePointerChange(change));
    EXPECT_EQ(guard.GetCandidate(), nullptr);
}

INSTANTIATE_TEST_SUITE_P(PointerIdentity, TPublicationIdentityTest, ::testing::Range(0, 6));

TEST_F(TPublicationGuardTest, RejectsStoreCapacityAndRootReplacement)
{
    WriteRoot();
    auto context = Context();
    context.Limits.Store.RootBytes = 0;
    TStoragePublicationGuard guard(std::move(context));
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
    EXPECT_EQ(guard.GetCandidate(), nullptr);
}

TEST_F(TPublicationGuardTest, RejectsAnUnfinishedMetadataStream)
{
    auto output = IO_->NewOutputFile(RootLocation_);
    ASSERT_TRUE(output);
    auto stream = (*output)->Create();
    ASSERT_TRUE(stream);
    TStoragePublicationGuard guard(Context());
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
    EXPECT_FALSE((*stream)->Close());
    EXPECT_EQ(guard.GetCandidate(), nullptr);
}

TEST_F(TPublicationGuardTest, CannotAuthorizeTwoPointerChanges)
{
    WriteRoot();
    TStoragePublicationGuard guard(Context());
    ASSERT_TRUE(guard.BeforePointerChange(Change()));
    EXPECT_FALSE(guard.BeforePointerChange(Change()));
    EXPECT_EQ(guard.GetCandidate(), nullptr);
}

TEST_F(TPublicationGuardTest, WritesTwoAppendsAndReopensEveryRetainedFile)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto empty = create.GetCandidate();
    auto first = Append(1, empty, 1000);
    ASSERT_TRUE(first) << first.error().message;
    auto second = Append(2, *first, 10);
    ASSERT_TRUE(second) << second.error().message;
    EXPECT_EQ(empty->Usage.Snapshots, 0);
    EXPECT_EQ((*first)->Usage.Snapshots, 1);
    EXPECT_EQ((*second)->Usage.Snapshots, 2);
    EXPECT_EQ((*second)->Usage.DataFiles, 2);
    EXPECT_EQ((*second)->CurrentManifests.size(), 2);
    EXPECT_GT((*second)->Usage.FooterBytes, (*first)->Usage.FooterBytes);
    auto reopenedDirs =
        NIO::TStoreDirs::Open(std::make_shared<NIO::TPosixLocalFS>(), Root_, std::make_shared<NIO::TWriteLatch>());
    ASSERT_TRUE(reopenedDirs);
    auto reopenedIO = TDurableLocalFileIO::Make(reopenedDirs.Value(), StoreID);
    ASSERT_TRUE(reopenedIO);
    const auto recovered = VerifyReachable(*reopenedIO, (*second)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage, (*second)->Usage);
    EXPECT_EQ(*recovered->Metadata, *(*second)->Metadata);
    ASSERT_EQ(DataLocations_.size(), 2);
    CheckRows(DataLocations_[0], 1000);
    CheckRows(DataLocations_[1], 10);
    ExpectFreshProcess(Root_, (*second)->MetadataLocation);
}

TEST_F(TPublicationGuardTest, ValidatesDelayedCommittedRootsAndReopensHistory)
{
    const auto catalog = iceberg::InMemoryCatalog::Make("delayed-commit-test-double", IO_, Location_, {});
    ASSERT_TRUE(catalog);
    auto created = (*catalog)->CreateTable({.ns = {}, .name = "events"},
        Profile_.Schema,
        iceberg::PartitionSpec::Unpartitioned(),
        iceberg::SortOrder::Unsorted(),
        Location_,
        Profile_.Properties);
    ASSERT_TRUE(created) << created.error().message;
    auto table = *created;
    RootLocation_ = table->metadata_file_location();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    auto base = create.GetCandidate();
    Scope_.reset();

    for (int index = 1; index <= 2; ++index) {
        const auto attemptID = std::string("33333333-3333-7333-8333-33333333333") + std::to_string(index + 3);
        const auto operationID = std::string("44444444-4444-4444-8444-44444444444") + std::to_string(index);
        auto attempt = NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, operationID},
            Dirs_->Latch());
        ASSERT_TRUE(attempt);
        Attempt_ = std::move(attempt).Value();
        auto scope = IO_->BindAttempt(Attempt_);
        ASSERT_TRUE(scope);
        Scope_.emplace(std::move(*scope));
        arrow::StringBuilder builder;
        ASSERT_TRUE(builder.Append("duplicate").ok());
        auto array = builder.Finish();
        ASSERT_TRUE(array.ok());
        const auto batch =
            arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false)}), 1, {*array});
        auto path = Location_;
        path.append("/data/").append(operationID).append("-").append(attemptID).append("-00000.parquet");
        const auto data = WriteDataFile(IO_, Attempt_, *table->metadata(), path, {batch});
        ASSERT_TRUE(data) << data.error().message;
        const std::unordered_map<std::string, std::string> summary{{"glamdb.operation-id", operationID}};
        auto staged = StageAppend(table, data->DataFile, summary, Attempt_);
        ASSERT_TRUE(staged) << staged.error().message;
        const auto snapshot = staged->Transaction->current().Snapshot();
        ASSERT_TRUE(snapshot);
        const auto snapshotTime = (*snapshot)->timestamp_ms;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        const auto committed = staged->Transaction->Commit();
        ASSERT_TRUE(committed) << committed.error().message;
        table = *committed;
        ASSERT_NE(table->metadata()->last_updated_ms, snapshotTime);
        EXPECT_EQ(table->metadata()->snapshot_log.back().timestamp_ms, snapshotTime);

        auto context = Context();
        context.Base = base;
        context.StoreUsage = base->Usage;
        context.ExpectedSummary = summary;
        TStoragePublicationGuard guard(std::move(context));
        const auto checked = guard.BeforePointerChange({EPointerOp::UpdateLocation,
            "test",
            "events",
            base->MetadataLocation,
            std::string(table->metadata_file_location())});
        ASSERT_TRUE(checked) << checked.error().message;
        base = guard.GetCandidate();
        ASSERT_NE(base, nullptr);
        Scope_.reset();
        const auto readOnly = IO_->ReadOnlyView();
        ASSERT_TRUE(readOnly);
        const auto recovered = VerifyReachable(*readOnly, base->MetadataLocation, StoreID, CollectionID);
        ASSERT_TRUE(recovered) << recovered.error().message;
        EXPECT_EQ(recovered->Usage, base->Usage);
        EXPECT_EQ(*recovered->Metadata, *table->metadata());
        CheckRows(path, 1);
    }
    ExpectFreshProcess(Root_, base->MetadataLocation);
}

class TAppendBoundaryTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TAppendBoundaryTest, RejectsPublicationWithoutAdvancingThePreparedBase)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto empty = create.GetCandidate();
    const auto result = Append(1, empty, 1, [&](auto* context, auto* change) {
        switch (GetParam()) {
            case 0:
                change->ExpectedCurrentLocation = "different-current-root";
                break;
            case 1:
                context->Base.reset();
                break;
            case 2:
                context->ExpectedSummary["glamdb.content-hash"] = "wrong-hash";
                break;
            case 3:
                context->Limits.Store.DataFiles = 0;
                break;
            case 4:
                context->Limits.NewManifests = 0;
                break;
            case 5:
                context->Limits.CommitBytes = 1;
                break;
            case 6:
                context->Limits.ManifestBytes = 1;
                break;
            case 7:
                context->Limits.ManifestListBytes = 1;
                break;
            case 8:
                context->Limits.Store.FooterBytes = 0;
                break;
            default:
                context->Directories->Latch()->Block();
                break;
        }
    });
    EXPECT_FALSE(result);
    EXPECT_EQ(empty->MetadataLocation, RootLocation_);
    EXPECT_EQ(empty->Usage.Snapshots, 0);
    EXPECT_TRUE(empty->CurrentManifests.empty());
}

INSTANTIATE_TEST_SUITE_P(PublicationBoundary, TAppendBoundaryTest, ::testing::Range(0, 10));

}
}
