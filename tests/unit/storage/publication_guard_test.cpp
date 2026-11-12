#include "core/io/durable_copy.h"
#include "core/storage/append_stager.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/format_verifier.h"
#include "core/storage/publication_guard.h"
#include "tests/support/fault_fs.h"
#include "tests/support/temp_dir.h"

#include <arrow/array/array_binary.h>
#include <arrow/array/builder_binary.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <format>
#include <gtest/gtest.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/manifest/manifest_writer.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>
#include <parquet/arrow/reader.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <limits>
#include <spawn.h>
#include <sys/wait.h>
#include <thread>
#include <tuple>

extern char** environ;

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
constexpr auto AttemptID = "33333333-3333-7333-8333-333333333333";

void ExpectFreshProcess(const std::filesystem::path& root,
    const std::string& location,
    std::optional<std::uint64_t> expectedSnapshots = 2,
    std::string_view corruptFile = {},
    std::string_view validationError = {})
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        const std::string_view value(*entry);
        if (!value.starts_with("GLAMDB_RECOVERY_ROOT=") && !value.starts_with("GLAMDB_RECOVERY_METADATA=") &&
            !value.starts_with("GLAMDB_RECOVERY_EXPECTED_SNAPSHOTS="))
        {
            environment.emplace_back(value);
        }
    }
    environment.push_back("GLAMDB_RECOVERY_ROOT=" + root.string());
    environment.push_back("GLAMDB_RECOVERY_METADATA=" + location);
    const auto expected = expectedSnapshots ? std::to_string(*expectedSnapshots)
        : validationError.empty()           ? "CORRUPTION: " + std::string(corruptFile)
                                            : "VALIDATION: " + std::string(validationError);
    environment.push_back("GLAMDB_RECOVERY_EXPECTED_SNAPSHOTS=" + expected);
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
    static_cast<void>(expectedSnapshots);
    static_cast<void>(corruptFile);
    static_cast<void>(validationError);
    GTEST_SKIP() << "Fresh-process recovery is checked on Linux.";
#endif
}

TEST(RecoveryChildProcess, ReopensPublishedRoot)
{
    const auto* root = std::getenv("GLAMDB_RECOVERY_ROOT");
    const auto* location = std::getenv("GLAMDB_RECOVERY_METADATA");
    const auto* expected = std::getenv("GLAMDB_RECOVERY_EXPECTED_SNAPSHOTS");
    if (!root || !location) {
        GTEST_SKIP() << "Invoked by the storage pipeline test in a fresh process.";
    }
    auto directories =
        NIO::TStoreDirs::Open(std::make_shared<NIO::TPosixLocalFS>(), root, std::make_shared<NIO::TWriteLatch>());
    ASSERT_TRUE(directories);
    auto fileIO = TDurableLocalFileIO::Make(directories.Value(), StoreID);
    ASSERT_TRUE(fileIO);
    const auto recovered = VerifyReachable(*fileIO, location, StoreID, CollectionID);
    ASSERT_NE(expected, nullptr);
    const std::string_view expectation(expected);
    if (expectation.starts_with("VALIDATION: ")) {
        ASSERT_FALSE(recovered);
        EXPECT_EQ(recovered.error().kind, iceberg::ErrorKind::kValidationFailed);
        EXPECT_EQ(recovered.error().message, expectation.substr(std::string_view("VALIDATION: ").size()));
        return;
    }
    if (expectation.starts_with("CORRUPTION: ")) {
        ASSERT_FALSE(recovered);
        EXPECT_EQ(recovered.error().kind, iceberg::ErrorKind::kInvalid);
        EXPECT_TRUE(recovered.error().message.starts_with(expectation)) << recovered.error().message;
        return;
    }
    std::uint64_t snapshots = 0;
    const auto parsed = std::from_chars(expectation.data(), expectation.data() + expectation.size(), snapshots);
    ASSERT_EQ(parsed.ec, std::errc{});
    ASSERT_EQ(parsed.ptr, expectation.data() + expectation.size());
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage.Snapshots, snapshots);
    EXPECT_EQ(recovered->Usage.DataFiles, snapshots);
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
        const std::function<void(TPublicationContext*, TPointerChange*)>& mutate = {},
        const std::string& retainedDataPath = {},
        int bulkFiles = 0,
        bool duplicateBulkFile = false,
        const std::function<void(std::vector<iceberg::ManifestEntry>&)>& rewriteEntries = {})
    {
        Scope_.reset();
        const auto attemptID = std::string("33333333-3333-7333-8333-33333333333") + std::to_string(index + 3);
        const auto operationID = std::string("44444444-4444-4444-8444-44444444444") + std::to_string(index);
        auto attempt = NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, operationID},
            Dirs_->Latch(),
            bulkFiles ? NIO::TAttemptLimits::BulkAppend() : NIO::TAttemptLimits{});
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
        const auto reusesData = !retainedDataPath.empty();
        if (reusesData) {
            data.DataFile->file_path = retainedDataPath;
        }
        DataLocations_.push_back(dataPath);
        std::vector<std::shared_ptr<iceberg::DataFile>> dataFiles{data.DataFile};
        for (int file = 1; file < bulkFiles; ++file) {
            const auto path = std::format("{}/data/{}-{}-{:05}.parquet", Location_, operationID, attemptID, file);
            ICEBERG_ASSIGN_OR_RAISE(auto next, WriteDataFile(IO_, Attempt_, *base->Metadata, path, {batch}));
            dataFiles.push_back(next.DataFile);
            DataLocations_.push_back(path);
        }
        ICEBERG_ASSIGN_OR_RAISE(auto catalog, iceberg::InMemoryCatalog::Make("guard-test-double", IO_, Location_, {}));
        ICEBERG_ASSIGN_OR_RAISE(auto table,
            iceberg::Table::Make({.ns = {}, .name = "events"},
                std::make_shared<iceberg::TableMetadata>(*base->Metadata),
                base->MetadataLocation,
                IO_,
                catalog));
        std::unordered_map<std::string, std::string> summary{{"glamdb.operation-id", operationID},
            {"glamdb.content-hash", "test-fixture-hash"}};
        if (bulkFiles) {
            summary.emplace("glamdb.bulk-version", "1");
        }
        std::shared_ptr<iceberg::Transaction> transaction;
        std::shared_ptr<iceberg::TableMetadata> stagedMetadata;
        if (rewriteEntries) {
            stagedMetadata = std::make_shared<iceberg::TableMetadata>(*base->Metadata);
            auto snapshot = std::make_shared<iceberg::Snapshot>();
            snapshot->snapshot_id = 1690000 + index;
            snapshot->parent_snapshot_id = base->Metadata->current_snapshot_id;
            snapshot->sequence_number = ++stagedMetadata->last_sequence_number;
            stagedMetadata->last_updated_ms += std::chrono::milliseconds(1);
            snapshot->timestamp_ms = stagedMetadata->last_updated_ms;
            snapshot->schema_id = stagedMetadata->current_schema_id;
            snapshot->summary = summary;
            snapshot->summary["operation"] = "append";
            snapshot->summary[iceberg::SnapshotSummaryFields::kAddedRecords] = std::to_string(rows);
            snapshot->summary[iceberg::SnapshotSummaryFields::kAddedDataFiles] = "1";
            snapshot->summary[iceberg::SnapshotSummaryFields::kAddedFileSize] =
                std::to_string(data.DataFile->file_size_in_bytes);
            stagedMetadata->current_snapshot_id = snapshot->snapshot_id;
            stagedMetadata->snapshots.push_back(snapshot);
            stagedMetadata->snapshot_log.push_back({snapshot->timestamp_ms, snapshot->snapshot_id});
            stagedMetadata->refs["main"] =
                std::make_shared<iceberg::SnapshotRef>(iceberg::SnapshotRef{.snapshot_id = snapshot->snapshot_id,
                    .retention = iceberg::SnapshotRef::Branch{}});
        } else if (bulkFiles && !duplicateBulkFile) {
            ICEBERG_ASSIGN_OR_RAISE(auto staged, StageBulkAppend(table, dataFiles, summary, Attempt_));
            transaction = std::move(staged.Transaction);
        } else if (!reusesData && !duplicateBulkFile) {
            ICEBERG_ASSIGN_OR_RAISE(auto staged, StageAppend(table, data.DataFile, summary, Attempt_));
            transaction = std::move(staged.Transaction);
        } else {
            ICEBERG_ASSIGN_OR_RAISE(transaction,
                iceberg::Transaction::Make(std::move(table), iceberg::TransactionKind::kUpdate));
            ICEBERG_ASSIGN_OR_RAISE(auto append, transaction->NewFastAppend());
            if (duplicateBulkFile) {
                ICEBERG_ASSIGN_OR_RAISE(auto spec, base->Metadata->PartitionSpec());
                ICEBERG_ASSIGN_OR_RAISE(auto schema, base->Metadata->Schema());
                ICEBERG_ASSIGN_OR_RAISE(auto writer,
                    iceberg::ManifestWriter::MakeWriter(2,
                        std::nullopt,
                        Location_ + "/metadata/" + attemptID + "-m0.avro",
                        IO_,
                        spec,
                        schema));
                ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(data.DataFile));
                ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(data.DataFile));
                ICEBERG_RETURN_UNEXPECTED(writer->Close());
                ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());
                append->AppendManifest(manifest);
            } else {
                append->AppendFile(data.DataFile);
            }
            for (const auto& [key, value] : summary) {
                append->Set(key, value);
            }
            ICEBERG_RETURN_UNEXPECTED(append->Commit());
            if (duplicateBulkFile) {
                ICEBERG_ASSIGN_OR_RAISE(auto snapshot, transaction->current().Snapshot());
                snapshot->summary[iceberg::SnapshotSummaryFields::kAddedRecords] = std::to_string(2 * rows);
                snapshot->summary[iceberg::SnapshotSummaryFields::kAddedDataFiles] = "2";
                snapshot->summary[iceberg::SnapshotSummaryFields::kAddedFileSize] =
                    std::to_string(2 * data.DataFile->file_size_in_bytes);
            }
        }
        if (!stagedMetadata) {
            stagedMetadata = std::make_shared<iceberg::TableMetadata>(transaction->current());
        }
        if (rewriteEntries) {
            ICEBERG_ASSIGN_OR_RAISE(auto snapshot, stagedMetadata->Snapshot());
            ICEBERG_ASSIGN_OR_RAISE(auto schema, base->Metadata->Schema());
            ICEBERG_ASSIGN_OR_RAISE(auto spec, base->Metadata->PartitionSpec());
            std::vector<iceberg::ManifestEntry> entries{{.status = iceberg::ManifestStatus::kAdded,
                .snapshot_id = snapshot->snapshot_id,
                .sequence_number = snapshot->sequence_number,
                .file_sequence_number = snapshot->sequence_number,
                .data_file = data.DataFile}};
            for (const auto& manifest : base->CurrentManifests) {
                ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestReader::Make(manifest, IO_, schema, spec));
                ICEBERG_ASSIGN_OR_RAISE(auto files, reader->Entries());
                for (const auto& entry : files) {
                    entries.push_back(entry.AsExisting());
                }
            }
            rewriteEntries(entries);
            ICEBERG_ASSIGN_OR_RAISE(auto writer,
                iceberg::ManifestWriter::MakeWriter(2,
                    snapshot->snapshot_id,
                    Location_ + "/metadata/" + attemptID + "-m7.avro",
                    IO_,
                    spec,
                    schema));
            for (const auto& entry : entries) {
                ICEBERG_RETURN_UNEXPECTED(entry.status == iceberg::ManifestStatus::kAdded
                        ? writer->WriteAddedEntry(entry)
                        : entry.status == iceberg::ManifestStatus::kDeleted ? writer->WriteDeletedEntry(entry)
                                                                            : writer->WriteExistingEntry(entry));
            }
            ICEBERG_RETURN_UNEXPECTED(writer->Close());
            ICEBERG_ASSIGN_OR_RAISE(auto manifest, writer->ToManifestFile());
            const auto list = std::format("{}/metadata/snap-{}-1-{}.avro", Location_, snapshot->snapshot_id, attemptID);
            ICEBERG_ASSIGN_OR_RAISE(auto listWriter,
                iceberg::ManifestListWriter::MakeWriter(2,
                    snapshot->snapshot_id,
                    snapshot->parent_snapshot_id,
                    list,
                    IO_,
                    snapshot->sequence_number));
            ICEBERG_RETURN_UNEXPECTED(listWriter->Add(manifest));
            ICEBERG_RETURN_UNEXPECTED(listWriter->Close());
            snapshot->manifest_list = list;
        }
        const auto location = Location_ + "/metadata/0000" + std::to_string(index) + "-" + attemptID + ".metadata.json";
        ICEBERG_RETURN_UNEXPECTED(iceberg::TableMetadataUtil::Write(*IO_, location, *stagedMetadata));
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

    iceberg::Result<std::string> WriteRecoveryRoot(const iceberg::TableMetadata& metadata,
        std::string_view version = "00009")
    {
        constexpr auto attemptID = "33333333-3333-7333-8333-333333333339";
        constexpr auto operationID = "44444444-4444-4444-8444-444444444449";
        auto attempt = NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, operationID},
            Dirs_->Latch());
        if (!attempt) {
            return iceberg::IOError(attempt.Error().message());
        }
        ICEBERG_ASSIGN_OR_RAISE(auto scope, IO_->BindAttempt(attempt.Value()));
        auto location = Location_;
        location.append("/metadata/").append(version).append("-").append(attemptID).append(".metadata.json");
        ICEBERG_RETURN_UNEXPECTED(iceberg::TableMetadataUtil::Write(*IO_, location, metadata));
        return location;
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

TEST_F(TPublicationGuardTest, MergedClosurePreservesMembershipAndIdenticalRecoveryAccounting)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 3);
    ASSERT_TRUE(first) << first.error().message;
    const auto second = Append(2, *first, 2, {}, {}, 0, false, [](auto&) {
    });
    ASSERT_TRUE(second) << second.error().message;
    EXPECT_EQ((*second)->CurrentManifests.size(), 1U);
    EXPECT_EQ((*second)->Usage.Manifests, 2U);
    EXPECT_EQ((*second)->Usage.DataFiles, 2U);
    ASSERT_EQ((*second)->ScanIndex->Files.size(), 2U);
    EXPECT_EQ((*second)->ScanIndex->Files[0].RecordCount, 3);
    EXPECT_EQ((*second)->ScanIndex->Files[1].RecordCount, 2);
    const auto recovered = VerifyReachable(IO_, (*second)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage, (*second)->Usage);
    EXPECT_EQ(recovered->ScanIndex->Files, (*second)->ScanIndex->Files);
    EXPECT_TRUE(VerifyRetainedSnapshot(IO_,
        (*second)->MetadataLocation,
        StoreID,
        CollectionID,
        (*second)->Metadata->current_snapshot_id));
    ExpectFreshProcess(Root_, (*second)->MetadataLocation);
}

class TMergedClosureCorruptionTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TMergedClosureCorruptionTest, RejectsChangesToExistingEntries)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 3);
    ASSERT_TRUE(first) << first.error().message;
    const auto second = Append(2, *first, 2, {}, {}, 0, false, [](auto& entries) {
        const auto existing =
            std::ranges::find(entries, iceberg::ManifestStatus::kExisting, &iceberg::ManifestEntry::status);
        ASSERT_NE(existing, entries.end());
        switch (GetParam()) {
            case 0:
                entries.erase(existing);
                break;
            case 1:
                entries.push_back(*existing);
                break;
            case 2:
                existing->snapshot_id = 123;
                break;
            case 3:
                existing->file_sequence_number = 123;
                break;
            case 4: {
                existing->data_file = std::make_shared<iceberg::DataFile>(*existing->data_file);
                ++existing->data_file->record_count;
                break;
            }
            case 5:
                existing->status = iceberg::ManifestStatus::kDeleted;
                break;
            case 6: {
                existing->data_file = std::make_shared<iceberg::DataFile>(*existing->data_file);
                existing->data_file->value_counts[1] = 123;
                break;
            }
            default:
                FAIL() << "Invalid corruption test mode";
        }
    });
    EXPECT_FALSE(second);
    const auto readOnly = IO_->ReadOnlyView();
    ASSERT_TRUE(readOnly);
    EXPECT_TRUE(VerifyReachable(*readOnly, (*first)->MetadataLocation, StoreID, CollectionID));
}

INSTANTIATE_TEST_SUITE_P(Membership, TMergedClosureCorruptionTest, ::testing::Range(0, 7));

class TMergedClosureLimitTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TMergedClosureLimitTest, CountsRewriteInputsAtTheExactCommitByteBoundary)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 3);
    ASSERT_TRUE(first) << first.error().message;
    TStorageLimits limits;
    const auto second = Append(
        2,
        *first,
        2,
        [&](TPublicationContext* context, auto*) {
            auto bytes = static_cast<std::uint64_t>((*first)->CurrentManifests[0].manifest_length);
            for (const auto& record : context->Attempt->Records()) {
                if (record.Kind != NIO::EFileKind::Data) {
                    bytes += record.Length;
                }
            }
            limits.CommitBytes = bytes - (GetParam() ? 0 : 1);
            context->Limits = limits;
        },
        {},
        0,
        false,
        [](auto&) {
        });
    if (GetParam()) {
        ASSERT_TRUE(second) << second.error().message;
        const auto recovered = VerifyReachable(IO_, (*second)->MetadataLocation, StoreID, CollectionID, limits);
        ASSERT_TRUE(recovered) << recovered.error().message;
        EXPECT_EQ(recovered->Usage, (*second)->Usage);
        --limits.CommitBytes;
        EXPECT_FALSE(VerifyReachable(IO_, (*second)->MetadataLocation, StoreID, CollectionID, limits));
    } else {
        ASSERT_FALSE(second);
        EXPECT_TRUE(second.error().message.starts_with("CAPACITY:"));
    }
}

INSTANTIATE_TEST_SUITE_P(Bytes, TMergedClosureLimitTest, ::testing::Bool());

TEST_F(TPublicationGuardTest, OrdinaryMergePreservesEveryFileOfAnEarlierBulkAppend)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 3, {}, {}, 2);
    ASSERT_TRUE(first) << first.error().message;
    const auto second = Append(2, *first, 2, {}, {}, 0, false, [](auto&) {
    });
    ASSERT_TRUE(second) << second.error().message;
    EXPECT_EQ((*second)->CurrentManifests.size(), 1U);
    EXPECT_EQ((*second)->Usage.DataFiles, 3U);
    const auto recovered = VerifyReachable(IO_, (*second)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage, (*second)->Usage);
    EXPECT_EQ(recovered->ScanIndex->Files, (*second)->ScanIndex->Files);
}

TEST_F(TPublicationGuardTest, BulkClosureAndRecoveryVerifyEveryFile)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto bulk = Append(1, create.GetCandidate(), 3, {}, {}, 2);
    ASSERT_TRUE(bulk) << bulk.error().message;
    EXPECT_EQ((*bulk)->Usage.DataFiles, 2);
    EXPECT_EQ((*bulk)->Usage.Snapshots, 1);
    ASSERT_NE((*bulk)->ScanIndex, nullptr);
    EXPECT_EQ((*bulk)->ScanIndex->Files.size(), 2);
    const auto next = Append(2, *bulk, 1);
    ASSERT_TRUE(next) << next.error().message;
    const auto recovered = VerifyReachable(IO_, (*next)->MetadataLocation, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage, (*next)->Usage);
    ASSERT_EQ(DataLocations_.size(), 3);
    CheckRows(DataLocations_[0], 3);
    CheckRows(DataLocations_[1], 3);
    CheckRows(DataLocations_[2], 1);
}

TEST_F(TPublicationGuardTest, BulkRecoveryRejectsMissingOrUnsupportedMarker)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto bulk = Append(1, create.GetCandidate(), 3, {}, {}, 2);
    ASSERT_TRUE(bulk) << bulk.error().message;
    int version = 10;
    for (const auto marker : {std::string_view{}, std::string_view{"2"}}) {
        auto metadata = std::make_shared<iceberg::TableMetadata>(*(*bulk)->Metadata);
        auto snapshot = std::make_shared<iceberg::Snapshot>(*metadata->snapshots.front());
        if (marker.empty()) {
            snapshot->summary.erase("glamdb.bulk-version");
        } else {
            snapshot->summary["glamdb.bulk-version"] = marker;
        }
        metadata->snapshots.front() = snapshot;
        const auto root = WriteRecoveryRoot(*metadata, "000" + std::to_string(version++));
        ASSERT_TRUE(root) << root.error().message;
        const auto recovered = VerifyReachable(IO_, *root, StoreID, CollectionID);
        ASSERT_FALSE(recovered);
        EXPECT_EQ(recovered.error().kind,
            marker.empty() ? iceberg::ErrorKind::kInvalidArgument : iceberg::ErrorKind::kValidationFailed)
            << recovered.error().message;
    }
}

TEST_F(TPublicationGuardTest, BulkRecoveryRejectsSummaryCountersDifferentFromActualFiles)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto bulk = Append(1, create.GetCandidate(), 3, {}, {}, 2);
    ASSERT_TRUE(bulk) << bulk.error().message;
    int version = 10;
    for (const auto& key : {iceberg::SnapshotSummaryFields::kAddedRecords,
             iceberg::SnapshotSummaryFields::kAddedDataFiles,
             iceberg::SnapshotSummaryFields::kAddedFileSize})
    {
        auto metadata = std::make_shared<iceberg::TableMetadata>(*(*bulk)->Metadata);
        auto snapshot = std::make_shared<iceberg::Snapshot>(*metadata->snapshots.front());
        snapshot->summary[key] = "18446744073709551616";
        metadata->snapshots.front() = snapshot;
        const auto root = WriteRecoveryRoot(*metadata, "000" + std::to_string(version++));
        ASSERT_TRUE(root) << root.error().message;
        const auto recovered = VerifyReachable(IO_, *root, StoreID, CollectionID);
        ASSERT_FALSE(recovered);
        EXPECT_EQ(recovered.error().kind, iceberg::ErrorKind::kValidationFailed);
    }
}

TEST_F(TPublicationGuardTest, BulkClosureRejectsDuplicateDataPathsBeforePublication)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    std::string root;
    const auto duplicate = Append(
        1,
        create.GetCandidate(),
        3,
        [&](auto* /*context*/, auto* change) {
            root = change->NewLocation;
        },
        {},
        1,
        true);
    ASSERT_FALSE(duplicate);
    EXPECT_NE(duplicate.error().message.find("reuses"), std::string::npos) << duplicate.error().message;
    ASSERT_FALSE(root.empty());
    const auto readOnly = IO_->ReadOnlyView();
    ASSERT_TRUE(readOnly);
    EXPECT_FALSE(VerifyReachable(*readOnly, root, StoreID, CollectionID));
}

// An append's staged root replaces the parse of its durable file only when
// the file holds exactly its bytes; the verification mode also requires the
// parse of those bytes to equal the staged metadata.
TEST_F(TPublicationGuardTest, StagedRootRequiresTheDurableBytesAndRoundTrips)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto empty = create.GetCandidate();
    enum class EStaged : std::uint8_t { Exact, Verified, ChangedByte, Truncated, DifferentMetadata };
    std::shared_ptr<const iceberg::TableMetadata> stagedMetadata;
    const auto stage = [&](EStaged mode) {
        return [&, mode](TPublicationContext* context, TPointerChange* change) {
            auto bytes = IO_->ReadFile(change->NewLocation, std::nullopt);
            ASSERT_TRUE(bytes) << bytes.error().message;
            auto parsed = ParseTableMetadata(*bytes);
            ASSERT_TRUE(parsed) << parsed.error().message;
            if (mode == EStaged::DifferentMetadata) {
                (*parsed)->last_updated_ms += std::chrono::milliseconds(1);
            }
            if (mode == EStaged::ChangedByte) {
                bytes->back() = bytes->back() == ' ' ? '\n' : ' ';
            }
            if (mode == EStaged::Truncated) {
                bytes->pop_back();
            }
            stagedMetadata = std::shared_ptr<const iceberg::TableMetadata>(std::move(*parsed));
            context->StagedRoot = TStagedRoot{stagedMetadata, std::make_shared<const std::string>(std::move(*bytes))};
            context->VerifyStagedRoot = mode != EStaged::Exact;
        };
    };
    const auto parses = TableMetadataParses();
    auto first = Append(1, empty, 3, stage(EStaged::Exact));
    ASSERT_TRUE(first) << first.error().message;
    EXPECT_EQ((*first)->Metadata, stagedMetadata) << "the candidate is the staged metadata";
    EXPECT_EQ(TableMetadataParses(), parses + 1) << "only the test parsed the root";
    auto second = Append(2, *first, 3, stage(EStaged::Verified));
    ASSERT_TRUE(second) << second.error().message;
    EXPECT_EQ((*second)->Metadata, stagedMetadata);
    int index = 3;
    for (const auto mode : {EStaged::ChangedByte, EStaged::Truncated, EStaged::DifferentMetadata}) {
        const auto rejected = Append(index++, *second, 3, stage(mode));
        ASSERT_FALSE(rejected);
        if (mode == EStaged::Truncated) {
            EXPECT_EQ(rejected.error().kind, iceberg::ErrorKind::kValidationFailed) << rejected.error().message;
        } else {
            EXPECT_EQ(rejected.error().kind, iceberg::ErrorKind::kInvalid) << rejected.error().message;
            EXPECT_TRUE(rejected.error().message.starts_with("CORRUPTION: ")) << rejected.error().message;
        }
    }
}

TEST_F(TPublicationGuardTest, StagedRootVerificationComparesSnapshotSummaryAndManifestList)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto empty = create.GetCandidate();
    // The pinned Iceberg snapshot equality ignores the summary and the
    // manifest list, so the durable bytes stay exact and only the staged
    // metadata differs in one of those fields.
    enum class EField : std::uint8_t { None, Summary, ManifestList };
    const auto stage = [&](EField field, bool verify) {
        return [&, field, verify](TPublicationContext* context, TPointerChange* change) {
            auto bytes = IO_->ReadFile(change->NewLocation, std::nullopt);
            ASSERT_TRUE(bytes) << bytes.error().message;
            auto parsed = ParseTableMetadata(*bytes);
            ASSERT_TRUE(parsed) << parsed.error().message;
            ASSERT_FALSE((*parsed)->snapshots.empty());
            if (field != EField::None) {
                auto snapshot = std::make_shared<iceberg::Snapshot>(*(*parsed)->snapshots.back());
                if (field == EField::Summary) {
                    snapshot->summary["extra"] = "different";
                } else {
                    snapshot->manifest_list += ".different";
                }
                (*parsed)->snapshots.back() = std::move(snapshot);
            }
            context->StagedRoot = TStagedRoot{std::shared_ptr<const iceberg::TableMetadata>(std::move(*parsed)),
                std::make_shared<const std::string>(std::move(*bytes))};
            context->VerifyStagedRoot = verify;
        };
    };
    const auto base = Append(1, empty, 3, stage(EField::None, true));
    ASSERT_TRUE(base) << base.error().message;
    int index = 2;
    for (const auto field : {EField::Summary, EField::ManifestList}) {
        const auto rejected = Append(index++, *base, 3, stage(field, true));
        ASSERT_FALSE(rejected);
        EXPECT_EQ(rejected.error().kind, iceberg::ErrorKind::kInvalid) << rejected.error().message;
        EXPECT_TRUE(rejected.error().message.starts_with("CORRUPTION: ")) << rejected.error().message;
    }
}

TEST_F(TPublicationGuardTest, RecoveryHonorsExactAndReducedLimitsWithoutChangingTheStore)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 3);
    ASSERT_TRUE(first);
    const auto second = Append(2, *first, 2);
    ASSERT_TRUE(second);
    const auto readOnly = IO_->ReadOnlyView();
    ASSERT_TRUE(readOnly);
    const auto original = (*readOnly)->ReadFile((*second)->MetadataLocation, std::nullopt);
    ASSERT_TRUE(original);
    TStorageLimits exact;
    exact.Store = (*second)->Usage;
    exact.RootBytes = (*second)->Usage.RootBytes;
    const auto reopen = [&](const auto& limits) {
        return VerifyReachable(*readOnly, (*second)->MetadataLocation, StoreID, CollectionID, limits);
    };
    const auto accepted = reopen(exact);
    ASSERT_TRUE(accepted) << accepted.error().message;
    EXPECT_EQ(accepted->Usage, (*second)->Usage);
    constexpr std::array fields{&TStorageUsage::RootBytes,
        &TStorageUsage::Snapshots,
        &TStorageUsage::SnapshotLogEntries,
        &TStorageUsage::DataFiles,
        &TStorageUsage::Manifests,
        &TStorageUsage::ManifestBytes,
        &TStorageUsage::ManifestListBytes,
        &TStorageUsage::FooterBytes};
    for (const auto field : fields) {
        auto reduced = exact;
        ASSERT_GT(reduced.Store.*field, 0);
        --(reduced.Store.*field);
        EXPECT_FALSE(reopen(reduced));
    }
    auto reduced = exact;
    --reduced.RootBytes;
    EXPECT_FALSE(reopen(reduced));
    EXPECT_TRUE(reopen(exact));
    const auto unchanged = (*readOnly)->ReadFile((*second)->MetadataLocation, std::nullopt);
    ASSERT_TRUE(unchanged);
    EXPECT_EQ(*unchanged, *original);
    EXPECT_FALSE(Dirs_->Latch()->IsSet());
}

class TCollectionCopyTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TCollectionCopyTest, ReopensCopiedRootsWithoutTheSourceAndRejectsInterruptedCopies)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 3);
    ASSERT_TRUE(first);
    const auto second = Append(2, *first, 2);
    ASSERT_TRUE(second);
    Scope_.reset();
    const glamdb::test::TempDir temporary;
    const auto destination = std::filesystem::canonical(temporary.path) / "copy";
    auto posix = std::make_shared<NIO::TPosixLocalFS>();
    auto targetFS = std::make_shared<NTesting::TFaultFS>(*posix);
    const auto directories = NIO::TStoreDirs::Create(targetFS, destination, std::make_shared<NIO::TWriteLatch>());
    ASSERT_TRUE(directories);
    const auto target = directories.Value()->CreateCollection(CollectionID);
    ASSERT_TRUE(target);
    const auto source = Dirs_->OpenCollection(CollectionID);
    ASSERT_TRUE(source);
    const auto targetIO = TDurableLocalFileIO::Make(directories.Value(), StoreID);
    ASSERT_TRUE(targetIO);
    if (GetParam()) {
        targetFS->AddRule({NTesting::EIOOp::Write, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), {}});
    }
    bool interrupted = false;
    for (const bool metadata : {true, false}) {
        const std::string name = metadata ? "metadata" : "data";
        std::vector<std::filesystem::path> paths;
        for (const auto& entry : std::filesystem::directory_iterator(Root_ / "warehouse" / CollectionID / name)) {
            ASSERT_TRUE(entry.is_regular_file());
            paths.push_back(entry.path());
        }
        std::ranges::sort(paths);
        for (const auto& path : paths) {
            const auto leaf = path.filename().string();
            const auto copied =
                NIO::CopyDurableFile({Dirs_->Fs(), metadata ? source.Value().Metadata : source.Value().Data, leaf},
                    {targetFS,
                        metadata ? target.Value().Metadata : target.Value().Data,
                        leaf,
                        directories.Value()->Latch()},
                    std::filesystem::file_size(path));
            if (!copied) {
                ASSERT_TRUE(GetParam());
                ASSERT_FALSE(metadata);
                ASSERT_EQ(leaf, std::filesystem::path(DataLocations_.front()).filename());
                EXPECT_EQ(copied.Error(), std::errc::io_error);
                const auto partial = destination / "warehouse" / CollectionID / name / leaf;
                ASSERT_EQ(std::filesystem::file_size(partial), 0);
                EXPECT_EQ(std::ranges::count_if(targetFS->Events(),
                              [&](const auto& event) {
                                  return event.Op == NTesting::EIOOp::Write && event.Path == partial &&
                                      event.Error == std::errc::io_error;
                              }),
                    1);
                interrupted = true;
                break;
            }
            EXPECT_EQ(copied.Value().Length, std::filesystem::file_size(path));
            auto uri = Location_;
            uri.append("/").append(name).append("/").append(leaf);
            const auto original = IO_->ReadFile(uri, std::nullopt);
            const auto actual = (*targetIO)->ReadFile(uri, std::nullopt);
            ASSERT_TRUE(original);
            ASSERT_TRUE(actual);
            EXPECT_EQ(*actual, *original);
        }
        if (interrupted) {
            break;
        }
    }
    ASSERT_EQ(interrupted, GetParam());
    ASSERT_TRUE(std::filesystem::exists(destination / "warehouse" / CollectionID / "metadata" /
        std::filesystem::path((*second)->MetadataLocation).filename()));
    auto movedSource = Root_;
    movedSource += "-unavailable-source";
    std::filesystem::rename(Root_, movedSource);
    ASSERT_GT(std::filesystem::remove_all(movedSource), 0);
    ASSERT_FALSE(std::filesystem::exists(Root_));
    ASSERT_FALSE(std::filesystem::exists(movedSource));
    EXPECT_FALSE(IO_->ReadFile((*second)->MetadataLocation, std::nullopt));
    const auto reopenedDirs = NIO::TStoreDirs::Open(posix, destination, std::make_shared<NIO::TWriteLatch>());
    ASSERT_TRUE(reopenedDirs);
    const auto reopened = TDurableLocalFileIO::Make(reopenedDirs.Value(), StoreID);
    ASSERT_TRUE(reopened);
    IO_ = *reopened;
    for (const auto& expected : {*first, *second}) {
        const auto recovered = VerifyReachable(IO_, expected->MetadataLocation, StoreID, CollectionID);
        if (interrupted) {
            ASSERT_FALSE(recovered);
            EXPECT_EQ(recovered.error().kind, iceberg::ErrorKind::kInvalid);
            EXPECT_TRUE(recovered.error().message.starts_with("CORRUPTION: " + DataLocations_.front()))
                << recovered.error().message;
            ExpectFreshProcess(destination, expected->MetadataLocation, std::nullopt, DataLocations_.front());
        } else {
            ASSERT_TRUE(recovered) << recovered.error().message;
            EXPECT_EQ(recovered->Usage, expected->Usage);
            EXPECT_EQ(*recovered->Metadata, *expected->Metadata);
            EXPECT_EQ(recovered->Metadata->current_snapshot_id, expected->Metadata->current_snapshot_id);
            ExpectFreshProcess(destination, expected->MetadataLocation, expected->Usage.Snapshots);
        }
    }
    if (!interrupted) {
        CheckRows(DataLocations_[0], 3);
        CheckRows(DataLocations_[1], 2);
    }
}

INSTANTIATE_TEST_SUITE_P(CompleteAndInterrupted, TCollectionCopyTest, ::testing::Bool());

class TRecoveryHistoryTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TRecoveryHistoryTest, ChecksEveryRetainedTransitionAndTheFinalRoot)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    auto base = create.GetCandidate();
    for (int index = 1; index <= 3; ++index) {
        auto appended = Append(index, base, 1);
        ASSERT_TRUE(appended) << appended.error().message;
        base = *appended;
    }
    auto metadata = *base->Metadata;
    for (auto& snapshot : metadata.snapshots) {
        snapshot = std::make_shared<iceberg::Snapshot>(*snapshot);
    }
    std::string expectedError;
    switch (GetParam()) {
        case 0:
            break;
        case 1: {
            metadata.snapshots.back()->snapshot_id = metadata.snapshots.front()->snapshot_id;
            expectedError = "Invalid retained snapshot history";
            break;
        }
        case 2: {
            metadata.snapshots.back()->manifest_list = metadata.snapshots.front()->manifest_list;
            expectedError = "Invalid retained snapshot history";
            break;
        }
        case 3: {
            metadata.snapshots[1]->parent_snapshot_id.reset();
            expectedError = "Invalid append snapshot identity";
            break;
        }
        case 4: {
            metadata.snapshot_log[1].timestamp_ms += std::chrono::milliseconds(1);
            expectedError = "Append snapshot log differs";
            break;
        }
        case 5: {
            metadata.current_snapshot_id = metadata.snapshots.front()->snapshot_id;
            expectedError = "Recovery root has an inconsistent current snapshot";
            break;
        }
        case 6: {
            ++metadata.snapshots[1]->sequence_number;
            expectedError = "Invalid append metadata delta";
            break;
        }
        default: {
            auto ref = std::make_shared<iceberg::SnapshotRef>(*metadata.refs.at("main"));
            ref->snapshot_id = metadata.snapshots.front()->snapshot_id;
            metadata.refs["main"] = std::move(ref);
            expectedError = "Recovery root has an inconsistent current snapshot";
            break;
        }
    }
    const auto root = WriteRecoveryRoot(metadata);
    ASSERT_TRUE(root) << root.error().message;
    const auto readOnly = IO_->ReadOnlyView();
    ASSERT_TRUE(readOnly);
    const auto recovered = VerifyReachable(*readOnly, *root, StoreID, CollectionID);
    if (expectedError.empty()) {
        ASSERT_TRUE(recovered) << recovered.error().message;
        EXPECT_EQ(*recovered->Metadata, metadata);
        EXPECT_EQ(recovered->Usage.DataFiles, 3);
    } else {
        ASSERT_FALSE(recovered);
        EXPECT_NE(recovered.error().message.find(expectedError), std::string::npos) << recovered.error().message;
    }
    EXPECT_TRUE(VerifyReachable(*readOnly, base->MetadataLocation, StoreID, CollectionID));
}

INSTANTIATE_TEST_SUITE_P(RetainedHistory, TRecoveryHistoryTest, ::testing::Range(0, 8));

class TRecoveryTimestampTest
    : public TPublicationGuardTest
    , public ::testing::WithParamInterface<std::tuple<std::int64_t, std::size_t>>
{ };

TEST_P(TRecoveryTimestampTest, ValidatesCurrentAndHistoricalSnapshotTimesInFreshProcess)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 1);
    ASSERT_TRUE(first) << first.error().message;
    const auto second = Append(2, *first, 1);
    ASSERT_TRUE(second) << second.error().message;
    auto metadata = *(*second)->Metadata;
    const auto [milliseconds, index] = GetParam();
    auto snapshot = std::make_shared<iceberg::Snapshot>(*metadata.snapshots[index]);
    snapshot->timestamp_ms = iceberg::TimePointMs(std::chrono::milliseconds(milliseconds));
    metadata.snapshot_log[index].timestamp_ms = snapshot->timestamp_ms;
    metadata.snapshots[index] = std::move(snapshot);
    const auto root = WriteRecoveryRoot(metadata);
    ASSERT_TRUE(root) << root.error().message;
    const auto recovered = VerifyReachable(IO_, *root, StoreID, CollectionID);
    if (milliseconds == -62135596800000LL || milliseconds == 253402300799999LL) {
        ASSERT_TRUE(recovered) << recovered.error().message;
        EXPECT_EQ(*recovered->Metadata, metadata);
        ExpectFreshProcess(Root_, *root);
    } else {
        constexpr auto message = "Snapshot timestamp is outside UTC years 0001..9999.";
        ASSERT_FALSE(recovered);
        EXPECT_EQ(recovered.error().kind, iceberg::ErrorKind::kValidationFailed);
        EXPECT_EQ(recovered.error().message, message);
        ExpectFreshProcess(Root_, *root, std::nullopt, {}, message);
    }
    EXPECT_TRUE(VerifyReachable(IO_, (*second)->MetadataLocation, StoreID, CollectionID));
    EXPECT_FALSE(Dirs_->Latch()->Error());
}

INSTANTIATE_TEST_SUITE_P(SupportedUtcRange,
    TRecoveryTimestampTest,
    ::testing::Combine(::testing::Values(std::int64_t{-62135596800000LL},
                           std::int64_t{253402300799999LL},
                           std::int64_t{-62135596800001LL},
                           std::int64_t{253402300800000LL},
                           std::numeric_limits<std::int64_t>::min(),
                           std::numeric_limits<std::int64_t>::max()),
        ::testing::Values(std::size_t{0}, std::size_t{1})));

TEST_F(TPublicationGuardTest, RecoveryRejectsADataFileReusedByDistinctManifests)
{
    WriteRoot();
    TStoragePublicationGuard create(Context());
    ASSERT_TRUE(create.BeforePointerChange(Change()));
    const auto first = Append(1, create.GetCandidate(), 1);
    ASSERT_TRUE(first) << first.error().message;
    std::string candidateRoot;
    const auto reused = Append(
        2,
        *first,
        1,
        [&](auto* /*context*/, auto* change) {
            candidateRoot = change->NewLocation;
        },
        DataLocations_.front());
    ASSERT_FALSE(reused);
    ASSERT_FALSE(candidateRoot.empty());
    const auto readOnly = IO_->ReadOnlyView();
    ASSERT_TRUE(readOnly);
    const auto recovered = VerifyReachable(*readOnly, candidateRoot, StoreID, CollectionID);
    ASSERT_FALSE(recovered);
    EXPECT_NE(recovered.error().message.find("Append reuses a retained data file"), std::string::npos)
        << recovered.error().message;
    EXPECT_TRUE(VerifyReachable(*readOnly, (*first)->MetadataLocation, StoreID, CollectionID));
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
