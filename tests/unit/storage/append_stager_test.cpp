#include "core/storage/append_stager.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/table_profile.h"
#include "tests/support/fault_fs.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <gtest/gtest.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <parquet/file_reader.h>
#include <parquet/statistics.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <span>
#include <string_view>
#include <vector>

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
    TSchemaSpec SchemaSpec_{{{"key", EFieldType::String}}};

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
        const auto profile = MakeTableProfile(SchemaSpec_);
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

    void BindAppend(const std::string& attemptID = AppendID, NIO::TAttemptLimits limits = {})
    {
        Scope_.reset();
        auto attempt = NIO::TIOAttempt::Make({StoreID, CollectionID, attemptID, NIO::EAttemptKind::Append, OperationID},
            Latch_,
            limits);
        ASSERT_TRUE(attempt);
        Attempt_ = std::move(attempt).Value();
        auto scope = IO_->BindAttempt(Attempt_);
        ASSERT_TRUE(scope) << scope.error().message;
        Scope_.emplace(std::move(*scope));
    }

    void PrepareData(bool verified = true, const std::string& attemptID = AppendID)
    {
        ASSERT_NO_FATAL_FAILURE(BindAppend(attemptID));
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

TEST_F(TAppendStagerTest, BulkStagesEveryVerifiedFileInOneSnapshot)
{
    BindAppend(AppendID, NIO::TAttemptLimits::BulkAppend());
    arrow::StringBuilder builder;
    ASSERT_TRUE(builder.Append("duplicate").ok());
    auto array = builder.Finish();
    ASSERT_TRUE(array.ok());
    const auto batch =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false)}), 1, {*array});
    std::vector<std::shared_ptr<iceberg::DataFile>> files;
    for (int index = 0; index < 2; ++index) {
        const auto path =
            Location() + "/data/" + OperationID + "-" + AppendID + "-0000" + std::to_string(index) + ".parquet";
        auto written = WriteDataFile(IO_, Attempt_, *Table_->metadata(), path, {batch});
        ASSERT_TRUE(written) << written.error().message;
        files.push_back(written->DataFile);
    }
    const auto staged = StageBulkAppend(Table_, files, {{"glamdb.bulk-version", "1"}}, Attempt_);
    ASSERT_TRUE(staged) << staged.error().message;
    CheckUnpublished();
    const auto snapshot = staged->Transaction->current().Snapshot();
    ASSERT_TRUE(snapshot);
    EXPECT_EQ((*snapshot)->summary.at("glamdb.bulk-version"), "1");
    EXPECT_EQ((*snapshot)->summary.at(iceberg::SnapshotSummaryFields::kAddedDataFiles), "2");
    EXPECT_EQ((*snapshot)->summary.at(iceberg::SnapshotSummaryFields::kAddedRecords), "2");
    iceberg::SnapshotReader snapshotReader(snapshot->get());
    const auto manifests = snapshotReader.Manifests(IO_);
    ASSERT_TRUE(manifests) << manifests.error().message;
    std::vector<std::string> actual;
    for (const auto& manifest : *manifests) {
        auto reader = iceberg::ManifestReader::Make(manifest, IO_, Table_->schema().value(), Table_->spec().value());
        ASSERT_TRUE(reader) << reader.error().message;
        const auto entries = (*reader)->Entries();
        ASSERT_TRUE(entries) << entries.error().message;
        for (const auto& entry : *entries) {
            ASSERT_NE(entry.data_file, nullptr);
            actual.push_back(entry.data_file->file_path);
        }
    }
    ASSERT_EQ(actual.size(), files.size());
    EXPECT_EQ(actual[0], files[0]->file_path);
    EXPECT_EQ(actual[1], files[1]->file_path);
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TAppendStagerTest, BulkRequiresMarkerBeforeMetadataIO)
{
    PrepareData();
    const auto eventCount = FS_->Events().size();
    EXPECT_FALSE(StageBulkAppend(Table_, {Data_}, {}, Attempt_));
    EXPECT_EQ(FS_->Events().size(), eventCount);
    CheckUnpublished();
}

TEST_F(TAppendStagerTest, BulkRejectsDuplicateFilesBeforeMetadataIO)
{
    PrepareData();
    const auto eventCount = FS_->Events().size();
    EXPECT_FALSE(StageBulkAppend(Table_, {Data_, Data_}, {{"glamdb.bulk-version", "1"}}, Attempt_));
    EXPECT_EQ(FS_->Events().size(), eventCount);
    CheckUnpublished();
}

TEST_F(TAppendStagerTest, OrdinaryRejectsBulkMarkerBeforeMetadataIO)
{
    PrepareData();
    const auto eventCount = FS_->Events().size();
    EXPECT_FALSE(StageAppend(Table_, Data_, {{"glamdb.bulk-version", "1"}}, Attempt_));
    EXPECT_EQ(FS_->Events().size(), eventCount);
    CheckUnpublished();
}

constexpr std::string_view AsciiLower = "common_ascii_prefix_longer_than_16_a";
constexpr std::int64_t MinTimestamp = -62135596800000000LL;
constexpr std::int64_t MaxTimestamp = 253402300799999999LL;

std::vector<std::uint8_t> LittleEndian64(std::uint64_t value)
{
    std::vector<std::uint8_t> bytes(8);
    for (auto& byte : bytes) {
        byte = static_cast<std::uint8_t>(value);
        value >>= 8;
    }
    return bytes;
}

std::shared_ptr<arrow::RecordBatch> MakeMetricsBatch(const std::string& longKey)
{
    arrow::StringBuilder keys;
    EXPECT_TRUE(keys.Append(AsciiLower).ok());
    EXPECT_TRUE(keys.Append("common_ascii_prefix_longer_than_16_b").ok());
    EXPECT_TRUE(keys.Append(longKey).ok());
    arrow::Int64Builder counts;
    EXPECT_TRUE(counts.Append(-7).ok());
    EXPECT_TRUE(counts.AppendNull().ok());
    EXPECT_TRUE(counts.Append(42).ok());
    arrow::BooleanBuilder flags;
    EXPECT_TRUE(flags.AppendValues(std::vector<std::uint8_t>{0, 1, 0}).ok());
    arrow::DoubleBuilder ratios;
    EXPECT_TRUE(ratios.Append(-1.25).ok());
    EXPECT_TRUE(ratios.AppendNull().ok());
    EXPECT_TRUE(ratios.Append(2.5).ok());
    arrow::TimestampBuilder times(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    EXPECT_TRUE(times.Append(MinTimestamp).ok());
    EXPECT_TRUE(times.AppendNull().ok());
    EXPECT_TRUE(times.Append(MaxTimestamp).ok());
    arrow::StringBuilder empty;
    EXPECT_TRUE(empty.AppendNulls(3).ok());
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(6);
    for (arrow::ArrayBuilder* builder :
        std::array<arrow::ArrayBuilder*, 6>{&keys, &counts, &flags, &ratios, &times, &empty})
    {
        auto array = builder->Finish();
        EXPECT_TRUE(array.ok());
        arrays.push_back(array.ok() ? *array : nullptr);
    }
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false),
                                        arrow::field("count", arrow::int64()),
                                        arrow::field("enabled", arrow::boolean(), false),
                                        arrow::field("ratio", arrow::float64()),
                                        arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                                        arrow::field("empty", arrow::utf8())}),
        3,
        std::move(arrays));
}

class TManifestMetricsTest : public TAppendStagerTest
{
protected:
    void SetUp() override
    {
        SchemaSpec_ = {{{"key", EFieldType::String},
            {"count", EFieldType::Int64, true},
            {"enabled", EFieldType::Bool},
            {"ratio", EFieldType::Float64, true},
            {"time", EFieldType::TimestampUsUtc, true},
            {"empty", EFieldType::String, true}}};
        TAppendStagerTest::SetUp();
    }
};

TEST_F(TManifestMetricsTest, RoundTripsIndependentMetricsThroughRealParquetAndAvro)
{
    std::string longKey;
    for (int index = 0; index < 512; ++index) {
        longKey += "я";
    }
    ASSERT_EQ(longKey.size(), 1024);
    ASSERT_NO_FATAL_FAILURE(BindAppend());
    const auto path = Location() + "/data/" + OperationID + "-" + AppendID + "-00000.parquet";
    const auto written = WriteDataFile(IO_, Attempt_, *Table_->metadata(), path, {MakeMetricsBatch(longKey)});
    ASSERT_TRUE(written) << written.error().message;
    const auto staged = StageAppend(Table_, written->DataFile, {}, Attempt_);
    ASSERT_TRUE(staged) << staged.error().message;
    const auto snapshot = staged->Transaction->current().Snapshot();
    ASSERT_TRUE(snapshot);
    iceberg::SnapshotReader snapshotReader(snapshot->get());
    const auto manifests = snapshotReader.Manifests(IO_);
    ASSERT_TRUE(manifests) << manifests.error().message;
    ASSERT_EQ(manifests->size(), 1);
    const auto reader =
        iceberg::ManifestReader::Make(manifests->front(), IO_, Table_->schema().value(), Table_->spec().value());
    ASSERT_TRUE(reader) << reader.error().message;
    const auto entries = (*reader)->Entries();
    ASSERT_TRUE(entries) << entries.error().message;
    ASSERT_EQ(entries->size(), 1);
    ASSERT_TRUE(entries->front().data_file);
    const auto& data = *entries->front().data_file;
    const std::map<std::int32_t, std::int64_t> values{{1, 3}, {2, 3}, {3, 3}, {4, 3}, {5, 3}, {6, 3}};
    const std::map<std::int32_t, std::int64_t> nulls{{1, 0}, {2, 1}, {3, 0}, {4, 1}, {5, 1}, {6, 3}};
    const std::map<std::int32_t, std::int64_t> nans{{4, 0}};
    const std::map<std::int32_t, std::vector<std::uint8_t>> lower{{1, {AsciiLower.begin(), AsciiLower.end()}},
        {2, LittleEndian64(static_cast<std::uint64_t>(-7))},
        {3, {0}},
        {4, LittleEndian64(std::bit_cast<std::uint64_t>(-1.25))},
        {5, LittleEndian64(static_cast<std::uint64_t>(MinTimestamp))}};
    const std::map<std::int32_t, std::vector<std::uint8_t>> upper{{1, {longKey.begin(), longKey.end()}},
        {2, LittleEndian64(42)},
        {3, {1}},
        {4, LittleEndian64(std::bit_cast<std::uint64_t>(2.5))},
        {5, LittleEndian64(static_cast<std::uint64_t>(MaxTimestamp))}};
    EXPECT_EQ(data.record_count, 3);
    EXPECT_EQ(data.value_counts, values);
    EXPECT_EQ(data.null_value_counts, nulls);
    EXPECT_EQ(data.nan_value_counts, nans);
    EXPECT_EQ(data.lower_bounds, lower);
    EXPECT_EQ(data.upper_bounds, upper);

    const auto bytes = IO_->ReadFile(path, std::nullopt);
    ASSERT_TRUE(bytes) << bytes.error().message;
    auto parquet = parquet::ParquetFileReader::Open(arrow::io::BufferReader::FromString(*bytes));
    const auto footer = parquet->metadata();
    ASSERT_EQ(footer->num_columns(), 6);
    ASSERT_EQ(footer->num_row_groups(), 1);
    std::map<std::int32_t, std::int64_t> columnSizes;
    for (int column = 0; column < 6; ++column) {
        EXPECT_EQ(Table_->schema().value()->fields()[column].field_id(), column + 1);
        EXPECT_EQ(footer->schema()->Column(column)->schema_node()->field_id(), column + 1);
        columnSizes.emplace(column + 1, footer->RowGroup(0)->ColumnChunk(column)->total_compressed_size());
    }
    EXPECT_EQ(data.column_sizes, columnSizes);
    const auto missingBounds = footer->RowGroup(0)->ColumnChunk(5)->statistics();
    ASSERT_TRUE(missingBounds);
    EXPECT_FALSE(missingBounds->HasMinMax());
    EXPECT_EQ(missingBounds->null_count(), 3);
    EXPECT_FALSE(Attempt_->FirstError());
}

class TNativeManifestMetricsTest : public TAppendStagerTest
{
protected:
    void SetUp() override
    {
        SchemaSpec_ = {{{"amount", EFieldType::Decimal, true, 38, 2}, {"day", EFieldType::Date, true}}};
        TAppendStagerTest::SetUp();
    }
};

TEST_F(TNativeManifestMetricsTest, PreservesSignedDecimalAndDateBoundsThroughAvroManifests)
{
    arrow::Decimal128Builder amounts(arrow::decimal128(38, 2), arrow::default_memory_pool());
    ASSERT_TRUE(amounts.Append(arrow::Decimal128(32768)).ok());
    ASSERT_TRUE(amounts.Append(arrow::Decimal128(-32769)).ok());
    ASSERT_TRUE(amounts.AppendNull().ok());
    arrow::Date32Builder dates;
    ASSERT_TRUE(dates.Append(-719162).ok());
    ASSERT_TRUE(dates.Append(2932896).ok());
    ASSERT_TRUE(dates.AppendNull().ok());
    const auto amountArray = amounts.Finish();
    const auto dateArray = dates.Finish();
    ASSERT_TRUE(amountArray.ok());
    ASSERT_TRUE(dateArray.ok());
    const auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("amount", arrow::decimal128(38, 2)), arrow::field("day", arrow::date32())}),
        3,
        {*amountArray, *dateArray});
    ASSERT_NO_FATAL_FAILURE(BindAppend());
    const auto path = Location() + "/data/" + OperationID + "-" + AppendID + "-00000.parquet";
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 22;
    const auto written = WriteDataFile(IO_, Attempt_, *Table_->metadata(), path, {batch}, limits);
    ASSERT_TRUE(written) << written.error().message;
    const auto staged = StageAppend(Table_, written->DataFile, {}, Attempt_);
    ASSERT_TRUE(staged) << staged.error().message;
    const auto snapshot = staged->Transaction->current().Snapshot();
    ASSERT_TRUE(snapshot);
    iceberg::SnapshotReader snapshotReader(snapshot->get());
    const auto manifests = snapshotReader.Manifests(IO_);
    ASSERT_TRUE(manifests) << manifests.error().message;
    ASSERT_EQ(manifests->size(), 1);
    const auto reader =
        iceberg::ManifestReader::Make(manifests->front(), IO_, Table_->schema().value(), Table_->spec().value());
    ASSERT_TRUE(reader) << reader.error().message;
    const auto entries = (*reader)->Entries();
    ASSERT_TRUE(entries) << entries.error().message;
    ASSERT_EQ(entries->size(), 1);
    ASSERT_TRUE(entries->front().data_file);
    const auto& data = *entries->front().data_file;
    EXPECT_EQ(data.value_counts, (std::map<std::int32_t, std::int64_t>{{1, 3}, {2, 3}}));
    EXPECT_EQ(data.null_value_counts, (std::map<std::int32_t, std::int64_t>{{1, 1}, {2, 1}}));
    EXPECT_EQ(data.lower_bounds,
        (std::map<std::int32_t, std::vector<std::uint8_t>>{{1, {0xff, 0x7f, 0xff}}, {2, {0xc6, 0x06, 0xf5, 0xff}}}));
    EXPECT_EQ(data.upper_bounds,
        (std::map<std::int32_t, std::vector<std::uint8_t>>{{1, {0x00, 0x80, 0x00}}, {2, {0xa0, 0xc0, 0x2c, 0x00}}}));
    EXPECT_EQ(data.split_offsets.size(), 3);
}

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
