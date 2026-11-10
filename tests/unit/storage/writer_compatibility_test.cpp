#include "core/storage/append_stager.h"
#include "core/storage/base_state.h"
#include "core/storage/data_file_writer.h"
#include "core/storage/durable_local_file_io.h"
#include "core/storage/format_verifier.h"
#include "core/storage/table_profile.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/bridge.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>
#include <iceberg/catalog/memory/in_memory_catalog.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/properties.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <span>

namespace NGlamDB::NStorage {
namespace {

constexpr auto StoreID = "11111111-1111-4111-8111-111111111111";
constexpr auto CollectionID = "22222222-2222-4222-8222-222222222222";
constexpr auto OperationID = "44444444-4444-4444-8444-444444444444";

std::shared_ptr<arrow::RecordBatch> MakeNoKeyBatch()
{
    arrow::StringBuilder labels;
    EXPECT_TRUE(labels.Append("common_ascii_prefix_longer_than_16_a").ok());
    EXPECT_TRUE(labels.Append("common_ascii_prefix_longer_than_16_b").ok());
    EXPECT_TRUE(labels.Append("я").ok());
    arrow::BooleanBuilder enabled;
    EXPECT_TRUE(enabled.AppendValues(std::vector<std::uint8_t>{0, 1, 0}).ok());
    arrow::Int64Builder counts;
    EXPECT_TRUE(counts.Append(-7).ok());
    EXPECT_TRUE(counts.AppendNull().ok());
    EXPECT_TRUE(counts.Append(42).ok());
    arrow::DoubleBuilder ratios;
    EXPECT_TRUE(ratios.Append(-1.25).ok());
    EXPECT_TRUE(ratios.AppendNull().ok());
    EXPECT_TRUE(ratios.Append(2.5).ok());
    arrow::TimestampBuilder times(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    EXPECT_TRUE(times.Append(-62135596800000000LL).ok());
    EXPECT_TRUE(times.AppendNull().ok());
    EXPECT_TRUE(times.Append(253402300799999999LL).ok());
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(5);
    for (arrow::ArrayBuilder* builder :
        std::array<arrow::ArrayBuilder*, 5>{&labels, &enabled, &counts, &ratios, &times})
    {
        auto array = builder->Finish();
        EXPECT_TRUE(array.ok());
        arrays.push_back(array.ok() ? *array : nullptr);
    }
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("label", arrow::utf8(), false),
                                        arrow::field("enabled", arrow::boolean(), false),
                                        arrow::field("count", arrow::int64()),
                                        arrow::field("ratio", arrow::float64()),
                                        arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))}),
        3,
        std::move(arrays));
}

class TWriterCompatibilityTest : public ::testing::Test
{
protected:
    std::filesystem::path Parent_;
    std::shared_ptr<NIO::TWriteLatch> Latch_ = std::make_shared<NIO::TWriteLatch>();
    std::shared_ptr<TDurableLocalFileIO> IO_;
    std::unique_ptr<iceberg::TableMetadata> Metadata_;
    std::shared_ptr<arrow::RecordBatch> Batch_;
    std::shared_ptr<NIO::TIOAttempt> Attempt_;
    std::optional<TAttemptScope> Scope_;
    std::string Path_;

    static std::string Location()
    {
        return std::string("store://") + StoreID + "/warehouse/" + CollectionID;
    }

    void SetUp() override
    {
        auto path = (std::filesystem::temp_directory_path() / "glamdb-writer-compat-XXXXXX").string();
        ASSERT_NE(::mkdtemp(path.data()), nullptr);
        Parent_ = std::filesystem::canonical(path);
        auto dirs = NIO::TStoreDirs::Create(std::make_shared<NIO::TPosixLocalFS>(), Parent_ / "store", Latch_);
        ASSERT_TRUE(dirs);
        ASSERT_TRUE(dirs.Value()->CreateCollection(CollectionID));
        auto fileIO = TDurableLocalFileIO::Make(dirs.Value(), StoreID);
        ASSERT_TRUE(fileIO);
        IO_ = *fileIO;
        const auto profile = MakeTableProfile({{{"label", EFieldType::String},
            {"enabled", EFieldType::Bool},
            {"count", EFieldType::Int64, true},
            {"ratio", EFieldType::Float64, true},
            {"time", EFieldType::TimestampUsUtc, true}}});
        ASSERT_TRUE(profile);
        auto metadata = iceberg::TableMetadata::Make(*profile->Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Location(),
            profile->Properties);
        ASSERT_TRUE(metadata);
        Metadata_ = std::move(*metadata);
        Batch_ = MakeNoKeyBatch();
        ASSERT_TRUE(Batch_->ValidateFull().ok());
    }

    void TearDown() override
    {
        Scope_.reset();
        IO_.reset();
        std::error_code error;
        std::filesystem::remove_all(Parent_, error);
        EXPECT_FALSE(error);
    }

    void BeginAppend(int index)
    {
        Scope_.reset();
        const auto id = "33333333-3333-7333-8333-33333333333" + std::to_string(index);
        auto attempt =
            NIO::TIOAttempt::Make({StoreID, CollectionID, id, NIO::EAttemptKind::Append, OperationID}, Latch_);
        ASSERT_TRUE(attempt);
        Attempt_ = std::move(attempt).Value();
        auto scope = IO_->BindAttempt(Attempt_);
        ASSERT_TRUE(scope);
        Scope_.emplace(std::move(*scope));
        Path_ = Location() + "/data/" + OperationID + "-" + id + "-00000.parquet";
    }

    void CheckRows(const std::string& path)
    {
        auto bytes = IO_->ReadFile(path, std::nullopt);
        ASSERT_TRUE(bytes) << bytes.error().message;
        auto reader = parquet::arrow::OpenFile(arrow::io::BufferReader::FromString(std::move(*bytes)),
            arrow::default_memory_pool());
        ASSERT_TRUE(reader.ok()) << reader.status();
        const auto actual = (*reader)->ReadTable();
        ASSERT_TRUE(actual.ok()) << actual.status();
        const auto expected = arrow::Table::FromRecordBatches({Batch_});
        ASSERT_TRUE(expected.ok());
        EXPECT_TRUE((*actual)->Equals(**expected)) << (*actual)->ToString();
    }
};

TEST_F(TWriterCompatibilityTest, MatchesUpstreamDataWriterForNoKeySchema)
{
    ASSERT_NO_FATAL_FAILURE(BeginAppend(1));
    const auto format = WriteDataFile(IO_, Attempt_, *Metadata_, Path_, {Batch_});
    ASSERT_TRUE(format) << format.error().message;
    const auto formatPath = Path_;
    const auto formatBytes = IO_->ReadFile(formatPath, std::nullopt);
    ASSERT_TRUE(formatBytes);

    ASSERT_NO_FATAL_FAILURE(BeginAppend(2));
    ASSERT_FALSE(Attempt_->DeclareDataFile(Path_));
    iceberg::DataWriterOptions options;
    options.path = Path_;
    options.schema = Metadata_->Schema().value();
    options.spec = Metadata_->PartitionSpec().value();
    options.io = IO_;
    options.sort_order_id = 0;
    options.properties = {{"write.parquet.max-row-group-rows", "3"}};
    auto writer = iceberg::DataWriter::Make(options);
    ASSERT_TRUE(writer) << writer.error().message;
    ArrowArray exported{};
    ASSERT_TRUE(arrow::ExportRecordBatch(*Batch_, &exported).ok());
    const auto written = (*writer)->Write(&exported);
    if (exported.release) {
        exported.release(&exported);
    }
    ASSERT_TRUE(written) << written.error().message;
    const auto closed = (*writer)->Close();
    ASSERT_TRUE(closed) << closed.error().message;
    const auto metadata = (*writer)->Metadata();
    ASSERT_TRUE(metadata) << metadata.error().message;
    ASSERT_EQ(metadata->data_files.size(), 1);
    ASSERT_TRUE(metadata->data_files.front());
    auto normalized = *format->DataFile;
    normalized.file_path = Path_;
    EXPECT_EQ(normalized, *metadata->data_files.front());
    EXPECT_EQ(normalized.record_count, 3);
    EXPECT_EQ(normalized.null_value_counts.at(3), 1);
    EXPECT_EQ(normalized.value_counts.size(), 5);
    const auto upstreamBytes = IO_->ReadFile(Path_, std::nullopt);
    ASSERT_TRUE(upstreamBytes);
    EXPECT_EQ(*formatBytes, *upstreamBytes);
    ASSERT_NO_FATAL_FAILURE(CheckRows(formatPath));
    ASSERT_NO_FATAL_FAILURE(CheckRows(Path_));
    EXPECT_FALSE(Attempt_->FirstError());
}

TEST_F(TWriterCompatibilityTest, AcceptsEntirelyAbsentFooterStatisticsWithoutInventingManifestMetrics)
{
    auto fields = Batch_->schema()->fields();
    for (std::size_t index = 0; index < fields.size(); ++index) {
        fields[index] =
            fields[index]->WithMetadata(arrow::key_value_metadata({"PARQUET:field_id"}, {std::to_string(index + 1)}));
    }
    const auto annotated = arrow::RecordBatch::Make(arrow::schema(fields), 3, Batch_->columns());
    const auto table = arrow::Table::FromRecordBatches({annotated});
    ASSERT_TRUE(table.ok());
    const auto buffer = arrow::io::BufferOutputStream::Create();
    ASSERT_TRUE(buffer.ok());
    const auto properties = parquet::WriterProperties::Builder().disable_statistics()->build();
    ASSERT_TRUE(parquet::arrow::WriteTable(**table, arrow::default_memory_pool(), *buffer, 3, properties).ok());
    const auto bytes = (*buffer)->Finish();
    ASSERT_TRUE(bytes.ok());
    const auto footer = parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(*bytes))->metadata();
    ASSERT_EQ(footer->num_row_groups(), 1);
    ASSERT_EQ(footer->num_columns(), 5);
    for (int column = 0; column < 5; ++column) {
        const auto chunk = footer->RowGroup(0)->ColumnChunk(column);
        EXPECT_FALSE(chunk->is_stats_set());
        EXPECT_EQ(chunk->statistics(), nullptr);
        EXPECT_EQ(footer->schema()->Column(column)->schema_node()->field_id(), column + 1);
    }

    ASSERT_NO_FATAL_FAILURE(BeginAppend(1));
    ASSERT_FALSE(Attempt_->DeclareDataFile(Path_));
    const auto output = IO_->NewOutputFile(Path_);
    ASSERT_TRUE(output);
    const auto stream = (*output)->Create();
    ASSERT_TRUE(stream);
    ASSERT_TRUE((*stream)->Write(std::as_bytes(std::span((*bytes)->data(), (*bytes)->size()))));
    ASSERT_TRUE((*stream)->Close());
    const auto input = IO_->NewInputFile(Path_);
    ASSERT_TRUE(input);
    const auto verified = CheckParquet(**input, *Metadata_->Schema().value(), {(*bytes)->size(), 3});
    ASSERT_TRUE(verified) << verified.error().message;
    ASSERT_FALSE(Attempt_->MarkFormatVerified(Path_));
    ASSERT_NO_FATAL_FAILURE(CheckRows(Path_));

    auto data = std::make_shared<iceberg::DataFile>();
    data->file_path = Path_;
    data->record_count = 3;
    data->file_size_in_bytes = (*bytes)->size();
    data->partition_spec_id = 0;
    data->sort_order_id = 0;
    const auto catalog = iceberg::InMemoryCatalog::Make("missing-stats-test-double", IO_, Location(), {});
    ASSERT_TRUE(catalog);
    const auto root = Location() + "/metadata/00001-55555555-5555-4555-8555-555555555555.metadata.json";
    const auto icebergTable = iceberg::Table::Make({.ns = {}, .name = "events"},
        std::make_shared<iceberg::TableMetadata>(*Metadata_),
        root,
        IO_,
        *catalog);
    ASSERT_TRUE(icebergTable);
    const auto staged = StageAppend(*icebergTable, data, {}, Attempt_);
    ASSERT_TRUE(staged) << staged.error().message;
    const auto snapshot = staged->Transaction->current().Snapshot();
    ASSERT_TRUE(snapshot);
    const iceberg::SnapshotReader snapshotReader(snapshot->get());
    const auto manifests = snapshotReader.Manifests(IO_);
    ASSERT_TRUE(manifests);
    ASSERT_EQ(manifests->size(), 1);
    const auto reader = iceberg::ManifestReader::Make(manifests->front(),
        IO_,
        Metadata_->Schema().value(),
        Metadata_->PartitionSpec().value());
    ASSERT_TRUE(reader);
    const auto entries = (*reader)->Entries();
    ASSERT_TRUE(entries);
    ASSERT_EQ(entries->size(), 1);
    ASSERT_TRUE(entries->front().data_file);
    const auto& actual = *entries->front().data_file;
    EXPECT_EQ(actual.record_count, 3);
    EXPECT_EQ(actual.file_size_in_bytes, (*bytes)->size());
    EXPECT_TRUE(actual.column_sizes.empty());
    EXPECT_TRUE(actual.value_counts.empty());
    EXPECT_TRUE(actual.null_value_counts.empty());
    EXPECT_TRUE(actual.nan_value_counts.empty());
    EXPECT_TRUE(actual.lower_bounds.empty());
    EXPECT_TRUE(actual.upper_bounds.empty());
    ASSERT_TRUE(iceberg::TableMetadataUtil::Write(*IO_, root, staged->Transaction->current()));
    Scope_.reset();
    const auto readOnly = IO_->ReadOnlyView();
    ASSERT_TRUE(readOnly);
    const auto recovered = VerifyReachable(*readOnly, root, StoreID, CollectionID);
    ASSERT_TRUE(recovered) << recovered.error().message;
    EXPECT_EQ(recovered->Usage.DataFiles, 1);
    EXPECT_EQ(recovered->Usage.Manifests, 1);
}

} // namespace
} // namespace NGlamDB::NStorage
