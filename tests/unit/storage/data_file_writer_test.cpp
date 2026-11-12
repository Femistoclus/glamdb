#include "core/storage/data_file_writer.h"

#include "core/storage/durable_local_file_io.h"
#include "core/storage/format_verifier.h"
#include "core/storage/table_profile.h"
#include "tests/support/fault_fs.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/file.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/byte_size.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/partition_spec.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <limits>
#include <optional>
#include <utility>

namespace NGlamDB::NStorage {
namespace {

constexpr std::string_view StoreID = "01234567-89ab-cdef-0123-456789abcdef";
constexpr std::string_view CollectionID = "89abcdef-0123-4567-89ab-cdef01234567";
constexpr std::string_view AttemptID = "01234567-89ab-7def-8123-456789abcdef";
constexpr std::string_view OperationID = "fedcba98-7654-3210-fedc-ba9876543210";

std::string LongKey()
{
    std::string value;
    for (int index = 0; index < 512; ++index) {
        value += "я";
    }
    return value;
}

struct TBatchValues
{
    std::string Key = "common_ascii_prefix_longer_than_16_a";
    double Ratio = 1.5;
    std::int64_t Timestamp = 253402300799999999LL;
    bool NullKey = false;
};

class TDataFileWriterTest : public ::testing::Test
{
protected:
    std::filesystem::path Parent_;
    std::shared_ptr<NIO::TPosixLocalFS> LocalFs_ = std::make_shared<NIO::TPosixLocalFS>();
    std::shared_ptr<NTesting::TFaultFS> Fs_ = std::make_shared<NTesting::TFaultFS>(*LocalFs_);
    std::shared_ptr<NIO::TWriteLatch> Latch_ = std::make_shared<NIO::TWriteLatch>();
    std::shared_ptr<TDurableLocalFileIO> IO_;
    std::shared_ptr<NIO::TIOAttempt> Attempt_;
    std::unique_ptr<iceberg::TableMetadata> Metadata_;
    std::string Path_;

    void SetUp() override
    {
        auto name = (std::filesystem::temp_directory_path() / "glamdb-writer-XXXXXX").string();
        ASSERT_NE(::mkdtemp(name.data()), nullptr);
        Parent_ = std::filesystem::canonical(name);
        auto directories = NIO::TStoreDirs::Create(Fs_, Parent_ / "store", Latch_);
        ASSERT_TRUE(directories);
        ASSERT_TRUE(directories.Value()->CreateCollection(CollectionID));
        auto fileIO = TDurableLocalFileIO::Make(directories.Value(), std::string(StoreID));
        ASSERT_TRUE(fileIO);
        IO_ = *fileIO;
        auto profile = MakeTableProfile({{{"key", EFieldType::String},
            {"count", EFieldType::Int64, true},
            {"enabled", EFieldType::Bool},
            {"ratio", EFieldType::Float64, true},
            {"time", EFieldType::TimestampUsUtc, true}}});
        ASSERT_TRUE(profile);
        const auto location = "store://" + std::string(StoreID) + "/warehouse/" + std::string(CollectionID);
        auto metadata = iceberg::TableMetadata::Make(*profile->Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            location,
            profile->Properties);
        ASSERT_TRUE(metadata);
        Metadata_ = std::move(*metadata);
        Path_ = location + "/data/" + std::string(OperationID) + "-" + std::string(AttemptID) + "-00000.parquet";
    }

    void TearDown() override
    {
        IO_.reset();
        std::error_code error;
        std::filesystem::remove_all(Parent_, error);
        EXPECT_FALSE(error);
    }

    std::shared_ptr<arrow::RecordBatch> Batch(const TBatchValues& values = {})
    {
        arrow::StringBuilder keys;
        EXPECT_TRUE((values.NullKey ? keys.AppendNull() : keys.Append(values.Key)).ok());
        EXPECT_TRUE(keys.Append("common_ascii_prefix_longer_than_16_b").ok());
        EXPECT_TRUE(keys.Append(LongKey()).ok());
        arrow::Int64Builder counts;
        EXPECT_TRUE(counts.Append(1).ok());
        EXPECT_TRUE(counts.AppendNull().ok());
        EXPECT_TRUE(counts.Append(3).ok());
        arrow::BooleanBuilder enabled;
        EXPECT_TRUE(enabled.AppendValues(std::vector<std::uint8_t>{1, 0, 1}).ok());
        arrow::DoubleBuilder ratios;
        EXPECT_TRUE(ratios.Append(values.Ratio).ok());
        EXPECT_TRUE(ratios.AppendNull().ok());
        EXPECT_TRUE(ratios.Append(2.5).ok());
        arrow::TimestampBuilder times(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
        EXPECT_TRUE(times.Append(-62135596800000000LL).ok());
        EXPECT_TRUE(times.AppendNull().ok());
        EXPECT_TRUE(times.Append(values.Timestamp).ok());
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (arrow::ArrayBuilder* builder :
            std::array<arrow::ArrayBuilder*, 5>{&keys, &counts, &enabled, &ratios, &times})
        {
            auto array = builder->Finish();
            EXPECT_TRUE(array.ok());
            arrays.push_back(array.ok() ? *array : nullptr);
        }
        return arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false),
                                            arrow::field("count", arrow::int64()),
                                            arrow::field("enabled", arrow::boolean(), false),
                                            arrow::field("ratio", arrow::float64()),
                                            arrow::field("time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))}),
            3,
            std::move(arrays));
    }

    iceberg::Result<TDataWriteResult> Write(std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
        TDataWriterLimits limits = {},
        TParquetWriteSpec spec = {},
        std::string path = {})
    {
        auto attempt = NIO::TIOAttempt::Make({std::string(StoreID),
                                                 std::string(CollectionID),
                                                 std::string(AttemptID),
                                                 NIO::EAttemptKind::Append,
                                                 std::string(OperationID)},
            Latch_);
        if (!attempt) {
            return iceberg::IOError(attempt.Error().message());
        }
        Attempt_ = attempt.Value();
        auto scope = IO_->BindAttempt(Attempt_);
        if (!scope) {
            return std::unexpected(scope.error());
        }
        return WriteDataFile(IO_,
            Attempt_,
            *Metadata_,
            path.empty() ? Path_ : path,
            std::move(batches),
            limits,
            std::move(spec));
    }

    // Data file names end with a five-digit index.
    std::string OtherPath(int index) const
    {
        return Path_.substr(0, Path_.size() - std::string_view("00000.parquet").size()) +
            std::format("{:05}.parquet", index);
    }

    std::vector<arrow::Compression::type> FooterCodecs(const std::string& path, std::int64_t length)
    {
        auto file = IO_->NewInputFile(path);
        EXPECT_TRUE(file);
        auto schema = Metadata_->Schema();
        EXPECT_TRUE(schema);
        auto footer = ReadParquetFooter(**file, **schema, {length, 3});
        EXPECT_TRUE(footer) << (footer ? "" : footer.error().message);
        std::vector<arrow::Compression::type> codecs;
        if (footer) {
            for (int column = 0; column < (*footer)->num_columns(); ++column) {
                codecs.push_back((*footer)->RowGroup(0)->ColumnChunk(column)->compression());
            }
        }
        return codecs;
    }

    std::string Bytes(const std::string& path)
    {
        auto bytes = IO_->ReadFile(path, std::nullopt);
        EXPECT_TRUE(bytes);
        return bytes.value_or("");
    }

    std::filesystem::path LocalPath() const
    {
        return Parent_ / "store" / "warehouse" / CollectionID / "data" / std::filesystem::path(Path_).filename();
    }

    std::shared_ptr<parquet::FileMetaData> Footer() const
    {
        return parquet::ParquetFileReader::OpenFile(LocalPath().string())->metadata();
    }

    std::vector<std::int64_t> GroupRows() const
    {
        const auto footer = Footer();
        std::vector<std::int64_t> rows;
        for (int group = 0; group < footer->num_row_groups(); ++group) {
            rows.push_back(footer->RowGroup(group)->num_rows());
        }
        return rows;
    }

    std::shared_ptr<arrow::Table> ReadBack() const
    {
        auto input = arrow::io::ReadableFile::Open(LocalPath().string());
        EXPECT_TRUE(input.ok());
        auto reader = parquet::arrow::OpenFile(*input, arrow::default_memory_pool());
        EXPECT_TRUE(reader.ok());
        auto table = (*reader)->ReadTable();
        EXPECT_TRUE(table.ok());
        return table.ok() ? *table : nullptr;
    }

    std::size_t CountEvents(NTesting::EIOOp operation) const
    {
        const auto events = Fs_->Events();
        return static_cast<std::size_t>(std::ranges::count_if(events, [&](const auto& event) {
            return event.Op == operation && event.Path.ends_with(".parquet");
        }));
    }
};

TEST_F(TDataFileWriterTest, WritesAllTypesWithExactMetricsFullKeyBoundsAndVerifiedReceipt)
{
    const auto result = Write({Batch()});
    ASSERT_TRUE(result) << result.error().message;
    const auto& data = *result->DataFile;
    EXPECT_EQ(data.file_path, Path_);
    EXPECT_EQ(data.record_count, 3);
    EXPECT_EQ(data.partition_spec_id, 0);
    EXPECT_EQ(data.value_counts.at(1), 3);
    EXPECT_EQ(data.null_value_counts.at(2), 1);
    EXPECT_EQ(data.null_value_counts.at(4), 1);
    EXPECT_EQ(data.lower_bounds.at(1),
        std::vector<std::uint8_t>(std::string_view("common_ascii_prefix_longer_than_16_a").begin(),
            std::string_view("common_ascii_prefix_longer_than_16_a").end()));
    const auto upper = LongKey();
    EXPECT_EQ(data.upper_bounds.at(1), std::vector<std::uint8_t>(upper.begin(), upper.end()));
    EXPECT_EQ(static_cast<std::uint64_t>(data.file_size_in_bytes), result->Receipt.Length);
    EXPECT_GT(result->FooterBytes, 0);
    const auto record = Attempt_->Record(Path_);
    ASSERT_TRUE(record);
    EXPECT_EQ(record.Value().State, NIO::EFileState::DirSynced);
    EXPECT_TRUE(record.Value().FormatVerified);
    EXPECT_EQ(record.Value().Sha256, result->Receipt.Sha256);
    EXPECT_EQ(CountEvents(NTesting::EIOOp::SyncFile), 1);
}

TEST_F(TDataFileWriterTest, BoundsRowGroupsAndAcceptsEmptyBatchesAmongRows)
{
    // Row estimates are 70, 46 and 1058 bytes plus one 4-byte key offset per
    // group: the first two rows share a group, the wide third row is alone.
    const auto batch = Batch();
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 1100;
    const auto result = Write({batch->Slice(0, 0), batch->Slice(0, 1), batch->Slice(1, 2)}, limits);
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->DataFile->split_offsets.size(), 2);
    auto file = IO_->NewInputFile(Path_);
    ASSERT_TRUE(file);
    auto schema = Metadata_->Schema();
    ASSERT_TRUE(schema);
    const auto checked = CheckParquet(**file, **schema, {result->DataFile->file_size_in_bytes, 3});
    ASSERT_TRUE(checked);
    EXPECT_EQ(checked->RowGroupCount, 2);
    EXPECT_EQ(GroupRows(), (std::vector<std::int64_t>{2, 1}));
}

TEST_F(TDataFileWriterTest, RejectsInvalidOwnedInputBeforeCreatingAnyFile)
{
    const auto batch = Batch();
    auto wrongFields = batch->schema()->fields();
    wrongFields[0] = wrongFields[0]->WithName("wrong");
    auto wrongName = arrow::RecordBatch::Make(arrow::schema(wrongFields), 3, batch->columns());
    for (const auto& invalid : std::vector<std::shared_ptr<arrow::RecordBatch>>{nullptr,
             batch->Slice(0, 0),
             wrongName,
             Batch({.Key = std::string(1025, 'x')}),
             Batch({.Ratio = std::numeric_limits<double>::infinity()}),
             Batch({.Ratio = std::numeric_limits<double>::quiet_NaN()}),
             Batch({.Timestamp = 253402300800000000LL}),
             Batch({.Key = std::string("\xff", 1)}),
             Batch({.NullKey = true})})
    {
        EXPECT_FALSE(Write({invalid}));
        EXPECT_TRUE(Attempt_->Receipts().empty());
    }
    TDataWriterLimits limits;
    limits.MaxRows = 2;
    EXPECT_FALSE(Write({batch}, limits));
    limits = {};
    limits.MaxInputBytes = 1;
    EXPECT_FALSE(Write({batch}, limits));
    limits = {};
    limits.MaxRowGroupBytes = 100;
    EXPECT_FALSE(Write({batch}, limits));
    EXPECT_EQ(CountEvents(NTesting::EIOOp::CreateExclusive), 0);
}

TEST_F(TDataFileWriterTest, EmptySpecKeepsTheWriterDefaultsByteForByte)
{
    const auto defaults = Write({Batch()});
    ASSERT_TRUE(defaults) << defaults.error().message;
    const auto bytes = Bytes(Path_);
    // An empty spec adds no writer properties: zstd at its default level 1,
    // exactly what an explicit request for that codec produces.
    for (const auto& [index, level] :
        std::vector<std::pair<int, std::optional<std::int32_t>>>{{1, std::nullopt}, {2, 1}})
    {
        const auto path = OtherPath(index);
        const auto result =
            Write({Batch()}, {}, {.Compression = TParquetCompressionSpec{arrow::Compression::ZSTD, level}}, path);
        ASSERT_TRUE(result) << result.error().message;
        EXPECT_EQ(Bytes(path), bytes) << index;
    }
    EXPECT_EQ(FooterCodecs(Path_, defaults->DataFile->file_size_in_bytes),
        std::vector<arrow::Compression::type>(5, arrow::Compression::ZSTD));
}

TEST_F(TDataFileWriterTest, AppliesDefaultAndColumnCodecsAndVerifiesTheFooter)
{
    const TParquetWriteSpec spec{.Compression = TParquetCompressionSpec{arrow::Compression::GZIP, 5},
        .Columns = {{"key", {arrow::Compression::UNCOMPRESSED, std::nullopt}},
            {"ratio", {arrow::Compression::SNAPPY, std::nullopt}},
            {"time", {arrow::Compression::ZSTD, 19}}}};
    const auto result = Write({Batch()}, {}, spec);
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(FooterCodecs(Path_, result->DataFile->file_size_in_bytes),
        (std::vector<arrow::Compression::type>{arrow::Compression::UNCOMPRESSED,
            arrow::Compression::GZIP,
            arrow::Compression::GZIP,
            arrow::Compression::SNAPPY,
            arrow::Compression::ZSTD}));
    EXPECT_EQ(result->DataFile->record_count, 3);
    // A column codec without a level uses that codec's default, never the
    // file-level level of another codec (gzip 5 would be invalid for lz4 > 12
    // and is ignored by snappy).
    const auto lz4 = Write({Batch()},
        {},
        {.Compression = TParquetCompressionSpec{arrow::Compression::ZSTD, 22},
            .Columns = {{"count", {arrow::Compression::LZ4, std::nullopt}}}},
        OtherPath(1));
    ASSERT_TRUE(lz4) << lz4.error().message;
    EXPECT_EQ(FooterCodecs(OtherPath(1), lz4->DataFile->file_size_in_bytes)[1], arrow::Compression::LZ4);
}

TEST_F(TDataFileWriterTest, RejectsInvalidCodecSettingsBeforeCreatingAnyFile)
{
    for (const auto& spec : std::vector<TParquetWriteSpec>{
             {.Columns = {{"missing", {arrow::Compression::ZSTD, std::nullopt}}}},
             {.Compression = TParquetCompressionSpec{arrow::Compression::ZSTD, 99}},
             {.Compression = TParquetCompressionSpec{arrow::Compression::GZIP, 0}},
             {.Compression = TParquetCompressionSpec{arrow::Compression::SNAPPY, 1}},
             {.Columns = {{"key", {arrow::Compression::LZ4, 13}}}},
             {.Columns = {{"key", {arrow::Compression::UNCOMPRESSED, 1}}}},
             {.Compression = TParquetCompressionSpec{arrow::Compression::BROTLI, std::nullopt}},
         })
    {
        EXPECT_FALSE(Write({Batch()}, {}, spec));
        EXPECT_TRUE(Attempt_->Receipts().empty());
    }
    EXPECT_EQ(CountEvents(NTesting::EIOOp::CreateExclusive), 0);
}

TEST_F(TDataFileWriterTest, OversizedFinalFileIsAnUnpublishedOrphan)
{
    TDataWriterLimits limits;
    limits.MaxFileBytes = 16;
    EXPECT_FALSE(Write({Batch()}, limits));
    EXPECT_TRUE(Attempt_->FirstError());
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_EQ(Attempt_->Record(Path_).Value().State, NIO::EFileState::Failed);
    EXPECT_EQ(CountEvents(NTesting::EIOOp::CreateExclusive), 1);
}

TEST_F(TDataFileWriterTest, FailedFirstCloseNeverBecomesSuccessfulReceipt)
{
    Fs_->AddRule({NTesting::EIOOp::Close, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_FALSE(Write({Batch()}));
    EXPECT_EQ(Attempt_->FirstError(), std::errc::io_error);
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_TRUE(Latch_->IsSet());
    EXPECT_EQ(CountEvents(NTesting::EIOOp::Close), 1);
    EXPECT_EQ(CountEvents(NTesting::EIOOp::PRead), 0);
}

TEST_F(TDataFileWriterTest, MaskedWriteErrorIsDetectedWithoutRepeatedIo)
{
    Fs_->AddRule(
        {NTesting::EIOOp::Write, "*.parquet", 1, 1, std::make_error_code(std::errc::no_space_on_device), std::nullopt});
    EXPECT_FALSE(Write({Batch()}));
    EXPECT_EQ(Attempt_->FirstError(), std::errc::no_space_on_device);
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_FALSE(Latch_->IsSet());
    EXPECT_EQ(CountEvents(NTesting::EIOOp::Write), 1);
    EXPECT_EQ(CountEvents(NTesting::EIOOp::Close), 1);
}

TEST_F(TDataFileWriterTest, ReadbackErrorInvalidatesDurableButUnverifiedFile)
{
    Fs_->AddRule({NTesting::EIOOp::PRead, "*.parquet", 1, 1, std::make_error_code(std::errc::io_error), std::nullopt});
    EXPECT_FALSE(Write({Batch()}));
    EXPECT_EQ(CountEvents(NTesting::EIOOp::SyncFile), 1);
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_FALSE(Attempt_->Record(Path_).Value().FormatVerified);
    EXPECT_FALSE(Latch_->IsSet());
}

// Optional payload strings up to 1 MiB, next to the required key.
class TRowGroupLayoutTest : public TDataFileWriterTest
{
protected:
    void SetUp() override
    {
        TDataFileWriterTest::SetUp();
        auto profile = MakeTableProfile({{{"key", EFieldType::String}, {"payload", EFieldType::String, true}}});
        ASSERT_TRUE(profile);
        auto metadata = iceberg::TableMetadata::Make(*profile->Schema,
            *iceberg::PartitionSpec::Unpartitioned(),
            *iceberg::SortOrder::Unsorted(),
            Metadata_->location,
            profile->Properties);
        ASSERT_TRUE(metadata);
        Metadata_ = std::move(*metadata);
    }

    // Payload lengths in 'x' bytes; nullopt is NULL. Keys are k00000, k00001, ...
    static std::shared_ptr<arrow::RecordBatch> Payloads(const std::vector<std::optional<std::size_t>>& lengths)
    {
        arrow::StringBuilder keys;
        arrow::StringBuilder payloads;
        for (std::size_t row = 0; row < lengths.size(); ++row) {
            auto key = std::to_string(row);
            EXPECT_TRUE(keys.Append("k" + std::string(5 - std::min<std::size_t>(5, key.size()), '0') + key).ok());
            EXPECT_TRUE((lengths[row] ? payloads.Append(std::string(*lengths[row], 'x')) : payloads.AppendNull()).ok());
        }
        std::shared_ptr<arrow::Array> keyArray;
        std::shared_ptr<arrow::Array> payloadArray;
        EXPECT_TRUE(keys.Finish(&keyArray).ok());
        EXPECT_TRUE(payloads.Finish(&payloadArray).ok());
        return arrow::RecordBatch::Make(
            arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("payload", arrow::utf8())}),
            static_cast<std::int64_t>(lengths.size()),
            {keyArray, payloadArray});
    }

    // The documented decoded accounting, computed independently of the writer:
    // per field one validity byte and four offset bytes plus the string bytes,
    // and one trailing offset per string field and group.
    static std::int64_t RowBytes(const arrow::RecordBatch& batch, std::int64_t row)
    {
        std::int64_t bytes = 0;
        for (const auto& column : batch.columns()) {
            bytes += 5;
            if (!column->IsNull(row)) {
                bytes += static_cast<std::int64_t>(static_cast<const arrow::StringArray&>(*column).GetView(row).size());
            }
        }
        return bytes;
    }

    void ExpectGroupsWithin(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, std::int64_t cap)
    {
        std::vector<std::int64_t> rowBytes;
        for (const auto& batch : batches) {
            for (std::int64_t row = 0; row < batch->num_rows(); ++row) {
                rowBytes.push_back(RowBytes(*batch, row));
            }
        }
        const auto groups = GroupRows();
        std::size_t next = 0;
        for (std::size_t group = 0; group < groups.size(); ++group) {
            if (group + 1 < groups.size()) {
                EXPECT_EQ(groups[group], groups.front()) << "pinned writer layout: equal groups but the last";
            }
            std::int64_t bytes = 8;
            for (std::int64_t row = 0; row < groups[group]; ++row) {
                bytes += rowBytes.at(next++);
            }
            EXPECT_LE(bytes, cap) << "row group " << group;
        }
        EXPECT_EQ(next, rowBytes.size());
    }

    void ExpectReadBack(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
    {
        auto expected = arrow::Table::FromRecordBatches(batches.front()->schema(), batches);
        ASSERT_TRUE(expected.ok());
        const auto actual = ReadBack();
        ASSERT_NE(actual, nullptr);
        ASSERT_EQ(actual->num_rows(), (*expected)->num_rows());
        for (int column = 0; column < actual->num_columns(); ++column) {
            EXPECT_TRUE(actual->column(column)->Equals(*(*expected)->column(column))) << column;
        }
    }
};

std::vector<std::optional<std::size_t>> Uniform()
{
    std::vector<std::optional<std::size_t>> lengths(10000);
    for (std::size_t row = 0; row < lengths.size(); ++row) {
        lengths[row] = row < 8575 ? 106 : 105;
    }
    return lengths;
}

std::vector<std::optional<std::size_t>> Skewed(std::size_t wideRow = 0)
{
    std::vector<std::optional<std::size_t>> lengths(10000, 1);
    lengths[wideRow] = 1U << 20;
    return lengths;
}

// Issue #102: equal Arrow buffer bytes, one 1 MiB value previously forced
// 1429 groups of 4..7 rows, while the uniform control formed one group.
TEST_F(TRowGroupLayoutTest, EqualByteUniformAndSkewedInputsFormOneGroup)
{
    const auto uniform = Payloads(Uniform());
    const auto skewed = Payloads(Skewed());
    EXPECT_EQ(arrow::util::TotalBufferSize(*uniform), arrow::util::TotalBufferSize(*skewed));
    for (const auto& batch : {uniform, skewed}) {
        const auto result = Write({batch});
        ASSERT_TRUE(result) << result.error().message;
        EXPECT_EQ(GroupRows(), std::vector<std::int64_t>{10000});
        EXPECT_EQ(result->DataFile->split_offsets.size(), 1U);
        EXPECT_EQ(result->DataFile->record_count, 10000);
        EXPECT_EQ(result->DataFile->value_counts.at(2), 10000);
        EXPECT_EQ(result->DataFile->null_value_counts.at(2), 0);
        ExpectGroupsWithin({batch}, TDataWriterLimits{}.MaxRowGroupBytes);
        ExpectReadBack({batch});
        TearDown();
        SetUp();
    }
}

// With a bound smaller than the input, groups adapt to where the wide values
// are instead of sizing every group for the widest row.
TEST_F(TRowGroupLayoutTest, WideValuesAtStartMiddleAndEndWithNulls)
{
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 64 << 10;
    for (const std::size_t wide : {std::size_t{0}, std::size_t{1500}, std::size_t{2999}}) {
        SCOPED_TRACE(wide);
        std::vector<std::optional<std::size_t>> lengths(3000);
        for (std::size_t row = 0; row < lengths.size(); ++row) {
            lengths[row] = row % 5 == 0 ? std::nullopt : std::optional<std::size_t>(row % 13);
        }
        lengths[wide] = 40 << 10;
        const auto batch = Payloads(lengths);
        const auto result = Write({batch}, limits);
        ASSERT_TRUE(result) << result.error().message;
        // About 105 KiB in total; the widest-row rule formed 3000 one-row groups.
        const auto groups = GroupRows();
        EXPECT_GE(groups.size(), 2U);
        EXPECT_LE(groups.size(), 4U);
        ExpectGroupsWithin({batch}, limits.MaxRowGroupBytes);
        ExpectReadBack({batch});
        EXPECT_EQ(result->DataFile->null_value_counts.at(2), std::ranges::count(lengths, std::optional<std::size_t>{}));
        EXPECT_EQ(result->DataFile->value_counts.at(2), 3000);
        TearDown();
        SetUp();
    }
}

// The pinned writer continues one buffered group across Write calls, so the
// layout depends on input order, not on batch or chunk boundaries.
TEST_F(TRowGroupLayoutTest, LayoutIsIndependentOfInputBatchBoundaries)
{
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 64 << 10;
    std::vector<std::optional<std::size_t>> lengths(2000, 3);
    lengths[700] = 30 << 10;
    lengths[701] = std::nullopt;
    lengths[1400] = 30 << 10;
    const auto whole = Payloads(lengths);
    ASSERT_TRUE(Write({whole}, limits));
    const auto expected = GroupRows();
    EXPECT_LT(expected.size(), 10U);
    TearDown();
    SetUp();
    const std::vector<std::shared_ptr<arrow::RecordBatch>> chunks{whole->Slice(0, 700),
        whole->Slice(700, 1),
        whole->Slice(701, 0),
        whole->Slice(701, 699),
        whole->Slice(1400, 600)};
    const auto result = Write(chunks, limits);
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(GroupRows(), expected);
    ExpectGroupsWithin(chunks, limits.MaxRowGroupBytes);
    ExpectReadBack({whole});
}

TEST_F(TRowGroupLayoutTest, RowExactlyAtTheBoundIsAcceptedAndOneByteOverIsRejected)
{
    // Four 17-byte rows, then a row whose estimate plus both string offsets
    // equals the bound: it can share a group with no other row.
    const auto batch = Payloads({1, 1, 1, 1, 1000});
    const auto wide = RowBytes(*batch, 4);
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = wide + 8;
    const auto result = Write({batch}, limits);
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(GroupRows(), (std::vector<std::int64_t>{4, 1}));
    ExpectGroupsWithin({batch}, limits.MaxRowGroupBytes);
    ExpectReadBack({batch});
    TearDown();
    SetUp();
    limits.MaxRowGroupBytes = wide + 7;
    const auto created = CountEvents(NTesting::EIOOp::CreateExclusive);
    EXPECT_FALSE(Write({batch}, limits));
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_EQ(CountEvents(NTesting::EIOOp::CreateExclusive), created);
}

// A write failure after a complete row group reached the file still leaves an
// unpublished, unverified file and the first error. A fault-free run of the
// same deterministic input locates the first low-level write that starts
// after the first row group's bytes; the second run fails exactly there.
TEST_F(TRowGroupLayoutTest, WriteFailureAfterACompleteRowGroupLeavesNoReceipt)
{
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 64 << 10;
    std::vector<std::optional<std::size_t>> lengths(3000, 7);
    lengths[1500] = 40 << 10;
    const auto batch = Payloads(lengths);
    const auto parquetWrites = [&](std::size_t from) {
        std::vector<NTesting::TIOEvent> writes;
        const auto events = Fs_->Events();
        for (std::size_t index = from; index < events.size(); ++index) {
            if (events[index].Op == NTesting::EIOOp::Write && events[index].Path.ends_with(".parquet")) {
                writes.push_back(events[index]);
            }
        }
        return writes;
    };

    auto from = Fs_->Events().size();
    ASSERT_TRUE(Write({batch}, limits));
    const auto footer = Footer();
    ASSERT_GE(footer->num_row_groups(), 2);
    const auto second = footer->RowGroup(1)->ColumnChunk(0);
    const auto firstGroupEnd = static_cast<std::uint64_t>(
        second->has_dictionary_page() ? second->dictionary_page_offset() : second->data_page_offset());
    std::uint64_t written = 0;
    std::uint64_t occurrence = 0;
    for (const auto& write : parquetWrites(from)) {
        if (written >= firstGroupEnd) {
            break;
        }
        ++occurrence;
        written += write.Bytes;
    }
    ++occurrence;
    ASSERT_GE(written, firstGroupEnd);
    ASSERT_LE(occurrence, parquetWrites(from).size()) << "no low-level write starts after the first row group";
    TearDown();
    SetUp();

    Fs_->AddRule({NTesting::EIOOp::Write,
        "*.parquet",
        occurrence,
        1,
        std::make_error_code(std::errc::no_space_on_device),
        std::nullopt});
    from = Fs_->Events().size();
    EXPECT_FALSE(Write({batch}, limits));
    const auto writes = parquetWrites(from);
    ASSERT_EQ(writes.size(), occurrence);
    std::uint64_t before = 0;
    for (std::size_t index = 0; index + 1 < writes.size(); ++index) {
        EXPECT_FALSE(writes[index].Error);
        before += writes[index].Bytes;
    }
    // Prerequisite: the whole first row group was in the file before the fault.
    EXPECT_GE(before, firstGroupEnd);
    EXPECT_EQ(writes.back().Error, std::errc::no_space_on_device);
    EXPECT_EQ(Attempt_->FirstError(), std::errc::no_space_on_device);
    EXPECT_TRUE(Attempt_->Receipts().empty());
    EXPECT_FALSE(Attempt_->Record(Path_).Value().FormatVerified);
    EXPECT_EQ(Attempt_->Record(Path_).Value().State, NIO::EFileState::Failed);
}

// Uniform rows keep the previous layout: the bound divided by the row size.
TEST_F(TRowGroupLayoutTest, UniformRowsKeepTheWidestRowLayout)
{
    const auto batch = Payloads(std::vector<std::optional<std::size_t>>(30, 20));
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 8 + 7 * RowBytes(*batch, 0) + 3;
    ASSERT_TRUE(Write({batch->Slice(0, 11), batch->Slice(11)}, limits));
    EXPECT_EQ(GroupRows(), (std::vector<std::int64_t>{7, 7, 7, 7, 2}));
    ExpectReadBack({batch});
}

}
}
