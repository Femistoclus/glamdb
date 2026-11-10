#include "core/storage/data_file_writer.h"

#include "core/storage/durable_local_file_io.h"
#include "core/storage/format_verifier.h"
#include "core/storage/table_profile.h"
#include "tests/support/fault_fs.h"

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/partition_spec.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_metadata.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <filesystem>
#include <limits>

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
        TDataWriterLimits limits = {})
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
        return WriteDataFile(IO_, Attempt_, *Metadata_, Path_, std::move(batches), limits);
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
    const auto batch = Batch();
    TDataWriterLimits limits;
    limits.MaxRowGroupBytes = 1100;
    const auto result = Write({batch->Slice(0, 0), batch->Slice(0, 1), batch->Slice(1, 2)}, limits);
    ASSERT_TRUE(result) << result.error().message;
    EXPECT_EQ(result->DataFile->split_offsets.size(), 3);
    auto file = IO_->NewInputFile(Path_);
    ASSERT_TRUE(file);
    auto schema = Metadata_->Schema();
    ASSERT_TRUE(schema);
    const auto checked = CheckParquet(**file, **schema, {result->DataFile->file_size_in_bytes, 3});
    ASSERT_TRUE(checked);
    EXPECT_EQ(checked->RowGroupCount, 3);
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

}
}
