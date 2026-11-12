// The pruning decision itself: which statistics may exclude a file or row
// group, and that anything missing, truncated, NaN-affected or negated keeps it.

#include "core/scan/scan_pruning.h"
#include "core/storage/scan_index.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <iceberg/manifest/manifest_entry.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>

#include <cmath>
#include <cstring>
#include <limits>

namespace glamdb::detail {
namespace {

const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"value", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"flag", FieldType::Bool, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

TBoundPredicate Bind(const Predicate& predicate)
{
    auto bound = BindPredicate(predicate, Schema);
    EXPECT_TRUE(bound) << (bound ? std::string() : bound.error().message);
    return *bound;
}

std::vector<uint8_t> Int64Bytes(int64_t value)
{
    std::vector<uint8_t> bytes(8);
    for (size_t index = 0; index < 8; ++index) {
        bytes[index] = static_cast<uint8_t>(static_cast<uint64_t>(value) >> (8 * index));
    }
    return bytes;
}

std::vector<uint8_t> DoubleBytes(double value)
{
    int64_t bits = 0;
    std::memcpy(&bits, &value, sizeof(bits));
    return Int64Bytes(bits);
}

std::vector<uint8_t> TextBytes(std::string_view text)
{
    return {text.begin(), text.end()};
}

// Stats of every referenced field of `predicate`, taken from the scan-index
// encoding of `file`'s manifest metrics.
bool MayMatchFile(const Predicate& predicate, const iceberg::DataFile& file)
{
    const auto bound = Bind(predicate);
    const auto encoded = NGlamDB::NStorage::EncodeScanMetrics(file);
    std::vector<TFieldStats> stats;
    for (const auto field : bound.ReferencedFields) {
        const auto metrics = NGlamDB::NStorage::FindScanFieldMetrics(encoded, static_cast<int32_t>(field + 1));
        stats.push_back(
            FileFieldStats(metrics ? &*metrics : nullptr, file.record_count, Schema.fields[field].type, 16));
    }
    return MayMatch(bound, stats);
}

iceberg::DataFile File(int64_t rows)
{
    iceberg::DataFile file;
    file.record_count = rows;
    return file;
}

TEST(ScanPruningRules, IntegerBoundsExcludeOnlyImpossibleComparisons)
{
    auto file = File(10);
    file.lower_bounds[2] = Int64Bytes(10);
    file.upper_bounds[2] = Int64Bytes(20);
    file.null_value_counts[2] = 3;
    const auto value = [](CompareOp op, int64_t literal) {
        return Predicate::Compare("value", op, literal);
    };
    EXPECT_FALSE(MayMatchFile(value(CompareOp::Eq, 9), file));
    EXPECT_TRUE(MayMatchFile(value(CompareOp::Eq, 10), file));
    EXPECT_TRUE(MayMatchFile(value(CompareOp::Eq, 20), file));
    EXPECT_FALSE(MayMatchFile(value(CompareOp::Eq, 21), file));
    EXPECT_FALSE(MayMatchFile(value(CompareOp::Lt, 10), file));
    EXPECT_TRUE(MayMatchFile(value(CompareOp::Le, 10), file));
    EXPECT_FALSE(MayMatchFile(value(CompareOp::Gt, 20), file));
    EXPECT_TRUE(MayMatchFile(value(CompareOp::Ge, 20), file));
    EXPECT_TRUE(MayMatchFile(value(CompareOp::Ne, 10), file));
    EXPECT_FALSE(MayMatchFile(Predicate::In("value", {Literal{int64_t{1}}, Literal{int64_t{30}}, std::nullopt}), file));
    EXPECT_TRUE(MayMatchFile(Predicate::In("value", {Literal{int64_t{1}}, Literal{int64_t{15}}}), file));
    EXPECT_FALSE(MayMatchFile(Predicate::In("value", {}), file));
    EXPECT_FALSE(MayMatchFile(Predicate::In("value", {std::nullopt}), file));
    EXPECT_FALSE(MayMatchFile(Predicate::CompareNull("value", CompareOp::Eq), file));
    EXPECT_TRUE(MayMatchFile(Predicate::IsNull("value"), file));
    EXPECT_TRUE(MayMatchFile(Predicate::IsNotNull("value"), file));

    // A constant column excludes "<>" its value.
    file.upper_bounds[2] = Int64Bytes(10);
    EXPECT_FALSE(MayMatchFile(value(CompareOp::Ne, 10), file));
    EXPECT_TRUE(MayMatchFile(value(CompareOp::Ne, 11), file));
}

TEST(ScanPruningRules, NullCountsDecideNullChecks)
{
    auto file = File(10);
    EXPECT_TRUE(MayMatchFile(Predicate::IsNull("value"), file));
    EXPECT_TRUE(MayMatchFile(Predicate::IsNotNull("value"), file));
    file.null_value_counts[2] = 0;
    EXPECT_FALSE(MayMatchFile(Predicate::IsNull("value"), file));
    file.null_value_counts[2] = 10;
    EXPECT_FALSE(MayMatchFile(Predicate::IsNotNull("value"), file));
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("value", CompareOp::Ne, int64_t{1}), file));
    EXPECT_TRUE(MayMatchFile(Predicate::IsNull("value"), file));
    // Inconsistent counts are not used at all (the index omits the field).
    file.value_counts[2] = 9;
    EXPECT_TRUE(MayMatchFile(Predicate::IsNotNull("value"), file));
    file.value_counts[2] = 10;
    file.null_value_counts[2] = 11;
    EXPECT_TRUE(MayMatchFile(Predicate::IsNotNull("value"), file));
    // No rows: nothing can match.
    EXPECT_FALSE(MayMatchFile(Predicate::IsNull("value"), File(0)));
}

TEST(ScanPruningRules, MissingBoundsAndUnsupportedShapesKeepTheFile)
{
    const auto file = File(10);
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("value", CompareOp::Eq, int64_t{5}), file));
    auto oneSided = File(10);
    oneSided.lower_bounds[2] = Int64Bytes(10);
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("value", CompareOp::Lt, int64_t{5}), oneSided));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("value", CompareOp::Gt, int64_t{50}), oneSided));
    // Malformed bounds are ignored.
    auto malformed = File(10);
    malformed.lower_bounds[2] = {1, 2, 3};
    malformed.upper_bounds[2] = {1, 2, 3};
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("value", CompareOp::Eq, int64_t{5}), malformed));

    auto bounded = File(10);
    bounded.lower_bounds[2] = Int64Bytes(10);
    bounded.upper_bounds[2] = Int64Bytes(20);
    const auto impossible = Predicate::Compare("value", CompareOp::Gt, int64_t{50});
    const auto possible = Predicate::Compare("value", CompareOp::Eq, int64_t{15});
    // NOT is never reasoned about, even when its child is provably FALSE or TRUE.
    EXPECT_TRUE(MayMatchFile(Predicate::Not(impossible), bounded));
    EXPECT_TRUE(MayMatchFile(Predicate::Not(possible), bounded));
    EXPECT_TRUE(MayMatchFile(Predicate::Not(Predicate::Not(impossible)), bounded));
    // AND prunes when any child does, OR only when all do.
    EXPECT_FALSE(MayMatchFile(Predicate::And({possible, impossible}), bounded));
    EXPECT_FALSE(MayMatchFile(Predicate::And({Predicate::Not(possible), impossible}), bounded));
    EXPECT_TRUE(MayMatchFile(Predicate::Or({possible, impossible}), bounded));
    EXPECT_FALSE(MayMatchFile(Predicate::Or({impossible, Predicate::CompareNull("key", CompareOp::Eq)}), bounded));
    EXPECT_TRUE(MayMatchFile(Predicate::Or({impossible, Predicate::Not(impossible)}), bounded));
    EXPECT_TRUE(MayMatchFile(Predicate::True(), bounded));
}

TEST(ScanPruningRules, FloatBoundsNeedAKnownZeroNaNCount)
{
    auto file = File(10);
    file.lower_bounds[3] = DoubleBytes(0.0);
    file.upper_bounds[3] = DoubleBytes(10.0);
    const auto above = Predicate::Compare("score", CompareOp::Gt, 100.0);
    EXPECT_TRUE(MayMatchFile(above, file));
    file.nan_value_counts[3] = 2;
    EXPECT_TRUE(MayMatchFile(above, file));
    file.nan_value_counts[3] = 0;
    EXPECT_FALSE(MayMatchFile(above, file));
    // Negative zero bounds compare equal to the normalized zero literal.
    file.lower_bounds[3] = DoubleBytes(-0.0);
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("score", CompareOp::Le, 0.0), file));
    // A NaN or infinite bound is not a bound.
    file.upper_bounds[3] = DoubleBytes(std::numeric_limits<double>::quiet_NaN());
    EXPECT_TRUE(MayMatchFile(above, file));
    file.upper_bounds[3] = DoubleBytes(std::numeric_limits<double>::infinity());
    EXPECT_TRUE(MayMatchFile(above, file));
}

TEST(ScanPruningRules, PossiblyTruncatedStringUpperBoundsAreIgnored)
{
    auto file = File(10);
    // A naive 16-code-point truncation of "abcdefghijklmnopz" would be the
    // smaller "abcdefghijklmnop"; it must not exclude the larger real value.
    file.lower_bounds[1] = TextBytes("abcdefghijklmnop");
    file.upper_bounds[1] = TextBytes("abcdefghijklmnop");
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Eq, std::string("abcdefghijklmnopz")), file));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Ne, std::string("abcdefghijklmnop")), file));
    // The prefix lower bound still excludes smaller values.
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("key", CompareOp::Lt, std::string("abc")), file));
    // Code points, not bytes: 15 two-byte characters are not truncated.
    std::string cyrillic;
    for (int index = 0; index < 15; ++index) {
        cyrillic += "\xD0\xB0";
    }
    file.lower_bounds[1] = TextBytes(cyrillic);
    file.upper_bounds[1] = TextBytes(cyrillic);
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("key", CompareOp::Gt, cyrillic), file));
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("key", CompareOp::Ne, cyrillic), file));
    file.upper_bounds[1] = TextBytes(cyrillic + "\xD0\xB0");
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Gt, cyrillic + "\xD0\xB0"), file));
    // Unsigned byte order, as the residual filter.
    file.lower_bounds[1] = TextBytes("\xC3\xA9");
    file.upper_bounds[1] = TextBytes("\xC3\xA9");
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("key", CompareOp::Lt, std::string("z")), file));
}

TEST(ScanPruningRules, IndexEncodingRoundTripsAndRejectsTruncation)
{
    auto file = File(10);
    file.null_value_counts[2] = 3;
    file.lower_bounds[2] = Int64Bytes(-5);
    file.upper_bounds[1] = TextBytes("key");
    file.nan_value_counts[3] = 0;
    const auto encoded = NGlamDB::NStorage::EncodeScanMetrics(file);
    const auto value = NGlamDB::NStorage::FindScanFieldMetrics(encoded, 2);
    ASSERT_TRUE(value);
    EXPECT_EQ(value->NullCount, 3);
    EXPECT_FALSE(value->NanCount);
    const auto lower = Int64Bytes(-5);
    EXPECT_EQ(value->Lower, std::string_view(reinterpret_cast<const char*>(lower.data()), lower.size()));
    EXPECT_FALSE(value->Upper);
    const auto key = NGlamDB::NStorage::FindScanFieldMetrics(encoded, 1);
    ASSERT_TRUE(key);
    EXPECT_EQ(key->Upper, "key");
    EXPECT_EQ(NGlamDB::NStorage::FindScanFieldMetrics(encoded, 3)->NanCount, 0);
    EXPECT_FALSE(NGlamDB::NStorage::FindScanFieldMetrics(encoded, 4));
    // Any truncated encoding yields nothing for the fields it cuts.
    for (size_t size = 0; size + 1 < encoded.size(); ++size) {
        const auto prefix = std::string_view(encoded).substr(0, size);
        const auto found = NGlamDB::NStorage::FindScanFieldMetrics(prefix, 1);
        EXPECT_TRUE(!found || found->Upper == "key");
    }
}

TEST(ScanPruningRules, BooleanAndTimestampBounds)
{
    auto file = File(4);
    file.lower_bounds[4] = {1};
    file.upper_bounds[4] = {1};
    file.lower_bounds[5] = Int64Bytes(1000);
    file.upper_bounds[5] = Int64Bytes(2000);
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("flag", CompareOp::Eq, false), file));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("flag", CompareOp::Eq, true), file));
    EXPECT_FALSE(MayMatchFile(Predicate::In("flag", {Literal{false}}), file));
    EXPECT_FALSE(MayMatchFile(Predicate::Compare("at", CompareOp::Lt, TimestampUs{1000}), file));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("at", CompareOp::Le, TimestampUs{1000}), file));
    file.lower_bounds[4] = {2};
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("flag", CompareOp::Eq, false), file));
}

// One row group written by the Parquet writer with default statistics.
std::shared_ptr<parquet::FileMetaData> WriteGroup(const std::shared_ptr<arrow::Array>& doubles,
    const std::shared_ptr<arrow::Array>& strings)
{
    const auto schema = arrow::schema({arrow::field("score", arrow::float64()), arrow::field("name", arrow::utf8())});
    const auto table = arrow::Table::Make(schema, {doubles, strings});
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    EXPECT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 1024).ok());
    const auto buffer = sink->Finish().ValueOrDie();
    return parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer))->metadata();
}

TEST(ScanPruningRules, RowGroupStatisticsAreExactAndNaNAware)
{
    arrow::DoubleBuilder doubles;
    ASSERT_TRUE(doubles.AppendValues({1.0, 2.0, std::numeric_limits<double>::quiet_NaN()}).ok());
    ASSERT_TRUE(doubles.AppendNull().ok());
    arrow::StringBuilder strings;
    ASSERT_TRUE(strings.AppendValues({"b", "c", "d"}).ok());
    ASSERT_TRUE(strings.AppendNull().ok());
    const auto metadata = WriteGroup(doubles.Finish().ValueOrDie(), strings.Finish().ValueOrDie());
    const auto group = metadata->RowGroup(0);

    // Parquet excludes NaN from min/max: without the manifest's zero NaN count
    // the bounds are not used.
    const auto withNaN = RowGroupFieldStats(*group, 0, FieldType::Float64, false);
    EXPECT_EQ(withNaN.Rows, 4);
    EXPECT_EQ(withNaN.Nulls, 1);
    EXPECT_FALSE(withNaN.Lower);
    EXPECT_FALSE(withNaN.Upper);
    const auto trusted = RowGroupFieldStats(*group, 0, FieldType::Float64, true);
    EXPECT_EQ(trusted.Lower, TStatValue(1.0));
    EXPECT_EQ(trusted.Upper, TStatValue(2.0));

    const auto names = RowGroupFieldStats(*group, 1, FieldType::String, false);
    EXPECT_EQ(names.Lower, TStatValue(std::string("b")));
    EXPECT_EQ(names.Upper, TStatValue(std::string("d")));
    EXPECT_EQ(names.Nulls, 1);
    // A type that does not match the physical column yields no bounds.
    const auto mismatched = RowGroupFieldStats(*group, 1, FieldType::Int64, true);
    EXPECT_FALSE(mismatched.Lower);
    EXPECT_FALSE(mismatched.Upper);
    // Out of range columns yield nothing usable.
    const auto missing = RowGroupFieldStats(*group, 7, FieldType::Int64, true);
    EXPECT_FALSE(missing.Nulls);
    EXPECT_GT(missing.Rows, 0);
}

TEST(ScanPruningRules, ContradictoryOrMalformedManifestBoundsPruneNothing)
{
    // A one-row file whose bounds are reversed: value 15 may exist.
    auto ints = File(1);
    ints.null_value_counts[2] = 0;
    ints.lower_bounds[2] = Int64Bytes(20);
    ints.upper_bounds[2] = Int64Bytes(10);
    for (const auto op : {CompareOp::Eq, CompareOp::Lt, CompareOp::Gt, CompareOp::Ne}) {
        EXPECT_TRUE(MayMatchFile(Predicate::Compare("value", op, int64_t{15}), ints));
    }
    EXPECT_TRUE(MayMatchFile(Predicate::In("value", {Literal{int64_t{15}}}), ints));
    // The counts of a file with contradictory bounds are not trusted either.
    EXPECT_TRUE(MayMatchFile(Predicate::IsNull("value"), ints));

    auto doubles = File(1);
    doubles.nan_value_counts[3] = 0;
    doubles.lower_bounds[3] = DoubleBytes(2.0);
    doubles.upper_bounds[3] = DoubleBytes(1.0);
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("score", CompareOp::Eq, 1.5), doubles));

    auto flags = File(1);
    flags.lower_bounds[4] = {1};
    flags.upper_bounds[4] = {0};
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("flag", CompareOp::Eq, false), flags));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("flag", CompareOp::Eq, true), flags));

    auto times = File(1);
    times.lower_bounds[5] = Int64Bytes(2000);
    times.upper_bounds[5] = Int64Bytes(1000);
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("at", CompareOp::Eq, TimestampUs{1500}), times));

    auto strings = File(1);
    strings.lower_bounds[1] = TextBytes("b");
    strings.upper_bounds[1] = TextBytes("a");
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Eq, std::string("ab")), strings));

    // Invalid UTF-8: an upper bound of only 0xC3 must not exclude "\xC3\xA9" (é),
    // and a malformed lower bound must not exclude anything either.
    auto badUpper = File(1);
    badUpper.lower_bounds[1] = TextBytes("a");
    badUpper.upper_bounds[1] = TextBytes("\xC3");
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Eq, std::string("\xC3\xA9")), badUpper));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Lt, std::string("a")), badUpper));
    auto badLower = File(1);
    badLower.lower_bounds[1] = TextBytes("\xFF");
    badLower.upper_bounds[1] = TextBytes("b");
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Gt, std::string("c")), badLower));
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("key", CompareOp::Eq, std::string("a")), badLower));

    // One bound of the wrong width makes the other unusable too.
    auto wide = File(1);
    wide.lower_bounds[2] = Int64Bytes(20);
    wide.upper_bounds[2] = {1, 2, 3};
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("value", CompareOp::Lt, int64_t{5}), wide));
    auto wideBool = File(1);
    wideBool.lower_bounds[4] = {1, 1};
    wideBool.upper_bounds[4] = {1};
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("flag", CompareOp::Eq, false), wideBool));

    // Bounds in a column whose null count says every value is NULL contradict it.
    auto allNull = File(2);
    allNull.null_value_counts[2] = 2;
    allNull.lower_bounds[2] = Int64Bytes(1);
    allNull.upper_bounds[2] = Int64Bytes(1);
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("value", CompareOp::Eq, int64_t{1}), allNull));
    EXPECT_TRUE(MayMatchFile(Predicate::IsNotNull("value"), allNull));
    // Negative and excessive counts are ignored.
    auto counts = File(2);
    counts.null_value_counts[2] = -1;
    EXPECT_TRUE(MayMatchFile(Predicate::IsNull("value"), counts));
    counts.null_value_counts[2] = 3;
    EXPECT_TRUE(MayMatchFile(Predicate::IsNotNull("value"), counts));
    auto nans = File(2);
    nans.nan_value_counts[3] = -1;
    nans.lower_bounds[3] = DoubleBytes(0.0);
    nans.upper_bounds[3] = DoubleBytes(1.0);
    EXPECT_TRUE(MayMatchFile(Predicate::Compare("score", CompareOp::Gt, 5.0), nans));
}

// One row group of one column with the given encoded statistics, built with the
// Parquet metadata builder, so values the writer would never produce reach the
// reader unchanged (the footer verifier does not order min/max).
std::shared_ptr<parquet::FileMetaData>
GroupWithStatistics(parquet::schema::NodePtr node, parquet::EncodedStatistics statistics, int64_t rows)
{
    const auto root = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, {std::move(node)});
    parquet::SchemaDescriptor schema;
    schema.Init(root);
    const auto properties = parquet::WriterProperties::Builder().build();
    auto builder = parquet::FileMetaDataBuilder::Make(&schema, properties);
    auto* group = builder->AppendRowGroup();
    auto* column = group->NextColumnChunk();
    statistics.is_min_value_exact = true;
    statistics.is_max_value_exact = true;
    column->SetStatistics(statistics);
    column->Finish(rows, 0, 0, 4, 100, 100, false, false, {}, {{parquet::Encoding::PLAIN, 1}});
    group->set_num_rows(rows);
    group->Finish(100);
    return builder->Finish();
}

parquet::EncodedStatistics Encoded(std::string min, std::string max, int64_t nulls = 0)
{
    parquet::EncodedStatistics statistics;
    statistics.set_min(std::move(min));
    statistics.set_max(std::move(max));
    statistics.set_null_count(nulls);
    return statistics;
}

template <class T>
std::string Plain(T value)
{
    std::string bytes(sizeof(T), '\0');
    std::memcpy(bytes.data(), &value, sizeof(T));
    return bytes;
}

bool MayMatchGroup(const Predicate& predicate, const parquet::FileMetaData& metadata, FieldType type)
{
    const auto bound = Bind(predicate);
    const auto group = metadata.RowGroup(0);
    const std::vector<TFieldStats> stats{RowGroupFieldStats(*group, 0, type, true)};
    return MayMatch(bound, stats);
}

TEST(ScanPruningRules, ContradictoryOrMalformedRowGroupStatisticsPruneNothing)
{
    using parquet::schema::PrimitiveNode;
    const auto int64 = PrimitiveNode::Make("value", parquet::Repetition::OPTIONAL, parquet::Type::INT64);
    const auto reversed = GroupWithStatistics(int64, Encoded(Plain<int64_t>(20), Plain<int64_t>(10)), 1);
    EXPECT_FALSE(RowGroupFieldStats(*reversed->RowGroup(0), 0, FieldType::Int64, true).Lower);
    EXPECT_TRUE(MayMatchGroup(Predicate::Compare("value", CompareOp::Eq, int64_t{15}), *reversed, FieldType::Int64));
    EXPECT_TRUE(MayMatchGroup(Predicate::Compare("value", CompareOp::Lt, int64_t{5}), *reversed, FieldType::Int64));
    // Well-ordered statistics of the same shape still prune (control).
    const auto ordered = GroupWithStatistics(int64, Encoded(Plain<int64_t>(10), Plain<int64_t>(20)), 1);
    EXPECT_FALSE(MayMatchGroup(Predicate::Compare("value", CompareOp::Eq, int64_t{25}), *ordered, FieldType::Int64));

    const auto timestamp = PrimitiveNode::Make("at",
        parquet::Repetition::OPTIONAL,
        parquet::LogicalType::Timestamp(true, parquet::LogicalType::TimeUnit::MICROS),
        parquet::Type::INT64);
    const auto times = GroupWithStatistics(timestamp, Encoded(Plain<int64_t>(2000), Plain<int64_t>(1000)), 1);
    EXPECT_TRUE(
        MayMatchGroup(Predicate::Compare("at", CompareOp::Eq, TimestampUs{1500}), *times, FieldType::TimestampUsUtc));

    const auto real = PrimitiveNode::Make("score", parquet::Repetition::OPTIONAL, parquet::Type::DOUBLE);
    const auto doubles = GroupWithStatistics(real, Encoded(Plain(2.0), Plain(1.0)), 1);
    EXPECT_TRUE(MayMatchGroup(Predicate::Compare("score", CompareOp::Eq, 1.5), *doubles, FieldType::Float64));
    const auto infinite =
        GroupWithStatistics(real, Encoded(Plain(0.0), Plain(std::numeric_limits<double>::infinity())), 1);
    EXPECT_TRUE(MayMatchGroup(Predicate::Compare("score", CompareOp::Lt, -1.0), *infinite, FieldType::Float64));

    const auto boolean = PrimitiveNode::Make("flag", parquet::Repetition::OPTIONAL, parquet::Type::BOOLEAN);
    const auto flags = GroupWithStatistics(boolean, Encoded(std::string(1, '\1'), std::string(1, '\0')), 1);
    EXPECT_TRUE(MayMatchGroup(Predicate::Compare("flag", CompareOp::Eq, false), *flags, FieldType::Bool));

    const auto text = PrimitiveNode::Make("key",
        parquet::Repetition::REQUIRED,
        parquet::LogicalType::String(),
        parquet::Type::BYTE_ARRAY);
    const auto strings = GroupWithStatistics(text, Encoded("b", "a"), 1);
    EXPECT_TRUE(
        MayMatchGroup(Predicate::Compare("key", CompareOp::Eq, std::string("ab")), *strings, FieldType::String));
    const auto badUpper = GroupWithStatistics(text, Encoded("a", "\xC3"), 1);
    EXPECT_TRUE(
        MayMatchGroup(Predicate::Compare("key", CompareOp::Eq, std::string("\xC3\xA9")), *badUpper, FieldType::String));
    const auto badLower = GroupWithStatistics(text, Encoded("\xFF", "b"), 1);
    EXPECT_TRUE(
        MayMatchGroup(Predicate::Compare("key", CompareOp::Gt, std::string("c")), *badLower, FieldType::String));
    const auto validStrings = GroupWithStatistics(text, Encoded("a", "b"), 1);
    EXPECT_FALSE(
        MayMatchGroup(Predicate::Compare("key", CompareOp::Gt, std::string("c")), *validStrings, FieldType::String));

    // Bounds in an all-NULL group, and null counts above the row count.
    const auto allNull = GroupWithStatistics(int64, Encoded(Plain<int64_t>(1), Plain<int64_t>(1), 1), 1);
    EXPECT_TRUE(MayMatchGroup(Predicate::IsNotNull("value"), *allNull, FieldType::Int64));
    const auto excessive = GroupWithStatistics(int64, Encoded(Plain<int64_t>(1), Plain<int64_t>(1), 5), 1);
    EXPECT_FALSE(RowGroupFieldStats(*excessive->RowGroup(0), 0, FieldType::Int64, true).Nulls);
    EXPECT_TRUE(MayMatchGroup(Predicate::IsNull("value"), *excessive, FieldType::Int64));
}

} // namespace
} // namespace glamdb::detail
