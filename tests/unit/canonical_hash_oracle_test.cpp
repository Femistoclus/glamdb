// The canonical payload hash must stay bit-identical to the original
// definition: SHA-256 over the whole materialized canonical byte string
// (Encoder), row-major over fields in field-ID order. The oracle below builds
// that string independently of the streaming production paths.

#include "core/api/binary.h"
#include "core/api/canonical.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <bit>
#include <cmath>
#include <limits>
#include <random>

namespace glamdb::detail {
namespace {

constexpr char CollectionID[] = "00112233-4455-6677-8899-aabbccddeeff";

const SchemaSpec AllTypes{{{"key", FieldType::String, false},
    {"s", FieldType::String, true},
    {"b", FieldType::Bool, true},
    {"i", FieldType::Int64, true},
    {"f", FieldType::Float64, true},
    {"t", FieldType::TimestampUsUtc, true},
    {"omitted", FieldType::Int64, true}}};

std::string OracleHash(const SchemaSpec& schema, const std::vector<Row>& rows, size_t* decoded = nullptr)
{
    const auto header = EncodeAppendHeader(schema, CollectionID, rows.size());
    EXPECT_TRUE(header);
    Encoder out;
    out.bytes = *header;
    for (const auto& row : rows) {
        for (size_t field = 0; field < row.size(); ++field) {
            const auto& cell = row[field];
            if (std::holds_alternative<std::monostate>(cell)) {
                out.Number<uint8_t>(0);
                continue;
            }
            out.Number<uint8_t>(1);
            switch (schema.fields[field].type) {
                case FieldType::String:
                    out.Text(std::get<std::string>(cell));
                    break;
                case FieldType::Bool:
                    out.Number<uint8_t>(std::get<bool>(cell) ? 1 : 0);
                    break;
                case FieldType::Int64:
                    out.Number(std::get<int64_t>(cell));
                    break;
                case FieldType::Float64: {
                    const auto value = std::get<double>(cell);
                    out.Number(std::bit_cast<uint64_t>(value == 0 ? 0.0 : value));
                    break;
                }
                case FieldType::TimestampUsUtc:
                    out.Number(std::get<TimestampUs>(cell).value);
                    break;
            }
        }
    }
    if (decoded != nullptr) {
        *decoded = out.bytes.size() - header->size();
    }
    return Sha256(out.bytes);
}

std::shared_ptr<arrow::Array> Finish(arrow::ArrayBuilder& builder)
{
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

// Rows over AllTypes except the trailing omitted field, which stays NULL.
std::shared_ptr<arrow::RecordBatch> ToArrow(const std::vector<Row>& rows)
{
    arrow::StringBuilder key, text;
    arrow::BooleanBuilder flag;
    arrow::Int64Builder integer;
    arrow::DoubleBuilder number;
    arrow::TimestampBuilder time(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow::default_memory_pool());
    for (const auto& row : rows) {
        EXPECT_TRUE(key.Append(std::get<std::string>(row[0])).ok());
        const auto* s = std::get_if<std::string>(&row[1]);
        EXPECT_TRUE((s ? text.Append(*s) : text.AppendNull()).ok());
        const auto* b = std::get_if<bool>(&row[2]);
        EXPECT_TRUE((b ? flag.Append(*b) : flag.AppendNull()).ok());
        const auto* i = std::get_if<int64_t>(&row[3]);
        EXPECT_TRUE((i ? integer.Append(*i) : integer.AppendNull()).ok());
        const auto* f = std::get_if<double>(&row[4]);
        EXPECT_TRUE((f ? number.Append(*f) : number.AppendNull()).ok());
        const auto* t = std::get_if<TimestampUs>(&row[5]);
        EXPECT_TRUE((t ? time.Append(t->value) : time.AppendNull()).ok());
    }
    // Field order differs from the schema: mapping is by name.
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("t", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                                        arrow::field("key", arrow::utf8(), false),
                                        arrow::field("f", arrow::float64()),
                                        arrow::field("s", arrow::utf8()),
                                        arrow::field("b", arrow::boolean()),
                                        arrow::field("i", arrow::int64())}),
        static_cast<int64_t>(rows.size()),
        {Finish(time),
            Finish(key),
            Finish(number),
            Finish(text),
            Finish(flag),
            Finish(integer)});
}

// Every type, NULLs, duplicates, empty and multi-byte strings, int64 and
// timestamp extremes, both zeros, and strings around the hasher's buffer size.
std::vector<Row> Fixture()
{
    std::vector<Row> rows{
        {std::string("dup"), std::string(""), true, int64_t{0}, 0.0, TimestampUs{0}, std::monostate{}},
        {std::string("dup"), std::string(""), true, int64_t{0}, 0.0, TimestampUs{0}, std::monostate{}},
        {std::string(""),
            std::monostate{},
            std::monostate{},
            std::monostate{},
            std::monostate{},
            std::monostate{},
            std::monostate{}},
        {std::string("\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87"),
            std::string("\xF0\x9F\x92\x96"),
            false,
            std::numeric_limits<int64_t>::min(),
            -0.0,
            TimestampUs{-62135596800000000LL},
            std::monostate{}},
        {std::string("max"),
            std::string("x"),
            false,
            std::numeric_limits<int64_t>::max(),
            std::numeric_limits<double>::max(),
            TimestampUs{253402300799999999LL},
            std::monostate{}},
        {std::string("tiny"),
            std::string("y"),
            true,
            int64_t{-1},
            std::numeric_limits<double>::denorm_min(),
            TimestampUs{-1},
            std::monostate{}},
    };
    std::mt19937_64 random(7);
    for (const size_t length : {4088, 4094, 4095, 4096, 4097, 4103, 8192, 70001}) {
        rows.push_back({std::string(std::min<size_t>(length, 1024) / 4, 'k'),
            std::string(length, static_cast<char>('a' + length % 26)),
            length % 2 == 0,
            static_cast<int64_t>(random()),
            std::bit_cast<double>(random() & 0x7fefffffffffffffULL),
            TimestampUs{static_cast<int64_t>(random() % 253402300799999999ULL)},
            std::monostate{}});
    }
    for (int index = 0; index < 600; ++index) {
        const auto value = random();
        rows.push_back({std::string("row-") + std::to_string(index),
            value % 7 == 0 ? Cell(std::monostate{}) : Cell(std::string(value % 37, static_cast<char>('A' + value % 26))),
            value % 5 == 0 ? Cell(std::monostate{}) : Cell(value % 2 == 0),
            value % 3 == 0 ? Cell(std::monostate{}) : Cell(static_cast<int64_t>(value)),
            value % 11 == 0 ? Cell(std::monostate{}) : Cell(static_cast<double>(value % 100000) / 7 - 5000),
            value % 13 == 0 ? Cell(std::monostate{}) : Cell(TimestampUs{static_cast<int64_t>(value % 100000000)}),
            std::monostate{}});
    }
    return rows;
}

TEST(CanonicalHashOracle, HasherMatchesTheMaterializedEncoding)
{
    std::mt19937_64 random(11);
    for (int round = 0; round < 50; ++round) {
        Encoder encoder;
        CanonicalHasher hasher;
        for (int step = 0; step < 400; ++step) {
            switch (random() % 6) {
                case 0: {
                    const auto value = static_cast<uint8_t>(random());
                    encoder.Number(value);
                    hasher.Number(value);
                    break;
                }
                case 1: {
                    const auto value = static_cast<uint16_t>(random());
                    encoder.Number(value);
                    hasher.Number(value);
                    break;
                }
                case 2: {
                    const auto value = static_cast<uint32_t>(random());
                    encoder.Number(value);
                    hasher.Number(value);
                    break;
                }
                case 3: {
                    const auto value = static_cast<int64_t>(random());
                    encoder.Number(value);
                    hasher.Number(value);
                    break;
                }
                case 4: {
                    const std::string text(random() % (round % 2 == 0 ? 64 : 9000), static_cast<char>(random()));
                    encoder.Text(text);
                    hasher.Text(text);
                    break;
                }
                default: {
                    const std::string bytes(random() % 4200, static_cast<char>(random()));
                    encoder.bytes += bytes;
                    hasher.Bytes(bytes);
                    break;
                }
            }
            ASSERT_EQ(hasher.Size(), encoder.bytes.size());
        }
        EXPECT_EQ(hasher.Digest(), Sha256(encoder.bytes));
    }
    EXPECT_EQ(CanonicalHasher().Digest(), Sha256(""));
}

TEST(CanonicalHashOracle, AllInputPathsMatchTheOracle)
{
    const auto rows = Fixture();
    size_t decoded = 0;
    const auto expected = OracleHash(AllTypes, rows, &decoded);

    const auto fromRows = NormalizeRows(AllTypes, CollectionID, rows);
    ASSERT_TRUE(fromRows) << fromRows.error().message;
    EXPECT_EQ(fromRows->payload_hash, expected);
    EXPECT_EQ(fromRows->decoded_bytes, decoded);

    const auto batch = ToArrow(rows);
    const auto rowCount = batch->num_rows();
    const std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> inputs{
        {batch},
        {batch->Slice(0, 1), batch->Slice(1, 9), batch->Slice(10, rowCount - 10)},
        {batch->Slice(0, rowCount / 2), batch->Slice(rowCount / 2)},
    };
    for (const auto& batches : inputs) {
        SCOPED_TRACE(batches.size());
        const auto fingerprint = FingerprintBatches(AllTypes, CollectionID, batches);
        ASSERT_TRUE(fingerprint) << fingerprint.error().message;
        EXPECT_EQ(*fingerprint, expected);
        const auto normalized = NormalizeBatches(AllTypes, CollectionID, batches);
        ASSERT_TRUE(normalized) << normalized.error().message;
        EXPECT_EQ(normalized->payload_hash, expected);
        EXPECT_EQ(normalized->decoded_bytes, decoded);
        // -0.0 is stored as +0.0; everything else as given.
        auto stored = rows;
        for (auto& row : stored) {
            if (auto* value = std::get_if<double>(&row[4]); value != nullptr && *value == 0) {
                *value = 0.0;
            }
        }
        ASSERT_EQ(normalized->rows.size(), stored.size());
        for (size_t row = 0; row < stored.size(); ++row) {
            ASSERT_EQ(normalized->rows[row], stored[row]) << row;
            if (const auto* value = std::get_if<double>(&normalized->rows[row][4]); value != nullptr && *value == 0) {
                EXPECT_FALSE(std::signbit(*value)) << row;
            }
        }
    }
}

// NaN and infinities are rejected as before on every path.
TEST(CanonicalHashOracle, NonFiniteFloatsStillReject)
{
    for (const double value : {std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()}) {
        auto rows = Fixture();
        rows[5][4] = value;
        EXPECT_FALSE(NormalizeRows(AllTypes, CollectionID, rows));
        const auto batch = ToArrow(rows);
        EXPECT_FALSE(FingerprintBatches(AllTypes, CollectionID, {batch}));
        EXPECT_FALSE(NormalizeBatches(AllTypes, CollectionID, {batch}));
    }
}

} // namespace
} // namespace glamdb::detail
