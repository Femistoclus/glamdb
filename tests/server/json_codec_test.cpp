// Differential tests: the direct row encoder and the streaming append-body
// parser must match the previous DOM codecs byte for byte and error for error.
#include "apps/transport/codec.h"

#include <cfloat>
#include <cmath>
#include <gtest/gtest.h>
#include <random>

namespace glamdb::NTransport {
namespace {
using TBatch = std::shared_ptr<arrow::RecordBatch>;

// ---------------------------------------------------------------- encoder --

struct TEncoded
{
    size_t Count = 0;
    std::string Output{};
    std::string Error{};
    bool operator==(const TEncoded&) const = default;
};

std::ostream& operator<<(std::ostream& stream, const TEncoded& value)
{
    return stream << "count=" << value.Count << " error=" << value.Error << " output=" << value.Output;
}

template <class F>
TEncoded Encode(F encode, const arrow::RecordBatch& batch, std::string prefix, const TRowJsonBudget& budget)
{
    TEncoded result{.Output = std::move(prefix)};
    try {
        result.Count = encode(batch, result.Output, budget);
    } catch (const TFailure& error) {
        result.Error = error.Detail.code + ": " + error.Detail.message;
        result.Output.clear();
    } catch (const TJson::exception& error) {
        result.Error = std::string("json: ") + error.what();
        result.Output.clear();
    }
    return result;
}

void ExpectSameEncoding(const arrow::RecordBatch& batch)
{
    const auto all = TRowJsonBudget{.MaxRows = 100000, .TargetBytes = MaxResponseBytes, .HardBytes = MaxResponseBytes};
    const auto reference = Encode(AppendRowsJsonReference, batch, "[", all);
    ASSERT_EQ(Encode(AppendRowsJson, batch, "[", all), reference);
    // Soft target, hard limit and row bound around every row boundary.
    std::vector<size_t> bytes{0, 1, 2};
    if (reference.Error.empty()) {
        for (size_t i = 1; i < reference.Output.size(); ++i) {
            if (reference.Output[i] == ',') {
                bytes.insert(bytes.end(), {i - 1, i, i + 1});
            }
        }
        bytes.insert(bytes.end(), {reference.Output.size(), reference.Output.size() + 1});
    }
    for (const auto limit : bytes) {
        for (const auto* prefix : {"[", "[{}"}) {
            const TRowJsonBudget soft{.MaxRows = 100000, .TargetBytes = limit, .HardBytes = MaxResponseBytes};
            ASSERT_EQ(Encode(AppendRowsJson, batch, prefix, soft), Encode(AppendRowsJsonReference, batch, prefix, soft))
                << "target " << limit << " prefix " << prefix;
            const TRowJsonBudget hard{.MaxRows = 100000, .TargetBytes = limit, .HardBytes = limit};
            ASSERT_EQ(Encode(AppendRowsJson, batch, prefix, hard), Encode(AppendRowsJsonReference, batch, prefix, hard))
                << "hard " << limit << " prefix " << prefix;
        }
    }
    for (size_t rows = 0; rows <= 3; ++rows) {
        const TRowJsonBudget bounded{.MaxRows = rows, .TargetBytes = MaxResponseBytes, .HardBytes = MaxResponseBytes};
        ASSERT_EQ(Encode(AppendRowsJson, batch, "[", bounded), Encode(AppendRowsJsonReference, batch, "[", bounded));
    }
}

void ExpectSameEncodingOfSlices(const TBatch& batch)
{
    ExpectSameEncoding(*batch);
    for (int64_t offset = 0; offset <= batch->num_rows(); offset += std::max<int64_t>(1, batch->num_rows() / 7)) {
        for (const int64_t length : {int64_t{0}, int64_t{1}, int64_t{3}, batch->num_rows() - offset}) {
            if (offset + length <= batch->num_rows()) {
                ExpectSameEncoding(*batch->Slice(offset, length));
            }
        }
    }
}

template <class TBuilder, class T>
std::shared_ptr<arrow::Array> Column(std::shared_ptr<arrow::DataType> type, const std::vector<std::optional<T>>& values)
{
    TBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        if (value) {
            EXPECT_TRUE(builder.Append(*value).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::Array> Strings(const std::vector<std::optional<std::string>>& values)
{
    return Column<arrow::StringBuilder>(arrow::utf8(), values);
}
std::shared_ptr<arrow::Array> Int64s(const std::vector<std::optional<int64_t>>& values)
{
    return Column<arrow::Int64Builder>(arrow::int64(), values);
}
std::shared_ptr<arrow::Array> Doubles(const std::vector<std::optional<double>>& values)
{
    return Column<arrow::DoubleBuilder>(arrow::float64(), values);
}
std::shared_ptr<arrow::Array> Bools(const std::vector<std::optional<bool>>& values)
{
    return Column<arrow::BooleanBuilder>(arrow::boolean(), values);
}
std::shared_ptr<arrow::Array> Timestamps(const std::vector<std::optional<int64_t>>& values,
    std::shared_ptr<arrow::DataType> type = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))
{
    return Column<arrow::TimestampBuilder>(std::move(type), values);
}

TBatch MakeBatch(const std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>& columns, int64_t rows = -1)
{
    arrow::FieldVector fields;
    arrow::ArrayVector arrays;
    for (const auto& [name, array] : columns) {
        fields.push_back(arrow::field(name, array->type()));
        arrays.push_back(array);
        rows = array->length();
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), std::max<int64_t>(rows, 0), arrays);
}

std::vector<std::string> SpecialStrings()
{
    std::vector<std::string> values{"",
        "plain",
        "quote\"back\\slash/solidus",
        "\x7f",
        "caf\xc3\xa9",
        "\xe2\x82\xac",
        "\xef\xbf\xbf",
        "\xf0\x9f\x98\x80",
        "\xf4\x8f\xbf\xbf",
        "\xed\x9f\xbf",
        "\xee\x80\x80",
        std::string(1000, 'x') + "\n"};
    std::string controls;
    for (int c = 0; c < 0x20; ++c) {
        controls += static_cast<char>(c);
        values.push_back(std::string(1, static_cast<char>(c)) + "z");
    }
    values.push_back(controls);
    return values;
}

TEST(JsonRowEncoder, MatchesReferenceForEveryTypeAndExtreme)
{
    const auto special = SpecialStrings();
    std::vector<std::optional<std::string>> strings(special.begin(), special.end());
    const auto rows = strings.size() + 1;
    strings.push_back(std::nullopt);
    std::vector<std::optional<int64_t>>
        ints{std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max(), 0, -1, 1, std::nullopt, 1000000};
    std::vector<std::optional<double>> doubles{0.0,
        -0.0,
        1.0,
        -1.0,
        0.1,
        1.25,
        1e15,
        1e16,
        123456789012345678.0,
        1e-4,
        1e-5,
        1.5e-7,
        5e-324,
        DBL_MIN,
        DBL_MAX,
        -DBL_MAX,
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
        std::nullopt,
        1.0 / 3.0,
        9007199254740993.0,
        100.0,
        1e21,
        1e22,
        123.456e-300};
    std::vector<std::optional<int64_t>> timestamps{0,
        -1,
        1,
        -62135596800000000LL,
        253402300799999999LL,
        -62167219200000000LL,
        -62167219200000001LL,
        253402300800000000LL,
        // Far outside 0001..9999 but within std::chrono's overflow-free range,
        // which the reference formatter needs.
        -9000000000000000000LL,
        9000000000000000000LL,
        951782400000000LL,
        1767225600123456LL,
        std::nullopt};
    std::vector<std::optional<bool>> bools{true, false, std::nullopt};
    const auto cycle = [&]<class T>(const std::vector<std::optional<T>>& values) {
        std::vector<std::optional<T>> result;
        for (size_t i = 0; i < rows; ++i) {
            result.push_back(values[i % values.size()]);
        }
        return result;
    };
    std::vector<std::optional<double>> allDoubles = doubles;
    std::vector<std::optional<int64_t>> allTimestamps = timestamps;
    const auto batch = MakeBatch({{"zeta", Strings(strings)},
        {"Alpha", Int64s(cycle(ints))},
        {"b\"q\\\n\x01", Doubles(cycle(doubles))},
        {"\xc3\xa9t\xc3\xa9", Bools(cycle(bools))},
        {"", Timestamps(cycle(timestamps))},
        {"alpha", Strings(cycle(strings))}});
    ExpectSameEncodingOfSlices(batch);
    ExpectSameEncodingOfSlices(MakeBatch({{"d", Doubles(allDoubles)}}));
    ExpectSameEncodingOfSlices(MakeBatch({{"t", Timestamps(allTimestamps)}}));
    // Other timestamp units are encoded as microseconds, exactly like before.
    ExpectSameEncodingOfSlices(MakeBatch({{"t", Timestamps(allTimestamps, arrow::timestamp(arrow::TimeUnit::NANO))}}));
}

TEST(JsonRowEncoder, MatchesReferenceForRandomData)
{
    std::mt19937_64 random(20261112);
    for (int round = 0; round < 40; ++round) {
        const size_t rows = random() % 50;
        std::vector<std::optional<std::string>> strings;
        std::vector<std::optional<int64_t>> ints, timestamps;
        std::vector<std::optional<double>> doubles;
        std::vector<std::optional<bool>> bools;
        const auto nullable = [&] {
            return random() % 5 == 0;
        };
        for (size_t i = 0; i < rows; ++i) {
            std::string text;
            for (size_t n = random() % 12; n > 0; --n) {
                // Mostly ASCII, sometimes control and multibyte characters.
                const auto pick = random() % 10;
                if (pick < 7) {
                    text += static_cast<char>(random() % 0x80);
                } else if (pick < 9) {
                    text += "\xd0\xb0";
                } else {
                    text += "\xf0\x9f\x8e\x89";
                }
            }
            strings.push_back(nullable() ? std::nullopt : std::optional(text));
            ints.push_back(nullable() ? std::nullopt : std::optional(static_cast<int64_t>(random())));
            timestamps.push_back(nullable()
                    ? std::nullopt
                    : std::optional(static_cast<int64_t>(random() % 315537897600000000ULL) - 62135596800000000LL));
            const auto bits = random();
            double number = std::bit_cast<double>(bits);
            if (random() % 2 == 0) {
                number = static_cast<double>(static_cast<int64_t>(bits % 2000000) - 1000000) / 1000.0;
            }
            doubles.push_back(nullable() ? std::nullopt : std::optional(number));
            bools.push_back(nullable() ? std::nullopt : std::optional(random() % 2 == 0));
        }
        ExpectSameEncodingOfSlices(MakeBatch({{"s", Strings(strings)},
            {"i", Int64s(ints)},
            {"t", Timestamps(timestamps)},
            {"d", Doubles(doubles)},
            {"b", Bools(bools)}}));
    }
}

TEST(JsonRowEncoder, MatchesReferenceForEmptyDuplicateAndUnsupportedShapes)
{
    ExpectSameEncoding(*MakeBatch({{"s", Strings({})}}));
    ExpectSameEncoding(*MakeBatch({}, 3));
    ExpectSameEncoding(*MakeBatch({}, 0));
    // A repeated name keeps the last column, at the first name's sorted position.
    ExpectSameEncodingOfSlices(MakeBatch({{"b", Strings({"x", std::nullopt, "z"})},
        {"a", Int64s({1, 2, 3})},
        {"b", Doubles({1.5, 2.5, std::nullopt})}}));
    ExpectSameEncodingOfSlices(MakeBatch({{"b", Int64s({1, 2, 3})}, {"b", Strings({"x", "y", "z"})}}));
    // Unsupported types encode NULL cells and reject values.
    const auto int32 =
        Column<arrow::Int32Builder>(arrow::int32(), std::vector<std::optional<int32_t>>{std::nullopt, 1});
    ExpectSameEncodingOfSlices(MakeBatch({{"a", Strings({"x", "y"})}, {"n", int32}}));
}

TEST(JsonRowEncoder, InvalidUtf8RaisesTheSerializerError)
{
    for (const std::string invalid : {"\x80",
             "a\xc0\xaf",
             "\xc3",
             "\xe0\x80\x80",
             "\xed\xa0\x80",
             "\xf4\x90\x80\x80",
             "\xf5\x80\x80\x80",
             "\xff",
             "ok\xe2\x82",
             "\xf0\x9f\x98"})
    {
        SCOPED_TRACE(invalid);
        ExpectSameEncodingOfSlices(MakeBatch({{"s", Strings({"fine", invalid, "after"})}}));
        ExpectSameEncodingOfSlices(MakeBatch({{invalid, Strings({"fine"})}}));
        const auto encoded = Encode(AppendRowsJson,
            *MakeBatch({{"s", Strings({invalid})}}),
            "[",
            {.MaxRows = 1, .TargetBytes = MaxResponseBytes, .HardBytes = MaxResponseBytes});
        EXPECT_TRUE(encoded.Error.starts_with("json: ")) << encoded;
    }
}

// ----------------------------------------------------------------- parser --

struct TParsed
{
    std::string Error{};
    TJson Body{};
    TBatch Batch{};
};

void ExpectEqualParse(const TParsed& expected, const TParsed& actual, const std::string& body)
{
    ASSERT_EQ(actual.Error, expected.Error) << body.substr(0, 400);
    if (!expected.Error.empty()) {
        return;
    }
    EXPECT_EQ(actual.Body, expected.Body) << body.substr(0, 400);
    ASSERT_TRUE(actual.Batch && expected.Batch);
    EXPECT_TRUE(actual.Batch->schema()->Equals(*expected.Batch->schema(), true));
    EXPECT_TRUE(actual.Batch->Equals(*expected.Batch, true)) << body.substr(0, 400);
    ASSERT_TRUE(actual.Batch->ValidateFull().ok());
}

// The append route's sequence of checks, over either body representation.
template <class F>
TParsed RunAppend(F parse, std::string_view bytes)
{
    TParsed result;
    try {
        result.Body = parse(result.Batch, bytes);
    } catch (const TFailure& error) {
        result.Error = error.Detail.code + ": " + error.Detail.message;
    } catch (const TJson::exception&) {
        result.Error = "json exception";
    }
    return result;
}

void Prelude(const TJson& body)
{
    Require(body.is_object(), "request body must be an object");
    CheckKeys(body, {"operation_id", "rows", "expected_snapshot"});
    (void)body.at("operation_id").get<std::string>();
    if (body.contains("expected_snapshot") && !body["expected_snapshot"].is_null()) {
        (void)ParseInt64(body["expected_snapshot"]);
    }
}

TParsed ParseOld(const SchemaSpec& schema, std::string_view bytes)
{
    return RunAppend(
        [&](TBatch& batch, std::string_view input) {
            auto body = ParseJson(input);
            Prelude(body);
            batch = ParseRows(schema, body.at("rows"));
            body["rows"] = nullptr;
            return body;
        },
        bytes);
}

TParsed ParseNew(const SchemaSpec& schema, std::string_view bytes)
{
    return RunAppend(
        [&](TBatch& batch, std::string_view input) {
            TJsonRows rows;
            auto body = ParseJson(input, rows);
            Prelude(body);
            const auto& placeholder = body.at("rows");
            EXPECT_TRUE(placeholder.is_null());
            EXPECT_TRUE(rows.Present);
            batch = ParseRows(schema, rows);
            return body;
        },
        bytes);
}

void ExpectSameParse(const SchemaSpec& schema, const std::string& body)
{
    ExpectEqualParse(ParseOld(schema, body), ParseNew(schema, body), body);
}

const SchemaSpec& AllTypes()
{
    static const SchemaSpec schema{{{"key", FieldType::String, false},
        {"flag", FieldType::Bool, true},
        {"n", FieldType::Int64, true},
        {"f", FieldType::Float64, true},
        {"ts", FieldType::TimestampUsUtc, true},
        {"s", FieldType::String, true},
        {"req_n", FieldType::Int64, false}}};
    return schema;
}

std::string Body(const std::string& rows, const std::string& extra = R"("operation_id":"op")")
{
    return "{" + extra + (extra.empty() ? "" : ",") + R"("rows":)" + rows + "}";
}

TEST(JsonAppendParser, MatchesDomPathForHandWrittenBodies)
{
    const auto& schema = AllTypes();
    const std::string ok = R"({"key":"k","req_n":"1"})";
    std::vector<std::string> rows{
        "[" + ok + "]",
        R"([{"key":"k","req_n":"-9223372036854775808","n":"9223372036854775807","f":-0.0,"flag":false,"s":null,
            "ts":"2026-01-01T01:30:00.123+01:30"},{"req_n":"007","key":"k😀","f":1e308,"ts":null}])",
        R"([{"key":"k","req_n":"1","f":9007199254740993},{"key":"k","req_n":"1","f":18446744073709551615},
            {"key":"k","req_n":"1","f":-9223372036854775808},{"key":"k","req_n":"1","f":1E-320}])",
        // Shape errors.
        "[]",
        "{}",
        R"({"key":"k"})",
        "null",
        "\"rows\"",
        "1",
        "[1]",
        "[[]]",
        "[" + ok + ",null]",
        "[" + ok + ",\"x\"]",
        // Field errors, in schema order.
        R"([{"key":"k"}])",
        R"([{"req_n":"1"}])",
        R"([{"key":null,"req_n":"1"}])",
        R"([{"key":"k","req_n":"1","extra":1}])",
        R"([{"key":"k","req_n":"1","extra":null}])",
        R"([{"key":"k","req_n":"1","extra":{"a":[1,{"b":2}]}}])",
        R"([{"key":1,"req_n":"x"}])",
        R"([{"key":"k","req_n":1}])",
        R"([{"key":"k","req_n":"+1"}])",
        R"([{"key":"k","req_n":""}])",
        R"([{"key":"k","req_n":" 1"}])",
        R"([{"key":"k","req_n":"9223372036854775808"}])",
        R"([{"key":"k","req_n":"1","flag":1}])",
        R"([{"key":"k","req_n":"1","flag":"true"}])",
        R"([{"key":"k","req_n":"1","f":"1.0"}])",
        R"([{"key":"k","req_n":"1","f":true}])",
        R"([{"key":"k","req_n":"1","f":[1]}])",
        R"([{"key":"k","req_n":"1","s":{"x":1}}])",
        R"([{"key":["k"],"req_n":"1"}])",
        R"([{"key":"k","req_n":"1","ts":1}])",
        R"([{"key":"k","req_n":"1","ts":"0000-01-01T00:00:00Z"}])",
        R"([{"key":"k","req_n":"1","ts":"2026-02-30T00:00:00Z"}])",
        R"([{"key":"k","req_n":"1","ts":"2026-01-01T00:00:00.0000001Z"}])",
        R"([{"key":"k","req_n":"1","ts":"0001-01-01T00:00:00+01:00"}])",
        R"([{"key":"k","req_n":"1","ts":"9999-12-31T23:59:59.999999-00:01"}])",
        R"([{"key":"k","req_n":"1","ts":"2026-01-01 00:00:00Z"}])",
        R"([{"key":"k","req_n":"1","ts":"2026-01-01T00:00:00"}])",
        // Duplicate keys, including decoded escapes and nested objects.
        R"([{"key":"k","req_n":"1","key":"j"}])",
        R"([{"key":"k","req_n":"1","key":"j"}])",
        R"([{"key":"k","req_n":"1"},{"req_n":"1","key":"k","req_n":"2"}])",
        R"([{"key":"k","req_n":"1","extra":{"a":1,"a":2}}])",
        R"([{"key":"k","req_n":"1","extra":{"key":1,"req_n":2}}])",
        R"([{"key":"k","req_n":"1","x":[{"y":1},{"y":2}]}])",
        R"({"a":1,"a":2})",
        R"([{"key":"k","req_n":"1"},{"key":"k","req_n":"1","extra":[{"a":1,"a":2}]}])",
        // Invalid JSON inside rows.
        R"([{"key":"k","req_n":"1",}])",
        R"([{"key":"k","req_n":"1"},])",
        R"([{"key":"k","req_n":"1","f":1e400}])",
        R"([{"key":"k","req_n":"1","f":-1e400}])",
        R"([{"key":"k","req_n":"1","f":01}])",
        R"([{"key":"k","req_n":"1","f":NaN}])",
        R"([{"key":"\x","req_n":"1"}])",
        R"([{"key":"\ud800","req_n":"1"}])",
        "[{\"key\":\"\x01\",\"req_n\":\"1\"}]",
        "[{\"key\":\"\xff\",\"req_n\":\"1\"}]",
        "[{\"key\":\"k\",\"req_n\":\"1\"}",
    };
    for (const auto& value : rows) {
        SCOPED_TRACE(value);
        ExpectSameParse(schema, Body(value));
        ExpectSameParse(schema, Body(value, R"("expected_snapshot":"12","operation_id":"op")"));
    }
    // Top-level members around rows keep their strictness and order of checks.
    for (const auto& body : {Body("[" + ok + "]", ""),
             Body("[" + ok + "]", R"("operation_id":1)"),
             Body("[" + ok + "]", R"("operation_id":"op","expected_snapshot":null)"),
             Body("[" + ok + "]", R"("operation_id":"op","expected_snapshot":12)"),
             Body("[" + ok + "]", R"("operation_id":"op","extra":1)"),
             Body("[" + ok + "]", R"("operation_id":"op","operation_id":"op")"),
             Body("[" + ok + "]", R"("operation_id":"op","rows":[])"),
             "{\"operation_id\":\"op\",\"rows\":[" + ok + "],\"rows\":[" + ok + "]}",
             "{\"operation_id\":\"op\",\"rows\":[" + ok + "],\"r\\u006fws\":1}",
             "{\"operation_id\":\"op\",\"nested\":{\"rows\":[1]},\"rows\":[" + ok + "]}",
             std::string(R"({"operation_id":"op"})"),
             std::string("\xef\xbb\xbf") + Body("[" + ok + "]"),
             Body("[" + ok + "]") + " ",
             Body("[" + ok + "]") + " x",
             std::string("[") + Body("[" + ok + "]") + "]",
             std::string(R"("rows")"),
             std::string(""),
             std::string("{")})
    {
        ExpectSameParse(schema, body);
    }
}

TEST(JsonNativeScalars, DecimalAndDateSchemaAndExactRows)
{
    const auto wireSchema = ParseJson(R"({"fields":[{"name":"key","type":"string"},
        {"name":"amount","type":"decimal","precision":38,"scale":4,"nullable":true},
        {"name":"day","type":"date","nullable":true}]})");
    const auto schema = ParseSchema(wireSchema);
    const auto body = Body(R"([{"key":"a","amount":"9999999999999999999999999999999999.9999","day":"0001-01-01"},
        {"key":"b","amount":"-000123.450000","day":"2000-02-29"},
        {"key":"c","amount":"-00.00000","day":"9999-12-31"},{"key":"d"}])");
    ExpectSameParse(schema, body);
    const auto parsed = ParseNew(schema, body);
    ASSERT_TRUE(parsed.Error.empty()) << parsed.Error;
    ASSERT_TRUE(parsed.Batch);
    EXPECT_TRUE(parsed.Batch->column(1)->type()->Equals(arrow::decimal128(38, 4)));
    EXPECT_TRUE(parsed.Batch->column(2)->type()->Equals(arrow::date32()));
    EXPECT_EQ(EncodeSchema(*parsed.Batch->schema())["fields"][1]["precision"], 38);
    EXPECT_EQ(EncodeSchema(*parsed.Batch->schema())["fields"][1]["scale"], 4);
    const auto encoded = EncodeRows(*parsed.Batch);
    EXPECT_EQ(encoded[0]["amount"], "9999999999999999999999999999999999.9999");
    EXPECT_EQ(encoded[1]["amount"], "-123.4500");
    EXPECT_EQ(encoded[2]["amount"], "0.0000");
    EXPECT_EQ(encoded[3]["amount"], nullptr);
    EXPECT_EQ(encoded[0]["day"], "0001-01-01");
    EXPECT_EQ(encoded[2]["day"], "9999-12-31");
    ExpectSameEncodingOfSlices(parsed.Batch);
}

TEST(JsonNativeScalars, RejectsInvalidParametersAndInexactValues)
{
    for (const auto& field : {R"({"name":"n","type":"decimal"})",
             R"({"name":"n","type":"decimal","precision":0,"scale":0})",
             R"({"name":"n","type":"decimal","precision":39,"scale":0})",
             R"({"name":"n","type":"decimal","precision":8,"scale":9})",
             R"({"name":"n","type":"decimal","precision":8.0,"scale":2})",
             R"({"name":"n","type":"date","precision":8,"scale":2})"})
    {
        EXPECT_THROW(ParseSchema(ParseJson(std::string("{\"fields\":[") + field + "]}")), TFailure);
    }
    const auto schema = ParseSchema(ParseJson(R"({"fields":[{"name":"key","type":"string"},
        {"name":"n","type":"decimal","precision":8,"scale":2},{"name":"d","type":"date"}]})"));
    for (const TJson& value : {TJson("+1"),
             TJson("1e2"),
             TJson(" 1"),
             TJson("1."),
             TJson(".1"),
             TJson("1.001"),
             TJson("1000000"),
             TJson(std::string(129, '0')),
             TJson(1.5)})
    {
        const auto body = Body(TJson::array({{{"key", "k"}, {"n", value}, {"d", "2026-11-12"}}}).dump());
        ExpectSameParse(schema, body);
        EXPECT_FALSE(ParseNew(schema, body).Error.empty());
    }
    for (const auto* value : {"0000-01-01", "1900-02-29", "2026-04-31", "2026-11-12T00:00:00Z", "2026-1-01"}) {
        const auto body = Body(TJson::array({{{"key", "k"}, {"n", "1"}, {"d", value}}}).dump());
        ExpectSameParse(schema, body);
        EXPECT_FALSE(ParseNew(schema, body).Error.empty());
    }
    EXPECT_NO_THROW(ParseScan("c", schema, ParseJson(R"({"predicate":{"op":"eq","field":"n",
        "value":{"type":"decimal","precision":8,"scale":2,"value":"1.00"}}})")));
    EXPECT_NO_THROW(ParseScan("c", schema, ParseJson(R"({"predicate":{"op":"eq","field":"d",
        "value":{"type":"date","value":null}}})")));
    EXPECT_THROW(ParseScan("c", schema, ParseJson(R"({"predicate":{"op":"eq","field":"n",
        "value":{"type":"decimal","precision":7,"scale":2,"value":"1.00"}}})")),
        TFailure);
    EXPECT_THROW(ParseScan("c", schema, ParseJson(R"({"predicate":{"op":"eq","field":"d",
        "value":"2026-11-12"}})")),
        TFailure);
}

TEST(JsonAppendParser, MaximumWidthFitsTheFiniteJsonEventBudget)
{
    SchemaSpec schema;
    TJson row = TJson::object();
    for (size_t index = 0; index < 128; ++index) {
        const auto name = "c" + std::to_string(index);
        schema.fields.push_back({name, FieldType::Bool, false});
        row[name] = true;
    }
    const auto one = row.dump();
    std::string input = "[";
    for (size_t index = 0; index < 10000; ++index) {
        if (index != 0) {
            input += ',';
        }
        input += one;
    }
    input += ']';
    const auto body = Body(input);
    ASSERT_LE(body.size(), MaxRequestBytes);
    TJsonRows rows;
    EXPECT_NO_THROW(ParseJson(body, rows));
    const auto batch = ParseRows(schema, rows);
    ASSERT_EQ(batch->num_rows(), 10000);
    ASSERT_EQ(batch->num_columns(), 128);
    EXPECT_TRUE(std::static_pointer_cast<arrow::BooleanArray>(batch->column(127))->Value(9999));
}

TEST(JsonAppendParser, MatchesDomPathAtRowAndComplexityLimits)
{
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"x", FieldType::Int64, true}}};
    const auto rows = [](size_t count, const std::string& row) {
        std::string result = "[";
        for (size_t i = 0; i < count; ++i) {
            result += (i == 0 ? "" : ",") + row;
        }
        return result + "]";
    };
    ExpectSameParse(schema, Body(rows(10000, R"({"key":"k"})")));
    ExpectSameParse(schema, Body(rows(10001, R"({"key":"k"})")));
    ExpectSameParse(schema, Body(rows(10001, "1")));
    // Nesting inside a row value: the depth limit counts the enclosing frames.
    for (size_t depth = 155; depth <= 160; ++depth) {
        const auto nested = std::string(depth, '[') + "0" + std::string(depth, ']');
        ExpectSameParse(schema, Body(R"([{"key":"k","deep":)" + nested + "}]"));
        ExpectSameParse(schema, Body(R"([{"key":"k","x":)" + nested + "}]"));
        const auto empty = std::string(depth, '[') + std::string(depth, ']');
        ExpectSameParse(schema, Body(R"([{"key":"k","deep":)" + empty + "}]"));
    }
    // The event limit is shared by rows and the other members.
    const std::string row = R"({"key":"k","x":"1","a":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22]})";
    const auto many = rows(9999, row);
    for (const auto* extra : {R"("operation_id":"op")", R"("operation_id":"op","expected_snapshot":"1")"}) {
        ExpectSameParse(schema, Body(many, extra));
    }
    std::string flood = "[";
    for (size_t i = 0; i < MaxJsonEvents; ++i) {
        flood += i == 0 ? "0" : ",0";
    }
    ExpectSameParse(schema, Body(flood + "]"));
    for (size_t trim : {size_t{6}, size_t{7}, size_t{8}}) {
        ExpectSameParse(schema, Body(flood.substr(0, flood.size() - 2 * trim) + "]"));
    }
    ExpectSameParse(schema, std::string(MaxRequestBytes + 1, ' '));
}

std::string RandomValue(std::mt19937_64& random, FieldType type, bool valid)
{
    const auto pick = random() % 100;
    if (!valid && pick < 30) {
        const std::vector<std::string> wrong{"1",
            "-0.0",
            "true",
            "\"x\"",
            "[]",
            "{}",
            "[1,{\"a\":null}]",
            "\"\"",
            "\"9223372036854775808\"",
            "1e999",
            "\"2026-13-01T00:00:00Z\"",
            "18446744073709551616",
            "-1.5e-310"};
        return wrong[random() % wrong.size()];
    }
    if (pick < 8) {
        return "null";
    }
    switch (type) {
        case FieldType::String: {
            std::string text = "\"";
            for (size_t n = random() % 8; n > 0; --n) {
                const std::vector<std::string> parts{"a", "\\n", "\\\"", "\\u00e9", "\\ud83d\\ude00", "\xd0\xb0", "/"};
                text += parts[random() % parts.size()];
            }
            return text + "\"";
        }
        case FieldType::Bool:
            return random() % 2 ? "true" : "false";
        case FieldType::Int64:
            return "\"" + std::to_string(static_cast<int64_t>(random())) + "\"";
        case FieldType::Float64: {
            const std::vector<std::string> numbers{"0",
                "-0",
                "1.5",
                "-2e10",
                "123456789012345678901",
                "-9223372036854775809",
                "4.9e-324",
                "1.7976931348623157e308",
                "0.1",
                "3"};
            return numbers[random() % numbers.size()];
        }
        case FieldType::TimestampUsUtc: {
            const std::vector<std::string> times{"\"2026-01-01T00:00:00Z\"",
                "\"0001-01-01T00:00:00.000001Z\"",
                "\"9999-12-31T23:59:59.999999Z\"",
                "\"2024-02-29T12:30:00.5+05:30\"",
                "\"2026-06-01T00:00:00-12:00\""};
            return times[random() % times.size()];
        }
        case FieldType::Decimal:
            return "\"" + std::to_string(random() % 10000) + ".00\"";
        case FieldType::Date:
            return "\"2000-02-" + std::to_string(10 + random() % 20) + "\"";
    }
    return "null";
}

TEST(JsonAppendParser, MatchesDomPathForRandomizedBodies)
{
    std::mt19937_64 random(1109);
    const std::vector<std::string> types{"string", "bool", "int64", "float64", "timestamp_us_utc"};
    for (int round = 0; round < 400; ++round) {
        SchemaSpec schema;
        const auto width = 1 + random() % 12;
        for (size_t i = 0; i < width; ++i) {
            schema.fields.push_back({"f" + std::to_string(i) + (random() % 4 == 0 ? "\xc3\xa9" : ""),
                static_cast<FieldType>(1 + random() % 5),
                random() % 3 != 0});
        }
        const bool valid = round % 2 == 0;
        std::string rows = "[";
        const auto count = 1 + random() % 20;
        for (size_t row = 0; row < count; ++row) {
            std::vector<size_t> order(width);
            std::iota(order.begin(), order.end(), 0);
            std::shuffle(order.begin(), order.end(), random);
            rows += row == 0 ? "{" : ",{";
            bool first = true;
            for (const auto index : order) {
                const auto& field = schema.fields[index];
                if (field.nullable && random() % 6 == 0) {
                    continue;
                }
                rows += (first ? "\"" : ",\"") + field.name + "\":" + RandomValue(random, field.type, valid);
                first = false;
                if (!valid && random() % 40 == 0) {
                    rows += ",\"" + field.name + "\":null";
                }
            }
            if (!valid && random() % 30 == 0) {
                rows += std::string(first ? "" : ",") + "\"unknown\":1";
            }
            rows += "}";
        }
        rows += "]";
        auto body = Body(rows);
        SCOPED_TRACE(round);
        ExpectSameParse(schema, body);
        // Byte-level mutations exercise the parser's syntax and limit checks.
        for (int mutation = 0; mutation < 6; ++mutation) {
            auto mutated = body;
            const auto at = random() % mutated.size();
            const std::string tokens[] = {",", "]", "}", "\"", ":", "{", "[", "\\", "\x80", "0", " "};
            switch (random() % 3) {
                case 0:
                    mutated.erase(at, 1);
                    break;
                case 1:
                    mutated.insert(at, tokens[random() % std::size(tokens)]);
                    break;
                default:
                    mutated[at] = tokens[random() % std::size(tokens)][0];
                    break;
            }
            ExpectSameParse(schema, mutated);
        }
    }
}

bool StrictEqual(const TJson& left, const TJson& right)
{
    if (left.type() != right.type()) {
        return false;
    }
    if (left.is_object()) {
        if (left.size() != right.size()) {
            return false;
        }
        for (const auto& [key, value] : left.items()) {
            if (!right.contains(key) || !StrictEqual(value, right.at(key))) {
                return false;
            }
        }
        return true;
    }
    if (left.is_array()) {
        if (left.size() != right.size()) {
            return false;
        }
        for (size_t i = 0; i < left.size(); ++i) {
            if (!StrictEqual(left[i], right[i])) {
                return false;
            }
        }
        return true;
    }
    if (left.is_number_float()) {
        return std::bit_cast<uint64_t>(left.get<double>()) == std::bit_cast<uint64_t>(right.get<double>());
    }
    return left == right;
}

// The streaming reader must accept, decode and reject exactly like nlohmann's
// SAX parser, raising handler failures at the same events.
void ExpectSameJson(const std::string& bytes)
{
    TParsed expected, actual;
    try {
        expected.Body = ParseJson(bytes);
    } catch (const TFailure& error) {
        expected.Error = error.Detail.code + ": " + error.Detail.message;
    }
    TJsonRows rows;
    try {
        actual.Body = ParseJson(bytes, rows);
    } catch (const TFailure& error) {
        actual.Error = error.Detail.code + ": " + error.Detail.message;
    }
    ASSERT_EQ(actual.Error, expected.Error) << bytes;
    if (rows.Present && expected.Error.empty()) {
        expected.Body["rows"] = nullptr;
    }
    EXPECT_TRUE(StrictEqual(actual.Body, expected.Body)) << bytes << "\n" << actual.Body << "\n" << expected.Body;
}

std::string RandomJson(std::mt19937_64& random, int depth)
{
    const auto space = [&] {
        const char* spaces[] = {"", "", "", " ", "\n", "\t", "\r\n "};
        return std::string(spaces[random() % std::size(spaces)]);
    };
    const auto string = [&] {
        const std::vector<std::string> parts{"a",
            "rows",
            "op",
            "\\\"",
            "\\\\",
            "\\/",
            "\\b",
            "\\f",
            "\\n",
            "\\r",
            "\\t",
            "\\u0000",
            "\\u001F",
            "\\u00e9",
            "\\u20AC",
            "\\uD83D\\uDE00",
            "\\ud800",
            "\\udc00",
            "\\uD800\\u0041",
            "\\u12",
            "\\x",
            "\xc3\xa9",
            "\xe2\x82\xac",
            "\xf0\x9f\x98\x80",
            "\xed\xa0\x80",
            "\xc0\x80",
            "\x7f",
            "\x1f",
            " "};
        std::string text = "\"";
        for (size_t n = random() % 5; n > 0; --n) {
            text += parts[random() % parts.size()];
        }
        return text + "\"";
    };
    const auto pick = random() % (depth > 5 ? 6 : 10);
    if (pick < 2) {
        const std::vector<std::string> numbers{"0",
            "-0",
            "7",
            "-7",
            "123456789",
            "18446744073709551615",
            "18446744073709551616",
            "-9223372036854775808",
            "-9223372036854775809",
            "1.5",
            "-0.0",
            "1e5",
            "1E+5",
            "1e-5",
            "2.5E-3",
            "1e400",
            "-1e400",
            "1e-400",
            "0.1e1",
            "123456789012345678901234567890",
            "01",
            "-",
            "1.",
            ".5",
            "1e",
            "1e+",
            "+1",
            "-a",
            "0x10",
            "4.9e-324",
            "1.7976931348623157e308",
            "1.7976931348623159e308"};
        return numbers[random() % numbers.size()];
    }
    if (pick < 4) {
        return string();
    }
    if (pick < 6) {
        const std::vector<std::string> literals{"true", "false", "null", "tru", "nul", "falsey", "True"};
        return literals[random() % literals.size()];
    }
    const bool object = pick < 8;
    std::string text = object ? "{" : "[";
    for (size_t n = random() % 5, i = 0; i < n; ++i) {
        text += space() + (i == 0 ? "" : ",") + space();
        if (object) {
            text += string() + space() + ":" + space();
        }
        text += RandomJson(random, depth + 1);
    }
    return text + space() + (object ? "}" : "]");
}

TEST(JsonAppendParser, ReaderMatchesNlohmannForRandomDocuments)
{
    for (const auto& bytes : std::initializer_list<std::string>{"",
             " ",
             "\xef\xbb\xbf{}",
             "\xef\xbb\xbf",
             "\xef\xbb{}",
             "\xef{}",
             " \xef\xbb\xbf{}",
             std::string("{}\0garbage", 10),
             std::string("[1,\0]", 5),
             std::string("\0", 1),
             "{}x",
             "{} ",
             "[] []",
             "\"a\"",
             "1",
             "-1",
             "1e5",
             "null",
             "[1,]",
             "{\"a\":1,}",
             "{\"a\" 1}",
             "{1:2}",
             "[\"\\uDBFF\\uDFFF\"]",
             "{\"a\":1,\"a\"",
             "{\"a\":1,\"\\u0061\":2}",
             "[[[[[[[[[[]]]]]]]]]]",
             "{\"rows\":1,\"rows\":2}",
             "{\"rows\":[{\"a\":1,\"a\":2}",
             "{\"rows\":[{\"a\":{\"b\":1,\"b\":2}}]}"})
    {
        ExpectSameJson(bytes);
    }
    std::mt19937_64 random(4242);
    const std::string tokens[] = {",",
        "]",
        "}",
        "\"",
        ":",
        "{",
        "[",
        "\\",
        "\x80",
        "0",
        " ",
        std::string(1, '\0'),
        "e",
        ".",
        "-",
        "+",
        "u",
        "\xef\xbb\xbf"};
    for (int round = 0; round < 20000; ++round) {
        auto bytes = RandomJson(random, 0);
        if (round % 3 == 0) {
            bytes = "{\"operation_id\":\"x\",\"rows\":" + bytes + "}";
        }
        for (auto mutations = random() % 3; mutations > 0 && !bytes.empty(); --mutations) {
            const auto at = random() % bytes.size();
            const auto& token = tokens[random() % std::size(tokens)];
            switch (random() % 3) {
                case 0:
                    bytes.erase(at, 1);
                    break;
                case 1:
                    bytes.insert(at, token);
                    break;
                default:
                    bytes[at] = token.empty() ? 'x' : token[0];
                    break;
            }
        }
        ExpectSameJson(bytes);
        if (HasFatalFailure()) {
            return;
        }
    }
}

TEST(JsonAppendParser, KeepsTheDomForOtherMembersAndOtherRoutes)
{
    TJsonRows rows;
    const auto body = ParseJson(R"({"operation_id":"op","rows":[{"a":"1"}],"expected_snapshot":"3"})", rows);
    EXPECT_EQ(body, TJson({{"operation_id", "op"}, {"rows", nullptr}, {"expected_snapshot", "3"}}));
    EXPECT_TRUE(rows.Present);
    const auto plain = ParseJson(R"({"operation_id":"op"})", rows);
    EXPECT_FALSE(rows.Present);
    EXPECT_EQ(plain, TJson({{"operation_id", "op"}}));
    // Only a top-level member is captured.
    EXPECT_EQ(ParseJson(R"([{"rows":[1]}])", rows), TJson::parse(R"([{"rows":[1]}])"));
    EXPECT_FALSE(rows.Present);
}
} // namespace
} // namespace glamdb::NTransport
