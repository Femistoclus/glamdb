#include "apps/transport/arrow_ipc.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>
#include <openssl/sha.h>

namespace glamdb::NTransport {
namespace {
constexpr std::string_view Eos{"\xff\xff\xff\xff\0\0\0\0", 8};

void CheckEnvelope(const TResponse& response, std::string_view snapshot)
{
    EXPECT_EQ(response.Status, 200);
    EXPECT_EQ(response.ContentType, "application/vnd.apache.arrow.stream");
    EXPECT_EQ(response.Headers.at("X-GlamDB-Snapshot-Id"), snapshot);
    EXPECT_TRUE(response.Body.ends_with(Eos));
    EXPECT_NE(SerializeResponseHead(response).find("Content-Length: " + std::to_string(response.Body.size()) + "\r\n"),
        std::string::npos);
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(response.Body.data()), response.Body.size(), digest);
    std::string expected;
    for (const auto byte : digest) {
        expected += "0123456789abcdef"[byte >> 4];
        expected += "0123456789abcdef"[byte & 15];
    }
    EXPECT_EQ(response.Headers.at("X-Content-SHA256"), expected);
}

TEST(ArrowIpc, PreservesAllScalarsAndSnapshot)
{
    const SchemaSpec schema{{{"key", FieldType::String, false},
        {"n", FieldType::Int64, true},
        {"f", FieldType::Float64, true},
        {"b", FieldType::Bool, true},
        {"t", FieldType::TimestampUsUtc, true}}};
    const auto rows = ParseJson(R"([
        {"key":"\u0000\"\\я","n":"-9223372036854775808","f":1.25,"b":false,"t":"0001-01-01T00:00:00.000000Z"},
        {"key":"x","n":"9223372036854775807","f":-0.0,"b":true,"t":"9999-12-31T23:59:59.999999Z"},
        {"key":"x","n":null,"f":null,"b":null,"t":null}
    ])");
    const auto batch = ParseRows(schema, rows);
    for (int64_t offset = 0; offset < batch->num_rows(); ++offset) {
        SCOPED_TRACE(offset);
        const auto slice = batch->Slice(offset);
        const auto response = EncodeArrowPage(batch->schema(), slice, SnapshotSelector::Id(9007199254740993LL));
        CheckEnvelope(response, "9007199254740993");
        auto input = arrow::io::BufferReader::FromString(response.Body);
        auto reader = arrow::ipc::RecordBatchStreamReader::Open(input.get());
        ASSERT_TRUE(reader.ok()) << reader.status();
        const auto decoded = (*reader)->Next();
        ASSERT_TRUE(decoded.ok()) << decoded.status();
        ASSERT_TRUE(*decoded);
        EXPECT_TRUE((*decoded)->Equals(*slice));
        EXPECT_EQ(EncodeRows(**decoded), TJson(rows.begin() + offset, rows.end()));
        const auto end = (*reader)->Next();
        ASSERT_TRUE(end.ok());
        EXPECT_FALSE(*end);
        EXPECT_EQ(*input->Tell(), static_cast<int64_t>(response.Body.size()));
    }
}

TEST(ArrowIpc, EmptyResultRetainsSchema)
{
    const auto schema = arrow::schema(
        {arrow::field("key", arrow::utf8(), false, arrow::key_value_metadata({"PARQUET:field_id"}, {"1"}))});
    const auto response = EncodeArrowPage(schema, nullptr, SnapshotSelector::InitialEmpty());
    CheckEnvelope(response, "initial");
    auto input = arrow::io::BufferReader::FromString(response.Body);
    const auto reader = arrow::ipc::RecordBatchStreamReader::Open(input.get());
    ASSERT_TRUE(reader.ok()) << reader.status();
    EXPECT_TRUE((*reader)->schema()->Equals(*schema, true));
    const auto end = (*reader)->Next();
    ASSERT_TRUE(end.ok());
    EXPECT_FALSE(*end);
    EXPECT_EQ(*input->Tell(), static_cast<int64_t>(response.Body.size()));
}

TEST(ArrowIpc, ZeroColumnResultRetainsRowCount)
{
    const auto schema = arrow::schema({});
    const auto batch = arrow::RecordBatch::Make(schema, 3, arrow::ArrayVector{});
    const auto response = EncodeArrowPage(schema, batch, SnapshotSelector::Id(1));
    auto input = arrow::io::BufferReader::FromString(response.Body);
    const auto reader = arrow::ipc::RecordBatchStreamReader::Open(input.get());
    ASSERT_TRUE(reader.ok());
    const auto decoded = (*reader)->Next();
    ASSERT_TRUE(decoded.ok());
    ASSERT_TRUE(*decoded);
    EXPECT_EQ((*decoded)->num_rows(), 3);
    EXPECT_EQ((*decoded)->num_columns(), 0);
}

TEST(ArrowIpc, LimitIncludesSchemaBatchAndEos)
{
    const auto batch = ParseRows({{{"key", FieldType::String, false}}}, ParseJson(R"([{"key":"value"}])"));
    for (const auto& value : {std::shared_ptr<arrow::RecordBatch>{}, batch}) {
        const auto snapshot = SnapshotSelector::InitialEmpty();
        const auto full = EncodeArrowPage(batch->schema(), value, snapshot);
        EXPECT_EQ(EncodeArrowPage(batch->schema(), value, snapshot, full.Body.size()).Body, full.Body);
        for (const size_t limit : {size_t{0}, full.Body.size() - 8, full.Body.size() - 1}) {
            try {
                (void)EncodeArrowPage(batch->schema(), value, snapshot, limit);
                FAIL() << "accepted incomplete IPC budget " << limit;
            } catch (const TFailure& error) {
                EXPECT_EQ(error.Detail.code, "INPUT_TOO_LARGE");
            }
        }
    }
}

TEST(ArrowIpc, RejectsMismatchedSchemaAndUnresolvedSnapshot)
{
    const auto batch = ParseRows({{{"key", FieldType::String, false}}}, ParseJson(R"([{"key":"value"}])"));
    EXPECT_THROW(EncodeArrowPage(arrow::schema({}), batch, SnapshotSelector::Id(1)), TFailure);
    EXPECT_THROW(EncodeArrowPage(batch->schema(), batch, SnapshotSelector::Latest()), TFailure);
}
} // namespace
} // namespace glamdb::NTransport
