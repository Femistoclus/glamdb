#include "core/api/canonical.h"

#include <gtest/gtest.h>

namespace glamdb {
namespace {

TEST(SchemaWidth, WideCanonicalSchemasRoundTripAndKeepDuplicateRows)
{
    for (const size_t width : {65U, 105U, 128U}) {
        SchemaSpec schema;
        Row row;
        for (size_t index = 0; index < width; ++index) {
            schema.fields.push_back({"c" + std::to_string(index), FieldType::Int64, true});
            row.emplace_back(int64_t{9007199254740993LL} + static_cast<int64_t>(index));
        }
        const auto encoded = detail::EncodeSchema(schema);
        ASSERT_TRUE(encoded) << encoded.error().message;
        const auto decoded = detail::DecodeSchema(*encoded);
        ASSERT_TRUE(decoded) << decoded.error().message;
        EXPECT_EQ(*decoded, schema);
        const auto input = detail::NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", {row, row});
        ASSERT_TRUE(input) << input.error().message;
        EXPECT_EQ(input->rows, (std::vector<Row>{row, row}));
    }
}

TEST(SchemaWidth, OversizedSchemasFailBeforeCanonicalInput)
{
    SchemaSpec schema;
    for (size_t index = 0; index < 129; ++index) {
        schema.fields.push_back({"c" + std::to_string(index), FieldType::Int64, false});
    }
    const auto encoded = detail::EncodeSchema(schema);
    ASSERT_FALSE(encoded);
    EXPECT_EQ(encoded.error().code, "INVALID_ARGUMENT");
}

TEST(SchemaWidth, MaximumWidthAndFieldNameLengthsRoundTrip)
{
    SchemaSpec schema;
    for (size_t index = 0; index < 128; ++index) {
        const auto suffix = std::to_string(index);
        schema.fields.push_back({std::string(128 - suffix.size(), 'x') + suffix, FieldType::Int64, false});
    }
    const auto encoded = detail::EncodeSchema(schema);
    ASSERT_TRUE(encoded) << encoded.error().message;
    const auto decoded = detail::DecodeSchema(*encoded);
    ASSERT_TRUE(decoded) << decoded.error().message;
    EXPECT_EQ(*decoded, schema);
}

TEST(SchemaWidth, MaximumWidthDecimalSchemaWithLongNamesRoundTrips)
{
    SchemaSpec schema;
    for (size_t index = 0; index < MaxSchemaFields; ++index) {
        const auto suffix = std::to_string(index);
        schema.fields.push_back({std::string(128 - suffix.size(), 'd') + suffix, FieldType::Decimal, true, 38, 4});
    }
    const auto encoded = detail::EncodeSchema(schema);
    ASSERT_TRUE(encoded) << encoded.error().message;
    EXPECT_EQ(encoded->size(), 20 + MaxSchemaFields * 138);
    const auto decoded = detail::DecodeSchema(*encoded);
    ASSERT_TRUE(decoded) << decoded.error().message;
    EXPECT_EQ(*decoded, schema);
}

TEST(SchemaWidth, ExistingSchemaAndAppendGoldensRemainUnchanged)
{
    const SchemaSpec schema{{{"n", FieldType::Int64, true}}};
    const auto fingerprint = detail::SchemaFingerprint(schema);
    ASSERT_TRUE(fingerprint);
    EXPECT_EQ(*fingerprint, "4078bb95b0753db6f11470b1507afe1936afc16845b96029fbf1105110301685");
    const auto normalized = detail::NormalizeRows(schema,
        "00112233-4455-6677-8899-aabbccddeeff",
        {{int64_t{9007199254740993LL}}, {std::monostate{}}, {int64_t{9007199254740993LL}}});
    ASSERT_TRUE(normalized);
    EXPECT_EQ(normalized->payload_hash, "e9cbe7131f25c3f2932c48f755529fd10370a936c5182e2bca621d5f9e3c0fd7");
}

} // namespace
} // namespace glamdb
