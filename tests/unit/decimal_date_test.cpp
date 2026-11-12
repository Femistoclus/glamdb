#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/predicate.h"
#include "storage/scalars.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <limits>

namespace glamdb::detail {
namespace {

TEST(TDecimalDateTest, ParsesExactDecimalCoefficientsAndNormalizesText)
{
    for (const auto& [input, expected] :
        {std::pair{"00012.3400", "12.34"}, {"-0.000", "0.00"}, {"-999.99", "-999.99"}, {"1", "1.00"}})
    {
        const auto value = ParseDecimal(input, 5, 2);
        ASSERT_TRUE(value) << value.error().message;
        EXPECT_EQ(FormatDecimal(*value), expected);
    }
    const auto largest = ParseDecimal("99999999999999999999999999999999999999", 38, 0);
    ASSERT_TRUE(largest);
    EXPECT_EQ(FormatDecimal(*largest), "99999999999999999999999999999999999999");
    for (const auto input : {"1000", "1.001", "nan", "1e2", "+1", " 1", ".1", "1.", "--1"}) {
        EXPECT_FALSE(ParseDecimal(input, 5, 2)) << input;
    }
    EXPECT_FALSE(ParseDecimal("0", 39, 0));
    EXPECT_FALSE(ParseDecimal("0", 0, 0));
    EXPECT_FALSE(ParseDecimal("0", 2, 3));
    EXPECT_FALSE(ParseDecimal("0", 2, -1));
    const auto fractional = ParseDecimal("0.001", 3, 3);
    ASSERT_TRUE(fractional);
    EXPECT_EQ(fractional->coefficient_low, 1U);
    EXPECT_EQ(FormatDecimal(*fractional), "0.001");
}

TEST(TDecimalDateTest, ValidatesCalendarDatesWithoutTimestampCoercion)
{
    for (const auto& [text, days] : {std::pair{"0001-01-01", -719162},
             {"1969-12-31", -1},
             {"1970-01-01", 0},
             {"2000-02-29", 11016},
             {"9999-12-31", 2932896}})
    {
        const auto date = ParseDate(text);
        ASSERT_TRUE(date);
        EXPECT_EQ(date->days_since_epoch, days);
        EXPECT_EQ(FormatDate(*date), text);
    }
    for (const auto text : {"1900-02-29",
             "2023-02-29",
             "0000-01-01",
             "10000-01-01",
             "2024-1-01",
             "2024-02-30",
             "1970-01-01T00:00:00Z",
             "2024-01-00"})
    {
        EXPECT_FALSE(ParseDate(text)) << text;
    }
}

TEST(TDecimalDateTest, CalendarDateSchemaUsesVersionTwo)
{
    const SchemaSpec schema{{{"day", static_cast<FieldType>(7), false}}};
    const auto encoded = EncodeSchema(schema);
    ASSERT_TRUE(encoded) << encoded.error().message;
    ASSERT_GT(encoded->size(), 18U);
    EXPECT_EQ(static_cast<unsigned char>((*encoded)[16]), 2);
    EXPECT_EQ(static_cast<unsigned char>((*encoded)[17]), 0);
    const auto decoded = DecodeSchema(*encoded);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(*decoded, schema);
}

TEST(TDecimalDateTest, MixedRowsAndArrowMatchIndependentCanonicalFixture)
{
    const SchemaSpec schema{{{"key", FieldType::String, false},
        {"amount", FieldType::Decimal, true, 38, 2},
        {"day", FieldType::Date, true}}};
    const auto minimum = ParseDecimal("-999999999999999999999999999999999999.99", 38, 2);
    const auto maximum = ParseDecimal("999999999999999999999999999999999999.99", 38, 2);
    const auto zero = ParseDecimal("-000.0000", 38, 2);
    ASSERT_TRUE(minimum);
    ASSERT_TRUE(maximum);
    ASSERT_TRUE(zero);
    const std::vector<Row> rows{{std::string("min"), *minimum, Date{-719162}},
        {std::string("max"), *maximum, Date{2932896}},
        {std::string("zero"), *zero, Date{0}},
        {std::string("null"), std::monostate{}, std::monostate{}}};
    const auto fingerprint = SchemaFingerprint(schema);
    ASSERT_TRUE(fingerprint);
    EXPECT_EQ(*fingerprint, "9604413bcbf96f2c63a749c7aa9dee2dcc5fe2224a7c712e9e88cb382731dfb6");
    const auto normalized = NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", rows);
    ASSERT_TRUE(normalized);
    EXPECT_EQ(normalized->payload_hash, "e3dce310f241d4479f7bf5ed508ebf2a5661e2cc6424cc910658bea647ca0144");
    const auto batch = MakeRecordBatch(schema, rows);
    ASSERT_TRUE(batch);
    EXPECT_TRUE((*batch)->column(1)->type()->Equals(*arrow::decimal128(38, 2)));
    EXPECT_TRUE((*batch)->column(2)->type()->Equals(*arrow::date32()));
    const auto arrowHash = FingerprintBatches(schema,
        "00112233-4455-6677-8899-aabbccddeeff",
        {(*batch)->Slice(0, 1), (*batch)->Slice(1, 3)});
    ASSERT_TRUE(arrowHash);
    EXPECT_EQ(*arrowHash, normalized->payload_hash);
    const auto extracted = NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {*batch});
    ASSERT_TRUE(extracted);
    EXPECT_TRUE(extracted->rows.empty());
    ASSERT_NE(extracted->Columns, nullptr);
    EXPECT_TRUE(extracted->Columns->Equals(**batch));
    EXPECT_EQ(extracted->decoded_bytes, normalized->decoded_bytes);
    const auto schemaBytes = EncodeSchema(schema);
    ASSERT_TRUE(schemaBytes);
    auto legacy = *schemaBytes;
    legacy[16] = 1;
    EXPECT_FALSE(DecodeSchema(legacy));
    auto changed = schema;
    changed.fields[1].scale = 3;
    EXPECT_NE(SchemaFingerprint(changed), fingerprint);
    changed.fields[2].precision = 1;
    EXPECT_FALSE(EncodeSchema(changed));
}

TEST(TDecimalDateTest, RejectsInvalidCellsAndAllNullArrowParameterSubstitution)
{
    const SchemaSpec schema{{{"amount", FieldType::Decimal, true, 3, 2}, {"day", FieldType::Date, true}}};
    const auto tooLarge = ParseDecimal("10.00", 4, 2);
    ASSERT_TRUE(tooLarge);
    auto invalid = *tooLarge;
    invalid.precision = 3;
    EXPECT_FALSE(NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", {{invalid, Date{0}}}));
    EXPECT_FALSE(NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", {{*tooLarge, Date{0}}}));
    EXPECT_FALSE(NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", {{std::monostate{}, Date{2932897}}}));
    const SchemaSpec other{{{"amount", FieldType::Decimal, true, 4, 2}, {"day", FieldType::Date, true}}};
    const auto batch = MakeRecordBatch(other, {{std::monostate{}, std::monostate{}}});
    ASSERT_TRUE(batch);
    EXPECT_FALSE(NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {*batch}));
    EXPECT_FALSE(FingerprintBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {*batch}));
    EXPECT_FALSE(InputReservation(schema, {*batch}));
}

TEST(TDecimalDateTest, RejectsMinimumPhysicalCoefficientWithoutSignedNegationOverflow)
{
    const SchemaSpec schema{{{"amount", FieldType::Decimal, true, 38, 2}}};
    const Decimal minimum{std::numeric_limits<int64_t>::min(), 0, 38, 2};
    EXPECT_FALSE(NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", {{minimum}}));
    const auto batch = MakeRecordBatch(schema, {{minimum}});
    ASSERT_TRUE(batch);
    EXPECT_FALSE(NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {*batch}));
    EXPECT_FALSE(FingerprintBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {*batch}));
    EXPECT_FALSE(BindPredicate(Predicate::Compare("amount", CompareOp::Eq, minimum), schema));
}

TEST(TDecimalDateTest, ExactPredicatesAndRowSelectionRetainDecimalAndDateTypes)
{
    const SchemaSpec schema{{{"amount", FieldType::Decimal, true, 38, 2}, {"day", FieldType::Date, true}}};
    const auto low = ParseDecimal("-999999999999999999999999999999999999.99", 38, 2);
    const auto high = ParseDecimal("999999999999999999999999999999999999.99", 38, 2);
    const auto one = ParseDecimal("0.01", 38, 2);
    ASSERT_TRUE(low);
    ASSERT_TRUE(high);
    ASSERT_TRUE(one);
    const auto batch = MakeRecordBatch(schema,
        {{*low, Date{-1}},
            {*one, Date{0}},
            {*high, Date{2932896}},
            {std::monostate{}, std::monostate{}},
            {*one, Date{0}}});
    ASSERT_TRUE(batch);
    const auto evaluate = [&](const Predicate& predicate, const std::vector<int64_t>& expected) {
        const auto bound = BindPredicate(predicate, schema);
        ASSERT_TRUE(bound);
        const auto rows = SelectRows(*bound, **batch);
        ASSERT_TRUE(rows);
        EXPECT_EQ(*rows, expected);
    };
    evaluate(Predicate::Compare("amount", CompareOp::Lt, *one), {0});
    evaluate(Predicate::Compare("amount", CompareOp::Ge, *one), {1, 2, 4});
    evaluate(Predicate::In("amount", {*high, std::nullopt, *one, *one}), {1, 2, 4});
    evaluate(Predicate::Not(Predicate::In("amount", {*one, std::nullopt})), {});
    evaluate(Predicate::Compare("day", CompareOp::Le, Date{0}), {0, 1, 4});
    evaluate(Predicate::In("day", {Date{0}, std::nullopt}), {1, 4});
    auto mismatch = *one;
    mismatch.scale = 3;
    EXPECT_FALSE(BindPredicate(Predicate::Compare("amount", CompareOp::Eq, mismatch), schema));
    mismatch = *one;
    mismatch.precision = 37;
    EXPECT_FALSE(BindPredicate(Predicate::In("amount", {mismatch}), schema));
    EXPECT_FALSE(BindPredicate(Predicate::Compare("day", CompareOp::Eq, TimestampUs{0}), schema));
    for (int column = 0; column < 2; ++column) {
        const auto taken = TakeRows(*(*batch)->column(column), std::vector<int64_t>{4, 3, 0});
        ASSERT_TRUE(taken);
        EXPECT_TRUE((*taken)->type()->Equals(*(*batch)->column(column)->type()));
        EXPECT_TRUE((*taken)->IsNull(1));
    }
}

} // namespace
} // namespace glamdb::detail
