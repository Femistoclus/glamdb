// A bounded append (10,000 rows, 64 fields, at most 16 MiB of
// canonical input) fits the default owned-input budget on every supported
// standard library.

#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>

namespace glamdb::detail {
namespace {

std::shared_ptr<arrow::Array> Strings(int64_t rows, size_t length, char fill)
{
    arrow::StringBuilder builder;
    EXPECT_TRUE(builder.Reserve(rows).ok());
    EXPECT_TRUE(builder.ReserveData(rows * static_cast<int64_t>(length)).ok());
    std::string value(length, fill);
    for (int64_t row = 0; row < rows; ++row) {
        const auto digits = std::to_string(row);
        std::ranges::copy(digits, value.end() - static_cast<std::ptrdiff_t>(digits.size()));
        EXPECT_TRUE(builder.Append(value).ok());
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

TEST(ArrowInputReservation, NullHeavyReservationCoversOwnedRowsAndWriterAllocation)
{
    SchemaSpec schema;
    for (int column = 0; column < 64; ++column) {
        schema.fields.push_back({"n" + std::to_string(column), FieldType::Int64, true});
    }
    const auto batch = arrow::RecordBatch::Make(arrow::schema({}), 10000, std::vector<std::shared_ptr<arrow::Array>>{});
    const auto reservation = InputReservation(schema, {batch});
    ASSERT_TRUE(reservation);
    arrow::ProxyMemoryPool ownedPool(arrow::default_memory_pool());
    const auto normalized =
        NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {batch}, std::nullopt, &ownedPool);
    ASSERT_TRUE(normalized);
    ASSERT_NE(normalized->Columns, nullptr);
    arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
    const auto writerBatch = CopyRecordBatch(*normalized->Columns, &pool);
    ASSERT_TRUE(writerBatch);
    // The reservation must at least cover these simultaneous live allocations;
    // the canonical NULL encoding alone does not measure the writer's slots.
    EXPECT_GE(*reservation, static_cast<size_t>(ownedPool.max_memory() + pool.max_memory()));
    // Row-form input (NormalizeRows) with its writer batch fits it as well.
    std::vector<Row> rows(10000, Row(schema.fields.size(), std::monostate{}));
    const auto fromRows = NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", rows);
    ASSERT_TRUE(fromRows);
    arrow::ProxyMemoryPool rowPool(arrow::default_memory_pool());
    ASSERT_TRUE(MakeRecordBatch(schema, fromRows->rows, &rowPool));
    size_t ownedBytes = fromRows->rows.capacity() * sizeof(Row);
    for (const auto& row : fromRows->rows) {
        ownedBytes += row.capacity() * sizeof(Cell);
    }
    EXPECT_GE(*reservation, ownedBytes + static_cast<size_t>(rowPool.max_memory()));
    EXPECT_LE(*reservation, static_cast<size_t>(Limits{}.max_owned_input_bytes));
}

TEST(ArrowInputReservation, MaximumWideAppendFitsTheDefaultOwnedInputBudget)
{
    constexpr int64_t Rows = 10000;
    constexpr size_t Fields = 64;
    constexpr size_t KeyBytes = 34;
    constexpr size_t ValueBytes = 21;

    SchemaSpec schema;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    schema.fields.push_back({"key", FieldType::String, false});
    fields.push_back(arrow::field("key", arrow::utf8(), false));
    columns.push_back(Strings(Rows, KeyBytes, 'k'));
    for (size_t index = 1; index < Fields; ++index) {
        const auto name = "s" + std::to_string(index);
        schema.fields.push_back({name, FieldType::String, true});
        fields.push_back(arrow::field(name, arrow::utf8()));
        columns.push_back(Strings(Rows, ValueBytes, static_cast<char>('a' + index % 26)));
    }
    const auto batch = arrow::RecordBatch::Make(arrow::schema(fields), Rows, columns);

    // Per row: 64 presence bytes + 64 length prefixes + 34 + 63 * 21 value bytes = 1677.
    constexpr size_t Decoded = static_cast<size_t>(Rows) * (Fields * 5 + KeyBytes + (Fields - 1) * ValueBytes);
    static_assert(Decoded <= 16ULL * 1024 * 1024, "within the 16 MiB canonical per-append limit");

    // InputReservation applies the same shape limits as the canonical encoder
    // (rows, key, string, row and total bytes), so success means the append is
    // admissible. Hashing it as well would add nothing but minutes under MSan.
    const auto reservation = InputReservation(schema, {batch});
    ASSERT_TRUE(reservation) << reservation.error().message;
    const auto budget = static_cast<size_t>(Limits{}.max_owned_input_bytes);
    EXPECT_LE(*reservation, budget) << "sizeof(Cell)=" << sizeof(Cell) << " sizeof(Row)=" << sizeof(Row)
                                    << ": a maximum documented append can never be admitted with default Limits";
    // Standard libraries differ in sizeof(Cell): libstdc++ cells are 8 bytes
    // larger than libc++ ones. Charging cells per normalized copy must leave
    // room for that on whichever library this test is built with.
    constexpr size_t CellHeadroom = 16;
    EXPECT_LE(*reservation + static_cast<size_t>(Rows) * Fields * CellHeadroom, budget)
        << "the reservation leaves no room for a standard library with larger cells";
}

// The engine takes its reservation from the fingerprint's inspection; it must
// be exactly InputReservation, and normalization must reject any other shape.
TEST(ArrowInputReservation, FingerprintInspectionYieldsTheSameReservation)
{
    const SchemaSpec schema{{{"key", FieldType::String, false},
        {"s", FieldType::String, true},
        {"n", FieldType::Int64, true},
        {"omitted", FieldType::Bool, true}}};
    for (const int64_t rows : {1, 7, 1000}) {
        arrow::Int64Builder numbers;
        for (int64_t row = 0; row < rows; ++row) {
            EXPECT_TRUE((row % 3 == 0 ? numbers.AppendNull() : numbers.Append(row)).ok());
        }
        std::shared_ptr<arrow::Array> n;
        ASSERT_TRUE(numbers.Finish(&n).ok());
        const auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false),
                                                        arrow::field("s", arrow::utf8()),
                                                        arrow::field("n", arrow::int64())}),
            rows,
            {Strings(rows, 12, 'k'), Strings(rows, 5, 's'), n});
        const std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> inputs{{batch},
            {batch->Slice(0, rows / 2), batch->Slice(rows / 2)}};
        for (const auto& batches : inputs) {
            const auto fingerprinted = FingerprintInput(schema, "00112233-4455-6677-8899-aabbccddeeff", batches);
            ASSERT_TRUE(fingerprinted) << fingerprinted.error().message;
            EXPECT_EQ(fingerprinted->Reservation, InputReservation(schema, batches).value());
            EXPECT_EQ(fingerprinted->PayloadHash,
                FingerprintBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", batches).value());
            const auto normalized =
                NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", batches, fingerprinted->Reservation);
            ASSERT_TRUE(normalized) << normalized.error().message;
            EXPECT_EQ(normalized->payload_hash, fingerprinted->PayloadHash);
            for (const auto other : {fingerprinted->Reservation - 1, fingerprinted->Reservation + 1}) {
                const auto changed = NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", batches, other);
                ASSERT_FALSE(changed);
                EXPECT_EQ(changed.error().code, "INVALID_ARGUMENT");
                EXPECT_EQ(changed.error().message, "Arrow input changed during append");
            }
        }
    }
    // Validation failures are the fingerprint's, with the same retry hint.
    const auto invalid = FingerprintInput(schema, "00112233-4455-6677-8899-aabbccddeeff", {nullptr});
    ASSERT_FALSE(invalid);
    EXPECT_EQ(invalid.error().code, InputReservation(schema, {nullptr}).error().code);
    EXPECT_EQ(invalid.error().retry_hint, RetryHint::FixInput);
}

TEST(ArrowInputReservation, NativeScalarNullAndOmittedSlotsCoverMeasuredWriterGrowth)
{
    for (const auto type : {FieldType::Decimal, FieldType::Date}) {
        SchemaSpec schema;
        for (int column = 0; column < 64; ++column) {
            schema.fields.push_back({"v" + std::to_string(column),
                type,
                true,
                type == FieldType::Decimal ? 38 : 0,
                type == FieldType::Decimal ? 2 : 0});
        }
        const auto omitted =
            arrow::RecordBatch::Make(arrow::schema({}), 10000, std::vector<std::shared_ptr<arrow::Array>>{});
        const auto reservation = InputReservation(schema, {omitted});
        ASSERT_TRUE(reservation);
        arrow::ProxyMemoryPool ownedPool(arrow::default_memory_pool());
        const auto normalized =
            NormalizeBatches(schema, "00112233-4455-6677-8899-aabbccddeeff", {omitted}, std::nullopt, &ownedPool);
        ASSERT_TRUE(normalized);
        ASSERT_NE(normalized->Columns, nullptr);
        EXPECT_EQ(normalized->GetRowCount(), 10000U);
        arrow::ProxyMemoryPool pool(arrow::default_memory_pool());
        const auto writer = CopyRecordBatch(*normalized->Columns, &pool);
        ASSERT_TRUE(writer);
        EXPECT_GE(*reservation, static_cast<size_t>(ownedPool.max_memory() + pool.max_memory()));
        std::vector<Row> rows(10000, Row(schema.fields.size(), std::monostate{}));
        const auto fromRows = NormalizeRows(schema, "00112233-4455-6677-8899-aabbccddeeff", rows);
        ASSERT_TRUE(fromRows);
        arrow::ProxyMemoryPool rowPool(arrow::default_memory_pool());
        ASSERT_TRUE(MakeRecordBatch(schema, fromRows->rows, &rowPool));
        size_t ownedBytes = fromRows->rows.capacity() * sizeof(Row);
        for (const auto& row : fromRows->rows) {
            ownedBytes += row.capacity() * sizeof(Cell);
        }
        EXPECT_GE(*reservation, ownedBytes + static_cast<size_t>(rowPool.max_memory()));
        EXPECT_LE(*reservation, static_cast<size_t>(Limits{}.max_owned_input_bytes));
        const auto explicitNulls = InputReservation(schema, {*writer});
        ASSERT_TRUE(explicitNulls);
        EXPECT_EQ(*explicitNulls, *reservation);
    }
}

} // namespace
} // namespace glamdb::detail
