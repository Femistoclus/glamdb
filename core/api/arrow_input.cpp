#include "core/api/canonical.h"

#include "core/api/binary.h"

#include <arrow/api.h>

#include <arrow/util/bit_util.h>

#include <cmath>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <vector>

namespace glamdb::detail {
namespace {

// `validated`: Inspect already ran ValidateFull on this batch in the same call.
std::vector<int> MapFields(const SchemaSpec& schema, const arrow::RecordBatch& batch, bool validated = false)
{
    Require(batch.num_columns() <= static_cast<int>(schema.fields.size()) && (validated || batch.ValidateFull().ok()),
        "INVALID_ARGUMENT",
        "invalid Arrow batch");
    std::unordered_map<std::string, int> names;
    for (int i = 0; i < batch.num_columns(); ++i) {
        const auto& name = batch.schema()->field(i)->name();
        Require(names.emplace(name, i).second &&
                std::ranges::any_of(schema.fields,
                    [&](const auto& field) {
                        return field.name == name;
                    }),
            "INVALID_ARGUMENT",
            "unknown or duplicate Arrow field");
    }
    std::vector<int> columns;
    for (const auto& field : schema.fields) {
        const auto found = names.find(field.name);
        if (found == names.end()) {
            Require(field.nullable, "INVALID_ARGUMENT", "required Arrow field is missing");
            columns.push_back(-1);
            continue;
        }
        const auto& type = batch.column(found->second)->type();
        bool matches = false;
        switch (field.type) {
            case FieldType::String:
                matches = type->id() == arrow::Type::STRING;
                break;
            case FieldType::Bool:
                matches = type->id() == arrow::Type::BOOL;
                break;
            case FieldType::Int64:
                matches = type->id() == arrow::Type::INT64;
                break;
            case FieldType::Float64:
                matches = type->id() == arrow::Type::DOUBLE;
                break;
            case FieldType::TimestampUsUtc:
                if (type->id() == arrow::Type::TIMESTAMP) {
                    const auto& timestamp = static_cast<const arrow::TimestampType&>(*type);
                    matches = timestamp.unit() == arrow::TimeUnit::MICRO && timestamp.timezone() == "UTC";
                }
                break;
        }
        Require(matches, "INVALID_ARGUMENT", "Arrow field type does not match fixed schema");
        columns.push_back(found->second);
    }
    return columns;
}

struct Shape
{
    size_t rows = 0;
    size_t decoded_bytes = 0;
    size_t null_slot_bytes = 0;
};

// Typed view of one schema field in one batch, resolved once per batch so
// per-cell loops read raw buffers instead of boxed arrow::Array accessors.
// Readers mirror the arrow::Array accessors exactly, including slice offsets.
struct Column
{
    FieldType type = FieldType::String;
    bool nullable = true;
    // Omitted nullable field: every value is NULL.
    bool present = false;
    // Without a validity bitmap Array::IsNull reports null_count == length.
    bool all_null = false;
    size_t max_length = 0;
    int64_t offset = 0;
    const uint8_t* validity = nullptr;
    // Value bitmap (bool), fixed-width values, or string offsets.
    const uint8_t* values = nullptr;
    const char* data = nullptr;

    bool IsNull(int64_t row) const
    {
        if (!present) {
            return true;
        }
        return validity != nullptr ? !arrow::bit_util::GetBit(validity, offset + row) : all_null;
    }
    template <class T>
    T Value(int64_t row) const
    {
        T value;
        std::memcpy(&value, values + (offset + row) * static_cast<int64_t>(sizeof(T)), sizeof(T));
        return value;
    }
    bool Bool(int64_t row) const
    {
        return arrow::bit_util::GetBit(values, offset + row);
    }
    int32_t Begin(int64_t row) const
    {
        return Value<int32_t>(row);
    }
    int32_t Length(int64_t row) const
    {
        return Value<int32_t>(row + 1) - Value<int32_t>(row);
    }
    std::string_view Text(int64_t row) const
    {
        return {data + Begin(row), static_cast<size_t>(Length(row))};
    }
};

std::vector<Column> Columns(const SchemaSpec& schema, const arrow::RecordBatch& batch, const std::vector<int>& mapping)
{
    std::vector<Column> columns;
    columns.reserve(mapping.size());
    for (size_t field = 0; field < mapping.size(); ++field) {
        Column column;
        column.type = schema.fields[field].type;
        column.nullable = schema.fields[field].nullable;
        column.max_length = schema.fields[field].name == "key" ? 1024 : 1024 * 1024;
        if (mapping[field] >= 0) {
            const auto& data = *batch.column_data(mapping[field]);
            const auto buffer = [&data](size_t index) -> const uint8_t* {
                return index < data.buffers.size() && data.buffers[index] != nullptr ? data.buffers[index]->data()
                                                                                      : nullptr;
            };
            column.present = true;
            column.offset = data.offset;
            column.validity = buffer(0);
            column.all_null = column.validity == nullptr && data.null_count == data.length;
            column.values = buffer(1);
            column.data = reinterpret_cast<const char*>(buffer(2));
        }
        columns.push_back(column);
    }
    return columns;
}

Shape Inspect(const SchemaSpec& schema, const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    const auto fingerprint = SchemaFingerprint(schema);
    if (!fingerprint) {
        throw Failure(fingerprint.error());
    }
    Require(!batches.empty() && batches.size() <= 10000, "INVALID_ARGUMENT", "append requires bounded Arrow batches");
    Shape shape;
    for (const auto& batch : batches) {
        Require(batch != nullptr && batch->num_rows() >= 0 && batch->num_rows() <= 10000 &&
                shape.rows + static_cast<size_t>(batch->num_rows()) <= 10000,
            "INVALID_ARGUMENT",
            "append requires at most 10000 rows");
        shape.rows += static_cast<size_t>(batch->num_rows());
        const auto columns = Columns(schema, *batch, MapFields(schema, *batch));
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            size_t bytes = schema.fields.size();
            for (const auto& column : columns) {
                if (column.IsNull(row)) {
                    Require(column.nullable, "INVALID_ARGUMENT", "NULL in required Arrow field");
                    shape.null_slot_bytes +=
                        column.type == FieldType::String ? 4 : column.type == FieldType::Bool ? 1 : 8;
                    continue;
                }
                if (column.type == FieldType::String) {
                    const auto length = column.Length(row);
                    Require(static_cast<size_t>(length) <= column.max_length,
                        "INVALID_ARGUMENT",
                        "oversized Arrow string");
                    bytes += 4 + static_cast<size_t>(length);
                } else {
                    bytes += column.type == FieldType::Bool ? 1 : 8;
                }
            }
            shape.decoded_bytes += bytes;
            Require(bytes <= 2ULL * 1024 * 1024 && shape.decoded_bytes <= 16ULL * 1024 * 1024,
                "INPUT_TOO_LARGE",
                "Arrow input exceeds decoded byte limit");
        }
    }
    Require(shape.rows != 0, "INVALID_ARGUMENT", "empty append is not supported");
    return shape;
}

// Canonical row-major encoding of one batch, with the value checks of the
// owned-row encoder.
void HashRows(CanonicalHasher& hash, const std::vector<Column>& columns, int64_t rows)
{
    for (int64_t row = 0; row < rows; ++row) {
        for (const auto& column : columns) {
            if (column.IsNull(row)) {
                Require(column.nullable, "INVALID_ARGUMENT", "NULL in required field");
                hash.Number(uint8_t{0});
                continue;
            }
            hash.Number(uint8_t{1});
            switch (column.type) {
                case FieldType::String: {
                    const auto value = column.Text(row);
                    Require(value.size() <= column.max_length && ValidUtf8(value),
                        "INVALID_ARGUMENT",
                        "invalid UTF-8 string");
                    hash.Number(static_cast<uint32_t>(value.size()));
                    hash.Bytes(value);
                    break;
                }
                case FieldType::Bool:
                    hash.Number(static_cast<uint8_t>(column.Bool(row) ? 1 : 0));
                    break;
                case FieldType::Int64:
                case FieldType::TimestampUsUtc: {
                    const auto value = column.Value<int64_t>(row);
                    Require(column.type == FieldType::Int64 ||
                            (value >= -62135596800000000LL && value <= 253402300799999999LL),
                        "INVALID_ARGUMENT",
                        "timestamp outside years 0001..9999");
                    hash.Number(value);
                    break;
                }
                case FieldType::Float64: {
                    auto value = column.Value<double>(row);
                    Require(std::isfinite(value), "INVALID_ARGUMENT", "non-finite float");
                    if (value == 0) {
                        value = 0.0;
                    }
                    hash.Number(std::bit_cast<uint64_t>(value));
                    break;
                }
            }
        }
    }
}

Cell Read(const Column& column, int64_t row)
{
    if (column.IsNull(row)) {
        return std::monostate{};
    }
    switch (column.type) {
        case FieldType::String:
            return std::string(column.Text(row));
        case FieldType::Bool:
            return column.Bool(row);
        case FieldType::Int64:
            return column.Value<int64_t>(row);
        case FieldType::Float64:
            return column.Value<double>(row);
        case FieldType::TimestampUsUtc:
            return TimestampUs{column.Value<int64_t>(row)};
    }
    throw Failure({"INVALID_ARGUMENT", "unsupported Arrow field", RetryHint::FixInput});
}

size_t Reservation(const SchemaSpec& schema, const Shape& shape)
{
    // Payload bytes: the owned rows, the canonical string while hashing and
    // the Arrow batch with builder growth (4 copies). STL cells, including
    // tiny/null values whose buffers alone undercount RAM, exist once;
    // row headers twice while the extracted rows are moved into the
    // normalized input. NULL encodings omit typed slots, but Arrow still
    // allocates them; reserve their builder growth separately. Hashing now
    // streams the canonical bytes; the estimate keeps that copy so that the
    // owned-input budget admits exactly the same appends as before.
    return shape.decoded_bytes * 4 + shape.null_slot_bytes * 2 +
        shape.rows * (sizeof(Row) * 2 + schema.fields.size() * sizeof(Cell)) + 65536;
}

// Hashes batches that `shape` was inspected from in the same call.
std::string Fingerprint(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const Shape& shape)
{
    const auto header = EncodeAppendHeader(schema, collection_id, shape.rows);
    if (!header) {
        throw Failure(header.error());
    }
    CanonicalHasher hash;
    hash.Bytes(*header);
    for (const auto& batch : batches) {
        HashRows(hash, Columns(schema, *batch, MapFields(schema, *batch, true)), batch->num_rows());
    }
    return hash.Digest();
}

} // namespace

Result<size_t> InputReservation(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        return Reservation(schema, Inspect(schema, batches));
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<std::string> FingerprintBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    const auto result = FingerprintInput(schema, collection_id, batches);
    if (!result) {
        return std::unexpected(result.error());
    }
    return result->payload_hash;
}

Result<FingerprintedInput> FingerprintInput(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        const auto shape = Inspect(schema, batches);
        return FingerprintedInput{Fingerprint(schema, collection_id, batches, shape), Reservation(schema, shape)};
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<NormalizedInput> NormalizeBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    std::optional<size_t> reserved)
{
    try {
        // Inspected again: these are caller buffers, and this pass validates
        // exactly the memory the owned copy reads. A shape that differs from
        // the admitted reservation means the caller changed the input.
        const auto shape = Inspect(schema, batches);
        if (reserved && Reservation(schema, shape) != *reserved) {
            // The same error as the coordinator's hash comparison.
            return std::unexpected(
                ErrorDetail{"INVALID_ARGUMENT", "Arrow input changed during append", RetryHint::OperatorAction});
        }
        std::vector<Row> rows;
        rows.reserve(shape.rows);
        for (const auto& batch : batches) {
            const auto columns = Columns(schema, *batch, MapFields(schema, *batch, true));
            for (int64_t index = 0; index < batch->num_rows(); ++index) {
                Row row;
                row.reserve(columns.size());
                for (const auto& column : columns) {
                    row.push_back(Read(column, index));
                }
                rows.push_back(std::move(row));
            }
        }
        return NormalizeOwnedRows(schema, collection_id, std::move(rows));
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

} // namespace glamdb::detail
