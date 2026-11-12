#include "core/api/canonical.h"

#include "core/api/binary.h"
#include "core/engine/storage_mapping.h"

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
    int64_t data_bytes = 0;

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
            column.data_bytes = column.data == nullptr ? 0 : data.buffers[2]->size();
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
                    shape.null_slot_bytes += column.type == FieldType::String ? 4
                        : column.type == FieldType::Bool                      ? 1
                                                                              : 8;
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

// Caller buffers differ from what was inspected or fingerprinted.
struct InputChanged
{ };

ErrorDetail InputChangedError()
{
    // The same error as the coordinator's hash comparison.
    return {"INVALID_ARGUMENT", "Arrow input changed during append", RetryHint::OperatorAction};
}

[[noreturn]] void ThrowArrow(const arrow::Status& status)
{
    if (status.IsOutOfMemory()) {
        throw std::bad_alloc();
    }
    throw Failure(
        {"INTERNAL", BoundedDiagnostic("owned Arrow input: " + status.ToString()), RetryHint::OperatorAction});
}

std::shared_ptr<arrow::Buffer> Allocate(int64_t bytes, arrow::MemoryPool* pool)
{
    auto buffer = arrow::AllocateBuffer(bytes, pool);
    if (!buffer.ok()) {
        ThrowArrow(buffer.status());
    }
    // Padding and NULL slots are deterministic zeros.
    std::memset((*buffer)->mutable_data(), 0, static_cast<size_t>((*buffer)->capacity()));
    return std::shared_ptr<arrow::Buffer>(std::move(*buffer));
}

// Output buffers of one owned column, filled batch by batch.
struct TOwnedColumn
{
    std::shared_ptr<arrow::Buffer> Validity;
    // Fixed-width values, the value bitmap (bool) or string offsets.
    std::shared_ptr<arrow::Buffer> Values;
    std::shared_ptr<arrow::Buffer> Data;
    int64_t NullCount = 0;
    // String bytes counted in the first pass, and copied so far.
    int64_t DataBytes = 0;
    int32_t Written = 0;
};

// Appends rows [0, count) of one batch's column at `first` of the owned column.
void CopyRows(TOwnedColumn& owned, const Column& column, FieldType type, int64_t first, int64_t count)
{
    auto* valid = owned.Validity->mutable_data();
    auto* values = owned.Values->mutable_data();
    for (int64_t source = 0; source < count; ++source) {
        const auto row = first + source;
        if (column.IsNull(source)) {
            ++owned.NullCount;
            if (type == FieldType::String) {
                reinterpret_cast<int32_t*>(values)[row + 1] = owned.Written;
            }
            continue;
        }
        arrow::bit_util::SetBit(valid, row);
        switch (type) {
            case FieldType::String: {
                const auto begin = column.Begin(source);
                const auto length = column.Length(source);
                if (begin < 0 || length < 0 || length > owned.DataBytes - owned.Written ||
                    static_cast<int64_t>(begin) + length > column.data_bytes)
                {
                    throw InputChanged{};
                }
                std::memcpy(owned.Data->mutable_data() + owned.Written, column.data + begin, static_cast<size_t>(length));
                owned.Written += length;
                reinterpret_cast<int32_t*>(values)[row + 1] = owned.Written;
                break;
            }
            case FieldType::Bool:
                if (column.Bool(source)) {
                    arrow::bit_util::SetBit(values, row);
                }
                break;
            case FieldType::Int64:
            case FieldType::Float64:
            case FieldType::TimestampUsUtc: {
                auto value = column.Value<int64_t>(source);
                if (type == FieldType::Float64 && std::bit_cast<double>(value) == 0) {
                    value = std::bit_cast<int64_t>(0.0);
                }
                std::memcpy(values + row * 8, &value, 8);
                break;
            }
        }
    }
}

// Owned columns of every batch in schema form: no slice offset, a validity
// bitmap only when a value is NULL (as Arrow builders produce), zeroed NULL
// slots, +0.0 for -0.0 and no string bytes behind NULLs. Batches are visited
// one at a time with one batch's column views as scratch, so C++ scratch is
// O(fields) whatever the batch count. Every string range is checked against
// its source buffer and the bytes counted for it in the first pass, so input
// that changes after inspection cannot make the copy read or write out of
// bounds; it only fails the hash comparison afterwards.
std::shared_ptr<arrow::RecordBatch> OwnBatches(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    int64_t rows,
    arrow::MemoryPool* pool)
{
    std::vector<TOwnedColumn> owned(schema.fields.size());
    for (const auto& batch : batches) {
        const auto columns = Columns(schema, *batch, MapFields(schema, *batch, true));
        for (size_t field = 0; field < columns.size(); ++field) {
            if (columns[field].type != FieldType::String) {
                continue;
            }
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                if (!columns[field].IsNull(row)) {
                    owned[field].DataBytes += columns[field].Length(row);
                }
            }
            if (owned[field].DataBytes < 0 || owned[field].DataBytes > 16LL * 1024 * 1024) {
                throw InputChanged{};
            }
        }
    }
    for (size_t field = 0; field < owned.size(); ++field) {
        auto& column = owned[field];
        column.Validity = Allocate(arrow::bit_util::BytesForBits(rows), pool);
        switch (schema.fields[field].type) {
            case FieldType::String:
                column.Values = Allocate((rows + 1) * static_cast<int64_t>(sizeof(int32_t)), pool);
                column.Data = Allocate(column.DataBytes, pool);
                break;
            case FieldType::Bool:
                column.Values = Allocate(arrow::bit_util::BytesForBits(rows), pool);
                break;
            case FieldType::Int64:
            case FieldType::Float64:
            case FieldType::TimestampUsUtc:
                column.Values = Allocate(rows * 8, pool);
                break;
        }
    }
    int64_t first = 0;
    for (const auto& batch : batches) {
        const auto columns = Columns(schema, *batch, MapFields(schema, *batch, true));
        for (size_t field = 0; field < columns.size(); ++field) {
            CopyRows(owned[field], columns[field], schema.fields[field].type, first, batch->num_rows());
        }
        first += batch->num_rows();
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(owned.size());
    for (size_t field = 0; field < owned.size(); ++field) {
        auto& column = owned[field];
        std::vector<std::shared_ptr<arrow::Buffer>> buffers{
            column.NullCount == 0 ? nullptr : std::move(column.Validity), std::move(column.Values)};
        if (column.Data != nullptr) {
            buffers.push_back(std::move(column.Data));
        }
        column.Validity.reset();
        arrays.push_back(arrow::MakeArray(arrow::ArrayData::Make(
            MakeArrowType(schema.fields[field].type), rows, std::move(buffers), column.NullCount)));
    }
    return arrow::RecordBatch::Make(MakeArrowSchema(schema), rows, std::move(arrays));
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
    std::optional<size_t> reserved,
    arrow::MemoryPool* pool)
{
    try {
        // Inspected again: these are caller buffers, and this pass validates
        // exactly the memory the owned copy reads. A shape that differs from
        // the admitted reservation means the caller changed the input.
        const auto shape = Inspect(schema, batches);
        if (reserved && Reservation(schema, shape) != *reserved) {
            return std::unexpected(InputChangedError());
        }
        // The only copy of caller memory: canonical owned columns. Every
        // later check, the hash and the writer read this copy, never the
        // caller's buffers again.
        const auto rows = static_cast<int64_t>(shape.rows);
        NormalizedInput result;
        result.columns = OwnBatches(schema, batches, rows, pool);
        std::vector<int> identity(schema.fields.size());
        for (size_t field = 0; field < identity.size(); ++field) {
            identity[field] = static_cast<int>(field);
        }
        const auto header = EncodeAppendHeader(schema, collection_id, shape.rows);
        if (!header) {
            throw Failure(header.error());
        }
        CanonicalHasher hash;
        hash.Bytes(*header);
        HashRows(hash, Columns(schema, *result.columns, identity), rows);
        result.decoded_bytes = hash.Size() - header->size();
        Require(result.decoded_bytes <= 16ULL * 1024 * 1024, "INPUT_TOO_LARGE", "append exceeds decoded input limit");
        result.payload_hash = hash.Digest();
        return result;
    } catch (const InputChanged&) {
        return std::unexpected(InputChangedError());
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

size_t NormalizedInput::RowCount() const
{
    return columns != nullptr ? static_cast<size_t>(columns->num_rows()) : rows.size();
}

} // namespace glamdb::detail
