#include "core/api/canonical.h"

#include "core/api/binary.h"
#include "core/api/scalar_validation.h"
#include "core/engine/storage_mapping.h"

#include <arrow/api.h>

#include <arrow/util/bit_util.h>

#include <cmath>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace glamdb::detail {
namespace {

bool MatchesType(const FieldSpec& field, const arrow::DataType& type)
{
    switch (field.type) {
        case FieldType::String:
            return type.id() == arrow::Type::STRING;
        case FieldType::Bool:
            return type.id() == arrow::Type::BOOL;
        case FieldType::Int64:
            return type.id() == arrow::Type::INT64;
        case FieldType::Float64:
            return type.id() == arrow::Type::DOUBLE;
        case FieldType::TimestampUsUtc:
            if (type.id() == arrow::Type::TIMESTAMP) {
                const auto& timestamp = static_cast<const arrow::TimestampType&>(type);
                return timestamp.unit() == arrow::TimeUnit::MICRO && timestamp.timezone() == "UTC";
            }
            return false;
        case FieldType::Decimal:
            if (type.id() == arrow::Type::DECIMAL128) {
                const auto& decimal = static_cast<const arrow::Decimal128Type&>(type);
                return decimal.precision() == field.precision && decimal.scale() == field.scale;
            }
            return false;
        case FieldType::Date:
            return type.id() == arrow::Type::DATE32;
    }
    return false;
}

// Resolve only immutable schema information. Array validation remains per batch.
std::vector<int> MapFields(const SchemaSpec& schema, const arrow::Schema& input)
{
    Require(input.num_fields() <= static_cast<int>(schema.fields.size()), "INVALID_ARGUMENT", "invalid Arrow batch");
    std::unordered_map<std::string_view, int> names;
    for (int i = 0; i < input.num_fields(); ++i) {
        const auto& name = input.field(i)->name();
        // Fixed-schema names are at most 128 bytes. Do not copy or hash an
        // unbounded caller name before rejecting it as unknown.
        Require(name.size() <= 128 && names.emplace(name, i).second &&
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
        Require(MatchesType(field, *input.field(found->second)->type()),
            "INVALID_ARGUMENT",
            "Arrow field type does not match fixed schema");
        columns.push_back(found->second);
    }
    return columns;
}

// One entry, not one entry per distinct batch schema: scratch stays O(fields)
// even when every batch has its own schema. Caller schemas are immutable for
// the synchronous call, and the second inspection creates a fresh mapping.
class TFieldMapping
{
public:
    explicit TFieldMapping(const SchemaSpec& schema)
        : Schema_(schema)
    { }

    const std::vector<int>& Get(const arrow::RecordBatch& batch)
    {
        if (Input_ != batch.schema()) {
            Columns_ = MapFields(Schema_, *batch.schema());
            Input_ = batch.schema();
        }
        return Columns_;
    }

private:
    const SchemaSpec& Schema_;
    std::shared_ptr<arrow::Schema> Input_;
    std::vector<int> Columns_;
};

void ValidateBatch(const SchemaSpec& schema, const arrow::RecordBatch& batch, const std::vector<int>& fields)
{
    // RecordBatch::num_columns() comes from its schema, not its ArrayData
    // vector. Check the actual data before Arrow indexes or validates it.
    Require(batch.column_data().size() == static_cast<size_t>(batch.num_columns()),
        "INVALID_ARGUMENT",
        "invalid Arrow batch");
    for (size_t field = 0; field < fields.size(); ++field) {
        if (fields[field] < 0) {
            continue;
        }
        const auto& data = batch.column_data(fields[field]);
        // Do not use DataType::Equals here: Arrow fingerprints timestamp
        // timezones and can allocate an unbounded diagnostic before refusing.
        Require(data != nullptr && data->type != nullptr && MatchesType(schema.fields[field], *data->type),
            "INVALID_ARGUMENT",
            "invalid Arrow batch");
    }
    Require(batch.ValidateFull().ok(), "INVALID_ARGUMENT", "invalid Arrow batch");
}

struct Shape
{
    size_t rows = 0;
    size_t decoded_bytes = 0;
    size_t null_slot_bytes = 0;
    std::vector<int64_t> StringBytes;
};

// Typed view of one schema field in one batch, resolved once per batch so
// per-cell loops read raw buffers instead of boxed arrow::Array accessors.
// Readers mirror the arrow::Array accessors exactly, including slice offsets.
struct TColumnView
{
    FieldType Type = FieldType::String;
    bool Nullable = true;
    int32_t Precision = 0;
    // Omitted nullable field: every value is NULL.
    bool Present = false;
    // Without a validity bitmap Array::IsNull reports null_count == length.
    bool AllNull = false;
    size_t MaxLength = 0;
    int64_t Offset = 0;
    const uint8_t* Validity = nullptr;
    // Value bitmap (bool), fixed-width values, or string offsets.
    const uint8_t* Values = nullptr;
    const char* Data = nullptr;
    int64_t DataBytes = 0;

    bool IsNull(int64_t row) const
    {
        if (!Present) {
            return true;
        }
        return Validity != nullptr ? !arrow::bit_util::GetBit(Validity, Offset + row) : AllNull;
    }
    template <typename T>
    T GetValue(int64_t row) const
    {
        T value;
        std::memcpy(&value, Values + (Offset + row) * static_cast<int64_t>(sizeof(T)), sizeof(T));
        return value;
    }
    bool GetBool(int64_t row) const
    {
        return arrow::bit_util::GetBit(Values, Offset + row);
    }
    int32_t GetBegin(int64_t row) const
    {
        return GetValue<int32_t>(row);
    }
    int32_t GetLength(int64_t row) const
    {
        return GetValue<int32_t>(row + 1) - GetValue<int32_t>(row);
    }
    std::string_view GetText(int64_t row) const
    {
        return {Data + GetBegin(row), static_cast<size_t>(GetLength(row))};
    }
};

std::vector<TColumnView>
ResolveColumns(const SchemaSpec& schema, const arrow::RecordBatch& batch, const std::vector<int>& mapping)
{
    std::vector<TColumnView> columns;
    columns.reserve(mapping.size());
    for (size_t field = 0; field < mapping.size(); ++field) {
        TColumnView column;
        column.Type = schema.fields[field].type;
        column.Nullable = schema.fields[field].nullable;
        column.Precision = schema.fields[field].precision;
        column.MaxLength = schema.fields[field].name == "key" ? 1024 : 1024 * 1024;
        if (mapping[field] >= 0) {
            const auto& data = *batch.column_data(mapping[field]);
            const auto buffer = [&data](size_t index) -> const uint8_t* {
                return index < data.buffers.size() && data.buffers[index] != nullptr ? data.buffers[index]->data()
                                                                                     : nullptr;
            };
            column.Present = true;
            column.Offset = data.offset;
            column.Validity = buffer(0);
            column.AllNull = column.Validity == nullptr && data.null_count == data.length;
            column.Values = buffer(1);
            column.Data = reinterpret_cast<const char*>(buffer(2));
            column.DataBytes = column.Data == nullptr ? 0 : data.buffers[2]->size();
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
    Shape shape{.StringBytes = std::vector<int64_t>(schema.fields.size())};
    TFieldMapping mapping(schema);
    for (const auto& batch : batches) {
        Require(batch != nullptr && batch->num_rows() >= 0 && batch->num_rows() <= 10000 &&
                shape.rows + static_cast<size_t>(batch->num_rows()) <= 10000,
            "INVALID_ARGUMENT",
            "append requires at most 10000 rows");
        shape.rows += static_cast<size_t>(batch->num_rows());
        const auto& fields = mapping.Get(*batch);
        ValidateBatch(schema, *batch, fields);
        const auto columns = ResolveColumns(schema, *batch, fields);
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            size_t bytes = schema.fields.size();
            for (size_t field = 0; field < columns.size(); ++field) {
                const auto& column = columns[field];
                if (column.IsNull(row)) {
                    Require(column.Nullable, "INVALID_ARGUMENT", "NULL in required Arrow field");
                    shape.null_slot_bytes += column.Type == FieldType::Decimal               ? 16
                        : column.Type == FieldType::String || column.Type == FieldType::Date ? 4
                        : column.Type == FieldType::Bool                                     ? 1
                                                                                             : 8;
                    continue;
                }
                if (column.Type == FieldType::String) {
                    const auto length = column.GetLength(row);
                    Require(static_cast<size_t>(length) <= column.MaxLength,
                        "INVALID_ARGUMENT",
                        "oversized Arrow string");
                    bytes += 4 + static_cast<size_t>(length);
                    shape.StringBytes[field] += length;
                } else {
                    bytes += column.Type == FieldType::Decimal ? 16
                        : column.Type == FieldType::Date       ? 4
                        : column.Type == FieldType::Bool       ? 1
                                                               : 8;
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
void HashRows(TCanonicalHasher& hash, const std::vector<TColumnView>& columns, int64_t rows)
{
    for (int64_t row = 0; row < rows; ++row) {
        for (const auto& column : columns) {
            if (column.IsNull(row)) {
                Require(column.Nullable, "INVALID_ARGUMENT", "NULL in required field");
                hash.AddNumber(uint8_t{0});
                continue;
            }
            hash.AddNumber(uint8_t{1});
            switch (column.Type) {
                case FieldType::String: {
                    const auto value = column.GetText(row);
                    Require(value.size() <= column.MaxLength && ValidUtf8(value),
                        "INVALID_ARGUMENT",
                        "invalid UTF-8 string");
                    hash.AddNumber(static_cast<uint32_t>(value.size()));
                    hash.AddBytes(value);
                    break;
                }
                case FieldType::Bool:
                    hash.AddNumber(static_cast<uint8_t>(column.GetBool(row) ? 1 : 0));
                    break;
                case FieldType::Int64:
                case FieldType::TimestampUsUtc: {
                    const auto value = column.GetValue<int64_t>(row);
                    Require(column.Type == FieldType::Int64 ||
                            (value >= -62135596800000000LL && value <= 253402300799999999LL),
                        "INVALID_ARGUMENT",
                        "timestamp outside years 0001..9999");
                    hash.AddNumber(value);
                    break;
                }
                case FieldType::Float64: {
                    auto value = column.GetValue<double>(row);
                    Require(std::isfinite(value), "INVALID_ARGUMENT", "non-finite float");
                    if (value == 0) {
                        value = 0.0;
                    }
                    hash.AddNumber(std::bit_cast<uint64_t>(value));
                    break;
                }
                case FieldType::Decimal: {
                    const arrow::Decimal128 value(column.Values + (column.Offset + row) * 16);
                    Require(FitsDecimalPrecision(value, column.Precision),
                        "INVALID_ARGUMENT",
                        "decimal coefficient exceeds field precision");
                    hash.AddNumber(value.low_bits());
                    hash.AddNumber(value.high_bits());
                    break;
                }
                case FieldType::Date: {
                    const auto value = column.GetValue<int32_t>(row);
                    Require(value >= -719162 && value <= 2932896, "INVALID_ARGUMENT", "date outside years 0001..9999");
                    hash.AddNumber(value);
                    break;
                }
            }
        }
    }
}

// Caller buffers differ from what was inspected or fingerprinted.
struct TInputChanged
{ };

ErrorDetail MakeInputChangedError()
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
void CopyRows(TOwnedColumn& owned, const TColumnView& column, FieldType type, int64_t first, int64_t count)
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
                const auto begin = column.GetBegin(source);
                const auto length = column.GetLength(source);
                if (begin < 0 || length < 0 || length > owned.DataBytes - owned.Written ||
                    static_cast<int64_t>(begin) + length > column.DataBytes)
                {
                    throw TInputChanged{};
                }
                std::memcpy(owned.Data->mutable_data() + owned.Written,
                    column.Data + begin,
                    static_cast<size_t>(length));
                owned.Written += length;
                reinterpret_cast<int32_t*>(values)[row + 1] = owned.Written;
                break;
            }
            case FieldType::Bool:
                if (column.GetBool(source)) {
                    arrow::bit_util::SetBit(values, row);
                }
                break;
            case FieldType::Int64:
            case FieldType::Float64:
            case FieldType::TimestampUsUtc: {
                auto value = column.GetValue<int64_t>(source);
                if (type == FieldType::Float64 && std::bit_cast<double>(value) == 0) {
                    value = std::bit_cast<int64_t>(0.0);
                }
                std::memcpy(values + row * 8, &value, 8);
                break;
            }
            case FieldType::Decimal:
                std::memcpy(values + row * 16, column.Values + (column.Offset + source) * 16, 16);
                break;
            case FieldType::Date: {
                const auto value = column.GetValue<int32_t>(source);
                std::memcpy(values + row * 4, &value, 4);
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
    const Shape& shape,
    arrow::MemoryPool* pool)
{
    const auto rows = static_cast<int64_t>(shape.rows);
    std::vector<TOwnedColumn> owned(schema.fields.size());
    for (size_t field = 0; field < owned.size(); ++field) {
        auto& column = owned[field];
        column.DataBytes = shape.StringBytes[field];
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
            case FieldType::Decimal:
                column.Values = Allocate(rows * 16, pool);
                break;
            case FieldType::Date:
                column.Values = Allocate(rows * 4, pool);
                break;
        }
    }
    int64_t first = 0;
    TFieldMapping mapping(schema);
    for (const auto& batch : batches) {
        const auto columns = ResolveColumns(schema, *batch, mapping.Get(*batch));
        for (size_t field = 0; field < columns.size(); ++field) {
            CopyRows(owned[field], columns[field], schema.fields[field].type, first, batch->num_rows());
        }
        first += batch->num_rows();
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(owned.size());
    for (size_t field = 0; field < owned.size(); ++field) {
        auto& column = owned[field];
        std::vector<std::shared_ptr<arrow::Buffer>> buffers{column.NullCount == 0 ? nullptr
                                                                                  : std::move(column.Validity),
            std::move(column.Values)};
        if (column.Data != nullptr) {
            buffers.push_back(std::move(column.Data));
        }
        column.Validity.reset();
        arrays.push_back(arrow::MakeArray(
            arrow::ArrayData::Make(MakeArrowType(schema.fields[field]), rows, std::move(buffers), column.NullCount)));
    }
    return arrow::RecordBatch::Make(MakeArrowSchema(schema), rows, std::move(arrays));
}

size_t ComputeReservation(const SchemaSpec& schema, const Shape& shape)
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
    TCanonicalHasher hash;
    hash.AddBytes(*header);
    TFieldMapping mapping(schema);
    for (const auto& batch : batches) {
        HashRows(hash, ResolveColumns(schema, *batch, mapping.Get(*batch)), batch->num_rows());
    }
    return hash.ComputeDigest();
}

} // namespace

Result<size_t> InputReservation(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        return ComputeReservation(schema, Inspect(schema, batches));
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
    return result->PayloadHash;
}

Result<TFingerprintedInput> FingerprintInput(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
{
    try {
        const auto shape = Inspect(schema, batches);
        return TFingerprintedInput{Fingerprint(schema, collection_id, batches, shape),
            ComputeReservation(schema, shape)};
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<size_t> UpdateFingerprintBatches(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    TCanonicalHasher& hash)
{
    try {
        const auto shape = Inspect(schema, batches);
        std::unordered_set<const arrow::Buffer*> retained;
        size_t retainedBytes = 0;
        for (const auto& batch : batches) {
            for (const auto& column : batch->columns()) {
                for (auto buffer : column->data()->buffers) {
                    size_t depth = 0;
                    while (buffer && buffer->parent()) {
                        Require(++depth <= 64, "INPUT_TOO_LARGE", "Arrow buffer parent chain exceeds its limit");
                        buffer = buffer->parent();
                    }
                    if (!buffer || !retained.insert(buffer.get()).second) {
                        continue;
                    }
                    Require(buffer->capacity() >= 0 &&
                            static_cast<uint64_t>(buffer->capacity()) <=
                                static_cast<uint64_t>(BulkAppendLimits::MaxFileInputBytes) - retainedBytes,
                        "INPUT_TOO_LARGE",
                        "bulk source retains more than 16 MiB of Arrow buffers");
                    retainedBytes += static_cast<size_t>(buffer->capacity());
                }
            }
        }
        TFieldMapping mapping(schema);
        for (const auto& batch : batches) {
            HashRows(hash, ResolveColumns(schema, *batch, mapping.Get(*batch)), batch->num_rows());
        }
        return shape.decoded_bytes;
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
        if (reserved && ComputeReservation(schema, shape) != *reserved) {
            return std::unexpected(MakeInputChangedError());
        }
        // The only copy of caller memory: canonical owned columns. Every
        // later check, the hash and the writer read this copy, never the
        // caller's buffers again.
        const auto rows = static_cast<int64_t>(shape.rows);
        NormalizedInput result;
        result.Columns = OwnBatches(schema, batches, shape, pool);
        std::vector<int> identity(schema.fields.size());
        for (size_t field = 0; field < identity.size(); ++field) {
            identity[field] = static_cast<int>(field);
        }
        const auto header = EncodeAppendHeader(schema, collection_id, shape.rows);
        if (!header) {
            throw Failure(header.error());
        }
        TCanonicalHasher hash;
        hash.AddBytes(*header);
        HashRows(hash, ResolveColumns(schema, *result.Columns, identity), rows);
        result.decoded_bytes = hash.GetSize() - header->size();
        Require(result.decoded_bytes <= 16ULL * 1024 * 1024, "INPUT_TOO_LARGE", "append exceeds decoded input limit");
        result.payload_hash = hash.ComputeDigest();
        return result;
    } catch (const TInputChanged&) {
        return std::unexpected(MakeInputChangedError());
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

size_t NormalizedInput::GetRowCount() const
{
    return Columns != nullptr ? static_cast<size_t>(Columns->num_rows()) : rows.size();
}

} // namespace glamdb::detail
