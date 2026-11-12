#include "core/api/canonical.h"

#include "core/api/binary.h"
#include "core/api/scalar_validation.h"
#include "core/io/sha256_blocks.h"

#include <arrow/util/decimal.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <type_traits>
#include <unordered_set>
#include <utility>

namespace glamdb::detail {
namespace {

constexpr size_t MaxFieldNameBytes = 128;
// Magic, version, count, then ID/name/type/nullability and optional decimal parameters.
constexpr size_t MaxEncodedSchemaBytes = 20 + MaxSchemaFields * (4 + 2 + MaxFieldNameBytes + 1 + 1 + 2);

int Hex(char c)
{
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    return -1;
}

std::string Unhex(std::string_view text)
{
    std::string bytes;
    for (size_t i = 0; i < text.size(); i += 2) {
        bytes.push_back(static_cast<char>(Hex(text[i]) * 16 + Hex(text[i + 1])));
    }
    return bytes;
}

void ValidateSchema(const SchemaSpec& schema)
{
    Require(!schema.fields.empty() && schema.fields.size() <= MaxSchemaFields,
        "INVALID_ARGUMENT",
        "schema requires 1..128 fields");
    std::unordered_set<std::string> names;
    for (const auto& field : schema.fields) {
        Require(!field.name.empty() && field.name.size() <= MaxFieldNameBytes && ValidUtf8(field.name) &&
                field.name.find('\0') == std::string::npos && names.insert(field.name).second,
            "INVALID_ARGUMENT",
            "invalid or duplicate field name");
        Require(field.type >= FieldType::String && field.type <= FieldType::Date,
            "INVALID_ARGUMENT",
            "unsupported field type");
        Require(field.type == FieldType::Decimal
                ? field.precision >= 1 && field.precision <= 38 && field.scale >= 0 && field.scale <= field.precision
                : field.precision == 0 && field.scale == 0,
            "INVALID_ARGUMENT",
            "invalid decimal precision or scale");
        Require(field.name != "key" || (field.type == FieldType::String && !field.nullable),
            "INVALID_ARGUMENT",
            "key must be a required string");
    }
}

void EncodeCell(TCanonicalHasher& encoder, const FieldSpec& field, Cell& cell)
{
    if (std::holds_alternative<std::monostate>(cell)) {
        Require(field.nullable, "INVALID_ARGUMENT", "NULL in required field");
        encoder.AddNumber<uint8_t>(0);
        return;
    }
    Require(cell.index() == static_cast<size_t>(field.type), "INVALID_ARGUMENT", "value has the wrong type");
    encoder.AddNumber<uint8_t>(1);
    switch (field.type) {
        case FieldType::String: {
            const auto& value = std::get<std::string>(cell);
            Require(value.size() <= (field.name == "key" ? 1024U : 1024U * 1024) && ValidUtf8(value),
                "INVALID_ARGUMENT",
                "invalid UTF-8 or oversized string");
            encoder.AddText(value);
            break;
        }
        case FieldType::Bool:
            encoder.AddNumber<uint8_t>(std::get<bool>(cell) ? 1 : 0);
            break;
        case FieldType::Int64:
            encoder.AddNumber(std::get<int64_t>(cell));
            break;
        case FieldType::Float64: {
            auto& value = std::get<double>(cell);
            Require(std::isfinite(value), "INVALID_ARGUMENT", "non-finite float");
            if (value == 0) {
                value = 0.0;
            }
            encoder.AddNumber(std::bit_cast<uint64_t>(value));
            break;
        }
        case FieldType::TimestampUsUtc: {
            const auto value = std::get<TimestampUs>(cell).value;
            Require(value >= -62135596800000000LL && value <= 253402300799999999LL,
                "INVALID_ARGUMENT",
                "timestamp outside years 0001..9999");
            encoder.AddNumber(value);
            break;
        }
        case FieldType::Decimal: {
            const auto& value = std::get<Decimal>(cell);
            Require(value.precision == field.precision && value.scale == field.scale &&
                    FitsDecimalPrecision(arrow::Decimal128(value.coefficient_high, value.coefficient_low),
                        field.precision),
                "INVALID_ARGUMENT",
                "decimal coefficient or parameters do not match schema");
            encoder.AddNumber(value.coefficient_low);
            encoder.AddNumber(value.coefficient_high);
            break;
        }
        case FieldType::Date: {
            const auto value = std::get<Date>(cell).days_since_epoch;
            Require(value >= -719162 && value <= 2932896, "INVALID_ARGUMENT", "date outside years 0001..9999");
            encoder.AddNumber(value);
            break;
        }
    }
}

} // namespace

bool ValidUuid(std::string_view value)
{
    if (value.size() != 36) {
        return false;
    }
    for (size_t i = 0; i < value.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (value[i] != '-') {
                return false;
            }
        } else if (Hex(value[i]) < 0) {
            return false;
        }
    }
    return true;
}

bool ValidDigest(std::string_view value)
{
    return value.size() == 64 && std::ranges::all_of(value, [](char c) {
        return Hex(c) >= 0;
    });
}

bool ValidUtf8(std::string_view value)
{
    for (size_t i = 0; i < value.size();) {
        const auto first = static_cast<uint8_t>(value[i++]);
        if (first < 0x80) {
            continue;
        }
        int extra = first >= 0xf0 ? 3 : first >= 0xe0 ? 2 : first >= 0xc2 ? 1 : -1;
        if (extra < 0 || first > 0xf4 || i + static_cast<size_t>(extra) > value.size()) {
            return false;
        }
        uint32_t point = first & (0x7fU >> (extra + 1));
        for (int j = 0; j < extra; ++j) {
            const auto next = static_cast<uint8_t>(value[i++]);
            if ((next & 0xc0U) != 0x80U) {
                return false;
            }
            point = (point << 6) | (next & 0x3fU);
        }
        const uint32_t minimum = extra == 1 ? 0x80 : extra == 2 ? 0x800 : 0x10000;
        if (point < minimum || point > 0x10ffff || (point >= 0xd800 && point <= 0xdfff)) {
            return false;
        }
    }
    return true;
}

std::string BoundedDiagnostic(std::string_view message)
{
    message = message.substr(0, 2048);
    size_t bytes = 0;
    while (bytes < message.size()) {
        const auto first = static_cast<uint8_t>(message[bytes]);
        const size_t width = first < 0x80 ? 1 : first < 0xe0 ? 2 : first < 0xf0 ? 3 : 4;
        if (width > message.size() - bytes || !ValidUtf8(message.substr(bytes, width))) {
            break;
        }
        bytes += width;
    }
    return std::string(message.substr(0, bytes));
}

void Sha256State::Transform()
{
    NGlamDB::NIO::NSha256::CompressBlocks(hash_.data(), reinterpret_cast<const unsigned char*>(block_.data()), 1);
}

void Sha256State::Update(std::string_view input)
{
    bytes_ += input.size();
    if (used_ != 0) {
        const auto count = std::min(block_.size() - used_, input.size());
        std::copy_n(input.data(), count, block_.data() + used_);
        used_ += count;
        input.remove_prefix(count);
        if (used_ != block_.size()) {
            return;
        }
        Transform();
        used_ = 0;
    }
    if (const auto blocks = input.size() / block_.size(); blocks != 0) {
        NGlamDB::NIO::NSha256::CompressBlocks(hash_.data(),
            reinterpret_cast<const unsigned char*>(input.data()),
            blocks);
        input.remove_prefix(blocks * block_.size());
    }
    std::copy_n(input.data(), input.size(), block_.data());
    used_ = input.size();
}

std::string Sha256State::Digest() const
{
    auto final = *this;
    const uint64_t bits = bytes_ * 8;
    final.Update(std::string_view("\x80", 1));
    while (final.used_ != 56) {
        final.Update(std::string_view("\0", 1));
    }
    std::array<char, 8> length{};
    for (size_t i = 0; i < length.size(); ++i) {
        length[i] = static_cast<char>((bits >> (56 - i * 8)) & 255U);
    }
    final.Update(std::string_view(length.data(), length.size()));
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result;
    result.reserve(64);
    for (auto word : final.hash_) {
        for (int shift = 28; shift >= 0; shift -= 4) {
            result.push_back(digits[(word >> shift) & 15]);
        }
    }
    return result;
}

void TCanonicalHasher::Flush()
{
    Hash_.Update(std::string_view(Buffer_.data(), Used_));
    Used_ = 0;
}

void TCanonicalHasher::AddBytes(std::string_view bytes)
{
    Bytes_ += bytes.size();
    if (bytes.size() > Buffer_.size() - Used_) {
        Flush();
        if (bytes.size() >= Buffer_.size()) {
            Hash_.Update(bytes);
            return;
        }
    }
    std::copy_n(bytes.data(), bytes.size(), Buffer_.data() + Used_);
    Used_ += bytes.size();
}

void TCanonicalHasher::AddText(std::string_view text)
{
    Require(text.size() <= 16ULL * 1024 * 1024, "RESOURCE_EXHAUSTED", "encoded string too large");
    AddNumber(static_cast<uint32_t>(text.size()));
    AddBytes(text);
}

std::string TCanonicalHasher::ComputeDigest()
{
    Flush();
    return Hash_.Digest();
}

std::string Sha256(std::string_view input)
{
    Sha256State hash;
    hash.Update(input);
    return hash.Digest();
}

Result<std::string> EncodeSchema(const SchemaSpec& schema)
{
    try {
        ValidateSchema(schema);
        Encoder out;
        out.bytes.assign("COLUMNAR-SCHEMA\0", 16);
        const bool extended = std::ranges::any_of(schema.fields, [](const auto& field) {
            return field.type == FieldType::Decimal || field.type == FieldType::Date;
        });
        out.Number<uint16_t>(extended ? 2 : 1);
        out.Number(static_cast<uint16_t>(schema.fields.size()));
        uint32_t id = 1;
        for (const auto& field : schema.fields) {
            out.Number(id++);
            out.Number(static_cast<uint16_t>(field.name.size()));
            out.bytes.append(field.name);
            out.Number(static_cast<uint8_t>(field.type));
            out.Number<uint8_t>(field.nullable ? 1 : 0);
            if (field.type == FieldType::Decimal) {
                out.Number(static_cast<uint8_t>(field.precision));
                out.Number(static_cast<uint8_t>(field.scale));
            }
        }
        return out.bytes;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<SchemaSpec> DecodeSchema(std::string_view bytes)
{
    try {
        Require(bytes.size() <= MaxEncodedSchemaBytes && bytes.starts_with(std::string_view("COLUMNAR-SCHEMA\0", 16)),
            "CORRUPTION",
            "invalid schema encoding");
        Decoder in{bytes.substr(16)};
        const auto version = in.Number<uint16_t>();
        Require(version == 1 || version == 2, "UNSUPPORTED_STORE_VERSION", "unknown schema version");
        const auto count = in.Number<uint16_t>();
        Require(count > 0 && count <= MaxSchemaFields, "CORRUPTION", "invalid schema field count");
        SchemaSpec schema;
        for (uint32_t i = 1; i <= count; ++i) {
            Require(in.Number<uint32_t>() == i, "CORRUPTION", "invalid field ID");
            const auto length = in.Number<uint16_t>();
            Require(length <= bytes.size() - in.position - 16, "CORRUPTION", "truncated field name");
            auto name = std::string(in.bytes.substr(in.position, length));
            in.position += length;
            const auto type = static_cast<FieldType>(in.Number<uint8_t>());
            const auto nullable = in.Boolean();
            Require(version == 2 || type <= FieldType::TimestampUsUtc,
                "CORRUPTION",
                "extended scalar in legacy schema encoding");
            FieldSpec field{std::move(name), type, nullable};
            if (type == FieldType::Decimal) {
                field.precision = in.Number<uint8_t>();
                field.scale = in.Number<uint8_t>();
            }
            schema.fields.push_back(std::move(field));
        }
        in.End();
        ValidateSchema(schema);
        const auto canonical = EncodeSchema(schema);
        Require(canonical && *canonical == bytes, "CORRUPTION", "noncanonical schema encoding");
        return schema;
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    }
}

Result<std::string> SchemaFingerprint(const SchemaSpec& schema)
{
    const auto encoded = EncodeSchema(schema);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    return Sha256(*encoded);
}

Result<std::string> EncodeBulkAppendHeader(const SchemaSpec& schema, std::string_view collection_id, int64_t rows)
{
    try {
        ValidateSchema(schema);
        Require(ValidUuid(collection_id), "INVALID_ARGUMENT", "collection ID must be a canonical UUID");
        Require(rows > 0 && rows <= BulkAppendLimits::MaxRows, "INVALID_ARGUMENT", "bulk row count exceeds its limit");
        Encoder out;
        out.bytes.assign("COLUMNAR-APPEND\0", 16);
        out.Number<uint16_t>(1);
        std::string uuid;
        for (char c : collection_id) {
            if (c != '-') {
                uuid.push_back(c);
            }
        }
        out.bytes += Unhex(uuid);
        out.bytes += Unhex(*SchemaFingerprint(schema));
        out.Number(static_cast<uint64_t>(rows));
        return out.bytes;
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<std::string> EncodeAppendHeader(const SchemaSpec& schema, std::string_view collection_id, size_t rows)
{
    if (rows == 0 || rows > 10000) {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "append requires 1..10000 rows", RetryHint::FixInput});
    }
    return EncodeBulkAppendHeader(schema, collection_id, static_cast<int64_t>(rows));
}

namespace {

// Copies caller rows, or moves rows the caller already owns: either way the
// normalized rows hold the only copy of the payload cells.
template <class TRows>
Result<NormalizedInput> Normalize(const SchemaSpec& schema, std::string_view collection_id, TRows& rows)
{
    try {
        const auto header = EncodeAppendHeader(schema, collection_id, rows.size());
        if (!header) {
            throw Failure(header.error());
        }
        TCanonicalHasher out;
        out.AddBytes(*header);
        const auto header_size = out.GetSize();
        NormalizedInput result;
        result.rows.reserve(rows.size());
        for (auto& row : rows) {
            Require(row.size() == schema.fields.size(), "INVALID_ARGUMENT", "row width does not match schema");
            // Validate sizes before making an owned copy of caller memory.
            for (const auto& cell : row) {
                if (const auto* text = std::get_if<std::string>(&cell)) {
                    Require(text->size() <= 1024ULL * 1024, "INVALID_ARGUMENT", "oversized string");
                }
            }
            Row normalized;
            if constexpr (std::is_const_v<TRows>) {
                normalized = row;
            } else {
                normalized = std::move(row);
            }
            const auto before = out.GetSize();
            for (size_t i = 0; i < normalized.size(); ++i) {
                EncodeCell(out, schema.fields[i], normalized[i]);
                Require(out.GetSize() - before <= 2ULL * 1024 * 1024 &&
                        out.GetSize() - header_size <= 16ULL * 1024 * 1024,
                    "INPUT_TOO_LARGE",
                    "append exceeds decoded input limit");
            }
            result.rows.push_back(std::move(normalized));
        }
        result.decoded_bytes = out.GetSize() - header_size;
        result.payload_hash = out.ComputeDigest();
        return result;
    } catch (const Failure& error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

} // namespace

Result<NormalizedInput>
NormalizeRows(const SchemaSpec& schema, std::string_view collection_id, const std::vector<Row>& rows)
{
    return Normalize(schema, collection_id, rows);
}

Result<NormalizedInput>
NormalizeOwnedRows(const SchemaSpec& schema, std::string_view collection_id, std::vector<Row>&& rows)
{
    return Normalize(schema, collection_id, rows);
}

} // namespace glamdb::detail
