#include "core/api/canonical.h"

#include "core/api/binary.h"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <unordered_set>

namespace glamdb::detail {
namespace {

constexpr std::array<uint32_t, 64> round_constants{
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

int Hex(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    return -1;
}

std::string Unhex(std::string_view text) {
    std::string bytes;
    for (size_t i = 0; i < text.size(); i += 2) {
        bytes.push_back(static_cast<char>(Hex(text[i]) * 16 + Hex(text[i + 1])));
    }
    return bytes;
}

void ValidateSchema(const SchemaSpec &schema) {
    Require(!schema.fields.empty() && schema.fields.size() <= 64, "INVALID_ARGUMENT", "schema requires 1..64 fields");
    std::unordered_set<std::string> names;
    for (const auto &field : schema.fields) {
        Require(!field.name.empty() && field.name.size() <= 128 && ValidUtf8(field.name) &&
                    field.name.find('\0') == std::string::npos && names.insert(field.name).second,
                "INVALID_ARGUMENT", "invalid or duplicate field name");
        Require(field.type >= FieldType::String && field.type <= FieldType::TimestampUsUtc, "INVALID_ARGUMENT",
                "unsupported field type");
        Require(field.name != "key" || (field.type == FieldType::String && !field.nullable), "INVALID_ARGUMENT",
                "key must be a required string");
    }
}

void EncodeCell(Encoder &encoder, const FieldSpec &field, Cell &cell) {
    if (std::holds_alternative<std::monostate>(cell)) {
        Require(field.nullable, "INVALID_ARGUMENT", "NULL in required field");
        encoder.Number<uint8_t>(0);
        return;
    }
    Require(cell.index() == static_cast<size_t>(field.type), "INVALID_ARGUMENT", "value has the wrong type");
    encoder.Number<uint8_t>(1);
    switch (field.type) {
    case FieldType::String: {
        const auto &value = std::get<std::string>(cell);
        Require(value.size() <= (field.name == "key" ? 1024U : 1024U * 1024) && ValidUtf8(value), "INVALID_ARGUMENT",
                "invalid UTF-8 or oversized string");
        encoder.Text(value);
        break;
    }
    case FieldType::Bool:
        encoder.Number<uint8_t>(std::get<bool>(cell) ? 1 : 0);
        break;
    case FieldType::Int64:
        encoder.Number(std::get<int64_t>(cell));
        break;
    case FieldType::Float64: {
        auto &value = std::get<double>(cell);
        Require(std::isfinite(value), "INVALID_ARGUMENT", "non-finite float");
        if (value == 0) {
            value = 0.0;
        }
        encoder.Number(std::bit_cast<uint64_t>(value));
        break;
    }
    case FieldType::TimestampUsUtc: {
        const auto value = std::get<TimestampUs>(cell).value;
        Require(value >= -62135596800000000LL && value <= 253402300799999999LL, "INVALID_ARGUMENT",
                "timestamp outside years 0001..9999");
        encoder.Number(value);
        break;
    }
    }
}

} // namespace

bool ValidUuid(std::string_view value) {
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

bool ValidDigest(std::string_view value) {
    return value.size() == 64 && std::ranges::all_of(value, [](char c) { return Hex(c) >= 0; });
}

bool ValidUtf8(std::string_view value) {
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

std::string BoundedDiagnostic(std::string_view message) {
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

void Sha256State::Transform() {
    std::array<uint32_t, 64> words{};
    for (size_t i = 0; i < block_.size(); ++i) {
        words[i / 4] |= static_cast<uint32_t>(static_cast<uint8_t>(block_[i])) << ((3 - i % 4) * 8);
    }
    for (size_t i = 16; i < 64; ++i) {
        const auto a = words[i - 15];
        const auto b = words[i - 2];
        words[i] = words[i - 16] + (std::rotr(a, 7) ^ std::rotr(a, 18) ^ (a >> 3)) + words[i - 7] +
                   (std::rotr(b, 17) ^ std::rotr(b, 19) ^ (b >> 10));
    }
    auto work = hash_;
    for (size_t i = 0; i < 64; ++i) {
        const auto sum1 = std::rotr(work[4], 6) ^ std::rotr(work[4], 11) ^ std::rotr(work[4], 25);
        const auto choose = (work[4] & work[5]) ^ (~work[4] & work[6]);
        const auto temp = work[7] + sum1 + choose + round_constants[i] + words[i];
        const auto sum0 = std::rotr(work[0], 2) ^ std::rotr(work[0], 13) ^ std::rotr(work[0], 22);
        const auto majority = (work[0] & work[1]) ^ (work[0] & work[2]) ^ (work[1] & work[2]);
        for (size_t j = 7; j > 0; --j) {
            work[j] = work[j - 1];
        }
        work[4] += temp;
        work[0] = temp + sum0 + majority;
    }
    for (size_t i = 0; i < hash_.size(); ++i) {
        hash_[i] += work[i];
    }
}

void Sha256State::Update(std::string_view input) {
    bytes_ += input.size();
    while (!input.empty()) {
        const auto count = std::min(block_.size() - used_, input.size());
        std::copy_n(input.data(), count, block_.data() + used_);
        used_ += count;
        input.remove_prefix(count);
        if (used_ == block_.size()) {
            Transform();
            used_ = 0;
        }
    }
}

std::string Sha256State::Digest() const {
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

std::string Sha256(std::string_view input) {
    Sha256State hash;
    hash.Update(input);
    return hash.Digest();
}

Result<std::string> EncodeSchema(const SchemaSpec &schema) {
    try {
        ValidateSchema(schema);
        Encoder out;
        out.bytes.assign("COLUMNAR-SCHEMA\0", 16);
        out.Number<uint16_t>(1);
        out.Number(static_cast<uint16_t>(schema.fields.size()));
        uint32_t id = 1;
        for (const auto &field : schema.fields) {
            out.Number(id++);
            out.Number(static_cast<uint16_t>(field.name.size()));
            out.bytes.append(field.name);
            out.Number(static_cast<uint8_t>(field.type));
            out.Number<uint8_t>(field.nullable ? 1 : 0);
        }
        return out.bytes;
    } catch (const Failure &error) {
        return std::unexpected(error.detail);
    }
}

Result<SchemaSpec> DecodeSchema(std::string_view bytes) {
    try {
        Require(bytes.size() <= 16384 && bytes.starts_with(std::string_view("COLUMNAR-SCHEMA\0", 16)), "CORRUPTION",
                "invalid schema encoding");
        Decoder in{bytes.substr(16)};
        Require(in.Number<uint16_t>() == 1, "UNSUPPORTED_STORE_VERSION", "unknown schema version");
        const auto count = in.Number<uint16_t>();
        Require(count > 0 && count <= 64, "CORRUPTION", "invalid schema field count");
        SchemaSpec schema;
        for (uint32_t i = 1; i <= count; ++i) {
            Require(in.Number<uint32_t>() == i, "CORRUPTION", "invalid field ID");
            const auto length = in.Number<uint16_t>();
            Require(length <= bytes.size() - in.position - 16, "CORRUPTION", "truncated field name");
            auto name = std::string(in.bytes.substr(in.position, length));
            in.position += length;
            const auto type = static_cast<FieldType>(in.Number<uint8_t>());
            const auto nullable = in.Boolean();
            schema.fields.push_back({std::move(name), type, nullable});
        }
        in.End();
        ValidateSchema(schema);
        return schema;
    } catch (const Failure &error) {
        return std::unexpected(error.detail);
    }
}

Result<std::string> SchemaFingerprint(const SchemaSpec &schema) {
    const auto encoded = EncodeSchema(schema);
    if (!encoded) {
        return std::unexpected(encoded.error());
    }
    return Sha256(*encoded);
}

Result<std::string> EncodeAppendHeader(const SchemaSpec &schema, std::string_view collection_id, size_t rows) {
    try {
        ValidateSchema(schema);
        Require(ValidUuid(collection_id), "INVALID_ARGUMENT", "collection ID must be a canonical UUID");
        Require(rows != 0 && rows <= 10000, "INVALID_ARGUMENT", "append requires 1..10000 rows");
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
    } catch (const Failure &error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

Result<NormalizedInput> NormalizeRows(const SchemaSpec &schema, std::string_view collection_id,
                                      const std::vector<Row> &rows) {
    try {
        const auto header = EncodeAppendHeader(schema, collection_id, rows.size());
        if (!header) {
            throw Failure(header.error());
        }
        Encoder out;
        out.bytes = *header;
        const auto header_size = out.bytes.size();
        NormalizedInput result;
        result.rows.reserve(rows.size());
        for (const auto &row : rows) {
            Require(row.size() == schema.fields.size(), "INVALID_ARGUMENT", "row width does not match schema");
            // Validate sizes before making an owned copy of caller memory.
            for (const auto &cell : row) {
                if (const auto *text = std::get_if<std::string>(&cell)) {
                    Require(text->size() <= 1024ULL * 1024, "INVALID_ARGUMENT", "oversized string");
                }
            }
            Row normalized = row;
            const auto before = out.bytes.size();
            for (size_t i = 0; i < row.size(); ++i) {
                EncodeCell(out, schema.fields[i], normalized[i]);
                Require(out.bytes.size() - before <= 2ULL * 1024 * 1024 &&
                            out.bytes.size() - header_size <= 16ULL * 1024 * 1024,
                        "RESOURCE_EXHAUSTED", "append exceeds decoded input limit");
            }
            result.rows.push_back(std::move(normalized));
        }
        result.decoded_bytes = out.bytes.size() - header_size;
        result.payload_hash = Sha256(out.bytes);
        return result;
    } catch (const Failure &error) {
        auto detail = error.detail;
        detail.retry_hint = RetryHint::FixInput;
        return std::unexpected(std::move(detail));
    }
}

} // namespace glamdb::detail
