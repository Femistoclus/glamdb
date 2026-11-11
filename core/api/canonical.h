#pragma once

#include "storage/types.h"

#include <array>
#include <bit>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace glamdb::detail {

struct NormalizedInput
{
    std::vector<Row> rows;
    std::string payload_hash;
    size_t decoded_bytes = 0;
    // Built during backend admission and retained until this append finishes.
    std::shared_ptr<arrow::RecordBatch> writer_batch;
};

// Fingerprinting uses fixed-size state, independent of owned payload admission.
class Sha256State
{
public:
    void Update(std::string_view bytes);
    std::string Digest() const;

private:
    void Transform();
    std::array<uint32_t, 8>
        hash_{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
    std::array<char, 64> block_{};
    size_t used_ = 0;
    uint64_t bytes_ = 0;
};

// Streams canonical bytes into SHA-256 through a fixed buffer: the digest is
// the hash of exactly the bytes Encoder would have produced, without
// materializing them or updating the hash once per tiny field.
class CanonicalHasher
{
public:
    template <class T>
    void Number(T value)
    {
        using U = std::make_unsigned_t<T>;
        if (sizeof(T) > buffer_.size() - used_) {
            Flush();
        }
        // Little-endian, as Encoder::Number; a local cursor because char
        // stores may alias the members.
        auto* out = buffer_.data() + used_;
        if constexpr (std::endian::native == std::endian::little) {
            std::memcpy(out, &value, sizeof(T));
        } else {
            auto bits = static_cast<uint64_t>(std::bit_cast<U>(value));
            for (size_t i = 0; i < sizeof(T); ++i) {
                out[i] = static_cast<char>(bits & 255);
                bits >>= 8;
            }
        }
        used_ += sizeof(T);
        bytes_ += sizeof(T);
    }
    void Bytes(std::string_view bytes);
    // Same encoding and size check as Encoder::Text.
    void Text(std::string_view text);
    // Canonical bytes passed so far.
    uint64_t Size() const
    {
        return bytes_;
    }
    std::string Digest();

private:
    void Flush();
    Sha256State hash_;
    std::array<char, 4096> buffer_{};
    size_t used_ = 0;
    uint64_t bytes_ = 0;
};

bool ValidUuid(std::string_view value);
bool ValidUtf8(std::string_view value);
std::string BoundedDiagnostic(std::string_view message);
bool ValidDigest(std::string_view value);
std::string Sha256(std::string_view input);
Result<std::string> EncodeAppendHeader(const SchemaSpec& schema, std::string_view collection_id, size_t rows);
Result<std::string> EncodeSchema(const SchemaSpec& schema);
Result<SchemaSpec> DecodeSchema(std::string_view bytes);
Result<std::string> SchemaFingerprint(const SchemaSpec& schema);
Result<NormalizedInput>
NormalizeRows(const SchemaSpec& schema, std::string_view collection_id, const std::vector<Row>& rows);
// Normalizes rows the caller already owns by moving them: no second copy.
Result<NormalizedInput>
NormalizeOwnedRows(const SchemaSpec& schema, std::string_view collection_id, std::vector<Row>&& rows);
Result<NormalizedInput> NormalizeBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
Result<std::string> FingerprintBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
Result<size_t> InputReservation(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

} // namespace glamdb::detail
