#pragma once

#include "storage/types.h"

#include <array>
#include <functional>
#include <optional>
#include <string_view>

namespace arrow {
class MemoryPool;
}

namespace glamdb::detail {

struct TBulkInput;
struct TKvInput;

struct NormalizedInput
{
    // Owned input in one of two forms. Rows: NormalizeRows/NormalizeOwnedRows.
    std::vector<Row> rows;
    // Columns (rows stays empty): NormalizeBatches. One batch in schema order
    // (MakeArrowSchema) holding the same canonical values MakeRecordBatch
    // would build from the normalized rows.
    std::shared_ptr<arrow::RecordBatch> Columns;
    std::string payload_hash;
    size_t decoded_bytes = 0;
    // Built during backend admission and retained until this append finishes.
    std::shared_ptr<arrow::RecordBatch> writer_batch;
    std::shared_ptr<TBulkInput> bulk{};
    std::shared_ptr<TKvInput> kv{};

    size_t GetRowCount() const;
};

struct TBulkInput
{
    int64_t Rows = 0;
    // GetRowCount() == 0 means validated EOF. Each other result is one
    // independently bounded chunk, with no cross-chunk row accumulation.
    std::function<Result<NormalizedInput>()> Next;
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
class TCanonicalHasher
{
public:
    // Little-endian, as Encoder::Number.
    template <typename T>
    void AddNumber(T value);
    void AddBytes(std::string_view bytes);
    // Same encoding and size check as Encoder::Text.
    void AddText(std::string_view text);
    // Canonical bytes passed so far.
    uint64_t GetSize() const;
    std::string ComputeDigest();

private:
    Sha256State Hash_;
    std::array<char, 4096> Buffer_{};
    size_t Used_ = 0;
    uint64_t Bytes_ = 0;

    void Flush();
};

bool ValidUuid(std::string_view value);
bool ValidUtf8(std::string_view value);
std::string BoundedDiagnostic(std::string_view message);
bool ValidDigest(std::string_view value);
std::string Sha256(std::string_view input);
Result<std::string> EncodeAppendHeader(const SchemaSpec& schema, std::string_view collection_id, size_t rows);
Result<std::string> EncodeBulkAppendHeader(const SchemaSpec& schema, std::string_view collection_id, int64_t rows);
Result<size_t> UpdateFingerprintBatches(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    TCanonicalHasher& hash);
Result<std::string> EncodeSchema(const SchemaSpec& schema);
Result<SchemaSpec> DecodeSchema(std::string_view bytes);
Result<std::string> SchemaFingerprint(const SchemaSpec& schema);
Result<NormalizedInput>
NormalizeRows(const SchemaSpec& schema, std::string_view collection_id, const std::vector<Row>& rows);
// Normalizes rows the caller already owns by moving them: no second copy.
Result<NormalizedInput>
NormalizeOwnedRows(const SchemaSpec& schema, std::string_view collection_id, std::vector<Row>&& rows);
// Copies caller batches once into owned columns and hashes that copy.
// `reserved`, when set, is the owned-input reservation admitted for this input
// (FingerprintInput); a different shape rejects the input as changed. Owned
// buffers come from `pool` (default: Arrow's default pool).
Result<NormalizedInput> NormalizeBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    std::optional<size_t> reserved = std::nullopt,
    arrow::MemoryPool* pool = nullptr);
Result<std::string> FingerprintBatches(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

struct TFingerprintedInput
{
    std::string PayloadHash;
    // InputReservation of the same inspection.
    size_t Reservation = 0;
};

// FingerprintBatches and InputReservation from one inspection of the batches.
Result<TFingerprintedInput> FingerprintInput(const SchemaSpec& schema,
    std::string_view collection_id,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);
Result<size_t> InputReservation(const SchemaSpec& schema,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

} // namespace glamdb::detail

#define GLAMDB_CORE_API_CANONICAL_INL_H_
#include "canonical-inl.h"
#undef GLAMDB_CORE_API_CANONICAL_INL_H_
