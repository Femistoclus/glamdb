#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace arrow {
class RecordBatch;
}

namespace glamdb {

class ResourceOwner;

enum class RetryHint : uint8_t { SameIdAfterReconcile, NewOperationId, FixInput, OperatorAction };

struct ErrorDetail
{
    std::string code;
    std::string message;
    RetryHint retry_hint = RetryHint::FixInput;
    bool operator==(const ErrorDetail&) const = default;
};

template <class T>
using Result = std::expected<T, ErrorDetail>;
using Status = Result<void>;

enum class StoreState : uint8_t { Recovering, Ready, RecoveryRequired, StorageFailed, Unavailable, Stopping, Closed };

// Point-in-time admission information, not a reservation or a durability claim.
// Availability describes new scans/writes; already retained cursors keep their
// existing lifetime contract. Resource pressure can still reject individual calls.
struct StoreHealth
{
    StoreState state = StoreState::Recovering;
    bool read_available = false;
    bool write_ready = false;
    bool storage_failure_latched = false;
    // No environment has completed the durability qualification campaign.
    bool durability_qualified = false;
};

enum class FieldType : uint8_t { String = 1, Bool, Int64, Float64, TimestampUsUtc, Decimal, Date };

struct FieldSpec
{
    std::string name;
    FieldType type = FieldType::String;
    bool nullable = false;
    int32_t precision = 0;
    int32_t scale = 0;
    bool operator==(const FieldSpec&) const = default;
};

// Field IDs are assigned in order, starting at 1, and never change.
struct SchemaSpec
{
    std::vector<FieldSpec> fields;
    bool operator==(const SchemaSpec&) const = default;
};

// Parquet value compression of the data files that appends write. Unrelated to
// HTTP content coding. Wire/footer names: uncompressed, snappy, gzip, lz4_raw
// (Parquet LZ4_RAW), zstd.
enum class ParquetCodec : uint8_t { Uncompressed = 1, Snappy, Gzip, Lz4Raw, Zstd };

struct ParquetCompression
{
    ParquetCodec codec = ParquetCodec::Zstd;
    // Absent: the codec's default level (EngineCapabilities). Levels are codec
    // specific and never inherited; codecs without levels reject one.
    std::optional<int32_t> level{};
    bool operator==(const ParquetCompression&) const = default;
};

// Auto: dictionary-encode every type that supports it (all but bool) and
// fall back to plain pages when the dictionary page outgrows its limit.
// Enabled: the same, but rejected for bool columns instead of ignored.
// Disabled: values use the column's encoding (plain unless one is set).
enum class DictionaryPolicy : uint8_t { Auto = 1, Enabled, Disabled };

// Parquet value encodings of non-dictionary pages. Wire/footer names: plain,
// delta_binary_packed, delta_length_byte_array, delta_byte_array,
// byte_stream_split, rle. Valid types: plain all; delta_binary_packed int64
// and timestamp; delta_length_byte_array and delta_byte_array string;
// byte_stream_split int64, float64 and timestamp; rle bool.
enum class ParquetEncoding : uint8_t {
    Plain = 1,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
    ByteStreamSplit,
    Rle
};

struct ColumnWriteSettings
{
    // Exact name of a schema field.
    std::string field;
    // Absent: the collection default.
    std::optional<ParquetCompression> compression{};
    // Absent: the collection policy, unless an encoding is set (an explicit
    // encoding disables the column's dictionary; Enabled with an encoding is
    // a conflict).
    std::optional<DictionaryPolicy> dictionary{};
    std::optional<ParquetEncoding> encoding{};
    bool operator==(const ColumnWriteSettings&) const = default;
};

// Settings for data files written by later appends. Existing files keep the
// codecs recorded in their own footers and stay readable. Empty settings keep
// the engine defaults. Canonical form: at most one entry per field, each with
// at least one option, in schema order.
struct WriteSettings
{
    // Absent: the engine default (EngineCapabilities::default_parquet_compression).
    std::optional<ParquetCompression> compression{};
    // Absent: Auto.
    std::optional<DictionaryPolicy> dictionary{};
    // Bytes of one column chunk's dictionary before it falls back to plain
    // pages; checked after every 1,024 values. Absent: the engine default.
    std::optional<int64_t> dictionary_page_size_limit{};
    std::vector<ColumnWriteSettings> columns{};
    bool operator==(const WriteSettings&) const = default;
};

struct TimestampUs
{
    int64_t value = 0;
    bool operator==(const TimestampUs&) const = default;
};

// Signed 128-bit coefficient, without a dependency on an Arrow public header.
struct DecimalValue
{
    int64_t coefficient_high = 0;
    uint64_t coefficient_low = 0;
    int32_t precision = 1;
    int32_t scale = 0;
    bool operator==(const DecimalValue&) const = default;
};

using Decimal = DecimalValue;

struct Date
{
    int32_t days_since_epoch = 0;
    bool operator==(const Date&) const = default;
};

using Cell = std::variant<std::monostate, std::string, bool, int64_t, double, TimestampUs, Decimal, Date>;
using Row = std::vector<Cell>;

struct BaseIdentity
{
    std::string store_id;
    std::string collection_id;
    std::string table_uuid;
    std::string metadata_location;
    std::optional<int64_t> current_snapshot_id;
    int32_t schema_id = 0;
    int32_t spec_id = 0;
    bool operator==(const BaseIdentity&) const = default;
};

enum class CommitDisposition : uint8_t { Committed, DefinitelyNotCommitted, CommitUnknown };
enum class CommitPhase : uint8_t {
    Validation,
    Binding,
    Data,
    Metadata,
    CatalogTransaction,
    CatalogCommit,
    RecordingFailure,
    Reconciling,
    Complete
};

struct AppendResult
{
    std::string collection_id;
    std::string operation_id;
    std::string payload_hash;
    int32_t hash_version = 1;
    int64_t snapshot_id = 0;
    std::optional<int64_t> parent_snapshot_id;
    int64_t rows_appended = 0;
    int64_t data_files_added = 0;
    int64_t compressed_data_bytes_added = 0;
    int64_t snapshot_created_at_us = 0;
    std::string durability_profile;
    int32_t receipt_version = 1;
    bool operator==(const AppendResult&) const = default;
};

struct RecoveryContext
{
    std::string store_id;
    std::string collection_id;
    std::string operation_id;
    std::string payload_hash;
    int32_t hash_version = 1;
    BaseIdentity base;
    std::string attempt_id;
    std::optional<int64_t> candidate_snapshot_id;
    std::optional<std::string> candidate_metadata_location;
    ErrorDetail first_error;
    bool publication_started = false;
    // Before an authoritative operation lookup, base is the last confirmed
    // collection view and attempt_id identifies this resolution call. No new
    // operation has been bound or published in that call.
    bool lookup_blocked = false;
    bool operator==(const RecoveryContext&) const = default;
};

struct AppendOutcome
{
    std::string operation_id;
    CommitDisposition disposition = CommitDisposition::DefinitelyNotCommitted;
    CommitPhase phase = CommitPhase::Validation;
    std::optional<AppendResult> result;
    std::optional<ErrorDetail> error;
    std::optional<RecoveryContext> recovery;
    bool operator==(const AppendOutcome&) const = default;
};

struct CollectionInfo
{
    std::string collection_id;
    std::string name;
    std::string table_uuid;
    SchemaSpec schema;
    int64_t creation_sequence = 0;
    std::optional<int64_t> current_snapshot_id;
    // Current canonical settings; version 0 means never set (engine defaults).
    WriteSettings write_settings{};
    int64_t write_settings_version = 0;
    bool operator==(const CollectionInfo&) const = default;
};

struct CreateCollectionRequest
{
    std::string collection_id;
    std::string exact_name;
    SchemaSpec schema;
    // Part of the definition: a repeated creation must request the same settings.
    WriteSettings write_settings{};
};

// Compare-and-set: applies only when expected_version is the current version.
// Repeating an applied request (same content, current == expected + 1)
// succeeds without a change, so a lost response can be retried safely.
struct UpdateWriteSettingsRequest
{
    std::string collection_id;
    int64_t expected_version = 0;
    WriteSettings write_settings{};
};

struct CreateRecoveryContext
{
    std::string store_id;
    std::string collection_id;
    std::string exact_name;
    std::string schema_hash;
    std::string attempt_id;
    std::optional<std::string> candidate_table_uuid;
    std::optional<std::string> candidate_metadata_location;
    bool publication_started = false;
};

struct CreateOutcome
{
    CommitDisposition disposition = CommitDisposition::DefinitelyNotCommitted;
    CommitPhase phase = CommitPhase::Validation;
    std::optional<CollectionInfo> info;
    std::optional<ErrorDetail> error;
    std::optional<CreateRecoveryContext> recovery;
};

enum class SnapshotPreconditionKind : uint8_t { Any, NoSnapshot, Equals };
struct SnapshotPrecondition
{
    SnapshotPreconditionKind kind = SnapshotPreconditionKind::Any;
    int64_t snapshot_id = 0;
};

struct AppendRequest
{
    std::string collection_id;
    std::string operation_id;
    SnapshotPrecondition expected_snapshot;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
};

// Replayed synchronously for validation and production. Next returns nullptr
// at EOF. Each immutable batch is bounded to 10,000 rows and 16 MiB; callbacks
// must return promptly and must not reenter the engine writer.
class BulkAppendSource
{
public:
    virtual ~BulkAppendSource() = default;
    virtual Status Rewind() = 0;
    virtual Result<std::shared_ptr<arrow::RecordBatch>> Next() = 0;
};

struct BulkAppendRequest
{
    std::string collection_id;
    std::string operation_id;
    SnapshotPrecondition expected_snapshot{};
    int64_t rows = 0;
    std::shared_ptr<BulkAppendSource> source{};
    // Cooperative; cancellation cannot revoke a successful outer COMMIT.
    std::shared_ptr<const std::atomic<bool>> cancellation{};
    std::chrono::milliseconds timeout{600000};
};

// Fixed development profile. Ordinary AppendDurable retains its own limits.
struct BulkAppendLimits
{
    static constexpr int64_t MaxFiles = 256;
    static constexpr int64_t MaxFileRows = 100'000;
    static constexpr int64_t MaxRows = MaxFiles * MaxFileRows;
    static constexpr int64_t TargetFileBytes = 8LL << 20;
    static constexpr int64_t MaxFileInputBytes = 16LL << 20;
    static constexpr int64_t MaxDecodedBytes = MaxFiles * MaxFileInputBytes;
    static constexpr int64_t MemoryBytes = 128LL << 20;
    static constexpr uint64_t AttemptDiskBytes = (8ULL << 30) + (32ULL << 20);
    static constexpr uint64_t AttemptInodes = 320;
};

// Finite retained-history budgets. Counts and serialized bytes are store-wide;
// a root file and a commit also have individual byte limits. These budgets do
// not expire snapshots or permanent operation outcomes.
struct HistoryLimits
{
    int64_t max_snapshots = 4096;
    int64_t max_manifests = 4096;
    int64_t max_data_files = 16384;
    int64_t max_root_bytes = 64LL << 20;
    int64_t max_root_file_bytes = 16LL << 20;
    int64_t max_manifest_bytes = 256LL << 20;
    int64_t max_manifest_list_bytes = 512LL << 20;
    int64_t max_footer_bytes = 256LL << 20;
    int64_t max_commit_bytes = 32LL << 20;
    int64_t max_recovery_workspace_bytes = 256LL << 20;
};

struct Limits
{
    // Finite supported profile; larger selections also require shared resources.
    static constexpr int64_t MaxSupportedCollections = 64;
    int64_t max_operations = 4096;
    int64_t max_collections = 8;
    int64_t max_catalog_bytes = 256LL * 1024 * 1024;
    int64_t max_pending_calls = 16;
    int64_t max_owned_input_bytes = 128LL * 1024 * 1024;
    std::chrono::milliseconds writer_wait{5000};
    std::chrono::milliseconds append_deadline{60000};
    HistoryLimits history{};
};

struct StoreInfo
{
    std::string store_id;
    std::string profile_id;
    int32_t format_version = 1;
};

struct CreateStoreRequest
{
    std::string store_id;
    std::string absolute_root;
    std::string profile_id = "local-delete-extra-v1";
    Limits limits{};
    std::shared_ptr<ResourceOwner> resources{};
};

struct OpenStoreRequest
{
    std::string absolute_root;
    std::string expected_store_id;
    std::string profile_id = "local-delete-extra-v1";
    Limits limits{};
    std::shared_ptr<ResourceOwner> resources{};
};

} // namespace glamdb
