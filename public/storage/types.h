#pragma once

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

enum class FieldType : uint8_t { String = 1, Bool, Int64, Float64, TimestampUsUtc };

struct FieldSpec
{
    std::string name;
    FieldType type = FieldType::String;
    bool nullable = false;
    bool operator==(const FieldSpec&) const = default;
};

// Field IDs are assigned in order, starting at 1, and never change.
struct SchemaSpec
{
    std::vector<FieldSpec> fields;
    bool operator==(const SchemaSpec&) const = default;
};

struct TimestampUs
{
    int64_t value = 0;
    bool operator==(const TimestampUs&) const = default;
};

using Cell = std::variant<std::monostate, std::string, bool, int64_t, double, TimestampUs>;
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
    bool operator==(const CollectionInfo&) const = default;
};

struct CreateCollectionRequest
{
    std::string collection_id;
    std::string exact_name;
    SchemaSpec schema;
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

struct Limits
{
    int64_t max_operations = 4096;
    int64_t max_collections = 8;
    int64_t max_catalog_bytes = 256LL * 1024 * 1024;
    int64_t max_pending_calls = 16;
    int64_t max_owned_input_bytes = 128LL * 1024 * 1024;
    std::chrono::milliseconds writer_wait{5000};
    std::chrono::milliseconds append_deadline{60000};
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
};

struct OpenStoreRequest
{
    std::string absolute_root;
    std::string expected_store_id;
    std::string profile_id = "local-delete-extra-v1";
    Limits limits{};
};

} // namespace glamdb
