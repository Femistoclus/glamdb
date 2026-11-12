#include "core/backend/local_storage_backend.h"
#include "core/backend/archive_records.h"
#include "core/io/tree_inventory.h"
#include "core/storage/snapshot_archive.h"
#include "core/storage/snapshot_retention.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/kv_input.h"
#include "core/api/write_settings.h"
#include "core/backend/file_window.h"
#include "core/backend/inert_catalog.h"
#include "core/backend/kv_producer.h"
#include "core/backend/storage_errors.h"
#include "core/engine/storage_mapping.h"
#include "core/engine/store_lease.h"
#include "core/io/io_attempt.h"
#include "core/io/local_fs.h"
#include "core/io/store_dirs.h"
#include "core/io/store_uri.h"
#include "core/io/write_latch.h"
#include "core/storage/append_stager.h"
#include "core/storage/base_state.h"
#include "core/storage/format_verifier.h"
#include "core/storage/publication_guard.h"
#include "core/storage/table_profile.h"

#include "core/resources/arrow_pool.h"
#include "core/scan/metadata_cache.h"
#include "storage/resources.h"

#include <arrow/array/array_binary.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/byte_size.h>
#include <arrow/util/compression.h>
#include <iceberg/constants.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/util/timepoint.h>

#include <algorithm>
#include <charconv>
#include <format>
#include <limits>
#include <new>
#include <optional>
#include <set>
#include <unordered_map>
#include <utility>

namespace glamdb::detail {
namespace {

namespace NIO = NGlamDB::NIO;
namespace NStorage = NGlamDB::NStorage;

constexpr std::string_view CatalogNamespace = "columnar";
constexpr std::string_view OperationKey = "glamdb.operation-id";
constexpr std::string_view ContentHashKey = "glamdb.content-hash";
constexpr std::string_view BulkVersionKey = "glamdb.bulk-version";
constexpr std::string_view KvVersionKey = "glamdb.kv-version";
constexpr std::string_view KvKindKey = "glamdb.kv-kind";
constexpr std::string_view KvInputRowsKey = "glamdb.kv-input-rows";
constexpr std::string_view KvAffectedRowsKey = "glamdb.kv-affected-rows";

// Arrow buffers borrow their pool. The aliasing batch handle keeps the pool
// alive even if a backend caller retains it after releasing the backend.
struct TAdmittedBatch
{
    std::shared_ptr<arrow::MemoryPool> Pool;
    std::shared_ptr<arrow::RecordBatch> Batch;
};

struct TRetainedState
{
    std::shared_ptr<ResourceReservation> Memory;
    std::shared_ptr<const NStorage::TBaseState> State;
};

void AddMemory(uint64_t& bytes, uint64_t count, uint64_t width = 1)
{
    if (count > (std::numeric_limits<uint64_t>::max() - bytes) / width) {
        throw Failure({"RESOURCE_EXHAUSTED", "retained metadata size overflows", RetryHint::OperatorAction});
    }
    bytes += count * width;
}

// Saturated storage-layer sums mean the retained metadata size overflows.
uint64_t CheckedMemory(uint64_t bytes)
{
    if (bytes == std::numeric_limits<uint64_t>::max()) {
        throw Failure({"RESOURCE_EXHAUSTED", "retained metadata size overflows", RetryHint::OperatorAction});
    }
    return bytes;
}

uint64_t DataFileMemory(const iceberg::DataFile& file)
{
    return CheckedMemory(NStorage::DataFileMemory(file));
}

// O(1): the shared generation keeps every per-element term incrementally.
uint64_t ScanIndexMemory(const NStorage::TBaseState& state)
{
    if (!state.ScanIndex) {
        return 0;
    }
    return CheckedMemory(
        NStorage::ScanIndexBytes(*state.ScanIndex, state.ExpiredSnapshotIDs, state.RetainedManifestPaths));
}

struct TRootMemorySize
{
    uint64_t RootBytes = 0;
    uint64_t IndexBytes = 0;
};

uint64_t RootMemoryBytes(TRootMemorySize size)
{
    // Snapshot JSON does not bound the number or metric maps of bulk files.
    // Charge those independently and pin both charges to the generation.
    if (size.RootBytes > (64ULL << 20)) {
        throw Failure({"RESOURCE_EXHAUSTED", "root exceeds the metadata profile", RetryHint::OperatorAction});
    }
    uint64_t total = size.RootBytes * 32 + (4ULL << 20);
    AddMemory(total, size.IndexBytes);
    return total;
}

std::shared_ptr<ResourceReservation> ReserveRoot(const std::shared_ptr<ResourceOwner>& owner, TRootMemorySize size)
{
    auto memory = owner->Reserve(ResourceClass::Metadata, RootMemoryBytes(size));
    if (!memory) {
        throw Failure(memory.error());
    }
    return std::move(*memory);
}

std::shared_ptr<const NStorage::TBaseState> RetainState(std::shared_ptr<ResourceReservation> memory,
    std::shared_ptr<const NStorage::TBaseState> state)
{
    auto retained = std::make_shared<TRetainedState>(std::move(memory), std::move(state));
    return std::shared_ptr<const NStorage::TBaseState>(retained, retained->State.get());
}

[[noreturn]] void Fail(std::string_view code, std::string_view context, std::string_view message)
{
    const auto hint = code == "INVALID_ARGUMENT" ? RetryHint::FixInput : RetryHint::OperatorAction;
    throw Failure({std::string(code), BoundedDiagnostic(std::string(context) + ": " + std::string(message)), hint});
}

// Write-side Iceberg failure. Storage prefixes and kinds decide CORRUPTION and
// RESOURCE_EXHAUSTED; invalid input reported by a producer is INVALID_ARGUMENT;
// otherwise `code`.
template <class T>
T TakeIceberg(iceberg::Result<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        auto detail = StorageError(result.error(), context, EStoragePath::Write, code);
        if (detail.code == code && result.error().kind == iceberg::ErrorKind::kInvalidArgument) {
            detail.code = "INVALID_ARGUMENT";
            detail.retry_hint = RetryHint::FixInput;
        }
        throw Failure(std::move(detail));
    }
    return std::move(*result);
}

void CheckIceberg(const iceberg::Status& status, std::string_view code, std::string_view context)
{
    if (!status) {
        throw Failure(StorageError(status.error(), context, EStoragePath::Write, code));
    }
}

// Committed or retained state: invariant violations fail closed; I/O and
// resource failures keep their environmental classification.
template <class T>
T TakeRetained(iceberg::Result<T> result, std::string_view context)
{
    if (!result) {
        throw Failure(StorageError(result.error(), context, EStoragePath::Read, "CORRUPTION"));
    }
    return std::move(*result);
}

template <class T>
T TakeIO(NIO::TResult<T> result, EStoragePath path, std::string_view context)
{
    if (!result) {
        throw Failure(StorageError(result.Error(), path, context));
    }
    return std::move(result).Value();
}

void CheckIO(std::error_code error, EStoragePath path, std::string_view context)
{
    if (error) {
        throw Failure(StorageError(error, path, context));
    }
}

// Usage arithmetic and limit checks report an explicit outcome code.
NStorage::TStorageUsage
TakeUsage(NIO::TResult<NStorage::TStorageUsage> result, std::string_view code, std::string_view context)
{
    if (!result) {
        Fail(code, context, result.Error().message());
    }
    return result.Value();
}

void CheckUsage(std::error_code error, std::string_view code, std::string_view context)
{
    if (error) {
        Fail(code, context, error.message());
    }
}

// The latch is sticky for this engine instance after a failed durable write.
// It blocks writes only; reads, verification and recovery remain available.
void CheckWriteLatch(const NIO::TStoreDirs& dirs)
{
    if (dirs.Latch()->IsSet()) {
        throw Failure({"IO_ERROR",
            "write latch: a durable storage write failed in this engine instance; shut down and reopen the store",
            RetryHint::OperatorAction});
    }
}

// Storage failures are reported outcomes; no exception crosses the backend seam.
template <class TResult, class TFunction>
TResult Guarded(TFunction&& body)
{
    try {
        return body();
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

void CheckDeadline(std::chrono::steady_clock::time_point deadline)
{
    if (std::chrono::steady_clock::now() >= deadline) {
        Fail("DEADLINE_EXCEEDED", "append", "deadline expired during storage production");
    }
}

std::string TableLocation(std::string_view storeID, std::string_view collectionID)
{
    return std::format("store://{}/warehouse/{}", storeID, collectionID);
}

std::string MetadataLocation(std::string_view table, uint64_t version, std::string_view attemptID)
{
    return std::format("{}/metadata/{:05}-{}.metadata.json", table, version, attemptID);
}

uint64_t MetadataVersion(std::string_view location)
{
    const auto slash = location.rfind('/');
    const auto leaf = slash == std::string_view::npos ? location : location.substr(slash + 1);
    const auto dash = leaf.find('-');
    uint64_t version = 0;
    if (dash == std::string_view::npos || dash == 0) {
        Fail("CORRUPTION", "base metadata", "root location has no version prefix");
    }
    const auto parsed = std::from_chars(leaf.data(), leaf.data() + dash, version);
    if (parsed.ec != std::errc{} || parsed.ptr != leaf.data() + dash || version == std::numeric_limits<uint64_t>::max())
    {
        Fail("CORRUPTION", "base metadata", "root location has an invalid version prefix");
    }
    return version;
}

std::optional<int64_t> CurrentSnapshot(const iceberg::TableMetadata& metadata)
{
    if (metadata.current_snapshot_id == iceberg::kInvalidSnapshotId) {
        return std::nullopt;
    }
    return metadata.current_snapshot_id;
}

int64_t SnapshotTimeUs(const iceberg::Snapshot& snapshot, std::string_view code)
{
    const auto ms = iceberg::UnixMsFromTimePointMs(snapshot.timestamp_ms);
    if (ms < -62135596800000LL || ms > 253402300799999LL) {
        Fail(code, "snapshot timestamp", "snapshot timestamp is outside supported UTC range");
    }
    return ms * 1000;
}

NStorage::TTableProfile
MakeProfile(const SchemaSpec& schema, std::string_view code, CollectionMode mode = CollectionMode::Append)
{
    auto profile = mode == CollectionMode::KeyValue ? NStorage::MakeKVTableProfile(ToStorageSchema(schema))
                                                    : NStorage::MakeTableProfile(ToStorageSchema(schema));
    if (!profile) {
        Fail(code, "table profile", profile.error().message);
    }
    return std::move(*profile);
}

arrow::Compression::type ArrowCodec(ParquetCodec codec)
{
    switch (codec) {
        case ParquetCodec::Uncompressed:
            return arrow::Compression::UNCOMPRESSED;
        case ParquetCodec::Snappy:
            return arrow::Compression::SNAPPY;
        case ParquetCodec::Gzip:
            return arrow::Compression::GZIP;
        case ParquetCodec::Lz4Raw:
            return arrow::Compression::LZ4;
        case ParquetCodec::Zstd:
            return arrow::Compression::ZSTD;
    }
    Fail("INVALID_ARGUMENT", "write settings", "unknown Parquet codec");
}

parquet::Encoding::type ParquetValueEncoding(ParquetEncoding encoding)
{
    switch (encoding) {
        case ParquetEncoding::Plain:
            return parquet::Encoding::PLAIN;
        case ParquetEncoding::DeltaBinaryPacked:
            return parquet::Encoding::DELTA_BINARY_PACKED;
        case ParquetEncoding::DeltaLengthByteArray:
            return parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
        case ParquetEncoding::DeltaByteArray:
            return parquet::Encoding::DELTA_BYTE_ARRAY;
        case ParquetEncoding::ByteStreamSplit:
            return parquet::Encoding::BYTE_STREAM_SPLIT;
        case ParquetEncoding::Rle:
            return parquet::Encoding::RLE;
    }
    Fail("INVALID_ARGUMENT", "write settings", "unknown Parquet encoding");
}

// Settings the coordinator read with the collection before binding; empty
// settings leave the writer defaults untouched. An explicit column encoding
// turns that column's dictionary off; bool columns never take a dictionary.
NStorage::TParquetWriteSpec WriterSpec(const SchemaSpec& schema, const WriteSettings& settings)
{
    NStorage::TParquetWriteSpec spec;
    if (settings.compression) {
        spec.Compression =
            NStorage::TParquetCompressionSpec{ArrowCodec(settings.compression->codec), settings.compression->level};
    }
    if (settings.dictionary == DictionaryPolicy::Disabled) {
        spec.Dictionary = false;
    }
    spec.DictionaryPageSizeLimit = settings.dictionary_page_size_limit;
    for (const auto& column : settings.columns) {
        const auto field = std::ranges::find(schema.fields, column.field, &FieldSpec::name);
        Require(field != schema.fields.end(), "INVALID_ARGUMENT", "write settings name an unknown column");
        NStorage::TParquetColumnSpec item{column.field};
        if (column.compression) {
            item.Compression =
                NStorage::TParquetCompressionSpec{ArrowCodec(column.compression->codec), column.compression->level};
        }
        if (column.dictionary == DictionaryPolicy::Disabled || (column.encoding && !column.dictionary)) {
            item.Dictionary = false;
        } else if (column.dictionary && field->type != FieldType::Bool) {
            item.Dictionary = true;
        }
        if (column.encoding) {
            item.Encoding = ParquetValueEncoding(*column.encoding);
        }
        spec.Columns.push_back(std::move(item));
    }
    return spec;
}

void CheckWriterInput(const SchemaSpec& schema, const NormalizedInput& input, const NStorage::TDataWriterLimits& limits)
{
    if (limits.MaxInputBytes <= 0 || limits.MaxRows <= 0 || limits.MaxRowGroupBytes <= 0 || limits.MaxFileBytes <= 0) {
        Fail("INVALID_ARGUMENT", "append", "data writer limits must be positive");
    }
    const auto rows = input.GetRowCount();
    if (rows == 0 || static_cast<int64_t>(rows) > limits.MaxRows) {
        Fail("INVALID_ARGUMENT", "append", std::format("an append must contain 1..{} rows", limits.MaxRows));
    }
    int64_t fixedRowBytes = 0;
    for (const auto& field : schema.fields) {
        fixedRowBytes += 1 +
            (field.type == FieldType::Decimal                                          ? 16
                    : field.type == FieldType::String || field.type == FieldType::Date ? 4
                    : field.type == FieldType::Bool                                    ? 1
                                                                                       : 8);
    }
    // Per row: the fixed slots plus the string bytes.
    std::vector<int64_t> rowBytes(rows, fixedRowBytes);
    if (input.Columns != nullptr) {
        if (input.Columns->num_columns() != static_cast<int>(schema.fields.size()) ||
            !input.Columns->schema()->Equals(*MakeArrowSchema(schema)))
        {
            Fail("INVALID_ARGUMENT", "append", "input columns do not match the collection schema");
        }
        for (size_t column = 0; column < schema.fields.size(); ++column) {
            if (schema.fields[column].type != FieldType::String) {
                continue;
            }
            const auto& strings =
                static_cast<const arrow::StringArray&>(*input.Columns->column(static_cast<int>(column)));
            for (size_t row = 0; row < rows; ++row) {
                if (strings.IsValid(static_cast<int64_t>(row))) {
                    rowBytes[row] += strings.value_length(static_cast<int64_t>(row));
                }
            }
        }
    } else {
        for (size_t row = 0; row < rows; ++row) {
            if (input.rows[row].size() != schema.fields.size()) {
                Fail("INVALID_ARGUMENT", "append", "row field count does not match the collection schema");
            }
            for (const auto& cell : input.rows[row]) {
                if (const auto* text = std::get_if<std::string>(&cell)) {
                    rowBytes[row] += static_cast<int64_t>(text->size());
                }
            }
        }
    }
    int64_t maxRowBytes = 1;
    for (const auto bytes : rowBytes) {
        if (bytes > (2LL << 20)) {
            throw Failure({"RESOURCE_EXHAUSTED", "append: Row exceeds its byte limit.", RetryHint::FixInput});
        }
        maxRowBytes = std::max(maxRowBytes, bytes);
    }
    const auto offsetBytes = 4 * static_cast<int64_t>(std::ranges::count_if(schema.fields, [](const auto& field) {
        return field.type == FieldType::String;
    }));
    if (limits.MaxRowGroupBytes < offsetBytes || maxRowBytes > limits.MaxRowGroupBytes - offsetBytes) {
        throw Failure({"RESOURCE_EXHAUSTED", "append: row exceeds the row group byte limit", RetryHint::FixInput});
    }
    // An upper bound of the Arrow buffers WriteDataFile receives, so the
    // writer cannot reject what this preflight admitted.
    const auto totalBytes = input.Columns != nullptr ? EstimateRecordBatchBytes(schema, *input.Columns)
                                                     : EstimateRecordBatchBytes(schema, input.rows);
    if (totalBytes < 0 || totalBytes > limits.MaxInputBytes) {
        throw Failure({"RESOURCE_EXHAUSTED", "append: Input buffers exceed their byte limit.", RetryHint::FixInput});
    }
}

void CheckBulkInput(const NormalizedInput& input, const NStorage::TDataWriterLimits& limits)
{
    Require(input.bulk && input.bulk->Next && input.bulk->Rows > 0 && input.bulk->Rows <= BulkAppendLimits::MaxRows &&
            input.rows.empty() && !input.Columns && !input.writer_batch,
        "INVALID_ARGUMENT",
        "bulk append requires a bounded declared row count and a streaming source");
    Require(limits.MaxInputBytes > 0 && limits.MaxRows > 0 && limits.MaxRowGroupBytes > 0 && limits.MaxFileBytes > 0,
        "INVALID_ARGUMENT",
        "data writer limits must be positive");
}

std::string_view MutationName(MutationKind kind)
{
    switch (kind) {
        case MutationKind::Upsert:
            return "upsert";
        case MutationKind::Delete:
            return "delete";
        case MutationKind::Compact:
            return "compact";
        case MutationKind::None:
            break;
    }
    Fail("INVALID_ARGUMENT", "KV mutation", "unknown mutation kind");
}

SchemaSpec MutationSchema(const CollectionInfo& collection, const NormalizedInput& input)
{
    return input.kv->Kind == MutationKind::Delete ? SchemaSpec{{{"key", FieldType::String, false}}} : collection.schema;
}

void CheckKvInput(const CollectionInfo& collection,
    const NormalizedInput& input,
    const NStorage::TDataWriterLimits& limits)
{
    Require(collection.mode == CollectionMode::KeyValue && input.kv && input.kv->InputRows >= 0 &&
            input.kv->InputRows <= BulkAppendLimits::MaxRows && input.kv->MaxFiles > 0 && input.kv->MaxFiles <= 256 &&
            input.kv->TargetFileBytes > 0 && input.kv->TargetFileBytes <= (16LL << 20) &&
            input.kv->Duplicates >= DuplicatePolicy::Reject && input.kv->Duplicates <= DuplicatePolicy::LastWins,
        "INVALID_ARGUMENT",
        "KV mutation has an invalid mode, identity or finite limits");
    (void)MutationName(input.kv->Kind);
    if (input.kv->Kind == MutationKind::Compact) {
        Require(input.kv->InputRows == 0 && input.GetRowCount() == 0 && !input.bulk,
            "INVALID_ARGUMENT",
            "KV compaction requires empty input");
        return;
    }
    if (input.bulk) {
        Require(input.kv->Kind == MutationKind::Upsert && input.bulk->Rows == input.kv->InputRows,
            "INVALID_ARGUMENT",
            "KV bulk input has an invalid kind or row count");
        CheckBulkInput(input, limits);
        return;
    }
    Require(input.GetRowCount() == static_cast<size_t>(input.kv->InputRows),
        "INVALID_ARGUMENT",
        "KV mutation row count differs from its identity");
    if (input.GetRowCount() != 0) {
        CheckWriterInput(MutationSchema(collection, input), input, limits);
    }
}

void CheckIdentity(const NStorage::TBaseState& state, const BaseIdentity& base)
{
    Require(state.Metadata != nullptr && state.MetadataLocation == base.metadata_location &&
            state.Metadata->table_uuid == base.table_uuid &&
            CurrentSnapshot(*state.Metadata) == base.current_snapshot_id,
        "CORRUPTION",
        "retained table root does not match the catalog identity");
}

void CheckSchema(const iceberg::TableMetadata& metadata, const SchemaSpec& schema, CollectionMode mode)
{
    Require(NStorage::IsKVMetadata(metadata) == (mode == CollectionMode::KeyValue),
        "CORRUPTION",
        "retained table mode differs from the catalog mode");
    const auto profile = MakeProfile(schema, "CORRUPTION", mode);
    const auto current = TakeRetained(metadata.Schema(), "retained schema");
    Require(*current == *profile.Schema &&
            std::ranges::equal(current->fields(),
                profile.Schema->fields(),
                [](const auto& actual, const auto& expected) {
                    return actual.doc() == expected.doc();
                }),
        "CORRUPTION",
        "retained table schema differs from the catalog schema");
}

bool SummaryMatches(const iceberg::Snapshot& snapshot, const std::string& key, bool required, std::string_view expected)
{
    const auto it = snapshot.summary.find(key);
    if (it == snapshot.summary.end()) {
        return !required;
    }
    return it->second == expected;
}

void CheckReceipt(const iceberg::Snapshot& snapshot, const AppendResult& receipt)
{
    using Fields = iceberg::SnapshotSummaryFields;
    if (receipt.receipt_version == 3) {
        const auto metric = [&](const std::string& key, int64_t value) {
            return value >= 0 && SummaryMatches(snapshot, key, value != 0, std::to_string(value));
        };
        Require(receipt.snapshot_created && receipt.hash_version == 1 && receipt.rows_appended == 0 &&
                receipt.snapshot_id == snapshot.snapshot_id &&
                receipt.parent_snapshot_id == snapshot.parent_snapshot_id &&
                receipt.snapshot_created_at_us == SnapshotTimeUs(snapshot, "CORRUPTION") &&
                snapshot.summary.find(std::string(BulkVersionKey)) == snapshot.summary.end() &&
                SummaryMatches(snapshot, std::string(KvVersionKey), true, "1") &&
                receipt.mutation_kind >= MutationKind::Upsert && receipt.mutation_kind <= MutationKind::Compact &&
                SummaryMatches(snapshot, std::string(KvKindKey), true, MutationName(receipt.mutation_kind)) &&
                SummaryMatches(snapshot, std::string(ContentHashKey), true, receipt.payload_hash) &&
                receipt.input_rows >= 0 && receipt.rows_affected >= 0 &&
                SummaryMatches(snapshot, std::string(KvInputRowsKey), true, std::to_string(receipt.input_rows)) &&
                SummaryMatches(snapshot, std::string(KvAffectedRowsKey), true, std::to_string(receipt.rows_affected)) &&
                metric(Fields::kAddedRecords, receipt.rows_written) &&
                metric(Fields::kDeletedRecords, receipt.rows_removed) &&
                metric(Fields::kAddedDataFiles, receipt.data_files_added) &&
                metric(Fields::kDeletedDataFiles, receipt.data_files_removed) &&
                metric(Fields::kAddedFileSize, receipt.compressed_data_bytes_added) &&
                metric(Fields::kRemovedFileSize, receipt.compressed_data_bytes_removed) &&
                (receipt.mutation_kind == MutationKind::Compact
                        ? receipt.input_rows == 0 && receipt.rows_affected == 0 &&
                            receipt.rows_written == receipt.rows_removed && receipt.data_files_removed >= 4 &&
                            receipt.data_files_added <= receipt.data_files_removed / 4
                        : receipt.input_rows > 0 && receipt.rows_affected > 0 &&
                            receipt.rows_affected <= receipt.input_rows) &&
                (receipt.mutation_kind != MutationKind::Upsert ||
                    (receipt.rows_written >= receipt.rows_removed &&
                        receipt.rows_written - receipt.rows_removed <= receipt.rows_affected)) &&
                (receipt.mutation_kind != MutationKind::Delete ||
                    (receipt.rows_removed >= receipt.rows_written &&
                        receipt.rows_removed - receipt.rows_written == receipt.rows_affected)),
            "CORRUPTION",
            "retained KV snapshot differs from its committed receipt");
        return;
    }
    const auto marker = snapshot.summary.find(std::string(BulkVersionKey));
    const auto profileMatches = receipt.receipt_version == 2
        ? marker != snapshot.summary.end() && marker->second == "1"
        : receipt.receipt_version == 1 && marker == snapshot.summary.end();
    Require(profileMatches && receipt.hash_version == 1 && receipt.snapshot_id == snapshot.snapshot_id &&
            receipt.parent_snapshot_id == snapshot.parent_snapshot_id &&
            receipt.snapshot_created_at_us == SnapshotTimeUs(snapshot, "CORRUPTION") &&
            SummaryMatches(snapshot, std::string(ContentHashKey), true, receipt.payload_hash) &&
            SummaryMatches(snapshot, Fields::kAddedRecords, true, std::to_string(receipt.rows_appended)) &&
            SummaryMatches(snapshot, Fields::kAddedDataFiles, true, std::to_string(receipt.data_files_added)) &&
            SummaryMatches(snapshot, Fields::kAddedFileSize, true, std::to_string(receipt.compressed_data_bytes_added)),
        "CORRUPTION",
        "retained snapshot differs from its committed receipt");
}

// The ancestry before the oldest root snapshot: the expiration ledger
// reconstructed from permanent receipts, then the archive rows. Each archived
// row is bound to exactly one committed receipt, continues the ledger without
// a gap and records the cumulative data files and the metadata bytes that
// stay on disk for it. No archived metadata file is read here.
struct TRetainedPrefix
{
    std::vector<std::shared_ptr<iceberg::Snapshot>> History;
    std::size_t Expired = 0;
    NStorage::TStorageUsage Archive;
    std::vector<std::pair<int64_t, std::string>> Archived;
};

TRetainedPrefix RetainedPrefix(const CollectionView& view,
    const std::function<void(std::string_view, const DurableSqliteCatalogStore::OperationConsumer&)>& readOperations,
    const std::function<void(std::string_view, const DurableSqliteCatalogStore::ArchiveConsumer&)>& readArchive,
    const NStorage::TStorageLimits& limits)
{
    TRetainedPrefix result;
    std::vector<ArchivedSnapshotRecord> rows;
    std::vector<std::shared_ptr<iceberg::Snapshot>> archived;
    readArchive(view.info.collection_id, [&](const ArchivedSnapshotRecord& row) {
        auto snapshot = DecodeArchivedSnapshot(row);
        auto core = row;
        core.snapshot_blob.clear();
        rows.push_back(std::move(core));
        archived.push_back(std::move(snapshot));
    });
    if (view.expired_snapshots.empty() && rows.empty()) {
        return result;
    }
    Require(view.info.mode == CollectionMode::Append,
        "CORRUPTION",
        "KV collection has an unsupported expiration ledger or snapshot archive");
    Require(view.info.mode == CollectionMode::Append,
        "CORRUPTION",
        "KV collection has an unsupported expiration ledger");
    std::map<std::string, AppendResult, std::less<>> byOperation;
    readOperations(view.info.collection_id, [&](const auto& operation) {
        if (operation.state == OperationState::Committed && operation.outcome && operation.outcome->result) {
            byOperation.emplace(operation.binding.operation_id, *operation.outcome->result);
        }
    });
    result.History.reserve(view.expired_snapshots.size() + rows.size());
    std::set<int64_t> ids;
    uint64_t files = 0;
    const auto addFiles = [&](const AppendResult& receipt) {
        Require(receipt.data_files_added > 0 &&
                static_cast<uint64_t>(receipt.data_files_added) <= std::numeric_limits<uint64_t>::max() - files,
            "CORRUPTION",
            "retained history file count overflows");
        files += static_cast<uint64_t>(receipt.data_files_added);
    };
    for (const auto& record : view.expired_snapshots) {
        const auto found = byOperation.find(record.operation_id);
        Require(found != byOperation.end(), "CORRUPTION", "expired snapshot lacks its committed operation");
        const auto& receipt = found->second;
        Require(receipt.snapshot_id == record.snapshot_id && ids.insert(record.snapshot_id).second &&
                record.sequence_number == static_cast<int64_t>(result.History.size() + 1),
            "CORRUPTION",
            "invalid expiration ledger identity");
        addFiles(receipt);
        auto snapshot = std::make_shared<iceberg::Snapshot>();
        snapshot->snapshot_id = record.snapshot_id;
        snapshot->sequence_number = record.sequence_number;
        snapshot->parent_snapshot_id = receipt.parent_snapshot_id;
        snapshot->timestamp_ms = iceberg::TimePointMs(std::chrono::milliseconds(receipt.snapshot_created_at_us / 1000));
        snapshot->schema_id = view.base.schema_id;
        using Fields = iceberg::SnapshotSummaryFields;
        snapshot->summary = {{"operation", "append"},
            {std::string(OperationKey), record.operation_id},
            {std::string(ContentHashKey), receipt.payload_hash},
            {Fields::kAddedRecords, std::to_string(receipt.rows_appended)},
            {Fields::kAddedDataFiles, std::to_string(receipt.data_files_added)},
            {Fields::kAddedFileSize, std::to_string(receipt.compressed_data_bytes_added)}};
        if (receipt.receipt_version == 2) {
            snapshot->summary[std::string(BulkVersionKey)] = "1";
        }
        result.History.push_back(std::move(snapshot));
    }
    result.Expired = result.History.size();
    for (size_t index = 0; index < rows.size(); ++index) {
        const auto& row = rows[index];
        const auto& snapshot = archived[index];
        const auto found = byOperation.find(row.operation_id);
        Require(found != byOperation.end() && found->second.snapshot_id == row.snapshot_id &&
                ids.insert(row.snapshot_id).second &&
                row.sequence_number == static_cast<int64_t>(result.History.size() + 1),
            "CORRUPTION",
            "archived snapshot has no unique committed receipt or leaves a gap");
        const auto previous = result.History.empty() ? std::nullopt : std::optional(result.History.back()->snapshot_id);
        Require(row.parent_snapshot_id == previous, "CORRUPTION", "archived snapshot breaks the ancestry chain");
        CheckReceipt(*snapshot, found->second);
        addFiles(found->second);
        Require(static_cast<uint64_t>(row.data_files) == files,
            "CORRUPTION",
            "archived snapshot membership differs from its receipts");
        const auto list = NIO::TStoreUri::Parse(row.manifest_list, view.base.store_id);
        Require(list && list.Value().CollectionId() == view.info.collection_id &&
                list.Value().Kind() == NIO::EFileKind::ManifestList &&
                static_cast<uint64_t>(row.manifest_list_bytes) <= limits.ManifestListBytes &&
                static_cast<uint64_t>(row.retired_manifests) <= limits.Store.Manifests &&
                static_cast<uint64_t>(row.retired_manifest_bytes) <=
                    static_cast<uint64_t>(row.retired_manifests) * limits.ManifestBytes,
            "CORRUPTION",
            "archived snapshot metadata is outside its profile");
        result.Archive = TakeUsage(NStorage::ReplaceUsage(result.Archive,
                                       {.Previous = {},
                                           .Next = {.Manifests = static_cast<uint64_t>(row.retired_manifests),
                                               .ManifestBytes = static_cast<uint64_t>(row.retired_manifest_bytes),
                                               .ManifestListBytes = static_cast<uint64_t>(row.manifest_list_bytes)}}),
            "CORRUPTION",
            "archived metadata usage");
        result.Archived.emplace_back(row.snapshot_id, row.operation_id);
        result.History.push_back(snapshot);
    }
    return result;
}

void CheckNoOpRootPrefix(const iceberg::TableMetadata& historical, const iceberg::TableMetadata& current)
{
    Require(historical.snapshots.size() <= current.snapshots.size() &&
            historical.snapshot_log.size() == historical.snapshots.size() &&
            historical.snapshot_log.size() <= current.snapshot_log.size() &&
            std::equal(historical.snapshot_log.begin(), historical.snapshot_log.end(), current.snapshot_log.begin()),
        "CORRUPTION",
        "KV no-op historical root does not retain a prefix of the confirmed snapshot log");
    for (size_t i = 0; i < historical.snapshots.size(); ++i) {
        const auto& old = historical.snapshots[i];
        const auto& retained = current.snapshots[i];
        Require(old && retained && *old == *retained && old->summary == retained->summary &&
                old->manifest_list == retained->manifest_list,
            "CORRUPTION",
            "KV no-op historical root changes a retained snapshot");
    }
    const auto expectedSnapshot =
        historical.snapshots.empty() ? iceberg::kInvalidSnapshotId : historical.snapshots.back()->snapshot_id;
    const auto expectedSequence = historical.snapshots.empty() ? 0 : historical.snapshots.back()->sequence_number;
    Require(historical.current_snapshot_id == expectedSnapshot && historical.last_sequence_number == expectedSequence,
        "CORRUPTION",
        "KV no-op historical root has an inconsistent selected snapshot");
    auto normalized = current;
    normalized.snapshots = historical.snapshots;
    normalized.snapshot_log = historical.snapshot_log;
    normalized.current_snapshot_id = historical.current_snapshot_id;
    normalized.last_sequence_number = historical.last_sequence_number;
    normalized.last_updated_ms = historical.last_updated_ms;
    if (historical.snapshots.empty()) {
        normalized.refs.erase("main");
    } else {
        const auto main = normalized.refs.find("main");
        Require(main != normalized.refs.end() && main->second, "CORRUPTION", "confirmed KV root has no main reference");
        auto restored = std::make_shared<iceberg::SnapshotRef>(*main->second);
        restored->snapshot_id = historical.current_snapshot_id;
        main->second = std::move(restored);
    }
    Require(normalized == historical,
        "CORRUPTION",
        "KV no-op historical root differs from immutable confirmed metadata");
}

void CheckNoOpReceipt(const iceberg::TableMetadata& metadata,
    const CollectionInfo& collection,
    std::string_view root,
    const OperationRecord& operation,
    const std::shared_ptr<iceberg::FileIO>& fileIO,
    uint64_t rootLimit,
    std::map<std::string, BaseIdentity, std::less<>>& verifiedRoots)
{
    if (!operation.outcome || !operation.outcome->result) {
        Fail("CORRUPTION", "KV no-op history", "committed operation has no receipt");
    }
    const auto& receipt = *operation.outcome->result;
    const auto& base = operation.binding.base;
    const auto baseUri = NIO::TStoreUri::Parse(base.metadata_location, base.store_id);
    const auto rootUri = NIO::TStoreUri::Parse(root, base.store_id);
    Require(collection.mode == CollectionMode::KeyValue && NStorage::IsKVMetadata(metadata) &&
            receipt.receipt_version == 3 && receipt.hash_version == 1 && !receipt.snapshot_created &&
            receipt.mutation_kind >= MutationKind::Upsert && receipt.mutation_kind <= MutationKind::Compact &&
            receipt.collection_id == collection.collection_id &&
            receipt.operation_id == operation.binding.operation_id &&
            receipt.payload_hash == operation.binding.payload_hash && receipt.rows_appended == 0 &&
            receipt.snapshot_id == base.current_snapshot_id.value_or(0) &&
            receipt.parent_snapshot_id == base.current_snapshot_id && receipt.snapshot_created_at_us == 0 &&
            receipt.input_rows >= 0 && receipt.rows_affected == 0 && receipt.rows_written == 0 &&
            receipt.rows_removed == 0 && receipt.data_files_added == 0 && receipt.data_files_removed == 0 &&
            receipt.compressed_data_bytes_added == 0 && receipt.compressed_data_bytes_removed == 0 &&
            base.collection_id == collection.collection_id && base.table_uuid == collection.table_uuid &&
            base.schema_id == 0 && base.spec_id == 0 && baseUri && rootUri &&
            rootUri.Value().CollectionId() == collection.collection_id &&
            baseUri.Value().Kind() == NIO::EFileKind::Metadata &&
            baseUri.Value().CollectionId() == collection.collection_id &&
            MetadataVersion(base.metadata_location) <= MetadataVersion(root) &&
            (receipt.mutation_kind != MutationKind::Compact || receipt.input_rows == 0),
        "CORRUPTION",
        "committed KV no-op differs from its retained exact base");
    const auto found = verifiedRoots.find(base.metadata_location);
    if (found != verifiedRoots.end()) {
        Require(found->second == base, "CORRUPTION", "KV no-op receipts disagree on the same historical root identity");
        return;
    }
    if (base.current_snapshot_id) {
        const auto snapshot = metadata.SnapshotById(*base.current_snapshot_id);
        Require(snapshot && *snapshot, "CORRUPTION", "committed KV no-op base snapshot is not retained");
    }
    auto historical = TakeRetained(
        NStorage::ReadTableMetadata(*fileIO, base.metadata_location, std::nullopt, static_cast<int64_t>(rootLimit)),
        "verify exact KV no-op historical root");
    NStorage::TBaseState old{.MetadataLocation = base.metadata_location,
        .Metadata = std::shared_ptr<const iceberg::TableMetadata>(std::move(historical)),
        .CurrentManifests = {},
        .Usage = {},
        .ScanIndex = {}};
    CheckIdentity(old, base);
    CheckSchema(*old.Metadata, collection.schema, collection.mode);
    CheckNoOpRootPrefix(*old.Metadata, metadata);
    verifiedRoots.emplace(base.metadata_location, base);
}

// Snapshot-producing receipts and retained snapshots form a bijection. KV
// no-ops retain their prepared base separately and never claim a new snapshot.
void CheckHistory(const iceberg::TableMetadata& metadata,
    const CollectionInfo& collection,
    std::string_view root,
    const std::function<void(std::string_view, const DurableSqliteCatalogStore::OperationConsumer&)>& readOperations,
    const std::shared_ptr<iceberg::FileIO>& fileIO,
    uint64_t rootLimit,
    std::span<const ExpiredSnapshotRecord> expired,
    std::span<const std::pair<int64_t, std::string>> archived)
{
    Require(collection.mode != CollectionMode::KeyValue || expired.empty(),
        "CORRUPTION",
        "KV collection has an unsupported expiration ledger");
    Require(collection.mode != CollectionMode::KeyValue || archived.empty(),
        "CORRUPTION",
        "KV collection has an unsupported snapshot archive");
    Require(expired.size() <= std::numeric_limits<size_t>::max() - metadata.snapshots.size() - archived.size(),
        "CORRUPTION",
        "snapshot inventory size overflows");
    const auto snapshotCount = metadata.snapshots.size() + expired.size() + archived.size();
    std::map<std::string, BaseIdentity, std::less<>> verifiedRoots;
    std::map<std::string, AppendResult, std::less<>> committed;
    readOperations(collection.collection_id, [&](const auto& operation) {
        if (operation.binding.collection_id != collection.collection_id || operation.state != OperationState::Committed)
        {
            return;
        }
        if (!operation.outcome.has_value() || !operation.outcome->result.has_value()) {
            Fail("CORRUPTION", "history", "committed operation has no receipt");
        }
        const auto& receipt = *operation.outcome->result;
        if (receipt.receipt_version == 3 && !receipt.snapshot_created) {
            CheckNoOpReceipt(metadata, collection, root, operation, fileIO, rootLimit, verifiedRoots);
            return;
        }
        Require((collection.mode == CollectionMode::KeyValue) == (receipt.receipt_version == 3),
            "CORRUPTION",
            "committed receipt version differs from the collection mode");
        Require(committed.size() < snapshotCount,
            "CORRUPTION",
            "committed snapshot receipts exceed retained and expired snapshots");
        Require(committed.emplace(operation.binding.operation_id, receipt).second,
            "CORRUPTION",
            "committed operation identity is duplicated");
    });
    std::set<std::string, std::less<>> seen;
    std::set<int64_t> ids;
    for (const auto& snapshot : expired) {
        const auto receipt = committed.find(snapshot.operation_id);
        Require(receipt != committed.end() && receipt->second.snapshot_id == snapshot.snapshot_id &&
                seen.insert(snapshot.operation_id).second && ids.insert(snapshot.snapshot_id).second,
            "CORRUPTION",
            "expired snapshot has no unique committed receipt");
    }
    for (const auto& [snapshotID, operationID] : archived) {
        const auto receipt = committed.find(operationID);
        Require(receipt != committed.end() && receipt->second.snapshot_id == snapshotID &&
                seen.insert(operationID).second && ids.insert(snapshotID).second,
            "CORRUPTION",
            "archived snapshot has no unique committed receipt");
    }
    for (const auto& snapshot : metadata.snapshots) {
        Require(snapshot != nullptr, "CORRUPTION", "retained snapshot is missing");
        const auto operation = snapshot->summary.find(std::string(OperationKey));
        Require(operation != snapshot->summary.end(), "CORRUPTION", "retained snapshot has no operation identity");
        const auto receipt = committed.find(operation->second);
        Require(receipt != committed.end() && seen.insert(operation->second).second &&
                ids.insert(snapshot->snapshot_id).second,
            "CORRUPTION",
            "retained snapshot has no unique committed receipt");
        CheckReceipt(*snapshot, receipt->second);
    }
    Require(seen.size() == committed.size(), "CORRUPTION", "committed receipt has no retained snapshot");
}

std::vector<ArchivedSnapshotRecord> ArchivedRecords(const std::shared_ptr<const NStorage::TArchiveCut>& cut,
    std::size_t expected)
{
    Require(expected == 0 || (cut && cut->Snapshots.size() == expected),
        "IO_ERROR",
        "publication did not verify its snapshot archive cut");
    std::vector<ArchivedSnapshotRecord> records;
    if (cut) {
        records.reserve(cut->Snapshots.size());
        for (const auto& snapshot : cut->Snapshots) {
            records.push_back(MakeArchivedRecord(snapshot));
        }
    }
    return records;
}

class TStagedNoOp final : public StagedAppend
{
public:
    TStagedNoOp(AppendResult receipt, std::string root, std::function<Status()> verify)
        : Receipt_(std::move(receipt))
        , Root_(std::move(root))
        , Verify_(std::move(verify))
    { }

    AppendResult Receipt() const override
    {
        return Receipt_;
    }

    bool IsNoOp() const override
    {
        return true;
    }

    iceberg::Status Publish(iceberg::sql::CatalogStore&) override
    {
        return iceberg::Invalid("KV no-op cannot publish an Iceberg pointer.");
    }

    Status VerifyClosure(std::string_view location) override
    {
        if (location != Root_) {
            return std::unexpected(ErrorDetail{"CORRUPTION", "KV no-op exact base changed", RetryHint::OperatorAction});
        }
        return Verify_();
    }

private:
    const AppendResult Receipt_;
    const std::string Root_;
    const std::function<Status()> Verify_;
};

class TStagedCreate final : public StagedCreate
{
public:
    TStagedCreate(std::shared_ptr<TLocalStorageBackend> owner,
        CollectionInfo info,
        std::string root,
        NStorage::TPublicationContext context,
        NStorage::TAttemptScope scope)
        : Owner_(std::move(owner))
        , Info_(std::move(info))
        , Root_(std::move(root))
        , Context_(std::move(context))
        , Scope_(std::move(scope))
    { }

    ~TStagedCreate() override
    {
        Owner_->DiscardCandidate(Info_.collection_id);
    }

    CollectionInfo Info() const override
    {
        return Info_;
    }

    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        return catalog.InsertTable(CatalogNamespace, Info_.name, Root_);
    }

    Status VerifyClosure(std::string_view location) override
    {
        return Guarded<Status>([&]() -> Status {
            Require(location == Root_, "IO_ERROR", "catalog published an unexpected collection root");
            NStorage::TStoragePublicationGuard guard(Context_);
            CheckIceberg(guard.BeforePointerChange({NStorage::EPointerOp::InsertTable,
                             std::string(CatalogNamespace),
                             Info_.name,
                             std::nullopt,
                             std::string(location)}),
                "IO_ERROR",
                "verify collection root");
            Owner_->RecordCandidate(Info_.collection_id, guard.GetCandidate());
            return {};
        });
    }

private:
    const std::shared_ptr<TLocalStorageBackend> Owner_;
    const CollectionInfo Info_;
    const std::string Root_;
    const NStorage::TPublicationContext Context_;
    // Keeps the attempt bound until publication finishes or fails.
    NStorage::TAttemptScope Scope_;
};

class TStagedAppend final : public StagedAppend
{
public:
    TStagedAppend(std::shared_ptr<TLocalStorageBackend> owner,
        std::string collectionID,
        std::string name,
        std::string baseLocation,
        std::string root,
        AppendResult receipt,
        NStorage::TPublicationContext context,
        std::shared_ptr<ResourceReservation> memory,
        NStorage::TAttemptScope scope)
        : Owner_(std::move(owner))
        , CollectionID_(std::move(collectionID))
        , Name_(std::move(name))
        , BaseLocation_(std::move(baseLocation))
        , Root_(std::move(root))
        , Receipt_(std::move(receipt))
        , Context_(std::move(context))
        , Memory_(std::move(memory))
        , Scope_(std::move(scope))
    { }

    ~TStagedAppend() override
    {
        Owner_->DiscardCandidate(CollectionID_);
    }

    AppendResult Receipt() const override
    {
        return Receipt_;
    }

    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        const auto updated =
            catalog.UpdateTableMetadataLocation(CatalogNamespace, Name_, Root_, BaseLocation_, BaseLocation_);
        if (!updated) {
            return std::unexpected(updated.error());
        }
        if (*updated != 1) {
            return iceberg::CommitFailed("catalog pointer changed before publication");
        }
        return {};
    }

    Status VerifyClosure(std::string_view location) override
    {
        return Guarded<Status>([&]() -> Status {
            Require(location == Root_, "IO_ERROR", "catalog published an unexpected append root");
            NStorage::TStoragePublicationGuard guard(Context_);
            CheckIceberg(guard.BeforePointerChange({NStorage::EPointerOp::UpdateLocation,
                             std::string(CatalogNamespace),
                             Name_,
                             BaseLocation_,
                             std::string(location)}),
                "IO_ERROR",
                "verify append root");
            Archived_ = ArchivedRecords(guard.GetArchiveCut(), Context_.ArchivePrefix);
            Owner_->RecordCandidate(CollectionID_, guard.GetCandidate());
            return {};
        });
    }

    std::vector<ArchivedSnapshotRecord> Archived() const override
    {
        return Archived_;
    }

private:
    std::vector<ArchivedSnapshotRecord> Archived_;
    const std::shared_ptr<TLocalStorageBackend> Owner_;
    const std::string CollectionID_;
    const std::string Name_;
    const std::string BaseLocation_;
    const std::string Root_;
    const AppendResult Receipt_;
    const NStorage::TPublicationContext Context_;
    // File metric maps, attempt descriptors and bounded manifest workspace.
    const std::shared_ptr<ResourceReservation> Memory_;
    NStorage::TAttemptScope Scope_;
};

class TStagedExpiration final : public StagedExpiration
{
public:
    std::shared_ptr<TLocalStorageBackend> Owner;
    std::string CollectionID, Name, BaseLocation, Root;
    SnapshotRetentionPolicy Retention;
    std::vector<ExpiredSnapshotRecord> Snapshots;
    std::vector<std::string> Files;
    NStorage::TPublicationContext Context;
    NStorage::TAttemptScope Scope;
    int64_t ExpiredFromArchive = 0;
    std::vector<ArchivedSnapshotRecord> ArchivedRows;

    int64_t ArchivedExpired() const override
    {
        return ExpiredFromArchive;
    }
    std::vector<ArchivedSnapshotRecord> Archived() const override
    {
        return ArchivedRows;
    }

    ~TStagedExpiration() override
    {
        if (!Root.empty()) {
            Owner->DiscardCandidate(CollectionID);
        }
    }
    std::span<const ExpiredSnapshotRecord> Expired() const override
    {
        return Snapshots;
    }
    std::span<const std::string> Garbage() const override
    {
        return Files;
    }
    SnapshotRetentionPolicy Policy() const override
    {
        return Retention;
    }
    bool Changed() const override
    {
        return !Root.empty();
    }
    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        const auto changed =
            catalog.UpdateTableMetadataLocation(CatalogNamespace, Name, Root, BaseLocation, BaseLocation);
        if (!changed) {
            return std::unexpected(changed.error());
        }
        if (*changed != 1) {
            return iceberg::ValidationFailed("Maintenance exact base changed.");
        }
        return {};
    }
    Status VerifyClosure(std::string_view location) override
    {
        return Guarded<Status>([&]() -> Status {
            Require(location == Root, "IO_ERROR", "unexpected maintenance root");
            NStorage::TStoragePublicationGuard guard(Context);
            CheckIceberg(
                guard.BeforePointerChange(
                    {NStorage::EPointerOp::UpdateLocation, std::string(CatalogNamespace), Name, BaseLocation, Root}),
                "IO_ERROR",
                "verify maintenance root");
            ArchivedRows = ArchivedRecords(guard.GetArchiveCut(), Context.ArchivePrefix);
            Owner->RecordCandidate(CollectionID, guard.GetCandidate());
            return {};
        });
    }
};

} // namespace

void TLocalStorageOptions::ApplyHistoryLimits(const HistoryLimits& limits)
{
    Storage.Store = {static_cast<uint64_t>(limits.max_root_bytes),
        static_cast<uint64_t>(limits.max_snapshots),
        static_cast<uint64_t>(limits.max_snapshots),
        static_cast<uint64_t>(limits.max_data_files),
        static_cast<uint64_t>(limits.max_manifests),
        static_cast<uint64_t>(limits.max_manifest_bytes),
        static_cast<uint64_t>(limits.max_manifest_list_bytes),
        static_cast<uint64_t>(limits.max_footer_bytes)};
    Storage.RootBytes = static_cast<uint64_t>(limits.max_root_file_bytes);
    Storage.MetadataLogEntries = static_cast<uint64_t>(limits.max_snapshots);
    Storage.CommitBytes = static_cast<uint64_t>(limits.max_commit_bytes);
    Storage.RecoveryWorkspaceBytes = static_cast<uint64_t>(limits.max_recovery_workspace_bytes);
    FileIO.MetadataBytes = Storage.RootBytes;
}

TLocalStorageBackend::TLocalStorageBackend(StoreInfo store,
    TLocalStorageOptions options,
    std::shared_ptr<NIO::TStoreDirs> dirs,
    std::shared_ptr<NStorage::TDurableLocalFileIO> fileIO,
    std::shared_ptr<TSnapshotMetadataCache> readCache)
    : Store_(std::move(store))
    , Options_(std::move(options))
    , Dirs_(std::move(dirs))
    , FileIO_(std::move(fileIO))
    , Catalog_(std::make_shared<TInertCatalog>())
    , ReadCache_(std::move(readCache))
{ }

EngineCapabilities TLocalStorageBackend::GetCapabilities() const
{
    EngineCapabilities capabilities{.backend = BackendKind::LocalStorage,
        .append = true,
        .scan = true,
        .metadata = true,
        .file_pruning = Options_.EnablePruning,
        .row_group_pruning = Options_.EnablePruning,
        .write_settings = true,
        .parquet_codecs = ParquetCodecCapabilities()};
    capabilities.key_value = true;
    AddEncodingCapabilities(capabilities);
    capabilities.storage_footprint = true;
    capabilities.snapshot_archive = true;
    return capabilities;
}

StorageBackend::Health TLocalStorageBackend::GetHealth() const
{
    const bool latched = Dirs_->Latch()->IsSet();
    return {true, !latched, latched};
}

Result<std::shared_ptr<TLocalStorageBackend>>
TLocalStorageBackend::Open(const std::filesystem::path& root, const StoreInfo& store, TLocalStorageOptions options)
{
    return Guarded<Result<std::shared_ptr<TLocalStorageBackend>>>(
        [&]() -> Result<std::shared_ptr<TLocalStorageBackend>> {
            options.Resources = options.Resources ? options.Resources : ResourceOwner::ProcessDefault();
            options.InputPool = MakeResourcePool(options.Resources, options.InputPool);
            std::shared_ptr<TSnapshotMetadataCache> readCache;
            if (options.ReadCacheBytes > 0) {
                auto cacheMemory = options.Resources->Reserve(ResourceClass::Metadata, options.ReadCacheBytes);
                if (!cacheMemory) {
                    throw Failure(cacheMemory.error());
                }
                readCache = std::make_shared<TSnapshotMetadataCache>(std::move(*cacheMemory),
                    TSnapshotMetadataCacheOptions{.MaxBytes = options.ReadCacheBytes});
            }
            auto fs = options.Fs ? options.Fs : std::make_shared<NIO::TPosixLocalFS>();
            auto dirs = TakeIO(NIO::TStoreDirs::Open(std::move(fs), root, std::make_shared<NIO::TWriteLatch>()),
                EStoragePath::Preflight,
                "open store directories");
            auto fileIO = TakeIceberg(NStorage::TDurableLocalFileIO::Make(dirs, store.store_id, options.FileIO),
                "IO_ERROR",
                "open durable FileIO");
            auto backend = std::shared_ptr<TLocalStorageBackend>(
                new TLocalStorageBackend(store, options, std::move(dirs), std::move(fileIO), std::move(readCache)));
            return backend;
        });
}

Result<std::unique_ptr<StagedCreate>> TLocalStorageBackend::PrepareCreate(const StoreInfo& store,
    const CreateCollectionRequest& request,
    std::string_view attempt_id)
{
    return Guarded<Result<std::unique_ptr<StagedCreate>>>([&]() -> Result<std::unique_ptr<StagedCreate>> {
        CheckStore(store);
        CheckWriteLatch(*Dirs_);
        auto profile = MakeProfile(request.schema, "INVALID_ARGUMENT", request.mode);
        (void)TakeIO(CheckWriteCapacity(false, false), EStoragePath::Preflight, "storage preflight");
        const auto minUsage =
            TakeUsage(NStorage::ReplaceUsage(ComputeStoreUsage(), {.Previous = {}, .Next = {.RootBytes = 1}}),
                "RESOURCE_EXHAUSTED",
                "storage capacity");
        CheckUsage(NStorage::CheckStorageCapacity(minUsage, Options_.Storage),
            "RESOURCE_EXHAUSTED",
            "storage capacity");
        TakeIO(Dirs_->CreateCollection(request.collection_id), EStoragePath::Write, "create collection directories");
        auto attempt = TakeIO(NIO::TIOAttempt::Make({Store_.store_id,
                                                        request.collection_id,
                                                        std::string(attempt_id),
                                                        NIO::EAttemptKind::CreateCollection,
                                                        std::nullopt},
                                  Dirs_->Latch()),
            EStoragePath::Write,
            "create attempt");
        auto scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind create attempt");

        const auto location = TableLocation(Store_.store_id, request.collection_id);
        const auto metadata = TakeIceberg(iceberg::TableMetadata::Make(*profile.Schema,
                                              *iceberg::PartitionSpec::Unpartitioned(),
                                              *iceberg::SortOrder::Unsorted(),
                                              location,
                                              profile.Properties),
            "IO_ERROR",
            "build initial metadata");
        auto root = MetadataLocation(location, 0, attempt_id);
        CheckIceberg(iceberg::TableMetadataUtil::Write(*FileIO_, root, *metadata),
            "IO_ERROR",
            "write initial metadata");

        CollectionInfo info{request.collection_id,
            request.exact_name,
            metadata->table_uuid,
            request.schema,
            0,
            {},
            request.write_settings,
            0,
            request.mode};
        NStorage::TPublicationContext context{.Directories = Dirs_,
            .FileIO = FileIO_,
            .Attempt = attempt,
            .Namespace = std::string(CatalogNamespace),
            .Name = request.exact_name,
            .Profile = std::move(profile),
            .Base = {},
            .ExpectedSummary = {},
            .StoreUsage = ComputeStoreUsage(),
            .Limits = Options_.Storage};
        context.StoreLimits = Options_.Storage;
        return std::make_unique<TStagedCreate>(shared_from_this(),
            std::move(info),
            std::move(root),
            std::move(context),
            std::move(scope));
    });
}

Status TLocalStorageBackend::CheckAppendAdmissible(const StoreInfo& store,
    const CollectionInfo& collection,
    const BaseIdentity& base,
    NormalizedInput& input,
    std::chrono::steady_clock::time_point deadline,
    const std::function<bool()>& cancelled)
{
    return Guarded<Status>([&]() -> Status {
        CheckStore(store);
        CheckWriteLatch(*Dirs_);
        Require(base.collection_id == collection.collection_id && base.table_uuid == collection.table_uuid &&
                base.store_id == Store_.store_id,
            "INVALID_ARGUMENT",
            "base does not match the collection");
        Require((collection.mode == CollectionMode::KeyValue) == static_cast<bool>(input.kv),
            "INVALID_ARGUMENT",
            "collection mode does not support the requested mutation");
        (void)MakeProfile(collection.schema, "INVALID_ARGUMENT", collection.mode);
        if (input.kv) {
            CheckKvInput(collection, input, Options_.Writer);
            InitializeInventory();
            (void)TakeIO(CheckWriteCapacity(true, false, deadline, cancelled),
                EStoragePath::Preflight,
                "KV storage preflight");
            const auto current = ResolveBase(base);
            CheckSchema(*current->Metadata, collection.schema, collection.mode);
            if (input.bulk || input.GetRowCount() == 0) {
                return {};
            }
            const auto schema = MutationSchema(collection, input);
            auto batch = input.Columns ? CopyRecordBatch(*input.Columns, Options_.InputPool.get())
                                       : MakeRecordBatch(schema, input.rows, Options_.InputPool.get());
            if (!batch) {
                throw Failure(batch.error());
            }
            auto owner = std::make_shared<TAdmittedBatch>(Options_.InputPool, std::move(*batch));
            input.writer_batch = std::shared_ptr<arrow::RecordBatch>(owner, owner->Batch.get());
            return {};
        }
        if (input.bulk) {
            CheckBulkInput(input, Options_.Writer);
        } else {
            CheckWriterInput(collection.schema, input, Options_.Writer);
        }
        InitializeInventory();
        (void)TakeIO(CheckWriteCapacity(true, false, deadline, cancelled),
            EStoragePath::Preflight,
            "storage preflight");
        // Every append writes a strictly larger root with one more snapshot,
        // data file, manifest and manifest list. The pinned Iceberg builder
        // never records a previous metadata location for these commits, so the
        // next root carries the current metadata log unchanged.
        const auto current = ResolveBase(base);
        const auto collectionLimits = Options_.Storage;
        Require(current->CurrentManifests.size() < collectionLimits.Store.Manifests,
            "RESOURCE_EXHAUSTED",
            "collection manifest inventory is full");
        if (current->Metadata->metadata_log.size() > Options_.Storage.MetadataLogEntries) {
            Fail("RESOURCE_EXHAUSTED", "storage capacity", "collection metadata history is full");
        }
        if (current->Usage.RootBytes >= Options_.Storage.RootBytes) {
            Fail("RESOURCE_EXHAUSTED", "storage capacity", "collection root is at its byte limit");
        }
        // An append that moves snapshots to the archive does not grow the root.
        const auto rootGrowth =
            NStorage::AppendArchiveCut(current->Metadata->snapshots.size() + 1, collection.hot_snapshots) == 0 ? 1U
                                                                                                               : 0U;
        const auto minUsage = TakeUsage(
            NStorage::ReplaceUsage(ComputeStoreUsage(),
                {.Previous = {},
                    .Next = {.RootBytes = 1,
                        .Snapshots = rootGrowth,
                        .SnapshotLogEntries = rootGrowth,
                        .DataFiles = input.bulk
                            ? static_cast<uint64_t>((input.bulk->Rows - 1) / BulkAppendLimits::MaxFileRows + 1)
                            : 1,
                        .Manifests = 1,
                        .ManifestBytes = 1,
                        .ManifestListBytes = 1,
                        .FooterBytes = 1}}),
            "RESOURCE_EXHAUSTED",
            "storage capacity");
        CheckUsage(NStorage::CheckStorageCapacity(minUsage, Options_.Storage),
            "RESOURCE_EXHAUSTED",
            "storage capacity");
        if (input.bulk) {
            return {};
        }
        auto batch = input.Columns != nullptr
            ? CopyRecordBatch(*input.Columns, Options_.InputPool.get())
            : MakeRecordBatch(collection.schema, input.rows, Options_.InputPool.get());
        if (!batch) {
            throw Failure(batch.error());
        }
        auto owner = std::make_shared<TAdmittedBatch>(Options_.InputPool, std::move(*batch));
        input.writer_batch = std::shared_ptr<arrow::RecordBatch>(owner, owner->Batch.get());
        return {};
    });
}

Result<std::unique_ptr<StagedAppend>> TLocalStorageBackend::PrepareAppend(const StoreInfo& store,
    const BoundOperation& binding,
    const CollectionInfo& collection,
    const NormalizedInput& input,
    std::chrono::steady_clock::time_point deadline)
{
    return Guarded<Result<std::unique_ptr<StagedAppend>>>([&]() -> Result<std::unique_ptr<StagedAppend>> {
        CheckStore(store);
        CheckWriteLatch(*Dirs_);
        Require(binding.collection_id == collection.collection_id &&
                binding.base.collection_id == collection.collection_id &&
                binding.base.table_uuid == collection.table_uuid && binding.base.store_id == Store_.store_id,
            "INVALID_ARGUMENT",
            "binding does not match the collection");
        Require((collection.mode == CollectionMode::KeyValue) == static_cast<bool>(input.kv),
            "INVALID_ARGUMENT",
            "collection mode does not support the requested mutation");
        if (input.kv) {
            return PrepareKvAppend(binding, collection, input, deadline);
        }
        if (input.bulk) {
            CheckBulkInput(input, Options_.Writer);
        } else {
            CheckWriterInput(collection.schema, input, Options_.Writer);
        }
        const auto base = ResolveBase(binding.base);
        CheckDeadline(deadline);
        // A conservative bound from this append's admission (or a new walk), taken
        // before any file of this attempt exists; publication adds the attempt's
        // own files to it instead of walking the warehouse again.
        const auto warehouseBytes =
            TakeIO(CheckWriteCapacity(false, true, deadline), EStoragePath::Preflight, "storage preflight");
        std::optional<NStorage::TWarehouseBaseline> warehouse;
        std::optional<NStorage::TWarehouseBaseline> productionBaseline;
        if (!Options_.WalkWarehouseAtPublication || input.bulk) {
            productionBaseline = NStorage::TWarehouseBaseline{.Bytes = warehouseBytes,
                .Directories = TakeIO(NStorage::MeasureCollectionDirs(*Dirs_, binding.collection_id),
                    EStoragePath::Preflight,
                    "storage preflight")};
        }
        if (!Options_.WalkWarehouseAtPublication) {
            warehouse = productionBaseline;
        }

        std::shared_ptr<ResourceReservation> bulkMemory;
        if (input.bulk) {
            auto memory = Options_.Resources->Reserve(ResourceClass::Metadata, 32ULL << 20);
            if (!memory) {
                throw Failure(memory.error());
            }
            bulkMemory = std::move(*memory);
        }

        auto attempt = TakeIO(NIO::TIOAttempt::Make({Store_.store_id,
                                                        binding.collection_id,
                                                        binding.attempt_id,
                                                        NIO::EAttemptKind::Append,
                                                        binding.operation_id},
                                  Dirs_->Latch(),
                                  input.bulk ? NIO::TAttemptLimits::BulkAppend() : NIO::TAttemptLimits{}),
            EStoragePath::Write,
            "create attempt");
        auto scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind append attempt");
        const auto location = TableLocation(Store_.store_id, binding.collection_id);
        std::vector<std::shared_ptr<iceberg::DataFile>> dataFiles;
        const auto inputRows = input.bulk ? input.bulk->Rows : static_cast<int64_t>(input.GetRowCount());
        if (!input.bulk) {
            Require(input.writer_batch != nullptr, "INTERNAL", "append input was not admitted");
            const auto dataPath =
                std::format("{}/data/{}-{}-00000.parquet", location, binding.operation_id, binding.attempt_id);
            auto data = TakeIceberg(NStorage::WriteDataFile(FileIO_,
                                        attempt,
                                        *base->Metadata,
                                        dataPath,
                                        {input.writer_batch},
                                        Options_.Writer,
                                        WriterSpec(collection.schema, collection.write_settings)),
                "IO_ERROR",
                "write data file");
            Require(data.DataFile && data.DataFile->record_count == inputRows,
                "IO_ERROR",
                "data file does not contain every input row");
            dataFiles.push_back(std::move(data.DataFile));
        } else {
            auto limits = Options_.Writer;
            limits.MaxRows = BulkAppendLimits::MaxFileRows;
            limits.MaxInputBytes = std::min(limits.MaxInputBytes, BulkAppendLimits::MaxFileInputBytes);
            limits.MaxRowGroupBytes = std::min<int64_t>(limits.MaxRowGroupBytes, 8LL << 20);
            limits.MaxFileBytes = std::min<int64_t>(limits.MaxFileBytes, 32LL << 20);
            dataFiles.reserve(static_cast<size_t>(BulkAppendLimits::MaxFiles));
            int64_t totalRows = 0;
            uint64_t descriptorBytes = dataFiles.capacity() * sizeof(std::shared_ptr<iceberg::DataFile>);
            TBoundedFileWindow window(
                {limits.MaxRows, limits.MaxInputBytes, BulkAppendLimits::TargetFileBytes},
                Options_.InputPool.get(),
                [&](const TBoundedFileWindow::TBatches& batches, int64_t fileRows) {
                    if (dataFiles.size() >= static_cast<size_t>(BulkAppendLimits::MaxFiles)) {
                        Fail("RESOURCE_EXHAUSTED", "bulk append", "operation exceeds its data file limit");
                    }
                    CheckDeadline(deadline);
                    const auto path = std::format("{}/data/{}-{}-{:05}.parquet",
                        location,
                        binding.operation_id,
                        binding.attempt_id,
                        dataFiles.size());
                    auto data = TakeIceberg(NStorage::WriteDataFile(FileIO_,
                                                attempt,
                                                *base->Metadata,
                                                path,
                                                batches,
                                                limits,
                                                WriterSpec(collection.schema, collection.write_settings)),
                        "IO_ERROR",
                        "write bulk data file");
                    Require(data.DataFile && data.DataFile->record_count == fileRows,
                        "IO_ERROR",
                        "bulk data file does not contain every input row");
                    AddMemory(descriptorBytes, DataFileMemory(*data.DataFile));
                    Require(descriptorBytes <= bulkMemory->GetBytes(),
                        "RESOURCE_EXHAUSTED",
                        "bulk data file metadata exceeds its memory envelope");
                    dataFiles.push_back(std::move(data.DataFile));
                    // All files produced so far are durable, so the same physical
                    // accounting used by final publication can bound the stream.
                    CheckIO(Dirs_->CheckIdentity(), EStoragePath::Preflight, "bulk storage capacity");
                    const auto physical = TakeIO(
                        NStorage::EstimateAttemptWarehouse(*Dirs_, *productionBaseline, *attempt, Options_.Storage),
                        EStoragePath::Preflight,
                        "bulk storage capacity");
                    const auto space =
                        TakeIO(Dirs_->Fs()->StatFs(Dirs_->Root()), EStoragePath::Preflight, "bulk storage capacity");
                    CheckIO(NStorage::CheckDiskReserve(space, physical, Options_.Storage),
                        EStoragePath::Preflight,
                        "bulk storage capacity");
                    CheckDeadline(deadline);
                },
                [&] {
                    CheckDeadline(deadline);
                });
            while (true) {
                CheckDeadline(deadline);
                auto next = input.bulk->Next();
                if (!next) {
                    throw Failure(next.error());
                }
                CheckDeadline(deadline);
                if (next->GetRowCount() == 0) {
                    Require(totalRows == inputRows, "INVALID_ARGUMENT", "bulk source row count changed");
                    break;
                }
                CheckWriterInput(collection.schema, *next, Options_.Writer);
                const auto rows = static_cast<int64_t>(next->GetRowCount());
                Require(rows <= inputRows - totalRows, "INVALID_ARGUMENT", "bulk source exceeded its row count");
                auto built = next->Columns != nullptr
                    ? CopyRecordBatch(*next->Columns, Options_.InputPool.get())
                    : MakeRecordBatch(collection.schema, next->rows, Options_.InputPool.get());
                if (!built) {
                    throw Failure(built.error());
                }
                next->Columns.reset();
                next->rows.clear();
                window.Add(*built);
                totalRows += rows;
            }
            window.Flush();
        }
        CheckDeadline(deadline);

        int64_t compressedBytes = 0;
        for (const auto& file : dataFiles) {
            Require(file && file->file_size_in_bytes > 0 && file->file_size_in_bytes <= (32LL << 20) &&
                    file->file_size_in_bytes <= std::numeric_limits<int64_t>::max() - compressedBytes,
                "IO_ERROR",
                "append produced an invalid data file length");
            compressedBytes += file->file_size_in_bytes;
        }

        auto table = TakeIceberg(
            iceberg::Table::Make(
                iceberg::TableIdentifier{.ns = iceberg::Namespace{.levels = {std::string(CatalogNamespace)}},
                    .name = collection.name},
                std::make_shared<iceberg::TableMetadata>(*base->Metadata),
                base->MetadataLocation,
                FileIO_,
                Catalog_),
            "IO_ERROR",
            "load base table");
        std::unordered_map<std::string, std::string> summary{{std::string(OperationKey), binding.operation_id},
            {std::string(ContentHashKey), binding.payload_hash}};
        if (input.bulk) {
            summary.emplace(BulkVersionKey, "1");
        }
        auto staged =
            TakeIceberg(input.bulk ? NStorage::StageBulkAppend(std::move(table), dataFiles, summary, attempt)
                                   : NStorage::StageAppend(std::move(table), dataFiles.front(), summary, attempt),
                "IO_ERROR",
                "stage append");
        auto root = MetadataLocation(location, MetadataVersion(base->MetadataLocation) + 1, binding.attempt_id);
        // Serialize the staged root once, exactly as TableMetadataUtil::Write
        // does, and hand both forms to the guard, which compares the durable
        // file with these bytes instead of parsing it again. With
        // hot_snapshots the oldest snapshots leave this root for the archive,
        // recorded by the same outer transaction.
        auto writable = std::make_shared<iceberg::TableMetadata>(staged.Transaction->current());
        const auto archived = NStorage::AppendArchiveCut(writable->snapshots.size(), collection.hot_snapshots);
        Require(archived < writable->snapshots.size() && writable->snapshots.size() == writable->snapshot_log.size(),
            "IO_ERROR",
            "staged root cannot move its snapshots to the archive");
        writable->snapshots.erase(writable->snapshots.begin(),
            writable->snapshots.begin() + static_cast<std::ptrdiff_t>(archived));
        writable->snapshot_log.erase(writable->snapshot_log.begin(),
            writable->snapshot_log.begin() + static_cast<std::ptrdiff_t>(archived));
        auto stagedMetadata = std::shared_ptr<const iceberg::TableMetadata>(std::move(writable));
        auto stagedBytes = std::make_shared<const std::string>(
            TakeIceberg(NStorage::SerializeTableMetadata(*stagedMetadata), "IO_ERROR", "serialize append metadata"));
        CheckIceberg(FileIO_->WriteFile(root, *stagedBytes), "IO_ERROR", "write append metadata");
        const auto snapshot = TakeIceberg(staged.Transaction->current().SnapshotById(staged.SnapshotID),
            "IO_ERROR",
            "read staged snapshot");
        Require(staged.ParentID == binding.base.current_snapshot_id,
            "IO_ERROR",
            "staged snapshot has an unexpected parent");

        AppendResult receipt{.collection_id = binding.collection_id,
            .operation_id = binding.operation_id,
            .payload_hash = binding.payload_hash,
            .hash_version = 1,
            .snapshot_id = staged.SnapshotID,
            .parent_snapshot_id = staged.ParentID,
            .rows_appended = inputRows,
            .data_files_added = static_cast<int64_t>(dataFiles.size()),
            .compressed_data_bytes_added = compressedBytes,
            .snapshot_created_at_us = SnapshotTimeUs(*snapshot, "IO_ERROR"),
            .durability_profile = Store_.profile_id,
            .receipt_version = input.bulk ? 2 : 1};
        NStorage::TPublicationContext context{.Directories = Dirs_,
            .FileIO = FileIO_,
            .Attempt = attempt,
            .Namespace = std::string(CatalogNamespace),
            .Name = collection.name,
            .Profile = MakeProfile(collection.schema, "CORRUPTION"),
            .Base = base,
            .ExpectedSummary = summary,
            .StoreUsage = ComputeStoreUsage(),
            .Limits = Options_.Storage,
            .Warehouse = warehouse,
            .StagedRoot = NStorage::TStagedRoot{.Metadata = std::move(stagedMetadata), .Bytes = std::move(stagedBytes)},
            .VerifyStagedRoot = Options_.VerifyStagedRoot};
        context.StoreLimits = Options_.Storage;
        context.ArchivePrefix = archived;
        return std::make_unique<TStagedAppend>(shared_from_this(),
            binding.collection_id,
            collection.name,
            binding.base.metadata_location,
            std::move(root),
            std::move(receipt),
            std::move(context),
            std::move(bulkMemory),
            std::move(scope));
    });
}

Result<std::unique_ptr<StagedAppend>> TLocalStorageBackend::PrepareKvAppend(const BoundOperation& binding,
    const CollectionInfo& collection,
    const NormalizedInput& input,
    std::chrono::steady_clock::time_point deadline)
{
    CheckKvInput(collection, input, Options_.Writer);
    const auto base = ResolveBase(binding.base);
    CheckSchema(*base->Metadata, collection.schema, collection.mode);
    const auto cancelled = [&] {
        return input.kv->Cancellation && input.kv->Cancellation->load(std::memory_order_relaxed);
    };
    const auto check = [&] {
        CheckDeadline(deadline);
        Require(!cancelled(), "CANCELLED", "KV mutation was cancelled");
    };
    check();
    const auto warehouseBytes =
        TakeIO(CheckWriteCapacity(false, true, deadline, cancelled), EStoragePath::Preflight, "KV storage preflight");
    const auto productionBaseline = NStorage::TWarehouseBaseline{.Bytes = warehouseBytes,
        .Directories = TakeIO(NStorage::MeasureCollectionDirs(*Dirs_, binding.collection_id),
            EStoragePath::Preflight,
            "KV storage preflight")};
    // Routing copies scale with configured history. The existing index's
    // complete charged envelope conservatively covers live range copies;
    // scratch, pending rows and output descriptors retain their fixed margin.
    uint64_t temporaryBytes = 128ULL << 20;
    AddMemory(temporaryBytes, ScanIndexMemory(*base));
    auto memory = Options_.Resources->Reserve(ResourceClass::Metadata, temporaryBytes);
    if (!memory) {
        throw Failure(memory.error());
    }
    auto attempt = TakeIO(NIO::TIOAttempt::Make({Store_.store_id,
                                                    binding.collection_id,
                                                    binding.attempt_id,
                                                    NIO::EAttemptKind::Append,
                                                    binding.operation_id},
                              Dirs_->Latch(),
                              NIO::TAttemptLimits::KeyValue()),
        EStoragePath::Write,
        "create KV attempt");
    auto scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind KV attempt");
    const auto capacity = [&]() -> Status {
        return Guarded<Status>([&]() -> Status {
            CheckIO(Dirs_->CheckIdentity(), EStoragePath::Preflight, "KV storage capacity");
            const auto physical =
                TakeIO(NStorage::EstimateAttemptWarehouse(*Dirs_, productionBaseline, *attempt, Options_.Storage),
                    EStoragePath::Preflight,
                    "KV storage capacity");
            const auto space =
                TakeIO(Dirs_->Fs()->StatFs(Dirs_->Root()), EStoragePath::Preflight, "KV storage capacity");
            CheckIO(NStorage::CheckDiskReserve(space, physical, Options_.Storage),
                EStoragePath::Preflight,
                "KV storage capacity");
            CheckDeadline(deadline);
            return {};
        });
    };
    auto production = ProduceKvMutation({.Collection = collection,
        .Binding = binding,
        .Input = input,
        .Base = *base,
        .FileIO = FileIO_,
        .Attempt = attempt,
        .Pool = Options_.InputPool.get(),
        .ScratchDirectory = Dirs_->Path(),
        .Writer = Options_.Writer,
        .WriteSpec = WriterSpec(collection.schema, collection.write_settings),
        .Deadline = deadline,
        .CheckCapacity = capacity});
    const bool noOp = production.Added.empty() && production.Removed.empty();
    AppendResult receipt{.collection_id = binding.collection_id,
        .operation_id = binding.operation_id,
        .payload_hash = binding.payload_hash,
        .hash_version = 1,
        .snapshot_id = binding.base.current_snapshot_id.value_or(0),
        .parent_snapshot_id = binding.base.current_snapshot_id,
        .rows_appended = 0,
        .data_files_added = static_cast<int64_t>(production.Added.size()),
        .compressed_data_bytes_added = 0,
        .snapshot_created_at_us = 0,
        .durability_profile = Store_.profile_id,
        .receipt_version = 3,
        .mutation_kind = input.kv->Kind,
        .snapshot_created = !noOp,
        .input_rows = input.kv->InputRows,
        .rows_affected = production.RowsAffected,
        .rows_written = production.RowsWritten,
        .rows_removed = production.RowsRemoved,
        .data_files_removed = static_cast<int64_t>(production.Removed.size()),
        .compressed_data_bytes_removed = 0};
    if (noOp) {
        CheckIO(attempt->Seal(), EStoragePath::Write, "seal KV no-op verification");
        return std::make_unique<TStagedNoOp>(std::move(receipt),
            binding.base.metadata_location,
            [owner = shared_from_this(),
                expected = binding.base,
                schema = collection.schema,
                mode = collection.mode]() {
                return Guarded<Status>([&]() -> Status {
                    CheckWriteLatch(*owner->Dirs_);
                    CheckIO(owner->Dirs_->CheckIdentity(), EStoragePath::Read, "verify KV no-op base");
                    const auto confirmed = owner->ResolveBase(expected);
                    CheckSchema(*confirmed->Metadata, schema, mode);
                    const std::shared_ptr<iceberg::FileIO> view =
                        TakeIceberg(owner->FileIO_->ReadOnlyView(), "IO_ERROR", "open KV no-op base verification");
                    auto actual = TakeRetained(NStorage::ReadTableMetadata(*view,
                                                   expected.metadata_location,
                                                   std::nullopt,
                                                   static_cast<int64_t>(owner->Options_.Storage.RootBytes)),
                        "verify durable KV no-op base");
                    Require(actual->table_uuid == expected.table_uuid &&
                            CurrentSnapshot(*actual) == expected.current_snapshot_id,
                        "CORRUPTION",
                        "durable KV no-op root differs from its prepared identity");
                    CheckSchema(*actual, schema, mode);
                    CheckNoOpRootPrefix(*actual, *confirmed->Metadata);
                    owner->PinNoOpRoot(expected.metadata_location);
                    return {};
                });
            });
    }
    uint64_t descriptorBytes = 0;
    const auto count = [&](const auto& files, int64_t& compressed) {
        for (const auto& file : files) {
            Require(file && file->file_size_in_bytes > 0 && file->file_size_in_bytes <= (32LL << 20) &&
                    file->file_size_in_bytes <= std::numeric_limits<int64_t>::max() - compressed,
                "IO_ERROR",
                "KV producer returned an invalid data file length");
            compressed += file->file_size_in_bytes;
            AddMemory(descriptorBytes, DataFileMemory(*file));
        }
    };
    count(production.Added, receipt.compressed_data_bytes_added);
    count(production.Removed, receipt.compressed_data_bytes_removed);
    Require(descriptorBytes <= (*memory)->GetBytes(),
        "RESOURCE_EXHAUSTED",
        "KV data file descriptors exceed the metadata memory envelope");
    auto table =
        TakeIceberg(iceberg::Table::Make(
                        iceberg::TableIdentifier{.ns = iceberg::Namespace{.levels = {std::string(CatalogNamespace)}},
                            .name = collection.name},
                        std::make_shared<iceberg::TableMetadata>(*base->Metadata),
                        base->MetadataLocation,
                        FileIO_,
                        Catalog_),
            "IO_ERROR",
            "load KV base table");
    const std::unordered_map<std::string, std::string> summary{{std::string(OperationKey), binding.operation_id},
        {std::string(ContentHashKey), binding.payload_hash},
        {std::string(KvVersionKey), "1"},
        {std::string(KvKindKey), std::string(MutationName(input.kv->Kind))},
        {std::string(KvInputRowsKey), std::to_string(input.kv->InputRows)},
        {std::string(KvAffectedRowsKey), std::to_string(production.RowsAffected)}};
    auto staged = TakeIceberg(NStorage::StageKVOverwrite(std::move(table),
                                  production.Added,
                                  production.Removed,
                                  summary,
                                  attempt,
                                  input.kv->Kind == MutationKind::Compact),
        "IO_ERROR",
        "stage KV mutation");
    const auto location = TableLocation(Store_.store_id, binding.collection_id);
    auto root = MetadataLocation(location, MetadataVersion(base->MetadataLocation) + 1, binding.attempt_id);
    auto stagedMetadata = std::make_shared<const iceberg::TableMetadata>(staged.Transaction->current());
    auto stagedBytes = std::make_shared<const std::string>(
        TakeIceberg(NStorage::SerializeTableMetadata(*stagedMetadata), "IO_ERROR", "serialize KV metadata"));
    CheckIceberg(FileIO_->WriteFile(root, *stagedBytes), "IO_ERROR", "write KV metadata");
    const auto snapshot = TakeIceberg(staged.Transaction->current().SnapshotById(staged.SnapshotID),
        "IO_ERROR",
        "read staged KV snapshot");
    Require(staged.ParentID == binding.base.current_snapshot_id,
        "IO_ERROR",
        "staged KV snapshot has an unexpected parent");
    receipt.snapshot_id = staged.SnapshotID;
    receipt.parent_snapshot_id = staged.ParentID;
    receipt.snapshot_created_at_us = SnapshotTimeUs(*snapshot, "IO_ERROR");
    NStorage::TPublicationContext context{.Directories = Dirs_,
        .FileIO = FileIO_,
        .Attempt = attempt,
        .Namespace = std::string(CatalogNamespace),
        .Name = collection.name,
        .Profile = MakeProfile(collection.schema, "CORRUPTION", collection.mode),
        .Base = base,
        .ExpectedSummary = summary,
        .StoreUsage = ComputeStoreUsage(),
        .Limits = Options_.Storage,
        .Warehouse = Options_.WalkWarehouseAtPublication ? std::nullopt : std::optional(productionBaseline),
        .StagedRoot = NStorage::TStagedRoot{.Metadata = std::move(stagedMetadata), .Bytes = std::move(stagedBytes)},
        .VerifyStagedRoot = Options_.VerifyStagedRoot};
    context.StoreLimits = Options_.Storage;
    return std::make_unique<TStagedAppend>(shared_from_this(),
        binding.collection_id,
        collection.name,
        binding.base.metadata_location,
        std::move(root),
        std::move(receipt),
        std::move(context),
        std::move(*memory),
        std::move(scope));
}

Result<std::unique_ptr<StagedExpiration>> TLocalStorageBackend::PrepareExpiration(const StoreInfo& store,
    const CollectionInfo& collection,
    const BaseIdentity& identity,
    const std::optional<SnapshotRetentionPolicy>& requested,
    std::span<const ArchivedSnapshotRecord> oldestArchived)
{
    return Guarded<Result<std::unique_ptr<StagedExpiration>>>([&]() -> Result<std::unique_ptr<StagedExpiration>> {
        Require(collection.mode == CollectionMode::Append,
            "COLLECTION_MODE_MISMATCH",
            "snapshot retention is unsupported for KV collections");
        CheckStore(store);
        CheckWriteLatch(*Dirs_);
        const auto base = ResolveBase(identity);
        Require(!NStorage::IsKVMetadata(*base->Metadata), "CORRUPTION", "snapshot retention requires append metadata");
        auto policy = TakeRetained(NStorage::ReadRetentionPolicy(*base->Metadata), "read snapshot retention");
        if (requested) {
            policy = {requested->keep_last, requested->min_age.count()};
        }
        CheckIceberg(NStorage::CheckRetentionPolicy(policy), "INVALID_ARGUMENT", "snapshot retention");
        const auto now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        // Retention spans the archive and the root: archived snapshots are
        // older than every root snapshot and expire first.
        const auto& snapshots = base->Metadata->snapshots;
        const auto watermark = TakeRetained(NStorage::ExpiredSequence(*base->Metadata), "read expiration watermark");
        const auto archivedTotal = snapshots.empty() ? 0 : snapshots.front()->sequence_number - 1 - watermark;
        Require(archivedTotal >= 0 &&
                oldestArchived.size() ==
                    std::min<uint64_t>(static_cast<uint64_t>(archivedTotal), MaxExpiredArchivePerCall + 1U),
            "CORRUPTION",
            "snapshot archive differs from the retained root");
        std::vector<int64_t> timestamps;
        timestamps.reserve(oldestArchived.size());
        for (size_t index = 0; index < oldestArchived.size(); ++index) {
            const auto& row = oldestArchived[index];
            Require(row.sequence_number == watermark + 1 + static_cast<int64_t>(index),
                "CORRUPTION",
                "snapshot archive is not contiguous with the expiration ledger");
            if (index < static_cast<size_t>(MaxExpiredArchivePerCall)) {
                timestamps.push_back(row.timestamp_ms);
            }
        }
        const auto plan = TakeIceberg(
            NStorage::PlanExpiration(*base->Metadata, timestamps, static_cast<uint64_t>(archivedTotal), policy, now),
            "INVALID_ARGUMENT",
            "select snapshot expiration");
        const auto count = plan.Root;
        auto staged = std::make_unique<TStagedExpiration>();
        staged->Owner = shared_from_this();
        staged->CollectionID = collection.collection_id;
        staged->Name = collection.name;
        staged->BaseLocation = identity.metadata_location;
        staged->Retention = {policy.KeepLast, std::chrono::milliseconds(policy.MinAgeMs)};
        const auto previousPolicy =
            TakeRetained(NStorage::ReadRetentionPolicy(*base->Metadata), "read snapshot retention");
        if (count == 0 && plan.Archived == 0 && previousPolicy == policy) {
            return staged;
        }
        (void)TakeIO(CheckWriteCapacity(false, false), EStoragePath::Preflight, "maintenance preflight");
        auto metadata = std::make_shared<iceberg::TableMetadata>(*base->Metadata);
        auto expiredSequence = watermark;
        std::vector<NStorage::TArchivedList> expiredArchive;
        for (size_t i = 0; i < plan.Archived; ++i) {
            const auto& row = oldestArchived[i];
            staged->Snapshots.push_back({row.snapshot_id, row.sequence_number, row.operation_id});
            expiredArchive.push_back(ArchivedListIdentity(row));
            expiredSequence = row.sequence_number;
        }
        staged->ExpiredFromArchive = static_cast<int64_t>(plan.Archived);
        std::optional<NStorage::TArchivedList> remainingArchive;
        if (plan.Archived < static_cast<size_t>(archivedTotal)) {
            remainingArchive = ArchivedListIdentity(oldestArchived[plan.Archived]);
        }
        for (std::size_t i = 0; i < count; ++i) {
            const auto& snapshot = metadata->snapshots[i];
            staged->Snapshots.push_back(
                {snapshot->snapshot_id, snapshot->sequence_number, snapshot->summary.at(std::string(OperationKey))});
            expiredSequence = snapshot->sequence_number;
        }
        const auto retirement = TakeIceberg(NStorage::FindExpiredMetadata(FileIO_,
                                                *base->Metadata,
                                                count,
                                                Options_.Storage,
                                                expiredArchive,
                                                remainingArchive ? &*remainingArchive : nullptr),
            "IO_ERROR",
            "find expired metadata");
        staged->Files.reserve(retirement.Files.size());
        for (const auto& file : retirement.Files) {
            staged->Files.push_back(file.Path);
        }
        metadata->snapshots.erase(metadata->snapshots.begin(),
            metadata->snapshots.begin() + static_cast<std::ptrdiff_t>(count));
        metadata->snapshot_log.erase(metadata->snapshot_log.begin(),
            metadata->snapshot_log.begin() + static_cast<std::ptrdiff_t>(count));
        CheckIceberg(NStorage::WriteRetentionPolicy(*metadata, policy, expiredSequence),
            "INVALID_ARGUMENT",
            "persist snapshot retention");
        const auto attemptID = NewUuid();
        auto attempt = TakeIO(
            NIO::TIOAttempt::Make(
                {Store_.store_id, collection.collection_id, attemptID, NIO::EAttemptKind::Maintenance, std::nullopt},
                Dirs_->Latch()),
            EStoragePath::Write,
            "maintenance attempt");
        staged->Scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind maintenance attempt");
        staged->Root = MetadataLocation(metadata->location, MetadataVersion(identity.metadata_location) + 1, attemptID);
        CheckIceberg(iceberg::TableMetadataUtil::Write(*FileIO_, staged->Root, *metadata),
            "IO_ERROR",
            "write maintenance root");
        auto limits = Options_.Storage;
        staged->Context = {.Directories = Dirs_,
            .FileIO = FileIO_,
            .Attempt = attempt,
            .Namespace = std::string(CatalogNamespace),
            .Name = collection.name,
            .Profile = MakeProfile(collection.schema, "CORRUPTION"),
            .Base = base,
            .ExpectedSummary = {},
            .StoreUsage = ComputeStoreUsage(),
            .Limits = limits};
        staged->Context.StoreLimits = limits;
        staged->Context.RetentionPolicy = policy;
        staged->Context.ExpirationPrefix = count;
        staged->Context.ExpiredArchive = std::move(expiredArchive);
        staged->Context.RemainingArchive = std::move(remainingArchive);
        return staged;
    });
}

Result<std::unique_ptr<StagedExpiration>> TLocalStorageBackend::PrepareArchive(const StoreInfo& store,
    const CollectionInfo& collection,
    const BaseIdentity& identity)
{
    return Guarded<Result<std::unique_ptr<StagedExpiration>>>([&]() -> Result<std::unique_ptr<StagedExpiration>> {
        Require(collection.mode == CollectionMode::Append,
            "COLLECTION_MODE_MISMATCH",
            "the snapshot archive is unsupported for KV collections");
        CheckStore(store);
        CheckWriteLatch(*Dirs_);
        const auto base = ResolveBase(identity);
        Require(!NStorage::IsKVMetadata(*base->Metadata),
            "CORRUPTION",
            "the snapshot archive requires append metadata");
        auto staged = std::make_unique<TStagedExpiration>();
        staged->Owner = shared_from_this();
        staged->CollectionID = collection.collection_id;
        staged->Name = collection.name;
        staged->BaseLocation = identity.metadata_location;
        const auto policy = TakeRetained(NStorage::ReadRetentionPolicy(*base->Metadata), "read snapshot retention");
        staged->Retention = {policy.KeepLast, std::chrono::milliseconds(policy.MinAgeMs)};
        const auto count = NStorage::MaintenanceArchiveCut(base->Metadata->snapshots.size(), collection.hot_snapshots);
        if (count == 0) {
            return staged;
        }
        (void)TakeIO(CheckWriteCapacity(false, false), EStoragePath::Preflight, "archive maintenance preflight");
        auto metadata = std::make_shared<iceberg::TableMetadata>(*base->Metadata);
        metadata->snapshots.erase(metadata->snapshots.begin(),
            metadata->snapshots.begin() + static_cast<std::ptrdiff_t>(count));
        metadata->snapshot_log.erase(metadata->snapshot_log.begin(),
            metadata->snapshot_log.begin() + static_cast<std::ptrdiff_t>(count));
        const auto attemptID = NewUuid();
        auto attempt = TakeIO(
            NIO::TIOAttempt::Make(
                {Store_.store_id, collection.collection_id, attemptID, NIO::EAttemptKind::Maintenance, std::nullopt},
                Dirs_->Latch()),
            EStoragePath::Write,
            "archive maintenance attempt");
        staged->Scope = TakeIceberg(FileIO_->BindAttempt(attempt), "IO_ERROR", "bind archive maintenance attempt");
        staged->Root = MetadataLocation(metadata->location, MetadataVersion(identity.metadata_location) + 1, attemptID);
        CheckIceberg(iceberg::TableMetadataUtil::Write(*FileIO_, staged->Root, *metadata),
            "IO_ERROR",
            "write archive maintenance root");
        auto limits = Options_.Storage;
        staged->Context = {.Directories = Dirs_,
            .FileIO = FileIO_,
            .Attempt = attempt,
            .Namespace = std::string(CatalogNamespace),
            .Name = collection.name,
            .Profile = MakeProfile(collection.schema, "CORRUPTION"),
            .Base = base,
            .ExpectedSummary = {},
            .StoreUsage = ComputeStoreUsage(),
            .Limits = limits};
        staged->Context.StoreLimits = limits;
        staged->Context.ArchivePrefix = count;
        staged->Context.ArchiveMaintenance = true;
        return staged;
    });
}

// The override fixes parameter positions; UUID and URI formats are checked.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
Status TLocalStorageBackend::RemoveExpiredMetadata(std::string_view collectionID, std::string_view path)
{
    return Guarded<Status>([&]() -> Status {
        const auto uri = NIO::TStoreUri::Parse(path, Store_.store_id);
        Require(uri && uri.Value().CollectionId() == collectionID &&
                (uri.Value().Kind() == NIO::EFileKind::ManifestList || uri.Value().Kind() == NIO::EFileKind::Manifest),
            "CORRUPTION",
            "cleanup path is not expired metadata");
        {
            std::lock_guard lock(Mutex_);
            RequireVerified();
            const auto found = Collections_.find(collectionID);
            Require(found != Collections_.end() && found->second.Confirmed,
                "CORRUPTION",
                "cleanup collection is missing");
            Require(!NStorage::IsKVMetadata(*found->second.Confirmed->Metadata),
                "CORRUPTION",
                "KV collection has unsupported metadata cleanup");
            Require(!found->second.Confirmed->RetainedManifestPaths.Contains(path),
                "CORRUPTION",
                "cleanup manifest is still referenced");
            for (const auto& snapshot : found->second.Confirmed->Metadata->snapshots) {
                Require(snapshot->manifest_list != path, "CORRUPTION", "cleanup path is still referenced");
            }
        }
        CheckIO(Dirs_->RemoveExpiredMetadata(uri.Value()), EStoragePath::Write, "reclaim expired metadata");
        return {};
    });
}

// The StorageBackend override fixes the two string identity parameters.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void TLocalStorageBackend::PublicationFinished(std::string_view collectionID,
    std::string_view metadataLocation,
    CommitDisposition result)
{
    std::lock_guard lock(Mutex_);
    const auto it = Collections_.find(collectionID);
    if (result == CommitDisposition::DefinitelyNotCommitted) {
        if (it != Collections_.end()) {
            it->second.Tentative.reset();
            it->second.TentativeMemory.reset();
            if (it->second.ConfirmedMemory) {
                CacheMemory_[std::string(collectionID)] = it->second.ConfirmedMemory;
            }
            if (!it->second.Confirmed) {
                CacheMemory_.erase(std::string(collectionID));
            }
        }
        return;
    }
    if (result == CommitDisposition::Committed && VerifiedGeneration_ && it != Collections_.end() &&
        it->second.Tentative && it->second.Tentative->MetadataLocation == metadataLocation)
    {
        it->second.Confirmed = std::move(it->second.Tentative);
        it->second.ConfirmedMemory = std::move(it->second.TentativeMemory);
        return;
    }
    // An uncertain result or a confirmation without its exact verified
    // candidate cannot leave capacity/base accounting usable by later writes.
    VerifiedGeneration_ = false;
    Collections_.clear();
    AdmittedWarehouseBytes_.reset();
    {
        std::lock_guard inventoryLock(InventoryMutex_);
        if (Inventory_) {
            Inventory_->Invalidate();
        }
    }
    if (ReadCache_) {
        ReadCache_->Clear();
    }
}

void TLocalStorageBackend::DiscardCandidate(std::string_view collectionID)
{
    std::lock_guard lock(Mutex_);
    auto it = Collections_.find(collectionID);
    if (it != Collections_.end()) {
        it->second.Tentative.reset();
        it->second.TentativeMemory.reset();
        // The staged object unwinds before the coordinator classifies a
        // failed COMMIT. PublicationFinished alone may refund root capacity.
    }
}

// The coordinator invalidates under its commit gate, never between a guard's
// decision and the outer COMMIT, so no in-flight candidate is lost here.
void TLocalStorageBackend::InvalidateCache()
{
    std::lock_guard lock(Mutex_);
    VerifiedGeneration_ = false;
    Collections_.clear();
    AdmittedWarehouseBytes_.reset();
    {
        std::lock_guard inventoryLock(InventoryMutex_);
        if (Inventory_) {
            Inventory_->Invalidate();
        }
    }
    if (ReadCache_) {
        ReadCache_->Clear();
    }
}

void TLocalStorageBackend::InitializeInventory()
{
    std::lock_guard lock(InventoryMutex_);
    if (InventoryInitialized_ || Options_.WalkWarehouseAtAdmission) {
        return;
    }
    InventoryInitialized_ = true;
    auto memory = Options_.Resources->Reserve(ResourceClass::Metadata, 4ULL << 20);
    if (memory) {
        auto inventory = Dirs_->Fs()->TrackTree(Dirs_->Warehouse());
        if (inventory) {
            InventoryMemory_ = std::move(*memory);
            Inventory_ = std::move(inventory).Value();
        }
    }
}

NIO::TResult<std::uint64_t> TLocalStorageBackend::CheckWriteCapacity(bool remember,
    bool reuse,
    std::chrono::steady_clock::time_point deadline,
    const std::function<bool()>& cancelled)
{
    std::optional<std::uint64_t> bytes;
    {
        std::lock_guard lock(Mutex_);
        bytes = std::exchange(AdmittedWarehouseBytes_, std::nullopt);
    }
    if (const auto error = Dirs_->CheckIdentity()) {
        return error;
    }
    if (!reuse || !bytes) {
        std::lock_guard inventoryLock(InventoryMutex_);
        const auto measure = [&] {
            return Dirs_->Fs()->MeasureTree(Dirs_->Warehouse(), Options_.Storage.WarehouseBytes, deadline, cancelled);
        };
        const auto admissionLimit = Options_.Storage.AttemptReserveBytes <= Options_.Storage.WarehouseBytes
            ? Options_.Storage.WarehouseBytes - Options_.Storage.AttemptReserveBytes
            : 0;
        const auto measured =
            Inventory_ ? Inventory_->Measure(measure, admissionLimit, deadline, cancelled) : measure();
        if (!measured) {
            return measured.Error();
        }
        bytes = measured.Value();
    }
    const auto space = Dirs_->Fs()->StatFs(Dirs_->Root());
    if (!space) {
        return space.Error();
    }
    if (const auto error = NStorage::CheckDiskReserve(space.Value(), *bytes, Options_.Storage)) {
        return error;
    }
    if (remember) {
        std::lock_guard lock(Mutex_);
        AdmittedWarehouseBytes_ = bytes;
    }
    return *bytes;
}

Status TLocalStorageBackend::VerifyRetained(const StoreInfo& store,
    std::span<const CollectionView> collections,
    std::span<const OperationRecord> operations)
{
    // Callers without a catalog have no snapshot archive to stream.
    return VerifyRetainedImpl(
        store,
        collections,
        [&](std::string_view id, const auto& consume) {
            for (const auto& record : operations) {
                if (id.empty() || record.binding.collection_id == id) {
                    consume(record);
                }
            }
        },
        [](std::string_view, const auto&) {
        });
}

Status TLocalStorageBackend::VerifyRetainedCatalog(const StoreInfo& store,
    std::span<const CollectionView> collections,
    DurableSqliteCatalogStore& catalog)
{
    return VerifyRetainedImpl(
        store,
        collections,
        [&](std::string_view id, const auto& consume) {
            catalog.ForEachOperation(consume, id);
        },
        [&](std::string_view id, const auto& consume) {
            catalog.ForEachArchivedSnapshot(id, consume);
        });
}

Status TLocalStorageBackend::VerifyRetainedImpl(const StoreInfo& store,
    std::span<const CollectionView> collections,
    const TOperationReader& readOperations,
    const TArchiveReader& readArchive)
{
    // A failed rebuild must not leave the previous generation available.
    InvalidateCache();
    return Guarded<Status>([&]() -> Status {
        // Verification only reads; a set write latch does not block it.
        CheckStore(store);
        CheckIO(Dirs_->CheckIdentity(), EStoragePath::Read, "verify store directories");
        // Physical retained files include orphans; logical reachable usage below
        // is a separate budget. Recovery needs no reserve for a future append.
        (void)TakeIO(NStorage::MeasureWarehouse(*Dirs_, Options_.Storage),
            EStoragePath::Read,
            "retained warehouse capacity");
        const std::shared_ptr<iceberg::FileIO> historyIO =
            TakeIceberg(FileIO_->ReadOnlyView(), "IO_ERROR", "open no-op history FileIO");
        std::set<std::string, std::less<>> retainedNoOpRoots;
        readOperations({}, [&](const auto& operation) {
            if (operation.state == OperationState::Committed && operation.outcome && operation.outcome->result &&
                operation.outcome->result->receipt_version == 3 && !operation.outcome->result->snapshot_created)
            {
                retainedNoOpRoots.insert(operation.binding.base.metadata_location);
                PinNoOpRoot(operation.binding.base.metadata_location);
            }
        });
        std::map<std::string, TCollectionState, std::less<>> verified;
        NStorage::TStorageUsage totalUsage;
        for (const auto& view : collections) {
            Require(view.base.collection_id == view.info.collection_id &&
                    view.base.table_uuid == view.info.table_uuid &&
                    view.base.current_snapshot_id == view.info.current_snapshot_id,
                "CORRUPTION",
                "collection view identity is inconsistent");
            // The full reachable closure on open and Reconcile; terminal
            // receipt lookups reuse this verified generation.
            const auto prefix = RetainedPrefix(view, readOperations, readArchive, Options_.Storage);
            auto state = VerifyCollection(view.base, prefix.History, prefix.Expired, prefix.Archive);
            CheckSchema(*state->Metadata, view.info.schema, view.info.mode);
            CheckHistory(*state->Metadata,
                view.info,
                state->MetadataLocation,
                readOperations,
                historyIO,
                Options_.Storage.RootBytes,
                view.expired_snapshots,
                prefix.Archived);
            totalUsage = TakeUsage(NStorage::ReplaceUsage(totalUsage, {.Previous = {}, .Next = state->Usage}),
                "CORRUPTION",
                "aggregate storage usage");
            auto& entry = verified[view.info.collection_id];
            entry.Confirmed = std::move(state);
            {
                std::lock_guard lock(Mutex_);
                entry.ConfirmedMemory = CacheMemory_.at(view.info.collection_id);
            }
        }
        CheckUsage(NStorage::CheckStorageCapacity(totalUsage, Options_.Storage),
            "CORRUPTION",
            "retained storage capacity");
        readOperations({}, [&](const auto& operation) {
            Require(operation.state != OperationState::Committed || verified.contains(operation.binding.collection_id),
                "CORRUPTION",
                "committed operation belongs to an unknown collection");
        });
        std::lock_guard lock(Mutex_);
        Collections_ = std::move(verified);
        std::erase_if(NoOpRoots_, [&](const auto& item) {
            return !retainedNoOpRoots.contains(item.first);
        });
        std::erase_if(CacheMemory_, [&](const auto& item) {
            return !Collections_.contains(item.first);
        });
        VerifiedGeneration_ = true;
        return {};
    });
}

void TLocalStorageBackend::RetireRoot(const BaseIdentity& superseded)
{
    const auto& location = superseded.metadata_location;
    const auto uri = NIO::TStoreUri::Parse(location, Store_.store_id);
    if (superseded.store_id != Store_.store_id || !uri || uri.Value().Kind() != NIO::EFileKind::Metadata ||
        uri.Value().CollectionId() != superseded.collection_id)
    {
        return;
    }
    {
        // Never remove the confirmed or a tentative root of this collection.
        std::lock_guard lock(Mutex_);
        const auto state = Collections_.find(superseded.collection_id);
        if (NoOpRoots_.contains(location) || state == Collections_.end() || !state->second.Confirmed ||
            state->second.Confirmed->MetadataLocation == location ||
            (state->second.Tentative && state->second.Tentative->MetadataLocation == location))
        {
            return;
        }
    }
    if (ReadCache_) {
        ReadCache_->EvictRoot(location);
    }
    // Best effort: a root that cannot be removed stays as an orphan file.
    [[maybe_unused]] const auto error = Dirs_->RemoveMetadataFile(uri.Value());
}

Result<std::shared_ptr<iceberg::FileIO>> TLocalStorageBackend::ReadOnlyFileIO()
{
    auto view = FileIO_->ReadOnlyView();
    if (!view) {
        return std::unexpected(ErrorDetail{"IO_ERROR",
            BoundedDiagnostic("open read-only FileIO: " + view.error().message),
            RetryHint::OperatorAction});
    }
    return std::shared_ptr<iceberg::FileIO>(std::move(*view));
}

Result<std::shared_ptr<const NStorage::TBaseState>> TLocalStorageBackend::VerifiedScanState(const BaseIdentity& base)
{
    return Guarded<Result<std::shared_ptr<const NStorage::TBaseState>>>([&] {
        Require(base.store_id == Store_.store_id, "CORRUPTION", "scan index belongs to a different store");
        auto state = ResolveBase(base);
        Require(state->ScanIndex != nullptr, "CORRUPTION", "verified storage has no scan index");
        return state;
    });
}

void TLocalStorageBackend::RecordCandidate(std::string_view collectionID,
    std::shared_ptr<const NStorage::TBaseState> candidate)
{
    auto memory = ReserveRootMemory(collectionID, candidate->Usage.RootBytes, false, ScanIndexMemory(*candidate));
    candidate = RetainState(memory, std::move(candidate));
    std::lock_guard lock(Mutex_);
    auto it = Collections_.find(collectionID);
    if (it == Collections_.end()) {
        it = Collections_.emplace(std::string(collectionID), TCollectionState{}).first;
    }
    it->second.Tentative = std::move(candidate);
    it->second.TentativeMemory = std::move(memory);
}

void TLocalStorageBackend::PinNoOpRoot(std::string_view location)
{
    std::lock_guard lock(Mutex_);
    if (NoOpRoots_.contains(location)) {
        return;
    }
    auto memory = Options_.Resources->Reserve(ResourceClass::Metadata, 512 + location.size());
    if (!memory) {
        throw Failure(memory.error());
    }
    NoOpRoots_.emplace(std::string(location), std::move(*memory));
}

void TLocalStorageBackend::CheckStore(const StoreInfo& store) const
{
    Require(store.store_id == Store_.store_id && store.profile_id == Store_.profile_id,
        "INVALID_ARGUMENT",
        "storage backend belongs to a different store");
}

NStorage::TStorageUsage TLocalStorageBackend::ComputeStoreUsage()
{
    std::lock_guard lock(Mutex_);
    RequireVerified();
    NStorage::TStorageUsage total;
    for (const auto& [id, state] : Collections_) {
        if (state.Confirmed) {
            total = TakeUsage(NStorage::ReplaceUsage(total, {.Previous = {}, .Next = state.Confirmed->Usage}),
                "CORRUPTION",
                "aggregate storage usage");
        }
    }
    return total;
}

void TLocalStorageBackend::RequireVerified() const
{
    if (!VerifiedGeneration_) {
        throw Failure(
            {"RECOVERY_REQUIRED", "retained storage must be verified before writing", RetryHint::SameIdAfterReconcile});
    }
}

std::shared_ptr<const NStorage::TBaseState> TLocalStorageBackend::ResolveBase(const BaseIdentity& base)
{
    std::lock_guard lock(Mutex_);
    RequireVerified();
    const auto it = Collections_.find(base.collection_id);
    Require(it != Collections_.end() && it->second.Confirmed &&
            it->second.Confirmed->MetadataLocation == base.metadata_location,
        "RECOVERY_REQUIRED",
        "prepared base is not the explicitly confirmed storage generation");
    CheckIdentity(*it->second.Confirmed, base);
    return it->second.Confirmed;
}

std::shared_ptr<ResourceReservation>
TLocalStorageBackend::ReserveRootMemory(std::string_view collectionID, uint64_t bytes, bool reuse, uint64_t indexBytes)
{
    const TRootMemorySize size{.RootBytes = bytes, .IndexBytes = indexBytes};
    const auto required = RootMemoryBytes(size);
    if (reuse) {
        std::lock_guard lock(Mutex_);
        const auto found = CacheMemory_.find(collectionID);
        // Only the map can own a reused reservation. A still-live old state
        // needs its own charge until its last alias has been destroyed.
        if (found != CacheMemory_.end() && found->second.use_count() == 1 && found->second->GetBytes() >= required) {
            return found->second;
        }
    }
    auto memory = ReserveRoot(Options_.Resources, size);
    std::lock_guard lock(Mutex_);
    CacheMemory_[std::string(collectionID)] = memory;
    return memory;
}

std::shared_ptr<const NStorage::TBaseState> TLocalStorageBackend::VerifyCollection(const BaseIdentity& base,
    std::span<const std::shared_ptr<iceberg::Snapshot>> history,
    std::size_t expired,
    const NStorage::TStorageUsage& archive)
{
    const std::shared_ptr<iceberg::FileIO> view =
        TakeIceberg(FileIO_->ReadOnlyView(), "IO_ERROR", "open read-only FileIO");
    auto input = TakeRetained(view->NewInputFile(base.metadata_location), "inspect retained root");
    const auto size = TakeRetained(input->Size(), "size retained root");
    auto memory = ReserveRootMemory(base.collection_id, static_cast<uint64_t>(size), true);
    auto state = TakeRetained(NStorage::VerifyReachable(view,
                                  base.metadata_location,
                                  Store_.store_id,
                                  base.collection_id,
                                  Options_.Storage,
                                  Dirs_->Path().string(),
                                  history,
                                  expired),
        "verify retained collection");
    CheckIdentity(state, base);
    // Archived lists and the manifests only they reference stay on disk and
    // keep their publication-time charge.
    state.Usage = TakeUsage(NStorage::ReplaceUsage(state.Usage, {.Previous = {}, .Next = archive}),
        "CORRUPTION",
        "archived metadata usage");
    CheckUsage(NStorage::CheckStorageCapacity(state.Usage, Options_.Storage),
        "CORRUPTION",
        "archived storage capacity");
    const auto indexBytes = ScanIndexMemory(state);
    if (memory->GetBytes() < RootMemoryBytes({.RootBytes = state.Usage.RootBytes, .IndexBytes = indexBytes})) {
        // Recovery's protected workspace covers reconstruction. Before the
        // state escapes, retain the same index charge used at publication.
        memory.reset();
        memory = ReserveRootMemory(base.collection_id, state.Usage.RootBytes, true, indexBytes);
    }
    return RetainState(std::move(memory), std::make_shared<const NStorage::TBaseState>(std::move(state)));
}

} // namespace glamdb::detail
