#include "core/backend/local_storage_backend.h"
#include "core/io/tree_inventory.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/write_settings.h"
#include "core/backend/inert_catalog.h"
#include "core/backend/storage_errors.h"
#include "core/engine/storage_mapping.h"
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

uint64_t DataFileMemory(const iceberg::DataFile& file)
{
    // Conservative map-node/control-block allowance for the pinned standard
    // library, plus the actual capacities of every retained variable buffer.
    uint64_t bytes = sizeof(file) + 256;
    AddMemory(bytes, file.file_path.capacity() + 1);
    for (const auto count : {file.column_sizes.size(),
             file.value_counts.size(),
             file.null_value_counts.size(),
             file.nan_value_counts.size()})
    {
        AddMemory(bytes, count, 96);
    }
    for (const auto* bounds : {&file.lower_bounds, &file.upper_bounds}) {
        AddMemory(bytes, bounds->size(), 96);
        for (const auto& [field, value] : *bounds) {
            (void)field;
            AddMemory(bytes, value.capacity());
        }
    }
    AddMemory(bytes, file.key_metadata.capacity());
    AddMemory(bytes, file.split_offsets.capacity(), sizeof(int64_t));
    AddMemory(bytes, file.equality_ids.capacity(), sizeof(int32_t));
    if (file.referenced_data_file) {
        AddMemory(bytes, file.referenced_data_file->capacity() + 1);
    }
    return bytes;
}

uint64_t ScanIndexMemory(const NStorage::TBaseState& state)
{
    if (!state.ScanIndex) {
        return 0;
    }
    const auto& index = *state.ScanIndex;
    uint64_t bytes = sizeof(index);
    // Copy-and-grow publication and reserve-and-grow recovery can give the
    // same history different vector capacities. Both are bounded by twice
    // the final element count in the pinned standard library. Charge that
    // stable envelope so reconciliation can reuse its admitted reservation.
    AddMemory(bytes, index.Files.size(), 2 * sizeof(NStorage::TScanDataFile));
    AddMemory(bytes, index.Snapshots.size(), 2 * sizeof(NStorage::TSnapshotBoundary));
    for (const auto& file : index.Files) {
        AddMemory(bytes, file.Path.capacity() + 1);
        if (file.DataFile) {
            AddMemory(bytes, DataFileMemory(*file.DataFile));
        }
    }
    return bytes;
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

NStorage::TTableProfile MakeProfile(const SchemaSpec& schema, std::string_view code)
{
    auto profile = NStorage::MakeTableProfile(ToStorageSchema(schema));
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

void CheckIdentity(const NStorage::TBaseState& state, const BaseIdentity& base)
{
    Require(state.Metadata != nullptr && state.MetadataLocation == base.metadata_location &&
            state.Metadata->table_uuid == base.table_uuid &&
            CurrentSnapshot(*state.Metadata) == base.current_snapshot_id,
        "CORRUPTION",
        "retained table root does not match the catalog identity");
}

void CheckSchema(const iceberg::TableMetadata& metadata, const SchemaSpec& schema)
{
    const auto profile = MakeProfile(schema, "CORRUPTION");
    const auto current = TakeRetained(metadata.Schema(), "retained schema");
    Require(*current == *profile.Schema, "CORRUPTION", "retained table schema differs from the catalog schema");
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

// Bidirectional: every retained snapshot has exactly one committed receipt of
// this collection, and every committed receipt has exactly one snapshot.
// Bound and Failed operations therefore have no retained snapshot.
void CheckHistory(const iceberg::TableMetadata& metadata,
    std::string_view collectionID,
    const std::function<void(std::string_view, const DurableSqliteCatalogStore::OperationConsumer&)>& readOperations)
{
    std::map<std::string, AppendResult, std::less<>> committed;
    readOperations(collectionID, [&](const auto& operation) {
        if (operation.binding.collection_id != collectionID || operation.state != OperationState::Committed) {
            return;
        }
        if (!operation.outcome.has_value() || !operation.outcome->result.has_value()) {
            Fail("CORRUPTION", "history", "committed operation has no receipt");
        }
        committed.emplace(operation.binding.operation_id, *operation.outcome->result);
    });
    std::set<std::string, std::less<>> seen;
    for (const auto& snapshot : metadata.snapshots) {
        Require(snapshot != nullptr, "CORRUPTION", "retained snapshot is missing");
        const auto operation = snapshot->summary.find(std::string(OperationKey));
        Require(operation != snapshot->summary.end(), "CORRUPTION", "retained snapshot has no operation identity");
        const auto receipt = committed.find(operation->second);
        Require(receipt != committed.end() && seen.insert(operation->second).second,
            "CORRUPTION",
            "retained snapshot has no unique committed receipt");
        CheckReceipt(*snapshot, receipt->second);
    }
    Require(seen.size() == committed.size(), "CORRUPTION", "committed receipt has no retained snapshot");
}

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
            Owner_->RecordCandidate(CollectionID_, guard.GetCandidate());
            return {};
        });
    }

private:
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
    AddEncodingCapabilities(capabilities);
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
        auto profile = MakeProfile(request.schema, "INVALID_ARGUMENT");
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
            request.write_settings};
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
        (void)MakeProfile(collection.schema, "INVALID_ARGUMENT");
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
        if (current->Metadata->metadata_log.size() > Options_.Storage.MetadataLogEntries) {
            Fail("RESOURCE_EXHAUSTED", "storage capacity", "collection metadata history is full");
        }
        if (current->Usage.RootBytes >= Options_.Storage.RootBytes) {
            Fail("RESOURCE_EXHAUSTED", "storage capacity", "collection root is at its byte limit");
        }
        const auto minUsage = TakeUsage(
            NStorage::ReplaceUsage(ComputeStoreUsage(),
                {.Previous = {},
                    .Next = {.RootBytes = 1,
                        .Snapshots = 1,
                        .SnapshotLogEntries = 1,
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
            std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
            batches.reserve(128);
            int64_t fileRows = 0;
            int64_t fileBytes = 0;
            int64_t totalRows = 0;
            uint64_t descriptorBytes = dataFiles.capacity() * sizeof(std::shared_ptr<iceberg::DataFile>);
            const auto flush = [&] {
                if (batches.empty()) {
                    return;
                }
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
                batches.clear();
                fileRows = 0;
                fileBytes = 0;
                // All files produced so far are durable, so the same physical
                // accounting used by final publication can bound the stream.
                CheckIO(Dirs_->CheckIdentity(), EStoragePath::Preflight, "bulk storage capacity");
                const auto physical =
                    TakeIO(NStorage::EstimateAttemptWarehouse(*Dirs_, *productionBaseline, *attempt, Options_.Storage),
                        EStoragePath::Preflight,
                        "bulk storage capacity");
                const auto space =
                    TakeIO(Dirs_->Fs()->StatFs(Dirs_->Root()), EStoragePath::Preflight, "bulk storage capacity");
                CheckIO(NStorage::CheckDiskReserve(space, physical, Options_.Storage),
                    EStoragePath::Preflight,
                    "bulk storage capacity");
                CheckDeadline(deadline);
            };
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
                const auto& batch = *built;
                const auto bytes = arrow::util::TotalBufferSize(*batch);
                if (bytes < 0 || bytes > limits.MaxInputBytes) {
                    Fail("RESOURCE_EXHAUSTED", "bulk append", "source chunk exceeds the file input byte limit");
                }
                for (int64_t offset = 0; offset < rows;) {
                    const auto count = std::min(rows - offset, limits.MaxRows - fileRows);
                    auto piece = offset == 0 && count == rows ? batch : batch->Slice(offset, count);
                    const auto retainedBytes = arrow::util::TotalBufferSize(*piece);
                    if (retainedBytes > limits.MaxInputBytes - fileBytes) {
                        flush();
                        continue;
                    }
                    batches.push_back(std::move(piece));
                    fileRows += count;
                    fileBytes += retainedBytes;
                    offset += count;
                    // Descriptor count is bounded independently of caller
                    // chunk size; tiny batches never force an early file.
                    if (batches.size() >= 128) {
                        auto combined = arrow::ConcatenateRecordBatches(batches, Options_.InputPool.get());
                        if (!combined.ok()) {
                            Fail(combined.status().IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "INTERNAL",
                                "coalesce bulk batches",
                                combined.status().ToString());
                        }
                        batches.assign(1, *combined);
                        fileBytes = arrow::util::TotalBufferSize(*batches.front());
                        Require(fileBytes >= 0 && fileBytes <= limits.MaxInputBytes,
                            "RESOURCE_EXHAUSTED",
                            "coalesced bulk buffers exceed the file input byte limit");
                    }
                    if (fileRows == limits.MaxRows || fileBytes >= BulkAppendLimits::TargetFileBytes) {
                        flush();
                    }
                }
                totalRows += rows;
            }
            flush();
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
        // file with these bytes instead of parsing it again.
        auto stagedMetadata = std::make_shared<const iceberg::TableMetadata>(staged.Transaction->current());
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
    return VerifyRetainedImpl(store, collections, [&](std::string_view id, const auto& consume) {
        for (const auto& record : operations) {
            if (id.empty() || record.binding.collection_id == id) {
                consume(record);
            }
        }
    });
}

Status TLocalStorageBackend::VerifyRetainedCatalog(const StoreInfo& store,
    std::span<const CollectionView> collections,
    DurableSqliteCatalogStore& catalog)
{
    return VerifyRetainedImpl(store, collections, [&](std::string_view id, const auto& consume) {
        catalog.ForEachOperation(consume, id);
    });
}

Status TLocalStorageBackend::VerifyRetainedImpl(const StoreInfo& store,
    std::span<const CollectionView> collections,
    const TOperationReader& readOperations)
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
            auto state = VerifyCollection(view.base);
            CheckSchema(*state->Metadata, view.info.schema);
            CheckHistory(*state->Metadata, view.info.collection_id, readOperations);
            totalUsage = TakeUsage(NStorage::ReplaceUsage(totalUsage, {.Previous = {}, .Next = state->Usage}),
                "CORRUPTION",
                "aggregate storage usage");
            CheckUsage(NStorage::CheckStorageCapacity(totalUsage, Options_.Storage),
                "CORRUPTION",
                "retained storage capacity");
            auto& entry = verified[view.info.collection_id];
            entry.Confirmed = std::move(state);
            {
                std::lock_guard lock(Mutex_);
                entry.ConfirmedMemory = CacheMemory_.at(view.info.collection_id);
            }
        }
        readOperations({}, [&](const auto& operation) {
            Require(operation.state != OperationState::Committed || verified.contains(operation.binding.collection_id),
                "CORRUPTION",
                "committed operation belongs to an unknown collection");
        });
        std::lock_guard lock(Mutex_);
        Collections_ = std::move(verified);
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
        if (state == Collections_.end() || !state->second.Confirmed ||
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

std::shared_ptr<const NStorage::TBaseState> TLocalStorageBackend::VerifyCollection(const BaseIdentity& base)
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
                                  Dirs_->Path().string()),
        "verify retained collection");
    CheckIdentity(state, base);
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
