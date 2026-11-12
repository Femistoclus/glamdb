#include "core/scan/scan_cursor.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/backend/storage_errors.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/data_file_source.h"
#include "core/scan/predicate.h"
#include "core/scan/scan_planner.h"
#include "core/scan/scan_pruning.h"
#include "core/storage/format_verifier.h"
#include "core/storage/table_profile.h"

#include "core/resources/arrow_pool.h"
#include "core/resources/envelopes.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <deque>
#include <iterator>
#include <mutex>
#include <new>
#include <numeric>
#include <span>

namespace glamdb::detail {
namespace {

constexpr int64_t MaxBatchRows = 65536;

void RequireInput(bool condition, std::string_view message)
{
    if (!condition) {
        throw Failure({"INVALID_ARGUMENT", std::string(message), RetryHint::FixInput});
    }
}

[[noreturn]] void Fail(std::string_view code, std::string_view context, std::string_view message)
{
    throw Failure({std::string(code),
        BoundedDiagnostic(std::string(context) + ": " + std::string(message)),
        RetryHint::OperatorAction});
}

// Allocation failure is resource pressure, not a property of the decoded file.
void CheckArrow(const arrow::Status& status, std::string_view code, std::string_view context)
{
    if (!status.ok()) {
        Fail(status.IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : code, context, status.ToString());
    }
}

template <class T>
T TakeArrow(arrow::Result<T> result, std::string_view code, std::string_view context)
{
    CheckArrow(result.status(), code, context);
    return std::move(result).ValueUnsafe();
}

// Every path a cursor reads comes from committed metadata.
template <class T>
T TakeIceberg(iceberg::Result<T> result, std::string_view code, std::string_view context)
{
    if (!result) {
        throw Failure(StorageError(result.error(), context, EStoragePath::Read, code));
    }
    return std::move(*result);
}

// Limits of the canonical input (see core/api/canonical.cpp); stored values
// outside them cannot have been written by this engine.
constexpr size_t MaxKeyBytes = 1024;
constexpr size_t MaxStringBytes = 1U << 20;
constexpr int64_t MinTimestampUs = -62135596800000000LL;
constexpr int64_t MaxTimestampUs = 253402300799999999LL;

// Decoded values of one column must satisfy the canonical input domain.
void ValidateColumn(const FieldSpec& field, const arrow::Array& array)
{
    // Checks offsets and, for utf8, that every value is valid UTF-8; the
    // Parquet reader decodes strings without validation.
    CheckArrow(array.ValidateFull(), "CORRUPTION", "validate decoded column");
    if (!field.nullable && array.null_count() != 0) {
        Fail("CORRUPTION", "read Parquet batch", "required column contains NULL");
    }
    switch (field.type) {
        case FieldType::String: {
            const auto& strings = static_cast<const arrow::StringArray&>(array);
            const auto limit = field.name == "key" ? MaxKeyBytes : MaxStringBytes;
            for (int64_t row = 0; row < strings.length(); ++row) {
                if (!strings.IsNull(row) && strings.GetView(row).size() > limit) {
                    Fail("CORRUPTION", "read Parquet batch", "string value exceeds its byte limit");
                }
            }
            break;
        }
        case FieldType::Float64: {
            const auto& doubles = static_cast<const arrow::DoubleArray&>(array);
            for (int64_t row = 0; row < doubles.length(); ++row) {
                const auto value = doubles.IsNull(row) ? 0.0 : doubles.Value(row);
                if (!std::isfinite(value) || (value == 0.0 && std::signbit(value))) {
                    Fail("CORRUPTION", "read Parquet batch", "stored float is not canonical");
                }
            }
            break;
        }
        case FieldType::TimestampUsUtc: {
            const auto& times = static_cast<const arrow::TimestampArray&>(array);
            for (int64_t row = 0; row < times.length(); ++row) {
                if (!times.IsNull(row) && (times.Value(row) < MinTimestampUs || times.Value(row) > MaxTimestampUs)) {
                    Fail("CORRUPTION", "read Parquet batch", "stored timestamp is outside the supported UTC range");
                }
            }
            break;
        }
        case FieldType::Bool:
        case FieldType::Int64:
            break;
    }
}

template <class TFunction>
decltype(auto) DecodeParquet(std::string_view context, TFunction&& fn)
{
    try {
        return fn();
    } catch (const Failure&) {
        throw;
    } catch (const std::bad_alloc& error) {
        Fail("RESOURCE_EXHAUSTED", context, error.what());
    } catch (const std::exception& error) {
        Fail("CORRUPTION", context, error.what());
    }
}

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(result.error());
    }
    return std::move(*result);
}

class TLeaseBuffer final : public arrow::Buffer
{
public:
    TLeaseBuffer(std::shared_ptr<arrow::Buffer> inner, std::shared_ptr<const void> lease)
        : arrow::Buffer(inner->data(), inner->size())
        , Lease_(std::move(lease))
        , Inner_(std::move(inner))
    { }

private:
    // Destroy Inner_ before Lease_ so buffer memory is freed while the
    // allocator and engine resources are still held.
    std::shared_ptr<const void> Lease_;
    std::shared_ptr<arrow::Buffer> Inner_;
};

std::shared_ptr<arrow::ArrayData> AttachLease(const std::shared_ptr<arrow::ArrayData>& data,
    const std::shared_ptr<const void>& lease)
{
    if (!data || !lease) {
        return data;
    }
    auto copy = data->Copy();
    for (auto& buffer : copy->buffers) {
        if (buffer) {
            buffer = std::make_shared<TLeaseBuffer>(std::move(buffer), lease);
        }
    }
    for (auto& child : copy->child_data) {
        child = AttachLease(child, lease);
    }
    if (copy->dictionary) {
        copy->dictionary = AttachLease(copy->dictionary, lease);
    }
    return copy;
}

std::shared_ptr<arrow::Schema> RetainSchema(std::shared_ptr<arrow::Schema> schema,
    std::shared_ptr<ResourceReservation> memory)
{
    struct TSchemaLease
    {
        std::shared_ptr<ResourceReservation> Memory;
        std::shared_ptr<arrow::Schema> Schema;
    };
    auto* retainedSchema = schema.get();
    auto lease = std::make_shared<TSchemaLease>(std::move(memory), std::move(schema));
    return std::shared_ptr<arrow::Schema>(std::move(lease), retainedSchema);
}

struct TOutputUsage
{
    std::atomic<int64_t> Bytes{0};
    std::atomic<int64_t> Peak{0};
};

struct TOutputLease
{
    // Output schemas retain this lease even for zero-column batch slices.
    std::shared_ptr<const void> Cursor;
    std::shared_ptr<ResourceReservation> Memory;
    std::shared_ptr<TOutputUsage> Usage;
    std::shared_ptr<arrow::Schema> Schema;

    TOutputLease(std::shared_ptr<const void> cursor,
        std::shared_ptr<ResourceReservation> memory,
        std::shared_ptr<TOutputUsage> usage,
        std::shared_ptr<arrow::Schema> schema)
        : Cursor(std::move(cursor))
        , Memory(std::move(memory))
        , Usage(std::move(usage))
        , Schema(std::move(schema))
    {
        const auto bytes =
            Usage->Bytes.fetch_add(static_cast<int64_t>(Memory->GetBytes())) + static_cast<int64_t>(Memory->GetBytes());
        auto peak = Usage->Peak.load();
        while (peak < bytes && !Usage->Peak.compare_exchange_weak(peak, bytes)) {
        }
    }

    ~TOutputLease()
    {
        Usage->Bytes.fetch_sub(static_cast<int64_t>(Memory->GetBytes()));
    }
};

std::vector<size_t> ResolveProjection(const SchemaSpec& schema, const std::optional<std::vector<std::string>>& names)
{
    std::vector<size_t> fields;
    if (!names) {
        fields.resize(schema.fields.size());
        std::iota(fields.begin(), fields.end(), size_t{0});
        return fields;
    }
    for (const auto& name : *names) {
        const auto it = std::ranges::find_if(schema.fields, [&](const auto& field) {
            return field.name == name;
        });
        RequireInput(it != schema.fields.end(), "projection field does not exist");
        const auto index = static_cast<size_t>(it - schema.fields.begin());
        RequireInput(std::ranges::find(fields, index) == fields.end(), "projection repeats a field");
        fields.push_back(index);
    }
    return fields;
}

class TScanCursor final : public ScanCursor
{
public:
    TScanCursor(TScanContext context,
        std::shared_ptr<ResourceReservation> planningMemory,
        std::shared_ptr<ResourceReservation> controlMemory,
        std::shared_ptr<iceberg::Schema> fileSchema,
        std::shared_ptr<arrow::Schema> outputSchema,
        TScanManifests plan,
        TBoundPredicate predicate,
        std::vector<size_t> outputFields,
        std::vector<size_t> readFields,
        int64_t batchRows,
        std::optional<int64_t> limit,
        bool prune)
        : PlanningMemory_(std::move(planningMemory))
        , CursorMemory_(Take(context.Resources->Reserve(ResourceClass::Metadata, 16ULL << 20)))
        , SelectionMemory_(Take(context.Resources->Reserve(ResourceClass::Data, 1ULL << 20)))
        , ControlMemory_(std::move(controlMemory))
        , Context_(std::move(context))
        , Pool_(MakeResourcePool(Context_.Resources, Context_.Pool))
        , BufferLease_(std::make_shared<TBufferLease>(Context_.Lease, Context_.FileIO, Pool_, CursorMemory_))
        , FileSchema_(std::move(fileSchema))
        , OutputSchema_(RetainSchema(std::move(outputSchema), CursorMemory_))
        , Info_{OutputSchema_,
              Context_.Base,
              plan.SnapshotID ? SnapshotSelector::Id(*plan.SnapshotID) : SnapshotSelector::InitialEmpty()}
        , Plan_(std::move(plan))
        , Predicate_(std::move(predicate))
        , OutputFields_(std::move(outputFields))
        , ReadFields_(std::move(readFields))
        , BatchRows_(batchRows)
        , Prune_(prune)
        , Remaining_(limit)
    { }

    const ScanInfo& Info() const override
    {
        return Info_;
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        std::lock_guard lock(Mutex_);
        if (Failed_) {
            return std::unexpected(*Failed_);
        }
        try {
            return ReadNext();
        } catch (const Failure& error) {
            // Explicit admission fails before consuming the next batch/file.
            // Preserve pending descriptors and the current decoder for retry.
            if (error.detail.code == "RESOURCE_EXHAUSTED" && error.detail.retry_hint == RetryHint::SameIdAfterReconcile)
            {
                return std::unexpected(error.detail);
            }
            Release();
            if (State_ == ScanState::Open) {
                State_ = error.detail.code == "CANCELLED" ? ScanState::Cancelled : ScanState::Failed;
            }
            if (Context_.OnError) {
                Context_.OnError(error.detail);
            }
            if (error.detail.code != "CANCELLED") {
                Failed_ = error.detail;
            }
            return std::unexpected(error.detail);
        } catch (const std::bad_alloc& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = ScanState::Failed;
            }
            Failed_ = ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
            return std::unexpected(*Failed_);
        } catch (const std::exception& error) {
            Release();
            if (State_ == ScanState::Open) {
                State_ = ScanState::Failed;
            }
            Failed_ = ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction};
            return std::unexpected(*Failed_);
        }
    }

    void Cancel() override
    {
        Cancelled_ = true;
    }

    ScanProfile Profile() const override
    {
        std::lock_guard lock(Mutex_);
        const auto openBytes = Source_ ? Source_->Reads().Bytes : 0;
        ScanProfile profile{.state = State_,
            .manifests_read = {ManifestsRead_, MetricKind::Measured},
            .data_files_read = {DataFilesRead_, MetricKind::Measured},
            .data_bytes_read = {DataBytesRead_ + openBytes, MetricKind::Measured},
            .rows_decoded = {RowsDecoded_, MetricKind::Measured},
            .rows_returned = {RowsReturned_, MetricKind::Measured},
            .rows_filtered = {RowsFiltered_, MetricKind::Measured},
            .rows_limited = {RowsLimited_, MetricKind::Measured},
            .limit_stops = {LimitStopped_ ? 1 : 0, MetricKind::Measured},
            .cancellation_stops = {CancellationStopped_ ? 1 : 0, MetricKind::Measured},
            .data_files_pruned = {ReadStats_.FilesPruned, MetricKind::Measured},
            .row_groups_pruned = {ReadStats_.RowGroupsPruned, MetricKind::Measured},
            .arrow_bytes = {Pool_->bytes_allocated(), MetricKind::Measured},
            .peak_arrow_bytes = {Pool_->max_memory(), MetricKind::Measured},
            .output_metadata_bytes = {OutputUsage_->Bytes.load(), MetricKind::Measured},
            .peak_output_metadata_bytes = {OutputUsage_->Peak.load(), MetricKind::Measured}};
        if (profile.state == ScanState::Open && (Cancelled_.load() || (Context_.Closing && Context_.Closing->load()))) {
            profile.state = ScanState::Cancelled;
        }
        return profile;
    }

    TScanReadStats ReadStats() const
    {
        std::lock_guard lock(Mutex_);
        auto stats = ReadStats_;
        if (Source_) {
            const auto reads = Source_->Reads();
            stats.ReadCalls += reads.Calls;
            stats.MaxReadBytes = std::max(stats.MaxReadBytes, reads.MaxReadBytes);
            stats.PeakHeldBytes = std::max(stats.PeakHeldBytes, reads.PeakHeldBytes);
        }
        return stats;
    }

private:
    struct TBufferLease
    {
        std::shared_ptr<const void> Store;
        std::shared_ptr<iceberg::FileIO> FileIO;
        std::shared_ptr<arrow::MemoryPool> Pool;
        std::shared_ptr<ResourceReservation> Metadata;
    };
    // Reservations outlive the context, plan, predicate and decoder objects.
    const std::shared_ptr<ResourceReservation> PlanningMemory_, CursorMemory_, SelectionMemory_, ControlMemory_;
    const TScanContext Context_;
    const std::shared_ptr<arrow::MemoryPool> Pool_;
    const std::shared_ptr<TOutputUsage> OutputUsage_ = std::make_shared<TOutputUsage>();
    const std::shared_ptr<TBufferLease> BufferLease_;
    std::shared_ptr<ResourceReservation> FileMemory_;
    // The canonical Iceberg schema of every data file of this collection.
    const std::shared_ptr<iceberg::Schema> FileSchema_;
    const std::shared_ptr<arrow::Schema> OutputSchema_;
    const ScanInfo Info_;
    // Production plans pin verified membership; custom backends can retain
    // on-demand manifest planning.
    const TScanManifests Plan_;
    const TBoundPredicate Predicate_;
    // Schema field indices in output order, and every field read from files.
    const std::vector<size_t> OutputFields_;
    const std::vector<size_t> ReadFields_;
    const int64_t BatchRows_;
    // Skip files and row groups whose statistics exclude every match.
    const bool Prune_;

    mutable std::mutex Mutex_;
    ScanState State_ = ScanState::Open;
    int64_t ManifestsRead_ = 0;
    int64_t DataFilesRead_ = 0;
    int64_t DataBytesRead_ = 0;
    int64_t RowsDecoded_ = 0;
    int64_t RowsReturned_ = 0;
    int64_t RowsFiltered_ = 0;
    int64_t RowsLimited_ = 0;
    bool LimitStopped_ = false;
    bool CancellationStopped_ = false;
    std::atomic<bool> Cancelled_{false};
    std::optional<ErrorDetail> Failed_;
    size_t NextManifest_ = 0;
    size_t NextIndexedFile_ = 0;
    std::shared_ptr<ResourceReservation> PendingMemory_;
    std::deque<TScanFile> PendingFiles_;
    TScanReadStats ReadStats_;
    // The open data file; the decoder reads it through this source.
    std::shared_ptr<TDataFileSource> Source_;
    // A RecordBatchReader must not outlive its FileReader; members are
    // destroyed in reverse order.
    std::unique_ptr<parquet::arrow::FileReader> FileReader_;
    std::unique_ptr<arrow::RecordBatchReader> BatchReader_;
    std::optional<int64_t> Remaining_;

    void CheckActive()
    {
        if (Cancelled_.load() || (Context_.Closing && Context_.Closing->load())) {
            if (State_ == ScanState::Open) {
                CancellationStopped_ = true;
            }
            throw Failure({"CANCELLED", "scan was cancelled or engine is closing", RetryHint::OperatorAction});
        }
    }

    void ReleaseFile()
    {
        BatchReader_.reset();
        FileReader_.reset();
        FileMemory_.reset();
        FinishSource();
    }

    void Release()
    {
        ReleaseFile();
        PendingFiles_.clear();
        PendingMemory_.reset();
    }

    std::shared_ptr<ResourceReservation> ReserveAdmission(ResourceClass resource, uint64_t bytes)
    {
        auto result = Context_.Resources->Reserve(resource, bytes);
        if (!result) {
            auto error = result.error();
            if (error.code == "RESOURCE_EXHAUSTED") {
                error.retry_hint = RetryHint::SameIdAfterReconcile;
            }
            throw Failure(std::move(error));
        }
        return std::move(*result);
    }

    Result<std::shared_ptr<arrow::RecordBatch>> ReadNext()
    {
        CheckActive();
        while (!Remaining_ || *Remaining_ > 0) {
            CheckActive();
            std::shared_ptr<ResourceReservation> outputMemory;
            if (!BatchReader_) {
                bool opened = false;
                while (!opened) {
                    if (PendingFiles_.empty() && Plan_.Index && NextIndexedFile_ < Plan_.FileCount) {
                        PendingFiles_.push_back(Plan_.Index->Files[Plan_.FileCount - ++NextIndexedFile_]);
                    }
                    while (PendingFiles_.empty() && NextManifest_ < Plan_.Manifests.size()) {
                        CheckActive();
                        auto memory = ReserveAdmission(ResourceClass::Metadata, 256ULL << 20);
                        auto files = Take(ManifestFiles(Context_.FileIO, Plan_, NextManifest_++));
                        ++ManifestsRead_;
                        CheckActive();
                        PendingMemory_ = std::move(memory);
                        PendingFiles_.insert(PendingFiles_.end(),
                            std::make_move_iterator(files.begin()),
                            std::make_move_iterator(files.end()));
                    }
                    if (PendingFiles_.empty()) {
                        break;
                    }
                    CheckActive();
                    if (Prune_ && !FileMayMatch(PendingFiles_.front())) {
                        // Statistics exclude the file: no admission is taken for it.
                        ++ReadStats_.FilesPruned;
                    } else {
                        // Output admission precedes opening; a refusal here or in
                        // OpenFile leaves the descriptor queued for a retry.
                        outputMemory = ReserveAdmission(ResourceClass::Metadata,
                            4096 + static_cast<uint64_t>(OutputFields_.size()) * 2048);
                        opened = OpenFile(PendingFiles_.front());
                        if (!opened) {
                            // Every row group was excluded: only the footer was read.
                            outputMemory.reset();
                        }
                    }
                    // The descriptor is consumed (opened or pruned).
                    PendingFiles_.pop_front();
                    if (PendingFiles_.empty()) {
                        PendingMemory_.reset();
                    }
                }
                if (!opened) {
                    break;
                }
            }
            if (!outputMemory) {
                outputMemory = ReserveAdmission(ResourceClass::Metadata,
                    4096 + static_cast<uint64_t>(OutputFields_.size()) * 2048);
            }
            std::shared_ptr<arrow::RecordBatch> batch;
            DecodeData("read Parquet batch", [&] {
                CheckArrow(BatchReader_->ReadNext(&batch), "CORRUPTION", "read Parquet batch");
            });
            if (!batch) {
                CloseFile();
                continue;
            }
            RowsDecoded_ += batch->num_rows();
            CheckActive();
            if (auto produced = Produce(*batch, std::move(outputMemory))) {
                CheckActive();
                if (Remaining_) {
                    *Remaining_ -= produced->num_rows();
                    LimitStopped_ = *Remaining_ == 0;
                }
                RowsReturned_ += produced->num_rows();
                return produced;
            }
        }
        // Normal completion (all files read or the limit reached, possibly
        // inside an open file) closes the open data file through the checked
        // path, so a close failure is an error rather than EOS. Cancellation is
        // observed first; its cleanup, like a failed scan's, is best effort.
        CheckActive();
        CloseFile();
        Release();
        CheckActive();
        LimitStopped_ = Remaining_ && *Remaining_ == 0;
        State_ = ScanState::Complete;
        return std::shared_ptr<arrow::RecordBatch>{};
    }

    // Adds the reads of the current data file to the cursor totals and drops it.
    void FinishSource()
    {
        if (!Source_) {
            return;
        }
        const auto reads = Source_->Reads();
        DataBytesRead_ += reads.Bytes;
        ReadStats_.ReadCalls += reads.Calls;
        ReadStats_.MaxReadBytes = std::max(ReadStats_.MaxReadBytes, reads.MaxReadBytes);
        ReadStats_.PeakHeldBytes = std::max(ReadStats_.PeakHeldBytes, reads.PeakHeldBytes);
        Source_.reset();
    }

    // A FileIO failure under the decoder keeps its storage classification
    // (an I/O error is not corruption).
    void ThrowReadError() const
    {
        if (Source_) {
            if (auto error = Source_->ReadError()) {
                throw Failure(StorageError(*error, "read data file", EStoragePath::Read, "IO_ERROR"));
            }
        }
    }

    template <class TFunction>
    void DecodeData(std::string_view context, TFunction&& fn)
    {
        try {
            DecodeParquet(context, std::forward<TFunction>(fn));
        } catch (const Failure&) {
            ThrowReadError();
            throw;
        }
    }

    // The file is fully decoded: close its stream like a whole-file read did.
    void CloseFile()
    {
        BatchReader_.reset();
        FileReader_.reset();
        const auto closed = Source_ ? Source_->CloseStream() : iceberg::Status{};
        FinishSource();
        FileMemory_.reset();
        if (!closed) {
            throw Failure(StorageError(closed.error(), "read data file", EStoragePath::Read, "IO_ERROR"));
        }
    }

    // Manifest metrics of the predicate's fields, from the verified descriptor.
    bool FileMayMatch(const TScanFile& file) const
    {
        std::vector<TFieldStats> stats;
        stats.reserve(Predicate_.ReferencedFields.size());
        for (const auto field : Predicate_.ReferencedFields) {
            const auto metrics =
                NGlamDB::NStorage::FindScanFieldMetrics(file.Metrics, FileSchema_->fields()[field].field_id());
            stats.push_back(FileFieldStats(metrics ? &*metrics : nullptr,
                file.RecordCount,
                Context_.Collection.schema.fields[field].type,
                NGlamDB::NStorage::MetricsTruncateLength));
        }
        return MayMatch(Predicate_, stats);
    }

    // Row groups of a verified footer whose statistics may match.
    std::vector<int> SelectRowGroups(const parquet::FileMetaData& footer, const TScanFile& file)
    {
        std::vector<int> groups;
        groups.reserve(static_cast<size_t>(footer.num_row_groups()));
        std::vector<TFieldStats> stats(Predicate_.ReferencedFields.size());
        for (int group = 0; group < footer.num_row_groups(); ++group) {
            if (Prune_) {
                const auto metadata = footer.RowGroup(group);
                for (size_t index = 0; index < stats.size(); ++index) {
                    const auto field = Predicate_.ReferencedFields[index];
                    const auto metrics =
                        NGlamDB::NStorage::FindScanFieldMetrics(file.Metrics, FileSchema_->fields()[field].field_id());
                    const bool withoutNaN = metrics && metrics->NanCount == 0;
                    stats[index] = RowGroupFieldStats(*metadata,
                        static_cast<int>(field),
                        Context_.Collection.schema.fields[field].type,
                        withoutNaN);
                }
                if (!MayMatch(Predicate_, stats)) {
                    ++ReadStats_.RowGroupsPruned;
                    continue;
                }
            }
            groups.push_back(group);
        }
        return groups;
    }

    // Returns false when statistics exclude every row group: only the footer was read.
    bool OpenFile(const TScanFile& file)
    {
        // Reserve before the footer verifier materializes the footer bytes and
        // metadata, with the shared scan budget's data-file rule (twice the
        // file size plus 64 KiB; see SHARED_RESOURCES.md). Read buffers are
        // charged to it and each returned read buffer retains it; at most the
        // file length is held at once (the footer, then one column chunk per
        // read field). Arrow decoding is charged by Pool_.
        RequireInput(file.SizeBytes >= 0 && file.SizeBytes <= (32LL << 20), "invalid data file size");
        auto bytesMemory =
            ReserveAdmission(ResourceClass::Data, static_cast<uint64_t>(file.SizeBytes) * 2 + (64ULL << 10));
        FileMemory_ = ReserveAdmission(ResourceClass::Metadata, 256ULL << 20);
        // Only the footer and the column chunks of read fields are read, each
        // with one positioned read inside the length the manifest records
        // (writer limits bound one data file to 32 MiB in this profile).
        auto input = TakeIceberg(Context_.FileIO->NewInputFile(file.Path, static_cast<size_t>(file.SizeBytes)),
            "IO_ERROR",
            "read data file");
        auto stream = TakeIceberg(input->Open(), "IO_ERROR", "read data file");
        Source_ = std::make_shared<TDataFileSource>(file.Path,
            std::move(input),
            std::move(stream),
            file.SizeBytes,
            std::move(bytesMemory));
        // The same strict footer check as recovery: field IDs, names,
        // repetition, physical/logical types, length and row counts per row
        // group. The reader uses this verified footer instead of decoding it again.
        const auto verified = Source_->AsInputFile();
        auto footer = TakeIceberg(
            NGlamDB::NStorage::ReadParquetFooter(*verified, *FileSchema_, {file.SizeBytes, file.RecordCount}),
            "CORRUPTION",
            "verify Parquet file");
        ++DataFilesRead_;
        CheckActive();
        std::vector<int> rowGroups;
        DecodeData("read Parquet row group statistics", [&] {
            rowGroups = SelectRowGroups(*footer, file);
        });
        ReadStats_.RowGroupsRead += static_cast<int64_t>(rowGroups.size());
        if (rowGroups.empty()) {
            CloseFile();
            return false;
        }
        DecodeData("open Parquet file", [&] {
            parquet::ArrowReaderProperties properties;
            // Synchronous reads, one column chunk at a time, on this thread.
            properties.set_pre_buffer(false);
            properties.set_use_threads(false);
            properties.set_batch_size(BatchRows_);
            auto reader =
                TakeArrow(parquet::arrow::FileReader::Make(Pool_.get(),
                              parquet::ParquetFileReader::Open(Source_, parquet::ReaderProperties(Pool_.get()), footer),
                              properties),
                    "CORRUPTION",
                    "open Parquet file");
            std::vector<int> columns;
            columns.reserve(ReadFields_.size());
            for (const auto field : ReadFields_) {
                columns.push_back(static_cast<int>(field));
            }
            BatchReader_ =
                TakeArrow(reader->GetRecordBatchReader(rowGroups, columns), "CORRUPTION", "open Parquet batches");
            FileReader_ = std::move(reader);
        });
        return true;
    }

    std::shared_ptr<arrow::Array> Column(const arrow::RecordBatch& batch, size_t field) const
    {
        const auto& spec = Context_.Collection.schema.fields[field];
        auto array = batch.GetColumnByName(spec.name);
        if (!array) {
            Fail("CORRUPTION", "read Parquet batch", "batch lacks a requested column");
        }
        const auto expected = MakeArrowType(spec.type);
        if (!array->type()->Equals(*expected)) {
            const bool sameTimestamp = expected->id() == arrow::Type::TIMESTAMP &&
                array->type_id() == arrow::Type::TIMESTAMP &&
                static_cast<const arrow::TimestampType&>(*array->type()).unit() == arrow::TimeUnit::MICRO;
            if (!sameTimestamp) {
                Fail("CORRUPTION", "read Parquet batch", "column type differs from the collection schema");
            }
            array = TakeArrow(array->View(expected), "CORRUPTION", "normalize timestamp type");
        }
        if (!spec.nullable && array->null_count() != 0) {
            Fail("CORRUPTION", "read Parquet batch", "required column contains NULL");
        }
        return array;
    }

    // Filters, projects and applies the limit. Returns nullptr for no rows.
    std::shared_ptr<arrow::RecordBatch> Produce(const arrow::RecordBatch& batch,
        std::shared_ptr<ResourceReservation> outputMemory)
    {
        // Validate every decoded column before any value can be evaluated or returned.
        std::vector<std::shared_ptr<arrow::Array>> readColumns;
        readColumns.reserve(ReadFields_.size());
        for (const auto field : ReadFields_) {
            auto col = Column(batch, field);
            ValidateColumn(Context_.Collection.schema.fields[field], *col);
            readColumns.push_back(std::move(col));
        }
        const auto findReadColumn = [&](size_t field) -> const std::shared_ptr<arrow::Array>& {
            const auto it = std::ranges::lower_bound(ReadFields_, field);
            if (it == ReadFields_.end() || *it != field) {
                Fail("INTERNAL", "read Parquet batch", "missing validated column");
            }
            return readColumns[static_cast<size_t>(it - ReadFields_.begin())];
        };
        const auto selected = [&]() -> std::optional<std::vector<int64_t>> {
            if (!Predicate_.IsAlwaysTrue()) {
                std::vector<std::shared_ptr<arrow::Array>> predicateColumns;
                predicateColumns.reserve(Predicate_.ReferencedFields.size());
                for (const auto field : Predicate_.ReferencedFields) {
                    predicateColumns.push_back(findReadColumn(field));
                }
                auto rows = Take(SelectRows(Predicate_, batch.num_rows(), predicateColumns));
                if (static_cast<int64_t>(rows.size()) != batch.num_rows()) {
                    return rows;
                }
            }
            return std::nullopt;
        }();
        auto count = selected ? static_cast<int64_t>(selected->size()) : batch.num_rows();
        RowsFiltered_ += batch.num_rows() - count;
        if (Remaining_) {
            const auto limited = std::min(count, *Remaining_);
            RowsLimited_ += count - limited;
            count = limited;
        }
        if (count == 0) {
            return nullptr;
        }
        // Flat output arrays need ArrayData, buffer wrappers and vector nodes.
        // Reserve before constructing any retained descriptors. The shared
        // cursor envelope separately covers schema fields and allocator state.
        auto lease = std::make_shared<TOutputLease>(BufferLease_, std::move(outputMemory), OutputUsage_, OutputSchema_);
        auto schema = std::shared_ptr<arrow::Schema>(lease, OutputSchema_.get());
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(OutputFields_.size());
        for (const auto field : OutputFields_) {
            auto array = findReadColumn(field);
            if (selected) {
                const auto rows = std::span<const int64_t>(*selected).first(static_cast<size_t>(count));
                array = Take(TakeRows(*array, rows, Pool_.get()));
            } else if (count != array->length()) {
                array = array->Slice(0, count);
            }
            columns.push_back(arrow::MakeArray(AttachLease(array->data(), lease)));
        }
        return arrow::RecordBatch::Make(std::move(schema), count, std::move(columns));
    }
};

} // namespace

std::optional<TScanReadStats> GetScanReadStats(const ScanCursor& cursor)
{
    const auto* scan = dynamic_cast<const TScanCursor*>(&cursor);
    return scan ? std::optional(scan->ReadStats()) : std::nullopt;
}

Result<std::shared_ptr<ScanCursor>> OpenScan(TScanContext context, const ScanRequest& request)
{
    try {
        context.Resources = context.Resources ? context.Resources : ResourceOwner::ProcessDefault();
        auto controlMemory = Take(context.Resources->Reserve(ResourceClass::Control, ControlMemoryBytes));
        auto planningMemory = Take(context.Resources->Reserve(ResourceClass::Metadata, MetadataWorkBytes));
        RequireInput(request.batch_rows >= 1 && request.batch_rows <= MaxBatchRows,
            "batch_rows must be within 1..65536");
        RequireInput(!request.limit || *request.limit >= 0, "limit must not be negative");
        const auto& schema = context.Collection.schema;
        auto outputFields = ResolveProjection(schema, request.projection);
        auto predicate = Take(BindPredicate(request.predicate, schema, context.Resources));

        auto readFields = outputFields;
        readFields.insert(readFields.end(), predicate.ReferencedFields.begin(), predicate.ReferencedFields.end());
        std::ranges::sort(readFields);
        readFields.erase(std::unique(readFields.begin(), readFields.end()), readFields.end());

        const auto fullSchema = MakeArrowSchema(schema);
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(outputFields.size());
        for (const auto field : outputFields) {
            fields.push_back(fullSchema->field(static_cast<int>(field)));
        }
        auto profile = NGlamDB::NStorage::MakeTableProfile(ToStorageSchema(schema));
        if (!profile) {
            throw Failure({"CORRUPTION",
                BoundedDiagnostic("collection schema profile: " + profile.error().message),
                RetryHint::OperatorAction});
        }
        const bool prune = ScanPruningEnabled() && !predicate.IsAlwaysTrue();
        auto plan =
            Take(context.VerifiedState ? PlanVerifiedScan(*context.VerifiedState, context.Base, request.selector)
                                       : PlanManifests(context.FileIO, context.Base, request.selector));
        // Uncached lists/schemas remain materialized in the plan. Verified
        // membership already retains the backend's shared generation lease.
        auto retainedPlan = context.VerifiedState ? nullptr : planningMemory;
        return std::make_shared<TScanCursor>(std::move(context),
            std::move(retainedPlan),
            std::move(controlMemory),
            std::move(profile->Schema),
            arrow::schema(std::move(fields)),
            std::move(plan),
            std::move(predicate),
            std::move(outputFields),
            std::move(readFields),
            request.batch_rows,
            request.limit,
            prune);
    } catch (const Failure& error) {
        return std::unexpected(error.detail);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    } catch (const std::exception& error) {
        return std::unexpected(ErrorDetail{"INTERNAL", BoundedDiagnostic(error.what()), RetryHint::OperatorAction});
    }
}

} // namespace glamdb::detail
