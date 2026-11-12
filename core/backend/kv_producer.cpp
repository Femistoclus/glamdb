#include "core/backend/kv_producer.h"

#include "core/api/binary.h"
#include "core/api/kv_input.h"
#include "core/api/scalar_validation.h"
#include "core/backend/file_window.h"
#include "core/backend/kv_spool.h"
#include "core/backend/storage_errors.h"
#include "core/catalog/durable_sqlite_catalog.h"
#include "core/engine/storage_mapping.h"
#include "core/scan/parquet_input.h"
#include "core/storage/table_profile.h"

#include <arrow/api.h>
#include <arrow/util/byte_size.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/schema.h>
#include <iceberg/table_metadata.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <format>
#include <numeric>
#include <set>
#include <utility>

namespace glamdb::detail {
namespace {

namespace NStorage = NGlamDB::NStorage;
constexpr int64_t ReadBatchRows = 1024;
constexpr uint64_t MaxSourceBytes = 4ULL << 30;

[[noreturn]] void Fail(std::string_view code, std::string_view message)
{
    throw Failure(
        {std::string(code), BoundedDiagnostic("KV producer: " + std::string(message)), RetryHint::OperatorAction});
}

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(std::move(result.error()));
    }
    return std::move(*result);
}

void Check(Status result)
{
    if (!result) {
        throw Failure(std::move(result.error()));
    }
}

void CheckActive(const TKvProductionContext& context)
{
    if (context.Input.kv->Cancellation && context.Input.kv->Cancellation->load(std::memory_order_relaxed)) {
        Fail("CANCELLED", "operation was cancelled");
    }
    if (std::chrono::steady_clock::now() >= context.Deadline) {
        Fail("DEADLINE_EXCEEDED", "deadline expired during bounded production");
    }
}

size_t KeyColumn(const SchemaSpec& schema)
{
    const auto field = std::ranges::find(schema.fields, "key", &FieldSpec::name);
    Require(field != schema.fields.end() && field->type == FieldType::String && !field->nullable,
        "INVALID_ARGUMENT",
        "KV schema requires a required string key");
    return static_cast<size_t>(field - schema.fields.begin());
}

std::string EncodeRow(const SchemaSpec& schema, const Row& row)
{
    Require(row.size() == schema.fields.size(), "INTERNAL", "KV normalized row has an invalid width");
    Encoder encoder;
    for (size_t column = 0; column < row.size(); ++column) {
        const auto& cell = row[column];
        const auto present = !std::holds_alternative<std::monostate>(cell);
        encoder.Number<uint8_t>(present ? 1 : 0);
        if (!present) {
            continue;
        }
        switch (schema.fields[column].type) {
            case FieldType::String:
                encoder.Text(std::get<std::string>(cell));
                break;
            case FieldType::Bool:
                encoder.Number<uint8_t>(std::get<bool>(cell) ? 1 : 0);
                break;
            case FieldType::Int64:
                encoder.Number(std::get<int64_t>(cell));
                break;
            case FieldType::Float64:
                encoder.Number(std::bit_cast<uint64_t>(std::get<double>(cell)));
                break;
            case FieldType::TimestampUsUtc:
                encoder.Number(std::get<TimestampUs>(cell).value);
                break;
            case FieldType::Decimal: {
                const auto& value = std::get<Decimal>(cell);
                encoder.Number(value.coefficient_high);
                encoder.Number(value.coefficient_low);
                break;
            }
            case FieldType::Date:
                encoder.Number(std::get<Date>(cell).days_since_epoch);
                break;
        }
    }
    return std::move(encoder.bytes);
}

Row DecodeRow(const SchemaSpec& schema, std::string_view bytes)
{
    Decoder decoder{bytes};
    Row row;
    row.reserve(schema.fields.size());
    for (const auto& field : schema.fields) {
        const auto present = decoder.Number<uint8_t>();
        Require(present <= 1 && (present != 0 || field.nullable), "IO_ERROR", "invalid KV scratch row");
        if (present == 0) {
            row.emplace_back(std::monostate{});
            continue;
        }
        switch (field.type) {
            case FieldType::String:
                row.emplace_back(decoder.Text());
                break;
            case FieldType::Bool: {
                const auto value = decoder.Number<uint8_t>();
                Require(value <= 1, "IO_ERROR", "invalid KV scratch boolean");
                row.emplace_back(value != 0);
                break;
            }
            case FieldType::Int64:
                row.emplace_back(decoder.Number<int64_t>());
                break;
            case FieldType::Float64:
                row.emplace_back(std::bit_cast<double>(decoder.Number<uint64_t>()));
                break;
            case FieldType::TimestampUsUtc:
                row.emplace_back(TimestampUs{decoder.Number<int64_t>()});
                break;
            case FieldType::Decimal: {
                const auto high = decoder.Number<int64_t>();
                const auto low = decoder.Number<uint64_t>();
                row.emplace_back(Decimal{high, low, field.precision, field.scale});
                break;
            }
            case FieldType::Date:
                row.emplace_back(Date{decoder.Number<int32_t>()});
                break;
        }
    }
    decoder.End();
    return row;
}

Row ReadRow(const SchemaSpec& schema, const arrow::RecordBatch& batch, int64_t index)
{
    Row row;
    row.reserve(schema.fields.size());
    for (size_t column = 0; column < schema.fields.size(); ++column) {
        const auto& field = schema.fields[column];
        const auto& array = *batch.column(static_cast<int>(column));
        if (array.IsNull(index)) {
            Require(field.nullable, "CORRUPTION", "required KV column contains NULL");
            row.emplace_back(std::monostate{});
            continue;
        }
        switch (field.type) {
            case FieldType::String: {
                const auto value = static_cast<const arrow::StringArray&>(array).GetView(index);
                Require(value.size() <= (field.name == "key" ? 1024ULL : 1ULL << 20),
                    "CORRUPTION",
                    "KV stored string exceeds its canonical limit");
                row.emplace_back(std::string(value));
                break;
            }
            case FieldType::Bool:
                row.emplace_back(static_cast<const arrow::BooleanArray&>(array).Value(index));
                break;
            case FieldType::Int64:
                row.emplace_back(static_cast<const arrow::Int64Array&>(array).Value(index));
                break;
            case FieldType::Float64: {
                const auto value = static_cast<const arrow::DoubleArray&>(array).Value(index);
                Require(std::isfinite(value) && (value != 0.0 || !std::signbit(value)),
                    "CORRUPTION",
                    "KV stored float is not canonical");
                row.emplace_back(value);
                break;
            }
            case FieldType::TimestampUsUtc: {
                const auto value = static_cast<const arrow::TimestampArray&>(array).Value(index);
                Require(value >= -62135596800000000LL && value <= 253402300799999999LL,
                    "CORRUPTION",
                    "KV stored timestamp is outside the supported UTC range");
                row.emplace_back(TimestampUs{value});
                break;
            }
            case FieldType::Decimal: {
                const arrow::Decimal128 value(static_cast<const arrow::Decimal128Array&>(array).GetValue(index));
                Require(FitsDecimalPrecision(value, field.precision), "CORRUPTION", "KV decimal exceeds its precision");
                row.emplace_back(Decimal{value.high_bits(), value.low_bits(), field.precision, field.scale});
                break;
            }
            case FieldType::Date: {
                const auto value = static_cast<const arrow::Date32Array&>(array).Value(index);
                Require(value >= -719162 && value <= 2932896, "CORRUPTION", "KV stored date is outside its range");
                row.emplace_back(Date{value});
                break;
            }
        }
    }
    return row;
}

// Conservative per-row Arrow/writer estimate, including validity and offsets.
int64_t RowBytes(const SchemaSpec& schema, const Row& row)
{
    int64_t bytes = 0;
    for (size_t column = 0; column < schema.fields.size(); ++column) {
        bytes += 1;
        switch (schema.fields[column].type) {
            case FieldType::String:
                bytes += 8;
                if (const auto* value = std::get_if<std::string>(&row[column])) {
                    bytes += static_cast<int64_t>(value->size());
                }
                break;
            case FieldType::Bool:
                bytes += 1;
                break;
            case FieldType::Decimal:
                bytes += 16;
                break;
            case FieldType::Date:
                bytes += 4;
                break;
            case FieldType::Int64:
            case FieldType::Float64:
            case FieldType::TimestampUsUtc:
                bytes += 8;
                break;
        }
    }
    Require(bytes <= (2LL << 20), "RESOURCE_EXHAUSTED", "KV row exceeds the writer byte limit");
    return bytes;
}

struct TRoute
{
    std::shared_ptr<const iceberg::DataFile> File;
    std::string Lower;
    std::string Upper;
};

std::vector<TRoute> ReadRoutes(const TKvProductionContext& context)
{
    Require(context.Base.ScanIndex && context.Base.ScanIndex->KeyValue,
        "CORRUPTION",
        "KV mutation requires verified interval membership");
    const auto fieldID = static_cast<int32_t>(KeyColumn(context.Collection.schema) + 1);
    const auto sequence = context.Base.Metadata->last_sequence_number;
    const auto live = [sequence](const NStorage::TScanDataFile& file) {
        return file.IntroducedSequence <= sequence && (!file.RemovedSequence || *file.RemovedSequence > sequence);
    };
    size_t liveCount = 0;
    for (const auto& file : context.Base.ScanIndex->Files) {
        CheckActive(context);
        liveCount += live(file) ? 1 : 0;
    }
    std::vector<TRoute> routes;
    routes.reserve(liveCount);
    for (const auto& file : context.Base.ScanIndex->Files) {
        CheckActive(context);
        if (!live(file)) {
            continue;
        }
        Require(file.DataFile && file.RecordCount > 0 && file.SizeBytes > 0 && file.SizeBytes <= (32LL << 20),
            "CORRUPTION",
            "KV live file has an invalid verified descriptor");
        const auto lower = file.DataFile->lower_bounds.find(fieldID);
        const auto upper = file.DataFile->upper_bounds.find(fieldID);
        Require(lower != file.DataFile->lower_bounds.end() && upper != file.DataFile->upper_bounds.end(),
            "CORRUPTION",
            "KV live file lacks full key bounds");
        routes.push_back({file.DataFile,
            lower->second.empty()
                ? std::string()
                : std::string(reinterpret_cast<const char*>(lower->second.data()), lower->second.size()),
            upper->second.empty()
                ? std::string()
                : std::string(reinterpret_cast<const char*>(upper->second.data()), upper->second.size())});
    }
    size_t comparisons = 0;
    std::ranges::sort(routes, [&](const TRoute& left, const TRoute& right) {
        if ((++comparisons & 1023) == 0) {
            CheckActive(context);
        }
        return left.Lower < right.Lower;
    });
    CheckActive(context);
    for (size_t i = 0; i < routes.size(); ++i) {
        CheckActive(context);
        Require(routes[i].Lower <= routes[i].Upper && (i == 0 || routes[i - 1].Upper < routes[i].Lower),
            "CORRUPTION",
            "KV live key ranges overlap or are unordered");
    }
    return routes;
}

class TFileRows
{
public:
    TFileRows(const TKvProductionContext& context, const TRoute& route)
        : Context_(context)
        , Route_(route)
        , KeyColumn_(KeyColumn(context.Collection.schema))
    {
        CheckActive(Context_);
        auto schema = Context_.Base.Metadata->Schema();
        if (!schema) {
            throw Failure(StorageError(schema.error(), "read KV schema", EStoragePath::Read, "CORRUPTION"));
        }
        Source_ = Take(TScanRandomAccessFile::Open(*Context_.FileIO,
            route.File->file_path,
            route.File->file_size_in_bytes,
            {.Pool = Context_.Pool, .Closing = Context_.Input.kv->Cancellation}));
        auto footer = Take(Source_->ReadFooter(**schema, {route.File->file_size_in_bytes, route.File->record_count}));
        Decode([&] {
            parquet::ReaderProperties readerProperties(Context_.Pool);
            readerProperties.set_thrift_string_size_limit(32 << 20);
            readerProperties.set_thrift_container_size_limit(16384);
            auto parquetReader = parquet::ParquetFileReader::Open(Source_, readerProperties, std::move(footer));
            parquet::ArrowReaderProperties arrowProperties(false);
            arrowProperties.set_pre_buffer(false);
            arrowProperties.set_batch_size(ReadBatchRows);
            arrowProperties.set_io_context(arrow::io::IOContext(Context_.Pool));
            Reader_ =
                TakeArrow(parquet::arrow::FileReader::Make(Context_.Pool, std::move(parquetReader), arrowProperties));
            Batches_ = TakeArrow(Reader_->GetRecordBatchReader());
        });
    }

    ~TFileRows()
    {
        Batches_.reset();
        Reader_.reset();
        if (Source_) {
            // Normal EOF checks close in Next; unwinding preserves its first error.
            // NOLINTNEXTLINE(bugprone-unused-return-value)
            (void)Source_->CloseStream();
        }
    }

    std::optional<Row> Next()
    {
        CheckActive(Context_);
        if (Done_) {
            return std::nullopt;
        }
        while (!Batch_ || Offset_ == Batch_->num_rows()) {
            Decode([&] {
                const auto status = Batches_->ReadNext(&Batch_);
                if (!status.ok()) {
                    Fail(status.IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "CORRUPTION", status.ToString());
                }
            });
            CheckActive(Context_);
            Offset_ = 0;
            if (!Batch_) {
                Require(Rows_ == Route_.File->record_count && Previous_ && *Previous_ == Route_.Upper,
                    "CORRUPTION",
                    "KV file rows or bounds differ from verified membership");
                Done_ = true;
                Batches_.reset();
                Reader_.reset();
                Check(Source_->CloseStream());
                return std::nullopt;
            }
            ValidateBatch();
        }
        auto row = ReadRow(Context_.Collection.schema, *Batch_, Offset_++);
        const auto& key = std::get<std::string>(row[KeyColumn_]);
        Require((Previous_ ? *Previous_ < key : key == Route_.Lower) && key <= Route_.Upper,
            "CORRUPTION",
            "KV file is not strictly sorted and unique within its full key bounds");
        Previous_ = key;
        ++Rows_;
        return row;
    }

private:
    const TKvProductionContext& Context_;
    const TRoute& Route_;
    const size_t KeyColumn_;
    std::shared_ptr<TScanRandomAccessFile> Source_;
    std::unique_ptr<parquet::arrow::FileReader> Reader_;
    std::unique_ptr<arrow::RecordBatchReader> Batches_;
    std::shared_ptr<arrow::RecordBatch> Batch_;
    std::optional<std::string> Previous_;
    int64_t Offset_ = 0;
    int64_t Rows_ = 0;
    bool Done_ = false;

    template <class T>
    T TakeArrow(arrow::Result<T> result)
    {
        if (!result.ok()) {
            Fail(result.status().IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "CORRUPTION", result.status().ToString());
        }
        return std::move(result).ValueUnsafe();
    }

    template <class TFunction>
    void Decode(TFunction&& function)
    {
        try {
            std::forward<TFunction>(function)();
        } catch (const std::bad_alloc&) {
            throw;
        } catch (const std::exception& error) {
            if (auto sourceError = Source_->LastError()) {
                throw Failure(std::move(*sourceError));
            }
            if (const auto* failure = dynamic_cast<const Failure*>(&error)) {
                throw *failure;
            }
            if (const auto* parquetError = dynamic_cast<const parquet::ParquetStatusException*>(&error);
                parquetError && parquetError->status().IsOutOfMemory())
            {
                Fail("RESOURCE_EXHAUSTED", error.what());
            }
            Fail("CORRUPTION", error.what());
        }
    }

    void ValidateBatch()
    {
        const auto& schema = Context_.Collection.schema;
        Require(Batch_->num_columns() == static_cast<int>(schema.fields.size()) && Batch_->num_rows() <= ReadBatchRows,
            "CORRUPTION",
            "KV file returned an invalid batch shape");
        for (size_t column = 0; column < schema.fields.size(); ++column) {
            const auto& field = schema.fields[column];
            const auto& array = *Batch_->column(static_cast<int>(column));
            const auto type = MakeArrowType(field);
            const bool timestamp = field.type == FieldType::TimestampUsUtc &&
                array.type_id() == arrow::Type::TIMESTAMP &&
                static_cast<const arrow::TimestampType&>(*array.type()).unit() == arrow::TimeUnit::MICRO;
            Require(Batch_->column_name(static_cast<int>(column)) == field.name &&
                    (array.type()->Equals(*type) || timestamp),
                "CORRUPTION",
                "KV file column differs from the collection schema");
            const auto status = array.ValidateFull();
            if (!status.ok()) {
                Fail(status.IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "CORRUPTION", status.ToString());
            }
        }
    }
};

NStorage::TDataWriterLimits OutputLimits(NStorage::TDataWriterLimits limits)
{
    limits.MaxRows = 100'000;
    limits.MaxInputBytes = std::min<int64_t>(limits.MaxInputBytes, 16LL << 20);
    limits.MaxFileBytes = std::min<int64_t>(limits.MaxFileBytes, 32LL << 20);
    limits.MaxRowGroupBytes = std::min<int64_t>(limits.MaxRowGroupBytes, 8LL << 20);
    return limits;
}

class TOutput
{
public:
    TOutput(const TKvProductionContext& context, TKvProduction& result)
        : Context_(context)
        , Result_(result)
        , Limits_(OutputLimits(context.Writer))
        , Window_(
              {Limits_.MaxRows, Limits_.MaxInputBytes, context.Input.kv->TargetFileBytes},
              context.Pool,
              [this](const TBoundedFileWindow::TBatches& batches, int64_t rows) {
                  Write(batches, rows);
              },
              [this] {
                  CheckActive(Context_);
              })
    {
        Rows_.reserve(ReadBatchRows);
    }

    void Push(Row row)
    {
        CheckActive(Context_);
        const auto bytes = RowBytes(Context_.Collection.schema, row);
        Require(bytes <= Limits_.MaxInputBytes && bytes <= Limits_.MaxRowGroupBytes,
            "RESOURCE_EXHAUSTED",
            "KV output row exceeds its file or row-group budget");
        if (!Rows_.empty() && bytes > Limits_.MaxInputBytes - PendingBytes_) {
            FlushRows();
        }
        Rows_.push_back(std::move(row));
        PendingBytes_ += bytes;
        ++Result_.RowsWritten;
        if (Rows_.size() == static_cast<size_t>(ReadBatchRows) || PendingBytes_ >= Context_.Input.kv->TargetFileBytes) {
            FlushRows();
        }
    }

    void FlushRoute()
    {
        FlushRows();
        Window_.Flush();
    }

private:
    const TKvProductionContext& Context_;
    TKvProduction& Result_;
    const NStorage::TDataWriterLimits Limits_;
    TBoundedFileWindow Window_;
    std::vector<Row> Rows_;
    int64_t PendingBytes_ = 0;

    void Write(const TBoundedFileWindow::TBatches& batches, int64_t rows)
    {
        CheckActive(Context_);
        Require(Result_.Added.size() < 256, "RESOURCE_EXHAUSTED", "KV operation exceeds its output file limit");
        const auto path = std::format("store://{}/warehouse/{}/data/{}-{}-{:05}.parquet",
            Context_.Binding.base.store_id,
            Context_.Binding.collection_id,
            Context_.Binding.operation_id,
            Context_.Binding.attempt_id,
            Result_.Added.size());
        auto data = NStorage::WriteDataFile(Context_.FileIO,
            Context_.Attempt,
            *Context_.Base.Metadata,
            path,
            batches,
            Limits_,
            Context_.WriteSpec);
        if (!data) {
            throw Failure(StorageError(data.error(), "write KV data file", EStoragePath::Write, "IO_ERROR"));
        }
        Require(data->DataFile && data->DataFile->record_count == rows, "IO_ERROR", "KV output row count changed");
        Result_.Added.push_back(std::move(data->DataFile));
        if (Context_.CheckCapacity) {
            Check(Context_.CheckCapacity());
        }
        CheckActive(Context_);
    }

    void FlushRows()
    {
        if (Rows_.empty()) {
            return;
        }
        Window_.Add(Take(MakeRecordBatch(Context_.Collection.schema, Rows_, Context_.Pool)));
        Rows_.clear();
        PendingBytes_ = 0;
    }
};

int64_t
MergeRoute(const TKvProductionContext& context, TKvSpool& spool, int64_t ordinal, const TRoute* route, TOutput* output)
{
    auto mutations = Take(spool.ReadRoute(ordinal));
    auto next = Take(mutations->Next());
    std::unique_ptr<TFileRows> existing;
    if (route) {
        existing = std::make_unique<TFileRows>(context, *route);
    }
    auto old = existing ? existing->Next() : std::nullopt;
    const auto keyColumn = KeyColumn(context.Collection.schema);
    const bool deleting = context.Input.kv->Kind == MutationKind::Delete;
    int64_t affected = 0;
    while (old || next) {
        CheckActive(context);
        if (next && (!old || next->Key < std::get<std::string>((*old)[keyColumn]))) {
            if (!deleting) {
                ++affected;
                if (output) {
                    output->Push(DecodeRow(context.Collection.schema, next->Payload));
                }
            }
            next = Take(mutations->Next());
            continue;
        }
        if (next && next->Key == std::get<std::string>((*old)[keyColumn])) {
            if (deleting) {
                ++affected;
            } else {
                affected += EncodeRow(context.Collection.schema, *old) != next->Payload ? 1 : 0;
                if (output) {
                    output->Push(DecodeRow(context.Collection.schema, next->Payload));
                }
            }
            next = Take(mutations->Next());
        } else if (output) {
            output->Push(std::move(*old));
        }
        old = existing ? existing->Next() : std::nullopt;
    }
    if (output) {
        output->FlushRoute();
    }
    return affected;
}

TKvProduction Compact(const TKvProductionContext& context, const std::vector<TRoute>& routes)
{
    TKvProduction result;
    const auto maxFiles = static_cast<size_t>(std::min<int64_t>(context.Input.kv->MaxFiles, 256));
    const auto target = std::min<int64_t>(context.Input.kv->TargetFileBytes, context.Writer.MaxInputBytes);
    size_t start = 0;
    size_t count = 0;
    int64_t physical = 0;
    int64_t rows = 0;
    for (size_t i = 0; i < routes.size(); ++i) {
        const auto& file = *routes[i].File;
        if (file.file_size_in_bytes > target / 4 || count == maxFiles || file.record_count > 100'000 - rows ||
            file.file_size_in_bytes > target - physical)
        {
            if (count >= 4) {
                break;
            }
            start = i + 1;
            count = 0;
            physical = 0;
            rows = 0;
            continue;
        }
        ++count;
        physical += file.file_size_in_bytes;
        rows += file.record_count;
    }
    if (count < 4) {
        return result;
    }
    // Establish the useful size reduction before producing any output. This
    // also validates every old key/value while the base remains pinned.
    int64_t decoded = 0;
    for (size_t i = start; i < start + count; ++i) {
        TFileRows old(context, routes[i]);
        while (auto row = old.Next()) {
            const auto bytes = RowBytes(context.Collection.schema, *row);
            if (bytes > target - decoded) {
                return {};
            }
            decoded += bytes;
        }
    }
    TOutput output(context, result);
    for (size_t i = start; i < start + count; ++i) {
        TFileRows old(context, routes[i]);
        while (auto row = old.Next()) {
            output.Push(std::move(*row));
        }
        result.Removed.push_back(std::make_shared<iceberg::DataFile>(*routes[i].File));
        result.RowsRemoved += routes[i].File->record_count;
    }
    output.FlushRoute();
    Require(result.Added.size() * 4 <= result.Removed.size() && result.RowsWritten == result.RowsRemoved,
        "INTERNAL",
        "KV compaction did not preserve rows and improve the selected group");
    return result;
}

} // namespace

TKvProduction ProduceKvMutation(const TKvProductionContext& context)
{
    Require(context.Input.kv != nullptr, "INTERNAL", "KV producer requires mutation identity");
    CheckActive(context);
    const auto routes = ReadRoutes(context);
    if (context.Input.kv->Kind == MutationKind::Compact) {
        return Compact(context, routes);
    }
    std::string token;
    token.reserve(32);
    for (const char value : context.Binding.attempt_id) {
        if (value != '-') {
            token.push_back(value);
        }
    }
    auto spool = Take(TKvSpool::Make(context.ScratchDirectory, token, 4ULL << 30, 16ULL << 30, [&]() -> Status {
        try {
            CheckActive(context);
            return {};
        } catch (const Failure& error) {
            return std::unexpected(error.detail);
        }
    }));
    const bool deleting = context.Input.kv->Kind == MutationKind::Delete;
    const SchemaSpec keySchema{{{"key", FieldType::String, false}}};
    const auto& inputSchema = deleting ? keySchema : context.Collection.schema;
    const auto keyColumn = KeyColumn(inputSchema);
    std::set<int64_t> touched;
    uint64_t sourceBytes = 0;
    int64_t inputRows = 0;
    const auto insert = [&](const Row& row) {
        CheckActive(context);
        const auto bytes = static_cast<uint64_t>(RowBytes(inputSchema, row));
        Require(bytes <= MaxSourceBytes - sourceBytes,
            "RESOURCE_EXHAUSTED",
            "KV source exceeds its decoded byte limit");
        sourceBytes += bytes;
        ++inputRows;
        const auto& key = std::get<std::string>(row[keyColumn]);
        auto route = std::ranges::lower_bound(routes, key, {}, &TRoute::Upper);
        if (route == routes.end() && !routes.empty()) {
            --route;
        }
        if (deleting && (routes.empty() || key < route->Lower || key > route->Upper)) {
            return;
        }
        const auto ordinal = static_cast<int64_t>(route - routes.begin());
        touched.insert(ordinal);
        Require(touched.size() <= 256, "RESOURCE_EXHAUSTED", "KV mutation touches too many files");
        Check(spool->Insert(ordinal,
            key,
            deleting ? std::string() : EncodeRow(context.Collection.schema, row),
            deleting ? DuplicatePolicy::FirstWins : context.Input.kv->Duplicates));
    };
    const auto consume = [&](const NormalizedInput& chunk) {
        const auto batch = chunk.writer_batch ? chunk.writer_batch : chunk.Columns;
        if (batch) {
            Require(batch->num_columns() == static_cast<int>(inputSchema.fields.size()) && batch->num_rows() >= 0 &&
                    batch->num_rows() <= 10'000 && batch->schema()->Equals(*MakeArrowSchema(inputSchema)),
                "INVALID_ARGUMENT",
                "KV source column shape or types changed");
            const auto bytes = EstimateRecordBatchBytes(inputSchema, *batch);
            Require(bytes >= 0 && bytes <= (16LL << 20),
                "RESOURCE_EXHAUSTED",
                "KV source chunk exceeds its input byte limit");
            for (int64_t row = 0; row < batch->num_rows(); ++row) {
                insert(ReadRow(inputSchema, *batch, row));
            }
        } else {
            Require(chunk.rows.size() <= 10'000, "INVALID_ARGUMENT", "KV source chunk exceeds its row limit");
            for (const auto& row : chunk.rows) {
                Require(row.size() == inputSchema.fields.size(), "INVALID_ARGUMENT", "KV source row width changed");
                insert(row);
            }
        }
    };
    if (context.Input.bulk) {
        while (true) {
            CheckActive(context);
            auto next = Take(context.Input.bulk->Next());
            CheckActive(context);
            if (next.GetRowCount() == 0) {
                break;
            }
            Require(next.GetRowCount() <= 10'000, "INVALID_ARGUMENT", "KV source chunk exceeds its row limit");
            consume(next);
        }
    } else {
        consume(context.Input);
    }
    Require(inputRows == context.Input.kv->InputRows, "INVALID_ARGUMENT", "KV input row count changed");
    Check(spool->Seal());
    TKvProduction result;
    std::vector<int64_t> changed;
    changed.reserve(touched.size());
    for (const auto ordinal : touched) {
        const auto affected = MergeRoute(context,
            *spool,
            ordinal,
            routes.empty() ? nullptr : &routes[static_cast<size_t>(ordinal)],
            nullptr);
        if (affected == 0) {
            continue;
        }
        changed.push_back(ordinal);
        result.RowsAffected += affected;
    }
    TOutput output(context, result);
    for (const auto ordinal : changed) {
        const auto* route = routes.empty() ? nullptr : &routes[static_cast<size_t>(ordinal)];
        (void)MergeRoute(context, *spool, ordinal, route, &output);
        if (route) {
            result.Removed.push_back(std::make_shared<iceberg::DataFile>(*route->File));
            result.RowsRemoved += route->File->record_count;
        }
    }
    CheckActive(context);
    return result;
}

} // namespace glamdb::detail
