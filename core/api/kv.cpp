#include "storage/engine.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/api/kv_input.h"
#include "core/api/write_settings.h"
#include "core/engine/storage_mapping.h"
#include "core/resources/arrow_pool.h"
#include "core/scan/read_budget.h"
#include "storage/resources.h"

#include <arrow/api.h>

#include <algorithm>
#include <unordered_map>

namespace glamdb {
Result<std::string> detail::KvPayloadHash(const SchemaSpec& schema, std::string_view sourceHash, const TKvInput& input)
{
    auto schemaHash = SchemaFingerprint(schema);
    if (!schemaHash) {
        return std::unexpected(schemaHash.error());
    }
    return Sha256("glamdb.kv1/" + std::to_string(static_cast<int>(input.Kind)) + "/" +
        std::to_string(static_cast<int>(input.Duplicates)) + "/" + *schemaHash + "/" + std::string(sourceHash) + "/" +
        std::to_string(input.MaxFiles) + "/" + std::to_string(input.TargetFileBytes) + "/" + input.Identity);
}
namespace {
using detail::Failure;
using detail::Require;

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(std::move(result.error()));
    }
    return std::move(*result);
}

// Charge the complete DTO before allocating it, including string allocator
// slack. A second charge covers each duplicate result position.
uint64_t RowBytes(const arrow::RecordBatch& batch, int64_t row)
{
    uint64_t bytes = sizeof(Row) + static_cast<uint64_t>(batch.num_columns()) * sizeof(Cell);
    for (const auto& column : batch.columns()) {
        if (column->type_id() == arrow::Type::STRING && !column->IsNull(row)) {
            bytes += 2 * static_cast<uint64_t>(static_cast<const arrow::StringArray&>(*column).value_length(row)) + 32;
        }
    }
    return bytes;
}

Row ReadRow(const SchemaSpec& schema, const arrow::RecordBatch& batch, int64_t row)
{
    Row output;
    output.reserve(schema.fields.size());
    for (size_t i = 0; i < schema.fields.size(); ++i) {
        const auto& array = *batch.column(static_cast<int>(i));
        if (array.IsNull(row)) {
            output.emplace_back(std::monostate{});
            continue;
        }
        switch (schema.fields[i].type) {
            case FieldType::String:
                output.emplace_back(static_cast<const arrow::StringArray&>(array).GetString(row));
                break;
            case FieldType::Bool:
                output.emplace_back(static_cast<const arrow::BooleanArray&>(array).Value(row));
                break;
            case FieldType::Int64:
                output.emplace_back(static_cast<const arrow::Int64Array&>(array).Value(row));
                break;
            case FieldType::Float64:
                output.emplace_back(static_cast<const arrow::DoubleArray&>(array).Value(row));
                break;
            case FieldType::TimestampUsUtc:
                output.emplace_back(TimestampUs{static_cast<const arrow::TimestampArray&>(array).Value(row)});
                break;
            case FieldType::Decimal: {
                const auto scalar = static_cast<const arrow::Decimal128Array&>(array).GetScalar(row);
                Require(scalar.ok(), "CORRUPTION", "cannot decode decimal lookup value");
                const auto& value = static_cast<const arrow::Decimal128Scalar&>(**scalar).value;
                output.emplace_back(
                    Decimal{value.high_bits(), value.low_bits(), schema.fields[i].precision, schema.fields[i].scale});
                break;
            }
            case FieldType::Date:
                output.emplace_back(Date{static_cast<const arrow::Date32Array&>(array).Value(row)});
                break;
        }
    }
    return output;
}
// Reopens the same snapshot for both canonical-validation and production
// passes. Scan order is the verified append file order, then physical row order.
class TMigrationSource final : public BulkAppendSource
{
public:
    TMigrationSource(std::function<Result<std::shared_ptr<ScanCursor>>()> open,
        std::shared_ptr<detail::TReadBudget> budget,
        std::shared_ptr<ResourceOwner> owner,
        SchemaSpec schema)
        : Open_(std::move(open))
        , Budget_(std::move(budget))
        , Pool_(detail::MakeResourcePool(std::move(owner)))
        , Schema_(std::move(schema))
    { }
    Status Rewind() override
    {
        Batch_.reset();
        Cursor_.reset();
        Position_ = 0;
        auto opened = Open_();
        if (!opened) {
            return std::unexpected(Budget_->Error().value_or(opened.error()));
        }
        Cursor_ = std::move(*opened);
        return {};
    }
    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (auto active = Budget_->Charge(0); !active) {
            return std::unexpected(active.error());
        }
        if (!Batch_ || Position_ == Batch_->num_rows()) {
            auto next = Cursor_->Next();
            if (!next) {
                return std::unexpected(Budget_->Error().value_or(next.error()));
            }
            Batch_ = std::move(*next);
            Position_ = 0;
            if (!Batch_) {
                return nullptr;
            }
        }
        auto rows = std::min<int64_t>(1000, Batch_->num_rows() - Position_);
        auto slice = Batch_->Slice(Position_, rows);
        while (detail::EstimateRecordBatchBytes(Schema_, *slice) > (8LL << 20)) {
            if (rows == 1) {
                return std::unexpected(
                    ErrorDetail{"RESOURCE_EXHAUSTED", "migration row exceeds its bounded chunk", RetryHint::FixInput});
            }
            rows = std::max<int64_t>(1, rows / 2);
            slice = Batch_->Slice(Position_, rows);
        }
        auto copied = detail::CopyRecordBatch(*slice, Pool_.get());
        if (!copied) {
            return copied;
        }
        Position_ += rows;
        return copied;
    }

private:
    std::function<Result<std::shared_ptr<ScanCursor>>()> Open_;
    std::shared_ptr<detail::TReadBudget> Budget_;
    // Declared before the buffers so their borrowed pool remains alive.
    std::shared_ptr<arrow::MemoryPool> Pool_;
    SchemaSpec Schema_;
    std::shared_ptr<ScanCursor> Cursor_;
    std::shared_ptr<arrow::RecordBatch> Batch_;
    int64_t Position_ = 0;
};
} // namespace

Result<LookupResult> Engine::Lookup(const LookupRequest& request) const
{
    try {
        Require(request.keys.size() <= 4096, "INVALID_ARGUMENT", "lookup supports at most 4096 keys");
        const auto collection = Take(GetCollection(request.collection_id));
        Require(collection.mode == CollectionMode::KeyValue,
            "COLLECTION_MODE_MISMATCH",
            "lookup requires a KV collection");
        const auto keyField = std::ranges::find(collection.schema.fields, "key", &FieldSpec::name);
        Require(keyField != collection.schema.fields.end(), "CORRUPTION", "KV collection has no key field");
        const auto keyColumn = static_cast<int>(keyField - collection.schema.fields.begin());
        auto working = Take(GetResourceOwner()->Reserve(ResourceClass::Data, 64ULL << 20));
        auto retained = Take(GetResourceOwner()->Reserve(ResourceClass::Data, 32ULL << 20));
        std::unordered_map<std::string, size_t> multiplicity;
        for (const auto& key : request.keys) {
            Require(key.size() <= 1024 && detail::ValidUtf8(key),
                "INVALID_ARGUMENT",
                "lookup key must be UTF-8 and at most 1024 bytes");
            ++multiplicity[key];
        }
        std::vector<OptionalLiteral> keys;
        keys.reserve(multiplicity.size());
        for (const auto& [key, count] : multiplicity) {
            (void)count;
            keys.emplace_back(Literal(key));
        }
        ScanRequest scan;
        scan.collection_id = request.collection_id;
        scan.selector = request.snapshot;
        scan.predicate = Predicate::In("key", std::move(keys));
        scan.batch_rows = 128;
        if (request.keys.empty()) {
            scan.limit = 0;
        }
        const auto budget = std::make_shared<detail::TReadBudget>(8ULL << 30,
            std::chrono::steady_clock::now() + std::chrono::seconds(60));
        auto cursorResult = ScanImpl(scan, budget);
        if (!cursorResult) {
            return std::unexpected(budget->Error().value_or(cursorResult.error()));
        }
        auto cursor = *cursorResult;
        LookupResult result;
        result.resource_lease = std::move(retained);
        result.snapshot = cursor->Info().selector_resolved;
        std::unordered_map<std::string, Row> found;
        uint64_t resultBytes = request.keys.size() * sizeof(std::optional<Row>);
        for (;;) {
            auto next = cursor->Next();
            if (!next) {
                return std::unexpected(budget->Error().value_or(next.error()));
            }
            if (!*next) {
                break;
            }
            const auto& batch = **next;
            const auto& keyArray = static_cast<const arrow::StringArray&>(*batch.column(keyColumn));
            for (int64_t row = 0; row < batch.num_rows(); ++row) {
                const auto key = keyArray.GetString(row);
                const auto requested = multiplicity.find(key);
                Require(requested != multiplicity.end(), "CORRUPTION", "lookup returned an unrequested key");
                Require(!found.contains(key), "CORRUPTION", "KV snapshot contains duplicate keys");
                const auto bytes = RowBytes(batch, row);
                Require(bytes <= (16ULL << 20) && requested->second <= ((16ULL << 20) - resultBytes) / bytes,
                    "RESOURCE_EXHAUSTED",
                    "lookup result exceeds 16 MiB; request fewer keys");
                resultBytes += bytes * requested->second;
                found.emplace(key, ReadRow(collection.schema, batch, row));
            }
        }
        result.rows.reserve(request.keys.size());
        for (const auto& key : request.keys) {
            const auto row = found.find(key);
            result.rows.push_back(row == found.end() ? std::optional<Row>{} : row->second);
        }
        return result;
    } catch (const Failure& failure) {
        CloseOnCorruption(failure.detail);
        return std::unexpected(failure.detail);
    } catch (const std::bad_alloc&) {
        return std::unexpected(
            ErrorDetail{"RESOURCE_EXHAUSTED", "lookup memory allocation failed", RetryHint::SameIdAfterReconcile});
    }
}

MigrationOutcome Engine::MigrateAppendToKV(const MigrationRequest& request)
{
    MigrationOutcome outcome;
    bool validated = false;
    try {
        Require(request.source_collection_id != request.target.collection_id &&
                request.target.mode == CollectionMode::KeyValue,
            "INVALID_ARGUMENT",
            "migration requires a distinct KV target collection");
        Require(request.duplicate_policy == DuplicatePolicy::Reject ||
                request.duplicate_policy == DuplicatePolicy::FirstWins ||
                request.duplicate_policy == DuplicatePolicy::LastWins,
            "INVALID_ARGUMENT",
            "migration requires an explicit duplicate policy");
        Require(request.source_snapshot.kind == SnapshotSelectorKind::Id ||
                request.source_snapshot.kind == SnapshotSelectorKind::InitialEmpty,
            "INVALID_ARGUMENT",
            "migration requires a fixed source snapshot");
        Require(detail::ValidUuid(request.operation_id), "INVALID_ARGUMENT", "invalid migration operation ID");
        Require(request.timeout.count() > 0 && request.timeout <= std::chrono::minutes(10),
            "INVALID_ARGUMENT",
            "migration timeout must be positive and at most ten minutes");
        Require(detail::ValidUuid(request.target.collection_id) && detail::ValidUuid(request.source_collection_id),
            "INVALID_ARGUMENT",
            "migration collection IDs must be canonical UUIDs");
        Require(!request.target.exact_name.empty() && request.target.exact_name.size() <= 128 &&
                detail::ValidUtf8(request.target.exact_name) &&
                request.target.exact_name.find('\0') == std::string::npos,
            "INVALID_ARGUMENT",
            "invalid migration target name");
        (void)Take(detail::SchemaFingerprint(request.target.schema));
        validated = true;
        detail::Encoder origin;
        origin.Text("glamdb.migration1");
        origin.Text(request.source_collection_id);
        origin.Number(static_cast<uint8_t>(request.source_snapshot.kind));
        origin.Number(request.source_snapshot.snapshot_id);
        origin.Number(static_cast<uint8_t>(request.duplicate_policy));
        origin.Text(request.operation_id);
        origin.Text(request.target.collection_id);
        origin.Text(request.target.exact_name);
        origin.Text(Take(detail::SchemaFingerprint(request.target.schema)));
        origin.Text(Take(detail::EncodeWriteSettings(request.target.schema,
            Take(detail::CanonicalWriteSettings(request.target.schema, request.target.write_settings)))));
        const auto originHash = detail::Sha256(origin.bytes);
        if (auto blocked = detail::BlockedMigration(impl_, request)) {
            return *blocked;
        }
        if (auto replay = detail::ReplayMigration(impl_, request, originHash)) {
            if (replay->creation.disposition != CommitDisposition::Committed || replay->mutation ||
                replay->resolution_error)
            {
                return *replay;
            }
            // Creation remains authoritative even when the reserved population
            // never bound and a later source read can no longer proceed.
            outcome.creation = std::move(replay->creation);
        }
        const auto source = Take(GetCollection(request.source_collection_id));
        Require(source.mode == CollectionMode::Append && source.schema == request.target.schema,
            "INVALID_ARGUMENT",
            "migration requires an append source and an identical target schema");
        const auto deadline = std::chrono::steady_clock::now() + request.timeout;
        auto budget = std::make_shared<detail::TReadBudget>(16ULL << 30, deadline, request.cancellation);
        ScanRequest scan;
        scan.collection_id = request.source_collection_id;
        scan.selector = request.source_snapshot;
        scan.batch_rows = 1000;
        // Validate source selection before the first durable target step.
        auto selected = ScanImpl(scan, budget, true);
        if (!selected) {
            throw Failure(budget->Error().value_or(selected.error()));
        }
        scan.selector = (*selected)->Info().selector_resolved;
        selected->reset();
        const auto statistics = Take(GetStatistics(request.source_collection_id, scan.selector));
        const auto count = statistics.rows.value.value_or(-1);
        Require(count >= 0 && count <= BulkAppendLimits::MaxRows,
            "RESOURCE_EXHAUSTED",
            "migration source exceeds the bounded row profile");
        outcome.creation = CreateCollectionImpl(request.target, originHash, request.operation_id);
        if (outcome.creation.disposition != CommitDisposition::Committed) {
            return outcome;
        }
        auto migrationSource = std::make_shared<TMigrationSource>(
            [this, scan, budget] {
                return ScanImpl(scan, budget, true);
            },
            budget,
            GetResourceOwner(),
            source.schema);
        detail::TKvInput input;
        input.Duplicates = request.duplicate_policy;
        input.MigrationOwner = originHash;
        input.Cancellation = request.cancellation;
        input.Timeout =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        input.Identity = request.source_collection_id + "/" +
            (scan.selector.kind == SnapshotSelectorKind::InitialEmpty ? std::string("initial")
                                                                      : std::to_string(scan.selector.snapshot_id));
        AppendRequest mutation{request.target.collection_id,
            request.operation_id,
            {SnapshotPreconditionKind::NoSnapshot, 0},
            {}};
        if (count == 0) {
            outcome.mutation = AppendDurableImpl(mutation, nullptr, &input);
        } else {
            BulkAppendRequest bulk{request.target.collection_id,
                request.operation_id,
                mutation.expected_snapshot,
                count,
                std::move(migrationSource),
                request.cancellation,
                input.Timeout};
            outcome.mutation = AppendDurableImpl(mutation, &bulk, &input);
        }
        return outcome;
    } catch (const Failure& failure) {
        if (validated && (failure.detail.code == "RECOVERY_REQUIRED" || failure.detail.code == "CANCELLED")) {
            if (auto blocked = detail::BlockedMigration(impl_, request)) {
                return *blocked;
            }
        }
        if (outcome.creation.disposition == CommitDisposition::Committed) {
            AppendOutcome failed;
            failed.operation_id = request.operation_id;
            failed.error = failure.detail;
            outcome.mutation = std::move(failed);
        } else {
            outcome.creation.error = failure.detail;
        }
        return outcome;
    } catch (const std::bad_alloc&) {
        const ErrorDetail error{"RESOURCE_EXHAUSTED",
            "migration memory allocation failed",
            RetryHint::SameIdAfterReconcile};
        if (outcome.creation.disposition == CommitDisposition::Committed) {
            AppendOutcome failed;
            failed.operation_id = request.operation_id;
            failed.error = error;
            outcome.mutation = std::move(failed);
        } else {
            outcome.creation.error = error;
        }
        return outcome;
    }
}

} // namespace glamdb
