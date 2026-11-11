// Manual benchmark: per-stage cost of native synchronous appends of prebuilt
// Arrow batches. Skipped unless GLAMDB_APPEND_STAGE_BENCH is set to the number
// of measured appends per shape. GLAMDB_APPEND_STAGE_SHAPES selects shapes as a
// comma-separated list of <fields>x<rows> (default: 2, 16 and 64 fields with
// 1000 and 10000 rows); GLAMDB_APPEND_STAGE_DIR names an existing parent
// directory for the disposable stores (default: a temporary directory).
//
// Input-side stages (inspection, fingerprint, normalization, writer batch) are
// timed by calling the same functions on the same batches outside an append.
// Backend stages are timed inside real appends by a delegating backend; the
// remainder of the append wall time is coordinator/catalog work.

#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"
#include "tests/vertical/vertical_support.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <fstream>
#include <string>
#include <vector>

#include <unistd.h>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;
using Clock = std::chrono::steady_clock;

std::size_t EnvCount(const char* name, std::size_t fallback)
{
    const char* value = std::getenv(name);
    return value && *value ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

double Ms(Clock::duration value)
{
    return std::chrono::duration<double, std::milli>(value).count();
}

double Median(std::vector<double> values)
{
    if (values.empty()) {
        return 0;
    }
    std::ranges::sort(values);
    return values[values.size() / 2];
}

template <class TFn>
double Time(TFn&& fn)
{
    const auto start = Clock::now();
    fn();
    return Ms(Clock::now() - start);
}

long PeakRssKb()
{
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.starts_with("VmHWM:")) {
            return std::strtol(line.c_str() + 6, nullptr, 10);
        }
    }
    return -1;
}

// key (required string), then nullable fields cycling over the other types;
// every tenth value of a nullable field is NULL.
SchemaSpec MakeSchema(std::size_t fields)
{
    static constexpr FieldType Cycle[] = {
        FieldType::Int64, FieldType::Float64, FieldType::String, FieldType::Bool, FieldType::TimestampUsUtc};
    SchemaSpec schema{{{"key", FieldType::String, false}}};
    for (std::size_t field = 1; field < fields; ++field) {
        schema.fields.push_back({std::format("f{}", field), Cycle[(field - 1) % std::size(Cycle)], true});
    }
    return schema;
}

std::shared_ptr<arrow::RecordBatch> MakeBatch(const SchemaSpec& schema, std::size_t rows, std::size_t seed)
{
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (std::size_t field = 0; field < schema.fields.size(); ++field) {
        const auto type = detail::MakeArrowType(schema.fields[field].type);
        std::unique_ptr<arrow::ArrayBuilder> builder;
        EXPECT_TRUE(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder).ok());
        for (std::size_t row = 0; row < rows; ++row) {
            const auto value = seed * 1000003 + row * 131 + field;
            if (schema.fields[field].nullable && (row + field) % 10 == 0) {
                EXPECT_TRUE(builder->AppendNull().ok());
                continue;
            }
            arrow::Status status;
            switch (schema.fields[field].type) {
                case FieldType::String:
                    status = static_cast<arrow::StringBuilder&>(*builder).Append(
                        field == 0 ? std::format("key-{:08}", row) : std::format("value-{}", value % 100000));
                    break;
                case FieldType::Bool:
                    status = static_cast<arrow::BooleanBuilder&>(*builder).Append(value % 3 == 0);
                    break;
                case FieldType::Int64:
                    status = static_cast<arrow::Int64Builder&>(*builder).Append(static_cast<int64_t>(value) - 500);
                    break;
                case FieldType::Float64:
                    status = static_cast<arrow::DoubleBuilder&>(*builder).Append(static_cast<double>(value) * 0.25);
                    break;
                case FieldType::TimestampUsUtc:
                    status = static_cast<arrow::TimestampBuilder&>(*builder).Append(
                        1700000000000000LL + static_cast<int64_t>(value));
                    break;
            }
            EXPECT_TRUE(status.ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder->Finish(&array).ok());
        columns.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(detail::MakeArrowSchema(schema), static_cast<int64_t>(rows), std::move(columns));
}

struct TStageTimes
{
    std::vector<double> Admissible;
    std::vector<double> Prepare;
    std::vector<double> Publish;
    std::vector<double> Verify;
};

// Delegates to the real backend and times its append stages on the writer thread.
class TTimingBackend final : public detail::StorageBackend
{
public:
    explicit TTimingBackend(std::shared_ptr<detail::StorageBackend> inner)
        : Inner_(std::move(inner))
    { }

    static detail::BackendFactory Factory(std::shared_ptr<TTimingBackend>& slot)
    {
        return [&slot](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            auto inner = detail::TLocalStorageBackend::Open(path, info, {});
            if (!inner) {
                return std::unexpected(inner.error());
            }
            slot = std::make_shared<TTimingBackend>(std::move(*inner));
            return slot;
        };
    }

    TStageTimes Times;

    Result<std::unique_ptr<detail::StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override
    {
        return Inner_->PrepareCreate(store, request, attempt);
    }

    Status CheckAppendAdmissible(const StoreInfo& store,
        const CollectionInfo& collection,
        const BaseIdentity& base,
        detail::NormalizedInput& input) override
    {
        const auto start = Clock::now();
        auto result = Inner_->CheckAppendAdmissible(store, collection, base, input);
        Times.Admissible.push_back(Ms(Clock::now() - start));
        return result;
    }

    Result<std::unique_ptr<detail::StagedAppend>> PrepareAppend(const StoreInfo& store,
        const detail::BoundOperation& binding,
        const CollectionInfo& collection,
        const detail::NormalizedInput& input,
        Clock::time_point deadline) override
    {
        const auto start = Clock::now();
        auto staged = Inner_->PrepareAppend(store, binding, collection, input, deadline);
        Times.Prepare.push_back(Ms(Clock::now() - start));
        if (!staged) {
            return staged;
        }
        return std::make_unique<TStaged>(this, std::move(*staged));
    }

    void
    PublicationFinished(std::string_view collectionID, std::string_view location, CommitDisposition result) override
    {
        Inner_->PublicationFinished(collectionID, location, result);
    }

    void InvalidateCache() override
    {
        Inner_->InvalidateCache();
    }

    void RetireRoot(const BaseIdentity& superseded) override
    {
        Inner_->RetireRoot(superseded);
    }

    Status VerifyRetained(const StoreInfo& store,
        std::span<const detail::CollectionView> collections,
        std::span<const detail::OperationRecord> operations) override
    {
        return Inner_->VerifyRetained(store, collections, operations);
    }

    Health GetHealth() const override
    {
        return Inner_->GetHealth();
    }

    Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO() override
    {
        return Inner_->ReadOnlyFileIO();
    }

private:
    class TStaged final : public detail::StagedAppend
    {
    public:
        TStaged(TTimingBackend* owner, std::unique_ptr<detail::StagedAppend> inner)
            : Owner_(owner)
            , Inner_(std::move(inner))
        { }

        AppendResult Receipt() const override
        {
            return Inner_->Receipt();
        }

        iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
        {
            const auto start = Clock::now();
            auto published = Inner_->Publish(catalog);
            Owner_->Times.Publish.push_back(Ms(Clock::now() - start));
            return published;
        }

        Status VerifyClosure(std::string_view location) override
        {
            const auto start = Clock::now();
            auto verified = Inner_->VerifyClosure(location);
            Owner_->Times.Verify.push_back(Ms(Clock::now() - start));
            return verified;
        }

    private:
        TTimingBackend* const Owner_;
        const std::unique_ptr<detail::StagedAppend> Inner_;
    };

    std::shared_ptr<detail::StorageBackend> Inner_;
};

struct TShape
{
    std::size_t Fields = 0;
    std::size_t Rows = 0;
};

std::vector<TShape> Shapes()
{
    const char* value = std::getenv("GLAMDB_APPEND_STAGE_SHAPES");
    std::string text = value && *value ? value : "2x1000,16x1000,64x1000,2x10000,16x10000,64x10000";
    std::vector<TShape> shapes;
    std::size_t position = 0;
    while (position < text.size()) {
        auto end = text.find(',', position);
        end = end == std::string::npos ? text.size() : end;
        const auto item = text.substr(position, end - position);
        const auto x = item.find('x');
        shapes.push_back({std::stoul(item.substr(0, x)), std::stoul(item.substr(x + 1))});
        position = end + 1;
    }
    return shapes;
}

TEST(AppendStageBench, NativeAppendStagesByShape)
{
    const auto appends = EnvCount("GLAMDB_APPEND_STAGE_BENCH", 0);
    if (appends == 0) {
        GTEST_SKIP() << "Set GLAMDB_APPEND_STAGE_BENCH=<appends per shape> to run this benchmark.";
    }
    test::TempDir temporary;
    const char* parent = std::getenv("GLAMDB_APPEND_STAGE_DIR");
    const auto parentPath = parent && *parent ? std::filesystem::path(parent) : temporary.path;
    std::printf("fields,rows,decoded_mib,inspect_ms,fingerprint_ms,normalize_ms,owned_rows_ms,extract_ms,"
                "make_batch_ms,append_ms,admissible_ms,prepare_ms,publish_ms,verify_ms,other_ms,input_share_pct,"
                "rows_per_s,peak_rss_kb\n");
    for (const auto& shape : Shapes()) {
        const auto schema = MakeSchema(shape.Fields);
        // Distinct content per append, so every append binds a new operation.
        std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> inputs;
        for (std::size_t index = 0; index < appends + 2; ++index) {
            inputs.push_back({MakeBatch(schema, shape.Rows, index)});
        }

        // Input-side stages on the measured inputs, outside the engine.
        std::vector<double> inspect, fingerprint, normalize, owned, makeBatch;
        std::size_t decoded = 0;
        for (std::size_t index = 2; index < inputs.size(); ++index) {
            const auto& batches = inputs[index];
            inspect.push_back(Time([&] {
                ASSERT_TRUE(detail::InputReservation(schema, batches));
            }));
            fingerprint.push_back(Time([&] {
                ASSERT_TRUE(detail::FingerprintBatches(schema, test::collection_id, batches));
            }));
            Result<detail::NormalizedInput> normalized;
            normalize.push_back(Time([&] {
                normalized = detail::NormalizeBatches(schema, test::collection_id, batches);
            }));
            ASSERT_TRUE(normalized) << normalized.error().message;
            decoded = normalized->decoded_bytes;
            auto copy = normalized->rows;
            owned.push_back(Time([&] {
                ASSERT_TRUE(detail::NormalizeOwnedRows(schema, test::collection_id, std::move(copy)));
            }));
            makeBatch.push_back(Time([&] {
                ASSERT_TRUE(detail::MakeRecordBatch(schema, normalized->rows));
            }));
        }

        const auto root = parentPath / std::format("append-stage-{}-{}x{}", ::getpid(), shape.Fields, shape.Rows);
        std::filesystem::remove_all(root);
        std::shared_ptr<TTimingBackend> backend;
        auto created = detail::EngineFactory::CreateWith({test::store_id, root.string()}, TTimingBackend::Factory(backend));
        ASSERT_TRUE(created) << created.error().message;
        auto engine = std::move(*created);
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", schema}).disposition,
            CommitDisposition::Committed);
        std::vector<double> wall;
        for (std::size_t index = 0; index < inputs.size(); ++index) {
            const auto operation = std::format("00000000-0000-4000-8000-{:012x}", index + 1);
            const auto start = Clock::now();
            const auto outcome = engine->AppendDurable({test::collection_id, operation, {}, inputs[index]});
            const auto elapsed = Ms(Clock::now() - start);
            ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
                << "append " << index << ": " << (outcome.error ? outcome.error->message : "");
            if (index >= 2) {
                wall.push_back(elapsed);
            }
        }
        const auto measured = [&](std::vector<double>& values) {
            values.erase(values.begin(), values.begin() + 2);
            return Median(values);
        };
        const auto admissible = measured(backend->Times.Admissible);
        const auto prepare = measured(backend->Times.Prepare);
        const auto publish = measured(backend->Times.Publish);
        const auto verify = measured(backend->Times.Verify);
        const auto append = Median(wall);
        const auto inputSide = Median(fingerprint) + Median(inspect) + Median(normalize);
        // Admission builds the writer batch; the input side runs before it.
        // The real backend verifies closure inside Publish, so publish includes verify.
        const auto other = append - inputSide - admissible - prepare - publish;
        std::printf("%zu,%zu,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f,%.0f,%ld\n",
            shape.Fields,
            shape.Rows,
            static_cast<double>(decoded) / (1024.0 * 1024.0),
            Median(inspect),
            Median(fingerprint),
            Median(normalize),
            Median(owned),
            Median(normalize) - Median(inspect) - Median(owned),
            Median(makeBatch),
            append,
            admissible,
            prepare,
            publish,
            verify,
            other,
            100.0 * (inputSide + Median(makeBatch)) / append,
            1000.0 * static_cast<double>(shape.Rows) / append,
            PeakRssKb());
        std::fflush(stdout);
        ASSERT_TRUE(engine->Shutdown(5000ms));
        engine.reset();
        std::filesystem::remove_all(root);
    }
}

} // namespace
} // namespace glamdb
