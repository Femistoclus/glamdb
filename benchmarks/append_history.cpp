// Per-append publication cost versus data files already in a collection.
//
// glamdb_append_history_bench ROOT retain-all|keep-last CHECKPOINT...
//
// Appends one-row batches to one collection and times each append and its
// publication guard (StagedAppend::VerifyClosure: closure verification plus
// candidate accounting). At every checkpoint F it prints one JSON line with the
// medians/p90 of the last `window` appends before F. `keep-last` expires all
// but the newest 100 snapshots every 100 appends, so the root stays small and
// costs proportional to data files dominate the trend.

#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/engine/storage_backend.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>

namespace {

using namespace glamdb;
using namespace glamdb::detail;
using TJson = nlohmann::json;
using TClock = std::chrono::steady_clock;

constexpr auto StoreID = "64000000-0000-4000-8000-000000000001";
constexpr auto CollectionID = "64000000-0000-4000-8000-000000000002";
constexpr size_t Window = 200;
constexpr int64_t KeepLast = 100;

void Require(bool condition, const std::string& message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename T>
T Take(Result<T> result)
{
    Require(result.has_value(), result ? "" : result.error().code + ": " + result.error().message);
    return std::move(*result);
}

double ElapsedUs(TClock::time_point start)
{
    return std::chrono::duration<double, std::micro>(TClock::now() - start).count();
}

uint64_t PeakRSS()
{
    rusage usage{};
    Require(::getrusage(RUSAGE_SELF, &usage) == 0, "getrusage failed");
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
}

struct TGuardTimes
{
    std::vector<double> Us;
};

class TTimedAppend final : public StagedAppend
{
public:
    TTimedAppend(std::unique_ptr<StagedAppend> inner, std::shared_ptr<TGuardTimes> times)
        : Inner_(std::move(inner))
        , Times_(std::move(times))
    { }

    AppendResult Receipt() const override
    {
        return Inner_->Receipt();
    }

    iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
    {
        return Inner_->Publish(catalog);
    }

    Status VerifyClosure(std::string_view location) override
    {
        const auto started = TClock::now();
        auto status = Inner_->VerifyClosure(location);
        Times_->Us.push_back(ElapsedUs(started));
        return status;
    }

private:
    const std::unique_ptr<StagedAppend> Inner_;
    const std::shared_ptr<TGuardTimes> Times_;
};

// Forwards every call to the native backend and times append publication guards.
class TTimedBackend final : public StorageBackend
{
public:
    TTimedBackend(std::shared_ptr<StorageBackend> inner, std::shared_ptr<TGuardTimes> times)
        : Inner_(std::move(inner))
        , Times_(std::move(times))
    { }

    Result<StorageFootprint> GetStorageFootprint() override
    {
        return Inner_->GetStorageFootprint();
    }
    EngineCapabilities GetCapabilities() const override
    {
        return Inner_->GetCapabilities();
    }
    Health GetHealth() const override
    {
        return Inner_->GetHealth();
    }
    Result<std::unique_ptr<StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override
    {
        return Inner_->PrepareCreate(store, request, attempt);
    }
    Status CheckAppendAdmissible(const StoreInfo& store,
        const CollectionInfo& collection,
        const BaseIdentity& base,
        NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline,
        const std::function<bool()>& cancelled) override
    {
        return Inner_->CheckAppendAdmissible(store, collection, base, input, deadline, cancelled);
    }
    Result<std::unique_ptr<StagedAppend>> PrepareAppend(const StoreInfo& store,
        const BoundOperation& binding,
        const CollectionInfo& collection,
        const NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) override
    {
        auto staged = Inner_->PrepareAppend(store, binding, collection, input, deadline);
        if (!staged) {
            return std::unexpected(staged.error());
        }
        return std::unique_ptr<StagedAppend>(std::make_unique<TTimedAppend>(std::move(*staged), Times_));
    }
    Result<std::unique_ptr<StagedExpiration>> PrepareExpiration(const StoreInfo& store,
        const CollectionInfo& collection,
        const BaseIdentity& base,
        const std::optional<SnapshotRetentionPolicy>& policy) override
    {
        return Inner_->PrepareExpiration(store, collection, base, policy);
    }
    Status RemoveExpiredMetadata(std::string_view collection, std::string_view path) override
    {
        return Inner_->RemoveExpiredMetadata(collection, path);
    }
    void PublicationFinished(std::string_view collection, std::string_view location, CommitDisposition result) override
    {
        Inner_->PublicationFinished(collection, location, result);
    }
    void InvalidateCache() override
    {
        Inner_->InvalidateCache();
    }
    void RetireRoot(const BaseIdentity& base) override
    {
        Inner_->RetireRoot(base);
    }
    Status VerifyRetained(const StoreInfo& store,
        std::span<const CollectionView> collections,
        std::span<const OperationRecord> operations) override
    {
        return Inner_->VerifyRetained(store, collections, operations);
    }
    Status VerifyRetainedCatalog(const StoreInfo& store,
        std::span<const CollectionView> collections,
        DurableSqliteCatalogStore& catalog) override
    {
        return Inner_->VerifyRetainedCatalog(store, collections, catalog);
    }
    Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO() override
    {
        return Inner_->ReadOnlyFileIO();
    }
    Result<std::shared_ptr<const NGlamDB::NStorage::TBaseState>> VerifiedScanState(const BaseIdentity& base) override
    {
        return Inner_->VerifiedScanState(base);
    }
    std::shared_ptr<TSnapshotMetadataCache> ReadMetadataCache() override
    {
        return Inner_->ReadMetadataCache();
    }

private:
    const std::shared_ptr<StorageBackend> Inner_;
    const std::shared_ptr<TGuardTimes> Times_;
};

std::shared_ptr<arrow::RecordBatch> Batch(int64_t value)
{
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    Require(keys.Append("key").ok() && values.Append(value).ok(), "build batch failed");
    auto keyArray = keys.Finish();
    auto valueArray = values.Finish();
    Require(keyArray.ok() && valueArray.ok(), "finish batch failed");
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("value", arrow::int64())}),
        1,
        {*keyArray, *valueArray});
}

std::string OperationID(size_t index)
{
    char operation[37];
    std::snprintf(operation, sizeof(operation), "64000000-0000-4000-8001-%012zx", index + 1);
    return operation;
}

Limits BenchLimits()
{
    Limits limits;
    limits.max_operations = 40000;
    limits.max_catalog_bytes = 1LL << 30;
    limits.history.max_snapshots = 20000;
    limits.history.max_manifests = 20000;
    limits.history.max_data_files = 20000;
    limits.history.max_root_bytes = 256LL << 20;
    limits.history.max_root_file_bytes = 64LL << 20;
    limits.history.max_manifest_bytes = 8LL << 30;
    limits.history.max_manifest_list_bytes = 8LL << 30;
    limits.history.max_footer_bytes = 1LL << 30;
    limits.history.max_commit_bytes = 128LL << 20;
    limits.history.max_recovery_workspace_bytes = 1LL << 30;
    return limits;
}

double Percentile(std::vector<double> values, size_t percent)
{
    if (values.empty()) {
        return 0;
    }
    std::ranges::sort(values);
    return values[std::min(values.size() - 1, values.size() * percent / 100)];
}

std::vector<size_t> Checkpoints(int argc, char** argv)
{
    std::vector<size_t> result;
    for (int index = 3; index < argc; ++index) {
        const std::string_view text(argv[index]);
        size_t value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        Require(error == std::errc{} && end == text.data() + text.size() && value >= Window &&
                (result.empty() || value > result.back()) && value <= 19000,
            "checkpoints must increase within [window, 19000]");
        result.push_back(value);
    }
    Require(!result.empty(), "missing checkpoints");
    return result;
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc >= 4, "usage: glamdb_append_history_bench ROOT retain-all|keep-last CHECKPOINT...");
    const std::filesystem::path root = std::filesystem::absolute(argv[1]);
    const std::string mode(argv[2]);
    Require(mode == "retain-all" || mode == "keep-last", "unknown mode");
    Require(!std::filesystem::exists(root), "root must not exist");
    const auto checkpoints = Checkpoints(argc, argv);
    const auto limits = BenchLimits();
    auto owner = std::make_shared<ResourceOwner>();
    auto times = std::make_shared<TGuardTimes>();
    const BackendFactory factory = [&](const auto& path, const auto& info) -> Result<std::shared_ptr<StorageBackend>> {
        TLocalStorageOptions options;
        options.Resources = owner;
        options.ApplyHistoryLimits(limits.history);
        auto backend = TLocalStorageBackend::Open(path, info, std::move(options));
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return std::shared_ptr<StorageBackend>(std::make_shared<TTimedBackend>(std::move(*backend), times));
    };
    auto engine =
        Take(EngineFactory::CreateWith({StoreID, root.string(), "local-delete-extra-v1", limits, owner}, factory));
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::Int64, true}}};
    Require(engine->CreateCollection({CollectionID, CollectionID, schema}).disposition == CommitDisposition::Committed,
        "create collection failed");
    std::vector<double> appendUs;
    double expireUs = 0;
    size_t expirations = 0;
    const auto started = TClock::now();
    size_t next = 0;
    for (const auto checkpoint : checkpoints) {
        for (; next < checkpoint; ++next) {
            const auto batch = Batch(static_cast<int64_t>(next));
            const auto appendStarted = TClock::now();
            const auto outcome = engine->AppendDurable({CollectionID, OperationID(next), {}, {batch}});
            appendUs.push_back(ElapsedUs(appendStarted));
            Require(outcome.disposition == CommitDisposition::Committed,
                "append " + std::to_string(next) + " failed: " + (outcome.error ? outcome.error->message : ""));
            Require(times->Us.size() == next + 1, "missing guard timing");
            if (mode == "keep-last" && (next + 1) % 100 == 0) {
                const auto expireStarted = TClock::now();
                Take(engine->ExpireSnapshots({CollectionID, SnapshotRetentionPolicy{.keep_last = KeepLast}}));
                expireUs += ElapsedUs(expireStarted);
                ++expirations;
            }
        }
        const auto window = [&](const std::vector<double>& values) {
            return std::vector<double>(values.end() - static_cast<std::ptrdiff_t>(Window), values.end());
        };
        const auto guard = window(times->Us);
        const auto append = window(appendUs);
        const auto history = Take(engine->ListSnapshots(CollectionID));
        const auto usage = owner->GetUsage();
        std::cout << TJson{{"mode", mode},
                         {"data_files", checkpoint},
                         {"retained_snapshots", history.snapshots.size()},
                         {"window", Window},
                         {"guard_us_p50", Percentile(guard, 50)},
                         {"guard_us_p90", Percentile(guard, 90)},
                         {"append_us_p50", Percentile(append, 50)},
                         {"append_us_p90", Percentile(append, 90)},
                         {"expirations", expirations},
                         {"expire_us_total", expireUs},
                         {"elapsed_s", ElapsedUs(started) / 1e6},
                         {"rss_peak_bytes", PeakRSS()},
                         {"resource_memory_bytes", usage.memory_bytes},
                         {"resource_memory_peak_bytes", usage.memory_peak}}
                         .dump()
                  << std::endl;
    }
    Require(engine->Shutdown(std::chrono::seconds(30)).has_value(), "shutdown failed");
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
