// Append latency, root size and fresh-process open time of one collection
// with the snapshot archive tier, at growing history.
//
// glamdb_archive_history_bench append ROOT HOT TARGET
//   Creates the store (or opens it) and appends one-row batches until TARGET
//   committed appends. Prints one JSON line: p50/p95 of the last 200 appends,
//   root bytes/snapshots and archived snapshots.
// glamdb_archive_history_bench open ROOT [full]
//   Opens the existing store in this (fresh) process and prints the open time.
//   `full` requests the full verification path instead of the checkpoint.

#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/storage/base_state.h"
#include "storage/engine.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>

namespace {

using namespace glamdb;
using TJson = nlohmann::json;
using TClock = std::chrono::steady_clock;

constexpr auto StoreID = "65000000-0000-4000-8000-000000000001";
constexpr auto CollectionID = "65000000-0000-4000-8000-000000000002";
constexpr size_t Window = 200;
constexpr int64_t MaxAppends = 60000;

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

double ElapsedMs(TClock::time_point start)
{
    return std::chrono::duration<double, std::milli>(TClock::now() - start).count();
}

uint64_t PeakRSS()
{
    rusage usage{};
    Require(::getrusage(RUSAGE_SELF, &usage) == 0, "getrusage failed");
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
}

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

std::string OperationID(int64_t index)
{
    char operation[37];
    std::snprintf(operation, sizeof(operation), "65000000-0000-4000-8001-%012llx", static_cast<long long>(index + 1));
    return operation;
}

// Every archived snapshot keeps its list and the manifests it referenced last
// on disk and in the budgets; one data file per append.
Limits BenchLimits()
{
    Limits limits;
    limits.max_operations = MaxAppends + 16;
    limits.max_catalog_bytes = 512LL << 20;
    limits.history.max_snapshots = 4096;
    limits.history.max_manifests = 4 * MaxAppends;
    limits.history.max_data_files = MaxAppends + 16;
    limits.history.max_root_bytes = 64LL << 20;
    limits.history.max_manifest_bytes = 64LL << 30;
    limits.history.max_manifest_list_bytes = 64LL << 30;
    limits.history.max_footer_bytes = 4LL << 30;
    limits.history.max_commit_bytes = 128LL << 20;
    // Recovery admits Snapshots + Manifests + DataFiles <= workspace / 8 KiB;
    // with about one retained manifest per append 512 MiB ends near 32,500.
    limits.history.max_recovery_workspace_bytes = 1LL << 30;
    return limits;
}

// Recovery reserves an envelope per possible committed append; raise the
// reservation classes above the process default for 60,000 appends.
std::shared_ptr<ResourceOwner> BenchResources()
{
    ResourceLimits limits;
    limits.memory_bytes = {2ULL << 30, 16ULL << 30, 16ULL << 20, 64ULL << 20, 16ULL << 30};
    return std::make_shared<ResourceOwner>(limits);
}

int64_t ParseCount(std::string_view text)
{
    int64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size() && value > 0 && value <= MaxAppends,
        "invalid count");
    return value;
}

double Percentile(std::vector<double> values, size_t percent)
{
    if (values.empty()) {
        return 0;
    }
    std::ranges::sort(values);
    return values[std::min(values.size() - 1, values.size() * percent / 100)];
}

std::filesystem::path CurrentRoot(const std::filesystem::path& root)
{
    // Root file names start with a zero-padded version; after a clean shutdown
    // superseded roots are removed, and the highest version is current.
    std::filesystem::path newest;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse" / CollectionID)) {
        if (entry.is_regular_file() && entry.path().string().ends_with(".metadata.json") &&
            (newest.empty() || entry.path().filename() > newest.filename()))
        {
            newest = entry.path();
        }
    }
    return newest;
}

// The production backend, as OpenExisting creates it, kept for inspection.
std::shared_ptr<detail::TLocalStorageBackend> Backend;

std::shared_ptr<Engine> OpenStore(const std::filesystem::path& root, bool full, double& ms)
{
    OpenStoreRequest request{root.string(), StoreID, "local-delete-extra-v1", BenchLimits(), BenchResources()};
    request.verify_full = full;
    const auto started = TClock::now();
    auto engine = Take(detail::EngineFactory::OpenWith(request,
        [&](const std::filesystem::path& path,
            const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Resources = request.resources;
            options.ApplyHistoryLimits(request.limits.history);
            auto backend = detail::TLocalStorageBackend::Open(path, info, std::move(options));
            if (!backend) {
                return std::unexpected(backend.error());
            }
            Backend = *backend;
            return std::shared_ptr<detail::StorageBackend>(Backend);
        }));
    ms = ElapsedMs(started);
    return engine;
}

// Current manifest count and the largest decoded manifest, from the verified
// generation the open installed.
std::pair<size_t, uint64_t> CurrentManifests(Engine& engine)
{
    const auto cursor = Take(engine.Scan({.collection_id = CollectionID, .limit = 0}));
    const auto state = Take(Backend->VerifiedScanState(cursor->Info().identity));
    uint64_t largest = 0;
    for (const auto bytes : state->CurrentManifestDecodedBytes) {
        largest = std::max(largest, bytes);
    }
    return {state->CurrentManifests.size(), largest};
}

int Append(const std::filesystem::path& root, int64_t hot, int64_t target)
{
    std::shared_ptr<Engine> engine;
    double openMs = 0;
    if (std::filesystem::exists(root)) {
        engine = OpenStore(root, false, openMs);
    } else {
        engine = Take(CreateStore({StoreID, root.string(), "local-delete-extra-v1", BenchLimits(), BenchResources()}));
        CreateCollectionRequest request{CollectionID,
            "events",
            {{{"key", FieldType::String, false}, {"value", FieldType::Int64, true}}}};
        request.hot_snapshots = hot;
        Require(engine->CreateCollection(request).disposition == CommitDisposition::Committed,
            "create collection failed");
    }
    auto status = Take(engine->GetSnapshotArchive(CollectionID));
    int64_t next = status.root_snapshots + status.archived_snapshots + status.expired_snapshots;
    std::vector<double> appendMs;
    const auto started = TClock::now();
    for (; next < target; ++next) {
        const auto batch = Batch(next);
        const auto appendStarted = TClock::now();
        const auto outcome = engine->AppendDurable({CollectionID, OperationID(next), {}, {batch}});
        appendMs.push_back(ElapsedMs(appendStarted));
        Require(outcome.disposition == CommitDisposition::Committed,
            "append " + std::to_string(next) + " failed: " + (outcome.error ? outcome.error->message : ""));
    }
    const auto window =
        std::vector<double>(appendMs.end() - static_cast<std::ptrdiff_t>(std::min(Window, appendMs.size())),
            appendMs.end());
    status = Take(engine->GetSnapshotArchive(CollectionID));
    Require(engine->Shutdown(std::chrono::seconds(60)).has_value(), "shutdown failed");
    engine.reset();
    const auto rootFile = CurrentRoot(root);
    std::cout << TJson{{"command", "append"},
                     {"hot_snapshots", hot},
                     {"appends", target},
                     {"window", window.size()},
                     {"append_ms_p50", Percentile(window, 50)},
                     {"append_ms_p95", Percentile(window, 95)},
                     {"append_ms_max", window.empty() ? 0 : *std::ranges::max_element(window)},
                     {"appended_now", appendMs.size()},
                     {"elapsed_s", ElapsedMs(started) / 1000},
                     {"reopen_before_ms", openMs},
                     {"root_bytes", rootFile.empty() ? 0 : std::filesystem::file_size(rootFile)},
                     {"root_snapshots", status.root_snapshots},
                     {"archived_snapshots", status.archived_snapshots},
                     {"catalog_bytes", std::filesystem::file_size(root / "catalog.sqlite")},
                     {"rss_peak_bytes", PeakRSS()}}
                     .dump()
              << '\n';
    return 0;
}

int Open(const std::filesystem::path& root, bool full)
{
    double ms = 0;
    auto engine = OpenStore(root, full, ms);
    const auto status = Take(engine->GetSnapshotArchive(CollectionID));
    const auto [manifests, largestDecoded] = CurrentManifests(*engine);
    Require(engine->Shutdown(std::chrono::seconds(60)).has_value(), "shutdown failed");
    engine.reset();
    Backend.reset();
    std::cout << TJson{{"command", "open"},
                     {"full", full},
                     {"open_ms", ms},
                     {"current_manifests", manifests},
                     {"largest_manifest_decoded_bytes", largestDecoded},
                     {"root_snapshots", status.root_snapshots},
                     {"archived_snapshots", status.archived_snapshots},
                     {"rss_peak_bytes", PeakRSS()}}
                     .dump()
              << '\n';
    return 0;
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc >= 3, "usage: glamdb_archive_history_bench append ROOT HOT TARGET | open ROOT [full]");
    const std::string command(argv[1]);
    const std::filesystem::path root = std::filesystem::absolute(argv[2]);
    if (command == "append") {
        Require(argc == 5, "usage: append ROOT HOT TARGET");
        return Append(root, ParseCount(argv[3]), ParseCount(argv[4]));
    }
    Require(command == "open" && (argc == 3 || (argc == 4 && std::string_view(argv[3]) == "full")),
        "usage: open ROOT [full]");
    return Open(root, argc == 4);
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
