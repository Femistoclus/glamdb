#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/io/tree_inventory.h"
#include "core/storage/base_state.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/stat.h>

namespace {

using namespace glamdb;
using namespace NGlamDB::NIO;
using TJson = nlohmann::json;
using TClock = std::chrono::steady_clock;

constexpr auto StoreID = "63000000-0000-4000-8000-000000000001";
constexpr std::array CollectionIDs{"63000000-0000-4000-8000-000000000002", "63000000-0000-4000-8000-000000000003"};

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

TJson ReadJson(const std::filesystem::path& path)
{
    std::ifstream input(path);
    Require(input.good(), "cannot read " + path.string());
    return TJson::parse(input);
}

struct TInventoryCounters
{
    std::atomic<uint64_t> Calls{0}, Nanoseconds{0};
};

class TCountingInventory final : public ITreeInventory
{
public:
    TCountingInventory(std::shared_ptr<ITreeInventory> inner, std::shared_ptr<TInventoryCounters> counters)
        : Inner_(std::move(inner))
        , Counters_(std::move(counters))
    { }

    TResult<std::uint64_t> Measure(const std::function<TResult<std::uint64_t>()>& fullMeasure,
        std::uint64_t limit,
        TClock::time_point deadline,
        const std::function<bool()>& cancelled) override
    {
        const auto started = TClock::now();
        auto result = Inner_->Measure(fullMeasure, limit, deadline, cancelled);
        ++Counters_->Calls;
        Counters_->Nanoseconds += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(TClock::now() - started).count());
        return result;
    }

    void Invalidate() override
    {
        Inner_->Invalidate();
    }

private:
    const std::shared_ptr<ITreeInventory> Inner_;
    const std::shared_ptr<TInventoryCounters> Counters_;
};

// Forward real POSIX handles without retaining per-call or per-file records.
class TCountingFS final : public ILocalFS
{
public:
    TResult<THandle> OpenDir(const std::filesystem::path& path) override
    {
        return Posix_.OpenDir(path);
    }
    TResult<THandle> OpenDir(const THandle& parent, std::string_view name) override
    {
        return Posix_.OpenDir(parent, name);
    }
    std::error_code MakeDir(const THandle& parent, std::string_view name) override
    {
        return Posix_.MakeDir(parent, name);
    }
    TResult<THandle> CreateExclusive(const THandle& parent, std::string_view name, const TIOTag& tag) override
    {
        return Posix_.CreateExclusive(parent, name, tag);
    }
    TResult<THandle> OpenRead(const THandle& parent, std::string_view name) override
    {
        ++Opens_;
        return Posix_.OpenRead(parent, name);
    }
    TResult<std::size_t> Write(const THandle& file, std::span<const std::byte> bytes) override
    {
        auto result = Posix_.Write(file, bytes);
        if (result) {
            Written_ += result.Value();
        }
        return result;
    }
    TResult<std::size_t> PRead(const THandle& file, std::uint64_t offset, std::span<std::byte> bytes) override
    {
        auto result = Posix_.PRead(file, offset, bytes);
        if (result) {
            Read_ += result.Value();
        }
        return result;
    }
    TResult<TFileStat> Stat(const THandle& handle) override
    {
        return Posix_.Stat(handle);
    }
    TResult<TSpaceInfo> StatFs(const THandle& handle) override
    {
        return Posix_.StatFs(handle);
    }
    TResult<std::uint64_t> MeasureTree(const THandle& handle, std::uint64_t limit) override
    {
        return MeasureTree(handle, limit, TClock::time_point::max());
    }
    TResult<std::uint64_t> MeasureTree(const THandle& handle,
        std::uint64_t limit,
        TClock::time_point deadline,
        const std::function<bool()>& cancelled = {}) override
    {
        const auto started = TClock::now();
        auto result = Posix_.MeasureTree(handle, limit, deadline, cancelled);
        ++TreeCalls_;
        TreeNanoseconds_ += static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(TClock::now() - started).count());
        return result;
    }
    TResult<std::shared_ptr<ITreeInventory>> TrackTree(const THandle& handle) override
    {
        auto inventory = Posix_.TrackTree(handle);
        if (!inventory) {
            return inventory.Error();
        }
        return std::shared_ptr<ITreeInventory>(std::make_shared<TCountingInventory>(inventory.Value(), Inventory_));
    }
    std::error_code Unlink(const THandle& parent, std::string_view name) override
    {
        return Posix_.Unlink(parent, name);
    }
    std::error_code SyncFile(const THandle& file) override
    {
        ++FileSyncs_;
        return Posix_.SyncFile(file);
    }
    std::error_code SyncDir(const THandle& directory) override
    {
        ++DirSyncs_;
        return Posix_.SyncDir(directory);
    }
    std::error_code Close(const THandle& handle) override
    {
        return Posix_.Close(handle);
    }

    TJson Counters() const
    {
        return {{"read_bytes", Read_.load()},
            {"written_bytes", Written_.load()},
            {"read_opens", Opens_.load()},
            {"file_syncs", FileSyncs_.load()},
            {"directory_syncs", DirSyncs_.load()},
            {"warehouse_walks", TreeCalls_.load()},
            {"warehouse_walk_ns", TreeNanoseconds_.load()},
            {"inventory_calls", Inventory_->Calls.load()},
            {"inventory_ns", Inventory_->Nanoseconds.load()}};
    }

private:
    TPosixLocalFS Posix_;
    std::atomic<uint64_t> Read_{0}, Written_{0}, Opens_{0}, FileSyncs_{0}, DirSyncs_{0};
    std::atomic<uint64_t> TreeCalls_{0}, TreeNanoseconds_{0};
    const std::shared_ptr<TInventoryCounters> Inventory_ = std::make_shared<TInventoryCounters>();
};

struct TCounterBaseline
{
    TJson Values;
};

TJson Difference(TJson after, const TCounterBaseline& before)
{
    for (auto& [key, value] : after.items()) {
        value = value.get<uint64_t>() - before.Values.at(key).get<uint64_t>();
    }
    return after;
}

std::shared_ptr<arrow::RecordBatch> Batch()
{
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    for (int index = 0; index < 250; ++index) {
        Require(keys.AppendValues({"duplicate", "duplicate", "Привет", "other"}).ok(), "build keys failed");
        Require(values.Append(9007199254740993LL).ok() && values.Append(9007199254740993LL).ok() &&
                values.AppendNull().ok() && values.Append(-7).ok(),
            "build values failed");
    }
    auto keyArray = keys.Finish();
    auto valueArray = values.Finish();
    Require(keyArray.ok() && valueArray.ok(), "finish batch failed");
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("value", arrow::int64())}),
        1000,
        {*keyArray, *valueArray});
}

TJson Receipt(const AppendResult& result)
{
    return {{"collection_id", result.collection_id},
        {"operation_id", result.operation_id},
        {"payload_hash", result.payload_hash},
        {"hash_version", result.hash_version},
        {"snapshot_id", result.snapshot_id},
        {"parent_snapshot_id", result.parent_snapshot_id ? TJson(*result.parent_snapshot_id) : TJson(nullptr)},
        {"rows_appended", result.rows_appended},
        {"data_files_added", result.data_files_added},
        {"compressed_data_bytes_added", result.compressed_data_bytes_added},
        {"snapshot_created_at_us", result.snapshot_created_at_us},
        {"durability_profile", result.durability_profile},
        {"receipt_version", result.receipt_version}};
}

std::string OperationID(size_t index)
{
    char operation[37];
    std::snprintf(operation, sizeof(operation), "63000000-0000-4000-8001-%012zx", index + 1);
    return operation;
}

void CheckRows(const std::shared_ptr<Engine>& engine,
    const std::string& collection,
    SnapshotSelector selector,
    uint64_t appends)
{
    auto cursor = Take(engine->Scan({.collection_id = collection, .selector = selector}));
    std::array<uint64_t, 3> counts{};
    while (auto batch = Take(cursor->Next())) {
        const auto keys = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        const auto values = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            Require(!keys->IsNull(row), "NULL key in verified scan");
            const auto key = keys->GetView(row);
            if (key == "duplicate") {
                Require(!values->IsNull(row) && values->Value(row) == 9007199254740993LL, "duplicate value differs");
                ++counts[0];
            } else if (key == "Привет") {
                Require(values->IsNull(row), "UTF-8/NULL row differs");
                ++counts[1];
            } else {
                Require(key == "other" && !values->IsNull(row) && values->Value(row) == -7, "unexpected row");
                ++counts[2];
            }
        }
    }
    Require(counts == std::array<uint64_t, 3>{500 * appends, 250 * appends, 250 * appends}, "row multiplicity differs");
}

void Verify(const std::shared_ptr<Engine>& engine,
    const TJson& ledger,
    const std::shared_ptr<arrow::RecordBatch>& batch)
{
    for (const auto* collection : CollectionIDs) {
        uint64_t count = 0;
        const TJson* first = nullptr;
        for (const auto& receipt : ledger) {
            if (receipt.at("collection_id") == collection) {
                ++count;
                first = first ? first : &receipt;
                const auto found =
                    Take(engine->FindAppendOutcome(collection, receipt.at("operation_id").get<std::string>()));
                Require(found && found->disposition == CommitDisposition::Committed && found->result &&
                        Receipt(*found->result) == receipt,
                    "original receipt differs after reopen");
            }
        }
        CheckRows(engine, collection, SnapshotSelector::Latest(), count);
        CheckRows(engine, collection, SnapshotSelector::InitialEmpty(), 0);
        const auto history = Take(engine->ListSnapshots(collection));
        Require(history.snapshots.size() == count, "snapshot count differs");
        if (first) {
            CheckRows(engine, collection, SnapshotSelector::Id(first->at("snapshot_id")), 1);
            const auto retried = engine->AppendDurable({collection, first->at("operation_id"), {}, {batch}});
            Require(retried.disposition == CommitDisposition::Committed && retried.result &&
                    Receipt(*retried.result) == *first,
                "exact retry changed the original receipt");
        }
    }
}

TJson PhysicalUsage(const std::filesystem::path& root)
{
    uint64_t bytes = 0, logical = 0, files = 0;
    const auto add = [&](const std::filesystem::path& path) {
        struct stat info
        { };
        Require(::lstat(path.c_str(), &info) == 0 && (S_ISREG(info.st_mode) || S_ISDIR(info.st_mode)),
            "unexpected warehouse entry");
        const auto allocated = static_cast<uint64_t>(info.st_blocks) * 512;
        bytes += S_ISREG(info.st_mode) ? std::max(allocated, static_cast<uint64_t>(info.st_size)) : allocated;
        if (S_ISREG(info.st_mode)) {
            logical += static_cast<uint64_t>(info.st_size);
            ++files;
        }
    };
    add(root / "warehouse");
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        add(entry.path());
    }
    return {{"warehouse_physical_bytes", bytes},
        {"warehouse_file_bytes", logical},
        {"warehouse_files", files},
        {"catalog_bytes", std::filesystem::file_size(root / "catalog.sqlite")}};
}

TJson Usage(const std::shared_ptr<Engine>& engine, const std::shared_ptr<detail::TLocalStorageBackend>& backend)
{
    NGlamDB::NStorage::TStorageUsage total;
    auto latestManifests = TJson::array();
    for (const auto* collection : CollectionIDs) {
        auto cursor = Take(engine->Scan({.collection_id = collection, .limit = 0}));
        const auto state = Take(backend->VerifiedScanState(cursor->Info().identity));
        total.RootBytes += state->Usage.RootBytes;
        total.Snapshots += state->Usage.Snapshots;
        total.DataFiles += state->Usage.DataFiles;
        total.Manifests += state->Usage.Manifests;
        total.ManifestBytes += state->Usage.ManifestBytes;
        total.ManifestListBytes += state->Usage.ManifestListBytes;
        total.FooterBytes += state->Usage.FooterBytes;
        latestManifests.push_back(state->CurrentManifests.size());
    }
    return {{"root_bytes", total.RootBytes},
        {"snapshots", total.Snapshots},
        {"data_files", total.DataFiles},
        {"manifests", total.Manifests},
        {"latest_manifest_counts", std::move(latestManifests)},
        {"manifest_bytes", total.ManifestBytes},
        {"manifest_list_bytes", total.ManifestListBytes},
        {"footer_bytes", total.FooterBytes}};
}

uint64_t PeakRSS()
{
    rusage usage{};
    Require(::getrusage(RUSAGE_SELF, &usage) == 0, "getrusage failed");
#ifdef __APPLE__
    return static_cast<uint64_t>(usage.ru_maxrss);
#else
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
#endif
}

TJson ProcessIO()
{
    TJson result = TJson::object();
#ifdef __linux__
    std::ifstream input("/proc/self/io");
    Require(input.good(), "cannot read process I/O counters");
    std::string key;
    uint64_t value = 0;
    while (input >> key >> value) {
        key.pop_back();
        result[key] = value;
    }
#endif
    return result;
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc == 6, "usage: glamdb_capacity_bench segment|verify|pressure ROOT CONFIG LEDGER COUNT");
    const std::string mode(argv[1]);
    Require(mode == "segment" || mode == "verify" || mode == "pressure", "unknown mode");
    const std::filesystem::path root = std::filesystem::absolute(argv[2]);
    const auto config = ReadJson(argv[3]);
    auto ledger = ReadJson(argv[4]);
    Require(ledger.is_array() && ledger.size() <= 4096, "invalid ledger");
    int count = 0;
    const std::string_view countText(argv[5]);
    const auto [end, error] = std::from_chars(countText.data(), countText.data() + countText.size(), count);
    Require(error == std::errc{} && end == countText.data() + countText.size() && count >= 0 && count <= 5000,
        "invalid segment count");
    Limits limits;
    limits.max_operations = config.at("operations");
    auto fs = std::make_shared<TCountingFS>();
    auto owner = std::make_shared<ResourceOwner>();
    detail::TLocalStorageOptions options;
    options.Fs = fs;
    options.Resources = owner;
    options.Storage.Store.Snapshots = config.at("snapshots");
    options.Storage.WarehouseBytes = config.at("warehouse_bytes");
    options.Storage.AttemptReserveBytes = config.at("attempt_reserve_bytes");
    Require(options.Storage.Store.Snapshots > 0 && options.Storage.Store.Snapshots <= 4096,
        "history caps must not exceed defaults");
    std::shared_ptr<detail::TLocalStorageBackend> backend;
    const detail::BackendFactory factory = [&](const auto& path,
                                               const auto& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        backend = Take(detail::TLocalStorageBackend::Open(path, info, options));
        return backend;
    };
    auto started = TClock::now();
    const bool create = mode == "segment" && !std::filesystem::exists(root);
    auto opened = create
        ? detail::EngineFactory::CreateWith({StoreID, root.string(), "local-delete-extra-v1", limits, owner}, factory)
        : detail::EngineFactory::OpenWith({root.string(), StoreID, "local-delete-extra-v1", limits, owner}, factory);
    if (!opened) {
        std::cout << TJson{{"opened", false},
                         {"error", opened.error().code},
                         {"message", opened.error().message},
                         {"open_ms", ElapsedMs(started)},
                         {"rss_peak_bytes", PeakRSS()}}
                         .dump()
                  << '\n';
        return 0;
    }
    auto engine = std::move(*opened);
    const auto openMs = ElapsedMs(started);
    if (create) {
        Require(ledger.empty(), "new store with nonempty ledger");
        const SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::Int64, true}}};
        for (const auto* collection : CollectionIDs) {
            Require(engine->CreateCollection({collection, collection, schema}).disposition ==
                    CommitDisposition::Committed,
                "create collection failed");
        }
    }
    const auto emptyOrphans = config.value("empty_orphan_files", uint64_t{0});
    Require(emptyOrphans <= 1000000, "empty orphan fixture exceeds one million entries");
    if (create && emptyOrphans != 0) {
        const auto warehouse = fs->OpenDir(root / "warehouse");
        Require(static_cast<bool>(warehouse), "open fixture warehouse failed");
        Require(!fs->MakeDir(warehouse.Value(), "capacity-fixture"), "create fixture directory failed");
        const auto fixture = fs->OpenDir(warehouse.Value(), "capacity-fixture");
        Require(static_cast<bool>(fixture), "open fixture directory failed");
        for (uint64_t index = 0; index < emptyOrphans; ++index) {
            const auto file = fs->CreateExclusive(fixture.Value(), std::to_string(index), {});
            Require(file && !fs->Close(file.Value()), "create empty orphan failed");
        }
        Require(!fs->SyncDir(fixture.Value()) && !fs->Close(fixture.Value()) && !fs->SyncDir(warehouse.Value()) &&
                !fs->Close(warehouse.Value()),
            "finish empty orphan fixture failed");
    }
    const auto usageBefore = PhysicalUsage(root);
    const auto batch = Batch();
    std::shared_ptr<arrow::RecordBatch> retained;
    if (!ledger.empty()) {
        auto cursor = Take(engine->Scan({.collection_id = ledger[0].at("collection_id"),
            .selector = SnapshotSelector::Id(ledger[0].at("snapshot_id"))}));
        auto next = Take(cursor->Next());
        Require(next != nullptr, "missing historical rows");
        retained = next->Slice(0, 1);
    }
    TJson stop = nullptr;
    TJson added = TJson::array();
    TJson latencies = TJson::array();
    const TCounterBaseline before{fs->Counters()};
    const TCounterBaseline processBefore{ProcessIO()};
    int attempted = 0;
    started = TClock::now();
    for (; mode == "segment" && attempted < count; ++attempted) {
        const size_t index = ledger.size();
        const std::string collection = CollectionIDs[index % CollectionIDs.size()];
        const auto appendStarted = TClock::now();
        const auto outcome = engine->AppendDurable({collection, OperationID(index), {}, {batch}});
        latencies.push_back(ElapsedMs(appendStarted));
        if (outcome.disposition != CommitDisposition::Committed) {
            Require(outcome.disposition == CommitDisposition::DefinitelyNotCommitted && outcome.error &&
                    outcome.error->code == "RESOURCE_EXHAUSTED",
                "unexpected or unresolved append outcome");
            stop = {{"error", outcome.error->code},
                {"message", outcome.error->message},
                {"bound", Take(engine->FindAppendOutcome(collection, OperationID(index))).has_value()}};
            ++attempted;
            break;
        }
        if (!outcome.result) {
            throw std::runtime_error("committed append has no receipt");
        }
        ledger.push_back(Receipt(*outcome.result));
        added.push_back(ledger.back());
    }
    const auto appendMs = ElapsedMs(started);
    const auto appendIO = Difference(fs->Counters(), before);
    const auto processIO = Difference(ProcessIO(), processBefore);
    bool orphanVerified = false;
    if (mode == "pressure") {
        Require(!ledger.empty(), "pressure profile requires committed rows");
        Require(ledger.size() < static_cast<size_t>(limits.max_operations) &&
                ledger.size() < options.Storage.Store.Snapshots,
            "physical pressure requires unused operation and snapshot slots");
        const auto warehouse = PhysicalUsage(root).at("warehouse_physical_bytes").get<uint64_t>();
        const auto orphan = root / "warehouse" / "campaign-orphan.bin";
        Require(!std::filesystem::exists(orphan), "orphan evidence already exists");
        const auto handle = fs->OpenDir(root / "warehouse");
        Require(static_cast<bool>(handle), "open warehouse failed");
        const auto file = fs->CreateExclusive(handle.Value(), "campaign-orphan.bin", {});
        Require(static_cast<bool>(file), "create orphan failed");
        std::array<std::byte, 4096> bytes{};
        const auto written = fs->Write(file.Value(), bytes);
        Require(written && written.Value() == bytes.size() && !fs->SyncFile(file.Value()) && !fs->Close(file.Value()) &&
                !fs->SyncDir(handle.Value()) && !fs->Close(handle.Value()),
            "write orphan failed");
        // Reopen with a warehouse ceiling that includes the orphan, but has no
        // reserve for a new attempt. Existing reads and retries must still work.
        Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
        engine.reset();
        backend.reset();
        retained.reset();
        options.Storage.WarehouseBytes = warehouse + 65536;
        options.Storage.AttemptReserveBytes = 65536;
        engine = Take(
            detail::EngineFactory::OpenWith({root.string(), StoreID, "local-delete-extra-v1", limits, owner}, factory));
        const auto rejected = engine->AppendDurable({CollectionIDs[0], OperationID(5001), {}, {batch}});
        Require(rejected.disposition == CommitDisposition::DefinitelyNotCommitted && rejected.error &&
                rejected.error->code == "RESOURCE_EXHAUSTED" &&
                !Take(engine->FindAppendOutcome(CollectionIDs[0], OperationID(5001))),
            "orphan pressure did not refuse before binding");
        orphanVerified = true;
    }
    if (mode == "pressure") {
        auto cursor = Take(engine->Scan({.collection_id = ledger[0].at("collection_id"), .limit = 1}));
        auto next = Take(cursor->Next());
        Require(next != nullptr, "missing pressure slice");
        retained = next->Slice(0, 1);
        next.reset();
        cursor.reset();
        auto saturated = Take(owner->Reserve(ResourceClass::Data,
            owner->GetLimits().memory_bytes[0] - owner->GetUsage().memory_bytes[0] - (512ULL << 10)));
        const auto refused = engine->Scan({.collection_id = CollectionIDs[0]});
        Require(!refused && refused.error().code == "RESOURCE_EXHAUSTED", "retained data pressure did not refuse scan");
        const auto retried =
            engine->AppendDurable({ledger[0].at("collection_id"), ledger[0].at("operation_id"), {}, {batch}});
        Require(retried.result && Receipt(*retried.result) == ledger[0], "pressure changed old retry");
        saturated.reset();
    }
    Verify(engine, ledger, batch);
    const TCounterBaseline retryBefore{fs->Counters()};
    started = TClock::now();
    if (!ledger.empty()) {
        const auto& first = ledger[0];
        for (int repeat = 0; repeat < 10; ++repeat) {
            const auto retried =
                engine->AppendDurable({first.at("collection_id"), first.at("operation_id"), {}, {batch}});
            Require(retried.result && Receipt(*retried.result) == first, "retry receipt differs");
        }
    }
    const auto retryUs = ElapsedMs(started) * 1000 / 10;
    const auto retryIO = Difference(fs->Counters(), retryBefore);
    Require(retryIO.at("written_bytes") == 0 && retryIO.at("read_opens") == 0 && retryIO.at("file_syncs") == 0,
        "healthy retry performed external storage work");
    auto usage = Usage(engine, backend);
    usage.update(PhysicalUsage(root));
    Require(usage.at("snapshots") == ledger.size() && usage.at("data_files") == ledger.size(), "logical usage differs");
    if (!retained && !ledger.empty()) {
        auto cursor = Take(engine->Scan({.collection_id = ledger[0].at("collection_id"), .limit = 1}));
        auto next = Take(cursor->Next());
        Require(next != nullptr, "missing retained row");
        retained = next->Slice(0, 1);
    }
    Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
    engine.reset();
    backend.reset();
    bool retainedVerified = false;
    if (retained) {
        Require(owner->GetUsage().memory_bytes[0] > 0 &&
                std::static_pointer_cast<arrow::StringArray>(retained->column(0))->GetView(0) == "duplicate" &&
                std::static_pointer_cast<arrow::Int64Array>(retained->column(1))->Value(0) == 9007199254740993LL,
            "retained slice lost data or accounting after shutdown");
        retained.reset();
        Require(owner->GetUsage().memory_bytes[0] == 0, "retained data reservation leaked");
        retainedVerified = true;
    }
    const auto resources = owner->GetUsage();
    std::cout << TJson{{"opened", true},
                     {"created", create},
                     {"attempted_appends", attempted},
                     {"successful_appends", ledger.size()},
                     {"receipts", added},
                     {"stop", stop},
                     {"open_ms", openMs},
                     {"append_ms", appendMs},
                     {"append_latencies_ms", latencies},
                     {"append_rows_per_second",
                         appendMs > 0 ? 1000000.0 * static_cast<double>(added.size()) / appendMs : 0},
                     {"empty_orphan_files", emptyOrphans},
                     {"retry_us", retryUs},
                     {"retry_io", retryIO},
                     {"io", appendIO},
                     {"process_io", processIO},
                     {"usage_before", usageBefore},
                     {"usage", usage},
                     {"rss_peak_bytes", PeakRSS()},
                     {"resource_memory_peak_bytes", resources.memory_peak},
                     {"retained_slice_verified", retainedVerified},
                     {"orphan_pressure_verified", orphanVerified},
                     {"verified", true}}
                     .dump()
              << '\n';
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
