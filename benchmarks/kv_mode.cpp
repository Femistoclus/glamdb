#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <arrow/util/byte_size.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <sys/resource.h>

namespace {

using namespace glamdb;
using namespace NGlamDB::NIO;
using TJson = nlohmann::json;
using TClock = std::chrono::steady_clock;
constexpr auto StoreID = "71000000-0000-4000-8000-000000000001";
constexpr auto CollectionID = "71000000-0000-4000-8000-000000000002";
constexpr auto CompactCollectionID = "71000000-0000-4000-8000-000000000003";
constexpr uint64_t PayloadBytes = 4096;
constexpr uint64_t LogicalRowBytes = 20 + PayloadBytes + 8 + 8;
constexpr uint64_t RowsPerOperation = (256ULL << 20) / LogicalRowBytes;
const SchemaSpec Schema{
    {{"key", FieldType::String, false}, {"payload", FieldType::String, false}, {"version", FieldType::Int64, false}}};

struct TRowRange
{
    uint64_t Start;
    uint64_t Count;
};

struct TPaths
{
    std::filesystem::path Root;
    std::filesystem::path Ledger;
};

void Require(bool condition, const std::string& message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <class T>
T Take(Result<T> result)
{
    Require(result.has_value(), result ? "" : result.error().code + ": " + result.error().message);
    return std::move(*result);
}

void ArrowOK(const arrow::Status& status)
{
    Require(status.ok(), status.ToString());
}

double Milliseconds(TClock::time_point start)
{
    return std::chrono::duration<double, std::milli>(TClock::now() - start).count();
}

uint64_t PeakRSS()
{
    rusage usage{};
    Require(getrusage(RUSAGE_SELF, &usage) == 0, "getrusage failed");
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
}

std::string Operation(uint64_t ordinal)
{
    char value[64];
    const auto count = std::snprintf(value,
        sizeof(value),
        "71000000-0000-4000-8000-%012llx",
        static_cast<unsigned long long>(ordinal));
    Require(count == 36, "operation UUID has an invalid width");
    return {value, static_cast<size_t>(count)};
}

std::string Key(uint64_t row)
{
    char value[32];
    const auto count = std::snprintf(value, sizeof(value), "key:%016llu", static_cast<unsigned long long>(row));
    Require(count == 20, "key width differs");
    return {value, static_cast<size_t>(count)};
}

uint64_t Mix(uint64_t bits)
{
    bits = (bits ^ (bits >> 30)) * 0xbf58476d1ce4e5b9ULL;
    bits = (bits ^ (bits >> 27)) * 0x94d049bb133111ebULL;
    return bits ^ (bits >> 31);
}

std::string Payload(uint64_t row, uint64_t version)
{
    constexpr std::string_view Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string value(PayloadBytes, ' ');
    for (uint64_t offset = 0; offset < PayloadBytes; offset += 8) {
        auto bits = Mix(row * 0x9e3779b97f4a7c15ULL + offset / 8 + version * 0x517cc1b727220a95ULL);
        for (uint64_t byte = offset; byte < std::min(PayloadBytes, offset + 8); ++byte) {
            value[byte] = Alphabet[bits & 63];
            bits >>= 6;
        }
    }
    return value;
}

std::shared_ptr<arrow::RecordBatch> Batch(TRowRange range, int64_t version)
{
    const auto [start, rows] = range;
    arrow::StringBuilder keys;
    arrow::StringBuilder values;
    arrow::Int64Builder versions;
    ArrowOK(keys.Reserve(static_cast<int64_t>(rows)));
    ArrowOK(keys.ReserveData(static_cast<int64_t>(rows * 20)));
    ArrowOK(values.Reserve(static_cast<int64_t>(rows)));
    ArrowOK(values.ReserveData(static_cast<int64_t>(rows * PayloadBytes)));
    ArrowOK(versions.Reserve(static_cast<int64_t>(rows)));
    for (uint64_t row = start; row < start + rows; ++row) {
        ArrowOK(keys.Append(Key(row)));
        ArrowOK(values.Append(Payload(row, static_cast<uint64_t>(version))));
        ArrowOK(versions.Append(version));
    }
    auto keyArray = keys.Finish();
    auto valueArray = values.Finish();
    auto versionArray = versions.Finish();
    Require(keyArray.ok() && valueArray.ok() && versionArray.ok(), "cannot finish source batch");
    auto result = arrow::RecordBatch::Make(arrow::schema({arrow::field("key", arrow::utf8(), false),
                                               arrow::field("payload", arrow::utf8(), false),
                                               arrow::field("version", arrow::int64(), false)}),
        static_cast<int64_t>(rows),
        {*keyArray, *valueArray, *versionArray});
    Require(arrow::util::TotalBufferSize(*result) <= (16LL << 20), "source batch exceeds its byte limit");
    return result;
}

class TSource final : public BulkAppendSource
{
public:
    explicit TSource(TRowRange range)
        : Start_(range.Start)
        , Rows_(range.Count)
    { }

    Status Rewind() override
    {
        Position_ = 0;
        DecodedBytes = 0;
        ++Passes;
        return {};
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        if (Position_ == Rows_) {
            return nullptr;
        }
        const auto count = std::min<uint64_t>(1024, Rows_ - Position_);
        auto batch = Batch({Start_ + Position_, count}, 0);
        Position_ += count;
        DecodedBytes += static_cast<uint64_t>(arrow::util::TotalBufferSize(*batch));
        return batch;
    }

    uint64_t DecodedBytes = 0;
    uint64_t Passes = 0;

private:
    const uint64_t Start_;
    const uint64_t Rows_;
    uint64_t Position_ = 0;
};

// Counts requested and accepted external-file I/O. Catalog and SQLite mutation
// scratch use independent VFSs and are explicitly excluded from these values.
class TCountingFS final : public ILocalFS
{
public:
    TResult<THandle> OpenDir(const std::filesystem::path& path) override
    {
        return Native_.OpenDir(path);
    }
    TResult<THandle> OpenDir(const THandle& parent, std::string_view name) override
    {
        return Native_.OpenDir(parent, name);
    }
    std::error_code MakeDir(const THandle& parent, std::string_view name) override
    {
        return Native_.MakeDir(parent, name);
    }
    TResult<THandle> CreateExclusive(const THandle& parent, std::string_view name, const TIOTag& tag) override
    {
        auto result = Native_.CreateExclusive(parent, name, tag);
        if (result) {
            Tag(result.Value(), tag.Kind == EFileKind::Data);
        }
        return result;
    }
    TResult<THandle> OpenRead(const THandle& parent, std::string_view name) override
    {
        auto result = Native_.OpenRead(parent, name);
        if (result) {
            Tag(result.Value(), name.ends_with(".parquet"));
        }
        return result;
    }
    TResult<std::size_t> Write(const THandle& file, std::span<const std::byte> bytes) override
    {
        WriteRequested_ += bytes.size();
        if (IsData(file)) {
            DataWriteRequested_ += bytes.size();
        }
        auto result = Native_.Write(file, bytes);
        if (result) {
            Written_ += result.Value();
        }
        return result;
    }
    TResult<std::size_t> PRead(const THandle& file, uint64_t offset, std::span<std::byte> bytes) override
    {
        ReadRequested_ += bytes.size();
        if (IsData(file)) {
            DataReadRequested_ += bytes.size();
        }
        auto result = Native_.PRead(file, offset, bytes);
        if (result) {
            Read_ += result.Value();
        }
        return result;
    }
    TResult<TFileStat> Stat(const THandle& file) override
    {
        return Native_.Stat(file);
    }
    TResult<TSpaceInfo> StatFs(const THandle& file) override
    {
        return Native_.StatFs(file);
    }
    TResult<uint64_t> MeasureTree(const THandle& file, uint64_t limit) override
    {
        return Native_.MeasureTree(file, limit);
    }
    std::error_code Unlink(const THandle& parent, std::string_view name) override
    {
        return Native_.Unlink(parent, name);
    }
    std::error_code SyncFile(const THandle& file) override
    {
        if (IsData(file)) {
            ++DataSyncs_;
        }
        return Native_.SyncFile(file);
    }
    std::error_code SyncDir(const THandle& directory) override
    {
        return Native_.SyncDir(directory);
    }
    std::error_code Close(const THandle& handle) override
    {
        auto result = Native_.Close(handle);
        std::lock_guard lock(Mutex_);
        Data_.erase(handle.get());
        return result;
    }
    TJson Counters() const
    {
        return {{"external_read_requested_bytes", ReadRequested_.load()},
            {"external_write_requested_bytes", WriteRequested_.load()},
            {"external_read_accepted_bytes", Read_.load()},
            {"external_write_accepted_bytes", Written_.load()},
            {"parquet_read_requested_bytes", DataReadRequested_.load()},
            {"parquet_write_requested_bytes", DataWriteRequested_.load()},
            {"parquet_file_syncs", DataSyncs_.load()}};
    }

private:
    TPosixLocalFS Native_;
    mutable std::mutex Mutex_;
    std::map<const TFSHandle*, bool> Data_;
    std::atomic<uint64_t> ReadRequested_{0}, WriteRequested_{0}, Read_{0}, Written_{0};
    std::atomic<uint64_t> DataReadRequested_{0}, DataWriteRequested_{0}, DataSyncs_{0};

    void Tag(const THandle& handle, bool data)
    {
        std::lock_guard lock(Mutex_);
        Data_[handle.get()] = data;
    }
    bool IsData(const THandle& handle) const
    {
        std::lock_guard lock(Mutex_);
        const auto found = Data_.find(handle.get());
        return found != Data_.end() && found->second;
    }
};

// The operands intentionally mirror subtraction: after minus before.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
TJson Difference(TJson after, const TJson& before)
{
    for (auto& [key, value] : after.items()) {
        value = value.get<uint64_t>() - before.at(key).get<uint64_t>();
    }
    return after;
}

TJson ReadJson(const std::filesystem::path& path)
{
    std::ifstream input(path);
    Require(input.good(), "cannot read benchmark ledger");
    return TJson::parse(input);
}

void WriteJson(const std::filesystem::path& path, const TJson& value)
{
    std::ofstream output(path);
    output << value.dump(2) << '\n';
    output.close();
    Require(!output.fail(), "cannot write benchmark ledger");
}

TJson Receipt(const AppendOutcome& outcome)
{
    TJson result{{"disposition", static_cast<int>(outcome.disposition)}};
    if (outcome.error) {
        result["error"] = {{"code", outcome.error->code}, {"message", outcome.error->message}};
    }
    if (outcome.result) {
        const auto& value = *outcome.result;
        result["snapshot_id"] = value.snapshot_id;
        result["snapshot_created"] = value.snapshot_created;
        result["input_rows"] = value.input_rows;
        result["rows_affected"] = value.rows_affected;
        result["rows_written"] = value.rows_written;
        result["rows_removed"] = value.rows_removed;
        result["files_added"] = value.data_files_added;
        result["files_removed"] = value.data_files_removed;
        result["bytes_added"] = value.compressed_data_bytes_added;
        result["bytes_removed"] = value.compressed_data_bytes_removed;
    }
    return result;
}

const AppendResult& Committed(const AppendOutcome& outcome)
{
    if (outcome.disposition != CommitDisposition::Committed || !outcome.result) {
        throw std::runtime_error(Receipt(outcome).dump());
    }
    return *outcome.result;
}

std::shared_ptr<Engine> Open(const std::filesystem::path& root, const std::shared_ptr<TCountingFS>& fs, bool create)
{
    const detail::BackendFactory factory =
        [fs](const auto& path, const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        detail::TLocalStorageOptions options;
        options.Fs = fs;
        auto backend = detail::TLocalStorageBackend::Open(path, info, options);
        if (!backend) {
            return std::unexpected(backend.error());
        }
        return *backend;
    };
    return Take(create ? detail::EngineFactory::CreateWith({StoreID, root.string()}, factory)
                       : detail::EngineFactory::OpenWith({root.string(), StoreID}, factory));
}

void CreateCollection(Engine& engine, std::string_view id, std::string_view name)
{
    CreateCollectionRequest request{std::string(id), std::string(name), Schema};
    request.mode = CollectionMode::KeyValue;
    request.write_settings.compression = ParquetCompression{ParquetCodec::Uncompressed, std::nullopt};
    request.write_settings.dictionary = DictionaryPolicy::Disabled;
    const auto created = engine.CreateCollection(request);
    Require(created.disposition == CommitDisposition::Committed,
        created.error ? created.error->message : "collection creation failed");
}

TJson Build(const TPaths& paths, uint64_t mib)
{
    const auto& [root, ledger] = paths;
    auto fs = std::make_shared<TCountingFS>();
    auto engine = Open(root, fs, true);
    CreateCollection(*engine, CollectionID, "native-kv-fixture");
    const auto rows = ((mib << 20) + LogicalRowBytes - 1) / LogicalRowBytes;
    TJson result{{"profile", "native-kv-disjoint-ranges"},
        {"requested_decoded_mib", mib},
        {"rows", rows},
        {"payload_bytes_per_row", PayloadBytes},
        {"source_decoded_bytes", 0},
        {"operations", TJson::array()},
        {"io_scope",
            "requested/accepted external FileIO; Parquet separated; catalog and mutation scratch SQLite excluded"}};
    const auto started = TClock::now();
    uint64_t ordinal = 1;
    for (uint64_t start = 0; start < rows;) {
        const auto count = std::min(RowsPerOperation, rows - start);
        auto source = std::make_shared<TSource>(TRowRange{start, count});
        const auto counters = fs->Counters();
        const auto operationStarted = TClock::now();
        const auto outcome = engine->BulkUpsertDurable(
            {std::string(CollectionID), Operation(ordinal), {}, static_cast<int64_t>(count), source});
        auto measurement = Receipt(outcome);
        measurement["elapsed_ms"] = Milliseconds(operationStarted);
        measurement["io"] = Difference(fs->Counters(), counters);
        measurement["source_passes"] = source->Passes;
        measurement["source_decoded_bytes"] = source->DecodedBytes;
        result["operations"].push_back(measurement);
        WriteJson(ledger, result);
        std::cout << TJson{{"phase", "build"},
                         {"operation", ordinal},
                         {"rows_completed", start + count},
                         {"rows_total", rows},
                         {"elapsed_ms", measurement["elapsed_ms"]}}
                         .dump()
                  << '\n';
        std::cout.flush();
        Committed(outcome);
        result["source_decoded_bytes"] = result["source_decoded_bytes"].get<uint64_t>() + source->DecodedBytes;
        result["base_snapshot"] = Committed(outcome).snapshot_id;
        start += count;
        ++ordinal;
    }
    const auto statistics = Take(engine->GetStatistics(CollectionID));
    Require(statistics.rows.value == static_cast<int64_t>(rows), "native fixture row count differs");
    result["build_elapsed_ms"] = Milliseconds(started);
    result["current_files"] = statistics.data_files.value.value_or(-1);
    result["current_parquet_bytes"] = statistics.compressed_data_bytes.value.value_or(-1);
    result["peak_rss_bytes"] = PeakRSS();
    result["resource_memory_peak"] = engine->GetResourceOwner()->GetUsage().memory_peak;
    result["build_complete"] = true;
    Require(engine->Shutdown(std::chrono::seconds(30)).has_value(), "shutdown failed");
    WriteJson(ledger, result);
    return result;
}

void VerifyRows(Engine& engine, TRowRange range, int64_t version, SnapshotSelector selector)
{
    const auto [start, count] = range;
    LookupRequest request{std::string(CollectionID), {}, selector};
    request.snapshot = selector;
    for (uint64_t row = start; row < start + count; ++row) {
        request.keys.push_back(Key(row));
    }
    request.keys.push_back("absent-key");
    request.keys.push_back(Key(start));
    const auto found = Take(engine.Lookup(request));
    Require(found.rows.size() == count + 2 && !found.rows[static_cast<size_t>(count)], "lookup misses/order differs");
    for (uint64_t index = 0; index < count; ++index) {
        const auto& row = found.rows[static_cast<size_t>(index)];
        Require(row && row->size() == 3 && std::get<std::string>((*row)[0]) == Key(start + index) &&
                std::get<std::string>((*row)[1]) == Payload(start + index, static_cast<uint64_t>(version)) &&
                std::get<int64_t>((*row)[2]) == version,
            "full-row lookup differs from independent generated values");
    }
    Require(found.rows.back() == found.rows.front(), "lookup repeated key differs");
}

TJson Exercise(const TPaths& paths)
{
    const auto& [root, ledger] = paths;
    auto result = ReadJson(ledger);
    auto fs = std::make_shared<TCountingFS>();
    const auto opened = TClock::now();
    auto engine = Open(root, fs, false);
    result["exercise_open_ms"] = Milliseconds(opened);
    const auto rows = result.at("rows").get<uint64_t>();
    const auto count = std::min<uint64_t>(1000, rows);
    const auto start = (rows - count) / 2;
    const auto base = result.at("base_snapshot").get<int64_t>();
    auto oldCursor = Take(engine->Scan({.collection_id = std::string(CollectionID),
        .selector = SnapshotSelector::Id(base),
        .projection = std::vector<std::string>{"key"},
        .limit = 1}));
    const auto counters = fs->Counters();
    const auto begun = TClock::now();
    const auto changed =
        engine->UpsertDurable({std::string(CollectionID), Operation(0xf001), {}, {Batch({start, count}, 1)}});
    result["clustered_upsert"] = Receipt(changed);
    result["clustered_upsert"]["elapsed_ms"] = Milliseconds(begun);
    result["clustered_upsert"]["io"] = Difference(fs->Counters(), counters);
    Committed(changed);
    result["cluster_start"] = start;
    result["cluster_count"] = count;
    result["latest_snapshot"] = Committed(changed).snapshot_id;
    Require(Take(oldCursor->Next()) != nullptr, "held historical cursor lost its file");
    oldCursor->Cancel();
    oldCursor.reset();
    const auto lookupBefore = fs->Counters();
    const auto lookupStarted = TClock::now();
    VerifyRows(*engine, {start, count}, 1, SnapshotSelector::Latest());
    result["full_lookup"] = {{"elapsed_ms", Milliseconds(lookupStarted)},
        {"io", Difference(fs->Counters(), lookupBefore)},
        {"rows_with_miss_and_repeat", count + 2}};
    VerifyRows(*engine, {start, count}, 0, SnapshotSelector::Id(base));
    const auto same =
        engine->UpsertDurable({std::string(CollectionID), Operation(0xf002), {}, {Batch({start, count}, 1)}});
    Committed(same);
    Require(!Committed(same).snapshot_created, "unchanged full-row update created a snapshot");
    result["unchanged_upsert"] = Receipt(same);
    const auto missing =
        engine->DeleteDurable({std::string(CollectionID), Operation(0xf003), {}, {"absent-key", "absent-key"}});
    Committed(missing);
    Require(!Committed(missing).snapshot_created, "missing delete created a snapshot");
    result["missing_delete"] = Receipt(missing);

    CreateCollection(*engine, CompactCollectionID, "native-kv-compaction");
    for (uint64_t part = 0; part < 4; ++part) {
        auto source = std::make_shared<TSource>(TRowRange{part * 2048, 2048});
        Committed(
            engine->BulkUpsertDurable({std::string(CompactCollectionID), Operation(0xc000 + part), {}, 2048, source}));
    }
    DeleteRequest remove{std::string(CompactCollectionID), Operation(0xc010), {}, {}};
    for (uint64_t row = 0; row < 8192; ++row) {
        if (row % 2048 != 0) {
            remove.keys.push_back(Key(row));
        }
    }
    Committed(engine->DeleteDurable(remove));
    const auto compactBefore = fs->Counters();
    const auto compactStarted = TClock::now();
    const auto compact = engine->CompactDurable({std::string(CompactCollectionID), Operation(0xc011)});
    result["compaction"] = Receipt(compact);
    result["compaction"]["elapsed_ms"] = Milliseconds(compactStarted);
    result["compaction"]["io"] = Difference(fs->Counters(), compactBefore);
    Committed(compact);
    Require(Committed(compact).snapshot_created && Committed(compact).data_files_removed >= 4 &&
            Committed(compact).data_files_added == 1,
        "bounded compaction did not reduce contiguous small files");
    result["peak_rss_bytes_exercise"] = PeakRSS();
    result["resource_memory_peak_exercise"] = engine->GetResourceOwner()->GetUsage().memory_peak;
    result["exercise_complete"] = true;
    Require(engine->Shutdown(std::chrono::seconds(30)).has_value(), "exercise shutdown failed");
    WriteJson(ledger, result);
    return result;
}

TJson Reopen(const TPaths& paths)
{
    const auto& [root, ledger] = paths;
    auto result = ReadJson(ledger);
    auto fs = std::make_shared<TCountingFS>();
    const auto opened = TClock::now();
    auto engine = Open(root, fs, false);
    result["fresh_reopen_ms"] = Milliseconds(opened);
    result["fresh_reopen_io"] = fs->Counters();
    const auto start = result.at("cluster_start").get<uint64_t>();
    const auto count = result.at("cluster_count").get<uint64_t>();
    VerifyRows(*engine, {start, count}, 1, SnapshotSelector::Latest());
    VerifyRows(*engine, {start, count}, 0, SnapshotSelector::Id(result.at("base_snapshot").get<int64_t>()));
    const auto statistics = Take(engine->GetStatistics(CollectionID));
    Require(statistics.rows.value == result.at("rows").get<int64_t>(), "reopened fixture row count differs");
    const auto retry =
        engine->UpsertDurable({std::string(CollectionID), Operation(0xf001), {}, {Batch({start, count}, 1)}});
    Committed(retry);
    Require(Committed(retry).snapshot_id == result.at("latest_snapshot").get<int64_t>(), "exact retry changed receipt");
    const auto noOp =
        engine->UpsertDurable({std::string(CollectionID), Operation(0xf002), {}, {Batch({start, count}, 1)}});
    Committed(noOp);
    Require(!Committed(noOp).snapshot_created, "reopened no-op retry changed receipt");
    result["fresh_reopen_verified"] = true;
    result["peak_rss_bytes_reopen"] = PeakRSS();
    Require(engine->Shutdown(std::chrono::seconds(30)).has_value(), "reopen shutdown failed");
    WriteJson(ledger, result);
    return result;
}

} // namespace

int main(int argc, char** argv)
{
    try {
        Require(argc >= 4, "usage: kv_mode build|exercise|reopen ABSOLUTE_ROOT LEDGER [DECODED_MIB]");
        const std::string_view phase(argv[1]);
        const std::filesystem::path root(argv[2]);
        const std::filesystem::path ledger(argv[3]);
        Require(root.is_absolute() && ledger.is_absolute(), "root and ledger must be absolute");
        const TPaths paths{root, ledger};
        TJson result;
        if (phase == "build") {
            Require(argc == 5, "build needs bounded decoded MiB");
            uint64_t mib = 0;
            const std::string_view value(argv[4]);
            const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), mib);
            Require(error == std::errc{} && end == value.data() + value.size() && mib >= 1 && mib <= 16384,
                "decoded MiB must be within 1..16384");
            result = Build(paths, mib);
        } else if (phase == "exercise") {
            result = Exercise(paths);
        } else if (phase == "reopen") {
            result = Reopen(paths);
        } else {
            throw std::runtime_error("unknown benchmark phase");
        }
        std::cout << result.dump(2) << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
