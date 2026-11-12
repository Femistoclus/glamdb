#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/storage/base_state.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <sys/resource.h>
#include <unistd.h>

namespace {

using namespace glamdb;
using namespace NGlamDB::NIO;
using TJson = nlohmann::json;
using TClock = std::chrono::steady_clock;

constexpr auto StoreID = "64000000-0000-4000-8000-000000000001";
constexpr auto CollectionID = "64000000-0000-4000-8000-000000000002";
constexpr uint64_t CanonicalHeaderBytes = 74;
constexpr uint64_t KeyBytes = 20;

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

double Milliseconds(TClock::time_point start, TClock::time_point end = TClock::now())
{
    return std::chrono::duration<double, std::milli>(end - start).count();
}

uint64_t Count(const char* argument, uint64_t maximum)
{
    const std::string_view text(argument);
    uint64_t result = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), result);
    Require(error == std::errc{} && end == text.data() + text.size() && result > 0 && result <= maximum,
        "invalid finite workload count");
    return result;
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

TJson ReadJson(const std::filesystem::path& path)
{
    std::ifstream input(path);
    Require(input.good(), "cannot read measurement ledger");
    return TJson::parse(input);
}

void WriteJson(const std::filesystem::path& path, const TJson& value)
{
    std::ofstream output(path);
    output << value.dump(2) << '\n';
    output.close();
    Require(!output.fail(), "cannot write measurement ledger");
}

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

// Counts only external-file operations. SQLite uses its own VFS and is excluded.
// Handle tags are discarded on Close; the observer retains no payload or log.
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
        auto result = Posix_.CreateExclusive(parent, name, tag);
        if (result) {
            std::lock_guard lock(Mutex_);
            Kinds_.emplace(result.Value().get(), tag.Kind);
        }
        return result;
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
        return Posix_.MeasureTree(handle, limit);
    }
    std::error_code Unlink(const THandle& parent, std::string_view name) override
    {
        return Posix_.Unlink(parent, name);
    }
    std::error_code SyncFile(const THandle& file) override
    {
        ++FileSyncs_;
        EFileKind kind = EFileKind::Other;
        {
            std::lock_guard lock(Mutex_);
            if (const auto found = Kinds_.find(file.get()); found != Kinds_.end()) {
                kind = found->second;
            }
        }
        if (kind == EFileKind::Data) {
            ++DataSyncs_;
        }
        const auto error = Posix_.SyncFile(file);
        if (!error && kind == EFileKind::Data && CrashAfterDataSync_) {
            ::_exit(87);
        }
        return error;
    }
    std::error_code SyncDir(const THandle& directory) override
    {
        ++DirSyncs_;
        return Posix_.SyncDir(directory);
    }
    std::error_code Close(const THandle& handle) override
    {
        const auto error = Posix_.Close(handle);
        std::lock_guard lock(Mutex_);
        Kinds_.erase(handle.get());
        return error;
    }

    void CrashAfterDataSync()
    {
        CrashAfterDataSync_ = true;
    }

    TJson Counters() const
    {
        return {{"read_bytes", Read_.load()},
            {"written_bytes", Written_.load()},
            {"read_opens", Opens_.load()},
            {"file_syncs", FileSyncs_.load()},
            {"data_file_syncs", DataSyncs_.load()},
            {"directory_syncs", DirSyncs_.load()}};
    }

private:
    TPosixLocalFS Posix_;
    std::atomic<uint64_t> Read_{0}, Written_{0}, Opens_{0}, FileSyncs_{0}, DataSyncs_{0}, DirSyncs_{0};
    std::mutex Mutex_;
    std::map<const TFSHandle*, EFileKind> Kinds_;
    bool CrashAfterDataSync_ = false;
};

struct TWorkload
{
    uint64_t Rows;
    uint64_t PayloadBytes;
    uint64_t ChunkRows;
};

uint64_t NullRows(uint64_t rows)
{
    const auto pairs = rows / 2;
    return 2 * ((pairs + 6) / 7) + (rows % 2 && pairs % 7 == 0 ? 1 : 0);
}

// Canonical v1 has a presence byte per cell, uint32 string lengths and int64
// non-NULL values. This arithmetic does not call the engine's canonical codec.
uint64_t CanonicalRows(const TWorkload& workload)
{
    return workload.Rows * (3 + 4 + KeyBytes + 4 + workload.PayloadBytes) +
        8 * (workload.Rows - NullRows(workload.Rows));
}

uint64_t Mix(uint64_t bits)
{
    bits = (bits ^ (bits >> 30)) * 0xbf58476d1ce4e5b9ULL;
    bits = (bits ^ (bits >> 27)) * 0x94d049bb133111ebULL;
    return bits ^ (bits >> 31);
}

std::string Key(uint64_t pair)
{
    char buffer[32];
    const auto length = std::snprintf(buffer, sizeof(buffer), "key:%016llu", static_cast<unsigned long long>(pair));
    Require(length == static_cast<int>(KeyBytes), "generated key width differs");
    return {buffer, KeyBytes};
}

std::string Payload(const TWorkload& workload, uint64_t pair)
{
    constexpr std::string_view alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    const auto size = workload.PayloadBytes;
    std::string result(size, ' ');
    result[0] = static_cast<char>(0xce);
    result[1] = static_cast<char>(0xbb);
    for (uint64_t offset = 2; offset < size; offset += 8) {
        auto bits = Mix(pair * 0x9e3779b97f4a7c15ULL + (offset - 2) / 8);
        for (uint64_t byte = offset; byte < std::min(size, offset + 8); ++byte) {
            result[byte] = alphabet[bits & 63];
            bits >>= 6;
        }
    }
    return result;
}

int64_t Value(uint64_t pair)
{
    const auto magnitude = 9007199254740993LL + static_cast<int64_t>(pair);
    return pair % 2 ? -magnitude : magnitude;
}

std::shared_ptr<arrow::Schema> ArrowSchema()
{
    return arrow::schema({arrow::field("key", arrow::utf8(), false),
        arrow::field("payload", arrow::utf8(), false),
        arrow::field("value", arrow::int64(), true)});
}

uint64_t RetainedBytes(const arrow::RecordBatch& batch)
{
    uint64_t bytes = 0;
    for (const auto& column : batch.columns()) {
        for (const auto& buffer : column->data()->buffers) {
            if (buffer) {
                bytes += static_cast<uint64_t>(buffer->capacity());
            }
        }
    }
    return bytes;
}

std::shared_ptr<arrow::RecordBatch> Batch(const TWorkload& workload, uint64_t first, uint64_t count)
{
    arrow::StringBuilder keys;
    arrow::StringBuilder payloads;
    arrow::Int64Builder values;
    ArrowOK(keys.Reserve(static_cast<int64_t>(count)));
    ArrowOK(keys.ReserveData(static_cast<int64_t>(count * KeyBytes)));
    ArrowOK(payloads.Reserve(static_cast<int64_t>(count)));
    ArrowOK(payloads.ReserveData(static_cast<int64_t>(count * workload.PayloadBytes)));
    ArrowOK(values.Reserve(static_cast<int64_t>(count)));
    std::string key, payload;
    uint64_t lastPair = UINT64_MAX;
    for (uint64_t row = first; row < first + count; ++row) {
        const auto pair = row / 2;
        if (pair != lastPair) {
            key = Key(pair);
            payload = Payload(workload, pair);
            lastPair = pair;
        }
        ArrowOK(keys.Append(key));
        ArrowOK(payloads.Append(payload));
        ArrowOK(pair % 7 == 0 ? values.AppendNull() : values.Append(Value(pair)));
    }
    const auto keyArray = keys.Finish();
    const auto payloadArray = payloads.Finish();
    const auto valueArray = values.Finish();
    Require(keyArray.ok() && payloadArray.ok() && valueArray.ok(), "finish source chunk failed");
    auto batch =
        arrow::RecordBatch::Make(ArrowSchema(), static_cast<int64_t>(count), {*keyArray, *payloadArray, *valueArray});
    Require(RetainedBytes(*batch) <= (16ULL << 20), "generated source chunk exceeds retained-buffer bound");
    return batch;
}

class TGeneratedSource final : public BulkAppendSource
{
public:
    explicit TGeneratedSource(TWorkload workload, bool crash = false)
        : Workload_(workload)
        , Crash_(crash)
    { }

    Status Rewind() override
    {
        Position_ = 0;
        ++Passes_;
        PassStarted_ = TClock::now();
        CurrentChunks_ = 0;
        return {};
    }

    Result<std::shared_ptr<arrow::RecordBatch>> Next() override
    {
        // The first production chunk has already been handed to the engine.
        // The coordinator has bound this operation before production begins.
        if (Crash_ && Passes_ == 2 && Position_ > 0) {
            ::_exit(86);
        }
        if (Position_ == Workload_.Rows) {
            const auto now = TClock::now();
            if (Passes_ == 2) {
                ProductionEOF_ = now;
            }
            PassMetrics_.push_back({{"pass", Passes_},
                {"rows", Position_},
                {"chunks", CurrentChunks_},
                {"elapsed_ms", Milliseconds(PassStarted_, now)}});
            return nullptr;
        }
        const auto count = std::min(Workload_.ChunkRows, Workload_.Rows - Position_);
        auto batch = Batch(Workload_, Position_, count);
        Position_ += count;
        TotalRows_ += count;
        ++CurrentChunks_;
        MaxRows_ = std::max(MaxRows_, count);
        MaxRetained_ = std::max(MaxRetained_, RetainedBytes(*batch));
        return batch;
    }

    TJson Metrics() const
    {
        return {{"source_passes", Passes_},
            {"source_rows_read", TotalRows_},
            {"source_max_batch_rows", MaxRows_},
            {"source_max_retained_bytes", MaxRetained_},
            {"source_pass_metrics", PassMetrics_}};
    }

    std::optional<TClock::time_point> ProductionEOF() const
    {
        return ProductionEOF_;
    }

private:
    const TWorkload Workload_;
    bool Crash_;
    uint64_t Position_ = 0, Passes_ = 0, TotalRows_ = 0, CurrentChunks_ = 0, MaxRows_ = 0, MaxRetained_ = 0;
    TClock::time_point PassStarted_{};
    std::optional<TClock::time_point> ProductionEOF_;
    TJson PassMetrics_ = TJson::array();
};

std::string OperationID(size_t index)
{
    char buffer[37];
    std::snprintf(buffer, sizeof(buffer), "64000000-0000-4000-8001-%012zx", index + 1);
    return buffer;
}

TJson Receipt(const AppendResult& receipt)
{
    return {{"collection_id", receipt.collection_id},
        {"operation_id", receipt.operation_id},
        {"payload_hash", receipt.payload_hash},
        {"hash_version", receipt.hash_version},
        {"snapshot_id", receipt.snapshot_id},
        {"parent_snapshot_id", receipt.parent_snapshot_id ? TJson(*receipt.parent_snapshot_id) : TJson(nullptr)},
        {"rows_appended", receipt.rows_appended},
        {"data_files_added", receipt.data_files_added},
        {"compressed_data_bytes_added", receipt.compressed_data_bytes_added},
        {"snapshot_created_at_us", receipt.snapshot_created_at_us},
        {"durability_profile", receipt.durability_profile},
        {"receipt_version", receipt.receipt_version}};
}

const AppendResult& Committed(const AppendOutcome& outcome)
{
    if (outcome.disposition != CommitDisposition::Committed || !outcome.result) {
        throw std::runtime_error("append did not commit: " +
            (outcome.error ? outcome.error->code + ": " + outcome.error->message : "no error"));
    }
    return *outcome.result;
}

// The oracle parses returned keys, checks every payload byte and exact int64 /
// NULL value, and counts each expected duplicate. Its small multiplicity array
// is independent of scan order and does not retain source batches or scan rows.
uint64_t CheckRows(const std::shared_ptr<Engine>& engine, const TWorkload& workload, SnapshotSelector selector)
{
    auto cursor = Take(engine->Scan({.collection_id = CollectionID, .selector = selector, .batch_rows = 4096}));
    std::vector<uint8_t> counts(static_cast<size_t>((workload.Rows + 1) / 2), 0);
    uint64_t seen = 0;
    while (auto batch = Take(cursor->Next())) {
        Require(batch->schema()->Equals(*ArrowSchema()), "scan schema differs from the fixed typed source");
        const auto keys = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
        const auto payloads = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        const auto values = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            Require(!keys->IsNull(row) && !payloads->IsNull(row), "required source field became NULL");
            const auto key = keys->GetView(row);
            Require(key.size() == KeyBytes && key.starts_with("key:"), "unexpected source key");
            uint64_t pair = 0;
            const auto [end, error] = std::from_chars(key.data() + 4, key.data() + key.size(), pair);
            Require(error == std::errc{} && end == key.data() + key.size() && pair < counts.size(),
                "source key falls outside the exact expected domain");
            const auto expectedCount = std::min(uint64_t{2}, workload.Rows - 2 * pair);
            Require(++counts[pair] <= expectedCount, "duplicate multiplicity increased");
            Require(key == Key(pair), "key bytes differ");
            Require(payloads->GetView(row) == Payload(workload, pair), "payload bytes differ");
            Require(values->IsNull(row) == (pair % 7 == 0), "value NULL bitmap differs");
            if (!values->IsNull(row)) {
                Require(values->Value(row) == Value(pair), "precise signed int64 value differs");
            }
            ++seen;
        }
    }
    Require(seen == workload.Rows, "exact scan row count differs");
    for (size_t pair = 0; pair < counts.size(); ++pair) {
        Require(counts[pair] == std::min(uint64_t{2}, workload.Rows - 2 * pair), "expected duplicate was omitted");
    }
    return seen;
}

TJson Verify(const std::shared_ptr<Engine>& engine,
    const std::shared_ptr<TCountingFS>& fs,
    const TWorkload& workload,
    const TJson& receipts,
    bool bulk)
{
    const auto history = Take(engine->ListSnapshots(CollectionID));
    Require(history.snapshots.size() == receipts.size(), "snapshot count differs from the independent ledger");
    for (size_t index = 0; index < receipts.size(); ++index) {
        const auto& expected = receipts[index];
        const auto outcome =
            Take(engine->FindAppendOutcome(CollectionID, expected.at("operation_id").get<std::string>()));
        Require(outcome && outcome->disposition == CommitDisposition::Committed && outcome->result &&
                Receipt(*outcome->result) == expected,
            "permanent outcome differs from the original receipt");
        Require(Receipt(history.snapshots[index].receipt) == expected &&
                history.snapshots[index].snapshot_id == expected.at("snapshot_id"),
            "snapshot history differs from the original receipt");
    }
    CheckRows(engine, {0, workload.PayloadBytes, workload.ChunkRows}, SnapshotSelector::InitialEmpty());
    const auto rows = receipts.empty() ? 0 : workload.Rows;
    CheckRows(engine, {rows, workload.PayloadBytes, workload.ChunkRows}, SnapshotSelector::Latest());
    uint64_t historicalRows = 0;
    TJson retryIO = fs->Counters();
    retryIO = Difference(retryIO, {retryIO});
    double retryMs = 0;
    if (!receipts.empty()) {
        const auto& first = receipts[0];
        historicalRows = first.at("rows_appended").get<uint64_t>();
        CheckRows(engine,
            {historicalRows, workload.PayloadBytes, workload.ChunkRows},
            SnapshotSelector::Id(first.at("snapshot_id").get<int64_t>()));
        const TCounterBaseline before{fs->Counters()};
        const auto started = TClock::now();
        AppendOutcome retried;
        // A committed ID must return its old receipt even though NoSnapshot is
        // now false. Different chunking exercises canonical replay identity.
        if (bulk) {
            auto source = std::make_shared<TGeneratedSource>(
                TWorkload{historicalRows, workload.PayloadBytes, std::max(uint64_t{1}, workload.ChunkRows / 2 + 1)});
            retried = engine->BulkAppendDurable({CollectionID,
                first.at("operation_id").get<std::string>(),
                {SnapshotPreconditionKind::NoSnapshot},
                static_cast<int64_t>(historicalRows),
                source});
            Require(source->Metrics().at("source_passes") == 1, "successful retry repeated production");
        } else {
            retried = engine->AppendDurable({CollectionID,
                first.at("operation_id").get<std::string>(),
                {SnapshotPreconditionKind::NoSnapshot},
                {Batch(workload, 0, historicalRows)}});
        }
        retryMs = Milliseconds(started);
        retryIO = Difference(fs->Counters(), before);
        Require(Receipt(Committed(retried)) == first, "exact retry changed the permanent receipt");
        Require(retryIO.at("written_bytes") == 0 && retryIO.at("read_opens") == 0 && retryIO.at("file_syncs") == 0 &&
                retryIO.at("directory_syncs") == 0,
            "healthy retry performed external-file I/O");
        Require(Take(engine->ListSnapshots(CollectionID)).snapshots.size() == receipts.size(),
            "retry published an extra snapshot");
    }
    return {{"exact_rows_verified", rows},
        {"initial_empty_verified", true},
        {"historical_rows_verified", historicalRows},
        {"snapshot_count", history.snapshots.size()},
        {"receipt_count", receipts.size()},
        {"receipts", receipts},
        {"retry_receipt_verified", !receipts.empty()},
        {"retry_ms", retryMs},
        {"retry_io", retryIO}};
}

TJson StorageMetrics(const std::shared_ptr<Engine>& engine,
    const std::shared_ptr<detail::TLocalStorageBackend>& backend,
    const std::filesystem::path& root,
    bool bulk,
    uint64_t rows)
{
    const auto listing = Take(engine->ListFiles(CollectionID));
    uint64_t groups = 0, footerRows = 0, footerBytes = 0;
    TJson files = TJson::array();
    for (const auto& file : listing.Files) {
        const auto details = Take(engine->GetFileDetails(CollectionID, file.FileID));
        Require(file.RecordCount > 0 && file.RecordCount <= (bulk ? 100000 : 10000) && file.CompressedBytes > 0 &&
                file.CompressedBytes <= (32LL << 20) && !details.RowGroups.empty(),
            "actual data-file footer exceeds the fixed profile");
        uint64_t fileRows = 0;
        TJson rowGroups = TJson::array();
        for (const auto& group : details.RowGroups) {
            Require(group.RecordCount > 0 && group.Columns.size() == 3, "invalid actual Parquet row group");
            fileRows += static_cast<uint64_t>(group.RecordCount);
            rowGroups.push_back({{"rows", group.RecordCount},
                {"uncompressed_bytes", group.UncompressedBytes},
                {"compressed_bytes", group.CompressedBytes}});
        }
        Require(fileRows == static_cast<uint64_t>(file.RecordCount), "footer rows differ from manifest membership");
        footerRows += fileRows;
        footerBytes += static_cast<uint64_t>(details.FooterBytes);
        groups += details.RowGroups.size();
        files.push_back({{"rows", fileRows},
            {"file_bytes", file.CompressedBytes},
            {"footer_bytes", details.FooterBytes},
            {"row_groups", rowGroups}});
    }
    Require(footerRows == rows, "actual footer row total differs from the independently generated input");
    auto cursor = Take(engine->Scan({.collection_id = CollectionID, .limit = 0}));
    const auto state = Take(backend->VerifiedScanState(cursor->Info().identity));
    const auto& usage = state->Usage;
    uint64_t physicalMetadata = 0, physicalData = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        if (entry.is_regular_file()) {
            if (entry.path().extension() == ".parquet") {
                physicalData += entry.file_size();
            } else {
                physicalMetadata += entry.file_size();
            }
        }
    }
    return {{"data_files", listing.Files.size()},
        {"row_groups", groups},
        {"footer_rows", footerRows},
        {"footer_bytes", footerBytes},
        {"files", files},
        {"reachable_metadata_bytes", usage.RootBytes + usage.ManifestBytes + usage.ManifestListBytes},
        {"root_metadata_bytes", usage.RootBytes},
        {"manifest_bytes", usage.ManifestBytes},
        {"manifest_list_bytes", usage.ManifestListBytes},
        {"physical_metadata_file_bytes", physicalMetadata},
        {"physical_data_file_bytes", physicalData},
        {"catalog_bytes", std::filesystem::file_size(root / "catalog.sqlite")}};
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc == 8,
        "usage: glamdb_bulk_ingestion_bench ingest|reopen|crash-source|crash-data|crash-reopen "
        "ordinary|bulk ROOT LEDGER ROWS PAYLOAD_BYTES CHUNK_ROWS");
    const std::string command(argv[1]), method(argv[2]);
    const bool create = command == "ingest" || command == "crash-source" || command == "crash-data";
    Require(create || command == "reopen" || command == "crash-reopen", "unknown command");
    Require(method == "ordinary" || method == "bulk", "unknown ingestion method");
    const bool bulk = method == "bulk";
    Require(bulk || (command != "crash-source" && command != "crash-data" && command != "crash-reopen"),
        "crash probes require bulk ingestion");
    const std::filesystem::path root = std::filesystem::absolute(argv[3]);
    const std::filesystem::path ledgerPath = std::filesystem::absolute(argv[4]);
    const TWorkload workload{Count(argv[5], 25600000), Count(argv[6], 1048576), Count(argv[7], 10000)};
    Require(workload.PayloadBytes >= 16 && workload.ChunkRows * (workload.PayloadBytes + 64) <= (16ULL << 20) - 4096 &&
            CanonicalRows(workload) <= (4ULL << 30),
        "workload exceeds the fixed source or canonical byte bound");
    Require(bulk || (workload.Rows + workload.ChunkRows - 1) / workload.ChunkRows <= 4096,
        "ordinary campaign exceeds the permanent operation/history bound");
    auto ledger = create ? TJson{{"method", method},
                               {"rows", workload.Rows},
                               {"payload_bytes", workload.PayloadBytes},
                               {"chunk_rows", workload.ChunkRows},
                               {"receipts", TJson::array()}}
                         : ReadJson(ledgerPath);
    Require(ledger.at("method") == method && ledger.at("rows") == workload.Rows &&
            ledger.at("payload_bytes") == workload.PayloadBytes && ledger.at("chunk_rows") == workload.ChunkRows,
        "reopen workload differs from the independent ledger");
    Require(!create || !std::filesystem::exists(root), "ingestion requires a new store path");
    auto fs = std::make_shared<TCountingFS>();
    auto owner = std::make_shared<ResourceOwner>();
    std::shared_ptr<detail::TLocalStorageBackend> backend;
    const detail::BackendFactory factory = [&](const auto& path,
                                               const auto& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        detail::TLocalStorageOptions options;
        options.Fs = fs;
        options.Resources = owner;
        auto opened = detail::TLocalStorageBackend::Open(path, info, options);
        if (!opened) {
            return std::unexpected(opened.error());
        }
        backend = *opened;
        return backend;
    };
    const auto openStarted = TClock::now();
    auto engine = Take(create
            ? detail::EngineFactory::CreateWith({StoreID, root.string(), "local-delete-extra-v1", {}, owner}, factory)
            : detail::EngineFactory::OpenWith({root.string(), StoreID, "local-delete-extra-v1", {}, owner}, factory));
    const auto openMs = Milliseconds(openStarted);
    const auto openIO = fs->Counters();
    if (create) {
        const SchemaSpec schema{{{"key", FieldType::String, false},
            {"payload", FieldType::String, false},
            {"value", FieldType::Int64, true}}};
        Require(engine->CreateCollection({CollectionID, "bulk measurement", schema}).disposition ==
                CommitDisposition::Committed,
            "create collection failed");
        WriteJson(ledgerPath, ledger);
    }
    TJson metrics{{"command", command},
        {"method", method},
        {"rows", workload.Rows},
        {"payload_bytes", workload.PayloadBytes},
        {"source_chunk_rows", workload.ChunkRows},
        {"input_canonical_bytes", CanonicalRows(workload) + CanonicalHeaderBytes},
        {"canonical_row_bytes", CanonicalRows(workload)},
        {"open_ms", openMs},
        {"open_io", openIO},
        {"sync_counter_scope", "external files only; SQLite VFS is excluded"},
        {"reopen_cache_state", "fresh process; operating-system caches are not evicted"}};
    const TCounterBaseline ioBefore{fs->Counters()};
    const auto ingestStarted = TClock::now();
    auto ingestionReturned = ingestStarted;
    std::optional<TClock::time_point> productionEOF;
    if (create) {
        auto source = std::make_shared<TGeneratedSource>(workload, command == "crash-source");
        if (command == "crash-data") {
            fs->CrashAfterDataSync();
        }
        if (bulk) {
            const auto outcome = engine->BulkAppendDurable({CollectionID,
                OperationID(0),
                {SnapshotPreconditionKind::NoSnapshot},
                static_cast<int64_t>(workload.Rows),
                source});
            ingestionReturned = TClock::now();
            const auto& receipt = Committed(outcome);
            Require(receipt.receipt_version == 2, "bulk append did not return a version 2 receipt");
            ledger["receipts"].push_back(Receipt(receipt));
            productionEOF = source->ProductionEOF();
            Require(productionEOF.has_value(), "bulk append returned before complete production EOF");
            metrics["final_ack_boundary"] = "second-pass source EOF callback to BulkAppendDurable return";
        } else {
            Require(source->Rewind().has_value(), "rewind ordinary source failed");
            for (uint64_t position = 0; position < workload.Rows;) {
                auto batch = Take(source->Next());
                Require(batch != nullptr, "ordinary source ended early");
                position += static_cast<uint64_t>(batch->num_rows());
                if (position == workload.Rows) {
                    productionEOF = TClock::now();
                }
                const auto outcome =
                    engine->AppendDurable({CollectionID, OperationID(ledger["receipts"].size()), {}, {batch}});
                ingestionReturned = TClock::now();
                const auto& receipt = Committed(outcome);
                Require(receipt.receipt_version == 1 && receipt.data_files_added == 1,
                    "ordinary append changed its version 1 one-file contract");
                ledger["receipts"].push_back(Receipt(receipt));
            }
            Require(Take(source->Next()) == nullptr, "ordinary source exceeds its declaration");
            metrics["final_ack_boundary"] = "last ordinary batch generated to final AppendDurable return";
        }
        if (!productionEOF) {
            throw std::runtime_error("missing production EOF");
        }
        metrics["ingest_ms"] = Milliseconds(ingestStarted, ingestionReturned);
        metrics["final_ack_ms"] = Milliseconds(*productionEOF, ingestionReturned);
        metrics["ingest_mib_per_second"] = static_cast<double>(CanonicalRows(workload) + CanonicalHeaderBytes) /
            (1024 * 1024) / (metrics["ingest_ms"].get<double>() / 1000);
        metrics["ingest_rss_peak_bytes"] = PeakRSS();
        metrics.update(source->Metrics());
        if (bulk) {
            const auto& passes = metrics.at("source_pass_metrics");
            Require(passes.size() == 2 && metrics.at("source_rows_read") == 2 * workload.Rows,
                "bulk append did not consume two complete source passes");
            metrics["validation_ms"] = passes[0].at("elapsed_ms");
            metrics["production_ms"] = passes[1].at("elapsed_ms");
            metrics["validation_mib_per_second"] = static_cast<double>(CanonicalRows(workload) + CanonicalHeaderBytes) /
                (1024 * 1024) / (metrics["validation_ms"].get<double>() / 1000);
        }
        WriteJson(ledgerPath, ledger);
    }
    metrics["io"] = Difference(fs->Counters(), ioBefore);
    metrics["operation_canonical_bytes"] = ledger.at("receipts").empty()
        ? 0
        : CanonicalRows(workload) + CanonicalHeaderBytes * ledger.at("receipts").size();
    const auto verifyStarted = TClock::now();
    metrics.update(Verify(engine, fs, workload, ledger.at("receipts"), bulk));
    if (command == "crash-reopen") {
        const auto failed = Take(engine->FindAppendOutcome(CollectionID, OperationID(0)));
        if (!failed || failed->disposition != CommitDisposition::DefinitelyNotCommitted || !failed->error ||
            failed->error->code != "INTERRUPTED")
        {
            throw std::runtime_error("interrupted bound operation did not become a permanent definite failure");
        }
        const auto retried = engine->BulkAppendDurable({CollectionID,
            OperationID(0),
            {},
            static_cast<int64_t>(workload.Rows),
            std::make_shared<TGeneratedSource>(workload)});
        Require(retried == *failed, "interrupted operation retry changed its permanent failure");
        Require(Take(engine->ListSnapshots(CollectionID)).snapshots.empty(), "failed retry published a snapshot");
        metrics["failure_code"] = failed->error->code;
        metrics["failed_retry_verified"] = true;
    }
    metrics["verification_ms"] = Milliseconds(verifyStarted);
    metrics.update(StorageMetrics(engine, backend, root, bulk, ledger.at("receipts").empty() ? 0 : workload.Rows));
    metrics["resource_memory_peak_bytes"] = owner->GetUsage().memory_peak;
    metrics["rss_peak_bytes"] = PeakRSS();
    Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
    metrics["verified"] = true;
    std::cout << metrics.dump() << '\n';
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
