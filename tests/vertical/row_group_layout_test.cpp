#include "tests/support/checked.h"
#include "tests/vertical/vertical_support.h"

#include <arrow/util/byte_size.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include <arrow/memory_pool.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <unistd.h>
#ifdef __linux__
#include <spawn.h>
#include <sys/resource.h>
#include <sys/wait.h>

extern char** environ;
#endif

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace vertical;

// Issue #102 fixtures: required key and payload strings.
const SchemaSpec PayloadSchema{{
    {"key", FieldType::String, false},
    {"payload", FieldType::String, true},
}};

struct TPayloadRow
{
    std::string Key;
    std::optional<std::string> Payload;
};

// Builders reserve exact capacity, so building the input has no transient
// reallocation peak that could hide the append peak in pool statistics.
std::shared_ptr<arrow::RecordBatch> PayloadBatch(const std::vector<TPayloadRow>& rows)
{
    arrow::StringBuilder keys;
    arrow::StringBuilder payloads;
    int64_t keyBytes = 0;
    int64_t payloadBytes = 0;
    for (const auto& row : rows) {
        keyBytes += static_cast<int64_t>(row.Key.size());
        payloadBytes += row.Payload ? static_cast<int64_t>(row.Payload->size()) : 0;
    }
    const auto count = static_cast<int64_t>(rows.size());
    EXPECT_TRUE(keys.Reserve(count).ok());
    EXPECT_TRUE(keys.ReserveData(keyBytes).ok());
    EXPECT_TRUE(payloads.Reserve(count).ok());
    EXPECT_TRUE(payloads.ReserveData(payloadBytes).ok());
    for (const auto& row : rows) {
        keys.UnsafeAppend(row.Key);
        if (row.Payload) {
            payloads.UnsafeAppend(*row.Payload);
        } else {
            payloads.UnsafeAppendNull();
        }
    }
    std::shared_ptr<arrow::Array> keyArray;
    std::shared_ptr<arrow::Array> payloadArray;
    EXPECT_TRUE(keys.Finish(&keyArray).ok());
    EXPECT_TRUE(payloads.Finish(&payloadArray).ok());
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("payload", arrow::utf8())}),
        count,
        {keyArray, payloadArray});
}

std::string Key(size_t row)
{
    char text[16];
    std::snprintf(text, sizeof(text), "k%05zu", row);
    return text;
}

// Both occupy 1,058,575 payload bytes in 10,000 rows.
std::vector<TPayloadRow> UniformRows()
{
    std::vector<TPayloadRow> rows;
    for (size_t row = 0; row < 10000; ++row) {
        rows.push_back({Key(row), std::string(row < 8575 ? 106 : 105, 'x')});
    }
    return rows;
}

std::vector<TPayloadRow> SkewedRows()
{
    std::vector<TPayloadRow> rows;
    for (size_t row = 0; row < 10000; ++row) {
        rows.push_back({Key(row), std::string(row == 0 ? (1U << 20) : 1U, 'x')});
    }
    return rows;
}

// Duplicate keys, NULL payloads and three 40 KiB values at the start, middle
// and end; about 3.5 times a 64 KiB row group bound.
std::vector<TPayloadRow> MixedRows()
{
    std::vector<TPayloadRow> rows;
    for (size_t row = 0; row < 3000; ++row) {
        std::optional<std::string> payload;
        if (row == 0 || row == 1500 || row == 2999) {
            payload = std::string(40 << 10, static_cast<char>('a' + row % 26));
        } else if (row % 5 != 0) {
            payload = std::string(row % 13, static_cast<char>('a' + row % 26));
        }
        rows.push_back({"dup-" + std::to_string(row % 7), std::move(payload)});
    }
    return rows;
}

// Documented writer accounting: per string field a validity byte, four offset
// bytes and the value bytes; plus one trailing offset per string field per group.
int64_t RowBytes(const TPayloadRow& row)
{
    return 5 + static_cast<int64_t>(row.Key.size()) + 5 + (row.Payload ? static_cast<int64_t>(row.Payload->size()) : 0);
}

struct TFooter
{
    std::vector<int64_t> GroupRows;
    int64_t FooterBytes = 0;
    int64_t FileBytes = 0;
};

TFooter ReadFooter(const std::filesystem::path& path)
{
    const auto footer = parquet::ParquetFileReader::OpenFile(path.string())->metadata();
    TFooter result;
    result.FooterBytes = footer->size();
    result.FileBytes = static_cast<int64_t>(std::filesystem::file_size(path));
    for (int group = 0; group < footer->num_row_groups(); ++group) {
        result.GroupRows.push_back(footer->RowGroup(group)->num_rows());
    }
    return result;
}

void ExpectGroupsWithin(const std::vector<TPayloadRow>& rows, const std::vector<int64_t>& groups, int64_t cap)
{
    size_t next = 0;
    for (size_t group = 0; group < groups.size(); ++group) {
        int64_t bytes = 8;
        for (int64_t row = 0; row < groups[group]; ++row) {
            bytes += RowBytes(rows.at(next++));
        }
        EXPECT_LE(bytes, cap) << "row group " << group;
    }
    EXPECT_EQ(next, rows.size());
}

// Runs one test of this executable in a fresh process with extra environment
// variables and returns its exit status; the GoogleTest result must be success.
void RunChild(const std::string& test, const std::vector<std::string>& extra)
{
#ifdef __linux__
    std::vector<std::string> environment;
    for (auto** entry = environ; *entry; ++entry) {
        if (!std::string_view(*entry).starts_with("GLAMDB_RGL_")) {
            environment.emplace_back(*entry);
        }
    }
    environment.insert(environment.end(), extra.begin(), extra.end());
    std::vector<char*> variables;
    for (auto& value : environment) {
        variables.push_back(value.data());
    }
    variables.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=" + test;
    char* arguments[]{executable.data(), filter.data(), nullptr};
    pid_t child = 0;
    ASSERT_EQ(::posix_spawn(&child, executable.c_str(), nullptr, nullptr, arguments, variables.data()), 0);
    int status = 0;
    pid_t waited;
    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0) << test;
#else
    static_cast<void>(test);
    static_cast<void>(extra);
    GTEST_SKIP() << "Fresh-process checks run on Linux.";
#endif
}

constexpr int64_t MixedRowGroupBytes = 64 << 10;

AppendRequest MixedRequest(const std::shared_ptr<arrow::RecordBatch>& batch)
{
    return {test::collection_id, FirstOperation, {}, {batch->Slice(0, 1000), batch->Slice(1000)}};
}

std::vector<std::string> ScanInOrder(Engine& engine)
{
    auto cursor = engine.Scan({.collection_id = test::collection_id, .batch_rows = 997});
    EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
    std::vector<std::string> rows;
    while (cursor) {
        const auto batch = (*cursor)->Next();
        EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
        if (!batch || !*batch) {
            break;
        }
        const auto rendered = Render(**batch);
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
    return rows;
}

// Independent expectations for the committed mixed fixture: footer layout and
// per-group bound, exact rows in input order, manifest statistics and
// per-group file details. Returns the listed file metadata.
TDataFileMetadata ExpectMixedState(Engine& engine, const std::filesystem::path& root)
{
    const auto rows = MixedRows();
    const auto dataFiles = FilesIn(root / "warehouse" / test::collection_id / "data");
    EXPECT_EQ(dataFiles.size(), 1U);
    if (dataFiles.size() != 1) {
        return {};
    }
    const auto footer = ReadFooter(dataFiles.front());
    // The widest-row rule produced one group per row here.
    EXPECT_GE(footer.GroupRows.size(), 2U);
    EXPECT_LE(footer.GroupRows.size(), 4U);
    ExpectGroupsWithin(rows, footer.GroupRows, MixedRowGroupBytes);
    EXPECT_EQ(ScanInOrder(engine), Render(*PayloadBatch(rows)));

    const auto files = engine.ListFiles(test::collection_id);
    EXPECT_TRUE(files) << files.error().message;
    if (!files || files->Files.size() != 1) {
        ADD_FAILURE() << "expected one listed data file";
        return {};
    }
    const auto& file = files->Files.front();
    EXPECT_EQ(file.RecordCount, 3000);
    EXPECT_EQ(file.CompressedBytes, footer.FileBytes);
    EXPECT_EQ(file.FieldMetrics.size(), 2U);
    if (file.FieldMetrics.size() != 2) {
        return file;
    }
    EXPECT_EQ(file.FieldMetrics[0].ValueCount, 3000);
    EXPECT_EQ(file.FieldMetrics[0].NullCount, 0);
    const auto bytes = [](std::string_view text) {
        return std::vector<uint8_t>(text.begin(), text.end());
    };
    EXPECT_EQ(file.FieldMetrics[0].LowerBound, bytes("dup-0"));
    EXPECT_EQ(file.FieldMetrics[0].UpperBound, bytes("dup-6"));
    EXPECT_EQ(file.FieldMetrics[1].ValueCount, 3000);
    EXPECT_EQ(file.FieldMetrics[1].NullCount, std::ranges::count_if(rows, [](const auto& row) {
        return !row.Payload;
    }));
    const auto details = engine.GetFileDetails(test::collection_id, file.FileID);
    EXPECT_TRUE(details) << details.error().message;
    if (details) {
        EXPECT_EQ(details->RowGroups.size(), footer.GroupRows.size());
        for (size_t group = 0; group < std::min(footer.GroupRows.size(), details->RowGroups.size()); ++group) {
            EXPECT_EQ(details->RowGroups[group].RecordCount, footer.GroupRows[group]);
        }
    }
    return file;
}

// Serializes listed metadata so a fresh process can compare persisted statistics exactly.
std::string Describe(const TDataFileMetadata& file)
{
    std::string text = std::format("{}|{}|{}|{}", file.FileID, file.Format, file.RecordCount, file.CompressedBytes);
    const auto optional = [](const std::optional<int64_t>& value) {
        return value ? std::to_string(*value) : std::string("-");
    };
    const auto bound = [](const std::optional<std::vector<uint8_t>>& value) {
        std::string hex = value ? "" : "-";
        for (const auto byte : value.value_or(std::vector<uint8_t>{})) {
            hex += std::format("{:02x}", byte);
        }
        return hex;
    };
    for (const auto& metric : file.FieldMetrics) {
        text += std::format("|{}:{}:{}:{}:{}:{}:{}",
            metric.FieldID,
            optional(metric.CompressedBytes),
            optional(metric.ValueCount),
            optional(metric.NullCount),
            optional(metric.NanCount),
            bound(metric.LowerBound),
            bound(metric.UpperBound));
    }
    return text;
}

// Fresh-process reopen of the mixed fixture: same rows, statistics, and the
// original operation's retry returns its persisted receipt without new files.
TEST(RowGroupLayoutReopenChild, ReadsAndRetriesMixedLayout)
{
    const auto* root = std::getenv("GLAMDB_RGL_ROOT");
    const auto* snapshot = std::getenv("GLAMDB_RGL_SNAPSHOT");
    const auto* listed = std::getenv("GLAMDB_RGL_FILE");
    const auto* added = std::getenv("GLAMDB_RGL_ADDED");
    if (!root || !snapshot || !listed || !added) {
        GTEST_SKIP() << "Invoked by MixedLayoutSurvivesScanFreshProcessReopenAndRetry.";
    }
    detail::TLocalStorageOptions options;
    options.Writer.MaxRowGroupBytes = MixedRowGroupBytes;
    std::shared_ptr<THookBackend> hooks;
    auto opened = detail::EngineFactory::OpenWith({.absolute_root = root, .expected_store_id = test::store_id},
        THookBackend::Factory(hooks, options));
    ASSERT_TRUE(opened) << opened.error().message;
    auto engine = std::move(*opened);
    EXPECT_EQ(Describe(ExpectMixedState(*engine, root)), listed);
    const auto retried = engine->AppendDurable(MixedRequest(PayloadBatch(MixedRows())));
    ASSERT_EQ(retried.disposition, CommitDisposition::Committed)
        << (retried.error ? retried.error->message : std::string());
    ASSERT_TRUE(retried.result);
    EXPECT_EQ(std::to_string(retried.result->snapshot_id), snapshot);
    EXPECT_EQ(std::to_string(retried.result->compressed_data_bytes_added), added);
    EXPECT_EQ(FilesIn(std::filesystem::path(root) / "warehouse" / test::collection_id / "data").size(), 1U);
    EXPECT_EQ(Describe(ExpectMixedState(*engine, root)), listed);
    EXPECT_TRUE(engine->Shutdown(1000ms));
}

class RowGroupLayoutTest : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::filesystem::path Root_ = Temporary_.path / "store";
    std::shared_ptr<THookBackend> Hooks_;
    std::shared_ptr<Engine> Engine_;
    detail::TLocalStorageOptions Options_;

    void TearDown() override
    {
        if (Engine_) {
            EXPECT_TRUE(Engine_->Shutdown(1000ms));
        }
    }

    void Create(const SchemaSpec& schema = PayloadSchema)
    {
        auto created = detail::EngineFactory::CreateWith({test::store_id, Root_.string()},
            THookBackend::Factory(Hooks_, Options_));
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = std::move(*created);
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", schema}).disposition,
            CommitDisposition::Committed);
    }

    std::filesystem::path OnlyDataFile() const
    {
        const auto files = FilesIn(Root_ / "warehouse" / test::collection_id / "data");
        EXPECT_EQ(files.size(), 1U);
        return files.empty() ? std::filesystem::path() : files.front();
    }
};

TEST_F(RowGroupLayoutTest, EqualByteUniformAndSkewedAppendsEachFormOneGroup)
{
    const auto uniform = PayloadBatch(UniformRows());
    const auto skewed = PayloadBatch(SkewedRows());
    ASSERT_EQ(arrow::util::TotalBufferSize(*uniform), arrow::util::TotalBufferSize(*skewed));
    ASSERT_NO_FATAL_FAILURE(Create());
    ASSERT_EQ(Engine_->CreateCollection({SecondCollection, "skewed", PayloadSchema}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {uniform}}).disposition,
        CommitDisposition::Committed);
    ASSERT_EQ(Engine_->AppendDurable({SecondCollection, FirstOperation, {}, {skewed}}).disposition,
        CommitDisposition::Committed);
    const auto cap = Options_.Writer.MaxRowGroupBytes;
    for (const auto* collection : {test::collection_id, SecondCollection}) {
        const auto files = FilesIn(Root_ / "warehouse" / collection / "data");
        ASSERT_EQ(files.size(), 1U);
        const auto footer = ReadFooter(files.front());
        EXPECT_EQ(footer.GroupRows, std::vector<int64_t>{10000}) << collection;
        ExpectGroupsWithin(collection == SecondCollection ? SkewedRows() : UniformRows(), footer.GroupRows, cap);
        // Previously 278,488 footer bytes for the skewed file.
        EXPECT_LT(footer.FooterBytes, 4096) << collection;
    }
}

// Exact rows, order, duplicates, NULLs and manifest statistics survive scan,
// an old-operation retry and a reopen in a fresh process with a multi-group
// adaptive layout.
TEST_F(RowGroupLayoutTest, MixedLayoutSurvivesScanFreshProcessReopenAndRetry)
{
    Options_.Writer.MaxRowGroupBytes = MixedRowGroupBytes;
    ASSERT_NO_FATAL_FAILURE(Create());
    const auto request = MixedRequest(PayloadBatch(MixedRows()));
    const auto committed = Engine_->AppendDurable(request);
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed)
        << (committed.error ? committed.error->message : std::string());
    ASSERT_TRUE(committed.result);
    const auto file = ExpectMixedState(*Engine_, Root_);
    EXPECT_EQ(Engine_->AppendDurable(request), committed);
    ASSERT_TRUE(Engine_->Shutdown(1000ms));
    Engine_.reset();
    ASSERT_NO_FATAL_FAILURE(RunChild("RowGroupLayoutReopenChild.ReadsAndRetriesMixedLayout",
        {"GLAMDB_RGL_ROOT=" + Root_.string(),
            "GLAMDB_RGL_SNAPSHOT=" + std::to_string(committed.result->snapshot_id),
            "GLAMDB_RGL_ADDED=" + std::to_string(committed.result->compressed_data_bytes_added),
            "GLAMDB_RGL_FILE=" + Describe(file)}));
}

// The backend admission and writer still reject a row whose estimate plus
// string offsets exceeds the bound, without binding the operation.
TEST_F(RowGroupLayoutTest, RowOverTheBoundIsStillRefused)
{
    std::vector<TPayloadRow> rows{{"a", "x"}, {"b", std::string(1000, 'y')}};
    Options_.Writer.MaxRowGroupBytes = RowBytes(rows[1]) + 8 - 1;
    ASSERT_NO_FATAL_FAILURE(Create());
    const auto refused = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {PayloadBatch(rows)}});
    ASSERT_TRUE(refused.error);
    EXPECT_EQ(test::Present(refused.error).code, "RESOURCE_EXHAUSTED");
    EXPECT_EQ(refused.phase, CommitPhase::Validation);
    const auto found = Engine_->FindAppendOutcome(test::collection_id, FirstOperation);
    ASSERT_TRUE(found);
    EXPECT_FALSE(found->has_value());
    rows[1].Payload->pop_back();
    const auto committed = Engine_->AppendDurable({test::collection_id, FirstOperation, {}, {PayloadBatch(rows)}});
    ASSERT_EQ(committed.disposition, CommitDisposition::Committed);
    EXPECT_EQ(ReadFooter(OnlyDataFile()).GroupRows, (std::vector<int64_t>{1, 1}));
}

// Measurement fixtures. Near-cap inputs fill one row group to within 0.2% of
// the default 8 MiB bound with pseudo-random letters (not run-length trivial).
std::string Letters(size_t length, uint64_t seed)
{
    std::string text(length, 'a');
    for (auto& character : text) {
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        character = static_cast<char>('a' + (seed >> 33) % 26);
    }
    return text;
}

struct TMeasureFixture
{
    SchemaSpec Schema;
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::string SelectiveKey;
};

std::optional<TMeasureFixture> MeasureFixture(std::string_view name)
{
    std::vector<TPayloadRow> rows;
    if (name == "uniform") {
        rows = UniformRows();
    } else if (name == "skewed") {
        rows = SkewedRows();
    } else if (name == "ordinary") {
        std::vector<TInputRow> ordinary;
        for (int64_t row = 0; row < 10000; ++row) {
            ordinary.push_back({"key-" + std::to_string(row % 4000),
                row % 3 == 0 ? std::nullopt : std::optional<int64_t>(row),
                row % 7 == 0 ? std::nullopt : std::optional<double>(row * 0.5),
                row % 2 == 0,
                row % 11 == 0 ? std::nullopt : std::optional<int64_t>(row * 1000)});
        }
        return TMeasureFixture{Schema, Batch(ordinary), "key-1999"};
    } else if (name == "wide-varied") {
        for (size_t row = 0; row < 10000; ++row) {
            rows.push_back({Key(row), std::string(700 + (row * 7919) % 300, static_cast<char>('a' + row % 26))});
        }
    } else if (name == "near-cap-uniform") {
        // 10,000 rows of 838 accounted bytes: 8,380,008 of 8,388,608.
        for (size_t row = 0; row < 10000; ++row) {
            rows.push_back({Key(row), Letters(822, row)});
        }
    } else if (name == "near-cap-skewed") {
        // One 1 MiB value and 9,999 rows of 733 bytes: 8,377,867 of 8,388,608.
        for (size_t row = 0; row < 10000; ++row) {
            rows.push_back({Key(row), Letters(row == 0 ? (1U << 20) : 717U, row)});
        }
    } else {
        return std::nullopt;
    }
    return TMeasureFixture{PayloadSchema, PayloadBatch(rows), Key(9999)};
}

int64_t CurrentRssKiB()
{
    std::ifstream statm("/proc/self/statm");
    int64_t size = 0;
    int64_t resident = 0;
    statm >> size >> resident;
    return resident * ::sysconf(_SC_PAGESIZE) / 1024;
}

int64_t PeakRssKiB()
{
#ifdef __linux__
    rusage usage{};
    ::getrusage(RUSAGE_SELF, &usage);
    return usage.ru_maxrss;
#else
    return 0;
#endif
}

double Milliseconds(std::chrono::steady_clock::duration duration)
{
    return std::chrono::duration<double, std::milli>(duration).count();
}

int64_t DrainRows(Engine& engine, ScanRequest request)
{
    request.collection_id = test::collection_id;
    auto cursor = engine.Scan(request);
    EXPECT_TRUE(cursor) << (cursor ? std::string() : cursor.error().message);
    int64_t rows = 0;
    while (cursor) {
        const auto batch = (*cursor)->Next();
        EXPECT_TRUE(batch) << (batch ? std::string() : batch.error().message);
        if (!batch || !*batch) {
            break;
        }
        rows += (*batch)->num_rows();
    }
    return rows;
}

// One measured phase in a fresh process, so Arrow pool and RSS peaks belong
// to that phase only. "write" creates the store and appends; "scan" reopens it
// and runs a full scan, then a selective key predicate with a key projection.
TEST(RowGroupLayoutMeasurementChild, Phase)
{
    const auto* phase = std::getenv("GLAMDB_RGL_PHASE");
    const auto* name = std::getenv("GLAMDB_RGL_FIXTURE");
    const auto* root = std::getenv("GLAMDB_RGL_ROOT");
    const auto* output = std::getenv("GLAMDB_RGL_OUT");
    if (!phase || !name || !root || !output) {
        GTEST_SKIP() << "Invoked by RowGroupLayoutMeasurement in a fresh process.";
    }
    const auto fixture = MeasureFixture(name);
    ASSERT_TRUE(fixture) << name;
    auto* pool = arrow::default_memory_pool();
    std::map<std::string, double> results;
    if (std::string_view(phase) == "write") {
        auto created = CreateStore({test::store_id, root});
        ASSERT_TRUE(created) << created.error().message;
        auto engine = *created;
        ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", fixture->Schema}).disposition,
            CommitDisposition::Committed);
        const auto live = pool->bytes_allocated();
        const auto earlierPeak = pool->max_memory();
        results["rss_before_kib"] = static_cast<double>(CurrentRssKiB());
        const auto started = std::chrono::steady_clock::now();
        const auto outcome = engine->AppendDurable({test::collection_id, FirstOperation, {}, {fixture->Batch}});
        results["write_ms"] = Milliseconds(std::chrono::steady_clock::now() - started);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed)
            << (outcome.error ? outcome.error->message : std::string());
        // The append peak is visible only if it exceeds every earlier peak.
        EXPECT_GT(pool->max_memory(), earlierPeak) << "append peak is hidden by an earlier allocation peak";
        results["write_pool_peak_mib"] = static_cast<double>(pool->max_memory() - live) / (1 << 20);
        results["write_rss_peak_mib"] = static_cast<double>(PeakRssKiB()) / 1024;
        const auto footer =
            ReadFooter(FilesIn(std::filesystem::path(root) / "warehouse" / test::collection_id / "data").front());
        results["groups"] = static_cast<double>(footer.GroupRows.size());
        results["max_group_rows"] = static_cast<double>(std::ranges::max(footer.GroupRows));
        results["footer_bytes"] = static_cast<double>(footer.FooterBytes);
        results["file_bytes"] = static_cast<double>(footer.FileBytes);
        ASSERT_TRUE(engine->Shutdown(1000ms));
    } else {
        auto opened = OpenExisting({root, test::store_id});
        ASSERT_TRUE(opened) << opened.error().message;
        auto engine = *opened;
        const auto live = pool->bytes_allocated();
        auto started = std::chrono::steady_clock::now();
        EXPECT_EQ(DrainRows(*engine, {}), fixture->Batch->num_rows());
        results["full_scan_ms"] = Milliseconds(std::chrono::steady_clock::now() - started);
        started = std::chrono::steady_clock::now();
        EXPECT_GE(DrainRows(*engine,
                      {.projection = std::vector<std::string>{"key"},
                          .predicate = Predicate::Compare("key", CompareOp::Eq, fixture->SelectiveKey)}),
            1);
        results["selective_scan_ms"] = Milliseconds(std::chrono::steady_clock::now() - started);
        results["scan_pool_peak_mib"] = static_cast<double>(pool->max_memory() - live) / (1 << 20);
        results["scan_rss_peak_mib"] = static_cast<double>(PeakRssKiB()) / 1024;
        ASSERT_TRUE(engine->Shutdown(1000ms));
    }
    std::ofstream out(output, std::ios::trunc);
    for (const auto& [key, value] : results) {
        out << key << ' ' << std::format("{:.3f}", value) << '\n';
    }
    ASSERT_TRUE(out.good());
}

// Strict GLAMDB_BENCH_RUNS parsing: absent selects 7; otherwise the whole value
// must be a decimal integer in 1..1000.
std::optional<int> BenchRuns(std::string& diagnostic)
{
    const auto* env = std::getenv("GLAMDB_BENCH_RUNS");
    if (!env) {
        return 7;
    }
    const std::string_view text(env);
    int runs = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), runs);
    if (text.empty() || error != std::errc() || end != text.data() + text.size() || runs < 1 || runs > 1000) {
        diagnostic = std::format("GLAMDB_BENCH_RUNS must be an integer in 1..1000, got '{}'", text);
        return std::nullopt;
    }
    return runs;
}

// Manual measurement, excluded from CTest (Linux only). Run with
//   glamdb_vertical --gtest_also_run_disabled_tests --gtest_filter='RowGroupLayoutMeasurement.*'
// GLAMDB_BENCH_RUNS sets the repetitions (default 7); GLAMDB_BENCH_FIXTURES
// optionally selects a comma-separated subset. Each run uses a fresh store and
// one fresh process per phase.
TEST(RowGroupLayoutMeasurement, DISABLED_AppendAndScan)
{
    std::string diagnostic;
    const auto runs = BenchRuns(diagnostic);
    ASSERT_TRUE(runs) << diagnostic;
    std::vector<std::string> names{"uniform",
        "skewed",
        "ordinary",
        "wide-varied",
        "near-cap-uniform",
        "near-cap-skewed"};
    if (const auto* selected = std::getenv("GLAMDB_BENCH_FIXTURES")) {
        names.clear();
        std::stringstream list(selected);
        for (std::string name; std::getline(list, name, ',');) {
            ASSERT_TRUE(MeasureFixture(name)) << "unknown fixture " << name;
            names.push_back(name);
        }
    }
    for (const auto& name : names) {
        std::map<std::string, std::vector<double>> samples;
        for (int run = 0; run < *runs; ++run) {
            test::TempDir temporary;
            const auto root = temporary.path / "store";
            for (const auto* phase : {"write", "scan"}) {
                const auto output = temporary.path / (std::string(phase) + ".txt");
                ASSERT_NO_FATAL_FAILURE(RunChild("RowGroupLayoutMeasurementChild.Phase",
                    {std::string("GLAMDB_RGL_PHASE=") + phase,
                        "GLAMDB_RGL_FIXTURE=" + name,
                        "GLAMDB_RGL_ROOT=" + root.string(),
                        "GLAMDB_RGL_OUT=" + output.string()}));
                std::ifstream input(output);
                std::string key;
                double value = 0;
                while (input >> key >> value) {
                    samples[key].push_back(value);
                }
            }
        }
        std::string line = std::format("MEASURE fixture={} runs={}", name, *runs);
        for (auto& [key, values] : samples) {
            ASSERT_EQ(values.size(), static_cast<size_t>(*runs)) << key;
            std::ranges::sort(values);
            line +=
                std::format(" {}={:.2f}[{:.2f},{:.2f}]", key, values[values.size() / 2], values.front(), values.back());
        }
        std::printf("%s\n", line.c_str());
    }
}

}
}
