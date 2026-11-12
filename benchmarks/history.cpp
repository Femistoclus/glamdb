// Manual ordinary-append acceptance: deterministic TPC-H .tbl files are the
// independent row oracle. No bulk producer, mock backend or receipt expiration.
#include "benchmarks/history_oracle.h"
#include "core/api/codec.h"
#include "storage/engine.h"
#include "storage/resources.h"

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/stat.h>

namespace {
using namespace glamdb;
using TJson = nlohmann::json;
using TClock = std::chrono::steady_clock;
constexpr auto StoreID = "15600000-0000-4000-8000-000000000001";
struct TTable
{
    const char* Name;
    const char* Fields;
};
constexpr std::array Tables{TTable{"region", "r_regionkey:I,r_name:S,r_comment:S"},
    TTable{"nation", "n_nationkey:I,n_name:S,n_regionkey:I,n_comment:S"},
    TTable{"supplier", "s_suppkey:I,s_name:S,s_address:S,s_nationkey:I,s_phone:S,s_acctbal:D,s_comment:S"},
    TTable{"customer",
        "c_custkey:I,c_name:S,c_address:S,c_nationkey:I,c_phone:S,c_acctbal:D,c_mktsegment:S,c_comment:S"},
    TTable{"part",
        "p_partkey:I,p_name:S,p_mfgr:S,p_brand:S,p_type:S,p_size:I,p_container:S,p_retailprice:D,p_comment:S"},
    TTable{"partsupp", "ps_partkey:I,ps_suppkey:I,ps_availqty:I,ps_supplycost:D,ps_comment:S"},
    TTable{"orders",
        "o_orderkey:I,o_custkey:I,o_orderstatus:S,o_totalprice:D,o_orderdate:S,o_orderpriority:S,o_clerk:S,o_"
        "shippriority:I,o_comment:S"},
    TTable{"lineitem",
        "l_orderkey:I,l_partkey:I,l_suppkey:I,l_linenumber:I,l_quantity:D,l_extendedprice:D,l_discount:D,l_tax:D,l_"
        "returnflag:S,l_linestatus:S,l_shipdate:S,l_commitdate:S,l_receiptdate:S,l_shipinstruct:S,l_shipmode:S,l_"
        "comment:S"}};

void Check(bool condition, const std::string& message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}
template <class T>
T Take(Result<T> value)
{
    Check(value.has_value(), value ? "" : value.error().code + ": " + value.error().message);
    return std::move(*value);
}
void Arrow(const arrow::Status& status)
{
    Check(status.ok(), status.ToString());
}
double Ms(TClock::time_point start)
{
    return std::chrono::duration<double, std::milli>(TClock::now() - start).count();
}
TJson Read(const std::filesystem::path& path)
{
    std::ifstream input(path);
    Check(input.good(), "cannot read " + path.string());
    return TJson::parse(input);
}
void Save(const std::filesystem::path& path, const TJson& value)
{
    const auto temporary = path.string() + ".tmp";
    std::ofstream output(temporary);
    output << value.dump() << '\n';
    output.close();
    Check(!output.fail(), "cannot save ledger");
    std::filesystem::rename(temporary, path);
}
std::string ID(size_t table, uint64_t operation)
{
    std::array<char, 37> id{};
    std::snprintf(id.data(),
        id.size(),
        "15600000-0000-4000-%04x-%012llx",
        static_cast<unsigned>(0x8100 + table),
        static_cast<unsigned long long>(operation));
    return id.data();
}
std::string Outcome(const AppendOutcome& outcome)
{
    const auto bytes = Take(detail::EncodeOutcome(outcome));
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const unsigned char byte : bytes) {
        result += "0123456789abcdef"[byte >> 4];
        result += "0123456789abcdef"[byte & 15];
    }
    return result;
}
std::vector<std::string_view> Split(std::string_view line, char delimiter)
{
    std::vector<std::string_view> fields;
    while (!line.empty()) {
        const auto end = line.find(delimiter);
        fields.push_back(line.substr(0, end));
        if (end == std::string_view::npos) {
            break;
        }
        line.remove_prefix(end + 1);
    }
    return fields;
}
SchemaSpec Schema(const TTable& table)
{
    SchemaSpec schema;
    for (const auto field : Split(table.Fields, ',')) {
        const auto type = field.back() == 'I' ? FieldType::Int64
            : field.back() == 'D'             ? FieldType::Float64
                                              : FieldType::String;
        schema.fields.push_back({std::string(field.substr(0, field.size() - 2)), type, false});
    }
    return schema;
}
template <class T>
T Number(std::string_view text)
{
    if constexpr (std::is_same_v<T, double>) {
        const auto dot = text.find('.');
        if (dot == std::string_view::npos) {
            return static_cast<double>(Number<int64_t>(text));
        }
        Check(text.size() - dot - 1 == 2, "TPC-H money must have two decimal places");
        auto digits = std::string(text);
        digits.erase(dot, 1);
        return static_cast<double>(Number<int64_t>(digits)) / 100.0;
    } else {
        T value{};
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        Check(error == std::errc{} && end == text.data() + text.size(), "invalid source number");
        return value;
    }
}
std::shared_ptr<arrow::RecordBatch> Batch(std::ifstream& input, const SchemaSpec& schema, int64_t rows)
{
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& field : schema.fields) {
        auto type = field.type == FieldType::Int64 ? arrow::int64()
            : field.type == FieldType::Float64     ? arrow::float64()
                                                   : arrow::utf8();
        fields.push_back(arrow::field(field.name, type, false));
        std::unique_ptr<arrow::ArrayBuilder> builder;
        Arrow(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
        Arrow(builder->Reserve(rows));
        builders.push_back(std::move(builder));
    }
    std::string line;
    int64_t count = 0;
    for (; count < rows && std::getline(input, line); ++count) {
        const auto values = Split(line, '|');
        Check(values.size() == builders.size(), "source column count differs");
        for (size_t column = 0; column < values.size(); ++column) {
            if (schema.fields[column].type == FieldType::Int64) {
                Arrow(static_cast<arrow::Int64Builder&>(*builders[column]).Append(Number<int64_t>(values[column])));
            } else if (schema.fields[column].type == FieldType::Float64) {
                Arrow(static_cast<arrow::DoubleBuilder&>(*builders[column]).Append(Number<double>(values[column])));
            } else {
                Arrow(static_cast<arrow::StringBuilder&>(*builders[column]).Append(values[column]));
            }
        }
    }
    Check(!input.bad(), "source read failed");
    if (count == 0) {
        return {};
    }
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        Arrow(builder->Finish(&array));
        arrays.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(arrow::schema(std::move(fields)), count, std::move(arrays));
}
uint64_t CheckRows(const std::shared_ptr<Engine>& engine,
    size_t table,
    const std::filesystem::path& data,
    SnapshotSelector selector,
    uint64_t expected,
    bool completeSource = false)
{
    std::ifstream source(data / (std::string(Tables[table].Name) + ".tbl"));
    Check(source.good(), "missing source oracle");
    const auto schema = Schema(Tables[table]);
    auto cursor = Take(engine->Scan({.collection_id = ID(table, 0), .selector = selector}));
    auto scratch = (data.parent_path() / "history-oracle-XXXXXX").string();
    Check(::mkdtemp(scratch.data()) != nullptr, "cannot create private oracle workspace");
    const std::filesystem::path workspace(scratch);
    const auto expectedPath = workspace / (std::string("oracle-") + Tables[table].Name + "-expected.txt");
    const auto actualPath = workspace / (std::string("oracle-") + Tables[table].Name + "-actual.txt");
    std::ofstream expectedFile(expectedPath), actualFile(actualPath);
    Check(expectedFile.good() && actualFile.good(), "cannot create oracle files");
    std::string line;
    for (uint64_t row = 0; row < expected; ++row) {
        Check(static_cast<bool>(std::getline(source, line)), "source has fewer rows than ledger");
        expectedFile << NHistoryOracle::SourceRow(Split(line, '|'), schema) << '\n';
    }
    if (completeSource) {
        Check(!std::getline(source, line) && !source.bad(), "source contains unconsumed rows");
    }
    uint64_t count = 0;
    while (const auto batch = Take(cursor->Next())) {
        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            actualFile << NHistoryOracle::ScanRow(*batch, row, schema) << '\n';
            ++count;
        }
    }
    expectedFile.close();
    actualFile.close();
    Check(!expectedFile.fail() && !actualFile.fail(), "oracle write failed");
    Check(count == expected, std::string(Tables[table].Name) + " row multiplicity differs");
    NHistoryOracle::Sort(expectedPath, workspace);
    NHistoryOracle::Sort(actualPath, workspace);
    NHistoryOracle::Compare({expectedPath, actualPath});
    std::filesystem::remove(expectedPath);
    std::filesystem::remove(actualPath);
    std::filesystem::remove(workspace);
    return count;
}
uint64_t PeakRSS()
{
#if defined(__linux__)
    // ru_maxrss survives exec and would include the checkpoint writer's peak.
    // VmHWM belongs to the current address space and starts again after exec.
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.starts_with("VmHWM:")) {
            std::istringstream value(line);
            std::string name, unit;
            uint64_t kib = 0;
            Check(static_cast<bool>(value >> name >> kib >> unit) && unit == "kB", "invalid VmHWM measurement");
            return kib * 1024;
        }
    }
    throw std::runtime_error("cannot read current-image peak RSS");
#else
    rusage usage{};
    Check(getrusage(RUSAGE_SELF, &usage) == 0, "getrusage failed");
#if defined(__APPLE__)
    return static_cast<uint64_t>(usage.ru_maxrss);
#else
    return static_cast<uint64_t>(usage.ru_maxrss) * 1024;
#endif
#endif
}
TJson Usage(const std::filesystem::path& root)
{
    uint64_t warehouse = 0, physical = 0, roots = 0, manifests = 0, lists = 0;
    struct stat warehouseInfo
    { };
    Check(::lstat((root / "warehouse").c_str(), &warehouseInfo) == 0, "cannot measure warehouse root allocation");
    physical = static_cast<uint64_t>(warehouseInfo.st_blocks) * 512;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root / "warehouse")) {
        struct stat info
        { };
        Check(::lstat(entry.path().c_str(), &info) == 0, "cannot measure warehouse allocation");
        physical += static_cast<uint64_t>(info.st_blocks) * 512;
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto bytes = entry.file_size();
        const auto allocated = static_cast<uint64_t>(info.st_blocks) * 512;
        physical += bytes > allocated ? bytes - allocated : 0;
        warehouse += bytes;
        const auto name = entry.path().filename().string();
        if (name.ends_with(".metadata.json")) {
            roots += bytes;
        } else if (name.starts_with("snap-")) {
            lists += bytes;
        } else if (name.ends_with(".avro")) {
            manifests += bytes;
        }
    }
    return {{"warehouse_file_bytes", warehouse},
        {"warehouse_physical_bytes", physical},
        {"root_bytes", roots},
        {"manifest_bytes", manifests},
        {"manifest_list_bytes", lists},
        {"catalog_bytes", std::filesystem::file_size(root / "catalog.sqlite")},
        {"peak_rss_bytes", PeakRSS()}};
}
Limits Profile(const TJson& config)
{
    Limits limits;
    limits.max_operations = config.at("max_operations");
    limits.max_catalog_bytes = config.value("max_catalog_bytes", limits.max_catalog_bytes);
    const auto& history = config.at("history");
    limits.history.max_snapshots = history.at("max_snapshots");
    limits.history.max_manifests = history.at("max_manifests");
    limits.history.max_data_files = history.at("max_data_files");
    limits.history.max_root_bytes = history.at("max_root_bytes");
    limits.history.max_root_file_bytes = history.at("max_root_file_bytes");
    limits.history.max_manifest_bytes = history.at("max_manifest_bytes");
    limits.history.max_manifest_list_bytes = history.at("max_manifest_list_bytes");
    limits.history.max_footer_bytes = history.at("max_footer_bytes");
    limits.history.max_commit_bytes = history.at("max_commit_bytes");
    limits.history.max_recovery_workspace_bytes = history.at("max_recovery_workspace_bytes");
    return limits;
}
TJson ColdOpen(char** arguments)
{
    const auto started = TClock::now();
    const std::string output = std::string(arguments[5]) + ".reopen.json";
    std::array<char*, 7> command{arguments[0],
        const_cast<char*>("open"),
        arguments[2],
        arguments[3],
        arguments[4],
        arguments[5],
        nullptr};
    posix_spawn_file_actions_t actions;
    Check(posix_spawn_file_actions_init(&actions) == 0, "cannot initialize reopen process");
    auto error =
        posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, output.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    pid_t child = 0;
    if (error == 0) {
        error = posix_spawnp(&child, command[0], &actions, nullptr, command.data(), environ);
    }
    posix_spawn_file_actions_destroy(&actions);
    Check(error == 0, "cannot start reopen process");
    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    Check(waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0, "checkpoint reopen process failed");
    auto result = Read(output);
    Check(result.at("opened") == true, "checkpoint profile could not reopen accepted state");
    result["process_ms"] = Ms(started);
    return result;
}
} // namespace

int main(int argc, char** argv)
try {
    Check(argc == 6, "usage: glamdb_history_bench load|verify|open ROOT TBL_DIRECTORY PROFILE LEDGER");
    const std::string mode(argv[1]);
    Check(mode == "load" || mode == "verify" || mode == "open", "unknown mode");
    const auto root = std::filesystem::canonical(std::filesystem::path(argv[2]).parent_path()) /
        std::filesystem::path(argv[2]).filename();
    const std::filesystem::path data(argv[3]), ledgerPath(argv[5]);
    const auto config = Read(argv[4]);
    const auto limits = Profile(config);
    const int64_t batchRows = config.value("batch_rows", 10000);
    const uint64_t rowLimit = config.value("table_row_limit", uint64_t{0});
    const bool verifyRows = config.value("verify_rows", true);
    const uint64_t checkpointEvery = config.value("checkpoint_every", uint64_t{500});
    Check(batchRows > 0 && batchRows <= 10000, "invalid ordinary append size");
    Check(checkpointEvery > 0, "invalid checkpoint interval");
    auto owner = std::make_shared<ResourceOwner>();
    const auto started = TClock::now();
    auto opened = mode == "load" ? CreateStore({StoreID, root.string(), "local-delete-extra-v1", limits, owner})
                                 : OpenExisting({root.string(), StoreID, "local-delete-extra-v1", limits, owner});
    if (!opened) {
        std::cout
            << TJson{{"opened", false}, {"error", opened.error().code}, {"message", opened.error().message}}.dump()
            << '\n';
        return mode == "load" ? 1 : 0;
    }
    auto engine = std::move(*opened);
    const double openMs = Ms(started);
    if (mode == "open") {
        const auto usage = Usage(root);
        Check(engine->Shutdown(std::chrono::seconds(5)).has_value(), "checkpoint shutdown failed");
        std::cout << TJson{{"opened", true}, {"open_ms", openMs}, {"usage", usage}}.dump() << '\n';
        return 0;
    }
    TJson ledger = mode == "load" ? TJson::array() : Read(ledgerPath);
    std::array<uint64_t, Tables.size()> rows{};
    uint64_t committed = 0;
    uint64_t checkpointReopens = 0;
    double appendMs = 0;
    std::vector<double> latencies;
    if (mode == "load") {
        Save(ledgerPath, ledger);
        for (size_t table = 0; table < Tables.size(); ++table) {
            const auto collection = engine->CreateCollection({ID(table, 0), Tables[table].Name, Schema(Tables[table])});
            Check(collection.disposition == CommitDisposition::Committed, "create collection failed");
            std::ifstream input(data / (std::string(Tables[table].Name) + ".tbl"));
            Check(input.good(), "missing TPC-H source file");
            uint64_t operation = 0;
            while (!rowLimit || rows[table] < rowLimit) {
                auto batch = Batch(input,
                    Schema(Tables[table]),
                    rowLimit ? std::min(batchRows, static_cast<int64_t>(rowLimit - rows[table])) : batchRows);
                if (!batch) {
                    break;
                }
                if (operation == 0) {
                    const auto failed = engine->AppendDurable(
                        {ID(table, 0), ID(table, 1), {SnapshotPreconditionKind::Equals, 123}, {batch}});
                    Check(failed.disposition == CommitDisposition::DefinitelyNotCommitted && failed.error &&
                            failed.error->code == "PRECONDITION_FAILED",
                        "bound failure differs");
                    Check(Take(engine->FindAppendOutcome(ID(table, 0), ID(table, 1))).has_value(),
                        "failure was not bound");
                    ledger.push_back({{"table", table}, {"operation", 1}, {"rows", 0}, {"outcome", Outcome(failed)}});
                }
                const auto before = TClock::now();
                const auto result = engine->AppendDurable({ID(table, 0), ID(table, operation + 2), {}, {batch}});
                const auto elapsed = Ms(before);
                if (result.disposition != CommitDisposition::Committed || !result.result.has_value()) {
                    throw std::runtime_error(
                        result.error ? result.error->code + ": " + result.error->message : "append not committed");
                }
                const auto& receipt = *result.result;
                appendMs += elapsed;
                latencies.push_back(elapsed);
                rows[table] += static_cast<uint64_t>(batch->num_rows());
                ledger.push_back({{"table", table},
                    {"operation", operation + 2},
                    {"rows", batch->num_rows()},
                    {"snapshot_id", receipt.snapshot_id},
                    {"outcome", Outcome(result)}});
                ++operation;
                ++committed;
                if (committed % checkpointEvery == 0) {
                    Save(ledgerPath, ledger);
                    TJson reopen = nullptr;
                    if (config.value("checkpoint_reopen", false)) {
                        Check(engine->Shutdown(std::chrono::seconds(5)).has_value(), "checkpoint shutdown failed");
                        engine.reset();
                        reopen = ColdOpen(argv);
                        ++checkpointReopens;
                        engine = Take(OpenExisting({root.string(), StoreID, "local-delete-extra-v1", limits, owner}));
                    }
                    std::cerr << TJson{{"committed", committed},
                                     {"operations", ledger.size()},
                                     {"rows", rows},
                                     {"append_ms", appendMs},
                                     {"cold_reopen", reopen},
                                     {"usage", Usage(root)}}
                                     .dump()
                              << '\n'
                              << std::flush;
                }
            }
            Save(ledgerPath, ledger);
        }
    } else {
        for (const auto& record : ledger) {
            const auto table = record.at("table").get<size_t>();
            const auto operation = record.at("operation").get<uint64_t>();
            const auto found = Take(engine->FindAppendOutcome(ID(table, 0), ID(table, operation)));
            Check(found && Outcome(*found) == record.at("outcome"), "original outcome differs");
            const auto count = record.at("rows").get<uint64_t>();
            rows.at(table) += count;
            committed += count != 0;
        }
        for (size_t table = 0; table < Tables.size(); ++table) {
            if (verifyRows) {
                CheckRows(engine, table, data, SnapshotSelector::Latest(), rows[table], rowLimit == 0);
            }
            CheckRows(engine, table, data, SnapshotSelector::InitialEmpty(), 0);
            const auto first = std::ranges::find_if(ledger, [&](const auto& record) {
                return record.at("table") == table && record.at("rows") != 0;
            });
            Check(first != ledger.end(), "missing first receipt");
            CheckRows(engine, table, data, SnapshotSelector::Id(first->at("snapshot_id")), first->at("rows"));
            std::ifstream input(data / (std::string(Tables[table].Name) + ".tbl"));
            const auto batch = Batch(input, Schema(Tables[table]), first->at("rows"));
            const auto failed = std::ranges::find_if(ledger, [&](const auto& record) {
                return record.at("table") == table && record.at("operation") == 1;
            });
            Check(failed != ledger.end(), "missing original failed binding");
            const auto failedRetry = engine->AppendDurable({ID(table, 0), ID(table, 1), {}, {batch}});
            Check(Outcome(failedRetry) == failed->at("outcome"), "failed retry changed original failure");
            const auto retryStarted = TClock::now();
            for (int retry = 0; retry < 10; ++retry) {
                const auto retried = engine->AppendDurable({ID(table, 0), ID(table, 2), {}, {batch}});
                Check(Outcome(retried) == first->at("outcome"), "exact first retry changed outcome");
            }
            const auto retryMeanUs = Ms(retryStarted) * 100;
            const auto statistics = Take(engine->GetStatistics(ID(table, 0)));
            Check(statistics.rows.value == static_cast<int64_t>(rows[table]), "retries changed latest row count");
            const auto history = Take(engine->ListSnapshots(ID(table, 0)));
            const auto snapshots = std::ranges::count_if(ledger, [&](const auto& record) {
                return record.at("table") == table && record.at("rows") != 0;
            });
            Check(history.snapshots.size() == static_cast<size_t>(snapshots), "retries changed retained snapshots");
            std::cerr << TJson{{"table", Tables[table].Name},
                             {"verified_rows", rows[table]},
                             {"retry_mean_us", retryMeanUs}}
                             .dump()
                      << '\n'
                      << std::flush;
        }
    }
    std::sort(latencies.begin(), latencies.end());
    const auto percentile = [&](size_t percent) {
        return latencies.empty() ? 0.0 : latencies[std::min(latencies.size() - 1, latencies.size() * percent / 100)];
    };
    const auto usage = Usage(root);
    Check(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
    engine.reset();
    std::cout << TJson{{"opened", true},
                     {"mode", mode},
                     {"verified", mode == "verify"},
                     {"all_rows_verified", mode == "verify" && verifyRows},
                     {"committed", committed},
                     {"operations", ledger.size()},
                     {"checkpoint_reopens", checkpointReopens},
                     {"rows", rows},
                     {"open_ms", openMs},
                     {"append_ms", appendMs},
                     {"append_p50_ms", percentile(50)},
                     {"append_p95_ms", percentile(95)},
                     {"append_p99_ms", percentile(99)},
                     {"wall_ms", Ms(started)},
                     {"usage", usage}}
                     .dump()
              << '\n';
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
