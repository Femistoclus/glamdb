// Per-append latency and fresh-process open time versus the number of permanent
// operation receipts, with a fixed small retained Iceberg history (#174 E1).
//
//   glamdb_catalog_history_bench populate ROOT OPERATIONS
//     Creates a store with two collections, 10 committed and 1 failed append,
//     then copies the failed receipt offline until OPERATIONS receipts exist.
//   glamdb_catalog_history_bench measure ROOT APPENDS
//     Opens ROOT in this process and performs APPENDS committed appends;
//     prints one JSON line. Copy the populated ROOT before each measurement.
//   glamdb_catalog_history_bench catalog ROOT BINDS
//     Opens only the catalog of ROOT and times BINDS bind/fail pairs, the
//     catalog part of an append that fails after binding.

#include "storage/engine.h"

#include "core/api/codec.h"
#include "core/catalog/durable_sqlite_catalog.h"
#include "core/catalog/sqlite.h"
#include "tests/support/arrow_batch.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <stdexcept>

namespace {

using namespace glamdb;
using TClock = std::chrono::steady_clock;

constexpr auto StoreID = "64000000-0000-4000-8000-000000000001";
constexpr std::array CollectionIDs{"64000000-0000-4000-8000-000000000002", "64000000-0000-4000-8000-000000000003"};

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

std::string OperationID(unsigned group, unsigned value)
{
    char text[37];
    std::snprintf(text, sizeof(text), "%08x-0000-4000-8000-%012x", group, value);
    return text;
}

Limits BenchLimits()
{
    Limits limits;
    limits.max_operations = 250000;
    limits.max_catalog_bytes = 1LL << 30;
    return limits;
}

double ElapsedMs(TClock::time_point start)
{
    return std::chrono::duration<double, std::milli>(TClock::now() - start).count();
}

void AddFailedOperations(const std::filesystem::path& path, unsigned copies)
{
    detail::Database database;
    database.Open(path, SQLITE_OPEN_READWRITE, {});
    const auto rows = database.Query("SELECT collection_id,payload_hash,base_identity,attempt_id,outcome_blob FROM "
                                     "operations WHERE state=2");
    Require(rows.size() == 1, "expected one failed operation");
    const auto& row = rows.front();
    const auto outcome = detail::DecodeOutcome(detail::Required(row, 4));
    Require(outcome.has_value(), "invalid failed receipt");
    database.Execute("BEGIN IMMEDIATE");
    for (unsigned index = 1; index <= copies; ++index) {
        auto copy = *outcome;
        copy.operation_id = OperationID(0xc10e, index);
        const auto encoded = detail::EncodeOutcome(copy);
        Require(encoded.has_value(), "cannot encode a receipt");
        database.Execute("INSERT INTO operations(collection_id,operation_id,payload_hash,hash_version,state,"
                         "base_identity,attempt_id,receipt_version,outcome_blob) VALUES(?,?,?,1,2,?,?,1,?)",
            {detail::Required(row, 0),
                copy.operation_id,
                detail::Required(row, 1),
                detail::Blob{detail::Required(row, 2)},
                detail::Required(row, 3),
                detail::Blob{*encoded}});
    }
    database.Execute("COMMIT");
    database.Close();
}

void Populate(const std::filesystem::path& root, unsigned operations)
{
    Require(operations >= 11, "at least 11 operations");
    auto engine = Take(CreateStore({StoreID, root.string(), "local-delete-extra-v1", BenchLimits()}));
    const SchemaSpec schema{{{"n", FieldType::Int64, true}}};
    for (const auto* collection : CollectionIDs) {
        Require(engine->CreateCollection({collection, collection, schema}).disposition == CommitDisposition::Committed,
            "create collection failed");
    }
    for (unsigned index = 0; index < 10; ++index) {
        const auto outcome = engine->AppendDurable(
            {CollectionIDs[index % CollectionIDs.size()], OperationID(0xa, index), {}, {test::Batch()}});
        Require(outcome.disposition == CommitDisposition::Committed, "append failed");
    }
    const auto failed = engine->AppendDurable(
        {CollectionIDs[0], OperationID(0xb, 0), {SnapshotPreconditionKind::Equals, 1}, {test::Batch()}});
    Require(failed.disposition == CommitDisposition::DefinitelyNotCommitted && failed.error &&
            failed.error->code == "PRECONDITION_FAILED",
        "expected a failed bound append");
    Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
    engine.reset();
    AddFailedOperations(root / "catalog.sqlite", operations - 11);
}

void Measure(const std::filesystem::path& root, unsigned appends)
{
    const auto started = TClock::now();
    auto engine = Take(OpenExisting({root.string(), StoreID, "local-delete-extra-v1", BenchLimits()}));
    const auto openMs = ElapsedMs(started);
    std::vector<double> latencies;
    for (unsigned index = 0; index < appends; ++index) {
        const auto appendStarted = TClock::now();
        const auto outcome = engine->AppendDurable(
            {CollectionIDs[index % CollectionIDs.size()], OperationID(0xd, index), {}, {test::Batch()}});
        latencies.push_back(ElapsedMs(appendStarted));
        Require(outcome.disposition == CommitDisposition::Committed, "append failed");
    }
    Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
    auto sorted = latencies;
    std::ranges::sort(sorted);
    const auto quantile = [&](double q) {
        return sorted.empty() ? 0.0 : sorted[static_cast<size_t>(q * static_cast<double>(sorted.size() - 1))];
    };
    std::cout << "{\"open_ms\":" << openMs << ",\"appends\":" << appends << ",\"append_p50_ms\":" << quantile(0.5)
              << ",\"append_p90_ms\":" << quantile(0.9) << ",\"append_min_ms\":" << quantile(0.0) << "}\n";
}

void MeasureCatalog(const std::filesystem::path& root, unsigned binds)
{
    const auto started = TClock::now();
    auto catalog = detail::DurableSqliteCatalogStore::Open(root / "catalog.sqlite",
        {StoreID, "local-delete-extra-v1", 1},
        BenchLimits());
    const auto openMs = ElapsedMs(started);
    std::vector<double> latencies;
    for (unsigned index = 0; index < binds; ++index) {
        const auto bindStarted = TClock::now();
        const detail::BoundOperation binding{CollectionIDs[0],
            OperationID(0xe, index),
            std::string(64, 'a'),
            1,
            catalog->Base(CollectionIDs[0]),
            OperationID(0xf, index)};
        catalog->BindOperation(binding);
        catalog->RecordFailure(binding,
            {binding.operation_id,
                CommitDisposition::DefinitelyNotCommitted,
                CommitPhase::Data,
                std::nullopt,
                ErrorDetail{"IO_ERROR", "benchmark failure", RetryHint::NewOperationId},
                std::nullopt});
        latencies.push_back(ElapsedMs(bindStarted));
    }
    catalog->Close();
    std::ranges::sort(latencies);
    std::cout << "{\"catalog_open_ms\":" << openMs << ",\"binds\":" << binds
              << ",\"bind_p50_ms\":" << latencies[latencies.size() / 2] << ",\"bind_min_ms\":" << latencies.front()
              << "}\n";
}

unsigned ParseCount(std::string_view text)
{
    unsigned value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size(), "invalid count");
    return value;
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc == 4, "usage: glamdb_catalog_history_bench populate|measure|catalog ROOT COUNT");
    const std::string_view mode(argv[1]);
    const auto root = std::filesystem::absolute(argv[2]);
    const auto count = ParseCount(argv[3]);
    if (mode == "populate") {
        Populate(root, count);
    } else if (mode == "catalog") {
        MeasureCatalog(root, count);
    } else {
        Require(mode == "measure", "unknown mode");
        Measure(root, count);
    }
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
