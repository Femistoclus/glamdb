// Manual benchmark: per-append latency of one-row appends while the retained
// history grows. Skipped unless GLAMDB_APPEND_HISTORY_BENCH is set to the
// number of appends; GLAMDB_APPEND_HISTORY_BUCKET (default 250) sets the
// reporting bucket and GLAMDB_APPEND_HISTORY_DIR an existing parent directory
// for the disposable store (default: a temporary directory).

#include "tests/vertical/vertical_support.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <string>
#include <vector>

#include <unistd.h>

namespace glamdb {
namespace {

using namespace std::chrono_literals;
using namespace glamdb::vertical;

std::size_t EnvCount(const char* name, std::size_t fallback)
{
    const char* value = std::getenv(name);
    return value && *value ? static_cast<std::size_t>(std::strtoull(value, nullptr, 10)) : fallback;
}

double Percentile(std::vector<double> values, double fraction)
{
    std::ranges::sort(values);
    const auto index = static_cast<std::size_t>(fraction * static_cast<double>(values.size() - 1) + 0.5);
    return values[std::min(index, values.size() - 1)];
}

TEST(AppendHistoryBench, OneRowAppendLatencyByRetainedHistory)
{
    const auto appends = EnvCount("GLAMDB_APPEND_HISTORY_BENCH", 0);
    if (appends == 0) {
        GTEST_SKIP() << "Set GLAMDB_APPEND_HISTORY_BENCH=<appends> to run this benchmark.";
    }
    const auto bucket = std::max<std::size_t>(1, EnvCount("GLAMDB_APPEND_HISTORY_BUCKET", 250));
    test::TempDir temporary;
    const char* parent = std::getenv("GLAMDB_APPEND_HISTORY_DIR");
    const auto root = (parent && *parent ? std::filesystem::path(parent) : temporary.path) /
        std::format("append-history-{}", ::getpid());
    std::filesystem::remove_all(root);
    std::shared_ptr<THookBackend> hooks;
    auto created = detail::EngineFactory::CreateWith({test::store_id, root.string()}, THookBackend::Factory(hooks));
    ASSERT_TRUE(created) << created.error().message;
    auto engine = std::move(*created);
    ASSERT_EQ(engine->CreateCollection({test::collection_id, "events", Schema}).disposition,
        CommitDisposition::Committed);

    std::vector<double> latencies;
    latencies.reserve(appends);
    std::printf("history_from,history_to,appends,mean_ms,p50_ms,p90_ms,p99_ms,max_ms,appends_per_s\n");
    for (std::size_t index = 0; index < appends; ++index) {
        const auto operation = std::format("00000000-0000-4000-8000-{:012x}", index + 1);
        const auto batch = Batch({{std::format("key-{}", index), static_cast<int64_t>(index), 0.5, true, 10}});
        const auto start = std::chrono::steady_clock::now();
        const auto outcome = engine->AppendDurable({test::collection_id, operation, {}, {batch}});
        const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start);
        ASSERT_EQ(outcome.disposition, CommitDisposition::Committed) << "append " << index;
        latencies.push_back(elapsed.count());
        if (latencies.size() == bucket || index + 1 == appends) {
            double total = 0;
            for (const auto value : latencies) {
                total += value;
            }
            const auto mean = total / static_cast<double>(latencies.size());
            std::printf("%zu,%zu,%zu,%.3f,%.3f,%.3f,%.3f,%.3f,%.1f\n",
                index + 1 - latencies.size(),
                index + 1,
                latencies.size(),
                mean,
                Percentile(latencies, 0.5),
                Percentile(latencies, 0.9),
                Percentile(latencies, 0.99),
                *std::ranges::max_element(latencies),
                1000.0 / mean);
            std::fflush(stdout);
            latencies.clear();
        }
    }
    ASSERT_TRUE(engine->Shutdown(5000ms));
    engine.reset();
    std::filesystem::remove_all(root);
}

} // namespace
} // namespace glamdb
