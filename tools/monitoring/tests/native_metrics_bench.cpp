#include "core/telemetry/metrics.h"

#include <prometheus/collectable.h>
#include <prometheus/text_serializer.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {
void ObserveCalls(size_t count)
{
    using namespace NGlamDB::NTelemetry;
    for (size_t call = 0; call < count; ++call) {
        TOperationScope<glamdb::AppendOutcome> scope(EOperation::Append);
        scope.EnterPhase(glamdb::CommitPhase::Data);
        scope.EnterPhase(glamdb::CommitPhase::CatalogTransaction);
        (void)scope.Finish({{},
            glamdb::CommitDisposition::Committed,
            glamdb::CommitPhase::Complete,
            std::nullopt,
            std::nullopt,
            std::nullopt});
    }
}

size_t CountSeries(const std::string& text)
{
    size_t count = 0;
    bool start = true;
    for (char character : text) {
        if (start && character != '#') {
            ++count;
        }
        start = character == '\n';
    }
    return count;
}
} // namespace

int main()
{
    constexpr size_t Calls = 1'000'000;
    ObserveCalls(1);
    const auto before =
        CountSeries(prometheus::TextSerializer().Serialize(NGlamDB::NTelemetry::GetMetricsCollectable()->Collect()));
    const auto start = std::chrono::steady_clock::now();
    ObserveCalls(Calls);
    const auto single = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
    const auto parallelStart = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    for (size_t thread = 0; thread < 8; ++thread) {
        workers.emplace_back([] {
            ObserveCalls(Calls / 8);
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    const auto parallel = std::chrono::duration<double>(std::chrono::steady_clock::now() - parallelStart).count();
    const auto snapshot =
        prometheus::TextSerializer().Serialize(NGlamDB::NTelemetry::GetMetricsCollectable()->Collect());
    const auto after = CountSeries(snapshot);
    std::cout << "{\"fixture\":\"metrics component only, no database or storage operations\",\"calls_per_run\":"
              << Calls << ",\"single_seconds\":" << single << ",\"single_ns_per_call\":" << single * 1e9 / Calls
              << ",\"eight_threads_seconds\":" << parallel << ",\"series_before\":" << before
              << ",\"series_after\":" << after << ",\"snapshot_bytes\":" << snapshot.size() << "}\n";
    return before == after && after < 1000 ? 0 : 1;
}
