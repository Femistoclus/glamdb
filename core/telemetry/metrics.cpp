#include "core/telemetry/metrics.h"

#ifdef GLAMDB_METRICS_WITH_LOGGING
#include "core/telemetry/structured_log.h"
#endif

#include <prometheus/collectable.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <algorithm>
#include <array>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace NGlamDB::NTelemetry {
namespace {

constexpr std::array<std::string_view, 2> OperationNames{"append", "create_collection"};
constexpr std::array<std::string_view, 4> OutcomeNames{"committed",
    "definitely_not_committed",
    "commit_unknown",
    "incomplete"};
constexpr std::array<std::string_view, 9> PhaseNames{"validation",
    "binding",
    "data",
    "metadata",
    "catalog_transaction",
    "catalog_commit",
    "recording_failure",
    "reconciling",
    "complete"};
constexpr std::array<std::string_view, 13> ErrorNames{"INVALID_ARGUMENT",
    "RESOURCE_EXHAUSTED",
    "DEADLINE_EXCEEDED",
    "CANCELLED",
    "IO_ERROR",
    "CORRUPTION",
    "RECOVERY_REQUIRED",
    "OPERATION_CONFLICT",
    "PRECONDITION_FAILED",
    "DEPENDENCY_UNAVAILABLE",
    "SCHEMA_MISMATCH",
    "INTERNAL",
    "other"};
constexpr std::array<double, 15>
    Bounds{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 60};

// Domain schema and stable handles only. prometheus-cpp owns the registry,
// concurrent counters/gauges/histograms, collection and serialization.
struct TDomainMetrics final : prometheus::Collectable
{
    prometheus::Registry Registry;
    std::array<std::array<prometheus::Counter*, OutcomeNames.size()>, OperationNames.size()> Outcomes{};
    std::array<std::array<prometheus::Histogram*, OutcomeNames.size()>, OperationNames.size()> Durations{};
    std::array<std::array<prometheus::Histogram*, PhaseNames.size()>, OperationNames.size()> Phases{};
    std::array<std::array<std::array<prometheus::Counter*, ErrorNames.size()>, PhaseNames.size()>,
        OperationNames.size()>
        Errors{};
    std::array<prometheus::Gauge*, OperationNames.size()> InFlight{};
    prometheus::Gauge* Pending{};
    prometheus::Gauge* OwnedBytes{};
#ifdef GLAMDB_METRICS_WITH_LOGGING
    std::array<prometheus::Counter*, 11> LogCounters{};
    std::array<prometheus::Gauge*, 3> LogGauges{};
    mutable std::mutex LogScrapeMutex;
#endif

    TDomainMetrics()
    {
        const prometheus::Labels processLabels{{"service_instance", "core"}};
        const prometheus::Histogram::BucketBoundaries bounds(Bounds.begin(), Bounds.end());
        auto& outcomes = prometheus::BuildCounter()
                             .Name("glamdb_operations_total")
                             .Help("Completed API calls including retries; not unique durable writes.")
                             .Labels(processLabels)
                             .Register(Registry);
        auto& durations = prometheus::BuildHistogram()
                              .Name("glamdb_operation_duration_seconds")
                              .Help("End-to-end API duration including admission and retries.")
                              .Labels(processLabels)
                              .Register(Registry);
        auto& phases = prometheus::BuildHistogram()
                           .Name("glamdb_phase_duration_seconds")
                           .Help("Observed coordinator regions; data includes backend metadata work.")
                           .Labels(processLabels)
                           .Register(Registry);
        auto& errors = prometheus::BuildCounter()
                           .Name("glamdb_operation_errors_total")
                           .Help("Returned errors mapped to a fixed domain.")
                           .Labels(processLabels)
                           .Register(Registry);
        auto& inFlight = prometheus::BuildGauge()
                             .Name("glamdb_operations_in_flight")
                             .Help("Active synchronous API calls.")
                             .Labels(processLabels)
                             .Register(Registry);
        for (size_t operation = 0; operation < OperationNames.size(); ++operation) {
            const auto name = std::string(OperationNames[operation]);
            InFlight[operation] = &inFlight.Add({{"operation", name}});
            for (size_t outcome = 0; outcome < OutcomeNames.size(); ++outcome) {
                const prometheus::Labels labels{{"operation", name}, {"outcome", std::string(OutcomeNames[outcome])}};
                Outcomes[operation][outcome] = &outcomes.Add(labels);
                Durations[operation][outcome] = &durations.Add(labels, bounds);
            }
            for (size_t phase = 0; phase < PhaseNames.size(); ++phase) {
                prometheus::Labels labels{{"operation", name}, {"phase", std::string(PhaseNames[phase])}};
                Phases[operation][phase] = &phases.Add(labels, bounds);
                for (size_t error = 0; error < ErrorNames.size(); ++error) {
                    labels["error"] = ErrorNames[error];
                    Errors[operation][phase][error] = &errors.Add(labels);
                }
            }
        }
        Pending = &prometheus::BuildGauge()
                       .Name("glamdb_admission_pending_calls")
                       .Help("Admitted append calls retaining an admission slot.")
                       .Labels(processLabels)
                       .Register(Registry)
                       .Add({});
        OwnedBytes = &prometheus::BuildGauge()
                          .Name("glamdb_admission_owned_input_bytes")
                          .Help("Append input bytes owned by admission scopes.")
                          .Labels(processLabels)
                          .Register(Registry)
                          .Add({});
#ifdef GLAMDB_METRICS_WITH_LOGGING
        const std::array<std::string, 11> counters{"submitted",
            "accepted",
            "sampled",
            "rate_limited",
            "dropped_full",
            "dropped_unregistered",
            "dropped_stopped",
            "adapter_errors",
            "backend_notifications",
            "quill_reported_dropped",
            "sink_errors"};
        for (size_t index = 0; index < counters.size(); ++index) {
            LogCounters[index] = &prometheus::BuildCounter()
                                      .Name("glamdb_log_" + counters[index] + "_total")
                                      .Help("Process logging adapter or Quill callback accounting.")
                                      .Labels(processLabels)
                                      .Register(Registry)
                                      .Add({});
        }
        const std::array<std::string, 3> gauges{"producer_threads", "queue_capacity_bytes", "max_producer_threads"};
        for (size_t index = 0; index < gauges.size(); ++index) {
            LogGauges[index] = &prometheus::BuildGauge()
                                    .Name("glamdb_log_" + gauges[index])
                                    .Help("Native logging configured producer resource budget.")
                                    .Labels(processLabels)
                                    .Register(Registry)
                                    .Add({});
        }
#endif
    }

    std::vector<prometheus::MetricFamily> Collect() const override
    {
#ifdef GLAMDB_METRICS_WITH_LOGGING
        // External logger accounting is sampled at scrape. The library still
        // owns every metric; serialize concurrent scrapes of this adapter.
        const std::lock_guard lock(LogScrapeMutex);
        const auto stats = GetProcessLogStats();
        const std::array<uint64_t, 11> values{stats.Submitted,
            stats.Accepted,
            stats.Sampled,
            stats.RateLimited,
            stats.DroppedFull,
            stats.DroppedUnregistered,
            stats.DroppedStopped,
            stats.AdapterErrors,
            stats.BackendNotifications,
            stats.QuillReportedDropped,
            stats.SinkErrors};
        for (size_t index = 0; index < values.size(); ++index) {
            LogCounters[index]->Reset();
            LogCounters[index]->Increment(static_cast<double>(values[index]));
        }
        LogGauges[0]->Set(static_cast<double>(stats.ProducerThreads));
        LogGauges[1]->Set(static_cast<double>(stats.QueueCapacityBytes));
        LogGauges[2]->Set(static_cast<double>(stats.MaxProducerThreads));
#endif
        return Registry.Collect();
    }
};

const std::shared_ptr<TDomainMetrics>& GetDomainMetrics() noexcept
{
    // One initialization attempt. Telemetry allocation failure never changes
    // an engine outcome or causes repeated allocations in the operation path.
    static const auto Metrics = []() noexcept -> std::shared_ptr<TDomainMetrics> {
        try {
            return std::make_shared<TDomainMetrics>();
        } catch (...) {
            return {};
        }
    }();
    return Metrics;
}

} // namespace

void RecordPhase(EOperation operation, glamdb::CommitPhase phase, double seconds) noexcept
try {
    const auto operationIndex = static_cast<size_t>(operation);
    const auto phaseIndex = static_cast<size_t>(phase);
    const auto& metrics = GetDomainMetrics();
    if (metrics && operationIndex < OperationNames.size() && phaseIndex < PhaseNames.size()) {
        metrics->Phases[operationIndex][phaseIndex]->Observe(seconds);
    }
} catch (...) {
    // Telemetry must not change an engine operation's outcome.
    return;
}

void RecordFinished(EOperation operation,
    glamdb::CommitDisposition outcome,
    glamdb::CommitPhase phase,
    std::string_view error,
    double seconds) noexcept
try {
    const auto operationIndex = static_cast<size_t>(operation);
    const auto outcomeIndex = static_cast<size_t>(outcome);
    const auto phaseIndex = static_cast<size_t>(phase);
    const auto& metrics = GetDomainMetrics();
    if (!metrics || operationIndex >= OperationNames.size() || outcomeIndex >= 3 || phaseIndex >= PhaseNames.size()) {
        return;
    }
    metrics->Outcomes[operationIndex][outcomeIndex]->Increment();
    metrics->Durations[operationIndex][outcomeIndex]->Observe(seconds);
    if (!error.empty()) {
        const auto errorIt = std::ranges::find(ErrorNames, error);
        const auto errorIndex =
            errorIt == ErrorNames.end() ? ErrorNames.size() - 1 : static_cast<size_t>(errorIt - ErrorNames.begin());
        metrics->Errors[operationIndex][phaseIndex][errorIndex]->Increment();
    }
} catch (...) {
    // Telemetry must not change an engine operation's outcome.
    return;
}

void RecordIncomplete(EOperation operation, double seconds) noexcept
try {
    const auto operationIndex = static_cast<size_t>(operation);
    const auto& metrics = GetDomainMetrics();
    if (metrics && operationIndex < OperationNames.size()) {
        metrics->Outcomes[operationIndex][3]->Increment();
        metrics->Durations[operationIndex][3]->Observe(seconds);
    }
} catch (...) {
    // Telemetry must not change an engine operation's outcome.
    return;
}

void AdjustInFlight(EOperation operation, int64_t delta) noexcept
try {
    const auto operationIndex = static_cast<size_t>(operation);
    const auto& metrics = GetDomainMetrics();
    if (metrics && operationIndex < OperationNames.size()) {
        metrics->InFlight[operationIndex]->Increment(static_cast<double>(delta));
    }
} catch (...) {
    // Telemetry must not change an engine operation's outcome.
    return;
}

void AdjustAdmission(int64_t pendingDelta, int64_t bytesDelta) noexcept
try {
    const auto& metrics = GetDomainMetrics();
    if (metrics) {
        metrics->Pending->Increment(static_cast<double>(pendingDelta));
        metrics->OwnedBytes->Increment(static_cast<double>(bytesDelta));
    }
} catch (...) {
    // Telemetry must not change an engine operation's outcome.
    return;
}

std::shared_ptr<prometheus::Collectable> GetMetricsCollectable()
{
    const auto& metrics = GetDomainMetrics();
    if (!metrics) {
        throw std::runtime_error("native metrics initialization failed");
    }
    return metrics;
}

} // namespace NGlamDB::NTelemetry
