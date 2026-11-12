#include "core/telemetry/metrics.h"

#include <prometheus/collectable.h>
#include <prometheus/text_serializer.h>

#include <cmath>
#include <initializer_list>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {
void Check(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

const prometheus::ClientMetric& Find(const std::vector<prometheus::MetricFamily>& families,
    const std::string& name,
    std::initializer_list<std::pair<std::string, std::string>> labels)
{
    for (const auto& family : families) {
        if (family.name != name) {
            continue;
        }
        for (const auto& metric : family.metric) {
            bool matches = true;
            for (const auto& [key, value] : labels) {
                bool found = false;
                for (const auto& label : metric.label) {
                    found |= label.name == key && label.value == value;
                }
                matches &= found;
            }
            if (matches) {
                return metric;
            }
        }
    }
    throw std::runtime_error("expected metric not found: " + name);
}
} // namespace

int main()
{
    using namespace NGlamDB::NTelemetry;
    using namespace glamdb;
    const auto collectable = GetMetricsCollectable();
    const auto before = collectable->Collect();
    Check(Find(before, "glamdb_operations_total", {{"operation", "append"}, {"outcome", "committed"}}).counter.value ==
            0,
        "new process counter did not reset");
    Check(Find(before, "glamdb_phase_duration_seconds", {{"operation", "append"}, {"phase", "data"}})
                .histogram.sample_count == 0,
        "pre-registered phase has observations");
    Check(Find(before, "glamdb_rows_published_total", {}).counter.value == 0 &&
            Find(before, "glamdb_data_bytes_published_total", {}).counter.value == 0 &&
            Find(before, "glamdb_read_rows_total", {}).counter.value == 0 &&
            Find(before, "glamdb_scan_cursors_retained", {}).gauge.value == 0,
        "new process read or publication metrics did not reset");
    {
        TOperationScope<AppendOutcome> scope(EOperation::Append);
        scope.EnterPhase(CommitPhase::Data);
        (void)scope.Finish({"sensitive-operation-id",
            CommitDisposition::CommitUnknown,
            CommitPhase::Data,
            std::nullopt,
            ErrorDetail{"unbounded-secret-error", "private-message"},
            std::nullopt});
    }
    {
        TOperationScope<CreateOutcome> scope(EOperation::CreateCollection);
    }
    {
        TReadOperationScope scope(EReadOperation::ScanOpen);
        Check(Find(collectable->Collect(), "glamdb_read_operations_in_flight", {{"operation", "scan_open"}})
                    .gauge.value == 1,
            "read scope missing from in-flight gauge");
        const auto result = scope.Finish(Result<int>(7));
        Check(result && *result == 7, "read scope changed the successful result");
        scope.Error("CANCELLED");
        scope.Finish();
    }
    {
        TReadOperationScope scope(EReadOperation::CloseStore);
        const auto result = scope.Finish(Status(std::unexpected(ErrorDetail{"RESOURCE_EXHAUSTED", "private-message"})));
        Check(!result && result.error().code == "RESOURCE_EXHAUSTED", "read scope changed the failed result");
    }
    {
        TReadOperationScope scope(EReadOperation::Reconcile);
    }
    {
        TReadOperationScope scope(EReadOperation::ScanCancel);
        scope.Finish();
    }
    {
        TReadOperationScope scope(EReadOperation::ScanNext);
        scope.Error("CANCELLED");
    }
    {
        TReadOperationScope scope(EReadOperation::ScanNext);
        Check(scope.Finish(Result<int>(5), 5, 64).has_value(), "scan scope changed the result");
    }
    RecordReadFinished(EReadOperation::ScanNext, {}, .001);
    RecordReadFinished(EReadOperation::Reconcile, "unbounded-secret-error", .001);
    RecordReadFinished(static_cast<EReadOperation>(255), "unbounded-secret-error", .001, 999);
    std::vector<std::thread> writers;
    for (int thread = 0; thread < 8; ++thread) {
        writers.emplace_back([] {
            for (int call = 0; call < 10000; ++call) {
                RecordFinished(EOperation::Append,
                    CommitDisposition::Committed,
                    CommitPhase::Complete,
                    {},
                    0.01,
                    2,
                    16);
                RecordReadFinished(EReadOperation::ScanNext, {}, .005, 3, 24);
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    const auto completed = collectable->Collect();
    Check(Find(completed, "glamdb_rows_published_total", {}).counter.value == 0 &&
            Find(completed, "glamdb_data_bytes_published_total", {}).counter.value == 0,
        "receipt-facing outcomes were counted as new publications");
    RecordPublished(5, 128);
    AdjustRetainedCursors(2);
    Check(Find(collectable->Collect(), "glamdb_scan_cursors_retained", {}).gauge.value == 2,
        "retained cursor objects missing");
    AdjustRetainedCursors(-2);
    AdjustAdmission(1, 8192);
    Check(Find(collectable->Collect(), "glamdb_admission_owned_input_bytes", {}).gauge.value == 8192,
        "admission ownership missing");
    AdjustAdmission(-1, -8192);
    const auto after = collectable->Collect();
    Check(Find(after, "glamdb_operations_total", {{"operation", "append"}, {"outcome", "committed"}}).counter.value ==
            80000,
        "concurrent increments were lost");
    const auto& histogram =
        Find(after, "glamdb_operation_duration_seconds", {{"operation", "append"}, {"outcome", "committed"}}).histogram;
    Check(histogram.sample_count == 80000 && std::abs(histogram.sample_sum - 800) < 1e-7,
        "histogram count or sum incorrect");
    Check(histogram.bucket[1].cumulative_count == 0 && histogram.bucket[4].upper_bound == 0.01 &&
            histogram.bucket[4].cumulative_count == 80000,
        "histogram boundary incorrect");
    Check(Find(after, "glamdb_operations_total", {{"operation", "create_collection"}, {"outcome", "incomplete"}})
                .counter.value == 1,
        "incomplete scope hidden");
    Check(Find(after, "glamdb_operation_errors_total", {{"operation", "append"}, {"phase", "data"}, {"error", "other"}})
                .counter.value == 1,
        "error label unbounded");
    Check(Find(after, "glamdb_operations_in_flight", {{"operation", "append"}}).gauge.value == 0,
        "scope did not release in-flight gauge");
    Check(Find(after, "glamdb_read_operations_total", {{"operation", "scan_open"}, {"outcome", "ok"}}).counter.value ==
                1 &&
            Find(after, "glamdb_read_operations_total", {{"operation", "scan_open"}, {"outcome", "cancelled"}})
                    .counter.value == 0,
        "read scope completion counted more than once");
    Check(Find(after, "glamdb_read_operations_total", {{"operation", "close_store"}, {"outcome", "error"}})
                    .counter.value == 1 &&
            Find(after, "glamdb_read_operations_total", {{"operation", "reconcile"}, {"outcome", "incomplete"}})
                    .counter.value == 1 &&
            Find(after, "glamdb_read_operations_total", {{"operation", "reconcile"}, {"outcome", "error"}})
                    .counter.value == 1 &&
            Find(after, "glamdb_read_operations_total", {{"operation", "scan_next"}, {"outcome", "cancelled"}})
                    .counter.value == 1,
        "read error, cancellation or incomplete outcome hidden");
    Check(
        Find(after, "glamdb_read_operations_total", {{"operation", "scan_cancel"}, {"outcome", "ok"}}).counter.value ==
                1 &&
            Find(after, "glamdb_read_operations_total", {{"operation", "scan_next"}, {"outcome", "ok"}})
                    .counter.value == 80002 &&
            Find(after, "glamdb_read_duration_seconds", {{"operation", "scan_next"}}).histogram.sample_count == 80003 &&
            Find(after, "glamdb_read_rows_total", {}).counter.value == 240005,
        "concurrent scan calls or returned rows were lost");
    Check(Find(after, "glamdb_rows_published_total", {}).counter.value == 5 &&
            Find(after, "glamdb_data_bytes_published_total", {}).counter.value == 128,
        "confirmed publication row or byte accounting incorrect");
    Check(Find(after, "glamdb_scan_cursors_retained", {}).gauge.value == 0 &&
            Find(after, "glamdb_read_operations_in_flight", {{"operation", "scan_open"}}).gauge.value == 0 &&
            Find(after, "glamdb_read_operations_in_flight", {{"operation", "close_store"}}).gauge.value == 0 &&
            Find(after, "glamdb_read_operations_in_flight", {{"operation", "scan_next"}}).gauge.value == 0 &&
            Find(after, "glamdb_read_operations_in_flight", {{"operation", "reconcile"}}).gauge.value == 0,
        "read scopes or cursor retention did not release their gauges");
    prometheus::TextSerializer serializer;
    const auto snapshot = serializer.Serialize(after);
    Check(snapshot.find("sensitive") == std::string::npos && snapshot.find("secret") == std::string::npos &&
            snapshot.find("private") == std::string::npos,
        "identifier or diagnostic leaked");
    Check(snapshot.find("snapshot_timestamp") == std::string::npos, "stale textfile protocol retained");
    auto count = [](const auto& text) {
        size_t result = 0;
        bool start = true;
        for (char character : text) {
            if (start && character != '#') {
                ++result;
            }
            start = character == '\n';
        }
        return result;
    };
    Check(count(snapshot) == count(serializer.Serialize(before)) && count(snapshot) == 879,
        "pre-registered series budget changed");
    std::cout << snapshot;
}
