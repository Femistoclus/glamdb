#include "apps/server/metrics.h"

#include "apps/transport/http.h"

#include "core/telemetry/metrics.h"
#include "core/telemetry/structured_log.h"

#include <prometheus/collectable.h>
#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <algorithm>
#include <array>
#include <utility>

namespace glamdb::NServer {
namespace {

constexpr std::array<std::string_view, 17> Routes{"other",
    "health",
    "capabilities",
    "databases",
    "database",
    "database_lifecycle",
    "collections",
    "collection",
    "append",
    "scan",
    "scan_cancel",
    "operation",
    "snapshots",
    "snapshot",
    "files",
    "file",
    "static"};
constexpr std::array<std::string_view, 8> Methods{"OTHER", "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"};
constexpr std::array<std::string_view, 6> StatusClasses{"1xx", "2xx", "3xx", "4xx", "5xx", "cancelled"};
constexpr std::array StatusCodes{200,
    201,
    202,
    204,
    301,
    302,
    304,
    307,
    308,
    400,
    401,
    403,
    404,
    405,
    408,
    409,
    412,
    413,
    415,
    422,
    423,
    429,
    431,
    500,
    501,
    502,
    503,
    504,
    507,
    508};

template <size_t Size>
size_t FindLabel(const std::array<std::string_view, Size>& values, std::string_view value) noexcept
{
    const auto found = std::ranges::find(values, value);
    return found == values.end() ? 0 : static_cast<size_t>(found - values.begin());
}

size_t ClassifyRoute(std::string_view path)
{
    std::string_view suffix = path;
    if (suffix == "/healthz" || suffix == "/readyz") {
        return FindLabel(Routes, "health");
    }
    if (suffix == "/databases") {
        return FindLabel(Routes, "databases");
    }
    if (suffix.starts_with("/databases/")) {
        const auto split = suffix.find('/', 11);
        if (split == std::string_view::npos) {
            return FindLabel(Routes, "database");
        }
        suffix.remove_prefix(split);
        if (suffix == "/open" || suffix == "/close" || suffix == "/reconcile" || suffix == "/discard") {
            return FindLabel(Routes, "database_lifecycle");
        }
    }
    if (suffix == "/readyz") {
        return FindLabel(Routes, "health");
    }
    if (suffix == "/capabilities") {
        return FindLabel(Routes, "capabilities");
    }
    if (suffix == "/collections" || suffix == "/collections:resolve") {
        return FindLabel(Routes, "collections");
    }
    if (suffix.starts_with("/collections/")) {
        const auto split = suffix.find('/', 13);
        if (split == std::string_view::npos) {
            return FindLabel(Routes, "collection");
        }
        const auto action = suffix.substr(split);
        if (action == "/append") {
            return FindLabel(Routes, "append");
        }
        if (action == "/schema") {
            return FindLabel(Routes, "collection");
        }
        if (action == "/scan" || action == "/scan/open" || action == "/scan/arrow") {
            return FindLabel(Routes, "scan");
        }
        if (action == "/scan/cancel") {
            return FindLabel(Routes, "scan_cancel");
        }
        for (const auto name : {"operation", "snapshot", "file"}) {
            const auto plural = std::string("/") + name + "s";
            if (action == plural || action.starts_with(plural + "/")) {
                return FindLabel(Routes, action == plural ? plural.substr(1) : name);
            }
        }
    }
    return FindLabel(Routes,
        suffix == "/" || suffix == "/workbench/config" || suffix.starts_with("/assets/") ? "static" : "other");
}

class THttpMetrics final : public prometheus::Collectable
{
public:
    THttpMetrics()
    {
        const prometheus::Labels process{{"service_instance", "core"}};
        auto& requests = prometheus::BuildCounter()
                             .Name("glamdb_http_requests_total")
                             .Help("Accepted native HTTP connections through response completion or cancellation.")
                             .Labels(process)
                             .Register(Registry_);
        auto& responses = prometheus::BuildCounter()
                              .Name("glamdb_http_responses_total")
                              .Help("Fully written native HTTP responses by finite status code; excludes cancellation.")
                              .Labels(process)
                              .Register(Registry_);
        auto& bytes = prometheus::BuildCounter()
                          .Name("glamdb_http_response_bytes_total")
                          .Help("Encoded body bytes in completed local socket writes, including replays; excludes "
                                "headers and incomplete writes, and does not establish peer delivery.")
                          .Labels(process)
                          .Register(Registry_);
        for (size_t code = 0; code <= StatusCodes.size(); ++code) {
            Responses[code] = &responses.Add(
                {{"status_code", code < StatusCodes.size() ? std::to_string(StatusCodes[code]) : "other"}});
        }
        auto& durations =
            prometheus::BuildHistogram()
                .Name("glamdb_http_request_duration_seconds")
                .Help("Native HTTP connection lifetime including body read and full async response write.")
                .Labels(process)
                .Register(Registry_);
        auto& inFlight = prometheus::BuildGauge()
                             .Name("glamdb_http_requests_in_flight")
                             .Help("Accepted native HTTP connections retaining request or response resources.")
                             .Labels(process)
                             .Register(Registry_);
        const prometheus::Histogram::BucketBoundaries
            bounds{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 60};
        for (size_t route = 0; route < Routes.size(); ++route) {
            const prometheus::Labels label{{"route", std::string(Routes[route])}};
            Durations[route] = &durations.Add(label, bounds);
            InFlight[route] = &inFlight.Add(label);
            Bytes[route] = &bytes.Add(label);
            for (size_t method = 0; method < Methods.size(); ++method) {
                for (size_t status = 0; status < StatusClasses.size(); ++status) {
                    auto labels = label;
                    labels.emplace("method", Methods[method]);
                    labels.emplace("status_class", StatusClasses[status]);
                    Requests[route][method][status] = &requests.Add(labels);
                }
            }
        }
    }
    std::vector<prometheus::MetricFamily> Collect() const override
    {
        return Registry_.Collect();
    }

    std::array<std::array<std::array<prometheus::Counter*, StatusClasses.size()>, Methods.size()>, Routes.size()>
        Requests{};
    std::array<prometheus::Histogram*, Routes.size()> Durations{};
    std::array<prometheus::Gauge*, Routes.size()> InFlight{};
    std::array<prometheus::Counter*, Routes.size()> Bytes{};
    std::array<prometheus::Counter*, StatusCodes.size() + 1> Responses{};

private:
    prometheus::Registry Registry_;
};

const std::shared_ptr<THttpMetrics>& GetHttpMetrics() noexcept
{
    static const auto Metrics = []() noexcept -> std::shared_ptr<THttpMetrics> {
        try {
            return std::make_shared<THttpMetrics>();
        } catch (...) {
            return {};
        }
    }();
    return Metrics;
}

NGlamDB::NTelemetry::ELogOperation ClassifyOperation(size_t route, size_t method, std::string_view path) noexcept
{
    using NGlamDB::NTelemetry::ELogOperation;
    if (Routes[route] == "databases") {
        return Methods[method] == "POST" ? ELogOperation::CreateStore : ELogOperation::Databases;
    }
    if (Routes[route] == "health") {
        return path.ends_with("/readyz") ? ELogOperation::Readiness : ELogOperation::Health;
    }
    if (Routes[route] == "database_lifecycle") {
        return path.ends_with("/open")     ? ELogOperation::OpenStore
            : path.ends_with("/close")     ? ELogOperation::CloseStore
            : path.ends_with("/reconcile") ? ELogOperation::Reconcile
                                           : ELogOperation::Other;
    }
    if (Routes[route] == "collections") {
        return Methods[method] == "POST" && path.ends_with("/collections") ? ELogOperation::CreateCollection
                                                                           : ELogOperation::Collections;
    }
    if (Routes[route] == "collection") {
        return path.ends_with("/schema") ? ELogOperation::Schema : ELogOperation::Collections;
    }
    if (Routes[route] == "scan") {
        return path.ends_with("/scan/open") ? ELogOperation::ScanOpen : ELogOperation::ScanNext;
    }
    constexpr std::array<ELogOperation, Routes.size()> Operations{ELogOperation::Other,
        ELogOperation::Health,
        ELogOperation::Capabilities,
        ELogOperation::CreateStore,
        ELogOperation::InspectStore,
        ELogOperation::Other,
        ELogOperation::Collections,
        ELogOperation::Collections,
        ELogOperation::Append,
        ELogOperation::ScanNext,
        ELogOperation::ScanCancel,
        ELogOperation::Operation,
        ELogOperation::Snapshots,
        ELogOperation::Snapshots,
        ELogOperation::Files,
        ELogOperation::Files,
        ELogOperation::Other};
    return Operations[route];
}

class TReadiness final : public prometheus::Collectable
{
public:
    explicit TReadiness(std::function<bool()> isReady)
        : IsReady_(std::move(isReady))
        , Ready_(prometheus::BuildGauge()
                     .Name("glamdb_server_ready")
                     .Help("One when at least one database is open and all managed open engines accept writes.")
                     .Labels({{"service_instance", "core"}})
                     .Register(Registry_)
                     .Add({}))
    { }

    std::vector<prometheus::MetricFamily> Collect() const override
    {
        bool ready = false;
        try {
            ready = IsReady_();
        } catch (...) {
            // A health observation failure must close readiness.
            ready = false;
        }
        Ready_.Set(ready ? 1 : 0);
        return Registry_.Collect();
    }

private:
    const std::function<bool()> IsReady_;
    prometheus::Registry Registry_;
    prometheus::Gauge& Ready_;
};

} // namespace

THttpScope::THttpScope(NGlamDB::NTelemetry::TStructuredLogger* logger) noexcept
    : Logger_(logger)
    , RequestID_(logger == nullptr ? 0 : logger->NewRequestID())
    , Operation_(NGlamDB::NTelemetry::ELogOperation::Other)
{
    if (const auto& metrics = GetHttpMetrics()) {
        metrics->InFlight[Route_]->Increment();
    }
}

THttpScope::~THttpScope()
{
    Finish(0, false);
}

void THttpScope::SetRequest(std::string_view method, std::string_view target) noexcept
try {
    Method_ = FindLabel(Methods, method);
    const auto [path, query] = NTransport::ParseTarget(target);
    (void)query;
    const auto route = ClassifyRoute(path);
    if (const auto& metrics = GetHttpMetrics(); metrics && route != Route_) {
        metrics->InFlight[Route_]->Decrement();
        metrics->InFlight[route]->Increment();
    }
    Route_ = route;
    Operation_ = ClassifyOperation(Route_, Method_, path);
} catch (...) {
    return;
}

std::uint64_t THttpScope::GetRequestID() const noexcept
{
    return RequestID_;
}

void THttpScope::Finish(int status, bool completed, std::uint64_t bodyBytes) noexcept
{
    if (std::exchange(Finished_, true)) {
        return;
    }
    const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start_).count();
    if (const auto& metrics = GetHttpMetrics()) {
        const auto index = completed && status >= 100 && status < 600 ? static_cast<size_t>(status / 100 - 1) : 5;
        metrics->Requests[Route_][Method_][index]->Increment();
        metrics->Durations[Route_]->Observe(seconds);
        metrics->InFlight[Route_]->Decrement();
        if (completed) {
            const auto code = std::ranges::find(StatusCodes, status);
            metrics->Responses[static_cast<size_t>(code - StatusCodes.begin())]->Increment();
            metrics->Bytes[Route_]->Increment(static_cast<double>(bodyBytes));
        }
    }
    if (Logger_) {
        using namespace NGlamDB::NTelemetry;
        const bool denied = status == 401 || status == 403;
        Logger_->TryLog({.Operation = denied ? ELogOperation::Auth : Operation_,
            .Outcome = !completed ? ELogOutcome::Cancelled
                : denied          ? ELogOutcome::Denied
                : status >= 400   ? ELogOutcome::Error
                                  : ELogOutcome::Ok,
            .RequestID = RequestID_,
            .DurationUs = static_cast<uint64_t>(seconds * 1000000),
            .StatusCode = completed ? static_cast<uint32_t>(status) : 0,
            .ByteCount = completed ? bodyBytes : 0});
    }
}

TMetrics::TMetrics(const std::string& address, std::function<bool()> isReady)
{
    if (!address.empty()) {
        Domain_ = NGlamDB::NTelemetry::GetMetricsCollectable();
        Readiness_ = std::make_shared<TReadiness>(std::move(isReady));
        Http_ = GetHttpMetrics();
        Exposer_ = std::make_unique<prometheus::Exposer>(address);
        Exposer_->RegisterCollectable(Domain_);
        Exposer_->RegisterCollectable(Readiness_);
        Exposer_->RegisterCollectable(Http_);
    }
}

TMetrics::~TMetrics() = default;

} // namespace glamdb::NServer
