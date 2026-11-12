#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace NGlamDB::NTelemetry {
class TStructuredLogger;
}

namespace prometheus {
class Collectable;
class Exposer;
} // namespace prometheus

namespace glamdb::NServer {

// One accepted connection carries one request through body read and async response completion.
class THttpScope
{
public:
    explicit THttpScope(NGlamDB::NTelemetry::TStructuredLogger* logger) noexcept;
    ~THttpScope();
    THttpScope(const THttpScope&) = delete;
    THttpScope& operator=(const THttpScope&) = delete;
    void SetRequest(std::string_view method, std::string_view target) noexcept;
    void Finish(int status, bool completed) noexcept;

private:
    const std::chrono::steady_clock::time_point Start_ = std::chrono::steady_clock::now();
    NGlamDB::NTelemetry::TStructuredLogger* Logger_;
    size_t Route_ = 0, Method_ = 0;
    bool Finished_ = false;
};

// The application owns the optional private listener and its collectables.
class TMetrics
{
public:
    TMetrics(const std::string& address, std::function<bool()> isReady);
    ~TMetrics();

private:
    std::shared_ptr<prometheus::Collectable> Domain_;
    std::shared_ptr<prometheus::Collectable> Readiness_;
    std::shared_ptr<prometheus::Collectable> Http_;
    std::unique_ptr<prometheus::Exposer> Exposer_;
};

} // namespace glamdb::NServer
