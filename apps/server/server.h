#pragma once

#include "apps/server/routes.h"
#include "core/telemetry/structured_log.h"

namespace NGlamDB::NTelemetry {
class TStructuredLogger;
}

namespace glamdb::NServer {
struct TReaderLimits
{
    size_t Active = 4;
    size_t PerDatabase = 2;
};
void ValidateReaderLimits(TReaderLimits limits);

// SIGINT/SIGTERM close admission and cancel scans; accepted writes drain before returning.
void RunServer(TRoutes& routes,
    std::string_view address,
    uint16_t port,
    std::chrono::milliseconds timeout,
    size_t maxConnections = 96,
    NGlamDB::NTelemetry::TStructuredLogger* logger = nullptr,
    TReaderLimits readers = {});
} // namespace glamdb::NServer
