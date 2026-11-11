#pragma once

#include "apps/server/routes.h"

namespace glamdb::NServer {
// Runs until SIGINT/SIGTERM. Accepted core calls finish before returning.
void RunServer(TRoutes& routes,
    std::string_view address,
    uint16_t port,
    std::chrono::milliseconds timeout,
    size_t maxConnections = 96);
} // namespace glamdb::NServer
