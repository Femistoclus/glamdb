#pragma once

#include "apps/transport/http.h"

namespace glamdb::NServer {
NTransport::TJson ReadBulkHeader(const NTransport::TRequest& request);
NTransport::TResponse BulkAppend(Engine& engine,
    const std::string& collection,
    const NTransport::TRequest& request,
    const NTransport::TJson& header);
} // namespace glamdb::NServer
