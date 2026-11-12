#pragma once

#include "apps/cli/data.h"
#include "storage/engine.h"

#include <ostream>

namespace glamdb::NAdmin {
struct TCommandOutput
{
    std::ostream& Data;
    std::ostream& Diagnostics;
};

NClient::TResponse RunDataCommand(Engine& engine, const NClient::TDataCommand& data, const NClient::TRequest& request);
int WriteDataResponse(const NClient::TResponse& response, const Status& shutdown, TCommandOutput streams);
} // namespace glamdb::NAdmin
