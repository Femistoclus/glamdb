#pragma once

#include "apps/client/http.h"

#include <CLI/CLI.hpp>

#include <cstddef>
#include <string_view>

namespace glamdb::NClient {
inline constexpr size_t MaxDataBytes = size_t{16} * 1024 * 1024;
inline constexpr size_t MaxScanRows = 10000;

struct TDataCommand
{
    std::string_view Command;
    std::string CollectionID, Name, SchemaFile, BodyFile, OperationID, FileID, DatabaseID;
    std::string Snapshot = "latest";
};

// The command grammar and request builder have no embedded-engine dependency.
std::vector<CLI::App*> AddDataCommands(CLI::App& app, TDataCommand& data);
std::string ReadDataFile(const std::string& file);
std::string EncodeUrlComponent(std::string_view value);
TRequest BuildDataRequest(const TDataCommand& data);
std::string JoinDataUrl(std::string base, const std::string& route);
// Completes bounded JSON scans and metadata inventories, following fixed-view
// continuations. Never retries mutations. Cancels unfinished remote scans.
TResponse SendDataRequest(const TRequest& request, const TDataCommand& data);
int DataExitCode(const TResponse& response);
} // namespace glamdb::NClient
