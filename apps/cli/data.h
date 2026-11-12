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
    // Parquet write settings JSON file (or - for stdin) and its expected version.
    std::string WriteSettingsFile, ExpectedVersion;
    // hot_snapshots decimal text for create-collection and snapshot-archive;
    // UnsetHotSnapshots sends null (stop archiving later snapshots).
    std::string HotSnapshots;
    bool UnsetHotSnapshots = false;
    std::string Snapshot = "latest";
    std::string Mode = "append";
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
