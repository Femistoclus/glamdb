#include "data.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <set>
#include <utility>

namespace glamdb::NClient {
namespace {
using TJson = nlohmann::json;
void Require(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

class TScanCleanup
{
public:
    explicit TScanCleanup(TRequest request)
        : Request_(std::move(request))
    {
        Request_.Url += "/cancel";
        Request_.TimeoutSeconds = std::min(Request_.TimeoutSeconds, 5U);
    }
    ~TScanCleanup()
    {
        if (!Cursor.empty()) {
            try {
                Request_.Body = TJson{{"cursor", Cursor}}.dump();
                SendRequest(Request_);
            } catch (...) {
                // Expiry remains the server's fallback; cleanup cannot hide
                // the original transport, protocol or result-size failure.
                return;
            }
        }
    }
    std::string Cursor;

private:
    TRequest Request_;
};
} // namespace

std::vector<CLI::App*> AddDataCommands(CLI::App& app, TDataCommand& data)
{
    std::vector<CLI::App*> commands;
    const std::pair<const char*, const char*> commandHelp[] = {
        {"create-collection", "Create an append or KV collection with a fixed schema"},
        {"collections", "List collections in the selected database"},
        {"resolve-collection", "Find a collection by its exact name"},
        {"collection", "Show collection identity, schema and settings"},
        {"schema", "Show the fixed collection schema and field IDs"},
        {"append", "Append rows without replacing duplicate keys; return the write outcome"},
        {"bulk-append", "Stream a replayable NDJSON file as one atomic append"},
        {"upsert", "Insert or replace rows by key in a KV collection"},
        {"delete", "Delete keys from a KV collection"},
        {"lookup", "Read rows by key from a KV collection"},
        {"compact", "Merge KV data files while preserving current values"},
        {"scan", "Read bounded JSON rows with optional snapshot, predicate and projection"},
        {"operation", "Look up the original mutation outcome by operation UUID"},
        {"snapshots", "List receipt-backed snapshot history"},
        {"files", "List committed Parquet files in a selected snapshot"},
        {"file", "Inspect a committed Parquet file's footer, statistics and row groups"},
        {"write-settings", "Update Parquet settings for later writes using an expected version"},
        {"snapshot-archive", "Show, set or unset how many newest snapshots stay in the Iceberg root"},
        {"archive-snapshots", "Move root snapshots above hot_snapshots to the archive (bounded, repeatable)"},
    };
    for (const auto& [name, description] : commandHelp) {
        auto* command = app.add_subcommand(name, description);
        commands.push_back(command);
        const std::string action(name);
        command->callback([&data, name]() noexcept {
            data.Command = name;
        });
        command->add_option("--database-id", data.DatabaseID, "Managed database UUID (remote only)");
        if (action != "collections" && action != "resolve-collection") {
            command->add_option("--collection-id", data.CollectionID, "Collection UUID")->required();
        }
        if (action == "create-collection" || action == "resolve-collection") {
            command->add_option("--name", data.Name, "Exact collection name")->required();
        }
        if (action == "create-collection") {
            command->add_option("--mode", data.Mode, "Immutable collection mode: append or kv")
                ->check(CLI::IsMember({"append", "kv"}));
            command->add_option("--schema", data.SchemaFile, "Schema JSON file, or - for stdin")->required();
            command->add_option("--write-settings",
                data.WriteSettingsFile,
                "Parquet write settings JSON file (part of the definition), or - for stdin");
            command->add_option("--hot-snapshots",
                data.HotSnapshots,
                "Newest snapshots kept in the Iceberg root; older ones are archived and stay readable "
                "(append collections, part of the definition)");
        }
        if (action == "snapshot-archive") {
            auto* set = command->add_option("--hot-snapshots",
                data.HotSnapshots,
                "Set hot_snapshots for later appends and archive maintenance");
            auto* unset = command->add_flag("--unset",
                data.UnsetHotSnapshots,
                "Stop archiving later snapshots; archived snapshots stay archived");
            set->excludes(unset);
            unset->excludes(set);
        }
        if (action == "write-settings") {
            command->add_option("--expected-version", data.ExpectedVersion, "Current write_settings_version")
                ->required();
            command
                ->add_option("--settings",
                    data.WriteSettingsFile,
                    "Parquet write settings JSON file for later appends, or - for stdin")
                ->required();
        }
        if (action == "append" || action == "upsert" || action == "delete" || action == "lookup" ||
            action == "compact" || action == "scan")
        {
            auto* body = command->add_option("--body", data.BodyFile, "Native JSON request file, or - for stdin");
            if (action != "scan") {
                body->required();
            }
        }
        if (action == "bulk-append") {
            command->add_option("--body", data.BodyFile, "Replayable NDJSON bulk request file")->required();
        }
        if (action == "operation") {
            command->add_option("--operation-id", data.OperationID, "Original mutation UUID")->required();
        }
        if (action == "file") {
            command->add_option("--file-id", data.FileID, "Opaque ID returned by files")->required();
        }
        if (action == "snapshots" || action == "files" || action == "file") {
            command->add_option("--snapshot", data.Snapshot, "latest, initial, or an exact snapshot ID");
        }
    }
    return commands;
}

std::string ReadDataFile(const std::string& file)
{
    std::ifstream source;
    if (file != "-") {
        source.open(file, std::ios::binary);
        Require(static_cast<bool>(source), "cannot open input file");
    }
    auto& stream = file == "-" ? std::cin : source;
    std::string bytes;
    char block[4096];
    while (stream.read(block, sizeof(block)) || stream.gcount() > 0) {
        const auto count = static_cast<size_t>(stream.gcount());
        Require(count <= MaxDataBytes - bytes.size(), "input body exceeds 16 MiB");
        bytes.append(block, count);
    }
    Require(!stream.bad(), "input read failed");
    return bytes;
}

std::string EncodeUrlComponent(std::string_view value)
{
    constexpr char Hex[] = "0123456789ABCDEF";
    std::string result;
    for (const unsigned char byte : value) {
        if ((byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') || (byte >= '0' && byte <= '9') ||
            byte == '-' || byte == '_' || byte == '.' || byte == '~')
        {
            result += static_cast<char>(byte);
        } else {
            result += '%';
            result += Hex[byte >> 4];
            result += Hex[byte & 15];
        }
    }
    return result;
}

TRequest BuildDataRequest(const TDataCommand& data)
{
    TRequest request;
    std::string prefix = data.DatabaseID.empty() ? "" : "/databases/" + EncodeUrlComponent(data.DatabaseID);
    request.Url = prefix + "/collections";
    if (data.Command == "create-collection") {
        request.Method = "POST";
        // Preserve the schema bytes for the shared strict native JSON decoder,
        // including duplicate-key rejection, rather than normalizing a DOM here.
        Require(data.SchemaFile != "-" || data.WriteSettingsFile != "-", "only one input can be read from stdin");
        request.Body = "{\"collection_id\":" + TJson(data.CollectionID).dump() +
            ",\"name\":" + TJson(data.Name).dump() + ",\"schema\":" + ReadDataFile(data.SchemaFile) +
            (data.Mode == "append" ? "" : ",\"mode\":" + TJson(data.Mode).dump()) +
            (data.WriteSettingsFile.empty() ? "" : ",\"write_settings\":" + ReadDataFile(data.WriteSettingsFile)) +
            (data.HotSnapshots.empty() ? "" : ",\"hot_snapshots\":" + TJson(data.HotSnapshots).dump()) + "}";
    } else if (data.Command == "collections") {
        request.Url += "?page_size=1000";
    } else if (data.Command == "resolve-collection") {
        request.Url += ":resolve?name=" + EncodeUrlComponent(data.Name);
    } else {
        request.Url += '/' + EncodeUrlComponent(data.CollectionID);
        if (data.Command == "operation") {
            request.Url += "/operations/" + EncodeUrlComponent(data.OperationID);
        } else if (data.Command == "archive-snapshots") {
            request.Url += "/snapshot-archive/maintenance";
        } else if (data.Command != "collection") {
            request.Url +=
                '/' + (data.Command == "file" ? "files/" + EncodeUrlComponent(data.FileID) : std::string(data.Command));
        }
        if (data.Command == "snapshot-archive" && (!data.HotSnapshots.empty() || data.UnsetHotSnapshots)) {
            request.Method = "POST";
            request.Body =
                TJson{{"hot_snapshots", data.UnsetHotSnapshots ? TJson(nullptr) : TJson(data.HotSnapshots)}}.dump();
        } else if (data.Command == "archive-snapshots") {
            request.Method = "POST";
            request.Body = "{}";
        } else if (data.Command == "write-settings") {
            request.Method = "POST";
            request.Body = "{\"expected_version\":" + TJson(data.ExpectedVersion).dump() +
                ",\"write_settings\":" + ReadDataFile(data.WriteSettingsFile) + "}";
        } else if (data.Command == "bulk-append") {
            Require(data.BodyFile != "-", "bulk append requires a regular replayable file");
            request.Method = "POST";
            request.ContentType = "application/x-ndjson";
            request.BodyFile = data.BodyFile;
        } else if (data.Command == "append" || data.Command == "upsert" || data.Command == "delete" ||
            data.Command == "lookup" || data.Command == "compact" || data.Command == "scan")
        {
            request.Method = "POST";
            request.Body = data.BodyFile.empty() ? "{}" : ReadDataFile(data.BodyFile);
        } else if (data.Command == "snapshots" || data.Command == "files" || data.Command == "file") {
            request.Url += "?snapshot_id=" + EncodeUrlComponent(data.Snapshot);
            if (data.Command != "file") {
                request.Url += "&page_size=1000";
            }
        }
    }
    Require(request.Body.size() <= MaxDataBytes, "input body exceeds 16 MiB");
    return request;
}

std::string JoinDataUrl(std::string base, const std::string& route)
{
    Require(!base.empty() && base.find_first_of("?#") == std::string::npos,
        "server base URL must not contain a query or fragment");
    while (base.ends_with('/')) {
        base.pop_back();
    }
    return base + route;
}

TResponse SendDataRequest(const TRequest& request, const TDataCommand& data)
try {
    auto response = SendRequest(request);
    const bool scan = data.Command == "scan";
    if ((!scan && data.Command != "snapshots" && data.Command != "files" && data.Command != "collections") ||
        response.Status < 200 || response.Status >= 300)
    {
        return response;
    }
    TScanCleanup cleanup(request);
    auto result = TJson::parse(response.Body);
    const auto* member = scan ? "rows" : "items";
    Require(result.at(member).is_array(), "invalid native result array");
    auto next = result.at("next_cursor");
    // Charge the final output once. Repeated schema/identity/cursor envelopes
    // belong to wire pages, not to the completed result.
    auto firstItems = std::move(result[member]);
    result[member] = TJson::array();
    result["next_cursor"] = nullptr;
    if (scan) {
        result["profile"] = {{"rows_returned", "10000"}};
    }
    size_t bytes = result.dump().size();
    const auto appendItems = [&](TJson& items) {
        for (auto& item : items) {
            const auto size = item.dump().size() + (result.at(member).empty() ? 0 : 1);
            Require(bytes <= MaxDataBytes && size <= MaxDataBytes - bytes,
                "complete result exceeds 16 MiB; narrow the request");
            bytes += size;
            result[member].push_back(std::move(item));
            Require(!scan || result.at(member).size() <= MaxScanRows,
                "complete scan exceeds 10000 rows; set limit or predicate");
        }
    };
    if (scan && next.is_string()) {
        cleanup.Cursor = next.get<std::string>();
    }
    appendItems(firstItems);
    std::set<std::string> seen;
    while (!next.is_null()) {
        Require(next.is_string() && next.get_ref<const std::string&>().size() <= 256, "invalid native cursor");
        const auto token = next.get<std::string>();
        if (scan) {
            cleanup.Cursor = token;
        }
        Require(seen.size() < 10000 && seen.insert(token).second, "invalid repeated native cursor");
        Require(bytes <= MaxDataBytes, "complete result exceeds 16 MiB; narrow the request");
        auto continuation = request;
        if (scan) {
            continuation.Body = TJson{{"cursor", token}}.dump();
        } else {
            continuation.Url += "&cursor=" + EncodeUrlComponent(token);
        }
        const auto page = SendRequest(continuation);
        Require(page.Status >= 200 && page.Status < 300, "native continuation failed; no result was output");
        auto value = TJson::parse(page.Body);
        next = value.at("next_cursor");
        if (scan) {
            // Track the newest token before checking values, so failure cleanup
            // releases the currently retained cursor rather than an older page.
            cleanup.Cursor = next.is_string() ? next.get<std::string>() : token;
        }
        if (data.Command == "collections") {
            Require(value.at("creation_high_water_mark") == result.at("creation_high_water_mark"),
                "native continuation changed its fixed inventory");
        } else {
            Require(value.at("identity") == result.at("identity") &&
                    value.at("snapshot_id") == result.at("snapshot_id"),
                "native continuation changed its fixed view");
        }
        if (scan) {
            Require(value.at("schema") == result.at("schema"), "native continuation changed schema");
        }
        Require(value.at(member).is_array(), "invalid native result array");
        appendItems(value.at(member));
    }
    cleanup.Cursor.clear();
    result["next_cursor"] = nullptr;
    if (scan) {
        Require(result.at(member).size() <= MaxScanRows, "complete scan exceeds 10000 rows; set limit or predicate");
        result["profile"] = {{"rows_returned", std::to_string(result.at(member).size())}};
    }
    response.Body = result.dump();
    Require(response.Body.size() <= MaxDataBytes, "complete result exceeds 16 MiB; narrow the request");
    // The bytes now represent a completed JSON inventory, not one wire page.
    response.Headers.erase("x-content-sha256");
    response.Headers.erase("x-glamdb-next-cursor");
    response.Headers["content-length"] = {std::to_string(response.Body.size())};
    return response;
} catch (const TOutcomeUnknown&) {
    if (data.Command != "scan") {
        throw;
    }
    throw std::runtime_error("scan transport failed before completion; no rows were output");
}

int DataExitCode(const TResponse& response)
{
    const auto value = TJson::parse(response.Body, nullptr, false);
    if (value.is_object()) {
        const auto outcome = value.find("outcome");
        if (outcome != value.end() && *outcome == "commit_unknown") {
            return 3;
        }
        const auto error = value.find("error");
        if (error != value.end() && error->is_object()) {
            const auto code = error->find("code");
            if (code != error->end() && (*code == "OPERATION_IN_PROGRESS" || *code == "RECOVERY_REQUIRED")) {
                return 3;
            }
        }
    }
    return response.Status >= 200 && response.Status < 300 ? 0 : 1;
}
} // namespace glamdb::NClient
