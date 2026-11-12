#include "data.h"

#include "apps/transport/metadata.h"
#include "apps/transport/outcome_json.h"

namespace glamdb::NAdmin {
using namespace NTransport;

int WriteDataResponse(const NClient::TResponse& response, const Status& shutdown, TCommandOutput streams)
{
    auto& output = streams.Data;
    auto& diagnostics = streams.Diagnostics;
    const auto result = NClient::DataExitCode(response);
    output << response.Body << '\n';
    output.flush();
    if (!shutdown) {
        diagnostics << "shutdown failed after completed operation: " << shutdown.error().code << ": "
                    << shutdown.error().message << "; original outcome is preserved\n";
    }
    if (!output) {
        diagnostics << "output failed after completed operation; a write may have committed\n";
    }
    if (result == 3) {
        return 3;
    }
    return !output || (!shutdown && result == 0) ? 4 : result;
}

namespace {
NClient::TResponse Reply(const TJson& value)
{
    auto bytes = value.dump();
    Require(bytes.size() <= MaxResponseBytes, "offline response exceeds 16 MiB", "RESOURCE_EXHAUSTED");
    return {200, {}, std::move(bytes)};
}
NClient::TResponse Outcome(Result<json_adapter::Response> result)
{
    auto response = Unwrap(std::move(result));
    return {response.status, {}, std::move(response.body)};
}
TJson Collection(const CollectionInfo& info)
{
    return ParseJson(Unwrap(json_adapter::CollectionJson(info)));
}
NClient::TResponse Scan(const Engine& engine, const std::string& collection, const TJson& body)
{
    const auto collectionInfo = Unwrap(engine.GetCollection(collection));
    const auto cursor = Unwrap(engine.Scan(ParseScan(collection, collectionInfo.schema, body)));
    const auto& info = cursor->Info();
    auto schema = EncodeSchema(*info.output_schema);
    for (auto& field : schema["fields"]) {
        for (size_t i = 0; i < collectionInfo.schema.fields.size(); ++i) {
            if (field["name"] == collectionInfo.schema.fields[i].name) {
                field["field_id"] = i + 1;
            }
        }
    }
    TJson value{{"schema", std::move(schema)},
        {"identity", EncodeIdentity(info.identity)},
        {"snapshot_id",
            info.selector_resolved.kind == SnapshotSelectorKind::Id
                ? TJson(std::to_string(info.selector_resolved.snapshot_id))
                : TJson(nullptr)},
        {"next_cursor", nullptr},
        {"profile", {{"rows_returned", "10000"}}}};
    const auto overhead = value.dump().size() + 10;
    Require(overhead < MaxResponseBytes, "scan schema exceeds response limit", "RESOURCE_EXHAUSTED");
    std::string rows = "[";
    size_t count = 0;
    for (;;) {
        const auto batch = Unwrap(cursor->Next());
        if (!batch) {
            break;
        }
        Require(static_cast<uint64_t>(batch->num_rows()) <= NClient::MaxScanRows - count,
            "complete scan exceeds 10000 rows; set limit or predicate",
            "RESOURCE_EXHAUSTED");
        const auto taken = AppendRowsJson(*batch,
            rows,
            {.MaxRows = NClient::MaxScanRows - count,
                .TargetBytes = MaxResponseBytes,
                .HardBytes = MaxResponseBytes - overhead});
        Require(taken == static_cast<size_t>(batch->num_rows()),
            "complete result exceeds 16 MiB; narrow the request",
            "RESOURCE_EXHAUSTED");
        count += taken;
    }
    rows += ']';
    value["profile"]["rows_returned"] = std::to_string(count);
    auto bytes = value.dump();
    bytes.pop_back();
    bytes += ",\"rows\":" + rows + '}';
    Require(bytes.size() <= MaxResponseBytes, "offline response exceeds 16 MiB", "RESOURCE_EXHAUSTED");
    return {200, {}, std::move(bytes)};
}
} // namespace

NClient::TResponse RunDataCommand(Engine& engine, const NClient::TDataCommand& data, const NClient::TRequest& request)
{
    const auto& collection = data.CollectionID;
    if (data.Command == "create-collection") {
        const auto body = ParseJson(request.Body);
        CheckKeys(body, {"collection_id", "name", "schema"});
        return Outcome(json_adapter::ToJson(engine.CreateCollection({body.at("collection_id").get<std::string>(),
            body.at("name").get<std::string>(),
            ParseSchema(body.at("schema"))})));
    }
    if (data.Command == "append") {
        TJsonRows rows;
        const auto body = ParseJson(request.Body, rows);
        CheckKeys(body, {"operation_id", "rows", "expected_snapshot"});
        AppendRequest append;
        append.collection_id = collection;
        append.operation_id = body.at("operation_id").get<std::string>();
        if (body.contains("expected_snapshot")) {
            append.expected_snapshot = body.at("expected_snapshot").is_null()
                ? SnapshotPrecondition{SnapshotPreconditionKind::NoSnapshot, 0}
                : SnapshotPrecondition{SnapshotPreconditionKind::Equals, ParseInt64(body.at("expected_snapshot"))};
        }
        const auto schema = Unwrap(engine.SchemaForAppend(collection));
        Require(rows.Present, "append rows are required");
        append.batches.push_back(ParseRows(schema, rows));
        return Outcome(json_adapter::ToJson(engine.AppendDurable(append)));
    }
    if (data.Command == "operation") {
        const auto outcome = Unwrap(engine.FindAppendOutcome(collection, data.OperationID));
        if (!outcome.has_value()) {
            Fail("operation not found", "OPERATION_NOT_FOUND");
        }
        return Outcome(json_adapter::ToJson(outcome.value()));
    }
    if (data.Command == "scan") {
        return Scan(engine, collection, ParseJson(request.Body));
    }
    if (data.Command == "collections") {
        auto items = TJson::array();
        for (const auto& info : Unwrap(engine.ListCollections())) {
            items.push_back(Collection(info));
        }
        return Reply({{"items", std::move(items)}, {"next_cursor", nullptr}});
    }
    if (data.Command == "resolve-collection") {
        return Reply(Collection(Unwrap(engine.ResolveCollection(data.Name))));
    }
    if (data.Command == "collection" || data.Command == "schema") {
        auto info = Collection(Unwrap(engine.GetCollection(collection)));
        return Reply(data.Command == "schema" ? info.at("schema") : info);
    }
    const std::map<std::string, std::string> query{{"snapshot_id", data.Snapshot}};
    if (data.Command == "file") {
        return Reply(FileJson(engine, collection, data.FileID, query));
    }
    auto value = data.Command == "snapshots" ? SnapshotsJson(engine, collection, MetadataSelector(query))
                                             : FilesJson(engine, collection, MetadataSelector(query));
    value["next_cursor"] = nullptr;
    return Reply(value);
}
} // namespace glamdb::NAdmin
