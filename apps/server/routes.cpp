#include "apps/server/routes.h"
#include "apps/server/bulk.h"
#include "apps/server/databases.h"
#include "apps/server/metadata.h"
#include "apps/server/scans.h"
#include "core/api/binary.h"

#include "apps/transport/outcome_json.h"
#include "core/api/canonical.h"

#include "storage/resources.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <fstream>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
std::string Normalized(std::string_view text)
{
    const auto first = text.find_first_not_of(" \t");
    if (first == std::string_view::npos) {
        return {};
    }
    std::string result(text.substr(first, text.find_last_not_of(" \t") - first + 1));
    for (auto& c : result) {
        if (c >= 'A' && c <= 'Z') {
            c = static_cast<char>(c + ('a' - 'A'));
        }
    }
    return result;
}
std::optional<size_t> MediaSeparator(std::string_view text, char separator)
{
    bool quoted = false, escaped = false;
    for (size_t end = 0; end < text.size(); ++end) {
        const auto c = text[end];
        if (escaped) {
            escaped = false;
        } else if (quoted && c == '\\') {
            escaped = true;
        } else if (c == '"') {
            quoted = !quoted;
        } else if (!quoted && c == separator) {
            return end;
        }
    }
    if (quoted || escaped) {
        return {};
    }
    return std::string_view::npos;
}
template <class F>
bool MediaParameters(std::string_view text, F&& consume)
{
    while (!text.empty()) {
        const auto separator = MediaSeparator(text, ';');
        if (!separator) {
            return false;
        }
        const auto end = *separator;
        const auto parameter = text.substr(0, end);
        const auto equals = parameter.find('=');
        if (equals == std::string_view::npos) {
            return false;
        }
        const auto name = Normalized(parameter.substr(0, equals));
        const auto value = Normalized(parameter.substr(equals + 1));
        if (name.empty() || value.empty() || !consume(name, value)) {
            return false;
        }
        if (end == std::string_view::npos) {
            return true;
        }
        text.remove_prefix(end + 1);
        if (text.empty()) {
            return false;
        }
    }
    return true;
}
bool IsJsonContentType(std::string_view text)
{
    const auto semi = text.find(';');
    if (Normalized(text.substr(0, semi)) != "application/json") {
        return false;
    }
    if (semi == std::string_view::npos) {
        return true;
    }
    bool charset = false;
    return MediaParameters(text.substr(semi + 1), [&](const auto& name, const auto& value) {
        if (name != "charset") {
            return true;
        }
        const bool valid = !charset && (value == "utf-8" || value == "\"utf-8\"");
        charset = true;
        return valid;
    });
}
std::optional<bool> ArrowAccepted(std::string_view text)
{
    struct TChoice
    {
        int Specificity = -1;
        int Quality = 0;
    };
    TChoice json, arrow;
    while (!text.empty()) {
        const auto separator = MediaSeparator(text, ',');
        Require(separator.has_value(), "invalid Accept parameter");
        const auto comma = separator.value_or(std::string_view::npos);
        auto item = text.substr(0, comma);
        const auto semi = item.find(';');
        const auto media = Normalized(item.substr(0, semi));
        int quality = 1000;
        if (semi != std::string_view::npos) {
            bool seenQuality = false;
            Require(MediaParameters(item.substr(semi + 1),
                        [&](const auto& name, const auto& value) {
                            if (name != "q") {
                                return true;
                            }
                            Require(!seenQuality, "duplicate Accept quality");
                            seenQuality = true;
                            Require(!value.empty() && (value[0] == '0' || value[0] == '1') &&
                                    (value.size() == 1 || (value.size() <= 5 && value[1] == '.')),
                                "invalid Accept quality");
                            quality = value[0] == '1' ? 1000 : 0;
                            int scale = 100;
                            for (size_t i = 2; i < value.size(); ++i) {
                                Require(value[i] >= '0' && value[i] <= '9' && (value[0] != '1' || value[i] == '0'),
                                    "invalid Accept quality");
                                quality += (value[i] - '0') * scale;
                                scale /= 10;
                            }
                            return true;
                        }),
                "invalid Accept parameter");
        }
        const auto match = [&](std::string_view type, TChoice& choice) {
            const int specificity = media == type ? 2 : media == "application/*" ? 1 : media == "*/*" ? 0 : -1;
            if (specificity > choice.Specificity || (specificity == choice.Specificity && quality > choice.Quality)) {
                choice = {specificity, quality};
            }
        };
        match("application/json", json);
        match("application/vnd.apache.arrow.stream", arrow);
        if (comma == std::string_view::npos) {
            break;
        }
        text.remove_prefix(comma + 1);
    }
    if (json.Specificity < 0) {
        json.Quality = 0;
    }
    if (arrow.Specificity < 0) {
        arrow.Quality = 0;
    }
    if (json.Quality == 0 && arrow.Quality == 0) {
        return {};
    }
    return arrow.Quality > json.Quality;
}

bool IsApiPath(std::string_view path)
{
    return path == "/storage" || path == "/databases" || path.starts_with("/databases/") || path == "/capabilities" ||
        path == "/readyz" || path == "/collections" || path == "/collections:resolve" ||
        path.starts_with("/collections/");
}
TResponse JsonResponse(const TJson& value)
{
    auto body = value.dump();
    Require(body.size() <= MaxResponseBytes, "response exceeds transport limit", "INPUT_TOO_LARGE");
    return {200, std::move(body)};
}
TResponse OutcomeResponse(Result<json_adapter::Response> result)
{
    auto response = Unwrap(std::move(result));
    return {response.status, std::move(response.body)};
}
TJson Collection(const CollectionInfo& info)
{
    return TJson::parse(Unwrap(json_adapter::CollectionJson(info)));
}
std::vector<std::string> Split(const std::string& path)
{
    std::vector<std::string> parts;
    size_t position = 1;
    while (position <= path.size()) {
        const auto slash = path.find('/', position);
        parts.push_back(path.substr(position, slash == std::string::npos ? slash : slash - position));
        if (slash == std::string::npos) {
            break;
        }
        position = slash + 1;
    }
    return parts;
}
} // namespace

TRoutes::TRoutes(std::shared_ptr<Engine> engine, std::filesystem::path uiRoot, std::string token, TScanLimits scans)
    : Resources_(engine->GetResourceOwner())
    , Scans_(std::make_shared<TScans>(scans, Resources_))
    , Metadata_(std::make_shared<TMetadata>())
    , DatabaseID_(engine->GetStoreInfo().store_id)
    , Engine_(std::move(engine))
    , UIRoot_(std::move(uiRoot))
    , Token_(std::move(token))
{ }

TRoutes::TRoutes(std::shared_ptr<TDatabases> databases,
    std::filesystem::path uiRoot,
    std::string token,
    TScanLimits scans)
    : Resources_(databases->GetResourceOwner())
    , Scans_(std::make_shared<TScans>(scans, Resources_))
    , Metadata_(std::make_shared<TMetadata>())
    , Databases_(std::move(databases))
    , UIRoot_(std::move(uiRoot))
    , Token_(std::move(token))
{
    std::weak_ptr<TScans> weak = Scans_;
    std::weak_ptr<TMetadata> metadata = Metadata_;
    Databases_->SetScanCanceller([weak, metadata](const std::string& database) {
        if (const auto scans = weak.lock()) {
            scans->Clear(database);
        }
        if (const auto pages = metadata.lock()) {
            pages->Clear(database);
        }
    });
}

std::shared_ptr<ResourceOwner> TRoutes::GetResourceOwner() const
{
    return Resources_;
}

TJson TRoutes::DescribeCapabilities(const EngineCapabilities& capabilities,
    const std::optional<StoreHealth>& health,
    bool databaseManagement,
    const std::string& storeID,
    const Limits& limits) const
{
    auto value = CapabilitiesJson(capabilities, health, databaseManagement, storeID, Scans_->GetLimits(), limits);
    value["features"]["database_drop"] = static_cast<bool>(Databases_);
    value["features"]["bulk_append"] = true;
    value["limits"]["bulk_request_bytes"] = 4ULL << 30;
    value["limits"]["bulk_batch_bytes"] = MaxRequestBytes;
    value["limits"]["bulk_uploads"] = 2;
    value["limits"]["bulk_uploads_per_database"] = 1;
    value["scan_sessions"] = Scans_->GetStatistics(storeID);
    const auto usage = Resources_->GetUsage();
    value["shared_resources"] = {{"memory_limits_bytes", Resources_->GetLimits().memory_bytes},
        {"memory_bytes", usage.memory_bytes},
        {"memory_peak_bytes", usage.memory_peak}};
    THttpActivity activity;
    {
        std::lock_guard lock(HttpObservation_->Mutex);
        activity = HttpObservation_->Activity;
    }
    value["http_activity"] = {{"connected_clients", activity.Connected},
        {"pending_calls", activity.Pending},
        {"executing_calls", activity.Executing},
        {"waiting_bodies", activity.BodyWaiters},
        {"oldest_queue_age_ms", activity.OldestQueueAgeMs}};
    return value;
}

void TRoutes::SetHttpActivity(THttpActivity activity)
{
    std::lock_guard lock(HttpObservation_->Mutex);
    HttpObservation_->Activity = activity;
}

bool TRoutes::IsAuthorized(const TRequest& request) const
{
    const auto path = DecodeUrl(request.Target.substr(0, request.Target.find('?')));
    if (!IsApiPath(path)) {
        return true;
    }
    const auto found = request.Headers.find("authorization");
    if (found == request.Headers.end()) {
        return false;
    }
    const auto expected = "Bearer " + Token_;
    size_t diff = expected.size() ^ found->second.size();
    for (size_t i = 0; i < expected.size(); ++i) {
        diff |= static_cast<unsigned char>(expected[i]) ^
            static_cast<unsigned char>(i < found->second.size() ? found->second[i] : 0);
    }
    return diff == 0;
}

TResponse TRoutes::Handle(const TRequest& request) const
{
    try {
        if (!IsAuthorized(request)) {
            return ErrorResponse(401, "UNAUTHORIZED", "bearer token required");
        }
        const bool protectedCall = UsesProtectedAdmission(request) && request.Body.size() <= (64ULL << 10);
        const auto resource = protectedCall ? ResourceClass::Completion : ResourceClass::Data;
        const auto [path, query] = ParseTarget(request.Target);
        (void)query;
        const bool scan = request.Method == "POST" &&
            (path.starts_with("/collections/") || path.starts_with("/databases/")) &&
            (path.ends_with("/scan") || path.ends_with("/scan/open"));
        // JSON DOM/cells/Arrow copies and response assembly coexist. Ordinary
        // requests cannot use protected lookup/lifecycle capacity.
        // Scan assembly is admitted by TScans, where completed replay can be
        // reclaimed. Only request parsing belongs to this earlier boundary.
        auto work = Unwrap(Resources_->Reserve(resource,
            protectedCall ? 4ULL << 20 : (scan ? 64ULL << 10 : 128ULL << 20) + request.Body.size() * 32));
        auto response = Route(request);
        Require(response.Body.size() <= MaxResponseBytes, "response exceeds transport limit", "INPUT_TOO_LARGE");
        if (!response.ResourceLease) {
            response.ResourceLease = Unwrap(
                Resources_->Reserve(resource, static_cast<uint64_t>(response.Body.capacity()) * 2 + (64ULL << 10)));
        }
        return response;
    } catch (const TScanAdmissionFailure& error) {
        return ScanAdmissionResponse(error);
    } catch (const TFailure& error) {
        return {ErrorStatus(error.Detail), EncodeError(error.Detail).dump()};
    } catch (const detail::Failure& error) {
        return {ErrorStatus(error.detail), EncodeError(error.detail).dump()};
    } catch (const TJson::exception&) {
        return ErrorResponse(400, "INVALID_ARGUMENT", "invalid JSON request");
    } catch (const std::bad_alloc&) {
        return ErrorResponse(500,
            "INTERNAL",
            "request allocation failed; resolve write outcome by original ID",
            RetryHint::SameIdAfterReconcile);
    } catch (const std::exception&) {
        return ErrorResponse(500,
            "INTERNAL",
            "request failed; resolve write outcome by original ID",
            RetryHint::SameIdAfterReconcile);
    }
}

TResponse TRoutes::Route(const TRequest& request) const
{
    const auto [path, query] = ParseTarget(request.Target);
    Require(path != "/healthz" || request.Method == "GET", "health requires GET");
    TJson body;
    TJsonRows rows;
    if (request.Method == "POST") {
        const auto type = request.Headers.find("content-type");
        if (IsBulkUpload(request)) {
            Require(HasBulkContentType(request), "Content-Type must be application/x-ndjson", "UNSUPPORTED_MEDIA_TYPE");
            body = ReadBulkHeader(request);
            return RoutePrepared(request, path, query, body);
        }
        if (type == request.Headers.end() || !IsJsonContentType(type->second)) {
            return ErrorResponse(415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");
        }
        Require(query.empty(), "POST query parameters are unsupported");
        // Append rows go straight from the parser's compact form to Arrow
        // builders once the collection schema is known; no DOM is built for them.
        if (path.ends_with("/append")) {
            body = ParseJson(request.Body, rows);
        } else {
            body = ParseJson(request.Body);
        }
        Require(body.is_object(), "request body must be an object");
    }
    return RoutePrepared(request, path, query, body, &rows);
}

TResponse TRoutes::RoutePrepared(const TRequest& request,
    const std::string& path,
    const std::map<std::string, std::string>& query,
    const TJson& body,
    const TJsonRows* rows) const
{
    if (request.Method != "GET" && request.Method != "POST") {
        return ErrorResponse(405, "METHOD_NOT_ALLOWED", "use GET or POST");
    }
    if (!IsApiPath(path)) {
        Require(request.Method == "GET", "static resources require GET");
        if (path == "/healthz") {
            return JsonResponse(
                {{"status", "live"}, {"backend", "local"}, {"mock", false}, {"durability_validation", "designed"}});
        }
        return StaticFile(path);
    }
    if (request.Method == "GET" && path == "/readyz") {
        Require(query.empty(), "unknown query parameter");
        Require(static_cast<bool>(Engine_), "select a database using /databases/{id}", "DATABASE_REQUIRED");
        const auto health = Engine_->GetHealth();
        return ReadinessResponse(health);
    }
    if (request.Method == "GET" && path == "/capabilities") {
        Require(query.empty(), "unknown capability parameter");
        return JsonResponse(DescribeCapabilities(Engine_ ? Engine_->GetCapabilities() : InstalledCapabilities(),
            Engine_ ? std::optional(Engine_->GetHealth()) : std::nullopt,
            static_cast<bool>(Databases_),
            Engine_ ? Engine_->GetStoreInfo().store_id : std::string{},
            Engine_ ? Engine_->GetLimits() : Limits{}));
    }
    if (path == "/databases" || path.starts_with("/databases/")) {
        Require(static_cast<bool>(Databases_), "database management requires --data-dir", "UNSUPPORTED_OPERATION");
        if (path == "/databases") {
            Require(query.empty(), "unknown query parameter");
            return JsonResponse(request.Method == "GET" ? Databases_->List() : Databases_->Create(body));
        }
        const auto parts = Split(path);
        if (parts.size() == 2 && request.Method == "GET") {
            Require(query.empty(), "unknown query parameter");
            return JsonResponse(Databases_->Get(parts[1]));
        }
        if (parts.size() == 3 && request.Method == "GET" && (parts[2] == "readyz" || parts[2] == "capabilities")) {
            Require(query.empty(), "unknown query parameter");
            const auto health = Databases_->Health(parts[1]);
            if (parts[2] == "readyz") {
                return ReadinessResponse(health);
            }
            try {
                return Databases_->Use(
                    parts[1],
                    [&](const auto& engine) {
                        return JsonResponse(DescribeCapabilities(engine->GetCapabilities(),
                            health,
                            false,
                            engine->GetStoreInfo().store_id,
                            engine->GetLimits()));
                    },
                    true);
            } catch (const TFailure& error) {
                Require(error.Detail.code == "DATABASE_BUSY" || error.Detail.code == "DATABASE_CLOSED" ||
                        error.Detail.code == "RECOVERY_REQUIRED",
                    error.Detail.message,
                    error.Detail.code);
                // Installed operations are independent of lifecycle admission.
                // Keep health inspectable while the per-store gate is unavailable.
                Limits limits;
                limits.max_collections = Databases_->Get(parts[1]).at("limits").at("max_collections").get<int64_t>();
                return JsonResponse(DescribeCapabilities(InstalledCapabilities(), health, false, parts[1], limits));
            }
        }
        if (parts.size() == 3 && request.Method == "POST" &&
            (parts[2] == "open" || parts[2] == "close" || parts[2] == "reconcile" || parts[2] == "discard" ||
                parts[2] == "drop"))
        {
            Require(query.empty(), "unknown lifecycle query parameter");
            return JsonResponse(Databases_->Change(parts[1], parts[2], body));
        }
        Require(parts.size() >= 3 &&
                (parts[2] == "collections" || parts[2] == "collections:resolve" || parts[2] == "capabilities" ||
                    parts[2] == "storage"),
            "route not found",
            "ROUTE_NOT_FOUND");
        // Validation and dispatch use the same once-decoded path and parsed body.
        const auto suffix = path.substr(path.find('/', std::string("/databases/").size()));
        const bool recoveryOnly = (request.Method == "GET" && parts.size() == 6 && parts[4] == "operations") ||
            (request.Method == "POST" && parts.size() == 6 && parts[4] == "scan" && parts[5] == "cancel");
        return Databases_->Use(
            parts[1],
            [&](const auto& engine) {
                auto routes = *this;
                routes.Engine_ = engine;
                routes.Databases_.reset();
                routes.DatabaseID_ = parts[1];
                return routes.RoutePrepared(request, suffix, query, body, rows);
            },
            recoveryOnly);
    }
    Require(static_cast<bool>(Engine_), "select a database using /databases/{id}", "DATABASE_REQUIRED");
    if (request.Method == "GET" && path == "/storage") {
        Require(query.empty(), "unknown physical inventory parameter");
        return JsonResponse(Footprint(Unwrap(Engine_->GetStorageFootprint())));
    }
    if (request.Method == "GET" && path == "/collections:resolve") {
        Require(query.size() == 1 && query.contains("name"), "exact name is required");
        return JsonResponse(Collection(Unwrap(Engine_->ResolveCollection(query.at("name")))));
    }
    if (request.Method == "GET" && path == "/collections") {
        for (const auto& [key, value] : query) {
            (void)value;
            Require(key == "page_size" || key == "cursor", "unknown query parameter");
        }
        int64_t pageSize = 100;
        if (query.contains("page_size")) {
            pageSize = ParseInt64(query.at("page_size"));
        }
        Require(pageSize >= 1 && pageSize <= 1000, "page_size must be 1..1000");
        Require(!query.contains("cursor") || !query.at("cursor").empty(), "cursor must be nonempty");
        const auto page = Unwrap(Engine_->ListCollectionsPage({.PageSize = pageSize,
            .PageToken = query.contains("cursor") ? query.at("cursor") : std::string{},
            .AuthorizationContext = DatabaseID_}));
        auto items = TJson::array();
        for (const auto& info : page.Collections) {
            items.push_back(Collection(info));
        }
        return JsonResponse({{"items", std::move(items)},
            {"creation_high_water_mark", std::to_string(page.CreationHighWaterMark)},
            {"next_cursor", page.NextPageToken.empty() ? TJson(nullptr) : TJson(page.NextPageToken)}});
    }
    if (request.Method == "POST" && path == "/collections") {
        CheckKeys(body, {"collection_id", "name", "schema", "mode", "write_settings"});
        Require(body.value("mode", "append") == "append",
            "only append collections are supported",
            "UNSUPPORTED_OPERATION");
        return OutcomeResponse(
            json_adapter::ToJson(Engine_->CreateCollection({body.at("collection_id").get<std::string>(),
                body.at("name").get<std::string>(),
                ParseSchema(body.at("schema")),
                body.contains("write_settings") ? ParseWriteSettings(body["write_settings"]) : WriteSettings{}})));
    }
    const auto parts = Split(path);
    Require(parts.size() >= 2 && parts[0] == "collections", "route not found", "ROUTE_NOT_FOUND");
    const auto& id = parts[1];
    if (request.Method == "GET" && parts.size() == 4 && parts[2] == "operations") {
        Require(query.empty(), "unknown query parameter");
        auto outcome = Unwrap(Engine_->FindAppendOutcome(id, parts[3]));
        if (!outcome.has_value()) {
            throw TFailure({"OPERATION_NOT_FOUND", "operation not found"});
        }
        return OutcomeResponse(json_adapter::ToJson(*outcome));
    }
    const std::string action = parts.size() >= 3 ? parts[2] : "";
    if (request.Method == "POST" && parts.size() == 3 && action == "bulk-append") {
        return BulkAppend(*Engine_, id, request, body);
    }
    if (request.Method == "POST" && parts.size() == 4 && action == "scan" && parts[3] == "cancel") {
        // Releasing a scoped cursor must remain possible when an unrelated
        // core failure has fenced catalog reads pending reconciliation.
        Require(query.empty(), "unknown query parameter");
        return Scans_->Cancel(DatabaseID_, id, body);
    }
    if (request.Method == "POST" && parts.size() == 3 && action == "write-settings") {
        Require(query.empty(), "unknown query parameter");
        CheckKeys(body, {"expected_version", "write_settings"});
        Require(body.contains("expected_version") && body.contains("write_settings"),
            "write settings require expected_version and write_settings");
        return JsonResponse(Collection(Unwrap(Engine_->UpdateWriteSettings(
            {id, ParseInt64(body["expected_version"]), ParseWriteSettings(body["write_settings"])}))));
    }
    if (request.Method == "POST" && parts.size() == 3 && action == "append") {
        Require(query.empty(), "unknown query parameter");
        CheckKeys(body, {"operation_id", "rows", "expected_snapshot"});
        AppendRequest append;
        append.collection_id = id;
        append.operation_id = body.at("operation_id").get<std::string>();
        if (body.contains("expected_snapshot")) {
            append.expected_snapshot = body["expected_snapshot"].is_null()
                ? SnapshotPrecondition{SnapshotPreconditionKind::NoSnapshot, 0}
                : SnapshotPrecondition{SnapshotPreconditionKind::Equals, ParseInt64(body["expected_snapshot"])};
        }
        const auto schema = Unwrap(Engine_->SchemaForAppend(id));
        const auto& rowsJson = body.at("rows");
        append.batches.push_back(
            rows != nullptr && rows->Present ? ParseRows(schema, *rows) : ParseRows(schema, rowsJson));
        return OutcomeResponse(json_adapter::ToJson(Engine_->AppendDurable(append)));
    }
    if (request.Method == "GET" && parts.size() == 3 && action == "stats") {
        return JsonResponse(StatisticsJson(*Engine_, id, query));
    }
    if (request.Method == "GET" && parts.size() == 3 && (action == "snapshots" || action == "files")) {
        // Continuations retain already-read values, but still obey current core
        // admission and collection ownership just like scan continuations.
        (void)Unwrap(Engine_->GetCollection(id));
        return JsonResponse(Metadata_->Page(DatabaseID_, id, action, query, [&](SnapshotSelector selector) {
            return action == "snapshots" ? SnapshotsJson(*Engine_, id, selector) : FilesJson(*Engine_, id, selector);
        }));
    }
    if (request.Method == "GET" && parts.size() == 4 && action == "files") {
        return JsonResponse(FileJson(*Engine_, id, parts[3], query));
    }
    if (request.Method == "POST" && parts.size() == 3 && action == "explain") {
        return JsonResponse(ExplainJson(*Engine_, id, body, Scans_->GetLimits()));
    }
    const auto info = Unwrap(Engine_->GetCollection(id));
    Require(query.empty(), "unknown query parameter");
    if (request.Method == "GET") {
        if (parts.size() == 2) {
            return JsonResponse(Collection(info));
        }
        if (parts.size() == 3 && action == "schema") {
            return JsonResponse(Collection(info).at("schema"));
        }
    }
    if (request.Method == "POST" && (parts.size() == 3 || (parts.size() == 4 && action == "scan"))) {
        if (action == "scan") {
            Require(parts.size() == 3 || parts[3] == "open", "route not found", "ROUTE_NOT_FOUND");
            const auto accept = request.Headers.find("accept");
            const auto media = accept == request.Headers.end() ? "application/json" : accept->second;
            const auto arrow = ArrowAccepted(media);
            if (!arrow.has_value()) {
                return ErrorResponse(406, "NOT_ACCEPTABLE", "use JSON or Arrow stream");
            }
            return Scans_->Open(Engine_, DatabaseID_, id, body, *arrow, parts.size() == 4);
        }
    }
    return ErrorResponse(404, "ROUTE_NOT_FOUND", "route not found");
}

void TRoutes::CancelScans() const
{
    Scans_->Stop();
    Metadata_->Clear();
}
void TRoutes::CancelScans(const std::string& database) const
{
    Scans_->Clear(database);
    Metadata_->Clear(database);
}

TResponse TRoutes::StaticFile(const std::string& path) const
{
    const auto relative = path == "/" ? "index.html"
        : path == "/workbench/config" ? "config/theme.json"
                                      : path.substr(1);
    if (UIRoot_.empty() ||
        !(relative == "index.html" || relative == "config/theme.json" || relative.starts_with("assets/")))
    {
        return ErrorResponse(404, "NOT_FOUND", "resource not found");
    }
    std::filesystem::path file = UIRoot_;
    for (const auto& component : std::filesystem::path(relative)) {
        if (component == ".." || component == "." || component.empty()) {
            return ErrorResponse(404, "NOT_FOUND", "resource not found");
        }
        file /= component;
        if (std::filesystem::is_symlink(std::filesystem::symlink_status(file))) {
            return ErrorResponse(404, "NOT_FOUND", "resource not found");
        }
    }
    if (!std::filesystem::is_regular_file(file) || std::filesystem::file_size(file) > MaxResponseBytes) {
        return ErrorResponse(404, "NOT_FOUND", "resource not found");
    }
    std::ifstream input(file, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    Require(input.good() || input.eof(), "cannot read UI asset", "IO_ERROR");
    const std::map<std::string, std::string> types{{".html", "text/html; charset=utf-8"},
        {".css", "text/css"},
        {".js", "text/javascript"},
        {".json", "application/json"},
        {".png", "image/png"},
        {".svg", "image/svg+xml"}};
    const auto type = types.find(file.extension().string());
    if (type == types.end()) {
        return ErrorResponse(404, "NOT_FOUND", "resource not found");
    }
    return {200, std::move(data), type->second};
}
} // namespace glamdb::NServer
