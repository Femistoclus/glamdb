#include "apps/server/metadata.h"
#include "apps/server/scans.h"

#include "apps/transport/outcome_json.h"
#include "core/scan/predicate.h"

#include <arrow/util/utf8.h>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <mutex>
#include <unordered_map>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
constexpr size_t MaxSessions = 64;
constexpr size_t MaxDatabaseSessions = 8;
constexpr size_t MaxRetainedBytes = size_t{64} * 1024 * 1024;
constexpr size_t MaxInventoryBytes = size_t{16} * 1024 * 1024;
TJson OptionalInteger(std::optional<int64_t> value)
{
    return value ? TJson(std::to_string(*value)) : TJson(nullptr);
}
TJson Snapshot(SnapshotSelector selector)
{
    return selector.kind == SnapshotSelectorKind::Id ? TJson(std::to_string(selector.snapshot_id)) : TJson(nullptr);
}
TJson Value(const Metric& metric)
{
    return OptionalInteger(metric.value);
}
std::string MetricKindName(const Metric& metric)
{
    const std::array names{"unavailable", "metadata", "estimate", "measured"};
    return names.at(static_cast<size_t>(metric.kind));
}
TJson NativeIdentity(const BaseIdentity& identity, SnapshotSelector selector)
{
    return {{"identity", EncodeIdentity(identity)},
        {"snapshot_id", Snapshot(selector)},
        {"backend", "local"},
        {"mock", false},
        {"synthetic", false},
        {"durability_validation", "designed"}};
}
TJson Statistics(const CollectionStatistics& statistics)
{
    auto value = NativeIdentity(statistics.identity, statistics.selector_resolved);
    value.update({{"rows", Value(statistics.rows)},
        {"data_files", Value(statistics.data_files)},
        {"compressed_data_bytes", Value(statistics.compressed_data_bytes)},
        {"logical_bytes", Value(statistics.logical_bytes)},
        {"root_metadata_bytes", nullptr},
        {"byte_measurement", "committed_data_file_descriptors"},
        {"metric_kinds",
            {{"rows", MetricKindName(statistics.rows)},
                {"data_files", MetricKindName(statistics.data_files)},
                {"compressed_data_bytes", MetricKindName(statistics.compressed_data_bytes)},
                {"logical_bytes", MetricKindName(statistics.logical_bytes)},
                {"root_metadata_bytes", "unavailable"}}}});
    return value;
}
size_t HistorySize(const SnapshotHistory& history, SnapshotSelector selector)
{
    if (selector.kind == SnapshotSelectorKind::InitialEmpty) {
        return 0;
    }
    if (selector.kind == SnapshotSelectorKind::Latest) {
        return history.snapshots.size();
    }
    for (size_t i = 0; i < history.snapshots.size(); ++i) {
        if (history.snapshots[i].snapshot_id == selector.snapshot_id) {
            return i + 1;
        }
    }
    throw TFailure({"SNAPSHOT_NOT_FOUND", "snapshot is not in the confirmed collection history", RetryHint::FixInput});
}
using TBound = std::optional<std::vector<uint8_t>>;
TJson BinaryHex(const TBound& bound)
{
    if (!bound) {
        return nullptr;
    }
    constexpr char Hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bound->size() * 2);
    for (const auto byte : *bound) {
        result += Hex[byte >> 4];
        result += Hex[byte & 15];
    }
    return result;
}
TJson DecodeBound(const TBound& bound, FieldType type)
{
    if (!bound) {
        return nullptr;
    }
    // Supported Iceberg single values and Parquet statistics share these scalar
    // encodings. Their provenance and original bytes remain separate on the wire.
    if (type == FieldType::String) {
        const std::string text(bound->begin(), bound->end());
        static std::once_flag Utf8;
        std::call_once(Utf8, arrow::util::InitializeUTF8);
        return arrow::util::ValidateUTF8(text) ? TJson(text) : TJson(nullptr);
    }
    if (type == FieldType::Bool) {
        return bound->size() == 1 && bound->front() <= 1 ? TJson(bound->front() != 0) : TJson(nullptr);
    }
    if (bound->size() != 8) {
        return nullptr;
    }
    uint64_t bits = 0;
    for (size_t i = 0; i < bound->size(); ++i) {
        bits |= static_cast<uint64_t>((*bound)[i]) << (8 * i);
    }
    if (type == FieldType::Float64) {
        const auto value = std::bit_cast<double>(bits);
        return std::isfinite(value) ? TJson(value) : TJson(nullptr);
    }
    const auto value = std::bit_cast<int64_t>(bits);
    if (type == FieldType::TimestampUsUtc) {
        return value >= -62135596800000000LL && value <= 253402300799999999LL ? EncodeCell(TimestampUs{value})
                                                                              : TJson(nullptr);
    }
    return type == FieldType::Int64 ? TJson(std::to_string(value)) : TJson(nullptr);
}
TJson FieldStatistics(int32_t fieldID,
    const SchemaSpec& schema,
    const TBound& lower,
    const TBound& upper,
    std::string_view encoding)
{
    Require(fieldID > 0 && static_cast<size_t>(fieldID) <= schema.fields.size(),
        "metadata contains an unknown field ID",
        "CORRUPTION");
    const auto& field = schema.fields[static_cast<size_t>(fieldID) - 1];
    const std::array names{"string", "bool", "int64", "float64", "timestamp_us_utc"};
    return {{"field_id", fieldID},
        {"name", field.name},
        {"type", names.at(static_cast<size_t>(field.type) - 1)},
        {"nullable", field.nullable},
        {"distinct_count", nullptr},
        {"lower_bound", DecodeBound(lower, field.type)},
        {"upper_bound", DecodeBound(upper, field.type)},
        {"lower_bound_hex", BinaryHex(lower)},
        {"upper_bound_hex", BinaryHex(upper)},
        {"bound_encoding", encoding},
        {"lower_bound_exact", nullptr},
        {"upper_bound_exact", nullptr}};
}
TJson ManifestMetrics(const std::vector<TFileFieldMetrics>& fields, const SchemaSpec& schema)
{
    auto value = TJson::array();
    for (const auto& field : fields) {
        auto metrics =
            FieldStatistics(field.FieldID, schema, field.LowerBound, field.UpperBound, "iceberg-single-value");
        metrics.update({{"null_count", OptionalInteger(field.NullCount)},
            {"value_count", OptionalInteger(field.ValueCount)},
            {"nan_count", OptionalInteger(field.NanCount)},
            {"compressed_bytes", OptionalInteger(field.CompressedBytes)},
            {"uncompressed_bytes", nullptr}});
        value.push_back(std::move(metrics));
    }
    return value;
}
TJson File(const TDataFileMetadata& file, const SchemaSpec& schema)
{
    return {{"file_id", file.FileID},
        {"path", nullptr},
        {"content", "data"},
        {"format", file.Format},
        {"record_count", std::to_string(file.RecordCount)},
        {"compressed_bytes", std::to_string(file.CompressedBytes)},
        {"field_metrics", ManifestMetrics(file.FieldMetrics, schema)},
        {"synthetic", false},
        {"physical_file_exists", nullptr},
        {"backend", "local"},
        {"mock", false}};
}
TJson Columns(const std::vector<TColumnChunkMetadata>& columns, const SchemaSpec& schema)
{
    auto value = TJson::array();
    for (const auto& column : columns) {
        auto metrics = FieldStatistics(column.FieldID, schema, column.LowerBound, column.UpperBound, "parquet-plain");
        metrics.update({{"null_count", OptionalInteger(column.NullCount)},
            {"value_count", std::to_string(column.ValueCount)},
            {"compressed_bytes", std::to_string(column.CompressedBytes)},
            {"uncompressed_bytes", std::to_string(column.UncompressedBytes)},
            {"data_page_offset", std::to_string(column.DataPageOffset)},
            {"dictionary_page_offset", OptionalInteger(column.DictionaryPageOffset)},
            {"compression", column.Compression},
            {"indexes",
                {{"page", column.HasColumnIndex || column.HasOffsetIndex},
                    {"column", column.HasColumnIndex},
                    {"offset", column.HasOffsetIndex},
                    {"bloom", nullptr}}}});
        value.push_back(std::move(metrics));
    }
    return value;
}
TJson LiteralJson(const OptionalLiteral& value)
{
    return value ? std::visit(
                       [](const auto& literal) {
                           return EncodeCell(Cell(literal));
                       },
                       *value)
                 : TJson(nullptr);
}
TJson PredicateJson(const Predicate& predicate)
{
    switch (predicate.kind()) {
        case Predicate::Kind::True:
            return {{"op", "true"}};
        case Predicate::Kind::Compare: {
            const std::array operators{"eq", "ne", "lt", "le", "gt", "ge"};
            return {{"op", operators.at(static_cast<size_t>(predicate.op()))},
                {"field", predicate.field()},
                {"value", LiteralJson(predicate.optional_value())}};
        }
        case Predicate::Kind::In: {
            auto values = TJson::array();
            for (const auto& value : predicate.values()) {
                values.push_back(LiteralJson(value));
            }
            return {{"op", "in"}, {"field", predicate.field()}, {"values", std::move(values)}};
        }
        case Predicate::Kind::IsNull:
        case Predicate::Kind::IsNotNull:
            return {{"op", predicate.kind() == Predicate::Kind::IsNull ? "is_null" : "is_not_null"},
                {"field", predicate.field()}};
        case Predicate::Kind::And:
        case Predicate::Kind::Or: {
            auto args = TJson::array();
            for (const auto& child : predicate.children()) {
                args.push_back(PredicateJson(child));
            }
            return {{"op", predicate.kind() == Predicate::Kind::And ? "and" : "or"}, {"args", std::move(args)}};
        }
        case Predicate::Kind::Not:
            return {{"op", "not"}, {"arg", PredicateJson(predicate.children().front())}};
    }
    throw TFailure({"INTERNAL", "unsupported validated predicate"});
}
int64_t Now()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
std::string Token()
{
    std::array<unsigned char, 24> bytes{};
    Require(RAND_bytes(bytes.data(), static_cast<int>(bytes.size())) == 1, "cannot create metadata cursor", "INTERNAL");
    constexpr char Hex[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        result += Hex[byte >> 4];
        result += Hex[byte & 15];
    }
    return result;
}
} // namespace

EngineCapabilities InstalledCapabilities()
{
#if GLAMDB_HTTP_STORAGE
    return {BackendKind::LocalStorage, true, true, true, false, false, false};
#else
    return {};
#endif
}

TJson CapabilitiesJson(const EngineCapabilities& capabilities,
    const std::optional<StoreHealth>& health,
    bool databaseManagement,
    const std::string& storeID)
{
    const bool metadata = capabilities.metadata;
    const std::array backends{"unavailable", "custom", "local"};
    TJson value{{"api_version", "v0"},
        {"health", health ? EncodeHealth(*health) : TJson(nullptr)},
        {"backend", backends.at(static_cast<size_t>(capabilities.backend))},
        {"mock", false},
        {"durability_validation", capabilities.durability_qualified ? "qualified" : "designed"},
        {"durability_qualified", capabilities.durability_qualified},
        {"file_pruning", capabilities.file_pruning},
        {"row_group_pruning", capabilities.row_group_pruning},
        {"page_pruning", false},
        {"bloom_pruning", false},
        {"collection_modes", {"append"}},
        {"features",
            {{"database_management", databaseManagement},
                {"append", capabilities.append},
                {"scan", capabilities.scan},
                {"scan_pagination", capabilities.scan},
                {"scan_cancel", capabilities.scan},
                {"arrow_export", capabilities.scan},
                {"stats", metadata},
                {"snapshots", metadata},
                {"files", metadata},
                {"explain", metadata},
                {"metadata_pagination", metadata},
                {"pagination", false},
                {"zero_column_projection", false}}},
        {"predicate_ops",
            {"true", "eq", "ne", "lt", "le", "gt", "ge", "in", "is_null", "is_not_null", "and", "or", "not"}},
        {"limits",
            {{"request_bytes", MaxRequestBytes},
                {"response_bytes", MaxResponseBytes},
                {"append_rows", 10000},
                {"scan_page_rows", 10000},
                {"scan_sessions", TScans::SessionLimit},
                {"scan_sessions_per_database", TScans::DatabaseSessionLimit},
                {"scan_lifetime_seconds", TScans::LifetimeSeconds},
                {"scan_replay_bytes", TScans::ReplayBytes},
                {"scan_idle_seconds", TScans::IdleSeconds},
                {"metadata_page_size", 1000},
                {"metadata_sessions", MaxSessions},
                {"metadata_sessions_per_database", MaxDatabaseSessions},
                {"metadata_lifetime_seconds", 300},
                {"metadata_idle_seconds", 60},
                {"metadata_retained_json_bytes", MaxRetainedBytes},
                {"metadata_inventory_json_bytes", MaxInventoryBytes},
                {"batch_rows", 65536},
                {"predicate_depth", detail::MaxPredicateDepth},
                {"predicate_nodes", detail::MaxPredicateNodes},
                {"predicate_literals", detail::MaxPredicateLiterals}}}};
    if (!storeID.empty()) {
        value["store_id"] = storeID;
    }
    return value;
}

SnapshotSelector MetadataSelector(const std::map<std::string, std::string>& query)
{
    const auto found = query.find("snapshot_id");
    if (found == query.end() || found->second == "latest") {
        return SnapshotSelector::Latest();
    }
    if (found->second == "initial") {
        return SnapshotSelector::InitialEmpty();
    }
    const auto id = ParseInt64(found->second);
    Require(std::to_string(id) == found->second, "snapshot_id must be a canonical decimal int64");
    return SnapshotSelector::Id(id);
}

TJson StatisticsJson(const Engine& engine,
    const std::string& collection,
    const std::map<std::string, std::string>& query)
{
    for (const auto& [key, ignored] : query) {
        (void)ignored;
        Require(key == "snapshot_id", "unknown statistics parameter");
    }
    const auto statistics = Unwrap(engine.GetStatistics(collection, MetadataSelector(query)));
    const auto history = Unwrap(engine.ListSnapshots(collection));
    // Appends may advance the second call's root; count only the history prefix
    // belonging to the first call's confirmed root. Snapshot expiration is absent.
    const auto head = statistics.identity.current_snapshot_id;
    const auto count = HistorySize(history, head ? SnapshotSelector::Id(*head) : SnapshotSelector::InitialEmpty());
    auto result = Statistics(statistics);
    result["retained_snapshots"] = std::to_string(count);
    return result;
}

TJson SnapshotsJson(const Engine& engine, const std::string& collection, SnapshotSelector selector)
{
    const auto history = Unwrap(engine.ListSnapshots(collection));
    const auto size = HistorySize(history, selector);
    const auto selected =
        size ? SnapshotSelector::Id(history.snapshots[size - 1].snapshot_id) : SnapshotSelector::InitialEmpty();
    auto result = NativeIdentity(history.identity, selected);
    auto items = TJson::array();
    for (size_t i = 0; i < size; ++i) {
        const auto& snapshot = history.snapshots[i];
        AppendOutcome outcome;
        outcome.disposition = CommitDisposition::Committed;
        outcome.phase = CommitPhase::Complete;
        outcome.operation_id = snapshot.receipt.operation_id;
        outcome.result = snapshot.receipt;
        const auto receipt = TJson::parse(Unwrap(json_adapter::ToJson(outcome)).body).at("result");
        items.push_back({{"snapshot_id", std::to_string(snapshot.snapshot_id)},
            {"parent_id", OptionalInteger(snapshot.parent_snapshot_id)},
            {"parent_snapshot_id", OptionalInteger(snapshot.parent_snapshot_id)},
            {"created_at_us", std::to_string(snapshot.created_at_us)},
            {"rows_added", std::to_string(snapshot.receipt.rows_appended)},
            {"operation_id", snapshot.receipt.operation_id},
            {"receipt", receipt},
            {"backend", "local"},
            {"mock", false}});
    }
    result["items"] = std::move(items);
    return result;
}

TJson FilesJson(const Engine& engine, const std::string& collection, SnapshotSelector selector)
{
    const auto info = Unwrap(engine.GetCollection(collection));
    const auto inventory = Unwrap(engine.ListFiles(collection, selector));
    auto result = NativeIdentity(inventory.Identity, inventory.SelectorResolved);
    auto items = TJson::array();
    for (const auto& file : inventory.Files) {
        items.push_back(File(file, info.schema));
    }
    result["items"] = std::move(items);
    return result;
}

TJson FileJson(const Engine& engine,
    const std::string& collection,
    const std::string& file,
    const std::map<std::string, std::string>& query)
{
    for (const auto& [key, ignored] : query) {
        (void)ignored;
        Require(key == "snapshot_id", "unknown file parameter");
    }
    const auto info = Unwrap(engine.GetCollection(collection));
    const auto details = Unwrap(engine.GetFileDetails(collection, file, MetadataSelector(query)));
    auto value = NativeIdentity(details.Identity, details.SelectorResolved);
    value.update(File(details.File, info.schema));
    value["physical_file_exists"] = true;
    value["footer_bytes"] = std::to_string(details.FooterBytes);
    value["row_groups"] = TJson::array();
    bool columnIndex = false, offsetIndex = false;
    for (const auto& group : details.RowGroups) {
        for (const auto& column : group.Columns) {
            columnIndex |= column.HasColumnIndex;
            offsetIndex |= column.HasOffsetIndex;
        }
        value["row_groups"].push_back({{"ordinal", group.Ordinal},
            {"record_count", std::to_string(group.RecordCount)},
            {"compressed_bytes", std::to_string(group.CompressedBytes)},
            {"uncompressed_bytes", std::to_string(group.UncompressedBytes)},
            {"field_metrics", Columns(group.Columns, info.schema)},
            {"synthetic", false}});
    }
    // A single row group's values are file-wide observations. Multiple groups
    // remain separate rather than manufacturing aggregate extrema or counts.
    value["footer_statistics"] =
        details.RowGroups.size() == 1 ? value["row_groups"].at(0).at("field_metrics") : TJson(nullptr);
    value["indexes"] = {{"page", columnIndex || offsetIndex},
        {"column", columnIndex},
        {"offset", offsetIndex},
        {"bloom", nullptr}};
    value["execution_capabilities"] = {{"file_pruning", false},
        {"row_group_pruning", false},
        {"page_pruning", false},
        {"bloom_pruning", false}};
    return value;
}

TJson ExplainJson(const Engine& engine, const std::string& collection, const TJson& body)
{
    const auto info = Unwrap(engine.GetCollection(collection));
    const auto explanation = Unwrap(engine.ExplainScan(ParseScan(collection, info.schema, body)));
    auto value = NativeIdentity(explanation.info.identity, explanation.info.selector_resolved);
    auto output = TJson::array();
    const auto schema = EncodeSchema(*explanation.info.output_schema);
    for (const auto& field : schema.at("fields")) {
        output.push_back(field.at("name"));
    }
    auto capabilities =
        CapabilitiesJson(engine.GetCapabilities(), engine.GetHealth(), false, engine.GetStoreInfo().store_id);
    value.update({{"schema", schema},
        {"read_columns", explanation.read_fields},
        {"output_columns", output},
        {"predicate", PredicateJson(explanation.predicate)},
        {"limit", OptionalInteger(explanation.limit)},
        {"batch_rows", std::to_string(explanation.batch_rows)},
        {"plan",
            {"resolve confirmed fixed view",
                "read committed candidate files",
                "exact SQL three-valued filter",
                "projection",
                "limit"}},
        {"candidates", Statistics(explanation.candidates)},
        {"estimates",
            {{"rows_before_filter", Value(explanation.candidates.rows)},
                {"data_files", Value(explanation.candidates.data_files)},
                {"output_rows", Value(explanation.estimated_output_rows)},
                {"compressed_bytes", Value(explanation.candidates.compressed_data_bytes)},
                {"output_rows_kind", MetricKindName(explanation.estimated_output_rows)},
                {"synthetic", false}}},
        {"actual_io", nullptr},
        {"capabilities", capabilities}});
    return value;
}

struct TMetadata::TImpl
{
    struct TSession
    {
        std::string Database, Collection, Kind, Selector;
        size_t PageSize = 100;
        size_t Bytes = 0;
        int64_t Created = 0;
        int64_t Touched = 0;
        bool Completed = false;
        TJson Value;
        std::vector<std::string> Tokens;
    };
    struct TPage
    {
        std::shared_ptr<TSession> Session;
        size_t Position = 0;
    };
    std::mutex Mutex;
    std::unordered_map<std::string, TPage> Pages;
    std::vector<std::shared_ptr<TSession>> Sessions;
    size_t Bytes = 0;
    int64_t Idle;
    int64_t Lifetime;
    TImpl(std::chrono::milliseconds idle, std::chrono::milliseconds lifetime)
        : Idle(idle.count())
        , Lifetime(lifetime.count())
    { }
    void Erase(const std::function<bool(const TSession&)>& remove)
    {
        std::erase_if(Sessions, [&](const auto& session) {
            if (!remove(*session)) {
                return false;
            }
            for (const auto& token : session->Tokens) {
                Pages.erase(token);
            }
            Bytes -= session->Bytes;
            return true;
        });
    }
    void Expire(int64_t now)
    {
        Erase([&](const auto& session) {
            return now - session.Created >= Lifetime || now - session.Touched >= Idle;
        });
    }
    void Reclaim(const std::string& database, size_t incomingBytes)
    {
        for (;;) {
            const bool databaseFull = std::count_if(Sessions.begin(), Sessions.end(), [&](const auto& session) {
                return session->Database == database;
            }) >= static_cast<std::ptrdiff_t>(MaxDatabaseSessions);
            if (!databaseFull && Sessions.size() < MaxSessions && incomingBytes <= MaxRetainedBytes - Bytes) {
                return;
            }
            const auto victim = std::find_if(Sessions.begin(), Sessions.end(), [&](const auto& session) {
                return session->Completed && (!databaseFull || session->Database == database);
            });
            if (victim == Sessions.end()) {
                return;
            }
            const auto* retired = victim->get();
            Erase([&](const auto& session) {
                return &session == retired;
            });
        }
    }
    TJson Slice(const TSession& session, size_t position) const
    {
        auto result = TJson::object();
        for (const auto& [key, value] : session.Value.items()) {
            if (key != "items") {
                result[key] = value;
            }
        }
        const auto& items = session.Value.at("items");
        const auto end = std::min(items.size(), position + session.PageSize);
        result["items"] = TJson::array();
        for (size_t i = position; i < end; ++i) {
            result["items"].push_back(items[i]);
        }
        result["next_cursor"] =
            end < items.size() ? TJson(session.Tokens.at(end / session.PageSize - 1)) : TJson(nullptr);
        Require(result.dump().size() <= MaxResponseBytes, "metadata page exceeds response limit", "RESOURCE_EXHAUSTED");
        return result;
    }
};

TMetadata::TMetadata(std::chrono::milliseconds idle, std::chrono::milliseconds lifetime)
    : Impl_(std::make_unique<TImpl>(idle, lifetime))
{ }
TMetadata::~TMetadata() = default;

TJson TMetadata::Page(const std::string& database,
    const std::string& collection,
    const std::string& kind,
    const std::map<std::string, std::string>& query,
    const std::function<TJson(SnapshotSelector)>& read)
{
    for (const auto& [key, ignored] : query) {
        (void)ignored;
        Require(key == "snapshot_id" || key == "page_size" || key == "cursor", "unknown metadata page parameter");
    }
    const auto selector = MetadataSelector(query);
    const auto selectorText = query.contains("snapshot_id") ? query.at("snapshot_id") : "latest";
    const auto sizeText = query.contains("page_size") ? query.at("page_size") : "100";
    const auto size = ParseInt64(sizeText);
    Require(size >= 1 && size <= 1000 && std::to_string(size) == sizeText, "page_size must be 1..1000");
    const auto pageSize = static_cast<size_t>(size);
    if (query.contains("cursor")) {
        const auto& cursor = query.at("cursor");
        std::lock_guard lock(Impl_->Mutex);
        const auto now = Now();
        Impl_->Expire(now);
        const auto found = Impl_->Pages.find(cursor);
        Require(cursor.size() == 48 && found != Impl_->Pages.end(),
            "metadata cursor not found or expired",
            "NOT_FOUND");
        const auto& session = *found->second.Session;
        Require(session.Database == database && session.Collection == collection,
            "metadata cursor not found or expired",
            "NOT_FOUND");
        Require(session.Kind == kind && session.Selector == selectorText && session.PageSize == pageSize,
            "metadata cursor parameters do not match");
        const auto result = Impl_->Slice(session, found->second.Position);
        if (result.at("next_cursor").is_null()) {
            found->second.Session->Completed = true;
        }
        found->second.Session->Touched = now;
        return result;
    }
    auto value = read(selector);
    const auto bytes = value.dump().size();
    Require(bytes <= MaxInventoryBytes, "metadata inventory exceeds retained limit", "RESOURCE_EXHAUSTED");
    TImpl::TSession session{.Database = database,
        .Collection = collection,
        .Kind = kind,
        .Selector = selectorText,
        .PageSize = pageSize,
        .Bytes = bytes,
        .Created = Now(),
        .Touched = Now(),
        .Value = std::move(value),
        .Tokens = {}};
    const auto count = session.Value.at("items").size();
    if (count <= pageSize) {
        return Impl_->Slice(session, 0);
    }
    // Include token overhead before reclaiming completed replay inventories.
    session.Bytes += (count - 1) / pageSize * 256;
    Require(session.Bytes <= MaxRetainedBytes, "metadata cursor capacity reached", "RESOURCE_EXHAUSTED");
    std::lock_guard lock(Impl_->Mutex);
    Impl_->Expire(Now());
    Impl_->Reclaim(database, session.Bytes);
    Require(Impl_->Sessions.size() < MaxSessions && session.Bytes <= MaxRetainedBytes - Impl_->Bytes,
        "metadata retained page capacity reached",
        "RESOURCE_EXHAUSTED");
    Require(std::count_if(Impl_->Sessions.begin(),
                Impl_->Sessions.end(),
                [&](const auto& existing) {
                    return existing->Database == database;
                }) < static_cast<std::ptrdiff_t>(MaxDatabaseSessions),
        "database metadata page capacity reached",
        "RESOURCE_EXHAUSTED");
    auto retained = std::make_shared<TImpl::TSession>(std::move(session));
    try {
        for (size_t position = pageSize; position < count; position += pageSize) {
            auto token = Token();
            while (Impl_->Pages.contains(token)) {
                token = Token();
            }
            retained->Tokens.push_back(token);
            Impl_->Pages.emplace(std::move(token), TImpl::TPage{retained, position});
        }
        const auto result = Impl_->Slice(*retained, 0);
        Impl_->Sessions.push_back(retained);
        Impl_->Bytes += retained->Bytes;
        return result;
    } catch (...) {
        for (const auto& token : retained->Tokens) {
            Impl_->Pages.erase(token);
        }
        throw;
    }
}
void TMetadata::Clear(const std::string& database)
{
    std::lock_guard lock(Impl_->Mutex);
    Impl_->Erase([&](const auto& session) {
        return session.Database == database;
    });
}
void TMetadata::Clear()
{
    std::lock_guard lock(Impl_->Mutex);
    Impl_->Pages.clear();
    Impl_->Sessions.clear();
    Impl_->Bytes = 0;
}
} // namespace glamdb::NServer
