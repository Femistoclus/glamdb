#include "core/catalog/durable_sqlite_catalog.h"

#include "core/api/canonical.h"
#include "core/api/codec.h"

#include <algorithm>
#include <exception>
#include <limits>

namespace glamdb::detail {
namespace {

constexpr std::string_view collection_columns = "collection_id,name,table_uuid,schema_blob,creation_sequence,current_"
                                                "snapshot_id,schema_hash";
constexpr std::string_view operation_columns = "collection_id,operation_id,payload_hash,hash_version,base_identity,"
                                               "attempt_id,state,receipt_version,outcome_blob";

template <class T, class F>
iceberg::Result<T> Bridge(F&& body, bool& rollback_only, std::optional<ErrorDetail>* first_error = nullptr)
{
    try {
        if constexpr (std::is_void_v<T>) {
            body();
            return {};
        } else {
            return body();
        }
    } catch (const Failure& error) {
        rollback_only = true;
        if (first_error != nullptr && !first_error->has_value()) {
            *first_error = error.detail;
        }
        if (error.detail.code == "ALREADY_EXISTS") {
            return iceberg::AlreadyExists(error.detail.message);
        }
        return iceberg::IOError(error.detail.code + ": " + error.detail.message);
    } catch (const std::exception& error) {
        rollback_only = true;
        if (first_error != nullptr && !first_error->has_value()) {
            *first_error = ErrorDetail{"INTERNAL", std::string(error.what()), RetryHint::OperatorAction};
        }
        return iceberg::IOError(std::string(error.what()));
    }
}

void ValidateLocation(std::string_view location, const StoreInfo& info)
{
    const std::string prefix = "store://" + info.store_id + "/warehouse/";
    Require(location.starts_with(prefix) && location.size() <= 2048 && ValidUtf8(location) &&
            location.find('\0') == std::string_view::npos,
        "INVALID_ARGUMENT",
        "metadata must use this store's logical warehouse URI");
    auto path = location.substr(prefix.size());
    while (!path.empty()) {
        const auto slash = path.find('/');
        const auto part = path.substr(0, slash);
        Require(!part.empty() && part != "." && part != ".." && part.find('\\') == std::string_view::npos &&
                part.find('%') == std::string_view::npos && part.find('?') == std::string_view::npos &&
                part.find('#') == std::string_view::npos,
            "INVALID_ARGUMENT",
            "invalid logical path component");
        if (slash == std::string_view::npos) {
            break;
        }
        path.remove_prefix(slash + 1);
        Require(!path.empty(), "INVALID_ARGUMENT", "metadata URI ends in slash");
    }
    Require(!location.ends_with('/'), "INVALID_ARGUMENT", "metadata URI has no filename");
}

} // namespace

DurableSqliteCatalogStore::DurableSqliteCatalogStore(std::filesystem::path path,
    StoreInfo info,
    Limits limits,
    std::string vfs)
    : path_(std::move(path))
    , info_(std::move(info))
    , limits_(limits)
    , vfs_(std::move(vfs))
{ }

std::shared_ptr<DurableSqliteCatalogStore>
DurableSqliteCatalogStore::Create(const std::filesystem::path& path, StoreInfo info, Limits limits, std::string vfs)
{
    auto store = std::shared_ptr<DurableSqliteCatalogStore>(
        new DurableSqliteCatalogStore(path, std::move(info), limits, std::move(vfs)));
    Require(!std::filesystem::exists(path), "ALREADY_EXISTS", "catalog already exists");
    store->OpenConnection(true);
    return store;
}

std::shared_ptr<DurableSqliteCatalogStore>
DurableSqliteCatalogStore::Open(const std::filesystem::path& path, StoreInfo info, Limits limits, std::string vfs)
{
    auto store = std::shared_ptr<DurableSqliteCatalogStore>(
        new DurableSqliteCatalogStore(path, std::move(info), limits, std::move(vfs)));
    store->OpenConnection(false);
    return store;
}

void DurableSqliteCatalogStore::OpenConnection(bool create)
{
    Require(path_.is_absolute() && path_.string().find('\0') == std::string::npos && ValidUuid(info_.store_id) &&
            info_.format_version == 1,
        "INVALID_ARGUMENT",
        "catalog requires an absolute path and explicit store identity");
    Require(std::string_view(sqlite3_libversion()) == "3.54.1" &&
            std::string_view(sqlite3_sourceid()) ==
                "2026-10-16 14:45:20 "
                "555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8"
                "e6ca76",
        "UNSUPPORTED_STORE_VERSION",
        "SQLite runtime does not match the locked build");
    if (!create) {
        Require(std::filesystem::is_regular_file(std::filesystem::symlink_status(path_)),
            "STORE_MISSING",
            "catalog is absent or not a regular file");
        Database preflight;
        try {
            preflight.Open(path_, SQLITE_OPEN_READONLY, vfs_);
            Require(preflight.Text("PRAGMA main.journal_mode") == "delete",
                "UNSUPPORTED_STORE_VERSION",
                "unexpected SQLite journal mode");
            Require(preflight.Integer("PRAGMA user_version") == 1,
                "UNSUPPORTED_STORE_VERSION",
                "unknown catalog version");
            const auto rows = preflight.Query("SELECT store_id,format_major,profile_id FROM "
                                              "store_header WHERE singleton=1");
            Require(rows.size() == 1 && Required(rows[0], 0) == info_.store_id && Required(rows[0], 1) == "1" &&
                    Required(rows[0], 2) == info_.profile_id,
                "STORE_IDENTITY_MISMATCH",
                "catalog identity or profile does not match");
        } catch (const Failure& error) {
            // A real hot journal requires SQLite's own RW rollback recovery.
            if (error.native_code != SQLITE_READONLY_ROLLBACK) {
                throw;
            }
        }
        preflight.Close();
    }
    database_.Open(path_, SQLITE_OPEN_READWRITE | (create ? SQLITE_OPEN_CREATE : 0), vfs_);
    if (!create) {
        ValidateHeader();
        Require(database_.Text("PRAGMA main.journal_mode") == "delete",
            "UNSUPPORTED_STORE_VERSION",
            "unexpected SQLite journal mode");
    }
    Configure();
    if (create) {
        Transaction([&] {
            database_.Execute("CREATE TABLE store_header(singleton INTEGER PRIMARY KEY "
                              "CHECK(singleton=1),store_id TEXT NOT NULL,format_major "
                              "INTEGER NOT NULL,profile_id TEXT NOT NULL,durability_fence "
                              "INTEGER NOT NULL CHECK(durability_fence>=0))");
            database_.Execute("CREATE TABLE iceberg_namespace_properties(namespace TEXT NOT "
                              "NULL,property_key TEXT NOT NULL,property_value TEXT,PRIMARY "
                              "KEY(namespace,property_key))");
            database_.Execute("CREATE TABLE iceberg_tables(namespace TEXT NOT "
                              "NULL,table_name TEXT NOT NULL,metadata_location "
                              "TEXT NOT NULL,previous_metadata_location "
                              "TEXT,PRIMARY KEY(namespace,table_name))");
            database_.Execute("CREATE TABLE collections(collection_id TEXT PRIMARY KEY,name "
                              "TEXT NOT NULL UNIQUE,table_uuid TEXT NOT NULL "
                              "UNIQUE,schema_blob BLOB NOT NULL,schema_hash TEXT NOT "
                              "NULL,creation_sequence INTEGER NOT NULL "
                              "UNIQUE,current_snapshot_id INTEGER,schema_id INTEGER NOT NULL "
                              "DEFAULT 0,spec_id INTEGER NOT NULL DEFAULT 0)");
            database_.Execute("CREATE TABLE operations(collection_id TEXT NOT NULL "
                              "REFERENCES collections(collection_id),operation_id TEXT NOT "
                              "NULL,payload_hash TEXT NOT NULL,hash_version INTEGER NOT "
                              "NULL,state INTEGER NOT NULL CHECK(state "
                              "IN(0,1,2)),base_identity BLOB NOT NULL,attempt_id TEXT NOT "
                              "NULL,receipt_version INTEGER NOT NULL,outcome_blob BLOB "
                              "CHECK(length(outcome_blob)<=4096),PRIMARY "
                              "KEY(collection_id,operation_id),CHECK((state=0 AND "
                              "outcome_blob IS NULL) OR (state IN(1,2) AND outcome_blob IS "
                              "NOT NULL)))");
            database_.Execute("INSERT INTO store_header VALUES(1,?,1,?,0)", {info_.store_id, info_.profile_id});
            database_.Execute("INSERT INTO iceberg_namespace_properties "
                              "VALUES('columnar','exists','true')");
            database_.Execute("PRAGMA user_version=1");
            database_.Execute("PRAGMA application_id=1196179789");
        });
    }
    ValidateHeader();
    ValidateInventory();
}

void DurableSqliteCatalogStore::Configure()
{
    Require(database_.Text("PRAGMA main.journal_mode=DELETE") == "delete",
        "IO_ERROR",
        "cannot select DELETE journal mode");
    database_.Execute("PRAGMA main.synchronous=EXTRA");
    database_.Execute("PRAGMA foreign_keys=ON");
    database_.Execute("PRAGMA busy_timeout=5000");
    database_.Execute("PRAGMA cache_size=-8192");
    database_.Execute("PRAGMA mmap_size=0");
    database_.Execute("PRAGMA trusted_schema=OFF");
    const auto profile = Profile();
    Require(profile.synchronous == 3 && profile.foreign_keys == 1 && profile.busy_timeout == 5000 &&
            profile.cache_size == -8192 && profile.mmap_size == 0,
        "IO_ERROR",
        "SQLite working connection settings did not take effect");
    Require(limits_.max_catalog_bytes >= 65536 && limits_.max_catalog_bytes <= 256LL * 1024 * 1024 &&
            limits_.max_operations > 0 && limits_.max_operations <= 4096 && limits_.max_collections > 0 &&
            limits_.max_collections <= 8,
        "INVALID_ARGUMENT",
        "invalid catalog limits");
    const auto pages = limits_.max_catalog_bytes / database_.Integer("PRAGMA page_size");
    Require(database_.Integer("PRAGMA page_count") <= pages, "CONFIG_TOO_SMALL", "catalog exceeds configured capacity");
    Require(database_.Integer("PRAGMA max_page_count=" + std::to_string(pages)) == pages,
        "CONFIG_TOO_SMALL",
        "cannot enforce catalog capacity");
}

void DurableSqliteCatalogStore::ValidateHeader()
{
    Require(database_.Integer("PRAGMA user_version") == 1 && database_.Integer("PRAGMA application_id") == 1196179789,
        "UNSUPPORTED_STORE_VERSION",
        "unsupported catalog format");
    const auto rows = database_.Query("SELECT store_id,format_major,profile_id,durability_fence FROM "
                                      "store_header WHERE singleton=1");
    Require(rows.size() == 1 && database_.Integer("SELECT COUNT(*) FROM store_header") == 1,
        "CORRUPTION",
        "missing or duplicate store header");
    Require(Required(rows[0], 0) == info_.store_id && Required(rows[0], 2) == info_.profile_id,
        "STORE_IDENTITY_MISMATCH",
        "store identity or profile does not match");
    Require(Required(rows[0], 1) == "1", "UNSUPPORTED_STORE_VERSION", "unsupported store version");
    Require(ParseInteger(Required(rows[0], 3)) >= 0, "CORRUPTION", "invalid durability fence");
}

void DurableSqliteCatalogStore::Transaction(const std::function<void()>& body)
{
    Require(!uncertain_, "RECOVERY_REQUIRED", "catalog has an unresolved transaction");
    Require(!transaction_ && database_.Autocommit(), "INTERNAL", "unexpected nested outer transaction");
    database_.Execute("BEGIN IMMEDIATE");
    transaction_ = true;
    rollback_only_ = false;
    bool commit_started = false;
    try {
        body();
        Require(!rollback_only_, "IO_ERROR", "nested transaction failed; outer transaction is rollback-only");
        commit_started = true;
        database_.Execute("COMMIT");
        Require(database_.Autocommit(), "IO_ERROR", "COMMIT left a transaction active");
        transaction_ = false;
    } catch (...) {
        auto first = std::current_exception();
        bool unknown = commit_started;
        try {
            if (!database_.Autocommit()) {
                database_.Execute("ROLLBACK");
            }
            unknown = unknown || !database_.Autocommit();
        } catch (...) {
            unknown = true;
        }
        transaction_ = false;
        uncertain_ = unknown;
        try {
            std::rethrow_exception(first);
        } catch (const Failure& error) {
            auto result = error;
            result.uncertain = result.uncertain || unknown;
            result.phase = commit_started ? CommitPhase::CatalogCommit : CommitPhase::CatalogTransaction;
            throw result;
        } catch (const std::exception& error) {
            throw Failure({"IO_ERROR", BoundedDiagnostic(error.what()), RetryHint::OperatorAction}, unknown);
        } catch (...) {
            throw Failure({"INTERNAL", "storage callback raised an unknown exception", RetryHint::OperatorAction},
                unknown);
        }
    }
}

void DurableSqliteCatalogStore::Reopen()
{
    std::lock_guard lock(mutex_);
    Require(!transaction_ && publication_ == nullptr, "RECOVERY_REQUIRED", "publication is still active");
    try {
        if (!database_.Autocommit()) {
            database_.Execute("ROLLBACK");
        }
    } catch (const Failure&) {
        // Checked close must succeed before any new authoritative connection.
        uncertain_ = true;
    }
    database_.Close();
    uncertain_ = false;
    OpenConnection(false);
}

void DurableSqliteCatalogStore::Close()
{
    std::lock_guard lock(mutex_);
    Require(!transaction_ && publication_ == nullptr, "RECOVERY_REQUIRED", "publication is active");
    if (!database_.Autocommit()) {
        database_.Execute("ROLLBACK");
    }
    database_.Close();
}

CatalogProfile DurableSqliteCatalogStore::Profile()
{
    std::lock_guard lock(mutex_);
    CatalogProfile profile;
    profile.sqlite_version = sqlite3_libversion();
    profile.source_id = sqlite3_sourceid();
    profile.vfs = database_.VfsName();
    for (const auto& row : database_.Query("PRAGMA compile_options")) {
        profile.compile_options.push_back(Required(row, 0));
    }
    profile.journal_mode = database_.Text("PRAGMA main.journal_mode");
    profile.synchronous = database_.Integer("PRAGMA main.synchronous");
    profile.foreign_keys = database_.Integer("PRAGMA foreign_keys");
    profile.busy_timeout = database_.Integer("PRAGMA busy_timeout");
    profile.cache_size = database_.Integer("PRAGMA cache_size");
    profile.mmap_size = database_.Integer("PRAGMA mmap_size");
    return profile;
}

CollectionInfo DurableSqliteCatalogStore::ReadCollection(const SqlRow& row)
{
    const auto schema = DecodeSchema(Required(row, 3));
    Require(schema.has_value(), "CORRUPTION", "invalid collection schema");
    CollectionInfo info{Required(row, 0),
        Required(row, 1),
        Required(row, 2),
        *schema,
        ParseInteger(Required(row, 4)),
        row[5] ? std::optional(ParseInteger(Required(row, 5))) : std::nullopt};
    Require(ValidUuid(info.collection_id) && ValidUuid(info.table_uuid) && !info.name.empty() &&
            info.name.size() <= 128 && ValidUtf8(info.name) && info.name.find('\0') == std::string::npos &&
            info.creation_sequence > 0 && SchemaFingerprint(info.schema) == Required(row, 6),
        "CORRUPTION",
        "invalid collection registry");
    return info;
}

std::vector<CollectionInfo> DurableSqliteCatalogStore::Collections()
{
    std::lock_guard lock(mutex_);
    Require(!uncertain_, "RECOVERY_REQUIRED", "catalog is not authoritative");
    const auto rows =
        database_.Query("SELECT " + std::string(collection_columns) + " FROM collections ORDER BY creation_sequence");
    Require(rows.size() <= static_cast<size_t>(limits_.max_collections),
        "CONFIG_TOO_SMALL",
        "collection count exceeds configured capacity");
    std::vector<CollectionInfo> result;
    result.reserve(rows.size());
    for (const auto& row : rows) {
        result.push_back(ReadCollection(row));
    }
    return result;
}

CollectionInfo DurableSqliteCatalogStore::Collection(std::string_view id)
{
    std::lock_guard lock(mutex_);
    Require(!uncertain_, "RECOVERY_REQUIRED", "catalog is not authoritative");
    const auto rows =
        database_.Query("SELECT " + std::string(collection_columns) + " FROM collections WHERE collection_id=?",
            {std::string(id)});
    Require(rows.size() == 1, "COLLECTION_NOT_FOUND", "collection does not exist");
    return ReadCollection(rows[0]);
}

CollectionInfo DurableSqliteCatalogStore::Resolve(std::string_view name)
{
    std::lock_guard lock(mutex_);
    Require(!uncertain_, "RECOVERY_REQUIRED", "catalog is not authoritative");
    const auto rows = database_.Query("SELECT " + std::string(collection_columns) + " FROM collections WHERE name=?",
        {std::string(name)});
    Require(rows.size() == 1, "COLLECTION_NOT_FOUND", "exact collection name does not exist");
    return ReadCollection(rows[0]);
}

BaseIdentity DurableSqliteCatalogStore::Base(std::string_view id)
{
    std::lock_guard lock(mutex_);
    const auto collection = Collection(id);
    const auto rows = database_.Query("SELECT t.metadata_location,c.schema_id,c.spec_id FROM collections c "
                                      "JOIN iceberg_tables t ON t.namespace='columnar' AND "
                                      "t.table_name=c.name WHERE c.collection_id=?",
        {std::string(id)});
    Require(rows.size() == 1, "CORRUPTION", "registry table has no catalog pointer");
    Require(ParseInteger(Required(rows[0], 1)) == 0 && ParseInteger(Required(rows[0], 2)) == 0,
        "UNSUPPORTED_STORE_VERSION",
        "schema evolution or partitioning is unsupported");
    BaseIdentity base{info_.store_id,
        collection.collection_id,
        collection.table_uuid,
        Required(rows[0], 0),
        collection.current_snapshot_id,
        0,
        0};
    ValidateLocation(base.metadata_location, info_);
    Require(base.schema_id == 0 && base.spec_id == 0,
        "UNSUPPORTED_STORE_VERSION",
        "schema evolution or partitioning is unsupported");
    return base;
}

OperationRecord DurableSqliteCatalogStore::ReadOperation(const SqlRow& row)
{
    const auto base = DecodeBase(Required(row, 4));
    Require(base.has_value(), "CORRUPTION", "invalid bound base");
    OperationRecord record;
    Require(ParseInteger(Required(row, 3)) == 1, "UNSUPPORTED_STORE_VERSION", "unsupported hash version");
    record.binding = {Required(row, 0),
        Required(row, 1),
        Required(row, 2),
        static_cast<int32_t>(ParseInteger(Required(row, 3))),
        *base,
        Required(row, 5)};
    Require(record.binding.hash_version == 1 && Required(row, 7) == "1",
        "UNSUPPORTED_STORE_VERSION",
        "unsupported hash or receipt version");
    const auto& binding = record.binding;
    Require(ValidUuid(binding.collection_id) && ValidUuid(binding.operation_id) && ValidUuid(binding.attempt_id) &&
            ValidDigest(binding.payload_hash) && binding.base.collection_id == binding.collection_id &&
            binding.base.store_id == info_.store_id,
        "CORRUPTION",
        "invalid operation binding");
    const auto state = ParseInteger(Required(row, 6));
    Require(state >= 0 && state <= 2, "CORRUPTION", "invalid operation state");
    record.state = static_cast<OperationState>(state);
    if (row[8]) {
        record.outcome_bytes = Required(row, 8);
        const auto decoded = DecodeOutcome(record.outcome_bytes);
        if (!decoded) {
            throw Failure(decoded.error());
        }
        record.outcome = *decoded;
        Require(record.outcome->operation_id == binding.operation_id &&
                ((record.state == OperationState::Committed &&
                     record.outcome->disposition == CommitDisposition::Committed) ||
                    (record.state == OperationState::Failed &&
                        record.outcome->disposition == CommitDisposition::DefinitelyNotCommitted)),
            "CORRUPTION",
            "operation state and outcome disagree");
        if (record.outcome->result) {
            const auto& result = *record.outcome->result;
            Require(result.collection_id == binding.collection_id && result.payload_hash == binding.payload_hash &&
                    result.parent_snapshot_id == binding.base.current_snapshot_id &&
                    result.durability_profile == info_.profile_id,
                "CORRUPTION",
                "receipt does not match durable binding");
        }
    } else {
        Require(record.state == OperationState::Bound, "CORRUPTION", "terminal operation has no outcome");
    }
    return record;
}

std::vector<OperationRecord> DurableSqliteCatalogStore::Operations()
{
    std::lock_guard lock(mutex_);
    Require(!uncertain_, "RECOVERY_REQUIRED", "catalog is not authoritative");
    const auto rows = database_.Query(
        "SELECT " + std::string(operation_columns) + " FROM operations ORDER BY collection_id,operation_id");
    Require(rows.size() <= static_cast<size_t>(limits_.max_operations),
        "CONFIG_TOO_SMALL",
        "operation count exceeds configured capacity");
    std::vector<OperationRecord> result;
    result.reserve(rows.size());
    for (const auto& row : rows) {
        result.push_back(ReadOperation(row));
    }
    return result;
}

std::optional<OperationRecord> DurableSqliteCatalogStore::FindOperation(std::string_view collection_id,
    std::string_view operation_id)
{
    std::lock_guard lock(mutex_);
    Require(!uncertain_, "RECOVERY_REQUIRED", "catalog is not authoritative");
    const auto rows = database_.Query("SELECT " + std::string(operation_columns) +
            " FROM operations WHERE collection_id=? AND operation_id=?",
        {std::string(collection_id), std::string(operation_id)});
    if (rows.empty()) {
        return std::nullopt;
    }
    return ReadOperation(rows.front());
}

std::optional<OperationRecord> DurableSqliteCatalogStore::TryFindOperation(std::string_view collection_id,
    std::string_view operation_id)
{
    std::unique_lock lock(mutex_, std::try_to_lock);
    // Also reject reentrant reads from a callback on the publication thread.
    if (!lock.owns_lock() || transaction_ || publication_ != nullptr) {
        throw Failure({"OPERATION_IN_PROGRESS", "catalog publication is executing", RetryHint::SameIdAfterReconcile});
    }
    return FindOperation(collection_id, operation_id);
}

void DurableSqliteCatalogStore::CheckCapacity(bool operation)
{
    Require(database_.Integer(operation ? "SELECT COUNT(*) FROM operations" : "SELECT COUNT(*) FROM collections") <
            (operation ? limits_.max_operations : limits_.max_collections),
        "RESOURCE_EXHAUSTED",
        "permanent catalog capacity is full");
    const auto free = database_.Integer("PRAGMA freelist_count") * database_.Integer("PRAGMA page_size") +
        limits_.max_catalog_bytes - database_.Integer("PRAGMA page_count") * database_.Integer("PRAGMA page_size");
    Require(free >= 24576, "RESOURCE_EXHAUSTED", "catalog cannot reserve a binding and terminal receipt");
}

void DurableSqliteCatalogStore::BindOperation(const BoundOperation& binding)
{
    std::lock_guard lock(mutex_);
    Require(ValidUuid(binding.operation_id) && ValidUuid(binding.attempt_id) && ValidDigest(binding.payload_hash) &&
            binding.hash_version == 1 && binding.base == Base(binding.collection_id),
        "INVALID_ARGUMENT",
        "invalid prepared binding");
    Transaction([&] {
        CheckCapacity(true);
        database_.Execute("INSERT INTO "
                          "operations(collection_id,operation_id,payload_hash,"
                          "hash_version,state,base_identity,attempt_id,receipt_"
                          "version) VALUES(?,?,?,1,0,?,?,1)",
            {binding.collection_id,
                binding.operation_id,
                binding.payload_hash,
                Blob{EncodeBase(binding.base)},
                binding.attempt_id});
    });
}

void DurableSqliteCatalogStore::CheckBinding(const BoundOperation& binding)
{
    const auto record = FindOperation(binding.collection_id, binding.operation_id);
    Require(record && record->state == OperationState::Bound && record->binding == binding,
        "CORRUPTION",
        "publication does not match a pending bound operation");
}

void DurableSqliteCatalogStore::Complete(const BoundOperation& binding, const AppendOutcome& outcome)
{
    CheckBinding(binding);
    const auto encoded = EncodeOutcome(outcome);
    if (!encoded) {
        throw Failure(encoded.error());
    }
    database_.Execute("UPDATE operations SET state=?,outcome_blob=? WHERE collection_id=? "
                      "AND operation_id=? AND state=0",
        {outcome.disposition == CommitDisposition::Committed ? int64_t{1} : int64_t{2},
            Blob{*encoded},
            binding.collection_id,
            binding.operation_id});
    Require(database_.Changes() == 1, "CORRUPTION", "operation outcome transition failed");
}

void DurableSqliteCatalogStore::RecordFailure(const BoundOperation& binding, const AppendOutcome& outcome)
{
    std::lock_guard lock(mutex_);
    Require(outcome.disposition == CommitDisposition::DefinitelyNotCommitted &&
            outcome.operation_id == binding.operation_id,
        "INVALID_ARGUMENT",
        "failure outcome does not match binding");
    Transaction([&] {
        Complete(binding, outcome);
    });
}

CollectionInfo DurableSqliteCatalogStore::PublishCreate(CollectionInfo collection, const Guard& guard, const Body& body)
{
    std::lock_guard lock(mutex_);
    Require(guard && body && publication_ == nullptr,
        "INVALID_ARGUMENT",
        "publication requires a closure guard and synchronous body");
    const auto schema = EncodeSchema(collection.schema);
    Require(schema.has_value() && ValidUuid(collection.collection_id) && ValidUuid(collection.table_uuid) &&
            !collection.current_snapshot_id && !collection.name.empty() && collection.name.size() <= 128 &&
            ValidUtf8(collection.name) && collection.name.find('\0') == std::string::npos,
        "INVALID_ARGUMENT",
        "invalid collection definition");
    Publication scope{true, collection.name, std::nullopt, guard, {}, 0};
    publication_ = &scope;
    try {
        Transaction([&] {
            CheckCapacity(false);
            Require(database_
                        .Query("SELECT collection_id FROM collections WHERE "
                               "collection_id=? OR name=?",
                            {collection.collection_id, collection.name})
                        .empty(),
                "ALREADY_EXISTS",
                "collection ID or name already exists");
            const auto applied = body();
            if (scope.first_error) {
                throw Failure(*scope.first_error);
            }
            if (!applied) {
                throw Failure({"IO_ERROR", applied.error().message, RetryHint::OperatorAction});
            }
            Require(scope.mutations == 1, "IO_ERROR", "create did not publish exactly one guarded table");
            collection.creation_sequence =
                database_.Integer("SELECT COALESCE(MAX(creation_sequence),0)+1 FROM collections");
            database_.Execute("INSERT INTO "
                              "collections(collection_id,name,table_uuid,schema_blob,schema_"
                              "hash,creation_sequence) VALUES(?,?,?,?,?,?)",
                {collection.collection_id,
                    collection.name,
                    collection.table_uuid,
                    Blob{*schema},
                    Sha256(*schema),
                    collection.creation_sequence});
        });
        publication_ = nullptr;
        return collection;
    } catch (...) {
        publication_ = nullptr;
        throw;
    }
}

void DurableSqliteCatalogStore::PublishAppend(const BoundOperation& binding,
    const AppendResult& result,
    const Guard& guard,
    const Body& body)
{
    std::lock_guard lock(mutex_);
    Require(guard && body && publication_ == nullptr,
        "INVALID_ARGUMENT",
        "publication requires a closure guard and synchronous body");
    const auto collection = Collection(binding.collection_id);
    Publication scope{false, collection.name, binding.base, guard, {}, 0};
    publication_ = &scope;
    try {
        Transaction([&] {
            CheckBinding(binding);
            Require(Base(binding.collection_id) == binding.base, "STALE_BASE", "exact prepared base changed");
            Require(result.collection_id == binding.collection_id && result.operation_id == binding.operation_id &&
                    result.payload_hash == binding.payload_hash &&
                    result.parent_snapshot_id == binding.base.current_snapshot_id &&
                    result.durability_profile == info_.profile_id,
                "INVALID_ARGUMENT",
                "candidate receipt does not match prepared operation");
            const auto applied = body();
            if (scope.first_error) {
                throw Failure(*scope.first_error);
            }
            if (!applied) {
                throw Failure({"IO_ERROR", applied.error().message, RetryHint::OperatorAction});
            }
            Require(scope.mutations == 1, "IO_ERROR", "append did not publish exactly one guarded pointer");
            const auto pointer = GetTableMetadataLocation("columnar", collection.name);
            Require(pointer && *pointer == scope.location,
                "CORRUPTION",
                "published pointer does not match guarded root");
            Complete(binding,
                {binding.operation_id,
                    CommitDisposition::Committed,
                    CommitPhase::Complete,
                    result,
                    std::nullopt,
                    std::nullopt});
            database_.Execute("UPDATE collections SET current_snapshot_id=? "
                              "WHERE collection_id=?",
                {result.snapshot_id, binding.collection_id});
        });
        publication_ = nullptr;
    } catch (...) {
        publication_ = nullptr;
        throw;
    }
}

void DurableSqliteCatalogStore::GuardPointer(PointerTarget target, std::string_view location)
{
    Require(publication_ != nullptr && transaction_ && target.ns == "columnar" && target.name == publication_->name &&
            publication_->mutations == 0 && publication_->guard,
        "INVALID_ARGUMENT",
        "pointer mutation requires its owned publication scope");
    ValidateLocation(location, info_);
    const auto checked = publication_->guard(location);
    if (!checked) {
        throw Failure(checked.error());
    }
    publication_->location = location;
}

void DurableSqliteCatalogStore::ValidateInventory()
{
    std::lock_guard lock(mutex_);
    Require(database_.Text("PRAGMA integrity_check") == "ok", "CORRUPTION", "SQLite integrity check failed");
    Require(database_.Query("PRAGMA foreign_key_check").empty(), "CORRUPTION", "catalog foreign key check failed");
    const auto collections = Collections();
    Require(database_.Integer("SELECT COUNT(*) FROM iceberg_tables") == static_cast<int64_t>(collections.size()),
        "CORRUPTION",
        "catalog pointers and registry disagree");
    Require(database_.Integer("SELECT COUNT(*) FROM iceberg_namespace_properties WHERE "
                              "namespace='columnar' AND property_key='exists' AND "
                              "property_value='true'") == 1 &&
            database_.Integer("SELECT COUNT(*) FROM iceberg_namespace_properties") == 1,
        "CORRUPTION",
        "internal namespace is missing or modified");
    for (const auto& collection : collections) {
        (void)Base(collection.collection_id);
    }
    for (const auto& record : Operations()) {
        const auto base = Base(record.binding.collection_id);
        Require(record.binding.base.table_uuid == base.table_uuid && record.binding.base.schema_id == base.schema_id &&
                record.binding.base.spec_id == base.spec_id,
            "CORRUPTION",
            "operation bound to a different table");
    }
}

void DurableSqliteCatalogStore::DurabilityFence()
{
    std::lock_guard lock(mutex_);
    Transaction([&] {
        Require(FenceValue() < std::numeric_limits<int64_t>::max(), "CAPACITY_EXCEEDED", "durability fence exhausted");
        database_.Execute("UPDATE store_header SET durability_fence=durability_fence+1 WHERE "
                          "singleton=1");
        Require(database_.Changes() == 1, "CORRUPTION", "missing durability fence");
    });
}

int64_t DurableSqliteCatalogStore::FenceValue()
{
    std::lock_guard lock(mutex_);
    return database_.Integer("SELECT durability_fence FROM store_header WHERE singleton=1");
}

iceberg::Status DurableSqliteCatalogStore::Initialize()
{
    std::lock_guard lock(mutex_);
    return Bridge<void>(
        [&] {
            ValidateHeader();
            ValidateInventory();
        },
        rollback_only_);
}

iceberg::Result<std::vector<std::string>> DurableSqliteCatalogStore::ListNamespaceNames()
{
    std::lock_guard lock(mutex_);
    return Bridge<std::vector<std::string>>(
        [&] {
            std::vector<std::string> result;
            for (const auto& row : database_.Query("SELECT DISTINCT namespace FROM "
                                                   "iceberg_namespace_properties ORDER BY namespace"))
            {
                result.push_back(Required(row, 0));
            }
            return result;
        },
        rollback_only_,
        publication_ ? &publication_->first_error : nullptr);
}

iceberg::Result<std::vector<iceberg::sql::NamespaceProperty>> DurableSqliteCatalogStore::GetNamespaceProperties(
    std::string_view ns)
{
    std::lock_guard lock(mutex_);
    return Bridge<std::vector<iceberg::sql::NamespaceProperty>>(
        [&] {
            std::vector<iceberg::sql::NamespaceProperty> result;
            for (const auto& row : database_.Query("SELECT property_key,property_value FROM "
                                                   "iceberg_namespace_properties WHERE "
                                                   "namespace=? ORDER BY property_key",
                     {std::string(ns)}))
            {
                result.push_back({Required(row, 0), row[1]});
            }
            return result;
        },
        rollback_only_,
        publication_ ? &publication_->first_error : nullptr);
}

iceberg::Status
DurableSqliteCatalogStore::InsertNamespaceProperty(std::string_view, std::string_view, std::optional<std::string_view>)
{
    return iceberg::NotSupported("the internal namespace is immutable");
}
iceberg::Status DurableSqliteCatalogStore::DeleteNamespaceProperty(std::string_view, std::string_view)
{
    return iceberg::NotSupported("namespace deletion is disabled");
}
iceberg::Result<int64_t> DurableSqliteCatalogStore::DeleteNamespace(std::string_view)
{
    return iceberg::NotSupported("namespace deletion is disabled");
}

iceberg::Result<std::vector<std::string>> DurableSqliteCatalogStore::ListTableNames(std::string_view ns)
{
    std::lock_guard lock(mutex_);
    return Bridge<std::vector<std::string>>(
        [&] {
            std::vector<std::string> result;
            for (const auto& row : database_.Query("SELECT table_name FROM iceberg_tables WHERE "
                                                   "namespace=? ORDER BY table_name",
                     {std::string(ns)}))
            {
                result.push_back(Required(row, 0));
            }
            return result;
        },
        rollback_only_);
}

iceberg::Result<bool> DurableSqliteCatalogStore::TableExists(std::string_view ns, std::string_view name)
{
    std::lock_guard lock(mutex_);
    return Bridge<bool>(
        [&] {
            return !database_
                        .Query("SELECT 1 FROM iceberg_tables WHERE namespace=? "
                               "AND table_name=?",
                            {std::string(ns), std::string(name)})
                        .empty();
        },
        rollback_only_);
}

iceberg::Result<std::optional<std::string>> DurableSqliteCatalogStore::GetTableMetadataLocation(std::string_view ns,
    std::string_view name)
{
    std::lock_guard lock(mutex_);
    return Bridge<std::optional<std::string>>(
        [&]() -> std::optional<std::string> {
            const auto rows = database_.Query("SELECT metadata_location FROM iceberg_tables "
                                              "WHERE namespace=? AND table_name=?",
                {std::string(ns), std::string(name)});
            return rows.empty() ? std::nullopt : rows[0][0];
        },
        rollback_only_);
}

iceberg::Status
DurableSqliteCatalogStore::InsertTable(std::string_view ns, std::string_view name, std::string_view location)
{
    std::lock_guard lock(mutex_);
    return Bridge<void>(
        [&] {
            Require(publication_ && publication_->creating, "INVALID_ARGUMENT", "insert outside collection creation");
            GuardPointer({ns, name}, location);
            database_.Execute("INSERT INTO "
                              "iceberg_tables(namespace,table_name,metadata_location) "
                              "VALUES(?,?,?)",
                {std::string(ns), std::string(name), std::string(location)});
            ++publication_->mutations;
        },
        rollback_only_,
        publication_ ? &publication_->first_error : nullptr);
}

iceberg::Result<int64_t> DurableSqliteCatalogStore::UpdateTableMetadataLocation(std::string_view ns,
    std::string_view name,
    std::string_view location,
    std::string_view previous,
    std::string_view expected)
{
    std::lock_guard lock(mutex_);
    return Bridge<int64_t>(
        [&] {
            Require(publication_ && !publication_->creating && publication_->base &&
                    expected == publication_->base->metadata_location && previous == expected,
                "STALE_BASE",
                "CAS does not use the exact prepared location");
            Require(location != expected, "INVALID_ARGUMENT", "metadata publication must create an immutable root");
            GuardPointer({ns, name}, location);
            database_.Execute("UPDATE iceberg_tables SET "
                              "metadata_location=?,previous_metadata_location=? WHERE "
                              "namespace=? AND table_name=? AND metadata_location=?",
                {std::string(location),
                    std::string(previous),
                    std::string(ns),
                    std::string(name),
                    std::string(expected)});
            const auto changes = database_.Changes();
            if (changes == 1) {
                ++publication_->mutations;
            }
            return changes;
        },
        rollback_only_,
        publication_ ? &publication_->first_error : nullptr);
}

iceberg::Result<int64_t> DurableSqliteCatalogStore::DeleteTable(std::string_view, std::string_view)
{
    return iceberg::NotSupported("table deletion is disabled");
}
iceberg::Result<int64_t>
DurableSqliteCatalogStore::RenameTable(std::string_view, std::string_view, std::string_view, std::string_view)
{
    return iceberg::NotSupported("collection names are immutable");
}

iceberg::Status DurableSqliteCatalogStore::RunInTransaction(const Body& body)
{
    std::lock_guard lock(mutex_);
    return Bridge<void>(
        [&] {
            Require(static_cast<bool>(body), "INVALID_ARGUMENT", "missing transaction body");
            const auto invoke = [&] {
                const auto result = body();
                if (!result) {
                    rollback_only_ = true;
                    throw Failure({"IO_ERROR", result.error().message, RetryHint::OperatorAction});
                }
            };
            if (transaction_) {
                invoke();
            } else {
                Transaction(invoke);
            }
        },
        rollback_only_);
}

} // namespace glamdb::detail
