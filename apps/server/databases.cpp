#include "apps/server/databases.h"

#include "core/api/canonical.h"
#include "core/catalog/sqlite.h"
#include "core/engine/store_lease.h"

#include <atomic>
#include <fcntl.h>
#include <mutex>
#include <shared_mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace glamdb::NServer {
using namespace NTransport;
namespace {
TJson EncodeLimits(const Limits& limits)
{
    return {{"max_operations", limits.max_operations},
        {"max_collections", limits.max_collections},
        {"max_catalog_bytes", limits.max_catalog_bytes},
        {"max_pending_calls", limits.max_pending_calls},
        {"max_owned_input_bytes", limits.max_owned_input_bytes},
        {"writer_wait_ms", limits.writer_wait.count()},
        {"append_deadline_ms", limits.append_deadline.count()}};
}
Limits ParseLimits(const TJson& value, Limits limits = {})
{
    struct TBounds
    {
        int64_t Minimum;
        int64_t Maximum;
    };
    CheckKeys(value,
        {"max_operations",
            "max_collections",
            "max_catalog_bytes",
            "max_pending_calls",
            "max_owned_input_bytes",
            "writer_wait_ms",
            "append_deadline_ms"});
    const auto read = [&](const char* name, int64_t current, TBounds bounds) {
        const auto found = value.find(name);
        if (found == value.end()) {
            return current;
        }
        Require(found->is_number_integer(), std::string(name) + " must be an integer");
        if (found->is_number_unsigned()) {
            const auto number = found->get<uint64_t>();
            Require(number >= static_cast<uint64_t>(bounds.Minimum) && number <= static_cast<uint64_t>(bounds.Maximum),
                std::string(name) + " outside supported range");
            return static_cast<int64_t>(number);
        }
        const auto number = found->get<int64_t>();
        Require(number >= bounds.Minimum && number <= bounds.Maximum, std::string(name) + " outside supported range");
        return number;
    };
    limits.max_operations = read("max_operations", limits.max_operations, {1, 4096});
    limits.max_collections = read("max_collections", limits.max_collections, {1, 8});
    limits.max_catalog_bytes = read("max_catalog_bytes", limits.max_catalog_bytes, {65536, 256LL * 1024 * 1024});
    limits.max_pending_calls = read("max_pending_calls", limits.max_pending_calls, {1, 16});
    limits.max_owned_input_bytes =
        read("max_owned_input_bytes", limits.max_owned_input_bytes, {1, 128LL * 1024 * 1024});
    limits.writer_wait = std::chrono::milliseconds(read("writer_wait_ms", limits.writer_wait.count(), {0, 5000}));
    limits.append_deadline =
        std::chrono::milliseconds(read("append_deadline_ms", limits.append_deadline.count(), {1, 60000}));
    return limits;
}
struct TDirectoryLock
{
    int Descriptor = -1;
    ~TDirectoryLock()
    {
        if (Descriptor >= 0) {
            close(Descriptor);
        }
    }
};
struct TEntry
{
    std::string ID, Name;
    std::mutex Lifecycle;
    mutable std::shared_mutex Gate;
    std::atomic<bool> Accepting{false};
    std::shared_ptr<Engine> Instance;
    Limits ResourceLimits;
    bool Initialized = false;
    bool AutoOpen = true;
    bool Closing = false;
    std::string State = "closed";
    std::optional<ErrorDetail> Error;
    bool RegistryFence = false;
    bool Retired = false;
    std::shared_ptr<const TJson> Descriptor;
};
TJson Describe(const TEntry& entry)
{
    TJson value{{"database_id", entry.ID},
        {"name", entry.Name},
        {"state", entry.State},
        {"limits", EncodeLimits(entry.ResourceLimits)}};
    if (entry.Error) {
        value["error"] = EncodeError(*entry.Error).at("error");
    }
    if (entry.Instance && entry.State == "open") {
        const auto& info = entry.Instance->GetStoreInfo();
        value["store"] = {{"store_id", info.store_id},
            {"profile_id", info.profile_id},
            {"format_version", info.format_version}};
    }
    return value;
}
void Publish(TEntry& entry)
{
    std::atomic_store(&entry.Descriptor, std::make_shared<const TJson>(Describe(entry)));
}
TJson Current(const TEntry& entry)
{
    const auto descriptor = std::atomic_load(&entry.Descriptor);
    Require(descriptor->at("state") != "discarded", "database not found", "DATABASE_NOT_FOUND");
    return *descriptor;
}
void ConfigureRegistry(detail::Database& registry)
{
    registry.Execute("PRAGMA busy_timeout=250");
    Require(registry.Text("PRAGMA journal_mode=DELETE") == "delete", "registry requires DELETE journal", "IO_ERROR");
    registry.Execute("PRAGMA synchronous=EXTRA");
    Require(registry.Integer("PRAGMA synchronous") == 3, "registry requires EXTRA synchronous", "IO_ERROR");
}
void SyncDirectory(const std::filesystem::path& path)
{
    TDirectoryLock directory;
    directory.Descriptor = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    Require(directory.Descriptor >= 0 && fsync(directory.Descriptor) == 0,
        "cannot sync registry directory",
        "IO_ERROR");
}
void InitializeRegistry(const std::filesystem::path& directory, const std::string& vfs)
{
    const auto staging = directory / "registry.initializing.sqlite";
    // Only our unpublished initialization files and an empty database parent may
    // remain after a crash. Anything else could contain accepted user data.
    for (const auto& file : std::filesystem::directory_iterator(directory)) {
        const auto name = file.path().filename().string();
        const auto status = file.symlink_status();
        const bool emptyParent =
            name == "databases" && std::filesystem::is_directory(status) && std::filesystem::is_empty(file.path());
        const bool temporary =
            (name == "registry.initializing.sqlite" || name == "registry.initializing.sqlite-journal") &&
            std::filesystem::is_regular_file(status);
        Require(emptyParent || temporary,
            "registry missing from nonempty data directory; refusing to replace it",
            "CORRUPTION");
    }
    std::filesystem::remove(staging);
    std::filesystem::remove(staging.string() + "-journal");
    std::filesystem::create_directory(directory / "databases");
    SyncDirectory(directory / "databases");
    {
        detail::Database registry;
        registry.Open(staging, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, vfs);
        ConfigureRegistry(registry);
        registry.Execute("BEGIN IMMEDIATE");
        registry.Execute("CREATE TABLE databases(id TEXT PRIMARY KEY NOT NULL, name TEXT UNIQUE NOT NULL, "
                         "initialized INTEGER NOT NULL CHECK(initialized IN (0,1)), auto_open INTEGER NOT NULL "
                         "CHECK(auto_open IN (0,1)), limits TEXT NOT NULL DEFAULT '" +
            EncodeLimits(Limits{}).dump() + "') STRICT");
        registry.Execute("PRAGMA user_version=2");
        registry.Execute("COMMIT");
        registry.Close();
    }
    SyncDirectory(directory);
    std::filesystem::rename(staging, directory / "registry.sqlite");
    SyncDirectory(directory);
    SyncDirectory(directory.parent_path());
}
void PreserveIncompleteStore(const std::filesystem::path& path)
{
    Require(std::filesystem::is_directory(std::filesystem::symlink_status(path)),
        "incomplete store is not a directory",
        "CORRUPTION");
    TDirectoryLock ownership;
    ownership.Descriptor = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    Require(ownership.Descriptor >= 0 && flock(ownership.Descriptor, LOCK_EX | LOCK_NB) == 0,
        "incomplete store is already owned",
        "STATE_IN_USE");
    Require(!std::filesystem::exists(std::filesystem::symlink_status(path / "store.ready")),
        "ready store cannot be replaced",
        "CORRUPTION");
    // Preserve every byte for inspection. Never remove an initialized/ready store.
    std::filesystem::rename(path,
        path.parent_path() / ("incomplete-" + path.filename().string() + "-" + detail::NewUuid()));
    SyncDirectory(path.parent_path());
}
void ValidateName(const std::string& name)
{
    Require(!name.empty() && name.size() <= 128 && detail::ValidUtf8(name) &&
            name.find_first_of("/\\") == std::string::npos && name.find('\0') == std::string::npos,
        "database name must be 1..128 UTF-8 bytes without path separators");
}
} // namespace

struct TDatabases::TImpl
{
    TDirectoryLock DirectoryLock;
    std::filesystem::path Root;
    detail::Database Registry;
    mutable std::mutex Mutex;
    std::map<std::string, std::shared_ptr<TEntry>> Entries;
    std::atomic<bool> Healthy{true};
    using TEntries = std::vector<std::shared_ptr<TEntry>>;
    std::shared_ptr<const TEntries> PublishedEntries = std::make_shared<const TEntries>();
    std::function<void(const std::string&)> ScanCanceller;

    void CancelScans(const std::string& id)
    {
        std::function<void(const std::string&)> cancel;
        {
            std::lock_guard lock(Mutex);
            cancel = ScanCanceller;
        }
        if (cancel) {
            cancel(id);
        }
    }

    void Write(std::string_view sql, std::initializer_list<detail::SqlValue> values = {})
    {
        Require(Healthy, "registry needs restart after an uncertain write", "RECOVERY_REQUIRED");
        try {
            Registry.Execute(sql, values);
        } catch (const detail::Failure& error) {
            const int code = error.native_code & 255;
            const bool definite = Registry.Autocommit() &&
                (code == SQLITE_BUSY || code == SQLITE_LOCKED || code == SQLITE_FULL || code == SQLITE_CONSTRAINT);
            if (!definite) {
                Healthy = false;
            }
            throw;
        } catch (...) {
            Healthy = false;
            throw;
        }
    }
    void PublishEntries()
    {
        auto entries = std::make_shared<TEntries>();
        for (const auto& [id, entry] : Entries) {
            (void)id;
            entries->push_back(entry);
        }
        std::atomic_store(&PublishedEntries, std::shared_ptr<const TEntries>(std::move(entries)));
    }
    std::shared_ptr<TEntry> Find(const std::string& id) const
    {
        Require(detail::ValidUuid(id), "invalid database ID");
        for (const auto& entry : Snapshot()) {
            if (entry->ID == id) {
                return entry;
            }
        }
        throw TFailure({"DATABASE_NOT_FOUND", "database not found", RetryHint::OperatorAction});
    }
    TEntries Snapshot() const
    {
        return *std::atomic_load(&PublishedEntries);
    }
    void OpenError(TEntry& entry, const ErrorDetail& error)
    {
        entry.State = "error";
        entry.Error = error;
        if (entry.Instance && (!entry.Initialized || !entry.AutoOpen)) {
            entry.RegistryFence = true;
            entry.Error = ErrorDetail{"RECOVERY_REQUIRED",
                Healthy ? "database registration is incomplete; retry reconciliation"
                        : "database registration is uncertain; restart to resolve registry state",
                RetryHint::SameIdAfterReconcile};
        }
        Publish(entry);
    }
    void Open(TEntry& entry, bool create)
    {
        Require(!entry.Retired, "database not found", "DATABASE_NOT_FOUND");
        Require(!create || entry.Initialized || Healthy, "registry needs restart", "RECOVERY_REQUIRED");
        Require(!entry.RegistryFence || Healthy,
            "database registry intent is uncertain; restart before reopening",
            "RECOVERY_REQUIRED");
        Require(!entry.Closing, "retry close before reopening this database", "DATABASE_BUSY");
        if (entry.Instance) {
            Require(entry.State == "open", "database needs reconciliation or close", "RECOVERY_REQUIRED");
            return;
        }
        const auto path = Root / "databases" / entry.ID;
        entry.State = "opening";
        Publish(entry);
        try {
            const bool initialize = create && !entry.Initialized &&
                !std::filesystem::exists(std::filesystem::symlink_status(path / "store.ready"));
            if (initialize && std::filesystem::exists(std::filesystem::symlink_status(path))) {
                PreserveIncompleteStore(path);
            }
            auto engine = initialize
                ? Unwrap(CreateStore({entry.ID, path.string(), "local-delete-extra-v1", entry.ResourceLimits}))
                : Unwrap(OpenExisting({path.string(), entry.ID, "local-delete-extra-v1", entry.ResourceLimits}));
            entry.Instance = std::move(engine);
            if (!entry.Initialized || !entry.AutoOpen) {
                std::lock_guard lock(Mutex);
                Write("UPDATE databases SET initialized=1, auto_open=1 WHERE id=?", {entry.ID});
            }
            entry.Initialized = true;
            entry.AutoOpen = true;
            entry.RegistryFence = false;
            entry.State = "open";
            entry.Error.reset();
            Publish(entry);
            entry.Accepting = true;
        } catch (const TFailure& error) {
            OpenError(entry, error.Detail);
            throw;
        } catch (const detail::Failure& error) {
            OpenError(entry, error.detail);
            throw;
        } catch (const std::exception&) {
            OpenError(entry, {"IO_ERROR", "database open failed", RetryHint::OperatorAction});
            throw TFailure(*entry.Error);
        }
    }
};

TDatabases::TDatabases(const std::filesystem::path& directory, const std::string& registryVfs)
    : Impl_(std::make_unique<TImpl>())
{
    auto& self = *Impl_;
    Require(directory.is_absolute() && directory.has_filename() && directory == directory.lexically_normal() &&
            directory.string().find('\0') == std::string::npos &&
            std::filesystem::canonical(directory.parent_path()) == directory.parent_path(),
        "data directory must be an absolute path with an existing canonical parent");
    const bool exists = std::filesystem::exists(std::filesystem::symlink_status(directory));
    if (!exists) {
        Require(mkdir(directory.c_str(), 0700) == 0, "cannot create server data directory", "IO_ERROR");
    }
    self.DirectoryLock.Descriptor = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    Require(self.DirectoryLock.Descriptor >= 0, "cannot open server data directory", "IO_ERROR");
    Require(flock(self.DirectoryLock.Descriptor, LOCK_EX | LOCK_NB) == 0,
        "server data directory is already owned",
        "STATE_IN_USE");
    self.Root = directory;
    const auto registry = directory / "registry.sqlite";
    const bool fresh = !std::filesystem::exists(std::filesystem::symlink_status(registry));
    if (fresh) {
        InitializeRegistry(directory, registryVfs);
    }
    self.Registry.Open(registry, SQLITE_OPEN_READWRITE, registryVfs);
    ConfigureRegistry(self.Registry);
    Require(self.Registry.Integer("PRAGMA user_version") == 2, "unsupported server registry version", "CORRUPTION");
    Require(std::filesystem::is_directory(std::filesystem::symlink_status(directory / "databases")),
        "database parent is absent or a symlink",
        "CORRUPTION");
    for (const auto& row :
        self.Registry.Query("SELECT id,name,initialized,auto_open,limits FROM databases ORDER BY name"))
    {
        auto entry = std::make_shared<TEntry>();
        entry->ID = detail::Required(row, 0);
        entry->Name = detail::Required(row, 1);
        Require(detail::ValidUuid(entry->ID), "invalid registered database identity", "CORRUPTION");
        ValidateName(entry->Name);
        const auto initialized = detail::Required(row, 2), autoOpen = detail::Required(row, 3);
        Require((initialized == "0" || initialized == "1") && (autoOpen == "0" || autoOpen == "1"),
            "invalid database lifecycle flags",
            "CORRUPTION");
        entry->Initialized = initialized == "1";
        entry->AutoOpen = autoOpen == "1";
        try {
            const auto stored = ParseJson(detail::Required(row, 4));
            entry->ResourceLimits = ParseLimits(stored);
            Require(stored == EncodeLimits(entry->ResourceLimits), "incomplete persisted limits", "CORRUPTION");
        } catch (const TFailure&) {
            throw TFailure({"CORRUPTION", "invalid persisted database limits", RetryHint::OperatorAction});
        } catch (const TJson::exception&) {
            throw TFailure({"CORRUPTION", "invalid persisted database limits", RetryHint::OperatorAction});
        }
        Require(self.Entries.size() < 64 && self.Entries.emplace(entry->ID, entry).second,
            "invalid or oversized registry",
            "CORRUPTION");
    }
    for (const auto& [id, entry] : self.Entries) {
        (void)id;
        Publish(*entry);
    }
    self.PublishEntries();
    for (const auto& entry : self.Snapshot()) {
        if (entry->AutoOpen) {
            try {
                self.Open(*entry, false);
            } catch (const TFailure&) {
                // One unavailable store must not hide the database administration API.
                continue;
            } catch (const detail::Failure&) {
                continue;
            }
        }
    }
}
TDatabases::~TDatabases() = default;

TJson TDatabases::List() const
{
    auto items = TJson::array();
    for (const auto& entry : Impl_->Snapshot()) {
        const auto descriptor = std::atomic_load(&entry->Descriptor);
        if (descriptor->at("state") != "discarded") {
            items.push_back(*descriptor);
        }
    }
    return {{"items", items}, {"next_cursor", nullptr}};
}
TJson TDatabases::Get(const std::string& id) const
{
    const auto entry = Impl_->Find(id);
    return Current(*entry);
}
StoreHealth TDatabases::Health(const std::string& id) const
{
    const auto entry = Impl_->Find(id);
    // Never wait behind a lifecycle operation: its published descriptor is
    // available throughout recovery, draining, and shutdown.
    std::shared_lock lock(entry->Gate, std::try_to_lock);
    const auto descriptor = Current(*entry);
    const auto state = descriptor.at("state").get<std::string>();
    StoreHealth health;
    if (state == "closed") {
        health.state = StoreState::Closed;
    } else if (state == "closing") {
        health.state = StoreState::Stopping;
    } else if (state == "opening" || state == "reconciling" || !lock.owns_lock()) {
        health.state = StoreState::Recovering;
    } else if (!entry->Instance || entry->RegistryFence) {
        health.state = StoreState::RecoveryRequired;
    } else {
        health = entry->Instance->GetHealth();
        if (!entry->Accepting) {
            health.state = StoreState::Recovering;
            health.read_available = false;
            health.write_ready = false;
        }
    }
    return health;
}

TJson TDatabases::Create(const TJson& body)
{
    CheckKeys(body, {"name", "database_id", "limits"});
    const auto requestedLimits = body.contains("limits") ? ParseLimits(body.at("limits")) : Limits{};
    const auto name = body.at("name").get<std::string>();
    ValidateName(name);
    const auto supplied = body.value("database_id", "");
    Require(!body.contains("database_id") || detail::ValidUuid(supplied), "invalid database ID");
    std::shared_ptr<TEntry> entry;
    {
        std::lock_guard lock(Impl_->Mutex);
        for (const auto& [id, candidate] : Impl_->Entries) {
            if (candidate->Name == name || id == supplied) {
                Require(candidate->Name == name && (supplied.empty() || id == supplied),
                    "database name or ID already exists",
                    "ALREADY_EXISTS");
                entry = candidate;
                break;
            }
        }
        if (!entry) {
            Require(Impl_->Healthy, "registry needs restart", "RECOVERY_REQUIRED");
            Require(Impl_->Entries.size() < 64, "database limit reached", "RESOURCE_EXHAUSTED");
            entry = std::make_shared<TEntry>();
            entry->Name = name;
            entry->ID = supplied.empty() ? detail::NewUuid() : supplied;
            entry->ResourceLimits = requestedLimits;
            Impl_->Write("INSERT INTO databases(id,name,initialized,auto_open,limits) VALUES(?,?,0,1,?)",
                {entry->ID, entry->Name, EncodeLimits(entry->ResourceLimits).dump()});
            Publish(*entry);
            Impl_->Entries.emplace(entry->ID, entry);
            Impl_->PublishEntries();
        }
    }
    {
        std::shared_lock fast(entry->Gate, std::try_to_lock);
        if (fast.owns_lock() && entry->Accepting) {
            const auto descriptor = Current(*entry);
            Require(!body.contains("limits") || EncodeLimits(requestedLimits) == descriptor.at("limits"),
                "database already exists with different limits",
                "ALREADY_EXISTS");
            if (descriptor.at("state") == "open") {
                return descriptor;
            }
        }
    }
    std::lock_guard lifecycle(entry->Lifecycle);
    Require(!entry->Retired, "database not found", "DATABASE_NOT_FOUND");
    if (entry->Accepting && entry->State == "open") {
        Require(!body.contains("limits") || EncodeLimits(requestedLimits) == EncodeLimits(entry->ResourceLimits),
            "database already exists with different limits",
            "ALREADY_EXISTS");
        return Current(*entry);
    }
    const bool admitted = entry->Accepting.exchange(false);
    std::unique_lock lock(entry->Gate);
    try {
        Require(!body.contains("limits") || EncodeLimits(requestedLimits) == EncodeLimits(entry->ResourceLimits),
            "database already exists with different limits",
            "ALREADY_EXISTS");
        Impl_->Open(*entry, true);
        entry->Accepting = true;
    } catch (...) {
        entry->Accepting = admitted;
        throw;
    }
    return Describe(*entry);
}
TJson TDatabases::Change(const std::string& id, std::string_view action, const TJson& body)
{
    Require(action == "open" || action == "close" || action == "reconcile" || action == "discard",
        "unknown database action");
    if (action == "open") {
        CheckKeys(body, {"limits"});
        if (body.contains("limits")) {
            (void)ParseLimits(body.at("limits"));
        }
    } else {
        CheckKeys(body, {});
    }
    const auto entry = Impl_->Find(id);
    if (action == "open") {
        std::shared_lock fast(entry->Gate, std::try_to_lock);
        if (fast.owns_lock() && entry->Accepting) {
            const auto descriptor = Current(*entry);
            if (descriptor.at("state") == "open") {
                const auto limits = body.contains("limits")
                    ? ParseLimits(body.at("limits"), ParseLimits(descriptor.at("limits")))
                    : ParseLimits(descriptor.at("limits"));
                Require(EncodeLimits(limits) == descriptor.at("limits"),
                    "close database before changing limits",
                    "DATABASE_BUSY");
                return descriptor;
            }
        }
    }
    std::lock_guard lifecycle(entry->Lifecycle);
    Require(!entry->Retired, "database not found", "DATABASE_NOT_FOUND");
    if (action == "discard") {
        Require(!entry->Initialized && !entry->Instance && !entry->Closing,
            "only an uninitialized, unowned reservation can be discarded",
            "PRECONDITION_FAILED");
        Require(Impl_->Healthy, "registry needs restart", "RECOVERY_REQUIRED");
        std::unique_lock gate(entry->Gate);
        const auto path = Impl_->Root / "databases" / entry->ID;
        if (std::filesystem::exists(std::filesystem::symlink_status(path))) {
            Require(!std::filesystem::exists(std::filesystem::symlink_status(path / "store.ready")),
                "ready stores cannot be discarded",
                "PRECONDITION_FAILED");
            PreserveIncompleteStore(path);
        }
        TJson response{{"database_id", entry->ID}, {"discarded", true}};
        auto descriptor = Current(*entry);
        descriptor["state"] = "discarded";
        auto retired = std::make_shared<const TJson>(std::move(descriptor));
        std::lock_guard registryLock(Impl_->Mutex);
        auto entries = std::make_shared<TImpl::TEntries>();
        for (const auto& [registered, candidate] : Impl_->Entries) {
            if (registered != id) {
                entries->push_back(candidate);
            }
        }
        try {
            Impl_->Write("DELETE FROM databases WHERE id=? AND initialized=0", {id});
        } catch (...) {
            if (!Impl_->Healthy) {
                entry->RegistryFence = true;
            }
            throw;
        }
        Require(Impl_->Registry.Changes() == 1, "reservation changed before discard", "PRECONDITION_FAILED");
        // No allocation follows the confirmed registry commit. Stale callers
        // retaining this entry must observe retirement before they can reopen it.
        entry->Retired = true;
        entry->Accepting = false;
        std::atomic_store(&entry->Descriptor, std::move(retired));
        Impl_->Entries.erase(id);
        std::atomic_store(&Impl_->PublishedEntries, std::shared_ptr<const TImpl::TEntries>(std::move(entries)));
        return response;
    }
    if (action == "open" && entry->Accepting && entry->State == "open") {
        const auto limits =
            body.contains("limits") ? ParseLimits(body.at("limits"), entry->ResourceLimits) : entry->ResourceLimits;
        Require(EncodeLimits(limits) == EncodeLimits(entry->ResourceLimits),
            "close database before changing limits",
            "DATABASE_BUSY");
        return Current(*entry);
    }
    const bool admitted = entry->Accepting.exchange(false);
    const auto previousState = entry->State;
    entry->State = action == "close" ? "closing" : action == "reconcile" ? "reconciling" : "opening";
    Publish(*entry);
    std::unique_lock lock(entry->Gate);
    try {
        if (action == "open") {
            const auto limits =
                body.contains("limits") ? ParseLimits(body.at("limits"), entry->ResourceLimits) : entry->ResourceLimits;
            const bool changed = EncodeLimits(limits) != EncodeLimits(entry->ResourceLimits);
            Require(!changed || (!entry->Instance && !entry->Closing && previousState == "closed"),
                "close database before changing limits",
                "DATABASE_BUSY");
            entry->State = previousState;
            if (!entry->Instance && !entry->Closing && (changed || !entry->AutoOpen)) {
                std::lock_guard registryLock(Impl_->Mutex);
                Impl_->Write("UPDATE databases SET limits=?, auto_open=1 WHERE id=?",
                    {EncodeLimits(limits).dump(), entry->ID});
                entry->ResourceLimits = limits;
                entry->AutoOpen = true;
            }
            Impl_->Open(*entry, false);
            entry->Accepting = entry->State == "open";
        } else if (action == "close") {
            if (Impl_->Healthy && entry->AutoOpen) {
                try {
                    std::lock_guard registryLock(Impl_->Mutex);
                    Impl_->Write("UPDATE databases SET auto_open=0 WHERE id=?", {entry->ID});
                    entry->AutoOpen = false;
                } catch (...) {
                    if (!Impl_->Healthy) {
                        entry->RegistryFence = true;
                        entry->Error = ErrorDetail{"RECOVERY_REQUIRED",
                            "close intent is uncertain; restart to resolve registry state",
                            RetryHint::SameIdAfterReconcile};
                    }
                    throw;
                }
            }
            entry->Closing = true;
            Impl_->CancelScans(entry->ID);
            entry->Error.reset();
            if (entry->Instance) {
                const auto closed = entry->Instance->Shutdown(std::chrono::seconds(60));
                if (!closed) {
                    entry->Error = closed.error();
                    if (closed.error().code == "DEADLINE_EXCEEDED") {
                        throw TFailure(closed.error());
                    }
                    // Shutdown acquired the writer lock; admitted calls and cursor
                    // leases have drained. Destruction closes the catalog without
                    // requiring recovery of damaged files.
                }
                entry->Instance.reset();
            }
            entry->Closing = false;
            entry->State = "closed";
            if (!Impl_->Healthy) {
                entry->Error = ErrorDetail{"RECOVERY_REQUIRED",
                    "closed locally; restart to resolve registry open intent",
                    RetryHint::SameIdAfterReconcile};
            }
        } else {
            Require(entry->Instance && !entry->Closing, "open database or retry close first", "DATABASE_CLOSED");
            Impl_->CancelScans(entry->ID);
            const auto reconciled = entry->Instance->Reconcile();
            if (reconciled && entry->RegistryFence && Impl_->Healthy) {
                std::lock_guard registryLock(Impl_->Mutex);
                Impl_->Write("UPDATE databases SET initialized=1, auto_open=1 WHERE id=?", {entry->ID});
                entry->Initialized = true;
                entry->AutoOpen = true;
                entry->RegistryFence = false;
            }
            entry->Accepting = !entry->RegistryFence;
            if (!reconciled) {
                entry->State = "error";
                entry->Error = reconciled.error();
                throw TFailure(reconciled.error());
            }
            entry->State = entry->RegistryFence ? "error" : "open";
            if (!entry->RegistryFence) {
                entry->Error.reset();
            }
        }
    } catch (...) {
        if (entry->RegistryFence) {
            entry->State = "error";
        } else if (entry->Closing) {
            entry->State = "closing";
        } else if (entry->State == "opening" || entry->State == "closing" || entry->State == "reconciling") {
            entry->State = previousState;
            entry->Accepting = admitted;
        } else if (action == "open") {
            entry->Accepting = admitted;
        }
        Publish(*entry);
        throw;
    }
    Publish(*entry);
    return Current(*entry);
}
TResponse TDatabases::Use(const std::string& id,
    const std::function<TResponse(std::shared_ptr<Engine>)>& call,
    bool recoveryOnly) const
{
    const auto entry = Impl_->Find(id);
    const auto descriptor = Current(*entry);
    const bool recovery = recoveryOnly && descriptor.at("state") == "error";
    if (!entry->Accepting && !recovery) {
        const auto state = descriptor.at("state").get<std::string>();
        if (state == "error" && descriptor.contains("error") &&
            descriptor.at("error").at("code") == "RECOVERY_REQUIRED")
        {
            throw TFailure({"RECOVERY_REQUIRED",
                "database admission is fenced until registry recovery",
                RetryHint::SameIdAfterReconcile});
        }
        const bool busy = state == "opening" || state == "closing" || state == "reconciling" || state == "open";
        throw TFailure({busy ? "DATABASE_BUSY" : "DATABASE_CLOSED",
            busy ? "database lifecycle is busy; retry the same request" : "database is closed",
            busy ? RetryHint::SameIdAfterReconcile : RetryHint::OperatorAction});
    }
    std::shared_lock lock(entry->Gate, std::try_to_lock);
    Require(lock.owns_lock(), "database lifecycle is busy; retry the same request", "DATABASE_BUSY");
    Require(!entry->RegistryFence || recoveryOnly,
        "database admission is fenced until registry recovery",
        "RECOVERY_REQUIRED");
    Require(entry->Instance && (entry->Accepting || (recoveryOnly && entry->RegistryFence)),
        "database is closed",
        "DATABASE_CLOSED");
    return call(entry->Instance);
}
void TDatabases::SetScanCanceller(std::function<void(const std::string&)> cancel)
{
    std::lock_guard lock(Impl_->Mutex);
    Impl_->ScanCanceller = std::move(cancel);
}
void TDatabases::Shutdown()
{
    std::optional<ErrorDetail> first;
    for (const auto& entry : Impl_->Snapshot()) {
        std::lock_guard lifecycle(entry->Lifecycle);
        entry->Accepting = false;
        std::unique_lock lock(entry->Gate);
        Impl_->CancelScans(entry->ID);
        if (entry->Instance) {
            const auto closed = entry->Instance->Shutdown(std::chrono::seconds(60));
            if (closed || closed.error().code != "DEADLINE_EXCEEDED") {
                if (!closed) {
                    entry->Error = closed.error();
                }
                entry->Instance.reset();
                entry->State = "closed";
                Publish(*entry);
            } else if (!first) {
                first = closed.error();
            }
        }
    }
    if (first) {
        throw TFailure(*first);
    }
}
} // namespace glamdb::NServer
