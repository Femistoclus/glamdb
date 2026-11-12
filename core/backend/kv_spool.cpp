#include "core/backend/kv_spool.h"

#include <sqlite3.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <unistd.h>
#include <utility>

namespace glamdb::detail {
namespace {

constexpr uint64_t PageBytes = 4096;
constexpr uint64_t MaxRowBytes = (2ULL << 20) + 4096;

// A private VFS delegates to the configured native VFS and counts requested
// scratch reads/writes before issuing them. No global SQLite limits or VFS
// defaults are changed. mmap and journals are disabled for this disposable DB.
class TCountingVfs
{
public:
    explicit TCountingVfs(std::string name, uint64_t maxIoBytes)
        : Name_(std::move(name))
        , MaxIoBytes_(maxIoBytes)
        , Native_(sqlite3_vfs_find(nullptr))
    {
        if (Native_ == nullptr) {
            return;
        }
        Vfs_.iVersion = 1;
        Vfs_.szOsFile = static_cast<int>(sizeof(TFile)) + Native_->szOsFile;
        Vfs_.mxPathname = Native_->mxPathname;
        Vfs_.zName = Name_.c_str();
        Vfs_.pAppData = this;
        Vfs_.xOpen = Open;
        Vfs_.xDelete = [](sqlite3_vfs* v, const char* p, int sync) {
            auto& self = Self(v);
            return self.Native_->xDelete(self.Native_, p, sync);
        };
        Vfs_.xAccess = [](sqlite3_vfs* v, const char* p, int flags, int* result) {
            auto& self = Self(v);
            return self.Native_->xAccess(self.Native_, p, flags, result);
        };
        Vfs_.xFullPathname = [](sqlite3_vfs* v, const char* p, int size, char* result) {
            auto& self = Self(v);
            return self.Native_->xFullPathname(self.Native_, p, size, result);
        };
        Vfs_.xDlOpen = [](sqlite3_vfs* v, const char* p) {
            auto& self = Self(v);
            return self.Native_->xDlOpen(self.Native_, p);
        };
        Vfs_.xDlError = [](sqlite3_vfs* v, int size, char* result) {
            auto& self = Self(v);
            self.Native_->xDlError(self.Native_, size, result);
        };
        Vfs_.xDlSym = [](sqlite3_vfs* v, void* handle, const char* symbol) {
            auto& self = Self(v);
            return self.Native_->xDlSym(self.Native_, handle, symbol);
        };
        Vfs_.xDlClose = [](sqlite3_vfs* v, void* handle) {
            auto& self = Self(v);
            self.Native_->xDlClose(self.Native_, handle);
        };
        Vfs_.xRandomness = [](sqlite3_vfs* v, int size, char* result) {
            auto& self = Self(v);
            return self.Native_->xRandomness(self.Native_, size, result);
        };
        Vfs_.xSleep = [](sqlite3_vfs* v, int micros) {
            auto& self = Self(v);
            return self.Native_->xSleep(self.Native_, micros);
        };
        Vfs_.xCurrentTime = [](sqlite3_vfs* v, double* result) {
            auto& self = Self(v);
            return self.Native_->xCurrentTime(self.Native_, result);
        };
        Vfs_.xGetLastError = [](sqlite3_vfs* v, int size, char* result) {
            auto& self = Self(v);
            return self.Native_->xGetLastError ? self.Native_->xGetLastError(self.Native_, size, result) : 0;
        };
        Registered_ = sqlite3_vfs_register(&Vfs_, 0) == SQLITE_OK;
    }

    ~TCountingVfs()
    {
        if (Registered_) {
            sqlite3_vfs_unregister(&Vfs_);
        }
    }

    const char* Name() const
    {
        return Registered_ ? Name_.c_str() : nullptr;
    }

    uint64_t IoBytes() const
    {
        return IoBytes_;
    }

    bool Exceeded() const
    {
        return Exceeded_;
    }

private:
    struct TFile
    {
        sqlite3_file Base{};
        TCountingVfs* Owner = nullptr;
        sqlite3_file* Native = nullptr;
    };

    std::string Name_;
    uint64_t MaxIoBytes_;
    uint64_t IoBytes_ = 0;
    sqlite3_vfs* Native_;
    sqlite3_vfs Vfs_{};
    bool Registered_ = false;
    bool Exceeded_ = false;

    bool Charge(int count)
    {
        if (count < 0 || static_cast<uint64_t>(count) > MaxIoBytes_ - IoBytes_) {
            Exceeded_ = true;
            return false;
        }
        IoBytes_ += static_cast<uint64_t>(count);
        return true;
    }

    static TCountingVfs& Self(sqlite3_vfs* vfs)
    {
        return *static_cast<TCountingVfs*>(vfs->pAppData);
    }

    static TFile& File(sqlite3_file* file)
    {
        // sqlite3_file is the first member of the standard-layout wrapper.
        return *reinterpret_cast<TFile*>(file);
    }

    static int Open(sqlite3_vfs* vfs, const char* path, sqlite3_file* file, int flags, int* out)
    {
        auto& self = Self(vfs);
        auto& wrapper = *new (file) TFile;
        wrapper.Owner = &self;
        wrapper.Native = reinterpret_cast<sqlite3_file*>(reinterpret_cast<std::byte*>(file) + sizeof(TFile));
        std::memset(wrapper.Native, 0, static_cast<size_t>(self.Native_->szOsFile));
        const auto opened = self.Native_->xOpen(self.Native_, path, wrapper.Native, flags, out);
        if (opened != SQLITE_OK) {
            if (wrapper.Native->pMethods) {
                (void)wrapper.Native->pMethods->xClose(wrapper.Native);
            }
            return opened;
        }
        static const sqlite3_io_methods Methods{.iVersion = 1,
            .xClose =
                [](sqlite3_file* f) {
                    auto& wrapped = File(f);
                    const auto result = wrapped.Native->pMethods->xClose(wrapped.Native);
                    wrapped.Base.pMethods = nullptr;
                    return result;
                },
            .xRead =
                // SQLite fixes this callback's byte-count/offset parameter order.
                // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
            [](sqlite3_file* f, void* data, int count, sqlite3_int64 offset) {
                auto& wrapped = File(f);
                return wrapped.Owner->Charge(count)
                    ? wrapped.Native->pMethods->xRead(wrapped.Native, data, count, offset)
                    : SQLITE_FULL;
            },
            .xWrite =
                // SQLite fixes this callback's byte-count/offset parameter order.
                // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
            [](sqlite3_file* f, const void* data, int count, sqlite3_int64 offset) {
                auto& wrapped = File(f);
                return wrapped.Owner->Charge(count)
                    ? wrapped.Native->pMethods->xWrite(wrapped.Native, data, count, offset)
                    : SQLITE_FULL;
            },
            .xTruncate =
                [](sqlite3_file* f, sqlite3_int64 size) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xTruncate(wrapped.Native, size);
                },
            .xSync =
                [](sqlite3_file* f, int flags) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xSync(wrapped.Native, flags);
                },
            .xFileSize =
                [](sqlite3_file* f, sqlite3_int64* size) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xFileSize(wrapped.Native, size);
                },
            .xLock =
                [](sqlite3_file* f, int flags) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xLock(wrapped.Native, flags);
                },
            .xUnlock =
                [](sqlite3_file* f, int flags) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xUnlock(wrapped.Native, flags);
                },
            .xCheckReservedLock =
                [](sqlite3_file* f, int* result) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xCheckReservedLock(wrapped.Native, result);
                },
            .xFileControl =
                [](sqlite3_file* f, int op, void* argument) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xFileControl(wrapped.Native, op, argument);
                },
            .xSectorSize =
                [](sqlite3_file* f) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xSectorSize(wrapped.Native);
                },
            .xDeviceCharacteristics =
                [](sqlite3_file* f) {
                    auto& wrapped = File(f);
                    return wrapped.Native->pMethods->xDeviceCharacteristics(wrapped.Native);
                },
            .xShmMap = nullptr,
            .xShmLock = nullptr,
            .xShmBarrier = nullptr,
            .xShmUnmap = nullptr,
            .xFetch = nullptr,
            .xUnfetch = nullptr};
        wrapper.Base.pMethods = &Methods;
        return SQLITE_OK;
    }
};

ErrorDetail SqlError(int code, bool exceeded = false)
{
    if (exceeded || (code & 255) == SQLITE_FULL || (code & 255) == SQLITE_NOMEM || (code & 255) == SQLITE_TOOBIG) {
        return {"RESOURCE_EXHAUSTED",
            "KV scratch exceeds its finite space, memory or I/O budget",
            RetryHint::OperatorAction};
    }
    return {"IO_ERROR", std::string("KV scratch SQLite failure: ") + sqlite3_errstr(code), RetryHint::OperatorAction};
}

} // namespace

struct TKvSpool::TState
{
    std::filesystem::path Path;
    std::unique_ptr<TCountingVfs> Vfs;
    sqlite3* Database = nullptr;
    std::array<sqlite3_stmt*, 3> Insert{};
    std::function<Status()> CheckActive;
    std::optional<ErrorDetail> FirstError;
    bool Sealed = false;

    ~TState()
    {
        for (auto* statement : Insert) {
            sqlite3_finalize(statement);
        }
        sqlite3_close_v2(Database);
        // The private VFS must outlive every file, statement and cursor.
        Vfs.reset();
        if (!Path.empty()) {
            std::error_code ignored;
            std::filesystem::remove(Path, ignored);
        }
    }

    Status Active()
    {
        if (FirstError) {
            return std::unexpected(FirstError.value());
        }
        if (CheckActive) {
            auto status = CheckActive();
            if (!status) {
                FirstError = status.error();
                return status;
            }
        }
        return {};
    }

    Status Error(int code)
    {
        if (FirstError) {
            return std::unexpected(*FirstError);
        }
        const auto& detail = FirstError.emplace(SqlError(code, Vfs && Vfs->Exceeded()));
        return std::unexpected(detail);
    }

    Status Execute(const std::string& sql)
    {
        const auto code = sqlite3_exec(Database, sql.c_str(), nullptr, nullptr, nullptr);
        return code == SQLITE_OK ? Status{} : Error(code);
    }
};

struct TKvSpool::TCursor::TImpl
{
    std::shared_ptr<TState> State;
    sqlite3_stmt* Statement = nullptr;
    bool Done = false;

    ~TImpl()
    {
        sqlite3_finalize(Statement);
    }
};

TKvSpool::TKvSpool(std::shared_ptr<TState> state)
    : State_(std::move(state))
{ }

TKvSpool::~TKvSpool() = default;

Result<std::unique_ptr<TKvSpool>> TKvSpool::Make(const std::filesystem::path& directory,
    std::string_view token,
    // Space and requested I/O are independent defaulted dimensions in the API.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    uint64_t maxBytes,
    uint64_t maxIoBytes,
    std::function<Status()> checkActive)
{
    if (token.size() != 32 ||
        !std::ranges::all_of(token,
            [](char c) {
                return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
            }) ||
        maxBytes < 2 * PageBytes || maxBytes > (4ULL << 30) || maxIoBytes == 0 || maxIoBytes > (16ULL << 30))
    {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "invalid KV scratch identity or finite limits"});
    }
    auto state = std::make_shared<TState>();
    state->CheckActive = std::move(checkActive);
    if (auto active = state->Active(); !active) {
        return std::unexpected(active.error());
    }
    const auto path = directory / (".glamdb-kv-" + std::string(token) + ".sqlite");
    const auto descriptor = open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (descriptor < 0) {
        return std::unexpected(
            ErrorDetail{"IO_ERROR", "cannot create exclusive KV scratch file", RetryHint::OperatorAction});
    }
    state->Path = path;
    if (close(descriptor) != 0) {
        return std::unexpected(ErrorDetail{"IO_ERROR", "cannot close KV scratch file", RetryHint::OperatorAction});
    }
    state->Vfs = std::make_unique<TCountingVfs>("glamdb-kv-" + std::string(token) + "-" +
            std::to_string(reinterpret_cast<uintptr_t>(state.get())),
        maxIoBytes);
    if (!state->Vfs->Name()) {
        return std::unexpected(
            ErrorDetail{"IO_ERROR", "cannot register KV scratch I/O accounting", RetryHint::OperatorAction});
    }
    const auto opened = sqlite3_open_v2(path.c_str(),
        &state->Database,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_PRIVATECACHE | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_EXRESCODE |
            SQLITE_OPEN_NOFOLLOW,
        state->Vfs->Name());
    if (opened != SQLITE_OK) {
        return std::unexpected(SqlError(opened, state->Vfs->Exceeded()));
    }
    sqlite3_extended_result_codes(state->Database, 1);
    sqlite3_limit(state->Database, SQLITE_LIMIT_LENGTH, static_cast<int>(MaxRowBytes));
    sqlite3_limit(state->Database, SQLITE_LIMIT_ATTACHED, 0);
    auto configured =
        state->Execute("PRAGMA main.page_size=4096; PRAGMA main.cache_size=-8192; PRAGMA main.cache_spill=ON;"
                       "PRAGMA main.mmap_size=0; PRAGMA temp_store=FILE; PRAGMA main.journal_mode=OFF;"
                       "PRAGMA main.synchronous=OFF; PRAGMA main.max_page_count=" +
            std::to_string(maxBytes / PageBytes) +
            "; CREATE TABLE mutations(route INTEGER NOT NULL,key BLOB NOT NULL,payload BLOB NOT NULL,"
            "PRIMARY KEY(route,key)) WITHOUT ROWID; BEGIN");
    if (!configured) {
        return std::unexpected(configured.error());
    }
    constexpr std::array<const char*, 3> sql{"INSERT INTO mutations(route,key,payload) VALUES(?1,?2,?3)",
        "INSERT INTO mutations(route,key,payload) VALUES(?1,?2,?3) ON CONFLICT(route,key) DO NOTHING",
        "INSERT INTO mutations(route,key,payload) VALUES(?1,?2,?3) ON CONFLICT(route,key) DO UPDATE SET "
        "payload=excluded.payload"};
    for (size_t i = 0; i < sql.size(); ++i) {
        const auto code = sqlite3_prepare_v2(state->Database, sql[i], -1, &state->Insert[i], nullptr);
        if (code != SQLITE_OK) {
            return std::unexpected(SqlError(code));
        }
    }
    return std::unique_ptr<TKvSpool>(new TKvSpool(std::move(state)));
}

Status TKvSpool::Insert(int64_t route, std::string_view key, std::string_view payload, DuplicatePolicy policy)
{
    if (auto active = State_->Active(); !active) {
        return active;
    }
    if (State_->Sealed || route < 0 || key.size() > 1024 || payload.size() > (2ULL << 20) ||
        policy < DuplicatePolicy::Reject || policy > DuplicatePolicy::LastWins)
    {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "invalid KV scratch row, route or duplicate policy"});
    }
    auto* statement = State_->Insert[static_cast<size_t>(policy) - 1];
    auto code = sqlite3_bind_int64(statement, 1, route);
    if (code == SQLITE_OK) {
        code = sqlite3_bind_blob(statement,
            2,
            key.empty() ? "" : key.data(),
            static_cast<int>(key.size()),
            SQLITE_TRANSIENT);
    }
    if (code == SQLITE_OK) {
        // SQLite treats a nullptr zero-length blob as NULL, so preserve empty payloads explicitly.
        code = sqlite3_bind_blob(statement,
            3,
            payload.empty() ? "" : payload.data(),
            static_cast<int>(payload.size()),
            SQLITE_TRANSIENT);
    }
    if (code == SQLITE_OK) {
        code = sqlite3_step(statement);
    }
    const auto reset = sqlite3_reset(statement);
    const auto cleared = sqlite3_clear_bindings(statement);
    if ((code & 255) == SQLITE_CONSTRAINT && policy == DuplicatePolicy::Reject) {
        const auto& detail = State_->FirstError.emplace(
            ErrorDetail{"INVALID_ARGUMENT", "migration source contains a duplicate key", RetryHint::FixInput});
        return std::unexpected(detail);
    }
    if (code != SQLITE_DONE || reset != SQLITE_OK || cleared != SQLITE_OK) {
        return State_->Error(code != SQLITE_DONE ? code : reset != SQLITE_OK ? reset : cleared);
    }
    return State_->Active();
}

Status TKvSpool::Seal()
{
    if (auto active = State_->Active(); !active) {
        return active;
    }
    if (State_->Sealed) {
        return {};
    }
    if (auto status = State_->Execute("COMMIT; PRAGMA query_only=ON"); !status) {
        return status;
    }
    State_->Sealed = true;
    return {};
}

Result<std::unique_ptr<TKvSpool::TCursor>> TKvSpool::ReadRoute(int64_t route)
{
    if (auto active = State_->Active(); !active) {
        return std::unexpected(active.error());
    }
    if (!State_->Sealed || route < 0) {
        return std::unexpected(ErrorDetail{"INVALID_ARGUMENT", "KV scratch must be sealed before reading"});
    }
    auto impl = std::make_unique<TCursor::TImpl>();
    impl->State = State_;
    const auto prepared = sqlite3_prepare_v2(State_->Database,
        "SELECT key,payload FROM mutations WHERE route=?1 ORDER BY key",
        -1,
        &impl->Statement,
        nullptr);
    if (prepared != SQLITE_OK) {
        return std::unexpected(State_->Error(prepared).error());
    }
    const auto bound = sqlite3_bind_int64(impl->Statement, 1, route);
    if (bound != SQLITE_OK) {
        return std::unexpected(State_->Error(bound).error());
    }
    return std::unique_ptr<TCursor>(new TCursor(std::move(impl)));
}

uint64_t TKvSpool::IoBytes() const
{
    return State_->Vfs->IoBytes();
}

uint64_t TKvSpool::AllocatedBytes() const
{
    std::error_code error;
    const auto bytes = std::filesystem::file_size(State_->Path, error);
    return error ? 0 : bytes;
}

TKvSpool::TCursor::TCursor(std::unique_ptr<TImpl> impl)
    : Impl_(std::move(impl))
{ }

TKvSpool::TCursor::~TCursor() = default;

Result<std::optional<TKvSpool::TEntry>> TKvSpool::TCursor::Next()
{
    if (auto active = Impl_->State->Active(); !active) {
        return std::unexpected(active.error());
    }
    if (Impl_->Done) {
        return std::optional<TEntry>{};
    }
    const auto code = sqlite3_step(Impl_->Statement);
    if (code == SQLITE_DONE) {
        Impl_->Done = true;
        return std::optional<TEntry>{};
    }
    if (code != SQLITE_ROW) {
        return std::unexpected(Impl_->State->Error(code).error());
    }
    const auto bytes = [&](int column) {
        const auto count = sqlite3_column_bytes(Impl_->Statement, column);
        const auto* data = static_cast<const char*>(sqlite3_column_blob(Impl_->Statement, column));
        return count == 0 ? std::string() : std::string(data, static_cast<size_t>(count));
    };
    return std::optional<TEntry>(std::in_place, bytes(0), bytes(1));
}

} // namespace glamdb::detail
