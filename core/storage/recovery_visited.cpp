#include "core/storage/recovery_visited.h"
#include "core/storage/iceberg_error_boundary.h"

#include <iceberg/util/macros.h>
#include <sqlite3.h>

#include <filesystem>
#include <string>
#include <unistd.h>
#include <utility>

namespace NGlamDB::NStorage {
using iceberg::Error;

namespace {

constexpr std::uint64_t PageBytes = 4096;
constexpr std::uint64_t MaxWorkspaceBytes = 1ULL << 40;
constexpr std::size_t MaxPathBytes = 2048;

iceberg::Status SqliteError(int code)
{
    if ((code & 255) == SQLITE_FULL || (code & 255) == SQLITE_NOMEM) {
        return iceberg::IOError("{}CAPACITY_EXCEEDED: Recovery visited workspace is full (SQLite {}).",
            ResourcePrefix,
            code);
    }
    return iceberg::IOError("Recovery visited workspace failed (SQLite {}): {}.", code, sqlite3_errstr(code));
}

iceberg::Status Execute(sqlite3* database, const std::string& sql)
{
    const auto code = sqlite3_exec(database, sql.c_str(), nullptr, nullptr, nullptr);
    return code == SQLITE_OK ? iceberg::Status{} : SqliteError(code);
}

iceberg::Status CheckPragma(sqlite3* database, const std::string& sql, std::string_view expected)
{
    sqlite3_stmt* raw = nullptr;
    const auto prepared = sqlite3_prepare_v2(database, sql.c_str(), -1, &raw, nullptr);
    const std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(raw, sqlite3_finalize);
    if (prepared != SQLITE_OK) {
        return SqliteError(prepared);
    }
    const auto code = sqlite3_step(statement.get());
    if (code != SQLITE_ROW) {
        return SqliteError(code);
    }
    const auto* text = sqlite3_column_text(statement.get(), 0);
    if (text == nullptr || std::string_view(reinterpret_cast<const char*>(text)) != expected) {
        return iceberg::Invalid("Recovery visited workspace configuration was not applied.");
    }
    const auto done = sqlite3_step(statement.get());
    return done == SQLITE_DONE ? iceberg::Status{} : SqliteError(done);
}

bool IsKnownKind(NIO::EFileKind kind)
{
    switch (kind) {
        case NIO::EFileKind::Data:
        case NIO::EFileKind::Metadata:
        case NIO::EFileKind::Manifest:
        case NIO::EFileKind::ManifestList:
            return true;
        case NIO::EFileKind::Other:
            return false;
    }
    return false;
}

} // namespace

iceberg::Result<std::unique_ptr<TRecoveryVisited>> TRecoveryVisited::Make(std::uint64_t maxBytes,
    const std::string& directory)
{
    return CatchIcebergExceptions([&]() -> iceberg::Result<std::unique_ptr<TRecoveryVisited>> {
        if (maxBytes < 2 * PageBytes || maxBytes > MaxWorkspaceBytes) {
            return iceberg::InvalidArgument("Recovery visited workspace must be between 8 KiB and 1 TiB.");
        }
        // Empty-name databases select their backing store before PRAGMA temp_store can run.
        if (!sqlite3_compileoption_used("TEMP_STORE=0") && !sqlite3_compileoption_used("TEMP_STORE=1")) {
            return iceberg::NotSupported("SQLite must default temporary databases to disk storage.");
        }
        auto result = std::unique_ptr<TRecoveryVisited>(new TRecoveryVisited);
        ICEBERG_RETURN_UNEXPECTED(result->Initialize(maxBytes, directory));
        return result;
    });
}

iceberg::Status TRecoveryVisited::Initialize(std::uint64_t maxBytes, const std::string& directory)
{
    if (!directory.empty()) {
        // Process death can leave this file behind. Exclusive, identity-validated
        // StoreLease::Open reclaims this exact name pattern before device admission.
        Path_ = (std::filesystem::path(directory) / ".recovery-XXXXXX").string();
        const auto descriptor = mkstemp(Path_.data());
        if (descriptor < 0) {
            Path_.clear();
            return iceberg::IOError("Cannot create recovery workspace.");
        }
        if (close(descriptor) != 0) {
            return iceberg::IOError("Cannot close recovery workspace.");
        }
    }
    const auto opened = sqlite3_open_v2(Path_.c_str(),
        &Database_,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_PRIVATECACHE | SQLITE_OPEN_FULLMUTEX |
            SQLITE_OPEN_EXRESCODE,
        nullptr);
    if (opened != SQLITE_OK) {
        return SqliteError(opened);
    }
    sqlite3_extended_result_codes(Database_, 1);
    sqlite3_limit(Database_, SQLITE_LIMIT_LENGTH, static_cast<int>(MaxPathBytes + 256));
    sqlite3_limit(Database_, SQLITE_LIMIT_ATTACHED, 0);
    ICEBERG_RETURN_UNEXPECTED(Execute(Database_,
        "PRAGMA main.page_size=4096; PRAGMA temp_store=FILE; PRAGMA main.cache_size=8;"
        "PRAGMA main.cache_spill=ON; PRAGMA main.mmap_size=0;"
        "PRAGMA main.journal_mode=OFF; PRAGMA main.synchronous=OFF;"));
    const auto pages = std::to_string(maxBytes / PageBytes);
    ICEBERG_RETURN_UNEXPECTED(CheckPragma(Database_, "PRAGMA main.max_page_count=" + pages, pages));
    ICEBERG_RETURN_UNEXPECTED(CheckPragma(Database_, "PRAGMA main.page_size", "4096"));
    ICEBERG_RETURN_UNEXPECTED(CheckPragma(Database_, "PRAGMA main.cache_size", "8"));
    ICEBERG_RETURN_UNEXPECTED(CheckPragma(Database_, "PRAGMA temp_store", "1"));
    ICEBERG_RETURN_UNEXPECTED(CheckPragma(Database_, "PRAGMA main.journal_mode", "off"));
    ICEBERG_RETURN_UNEXPECTED(CheckPragma(Database_, "PRAGMA main.synchronous", "0"));
    ICEBERG_RETURN_UNEXPECTED(Execute(Database_,
        "CREATE TABLE visited(kind INTEGER NOT NULL,path BLOB NOT NULL,PRIMARY KEY(kind,path)) WITHOUT ROWID"));
    const auto prepared = sqlite3_prepare_v2(Database_,
        "INSERT INTO main.visited(kind,path) VALUES(?1,?2) ON CONFLICT(kind,path) DO NOTHING",
        -1,
        &Insert_,
        nullptr);
    return prepared == SQLITE_OK ? iceberg::Status{} : SqliteError(prepared);
}

TRecoveryVisited::~TRecoveryVisited()
{
    sqlite3_finalize(Insert_);
    sqlite3_close_v2(Database_);
    if (!Path_.empty()) {
        std::error_code ignored;
        std::filesystem::remove(Path_, ignored);
    }
}

iceberg::Result<bool> TRecoveryVisited::Insert(NIO::EFileKind kind, std::string_view path)
{
    auto result = CatchIcebergExceptions([&]() -> iceberg::Result<bool> {
        if (FirstError_) {
            return std::unexpected(*FirstError_);
        }
        if (!IsKnownKind(kind) || path.empty() || path.size() > MaxPathBytes ||
            path.find('\0') != std::string_view::npos)
        {
            return iceberg::InvalidArgument("Invalid recovery visited kind or path.");
        }
        auto code = sqlite3_bind_int(Insert_, 1, static_cast<int>(kind));
        if (code == SQLITE_OK) {
            code = sqlite3_bind_blob(Insert_, 2, path.data(), static_cast<int>(path.size()), SQLITE_TRANSIENT);
        }
        if (code == SQLITE_OK) {
            code = sqlite3_step(Insert_);
        }
        const auto changed = sqlite3_changes64(Database_);
        const auto reset = sqlite3_reset(Insert_);
        const auto cleared = sqlite3_clear_bindings(Insert_);
        if (code != SQLITE_DONE || reset != SQLITE_OK || cleared != SQLITE_OK) {
            const auto failed = SqliteError(code != SQLITE_DONE ? code : reset != SQLITE_OK ? reset : cleared);
            return std::unexpected(failed.error());
        }
        return changed != 0;
    });
    if (!result && !FirstError_) {
        FirstError_ = result.error();
    }
    return result;
}

} // namespace NGlamDB::NStorage
