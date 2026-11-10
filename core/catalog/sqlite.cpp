#include "core/catalog/sqlite.h"

#include <charconv>
#include <limits>

namespace glamdb::detail {

Database::~Database() {
    if (connection_ != nullptr) {
        sqlite3_close(connection_);
    }
}

[[noreturn]] void Database::Throw(int code) const {
    const auto primary = code & 255;
    std::string error = primary == SQLITE_CORRUPT || primary == SQLITE_NOTADB ? "CORRUPTION"
                        : primary == SQLITE_CONSTRAINT                        ? "ALREADY_EXISTS"
                        : primary == SQLITE_FULL                              ? "CAPACITY_EXCEEDED"
                        : primary == SQLITE_BUSY || primary == SQLITE_LOCKED  ? "RESOURCE_EXHAUSTED"
                                                                              : "IO_ERROR";
    Failure failure({std::move(error),
                     "SQLite " + std::to_string(code) + ": " +
                         (connection_ != nullptr ? sqlite3_errmsg(connection_) : sqlite3_errstr(code)),
                     RetryHint::OperatorAction});
    failure.native_code = code;
    throw failure;
}

void Database::Open(const std::filesystem::path &path, int flags, const std::string &vfs) {
    Require(connection_ == nullptr, "INTERNAL", "connection already open");
    const auto result = sqlite3_open_v2(path.c_str(), &connection_,
                                        flags | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_NOFOLLOW | SQLITE_OPEN_EXRESCODE,
                                        vfs.empty() ? nullptr : vfs.c_str());
    if (result != SQLITE_OK) {
        Throw(result);
    }
    sqlite3_extended_result_codes(connection_, 1);
    sqlite3_limit(connection_, SQLITE_LIMIT_LENGTH, 1024 * 1024);
}

void Database::Close() {
    if (connection_ != nullptr) {
        const auto result = sqlite3_close(connection_);
        if (result != SQLITE_OK) {
            Throw(result);
        }
        connection_ = nullptr;
    }
}

sqlite3_stmt *Database::Prepare(std::string_view sql, std::initializer_list<SqlValue> values) {
    sqlite3_stmt *statement = nullptr;
    auto result = sqlite3_prepare_v3(connection_, sql.data(), static_cast<int>(sql.size()), 0, &statement, nullptr);
    if (result != SQLITE_OK) {
        Throw(result);
    }
    int index = 1;
    for (const auto &value : values) {
        if (std::holds_alternative<std::monostate>(value)) {
            result = sqlite3_bind_null(statement, index);
        } else if (const auto *integer = std::get_if<int64_t>(&value)) {
            result = sqlite3_bind_int64(statement, index, *integer);
        } else {
            const auto *text = std::get_if<std::string>(&value);
            const auto &bytes = text != nullptr ? *text : std::get<Blob>(value).bytes;
            result = text != nullptr ? sqlite3_bind_text(statement, index, bytes.data(), static_cast<int>(bytes.size()),
                                                         SQLITE_TRANSIENT)
                                     : sqlite3_bind_blob(statement, index, bytes.data(), static_cast<int>(bytes.size()),
                                                         SQLITE_TRANSIENT);
        }
        if (result != SQLITE_OK) {
            sqlite3_finalize(statement);
            Throw(result);
        }
        ++index;
    }
    return statement;
}

std::vector<SqlRow> Database::Query(std::string_view sql, std::initializer_list<SqlValue> values) {
    auto *statement = Prepare(sql, values);
    std::vector<SqlRow> rows;
    size_t total = 0;
    int result = SQLITE_OK;
    while ((result = sqlite3_step(statement)) == SQLITE_ROW) {
        SqlRow row;
        for (int column = 0; column < sqlite3_column_count(statement); ++column) {
            if (sqlite3_column_type(statement, column) == SQLITE_NULL) {
                row.emplace_back(std::nullopt);
            } else {
                const auto *data = static_cast<const char *>(sqlite3_column_blob(statement, column));
                const auto size = static_cast<size_t>(sqlite3_column_bytes(statement, column));
                total += size;
                if (total > 32ULL * 1024 * 1024 || rows.size() >= 8192) {
                    sqlite3_finalize(statement);
                    throw Failure(
                        {"CONFIG_TOO_SMALL", "catalog result exceeds control budget", RetryHint::OperatorAction});
                }
                row.emplace_back(std::string(data != nullptr ? data : "", size));
            }
        }
        rows.push_back(std::move(row));
    }
    const auto finalized = sqlite3_finalize(statement);
    if (result != SQLITE_DONE) {
        Throw(result);
    }
    if (finalized != SQLITE_OK) {
        Throw(finalized);
    }
    return rows;
}

void Database::Execute(std::string_view sql, std::initializer_list<SqlValue> values) {
    (void)Query(sql, values);
}

int64_t Database::Integer(std::string_view sql) {
    return ParseInteger(Text(sql));
}

std::string Database::Text(std::string_view sql) {
    const auto rows = Query(sql);
    Require(rows.size() == 1 && rows.front().size() == 1, "CORRUPTION", "expected one catalog value");
    return Required(rows.front(), 0);
}

int64_t Database::Changes() const {
    return sqlite3_changes64(connection_);
}
bool Database::Autocommit() const {
    return connection_ == nullptr || sqlite3_get_autocommit(connection_) != 0;
}

std::string Database::VfsName() const {
    sqlite3_vfs *vfs = nullptr;
    if (sqlite3_file_control(connection_, "main", SQLITE_FCNTL_VFS_POINTER, reinterpret_cast<void *>(&vfs)) !=
            SQLITE_OK ||
        vfs == nullptr) {
        throw Failure({"IO_ERROR", "cannot identify SQLite VFS", RetryHint::OperatorAction});
    }
    return vfs->zName;
}

int64_t ParseInteger(std::string_view text) {
    int64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size(), "CORRUPTION", "invalid catalog integer");
    return value;
}

std::string Required(const SqlRow &row, size_t column) {
    if (column >= row.size()) {
        throw Failure({"CORRUPTION", "missing catalog column", RetryHint::OperatorAction});
    }
    const auto &value = row.at(column);
    if (!value.has_value()) {
        throw Failure({"CORRUPTION", "missing catalog value", RetryHint::OperatorAction});
    }
    return *value;
}

} // namespace glamdb::detail
