#pragma once

#include "core/api/binary.h"

#include <filesystem>
#include <sqlite3.h>

namespace glamdb::detail {

struct Blob
{
    std::string bytes;
};
using SqlValue = std::variant<std::monostate, int64_t, std::string, Blob>;
using SqlRow = std::vector<std::optional<std::string>>;

class Database
{
public:
    Database() = default;
    ~Database();
    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;
    void Open(const std::filesystem::path& path, int flags, const std::string& vfs);
    void Close();
    void Execute(std::string_view sql, std::initializer_list<SqlValue> values = {});
    std::vector<SqlRow> Query(std::string_view sql, std::initializer_list<SqlValue> values = {});
    int64_t Integer(std::string_view sql);
    std::string Text(std::string_view sql);
    int64_t Changes() const;
    bool Autocommit() const;
    std::string VfsName() const;

private:
    sqlite3* connection_ = nullptr;
    sqlite3_stmt* Prepare(std::string_view sql, std::initializer_list<SqlValue> values);
    [[noreturn]] void Throw(int code) const;
};

int64_t ParseInteger(std::string_view text);
std::string Required(const SqlRow& row, size_t column);

} // namespace glamdb::detail
