#pragma once

#include "core/io/local_fs.h"

#include <iceberg/result.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

struct sqlite3;
struct sqlite3_stmt;

namespace NGlamDB::NStorage {

class TRecoveryVisited
{
public:
    static iceberg::Result<std::unique_ptr<TRecoveryVisited>> Make(std::uint64_t maxBytes = 256ULL << 20,
        const std::string& directory = {});
    ~TRecoveryVisited();
    TRecoveryVisited(const TRecoveryVisited&) = delete;
    TRecoveryVisited& operator=(const TRecoveryVisited&) = delete;

    iceberg::Result<bool> Insert(NIO::EFileKind kind, std::string_view path);

private:
    std::string Path_;
    sqlite3* Database_ = nullptr;
    sqlite3_stmt* Insert_ = nullptr;
    std::optional<iceberg::Error> FirstError_;

    TRecoveryVisited() = default;
    iceberg::Status Initialize(std::uint64_t maxBytes, const std::string& directory);
};

} // namespace NGlamDB::NStorage
