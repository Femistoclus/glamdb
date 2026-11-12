#pragma once

#include "storage/types.h"

#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace glamdb::detail {

// Bounded disk scratch for mutation deduplication and bytewise UTF-8 sorting.
// It carries no publication authority and is discarded after production.
class TKvSpool
{
public:
    struct TEntry
    {
        std::string Key;
        std::string Payload;
    };
    class TCursor;

    static Result<std::unique_ptr<TKvSpool>> Make(const std::filesystem::path& directory,
        std::string_view token,
        uint64_t maxBytes = 4ULL << 30,
        uint64_t maxIoBytes = 16ULL << 30,
        std::function<Status()> checkActive = {});
    ~TKvSpool();
    Status Insert(int64_t route, std::string_view key, std::string_view payload, DuplicatePolicy policy);
    Status Seal();
    Result<std::unique_ptr<TCursor>> ReadRoute(int64_t route);
    uint64_t IoBytes() const;
    uint64_t AllocatedBytes() const;

private:
    struct TState;
    std::shared_ptr<TState> State_;
    explicit TKvSpool(std::shared_ptr<TState> state);
};

class TKvSpool::TCursor
{
public:
    ~TCursor();
    Result<std::optional<TEntry>> Next();

private:
    friend class TKvSpool;
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
    explicit TCursor(std::unique_ptr<TImpl> impl);
};

} // namespace glamdb::detail
