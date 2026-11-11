#pragma once

#include "storage/metadata.h"

#include <functional>
#include <map>
#include <mutex>

namespace glamdb::detail {

class TMetadataPages
{
public:
    using TClock = std::chrono::steady_clock;
    using TListing = std::variant<TCollectionPage, SnapshotHistory, TFileListing>;
    using TLoad = std::function<Result<TListing>()>;

    struct TOptions
    {
        std::chrono::milliseconds Lifetime = std::chrono::minutes(5);
        size_t MaxSessions = 32;
        size_t MaxBytes = size_t{16} * 1024 * 1024;
    };

    TMetadataPages();
    explicit TMetadataPages(TOptions options);
    Result<TListing> Read(const TMetadataPageRequest& request,
        std::string_view parameters,
        const TLoad& load,
        TClock::time_point now = TClock::now(),
        std::optional<uint64_t> admissionGeneration = {});
    uint64_t GetGeneration() const;
    void Clear();

private:
    struct TSession
    {
        std::string Binding;
        TListing Listing;
        TClock::time_point Expires;
        size_t ChargedBytes = 0;
        std::vector<std::string> Tokens{};
        std::map<std::string, size_t, std::less<>> Positions{};
    };

    void Expire(TClock::time_point now);
    static TListing ReadPage(const TSession& session, size_t offset, size_t pageSize);

    TOptions Options_;
    mutable std::mutex Mutex_;
    size_t Pending_ = 0;
    size_t ChargedBytes_ = 0;
    uint64_t Generation_ = 0;
    std::vector<TSession> Sessions_;
};

} // namespace glamdb::detail
