#pragma once

#include "apps/transport/http.h"
#include "storage/resources.h"

#include <chrono>
#include <memory>

namespace glamdb::NServer {
struct TScanLimits
{
    size_t Live = 64, LivePerDatabase = 8;
    size_t Completed = 64, CompletedPerDatabase = 8;
    uint64_t ReplayBytes = 64 * NTransport::MaxResponseBytes;
    uint64_t ReplayBytesPerDatabase = 8 * NTransport::MaxResponseBytes;
    std::chrono::milliseconds Idle = std::chrono::seconds(60);
    std::chrono::milliseconds Lifetime = std::chrono::seconds(0);
    std::chrono::milliseconds CompletedTtl = std::chrono::seconds(60);
};
enum class EScanRejection : uint8_t {
    GlobalLive,
    DatabaseLive,
    GlobalReplay,
    DatabaseReplay,
    SharedData,
    SharedControl,
    SharedPlanning,
    SharedDecode,
    SessionBusy,
    Count
};
struct TScanAdmissionFailure : NTransport::TFailure
{
    EScanRejection Reason;
    explicit TScanAdmissionFailure(EScanRejection reason);
};
NTransport::TResponse ScanAdmissionResponse(const TScanAdmissionFailure& failure);
void ValidateScanLimits(const TScanLimits& limits, const ResourceLimits* resources = nullptr);

// Bounded, expiring fixed-view cursors. Tokens are scoped to database, collection
// and representation; only the last consumed page can be replayed.
class TScans
{
public:
    static constexpr size_t SessionLimit = 64;
    static constexpr size_t DatabaseSessionLimit = 8;
    static constexpr size_t ReplayBytes = SessionLimit * NTransport::MaxResponseBytes;
    static constexpr int IdleSeconds = 60;
    static constexpr int LifetimeSeconds = 0;
    explicit TScans(TScanLimits limits = {}, std::shared_ptr<ResourceOwner> resources = {});
    explicit TScans(std::chrono::milliseconds idle,
        std::chrono::milliseconds lifetime = std::chrono::seconds(LifetimeSeconds));
    ~TScans();
    const TScanLimits& GetLimits() const;
    NTransport::TJson GetStatistics(const std::string& database = {}) const;
    NTransport::TResponse Open(const std::shared_ptr<Engine>& engine,
        const std::string& database,
        const std::string& collection,
        const NTransport::TJson& body,
        bool arrow,
        bool openOnly);
    NTransport::TResponse
    Cancel(const std::string& database, const std::string& collection, const NTransport::TJson& body);
    // Permanently close admission and mark sessions cancelled without waiting for reads.
    void Stop();
    void Clear(const std::string& database);
    void Clear();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};
} // namespace glamdb::NServer
