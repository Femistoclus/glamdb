#pragma once

#include "apps/server/scans.h"
#include "apps/transport/http.h"

#include <filesystem>
#include <mutex>

namespace glamdb::NServer {
class TDatabases;
class TScans;
class TMetadata;
class TRoutes
{
public:
    struct THttpActivity
    {
        size_t Connected = 0, Pending = 0, Executing = 0, BodyWaiters = 0;
        uint64_t OldestQueueAgeMs = 0;
    };
    TRoutes(std::shared_ptr<Engine> engine, std::filesystem::path uiRoot, std::string token, TScanLimits scans = {});
    TRoutes(std::shared_ptr<TDatabases> databases,
        std::filesystem::path uiRoot,
        std::string token,
        TScanLimits scans = {});
    std::shared_ptr<ResourceOwner> GetResourceOwner() const;
    // The listener snapshots its I/O-thread-owned queues before observation.
    void SetHttpActivity(THttpActivity activity);
    NTransport::TResponse Handle(const NTransport::TRequest& request) const;
    // Service shutdown: close scan admission and cancel without waiting for active reads.
    void CancelScans() const;
    void CancelScans(const std::string& database) const;
    bool IsAuthorized(const NTransport::TRequest& request) const;

private:
    NTransport::TJson DescribeCapabilities(const EngineCapabilities& capabilities,
        const std::optional<StoreHealth>& health,
        bool databaseManagement,
        const std::string& storeID = {}) const;
    NTransport::TResponse Route(const NTransport::TRequest& request) const;
    NTransport::TResponse RoutePrepared(const NTransport::TRequest& request,
        const std::string& path,
        const std::map<std::string, std::string>& query,
        const NTransport::TJson& body,
        const NTransport::TJsonRows* rows = nullptr) const;
    NTransport::TResponse StaticFile(const std::string& path) const;
    std::shared_ptr<ResourceOwner> Resources_;
    struct THttpObservation
    {
        std::mutex Mutex;
        THttpActivity Activity;
    };
    std::shared_ptr<THttpObservation> HttpObservation_ = std::make_shared<THttpObservation>();
    std::shared_ptr<TScans> Scans_;
    std::shared_ptr<TMetadata> Metadata_;
    std::string DatabaseID_;
    std::shared_ptr<Engine> Engine_;
    std::shared_ptr<TDatabases> Databases_;
    std::filesystem::path UIRoot_;
    std::string Token_;
};
} // namespace glamdb::NServer
