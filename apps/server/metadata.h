#pragma once

#include "apps/transport/http.h"

#include <chrono>
#include <functional>
#include <memory>

namespace glamdb::NServer {
EngineCapabilities InstalledCapabilities();
NTransport::TJson CapabilitiesJson(const EngineCapabilities& capabilities,
    const std::optional<StoreHealth>& health,
    bool databaseManagement,
    const std::string& storeID = {});
SnapshotSelector MetadataSelector(const std::map<std::string, std::string>& query);
NTransport::TJson
StatisticsJson(const Engine& engine, const std::string& collection, const std::map<std::string, std::string>& query);
NTransport::TJson SnapshotsJson(const Engine& engine, const std::string& collection, SnapshotSelector selector);
NTransport::TJson FilesJson(const Engine& engine, const std::string& collection, SnapshotSelector selector);
NTransport::TJson FileJson(const Engine& engine,
    const std::string& collection,
    const std::string& file,
    const std::map<std::string, std::string>& query);
NTransport::TJson ExplainJson(const Engine& engine, const std::string& collection, const NTransport::TJson& body);

// Bounded, expiring value pages. The first public-facade result and resolved
// identity are retained, so concurrent appends cannot change later pages.
// Completed inventories retain replay until expiry or capacity reclamation;
// admission never evicts an inventory with unread pages.
// Page/Clear are thread-safe. The caller holds its database lease and admits
// every request; read runs outside the cache mutex and is never retained.
class TMetadata
{
public:
    explicit TMetadata(std::chrono::milliseconds idle = std::chrono::seconds(60),
        std::chrono::milliseconds lifetime = std::chrono::minutes(5));
    ~TMetadata();
    NTransport::TJson Page(const std::string& database,
        const std::string& collection,
        const std::string& kind,
        const std::map<std::string, std::string>& query,
        const std::function<NTransport::TJson(SnapshotSelector)>& read);
    void Clear(const std::string& database);
    void Clear();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};
} // namespace glamdb::NServer
