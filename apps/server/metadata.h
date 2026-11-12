#pragma once

#include "apps/server/scans.h"
#include "apps/transport/http.h"
#include "apps/transport/metadata.h"

#include <chrono>
#include <functional>
#include <memory>

namespace glamdb::NServer {
EngineCapabilities InstalledCapabilities();
NTransport::TJson CapabilitiesJson(const EngineCapabilities& capabilities,
    const std::optional<StoreHealth>& health,
    bool databaseManagement,
    const std::string& storeID = {},
    const TScanLimits& scans = {},
    const Limits& limits = {});
NTransport::TJson ExplainJson(const Engine& engine,
    const std::string& collection,
    const NTransport::TJson& body,
    const TScanLimits& scans = {});

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
