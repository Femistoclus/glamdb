#pragma once

#include "apps/transport/http.h"

#include <filesystem>
#include <functional>

namespace glamdb::NServer {
// Owns the service registry and leases each Engine for a complete HTTP call.
class TDatabases
{
public:
    // Optional VFS is an internal seam for deterministic registry fault tests.
    explicit TDatabases(const std::filesystem::path& directory,
        const std::string& registryVfs = {},
        std::shared_ptr<ResourceOwner> resources = {});
    ~TDatabases();
    TDatabases(const TDatabases&) = delete;
    TDatabases& operator=(const TDatabases&) = delete;
    std::shared_ptr<ResourceOwner> GetResourceOwner() const;
    NTransport::TJson List() const;
    NTransport::TJson Get(const std::string& id) const;
    StoreHealth Health(const std::string& id) const;
    // Process readiness ignores explicitly closed databases and fails closed during lifecycle/error states.
    bool IsReady() const noexcept;
    NTransport::TJson Create(const NTransport::TJson& body);
    NTransport::TJson
    Change(const std::string& id, std::string_view action, const NTransport::TJson& body = NTransport::TJson::object());
    NTransport::TResponse Use(const std::string& id,
        const std::function<NTransport::TResponse(std::shared_ptr<Engine>)>& call,
        bool recoveryOnly = false) const;
    // Releases cursor leases after admitted requests drain and before engine lifecycle changes.
    void SetScanCanceller(std::function<void(const std::string&)> cancel);
    void Shutdown();

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};
} // namespace glamdb::NServer
