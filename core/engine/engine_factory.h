#pragma once

#include "core/engine/storage_backend.h"
#include "storage/engine.h"

#include <filesystem>
#include <functional>

namespace glamdb::detail {

// Builds the storage backend after the store lease has created or validated
// the store root and warehouse. An empty factory installs no backend.
using BackendFactory =
    std::function<Result<std::shared_ptr<StorageBackend>>(const std::filesystem::path& root, const StoreInfo& store)>;

// Internal integration seam; never installed and never accepts a client writer.
struct EngineFactory
{
    static Result<std::shared_ptr<Engine>> Create(const CreateStoreRequest& request,
        std::shared_ptr<StorageBackend> backend = {},
        std::string sqlite_vfs = {});
    static Result<std::shared_ptr<Engine>>
    Open(const OpenStoreRequest& request, std::shared_ptr<StorageBackend> backend = {}, std::string sqlite_vfs = {});
    static Result<std::shared_ptr<Engine>>
    CreateWith(const CreateStoreRequest& request, const BackendFactory& backend, std::string sqlite_vfs = {});
    static Result<std::shared_ptr<Engine>>
    OpenWith(const OpenStoreRequest& request, const BackendFactory& backend, std::string sqlite_vfs = {});
};

} // namespace glamdb::detail
