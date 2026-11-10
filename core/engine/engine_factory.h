#pragma once

#include "core/engine/storage_backend.h"
#include "storage/engine.h"

namespace glamdb::detail {

// Internal integration seam; never installed and never accepts a client writer.
struct EngineFactory {
    static Result<std::shared_ptr<Engine>> Create(const CreateStoreRequest &request,
                                                  std::shared_ptr<StorageBackend> backend = {},
                                                  std::string sqlite_vfs = {});
    static Result<std::shared_ptr<Engine>>
    Open(const OpenStoreRequest &request, std::shared_ptr<StorageBackend> backend = {}, std::string sqlite_vfs = {});
};

} // namespace glamdb::detail
