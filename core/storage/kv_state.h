#pragma once

#include "core/storage/base_state.h"

namespace NGlamDB::NStorage {

iceberg::Result<TBaseState> CheckKVClosure(const std::shared_ptr<iceberg::FileIO>& fileIO,
    TBaseState candidate,
    const TBaseState& base,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits);

iceberg::Result<TBaseState> VerifyKVReachable(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits = {},
    const std::string& workspaceDirectory = {});

}
