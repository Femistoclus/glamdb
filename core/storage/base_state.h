#pragma once

#include "core/storage/capacity.h"

#include <iceberg/manifest/manifest_list.h>

#include <memory>
#include <string>
#include <vector>

namespace iceberg {
class FileIO;
struct TableMetadata;
}

namespace NGlamDB::NIO {
class TIOAttempt;
}

namespace NGlamDB::NStorage {

struct TBaseState
{
    std::string MetadataLocation;
    std::shared_ptr<const iceberg::TableMetadata> Metadata;
    std::vector<iceberg::ManifestFile> CurrentManifests;
    TStorageUsage Usage;
};

iceberg::Result<TBaseState> CheckAppendClosure(const std::shared_ptr<iceberg::FileIO>& fileIO,
    TBaseState candidate,
    const std::vector<iceberg::ManifestFile>& previousManifests,
    const std::shared_ptr<NIO::TIOAttempt>& attempt,
    const TStorageLimits& limits);

iceberg::Result<TBaseState> VerifyReachable(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits = {});

}
