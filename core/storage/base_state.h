#pragma once

#include "core/storage/capacity.h"

#include <iceberg/manifest/manifest_list.h>

#include <cstdint>
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

// Verifies one retained append without walking the whole history: the root
// parses with the supported profile, `snapshotID` is retained, its transition
// from the parent snapshot is a valid append, and its manifest list, new
// manifest and data file are intact. Returns the root state (usage covers the
// root only).
iceberg::Result<TBaseState> VerifyRetainedSnapshot(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    std::int64_t snapshotID,
    const TStorageLimits& limits = {});

iceberg::Result<TBaseState> VerifyReachable(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const std::string& rootLocation,
    const std::string& storeID,
    const std::string& collectionID,
    const TStorageLimits& limits = {},
    const std::string& workspaceDirectory = {});

}
