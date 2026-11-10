#pragma once

#include <iceberg/result.h>

#include <string>
#include <unordered_map>

namespace iceberg {
struct ManifestFile;
struct TableMetadata;
}

namespace NGlamDB::NStorage {

struct TTableProfile;

iceberg::Status
CheckCreateMetadata(const iceberg::TableMetadata& candidate, const TTableProfile& profile, const std::string& location);
iceberg::Status CheckAppendMetadata(const iceberg::TableMetadata& base,
    const iceberg::TableMetadata& candidate,
    const std::unordered_map<std::string, std::string>& expectedSummary);
bool EqualManifest(const iceberg::ManifestFile& left, const iceberg::ManifestFile& right);

}
