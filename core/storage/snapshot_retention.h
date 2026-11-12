#pragma once

#include "core/io/store_uri.h"
#include "core/storage/capacity.h"

#include <iceberg/result.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace iceberg {
struct TableMetadata;
class FileIO;
}

namespace NGlamDB::NStorage {

struct TRetentionPolicy
{
    std::optional<std::int64_t> KeepLast{};
    std::int64_t MinAgeMs = 0;
    bool operator==(const TRetentionPolicy&) const = default;
};

struct TExpiredMetadataFile
{
    std::string Path;
    NIO::EFileKind Kind;
    std::uint64_t Bytes = 0;
};

struct TMetadataRetirement
{
    std::vector<TExpiredMetadataFile> Files;
    std::vector<std::string> RetainedManifests;
    TStorageUsage Removed;
};

// Finds last-reference metadata across the complete retained snapshot union.
iceberg::Result<TMetadataRetirement> FindExpiredMetadata(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const iceberg::TableMetadata& metadata,
    std::size_t prefix,
    const TStorageLimits& limits);

inline constexpr char RetentionVersionKey[] = "glamdb.retention.version";
inline constexpr char RetentionKeepKey[] = "glamdb.retention.keep-last";
inline constexpr char RetentionAgeKey[] = "glamdb.retention.min-age-ms";
inline constexpr char ExpiredSequenceKey[] = "glamdb.retention.expired-through-sequence";

iceberg::Status CheckRetentionPolicy(const TRetentionPolicy& policy);
iceberg::Result<TRetentionPolicy> ReadRetentionPolicy(const iceberg::TableMetadata& metadata);
iceberg::Result<std::int64_t> ExpiredSequence(const iceberg::TableMetadata& metadata);
iceberg::Status
WriteRetentionPolicy(iceberg::TableMetadata& metadata, const TRetentionPolicy& policy, std::int64_t expiredSequence);
iceberg::Result<std::size_t>
ExpirationPrefix(const iceberg::TableMetadata& metadata, const TRetentionPolicy& policy, std::int64_t nowMs);
// Validates and removes the supported extension properties from a profile copy.
iceberg::Status StripRetentionProperties(std::unordered_map<std::string, std::string>& properties);

}
