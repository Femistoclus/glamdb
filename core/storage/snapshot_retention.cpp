#include "core/storage/snapshot_retention.h"

#include "core/storage/format_registry.h"
#include "core/storage/format_verifier.h"

#include <iceberg/file_io.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>
#include <map>
#include <set>

#include <algorithm>
#include <charconv>
#include <limits>

namespace NGlamDB::NStorage {
using iceberg::Error;
namespace {
iceberg::Result<std::int64_t>
ReadInteger(const std::unordered_map<std::string, std::string>& properties, const char* key, std::int64_t missing)
{
    const auto found = properties.find(key);
    if (found == properties.end()) {
        return missing;
    }
    std::int64_t value = 0;
    const auto& text = found->second;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() || std::to_string(value) != text) {
        return iceberg::ValidationFailed("Invalid snapshot retention integer.");
    }
    return value;
}
}

iceberg::Status CheckRetentionPolicy(const TRetentionPolicy& policy)
{
    if ((policy.KeepLast && (*policy.KeepLast < 1 || *policy.KeepLast > (1LL << 40))) || policy.MinAgeMs < 0 ||
        policy.MinAgeMs > 253402300799999LL)
    {
        return iceberg::InvalidArgument(
            "Retention keep_last must be 1..1099511627776 and min_age must be nonnegative and bounded.");
    }
    return {};
}

iceberg::Status StripRetentionProperties(std::unordered_map<std::string, std::string>& properties)
{
    const bool enabled = properties.contains(RetentionVersionKey);
    if (enabled && properties.at(RetentionVersionKey) != "1") {
        return iceberg::ValidationFailed("Unsupported snapshot retention version.");
    }
    if (!enabled &&
        (properties.contains(RetentionKeepKey) || properties.contains(RetentionAgeKey) ||
            properties.contains(ExpiredSequenceKey)))
    {
        return iceberg::ValidationFailed("Retention properties lack their version.");
    }
    ICEBERG_ASSIGN_OR_RAISE(auto keep, ReadInteger(properties, RetentionKeepKey, 0));
    ICEBERG_ASSIGN_OR_RAISE(auto age, ReadInteger(properties, RetentionAgeKey, 0));
    ICEBERG_ASSIGN_OR_RAISE(auto expired, ReadInteger(properties, ExpiredSequenceKey, 0));
    if (keep < 0 || expired < 0 || (properties.contains(RetentionKeepKey) && keep == 0)) {
        return iceberg::ValidationFailed("Invalid snapshot retention state.");
    }
    ICEBERG_RETURN_UNEXPECTED(CheckRetentionPolicy({keep == 0 ? std::nullopt : std::optional(keep), age}));
    properties.erase(RetentionVersionKey);
    properties.erase(RetentionKeepKey);
    properties.erase(RetentionAgeKey);
    properties.erase(ExpiredSequenceKey);
    return {};
}

iceberg::Result<TRetentionPolicy> ReadRetentionPolicy(const iceberg::TableMetadata& metadata)
{
    auto properties = metadata.properties.configs();
    ICEBERG_RETURN_UNEXPECTED(StripRetentionProperties(properties));
    ICEBERG_ASSIGN_OR_RAISE(auto keep, ReadInteger(metadata.properties.configs(), RetentionKeepKey, 0));
    ICEBERG_ASSIGN_OR_RAISE(auto age, ReadInteger(metadata.properties.configs(), RetentionAgeKey, 0));
    return TRetentionPolicy{keep == 0 ? std::nullopt : std::optional(keep), age};
}

iceberg::Result<std::int64_t> ExpiredSequence(const iceberg::TableMetadata& metadata)
{
    ICEBERG_RETURN_UNEXPECTED(ReadRetentionPolicy(metadata));
    return ReadInteger(metadata.properties.configs(), ExpiredSequenceKey, 0);
}

iceberg::Status
WriteRetentionPolicy(iceberg::TableMetadata& metadata, const TRetentionPolicy& policy, std::int64_t expiredSequence)
{
    ICEBERG_RETURN_UNEXPECTED(CheckRetentionPolicy(policy));
    if (expiredSequence < 0 || (expiredSequence >= metadata.last_sequence_number && expiredSequence != 0)) {
        return iceberg::ValidationFailed("Expiration cannot remove the current snapshot.");
    }
    auto properties = metadata.properties.configs();
    ICEBERG_RETURN_UNEXPECTED(StripRetentionProperties(properties));
    properties[RetentionVersionKey] = "1";
    if (policy.KeepLast) {
        properties[RetentionKeepKey] = std::to_string(*policy.KeepLast);
    }
    properties[RetentionAgeKey] = std::to_string(policy.MinAgeMs);
    properties[ExpiredSequenceKey] = std::to_string(expiredSequence);
    metadata.properties = iceberg::TableProperties::FromMap(std::move(properties));
    return {};
}

iceberg::Result<std::size_t>
ExpirationPrefix(const iceberg::TableMetadata& metadata, const TRetentionPolicy& policy, std::int64_t nowMs)
{
    ICEBERG_RETURN_UNEXPECTED(CheckRetentionPolicy(policy));
    if ((!policy.KeepLast && policy.MinAgeMs == 0) || metadata.snapshots.empty()) {
        return 0;
    }
    const auto keep = std::max<std::size_t>(1, static_cast<std::size_t>(policy.KeepLast.value_or(1)));
    const auto maximum = metadata.snapshots.size() > keep ? metadata.snapshots.size() - keep : 0;
    std::size_t count = 0;
    for (; count < maximum; ++count) {
        const auto timestamp = metadata.snapshots[count]->timestamp_ms.time_since_epoch().count();
        // Supported timestamps and bounded ages keep this subtraction representable.
        if (policy.MinAgeMs != 0 && (timestamp > nowMs || nowMs - timestamp < policy.MinAgeMs)) {
            break;
        }
    }
    return count;
}

iceberg::Result<TExpirationPlan> PlanExpiration(const iceberg::TableMetadata& metadata,
    std::span<const std::int64_t> archived,
    std::uint64_t archivedTotal,
    const TRetentionPolicy& policy,
    std::int64_t nowMs)
{
    ICEBERG_RETURN_UNEXPECTED(CheckRetentionPolicy(policy));
    TExpirationPlan plan;
    if ((!policy.KeepLast && policy.MinAgeMs == 0) || metadata.snapshots.empty() || archived.size() > archivedTotal ||
        archivedTotal > std::numeric_limits<std::uint64_t>::max() - metadata.snapshots.size())
    {
        return plan;
    }
    const auto keep = std::max<std::uint64_t>(1, static_cast<std::uint64_t>(policy.KeepLast.value_or(1)));
    const auto total = archivedTotal + metadata.snapshots.size();
    const auto maximum = total > keep ? total - keep : 0;
    // Supported timestamps and bounded ages keep this subtraction representable.
    const auto old = [&](std::int64_t timestamp) {
        return policy.MinAgeMs == 0 || (timestamp <= nowMs && nowMs - timestamp >= policy.MinAgeMs);
    };
    for (; plan.Archived < archived.size() && plan.Archived < maximum && old(archived[plan.Archived]);
         ++plan.Archived)
    { }
    if (plan.Archived < archivedTotal) {
        return plan;
    }
    for (; plan.Root < metadata.snapshots.size() && plan.Archived + plan.Root < maximum &&
         old(metadata.snapshots[plan.Root]->timestamp_ms.time_since_epoch().count());
         ++plan.Root)
    { }
    return plan;
}

iceberg::Result<TMetadataRetirement> FindExpiredMetadata(const std::shared_ptr<iceberg::FileIO>& fileIO,
    const iceberg::TableMetadata& metadata,
    std::size_t prefix,
    const TStorageLimits& limits,
    std::span<const TArchivedList> expiredArchive,
    const TArchivedList* remainingArchive)
{
    if (!fileIO || limits.ManifestListBytes > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) ||
        limits.Store.Manifests > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    {
        return iceberg::InvalidArgument("Invalid retirement verification limits.");
    }
    if ((prefix >= metadata.snapshots.size() && prefix != 0) || (remainingArchive && prefix != 0)) {
        return iceberg::ValidationFailed("Metadata retirement removes the current snapshot.");
    }
    TMetadataRetirement result;
    if (prefix == 0 && expiredArchive.empty()) {
        return result;
    }
    RegisterStorageFormats();
    std::map<std::string, std::uint64_t> candidates;
    std::set<std::string> retained;
    // Manifests of the remaining archive are protected but are not root state.
    std::set<std::string> archiveRetained;
    const auto addCandidate = [&](const iceberg::ManifestFile& manifest) -> iceberg::Status {
        if (manifest.manifest_length <= 0 || static_cast<std::uint64_t>(manifest.manifest_length) > limits.ManifestBytes)
        {
            return iceberg::ValidationFailed("Invalid retirement manifest bytes.");
        }
        const auto [position, added] =
            candidates.emplace(manifest.manifest_path, static_cast<std::uint64_t>(manifest.manifest_length));
        if ((!added && position->second != static_cast<std::uint64_t>(manifest.manifest_length)) ||
            candidates.size() > limits.Store.Manifests)
        {
            return iceberg::ValidationFailed("Inconsistent retirement manifest inventory.");
        }
        return {};
    };
    for (const auto& archived : expiredArchive) {
        ICEBERG_ASSIGN_OR_RAISE(auto list,
            ReadManifestListIdentity(fileIO, archived.Path, limits, archived.Bytes, archived.Sha256));
        result.Files.push_back({archived.Path, NIO::EFileKind::ManifestList, list.Bytes});
        result.Removed.ManifestListBytes += list.Bytes;
        for (const auto& manifest : list.Manifests) {
            ICEBERG_RETURN_UNEXPECTED(addCandidate(manifest));
        }
    }
    if (remainingArchive) {
        ICEBERG_ASSIGN_OR_RAISE(auto list,
            ReadManifestListIdentity(fileIO,
                remainingArchive->Path,
                limits,
                remainingArchive->Bytes,
                remainingArchive->Sha256));
        for (const auto& manifest : list.Manifests) {
            archiveRetained.insert(manifest.manifest_path);
        }
    }
    for (std::size_t i = 0; i < metadata.snapshots.size(); ++i) {
        const auto& snapshot = metadata.snapshots[i];
        ICEBERG_ASSIGN_OR_RAISE(auto input, fileIO->NewInputFile(snapshot->manifest_list));
        ICEBERG_ASSIGN_OR_RAISE(auto bytes, input->Size());
        if (bytes <= 0 || static_cast<std::uint64_t>(bytes) > limits.ManifestListBytes) {
            return iceberg::ValidationFailed("Invalid retirement manifest-list bytes.");
        }
        ICEBERG_RETURN_UNEXPECTED(CheckAvroContainer(*input,
            bytes,
            {static_cast<std::int64_t>(limits.ManifestListBytes),
                static_cast<std::int64_t>(limits.Store.Manifests),
                static_cast<std::int64_t>(limits.ManifestListBytes)}));
        if (i < prefix) {
            result.Files.push_back(
                {snapshot->manifest_list, NIO::EFileKind::ManifestList, static_cast<std::uint64_t>(bytes)});
            result.Removed.ManifestListBytes += static_cast<std::uint64_t>(bytes);
        }
        ICEBERG_ASSIGN_OR_RAISE(auto reader, iceberg::ManifestListReader::Make(snapshot->manifest_list, fileIO));
        ICEBERG_ASSIGN_OR_RAISE(auto manifests, reader->Files());
        if (manifests.size() > limits.Store.Manifests) {
            return iceberg::ValidationFailed("Retirement manifest inventory exceeds capacity.");
        }
        for (const auto& manifest : manifests) {
            if (manifest.manifest_length <= 0 ||
                static_cast<std::uint64_t>(manifest.manifest_length) > limits.ManifestBytes)
            {
                return iceberg::ValidationFailed("Invalid retirement manifest bytes.");
            }
            if (i < prefix) {
                ICEBERG_RETURN_UNEXPECTED(addCandidate(manifest));
            } else {
                retained.insert(manifest.manifest_path);
                if (retained.size() > limits.Store.Manifests) {
                    return iceberg::ValidationFailed("Retained manifest union exceeds capacity.");
                }
            }
        }
    }
    for (const auto& [path, bytes] : candidates) {
        if (!retained.contains(path) && !archiveRetained.contains(path)) {
            result.Files.push_back({path, NIO::EFileKind::Manifest, bytes});
            result.Removed.Manifests += 1;
            result.Removed.ManifestBytes += bytes;
        }
    }
    result.RetainedManifests.assign(retained.begin(), retained.end());
    return result;
}

}
